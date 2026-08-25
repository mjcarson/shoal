//! Integration tests for how often a sorted table asks storage about a partition
//!
//! A sorted partition carries a `check_disk` flag saying it might still have rows in an archive
//! nobody has read yet. A get of such a partition asks the storage engine to load it, and if the
//! answer is that there is nothing on disk the get answers out of memory. That answer was thrown
//! away rather than recorded ([Resolved #80](../../docs/src/appendix/resolved/never-flushed-partitions.md)),
//! so a partition that had only ever been written to asked again on every single get.
//!
//! Nothing about the rows that came back said so - both paths answer identically - which is why
//! this is counted rather than asserted about an answer. The count is taken from the tracing span
//! `PersistentTable::block_on_load` opens, one per lookup, so the test measures the lookups the
//! shipping code actually makes rather than a probe added for it.
//!
//! **This binary holds one test.** The counter and the subscriber that feeds it are process wide,
//! and a second test running beside this one would add its own lookups to the same number.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::{EphemeralSortedTable, PersistentSortedTable};
use shoal_derive::{db, ShoalSortedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::span::Attributes;
use tracing::Id;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::util::SubscriberInitExt;

mod utils;

use utils::TestError;

/// How many times a table has asked its storage engine to load a partition
static DISK_LOOKUPS: AtomicUsize = AtomicUsize::new(0);

/// A tracing layer that counts the lookups a sorted table makes
///
/// `PersistentTable::block_on_load` is the only place either sorted table asks storage whether a
/// partition is on disk, and it is instrumented, so its span is the lookup.
struct CountDiskLookups;

impl<S: tracing::Subscriber> Layer<S> for CountDiskLookups {
    /// Count this span if it is a partition lookup
    ///
    /// # Arguments
    ///
    /// * `attrs` - The attributes of the span being opened
    /// * `_id` - The id assigned to the span
    /// * `_ctx` - The context of the subscriber this layer is part of
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        // only the lookup span counts
        if attrs.metadata().name() == "PersistentTable::block_on_load" {
            // one more question asked of storage
            DISK_LOOKUPS.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// A sorted row in a table that has a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct StoredRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// The sort key - orders records within a partition
    #[shoal(sort)]
    pub sort_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A sorted row in a table that has no storage engine at all
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct MemoryRecord {
    /// The partition key - groups related records
    #[shoal(partition)]
    pub partition_key: String,
    /// The sort key - orders records within a partition
    #[shoal(sort)]
    pub sort_key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A schema with one sorted table of each storage kind
#[db]
pub struct TestDb {
    /// The sorted table that keeps its rows on disk
    pub stored: PersistentSortedTable<StoredRecord, FileSystem>,
    /// The sorted table that never writes anything, so nothing is ever on disk
    pub memory: EphemeralSortedTable<MemoryRecord>,
}

/// A partition that was only ever written to is asked about once, not on every get
///
/// Both tables here are read three times over a partition that has never been flushed. Each
/// should ask storage exactly once: the first get does not know whether an archive exists, and
/// every get after it does.
#[tokio::test]
async fn a_partition_that_was_never_on_disk_is_looked_up_once() -> Result<(), TestError> {
    // count every partition lookup this process makes
    tracing_subscriber::registry().with(CountDiskLookups).init();
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // run a single shard so both partitions land on the same one
    let conf = utils::build_single_shard_config(&temp_dir);
    // start a shoal server and build a client
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // write several rows into one partition of each table, so neither answer is empty
    for sort_key in ["a", "b", "c", "d"] {
        // the table with a storage engine behind it
        client
            .send_one(StoredRecord {
                partition_key: "partition_key".to_owned(),
                sort_key: sort_key.to_owned(),
                data: "woot".to_owned(),
            })
            .await?;
        // and the one without
        client
            .send_one(MemoryRecord {
                partition_key: "partition_key".to_owned(),
                sort_key: sort_key.to_owned(),
                data: "woot".to_owned(),
            })
            .await?;
    }
    // read both partitions back three times over
    for _ in 0..3 {
        // the rows the persistent table is holding
        let stored = client
            .send_one(StoredRecordGet::new(vec!["partition_key".to_owned()]))
            .await?;
        assert_eq!(
            stored.access::<StoredRecord>()?.unwrap().len(),
            4,
            "the persistent table did not answer with the rows it was given"
        );
        // and the rows the ephemeral one is holding
        let memory = client
            .send_one(MemoryRecordGet::new(vec!["partition_key".to_owned()]))
            .await?;
        assert_eq!(
            memory.access::<MemoryRecord>()?.unwrap().len(),
            4,
            "the ephemeral table did not answer with the rows it was given"
        );
    }
    // six gets over two partitions asked storage twice - once per partition
    assert_eq!(
        DISK_LOOKUPS.load(Ordering::SeqCst),
        2,
        "a partition that storage said was not on disk was asked about again"
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}
