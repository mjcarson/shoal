//! How long a write can sit staged in an intent log buffer while its shard is busy
//!
//! A table stages each write into an aligned buffer and writes the buffer out when the next
//! record will not fit. Before item 36 was fixed the only other thing that wrote a partial
//! buffer out was the shard's queue running dry, so a write staged behind a queue that never
//! drained was not written, synced or answered until a rotation or a lull
//! ([Resolved #36](../../docs/src/appendix/resolved/staged-tail-deadline.md)).
//!
//! A flood of reads makes that wait long - tens to hundreds of milliseconds on the unfixed tree -
//! but not forever, since a queue fed by real clients drains now and then. These tests keep the
//! queue from draining on purpose instead (`ShoalPool::busy_shard`), which is the case the bound
//! exists for and the one a test can tell apart from a slow machine.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalUnsortedTable};
use std::time::{Duration, Instant};

mod utils;

use utils::TestError;

/// How long the shard's queue is kept from draining, in milliseconds
const BUSY_MS: u64 = 10_000;

/// How long the write behind the busy queue may take to be answered
///
/// Generous against the bound the shard keeps, which is a millisecond by default, so a loaded
/// machine does not fail this; and half the time the queue is kept busy, so a write that waited
/// for the queue to drain cannot pass by the queue draining first.
const ANSWER_WITHIN: Duration = Duration::from_secs(5);

/// A row in the table these tests write
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// The payload
    #[shoal(update)]
    pub data: String,
}

/// The schema these tests run against
///
/// A persistent table, since only a table with an intent log stages anything.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub rows: PersistentUnsortedTable<TestRecord, FileSystem>,
}

/// A write staged on a shard whose queue never drains is answered anyway
///
/// One shard, so the write and the message keeping it busy share a queue. The write is sent
/// once the queue is busy, and it has to be answered while the queue still is.
#[tokio::test]
async fn a_write_behind_a_queue_that_never_drains_is_answered() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    // a write before the queue is busy is answered at once, which is what the one after is
    // measured against
    client
        .send_one(TestRecord {
            key: 1,
            data: "written while idle".to_owned(),
        })
        .await?;
    // keep the queue from draining from here on
    pool.busy_shard(0, BUSY_MS)?;
    let busy_from = Instant::now();
    // write one row behind it, and time its answer
    let answered = tokio::time::timeout(
        ANSWER_WITHIN,
        client.send_one(TestRecord {
            key: 2,
            data: "staged behind a busy queue".to_owned(),
        }),
    )
    .await;
    let took = busy_from.elapsed();
    match answered {
        Ok(outcome) => {
            outcome?;
        }
        Err(_) => panic!(
            "a write behind a queue that never drains was not answered within {ANSWER_WITHIN:?}"
        ),
    }
    // the queue has to have still been busy when it was, or its draining is what answered it
    assert!(
        took < Duration::from_millis(BUSY_MS),
        "the write was answered after {took:?}, once the queue had drained"
    );
    // and the row is there to be read
    let response = client.send_one(TestRecordGet::new(vec![2])).await?;
    let rows = response
        .access::<TestRecord>()?
        .expect("the row that was written");
    assert_eq!(rows.len(), 1);
    println!("answered in {took:?} behind a busy queue");
    drop(client);
    assert_eq!(pool.failure(), None);
    pool.exit()?;
    Ok(())
}
