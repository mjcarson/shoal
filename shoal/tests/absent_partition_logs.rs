//! Integration tests for what a table logs when a query waits on a partition that is not there
//!
//! A table asks its archive map what it can say about a partition without a read, and since
//! [F76](../../docs/src/features/paged-archive-map.md) the answer can be "a run may hold it, and
//! only a read of its page can say". The query parks while the loader reads that page, and a
//! partition the page does not hold, never written or pruned, releases it to answer as an empty
//! partition does. The loader logs that read at DEBUG, since a partition that is not there is no
//! failure. The table logged the same release at WARN as a partition that "could not be read",
//! 63,874 times in three hours of a benchmark in which nothing failed
//! ([Resolved #318](../../docs/src/appendix/resolved/absent-partition-release.md)).
//!
//! **This binary holds one test.** The counters and the subscriber that feeds them are process
//! wide, and a second test running beside this one would add its own events to the same numbers.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::field::{Field, Visit};
use tracing::{Event, Level};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

mod utils;

use utils::TestError;

/// What a table logs when it releases the queries parked on a partition read
const RELEASED: &str = "Releasing queries parked on a partition that could not be read";

/// What the loader logs when a partition it was asked to read is not there
const ABSENT: &str = "Partition was pruned before it could be read";

/// How many reads the loader found had no partition to read
static ABSENT_READS: AtomicUsize = AtomicUsize::new(0);

/// How many releases a table logged at WARN
static WARNED_RELEASES: AtomicUsize = AtomicUsize::new(0);

/// The `msg` field of one event
#[derive(Default)]
struct Message(Option<String>);

impl Visit for Message {
    /// Keep the message when it is recorded as a string
    ///
    /// # Arguments
    ///
    /// * `field` - The field being recorded
    /// * `value` - Its value
    fn record_str(&mut self, field: &Field, value: &str) {
        // only the message names what happened
        if field.name() == "msg" {
            self.0 = Some(value.to_owned());
        }
    }

    /// Keep the message when it is recorded any other way
    ///
    /// # Arguments
    ///
    /// * `field` - The field being recorded
    /// * `value` - Its value
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        // only the message names what happened, without the quotes `Debug` puts around it
        if field.name() == "msg" {
            self.0 = Some(format!("{value:?}").trim_matches('"').to_owned());
        }
    }
}

/// A tracing layer that counts the absent reads and the warned releases this process logs
struct CountReleases;

impl<S: tracing::Subscriber> Layer<S> for CountReleases {
    /// Count this event if it is an absent read or a release logged at WARN
    ///
    /// # Arguments
    ///
    /// * `event` - The event being logged
    /// * `_ctx` - The context of the subscriber this layer is part of
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // read what this event says happened
        let mut message = Message::default();
        event.record(&mut message);
        match message.0.as_deref() {
            // the loader found nothing to read
            Some(ABSENT) => {
                ABSENT_READS.fetch_add(1, Ordering::SeqCst);
            }
            // a table released the queries parked on a read, loudly
            Some(RELEASED) if *event.metadata().level() == Level::WARN => {
                WARNED_RELEASES.fetch_add(1, Ordering::SeqCst);
            }
            _ => (),
        }
    }
}

/// An unsorted row with a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct UnsortedRow {
    /// The partition key
    #[shoal(partition)]
    pub key: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A sorted row with a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct SortedRow {
    /// The partition key
    #[shoal(partition)]
    pub key: String,
    /// The sort key
    #[shoal(sort)]
    pub sort: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A schema with one persistent table of each kind, since each releases its parked queries in
/// code of its own
#[db]
pub struct TestDb {
    /// The unsorted table
    pub unsorted: PersistentUnsortedTable<UnsortedRow, FileSystem>,
    /// The sorted table
    pub sorted: PersistentSortedTable<SortedRow, FileSystem>,
}

/// A get answered empty, or a failure of any other kind
///
/// # Arguments
///
/// * `answer` - How many rows the get was answered with, or its failure
/// * `what` - The get, for the message
fn assert_empty(answer: Result<usize, shoal::client::Errors>, what: &str) {
    match answer {
        // no rows at all, or an empty partition's answer
        Ok(0) | Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => (),
        Ok(rows) => panic!("{what} found {rows} rows of partitions never written"),
        Err(error) => panic!("{what} was answered {error:?}, where a partition never written is empty"),
    }
}

/// A query released by a partition that is not there logs no warning
///
/// The server pages its archive map as hard as it goes, a tiny delta and no page cache, and its
/// runs keep filters that rule nothing out, and it is started again so nothing is resident. A get
/// of a key never written then parks until the loader reads a run's page and finds nothing there:
/// the path the loop leg's 4 KiB puts of new paths took whenever a run's filter could not rule
/// their key out, about one in 130. Every get is answered as an empty partition, and nothing
/// failed, so nothing is logged at WARN.
#[tokio::test]
async fn a_query_released_by_an_absent_partition_logs_no_warning() -> Result<(), TestError> {
    // count what this process logs about absent partitions
    tracing_subscriber::registry().with(CountReleases).init();
    // get a new temp dir for this test
    let temp_dir = utils::test_dir();
    // a server that compacts often and pages its archive map with no cache
    let (client, pool) =
        utils::start_with_conf::<TestDb>(utils::build_pressured_config(&temp_dir)).await?;
    // rows in both tables, so each has runs in its map with filters to ask
    for index in 0..64 {
        // a row in the unsorted table
        client
            .send_one(UnsortedRow {
                key: format!("written_{index}"),
                data: "x".repeat(256),
            })
            .await?;
        // and one in the sorted table
        client
            .send_one(SortedRow {
                key: format!("written_{index}"),
                sort: "a".to_owned(),
                data: "x".repeat(256),
            })
            .await?;
    }
    // shut this server down, which compacts what is left of its logs into archives
    pool.exit()?;
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // start it again, so nothing is resident and no page of the map is cached, with filters that
    // rule nothing out, so a key no run holds is known to be absent only once a page is read
    let mut conf = utils::build_pressured_config(&temp_dir);
    conf.storage.default.filesystem.map = conf.storage.default.filesystem.map.clone().filter_bits(0);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // gets of keys never written, ten at a time
    for batch in 0..4 {
        let keys: Vec<String> = (0..10)
            .map(|index| format!("never_{batch}_{index}"))
            .collect();
        // the unsorted table's
        let unsorted = client
            .send_one(UnsortedRowGet::new(keys.clone()))
            .await
            .and_then(|response| Ok(response.access::<UnsortedRow>()?.map_or(0, |rows| rows.len())));
        assert_empty(unsorted, &format!("the unsorted get of batch {batch}"));
        // and the sorted table's
        let sorted = client
            .send_one(SortedRowGet::new(keys))
            .await
            .and_then(|response| Ok(response.access::<SortedRow>()?.map_or(0, |rows| rows.len())));
        assert_empty(sorted, &format!("the sorted get of batch {batch}"));
    }
    // a get had to read a page to find its partition was never there, or this test proves nothing
    let absent = ABSENT_READS.load(Ordering::SeqCst);
    assert!(
        absent > 0,
        "no get waited on a read of a partition that was not there"
    );
    // and a partition that is not there is not a failure to read one
    assert_eq!(
        WARNED_RELEASES.load(Ordering::SeqCst),
        0,
        "{absent} reads found their partition was not there, and their releases were logged as failed reads"
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}
