//! A scan's pages admitted apart from the drivers' (F116), and captured from the loop's second
//! inbox (F117), and drawn from the node's scan budget (F118)
//!
//! A shard reads a few walk pages of each class at once and sheds the rest of that class, so a
//! client's scan filling its count never sheds the page a backfill or a scrub asks for
//! ([F116](../../docs/src/features/scan-admission.md)). These hold a scan's reads in flight on a
//! standalone node, through the admin read a client sends, and ask what the shard does with the
//! next page of each class. Their own binary, since the hold is a process-wide static.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::server::conf::Scans;
use shoal::server::faults;
use shoal::shared::identity::TableId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest, WalkAnswer, WalkClass};
use shoal::shared::protocol::error::ErrorCode;
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

mod utils;
use utils::TestError;

/// A row of the table a scan walks
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "AdmitDb")]
pub struct Row {
    /// The partition key
    #[shoal(partition)]
    pub id: u64,
    /// Some bytes, so a row has a size
    pub body: String,
}

/// A schema of one table
#[db]
pub struct AdmitDb {
    /// The table walked
    pub rows: PersistentUnsortedTable<Row, FileSystem>,
}

/// One page of a walk of tablet zero of the table, as a driver's or a scan's
///
/// # Arguments
///
/// * `class` - The page's class, or none for a request that names none
fn page(class: Option<WalkClass>) -> AdminRequest {
    AdminRequest {
        op: Uuid::new_v4(),
        expected_version: 0,
        kind: AdminKind::Walk {
            table: "Row".to_string(),
            tablet: 0,
            low: None,
            high: None,
            after: None,
            limit: None,
            bytes: None,
            at_least: None,
            class,
        },
    }
}

/// Ask a node for a page, its answer or its refusal's code and message
///
/// # Arguments
///
/// * `client` - A client of the node
/// * `class` - The page's class
async fn ask(client: &Shoal<AdmitDbClient>, class: Option<WalkClass>) -> Result<WalkAnswer, (ErrorCode, String)> {
    let response = client
        .admin(&page(class))
        .await
        .map_err(|error| (ErrorCode::Unknown, format!("{error:?}")))?;
    match response.outcome {
        Ok(AdminOutcome::Read(json)) => {
            serde_json::from_value(json).map_err(|error| (ErrorCode::Unknown, error.to_string()))
        }
        Ok(other) => Err((ErrorCode::Unknown, format!("{other:?}"))),
        Err(error) => Err((error.code(), error.msg)),
    }
}

/// Held by every test of this binary for its whole length: the holds are process-wide statics and
/// every test walks the one table, so two tests at once would let each other's pages go
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Take this binary's turn, whichever test before it panicked
fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Wait until this many pages are held before their reads, rather than for a time
///
/// # Arguments
///
/// * `pages` - How many
async fn until_held(pages: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while faults::walk_reads_waiting() < pages {
        assert!(Instant::now() < deadline, "only {} pages were held", faults::walk_reads_waiting());
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// With a shard's scan pages all held in flight, the next scan page is shed by its class's name
/// and a driver's page is still read and answered
///
/// W7's `client_scans_never_shed_a_drivers_page`, proved before M23 on the walk as it is: before
/// F116 every page shared one count of two, and the driver's page here was shed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_scans_never_shed_a_drivers_page() -> Result<(), TestError> {
    let _turn = serial();
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<AdmitDb>(&temp_dir).await?;
    let client = Arc::new(client);
    // a scan's reads of the table held, so its pages stay in flight
    let table = TableId::of("Row").0;
    faults::hold_walk_reads(table, WalkClass::Scan, true);
    let held: Vec<_> = (0..2)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { ask(&client, Some(WalkClass::Scan)).await })
        })
        .collect();
    until_held(2).await;
    // the next scan page is shed, naming the class whose count is full
    let shed = ask(&client, Some(WalkClass::Scan)).await.expect_err("a third scan page was read");
    assert_eq!(shed.0, ErrorCode::Shedding, "{shed:?}");
    assert!(shed.1.contains("2 scan pages"), "{shed:?}");
    // a driver's page of the same tablet, on the same shard, is admitted and answered
    let driver = ask(&client, None).await.expect("a driver's page was shed beside a scan's");
    assert_eq!(driver.tablet, 0);
    // and once the scan's reads go, its pages are answered too
    faults::hold_walk_reads(table, WalkClass::Scan, false);
    for page in held {
        let answered = page.await.expect("the page's task").expect("a held scan page was refused");
        assert_eq!(answered.tablet, 0);
    }
    drop(pool);
    Ok(())
}

/// A node with scans off refuses a scan's page by name, as no refusal a scan asks again, and still
/// answers a driver's
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_answering_no_scans_refuses_a_scans_page_by_name() -> Result<(), TestError> {
    let _turn = serial();
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir).scans(Scans::default().enabled(false));
    let (client, pool) = utils::start_with_conf::<AdmitDb>(conf).await?;
    let refused = ask(&client, Some(WalkClass::Scan)).await.expect_err("a scan page was read");
    assert_eq!(refused.0, ErrorCode::InvalidRequest, "{refused:?}");
    assert!(refused.1.contains("scans.enabled"), "{refused:?}");
    ask(&client, None).await.expect("a driver's page was refused with scans off");
    drop(pool);
    Ok(())
}

/// Scan pages queued together in a shard's second inbox are all captured with no other message to
/// wake the loop, each saying how long it waited for its capture (F117)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_captures_drain_with_no_other_message() -> Result<(), TestError> {
    let _turn = serial();
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir).scans(Scans::default().pages_in_flight(8));
    let (client, pool) = utils::start_with_conf::<AdmitDb>(conf).await?;
    let client = Arc::new(client);
    // eight pages of one tablet at once, all on the shard hosting it
    let pages: Vec<_> = (0..8)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { ask(&client, Some(WalkClass::Scan)).await })
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    for page in pages {
        let answered = page.await.expect("the page's task").expect("a queued scan page was refused");
        assert_eq!(answered.tablet, 0);
    }
    assert!(Instant::now() < deadline, "the queued pages took past their bound");
    drop(pool);
    Ok(())
}

/// A scan's page draws its bytes from its node's scan budget before it is read: with the budget
/// held by one page in flight, the next is shed by name after its wait, a page larger than the
/// whole budget is refused at once, nothing is ever drawn past the budget, and every draw is given
/// back once the pages are answered
///
/// W7's `a_scans_buffers_are_drawn_from_its_budget_and_shed_past_it` (F118), proved before M23.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scans_buffers_are_drawn_from_its_budget_and_shed_past_it() -> Result<(), TestError> {
    let _turn = serial();
    let temp_dir = utils::test_dir();
    // a budget of one page of the default megabyte, on a node of two shards
    let mut conf = utils::build_config(&temp_dir);
    conf.resources = conf.resources.scan_memory("1MiB").expect("a size");
    let (client, pool) = utils::start_with_conf::<AdmitDb>(conf).await?;
    let client = Arc::new(client);
    let budget = pool.scan_budget().clone();
    assert_eq!(budget.limit(), 1 << 20);
    // one page holding the whole budget, its reads held
    let table = TableId::of("Row").0;
    faults::hold_walk_reads(table, WalkClass::Scan, true);
    let held = {
        let client = client.clone();
        tokio::spawn(async move { ask(&client, Some(WalkClass::Scan)).await })
    };
    until_held(1).await;
    assert_eq!(budget.used(), 1 << 20);
    // the next waits its quarter second and is shed, naming the budget
    let started = Instant::now();
    let shed = ask(&client, Some(WalkClass::Scan)).await.expect_err("a page drew past the budget");
    assert_eq!(shed.0, ErrorCode::Shedding, "{shed:?}");
    assert!(shed.1.contains("scan budget"), "{shed:?}");
    assert!(started.elapsed() >= Duration::from_millis(250));
    // a page past the whole budget is refused at once, by a code a scan does not ask again
    let request = AdminRequest {
        op: Uuid::new_v4(),
        expected_version: 0,
        kind: AdminKind::Walk {
            table: "Row".to_string(),
            tablet: 1,
            low: None,
            high: None,
            after: None,
            limit: None,
            bytes: Some(4 << 20),
            at_least: None,
            class: Some(WalkClass::Scan),
        },
    };
    let refused = client.admin(&request).await.expect("an answer").outcome.expect_err("a page past the budget was read");
    assert_eq!(refused.code(), ErrorCode::ResponseTooLarge, "{refused:?}");
    // nothing was ever drawn past the budget, and the sheds are counted
    assert!(budget.peak() <= budget.limit());
    assert!(budget.shed() >= 2);
    // let go, the held page is answered, and every draw comes back once it is
    faults::hold_walk_reads(table, WalkClass::Scan, false);
    held.await.expect("the page's task").expect("the held page was refused");
    let deadline = Instant::now() + Duration::from_secs(5);
    while budget.used() > 0 {
        assert!(Instant::now() < deadline, "{} bytes stayed drawn", budget.used());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(pool);
    Ok(())
}

/// A node refuses at start a scan's count of none while scans are answered
#[test]
fn no_scan_page_in_flight_is_refused_at_start() {
    let _turn = serial();
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir).scans(Scans::default().pages_in_flight(0));
    let refused = shoal::ShoalPool::<AdmitDb>::start(conf).err().expect("a node started reading no scan page");
    assert!(format!("{refused:?}").contains("scans.pages_in_flight"), "{refused:?}");
}
