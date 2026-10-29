//! A scan's pages admitted apart from the drivers' (F116), and captured from the loop's second
//! inbox (F117), and drawn from the node's scan budget (F118), and their reads paced by their
//! device's idle time (F119)
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
use shoal::shared::placement::tablet_of;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest, WalkAnswer, WalkClass};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::traits::PartitionKeySupport;
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

/// What a walk of a whole tablet came to, a page after another
#[derive(Debug, Default)]
struct Walked {
    /// The pages it took
    pages: u64,
    /// The partitions they handed out
    partitions: usize,
    /// How many came from memory
    resident: u32,
    /// How many came from an archive
    archived: u32,
    /// How long their reads waited for their device, in microseconds
    paced_us: u64,
}

/// Walk tablet zero of the table whole, a page after another, as a driver or a scan
///
/// # Arguments
///
/// * `client` - A client of the node
/// * `class` - The pages' class
async fn walk_tablet_zero(client: &Shoal<AdmitDbClient>, class: Option<WalkClass>) -> Walked {
    let mut walked = Walked::default();
    let mut after = None;
    loop {
        let request = AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::Walk {
                table: "Row".to_string(),
                tablet: 0,
                low: None,
                high: None,
                after,
                limit: None,
                bytes: None,
                at_least: None,
                class,
            },
        };
        let answer: WalkAnswer = match client.admin(&request).await.expect("an answer").outcome {
            Ok(AdminOutcome::Read(json)) => serde_json::from_value(json).expect("a walk's answer"),
            other => panic!("a page of tablet zero was refused: {other:?}"),
        };
        walked.pages += 1;
        walked.partitions += answer.keys.len();
        walked.resident += answer.resident;
        walked.archived += answer.archived;
        walked.paced_us += answer.paced_us;
        // the cursor is the last key a page handed out
        match answer.next {
            Some(next) => after = Some(next),
            None => return walked,
        }
    }
}

/// The trickle the pacing test reads at while a foreground read is in flight, bytes a second
const TRICKLE: u64 = 512 << 10;

/// With a get's read held in flight on its device, a scan reads the device at its trickle and no
/// faster; once the read is done, at once in the device's idle time; and every foreground read is
/// counted until it is read, one that failed too ([F119](../../docs/src/features/scan-pacing.md))
///
/// W7's `a_scan_reads_only_in_idle_time_or_its_trickle`, proved before M23: before F119 a scan's
/// reads went as fast as its pages were asked for, beside any read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_reads_only_in_idle_time_or_its_trickle() -> Result<(), TestError> {
    let _turn = serial();
    let temp_dir = utils::test_dir();
    // a ceiling no scan here reaches and a trickle a test can time; every shard loop evicts what
    // has been compacted, so after a restart every row is read from its archive
    let mut conf = utils::build_config(&temp_dir).scans(Scans::default().ceiling(1 << 30).trickle(TRICKLE));
    conf.resources.memory = 1;
    // four hundred rows of tablet zero alone, some two kilobytes each
    let ids: Vec<u64> = (0..u64::MAX)
        .filter(|id| tablet_of(Row { id: *id, body: String::new() }.get_partition_key()) == 0)
        .take(400)
        .collect();
    let (client, pool) = utils::start_with_conf::<AdmitDb>(conf.clone()).await?;
    for id in &ids {
        client
            .send_one(Row {
                id: *id,
                body: format!("{id:016x}").repeat(128),
            })
            .await?;
    }
    // a start compacts every table's intent logs, so after one every row is in its archive
    pool.exit()?;
    drop(client);
    let (client, pool) = utils::start_with_conf::<AdmitDb>(conf).await?;
    let client = Arc::new(client);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let walked = walk_tablet_zero(&client, None).await;
        assert_eq!(walked.partitions, ids.len());
        if walked.resident == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "the rows never left memory: {walked:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // the node's two shards share the one device's pace
    let paces = pool.scan_paces();
    assert_eq!(paces.len(), 1, "{paces:?}");
    let pace = paces[0].clone();
    assert_eq!(pace.foreground_reads(), 0);
    // a get's read of an archived row held in flight on the device
    let table = TableId::of("Row").0;
    faults::hold_partition_reads(table, true);
    let get = {
        let client = client.clone();
        let id = ids[0];
        tokio::spawn(async move { client.send_one(RowGet::new(vec![id])).await })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while faults::partition_reads_waiting() < 1 {
        assert!(Instant::now() < deadline, "the get's read was never held");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(pace.busy());
    // a whole scan of the tablet meanwhile reads every archived partition on the trickle alone
    let (idle, trickled) = (pace.idle_bytes(), pace.trickled_bytes());
    let started = Instant::now();
    let scanned = walk_tablet_zero(&client, Some(WalkClass::Scan)).await;
    let took = started.elapsed();
    let trickled = pace.trickled_bytes() - trickled;
    assert_eq!(scanned.archived as usize, ids.len(), "{scanned:?}");
    assert_eq!(pace.idle_bytes(), idle, "a scan read in idle time beside a read in flight");
    // no faster than its trickle and the tenth of a second it holds, and slower than the burst
    let burst = shoal::server::scans::burst_of(TRICKLE);
    let most = burst as f64 + TRICKLE as f64 * took.as_secs_f64();
    assert!(trickled as f64 <= most, "trickled {trickled} bytes in {took:?}, past {most}");
    assert!(trickled > 4 * burst, "the scan trickled only {trickled} bytes");
    let least = (trickled - burst) as f64 / TRICKLE as f64;
    assert!(took.as_secs_f64() >= least, "trickled {trickled} bytes in {took:?}");
    assert!(scanned.paced_us > 0, "{scanned:?}");
    // let go, the get is answered with its row, and its read no longer counts
    faults::hold_partition_reads(table, false);
    let answered = get.await.expect("the get's task")?;
    assert_eq!(answered.access::<Row>()?.map(|rows| rows.len()), Some(1));
    assert_eq!(pace.foreground_reads(), 0);
    // in the device's idle time the same scan takes the ceiling's bytes alone, and no wait
    let (idle, trickled) = (pace.idle_bytes(), pace.trickled_bytes());
    let started = Instant::now();
    let rescanned = walk_tablet_zero(&client, Some(WalkClass::Scan)).await;
    assert_eq!(rescanned.partitions, ids.len());
    assert!(started.elapsed() < took / 4, "an idle scan took {:?}", started.elapsed());
    assert!(pace.idle_bytes() > idle);
    assert_eq!(pace.trickled_bytes(), trickled);
    // a read that fails is counted only until it fails: a lost device under the table's archives
    let archives = temp_dir.path().join("Row").join("archives");
    faults::arm(&archives, faults::Fault::Lost);
    let failed = client.send_one(RowGet::new(vec![ids[1]])).await;
    faults::clear(&archives);
    assert!(
        failed.as_ref().map_or(true, |answer| answer.error().is_some()),
        "a read of a lost device answered: {failed:?}"
    );
    assert_eq!(pace.foreground_reads(), 0, "a failed read stayed counted");
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
