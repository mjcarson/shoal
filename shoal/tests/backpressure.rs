//! The admission bound on the shard mesh
//!
//! Every channel between a node's shards is unbounded, so a shard that fell behind grew its
//! queue until the process was killed and nothing throttled a client
//! ([Resolved #15](../../docs/src/appendix/resolved/shard-mesh-admission.md)). The bound is at
//! admission: a client's query bound for a shard whose queue already holds
//! `networking.max_queued_queries` messages is answered `Shedding` by the shard that accepted
//! it, at once, and never enqueued. This is the test that holds one shard and watches the other
//! turn queries away.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{Errors, PoolConfig, SendOptions, Shoal};
use shoal::shared::protocol::error::ErrorCode;
use shoal::tables::EphemeralUnsortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalUnsortedTable};
use std::time::{Duration, Instant};

mod utils;

use utils::TestError;

/// A row in the table this test queries
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

/// The schema this test runs against
///
/// An ephemeral table: the bound is about the mesh, not about storage.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub test_records: EphemeralUnsortedTable<TestRecord>,
}

/// A query bound for a shard that has fallen behind is shed at once, and answered by name
///
/// Two shards, a bound of eight, shard one held for two seconds. Two hundred gets over distinct
/// keys through a pool of twenty connections: the shares that reach shard zero's coordinator
/// bound for shard one are shed once its queue holds eight - `Shedding`, inside a second, never
/// two seconds on a queue nothing drains - and the rest are answered, some after the hold. Then
/// the held shard answers again, and the transport view counts what was shed. Against the tree
/// before the bound nothing is shed and every query to shard one waits the hold out.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_for_a_shard_that_fell_behind_is_shed() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // two shards, and a bound low enough that a held shard reaches it inside the hold
    let conf = utils::build_config(&temp_dir).networking(
        shoal::server::conf::Networking::default()
            .port(0)
            .max_queued_queries(8),
    );
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    // rows on every key, so a get has something to find once it runs
    let client = std::sync::Arc::new(
        Shoal::<TestDbClient>::builder()
            .endpoints(vec![addr.to_string()])
            .pool(PoolConfig {
                min_idle: 20,
                max_size: 20,
                ..PoolConfig::default()
            })
            .build()
            .await?,
    );
    for key in 0..200u64 {
        client
            .send_one(TestRecord {
                key,
                data: format!("row {key}"),
            })
            .await?;
    }
    // hold shard one where it stands: its queue grows behind it
    pool.hold_shard(1, 2_000)?;
    let held = Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // two hundred gets at once, each on its own connection when one is free
    let mut tasks = tokio::task::JoinSet::new();
    for key in 0..200u64 {
        let client = client.clone();
        tasks.spawn(async move {
            let sent = Instant::now();
            let outcome = client.send_one(TestRecordGet::new(vec![key])).await;
            (key, sent.elapsed(), outcome)
        });
    }
    let mut shed = 0u32;
    let mut answered = 0u32;
    let mut fastest_shed = Duration::MAX;
    while let Some(done) = tasks.join_next().await {
        let (key, took, outcome) = done.expect("a get task panicked");
        match outcome {
            Ok(_) => answered += 1,
            Err(Errors::Server { code, .. }) if code == ErrorCode::Shedding => {
                shed += 1;
                fastest_shed = fastest_shed.min(took);
            }
            Err(error) => panic!("get {key} failed for another reason: {error:?}"),
        }
    }
    eprintln!(
        "{shed} gets shed, the fastest in {fastest_shed:?}; {answered} answered; {:?} since the hold",
        held.elapsed()
    );
    // the bound turned queries away - and at once, for a query that had a connection to be
    // turned away on; a task's elapsed time includes its wait for one of the twenty, half of
    // which sit on the held shard's coordinator for the whole hold
    assert!(shed > 0, "no query was shed while shard one was held");
    assert!(
        fastest_shed < Duration::from_millis(500),
        "the fastest refusal took {fastest_shed:?}"
    );
    assert!(answered > 0, "every query was shed");
    // the held shard is back, and answers
    if let Some(left) = Duration::from_millis(2_500).checked_sub(held.elapsed()) {
        tokio::time::sleep(left).await;
    }
    let found = client.send_one(TestRecordGet::new(vec![7])).await?;
    assert!(
        found
            .access::<TestRecord>()?
            .is_some_and(|rows| rows.len() == 1),
        "the held shard did not answer once released"
    );
    // a shed query is safe to ask again, and the client does when told to
    let retried = client
        .send_one_with(
            TestRecordGet::new(vec![8]),
            &SendOptions::new().retry(Duration::from_secs(5)),
        )
        .await?;
    assert!(retried.access::<TestRecord>()?.is_some());
    // and the transport view says how many were turned away
    let views = pool.transport()?;
    let counted: u64 = views.iter().map(|view| view.shed).sum();
    assert_eq!(
        counted,
        u64::from(shed),
        "the transport view counts the shed queries"
    );
    pool.exit()?;
    Ok(())
}
