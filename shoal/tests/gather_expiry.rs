//! A split query on a standalone node expires at the bundle's deadline instead of hanging
//!
//! Before [F41](../../docs/src/features/read-consistency.md) a `Gather` was removed only when
//! every share arrived, so a shard that never sent one held the client forever
//! ([Resolved #33](../../docs/src/appendix/resolved/gather-expiry.md)). This is the reproduction:
//! a two shard server holds every share it would send, and a get spanning both shards is answered
//! `Timeout` at the deadline. Run against the tree before the fix, with the hold in place and the
//! expiry absent, the get never returns and the test's own timeout is what ends it.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Errors;
use shoal::server::conf::Networking;
use shoal::server::replication::ReadVerb;
use shoal::shared::protocol::error::ErrorCode;
use shoal::tables::EphemeralUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::time::Duration;

mod utils;

use utils::TestError;

/// A row in an ephemeral table, since none of this is about storage
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ExpiryDb")]
pub struct Item {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// A payload
    #[shoal(update)]
    pub value: String,
}

/// The schema: one table, so a get over many keys is split across the two shards
#[db]
pub struct ExpiryDb {
    /// The table
    pub items: EphemeralUnsortedTable<Item>,
}

/// A split get whose shares are held is answered `Timeout` once, at the deadline, and the
/// gather is gone afterwards (Resolved #33, F41)
#[tokio::test(flavor = "multi_thread")]
async fn a_standalone_gather_expires_at_the_query_deadline() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // two shards, and a second's budget for a bundle
    let conf = utils::build_config(&temp_dir).networking(
        Networking::default()
            .port(0)
            .query_deadline(Duration::from_secs(1)),
    );
    let (client, pool) = utils::start_with_conf::<ExpiryDb>(conf).await?;
    // rows on enough keys that a get over all of them is split across both shards
    let keys: Vec<u64> = (1..=32).collect();
    for key in &keys {
        client
            .send_one(Item {
                key: *key,
                value: format!("item {key}"),
            })
            .await?;
    }
    // a get over every key comes back whole while nothing is held
    let whole = client.send_one(ItemGet::new(keys.clone())).await?;
    let rows = whole.access::<Item>()?.map(|rows| rows.len()).unwrap_or(0);
    assert_eq!(
        rows,
        keys.len(),
        "the get over every key did not find every row"
    );
    // hold every share on every shard for longer than the deadline, and longer than this
    // test is willing to wait for the get: an unexpired gather is a get that never returns
    for answer in pool.read_verb(
        None,
        ReadVerb::HoldShares {
            ms: 6000,
            dup: false,
        },
    )? {
        answer.map_err(|error| TestError::Io(std::io::Error::other(error)))?;
    }
    // the same get is now answered with a timeout, once, within its budget
    let started = std::time::Instant::now();
    let held = tokio::time::timeout(
        Duration::from_secs(4),
        client.send_one(ItemGet::new(keys.clone())),
    )
    .await;
    let elapsed = started.elapsed();
    let held = held.map_err(|_| {
        TestError::Io(std::io::Error::other(
            "the split get never returned: the gather did not expire",
        ))
    })?;
    match held {
        Err(Errors::Server { code, msg, .. }) => {
            assert_eq!(code, ErrorCode::Timeout, "{msg}");
            assert!(msg.contains("shares arrived"), "{msg}");
        }
        other => panic!("a held get was not answered Timeout: {other:?}"),
    }
    assert!(
        elapsed >= Duration::from_millis(900),
        "the timeout came before the deadline: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the timeout waited for the hold rather than the deadline: {elapsed:?}"
    );
    // the gather is gone and counted, on whichever shard coordinated the bundle
    let mut resident = 0u64;
    let mut timeouts = 0u64;
    for answer in pool.read_verb(None, ReadVerb::Gathers)? {
        let view = answer.map_err(|error| TestError::Io(std::io::Error::other(error)))?;
        resident += view["resident"].as_u64().unwrap_or(0);
        timeouts += view["stats"]["timeouts"].as_u64().unwrap_or(0);
    }
    assert_eq!(resident, 0, "a gather stayed resident after its expiry");
    assert_eq!(timeouts, 1, "the expiry was not counted once");
    // once the hold releases, the late shares are dropped and the next get is whole again
    tokio::time::sleep(Duration::from_secs(6)).await;
    let again = client.send_one(ItemGet::new(keys.clone())).await?;
    let rows = again.access::<Item>()?.map(|rows| rows.len()).unwrap_or(0);
    assert_eq!(
        rows,
        keys.len(),
        "the get after the release did not find every row"
    );
    let mut late = 0u64;
    for answer in pool.read_verb(None, ReadVerb::Gathers)? {
        let view = answer.map_err(|error| TestError::Io(std::io::Error::other(error)))?;
        late += view["stats"]["late_shares"].as_u64().unwrap_or(0);
    }
    assert!(late >= 1, "the released shares were not counted late");
    pool.exit()?;
    Ok(())
}
