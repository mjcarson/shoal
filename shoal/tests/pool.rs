//! Integration tests for the connection pool and the builder that configures it
//!
//! Before this, everything about the pool was a literal: ten idle connections, fifty at most, and
//! one endpoint taken from the first answer a resolver gave. None of it could be set and none of
//! it was reachable from a test, which is why
//! [Test Coverage](../../docs/src/appendix/test-coverage.md) listed "concurrency and the
//! connection pool" as a gap with nothing in it at all.
//!
//! These tests are about the pool rather than about queries. The one query each of them sends is
//! there to prove the client that was built actually works.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{Errors, PoolConfig, Shoal};
use shoal::shared::traits::RkyvSupport;
use shoal::tables::EphemeralSortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable};
use tokio::net::TcpListener;

mod utils;

use utils::TestError;

/// A row in the table these tests query
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key
    #[shoal(partition)]
    pub partition_key: String,
    /// The sort key
    #[shoal(sort)]
    pub sort_key: String,
    /// The payload
    #[shoal(update)]
    pub data: String,
}

/// The schema these tests run against
///
/// An ephemeral table, because none of these tests are about storage — they are about how a client
/// reaches a server at all.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub test_records: EphemeralSortedTable<TestRecord>,
}

/// Find a port that nothing is listening on
///
/// Bound and immediately released, so a connection to it is refused rather than hanging. That is
/// what makes it stand in for a server that has gone away: a client trying it gets an answer, and
/// the answer is no.
async fn dead_port() -> Result<u16, TestError> {
    // take a port from the kernel, ask which one it was, and give it straight back
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Start a server and hand back the address to reach it on
///
/// The shared helper builds a client of its own, and every test here wants to build its own
/// differently, so this stops one step earlier.
///
/// # Arguments
///
/// * `temp_dir` - The directory this server should store anything in
async fn start_server(temp_dir: &tempfile::TempDir) -> Result<(ShoalPool<TestDb>, String), TestError>
{
    // build a config on a port nothing else in this binary is using
    let conf = utils::build_config(temp_dir);
    // start the server and wait until its shards are answering
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    Ok((pool, addr))
}

/// Write one row through a client and read it back
///
/// Every test here ends with this, because a client that was built is not yet a client that works
/// and the difference is the whole point of building one.
///
/// # Arguments
///
/// * `client` - The client to prove
/// * `key` - The partition key to write under, so two clients in one test do not collide
async fn round_trip(client: &Shoal<TestDbClient>, key: &str) -> Result<(), TestError> {
    // write a row
    //
    // a row is its own insert query, so this is the whole of writing one
    client
        .send_one(TestRecord {
            partition_key: key.to_owned(),
            sort_key: "sort".to_owned(),
            data: "data".to_owned(),
        })
        .await?;
    // read it back and check it is the row we wrote
    let response = client
        .send_one(TestRecordGet::new(vec![key.to_owned()]))
        .await?;
    let rows = response
        .access::<TestRecord>()?
        .expect("a get that found nothing");
    let row = TestRecord::deserialize(rows.first().expect("a get that returned no rows"))
        .expect("failed to read a row back");
    assert_eq!(row.data, "data");
    Ok(())
}

/// A client the builder made is a client that works
///
/// The three constructors now route through the same builder, so this is what says the route did
/// not lose anything on the way.
#[tokio::test]
async fn a_client_the_builder_built_answers_a_query() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start_server(&temp_dir).await?;
    // build a client the long way round
    let client = Shoal::<TestDbClient>::builder().endpoint(&addr).build().await?;
    round_trip(&client, "builder").await
}

/// An endpoint that refuses a connection is tried past rather than being the end of the attempt
///
/// This is the whole of what an endpoint list is for. Before it, a client resolved one address and
/// kept it, so the first server being down was the same as every server being down.
#[tokio::test]
async fn an_endpoint_that_is_down_is_tried_past() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start_server(&temp_dir).await?;
    // put a port nothing is listening on ahead of the real server
    let dead = format!("127.0.0.1:{}", dead_port().await?);
    let client = Shoal::<TestDbClient>::builder()
        .endpoints([dead, addr])
        .build()
        .await?;
    round_trip(&client, "failover").await
}

/// A client whose every endpoint is down fails, rather than reporting one that worked
///
/// The other half of the test above: trying past a dead endpoint must not become trying past a
/// dead *client*.
#[tokio::test]
async fn a_client_with_no_live_endpoint_fails() -> Result<(), TestError> {
    // two ports nothing is listening on, and nothing else
    let first = format!("127.0.0.1:{}", dead_port().await?);
    let second = format!("127.0.0.1:{}", dead_port().await?);
    let built = Shoal::<TestDbClient>::builder()
        .endpoints([first, second])
        .build()
        .await;
    // this is the pool giving up rather than a configuration being refused
    assert!(
        matches!(built, Err(Errors::Handshake(_))),
        "a client with no live endpoint was built anyway"
    );
    Ok(())
}

/// The pool a caller asked for is the pool that gets built
///
/// A client held to two connections still answers, which is what says the numbers reached `bb8`
/// rather than being taken and dropped.
#[tokio::test]
async fn a_pool_sized_by_the_caller_still_answers() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start_server(&temp_dir).await?;
    // ask for a pool far smaller than the default ten and fifty
    let client = Shoal::<TestDbClient>::builder()
        .endpoint(&addr)
        .pool(PoolConfig {
            min_idle: 1,
            max_size: 2,
            ..PoolConfig::default()
        })
        .build()
        .await?;
    // send more queries at once than the pool has connections, so they have to share
    let sends = (0..8).map(|i| {
        let client = &client;
        async move {
            client
                .send_one(TestRecord {
                    partition_key: format!("sized-{i}"),
                    sort_key: "sort".to_owned(),
                    data: "data".to_owned(),
                })
                .await
        }
    });
    for outcome in futures::future::join_all(sends).await {
        outcome?;
    }
    round_trip(&client, "sized").await
}

/// A pool that can never be satisfied is refused before a socket is opened
///
/// There is no server in this test on purpose. A configuration error has to be caught by looking
/// at the configuration, not by failing to connect with it.
#[tokio::test]
async fn a_pool_that_cannot_be_satisfied_never_opens_a_socket() -> Result<(), TestError> {
    let built = Shoal::<TestDbClient>::builder()
        .endpoint("127.0.0.1:1")
        .pool(PoolConfig {
            min_idle: 60,
            max_size: 50,
            ..PoolConfig::default()
        })
        .build()
        .await;
    assert!(
        matches!(built, Err(Errors::Config(_))),
        "a pool that cannot be satisfied was not refused"
    );
    Ok(())
}

/// A builder with nowhere to go is refused, rather than resolving nothing and connecting to it
#[tokio::test]
async fn a_client_with_no_endpoint_is_refused() -> Result<(), TestError> {
    let built = Shoal::<TestDbClient>::builder().build().await;
    assert!(
        matches!(built, Err(Errors::Config(_))),
        "a client with no endpoint was built anyway"
    );
    Ok(())
}

/// A shard that cannot bind is reported by the pool, not swallowed
///
/// Item 58: `ShoalPool::start` spawns its shard threads and returns before any of them has bound,
/// so a shard that fails on its first line - a held port, a missing kernel module - was
/// indistinguishable from one still starting. The port is held by a plain listener without
/// `SO_REUSEPORT`, which is exactly what makes every shard's reuse-port bind fail with
/// `EADDRINUSE`.
#[tokio::test]
async fn a_shard_that_cannot_bind_is_reported() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // hold a port with a socket that will refuse to share it
    let holder = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = holder.local_addr()?.port();
    // point a server at that port
    let conf = utils::build_config(&temp_dir)
        .networking(shoal::server::conf::Networking::default().port(port));
    // the pool must say so rather than report a server that will never answer
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let reported = pool.ready(std::time::Duration::from_secs(10));
    drop(holder);
    match reported {
        Err(error) => {
            let text = format!("{error:?}");
            assert!(text.contains("ShardFailed"), "the error names no shard: {text}");
            assert!(text.contains("in use"), "the error does not say the port was held: {text}");
        }
        Ok(addr) => panic!("start reported {addr} ready although no shard could bind port {port}"),
    }
    Ok(())
}

/// A port of zero resolves to one real port that every shard binds
///
/// Every shard binds with `SO_REUSEPORT`, so a zero handed to each of them would give each its
/// own ephemeral port and a client only one of them. The pool reserves a port first, tells the
/// shards that number, and reports it from `ready`; a client on that address is served by any
/// shard, which `round_trip` proves.
#[tokio::test]
async fn a_port_of_zero_resolves_to_one_every_shard_binds() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // ask for any port at all
    let conf = utils::build_config(&temp_dir)
        .networking(shoal::server::conf::Networking::default().port(0));
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    // the pool knows the port before a shard has bound it
    let promised = pool.bound_addr();
    assert_ne!(promised.port(), 0, "start left the port unresolved");
    // and readiness reports the same one
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    assert_eq!(addr, promised, "ready reported a different address than start promised");
    // nothing has died
    assert_eq!(pool.failure(), None);
    // a client on that address is served
    let client = Shoal::<TestDbClient>::new(&addr.to_string()).await?;
    round_trip(&client, "port-zero").await?;
    pool.exit()?;
    Ok(())
}
