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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{Errors, PoolConfig, Shoal};
use shoal::shared::traits::RkyvSupport;
use shoal::tables::EphemeralSortedTable;
use shoal::{Routing, ShoalPool};
use shoal_derive::{db, ShoalSortedTable};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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
async fn start_server(
    temp_dir: &tempfile::TempDir,
) -> Result<(ShoalPool<TestDb>, String), TestError> {
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
    let client = Shoal::<TestDbClient>::builder()
        .endpoint(&addr)
        .build()
        .await?;
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
            assert!(
                text.contains("ShardFailed"),
                "the error names no shard: {text}"
            );
            assert!(
                text.contains("in use"),
                "the error does not say the port was held: {text}"
            );
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
    assert_eq!(
        addr, promised,
        "ready reported a different address than start promised"
    );
    // nothing has died
    assert_eq!(pool.failure(), None);
    // a client on that address is served
    let client = Shoal::<TestDbClient>::new(&addr.to_string()).await?;
    round_trip(&client, "port-zero").await?;
    pool.exit()?;
    Ok(())
}

/// The transport view names every shard, each answering for its own links
///
/// `ShoalPool::transport` said it gathered every shard's links and asked shard zero alone, so a
/// node of four shards was reported as whatever shard zero happened to dial
/// ([Resolved #95](../../docs/src/appendix/resolved/transport-view-every-shard.md)). A standalone
/// node holds no links, which is the point: what this asserts is that every shard answered, in
/// shard order, and that a node is never reported as though one shard were all of it.
#[tokio::test]
async fn the_transport_view_names_every_shard() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // two shards, which is one more than the pool used to ask
    let conf = utils::build_config(&temp_dir);
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    pool.ready(utils::READY_TIMEOUT)?;
    // ask for the transport view
    let views = pool.transport()?;
    // one view per shard, in shard order, each naming itself
    let shards: Vec<usize> = views.iter().map(|view| view.shard).collect();
    assert_eq!(
        shards,
        vec![0, 1],
        "the transport view did not come from every shard"
    );
    // and a standalone node holds no links on any of them
    assert!(
        views.iter().all(|view| view.links.is_empty()),
        "a standalone node reported a peer link: {views:?}"
    );
    pool.exit()?;
    Ok(())
}

/// A forwarder between a client and its server that counts every connection the client opens
///
/// Nothing in a server counts the connections it accepts, and item 303 is about the connections a
/// client opens after it is gone. Each connection is counted as it is accepted, held for the
/// forwarder's current hold, and only then joined to the server, so a dial can be caught in flight;
/// `cut` ends every connection forwarded so far, as a server that went away would.
struct Forwarder {
    /// The address a client dials
    addr: String,
    /// When each connection was accepted
    accepted: Arc<Mutex<Vec<Instant>>>,
    /// How long a new connection is held before it is joined to the server, in milliseconds
    hold_ms: Arc<AtomicU64>,
    /// Every connection forwarded so far, which `cut` ends
    links: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The loop accepting connections
    acceptor: JoinHandle<()>,
}

impl Forwarder {
    /// Start a forwarder to a server
    ///
    /// # Arguments
    ///
    /// * `server` - The address of the server every connection is joined to
    async fn start(server: &str) -> Result<Self, TestError> {
        // listen on a port of our own
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        // the accept times, the hold and the links, shared with the accept loop
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let hold_ms = Arc::new(AtomicU64::new(0));
        let links = Arc::new(Mutex::new(Vec::new()));
        let (accepted_by, hold_by, links_by) = (accepted.clone(), hold_ms.clone(), links.clone());
        let server = server.to_owned();
        // count and forward every connection until the forwarder is dropped
        let acceptor = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                // a dial is counted the moment it arrives
                accepted_by.lock().unwrap().push(Instant::now());
                // and held before it reaches the server, so it can be caught in flight
                let hold = Duration::from_millis(hold_by.load(Ordering::Relaxed));
                let server = server.clone();
                let link = tokio::spawn(async move {
                    tokio::time::sleep(hold).await;
                    // joined to the server until either end closes
                    if let Ok(mut outbound) = TcpStream::connect(&server).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                links_by.lock().unwrap().push(link);
            }
        });
        Ok(Forwarder {
            addr,
            accepted,
            hold_ms,
            links,
            acceptor,
        })
    }

    /// Hold every connection accepted from now on before it is joined to the server
    ///
    /// # Arguments
    ///
    /// * `hold` - How long each is held
    fn hold(&self, hold: Duration) {
        self.hold_ms
            .store(hold.as_millis() as u64, Ordering::Relaxed);
    }

    /// End every connection forwarded so far, both of its sockets closed
    fn cut(&self) {
        // a link's task owns both of its sockets, so ending it closes them
        for link in self.links.lock().unwrap().drain(..) {
            link.abort();
        }
    }

    /// How many connections were accepted after an instant
    ///
    /// # Arguments
    ///
    /// * `instant` - The instant to count from
    fn accepted_after(&self, instant: Instant) -> usize {
        self.accepted
            .lock()
            .unwrap()
            .iter()
            .filter(|at| **at > instant)
            .count()
    }
}

impl Drop for Forwarder {
    /// Stop accepting, and end every connection still forwarded
    fn drop(&mut self) {
        self.acceptor.abort();
        self.cut();
    }
}

/// A dropped client's pool opens no connection
///
/// Item 303: dropping a `Shoal` stopped the proxy its connections are handed to, but `bb8` went on
/// opening connections for the pool it left behind, toward `min_idle` or in place of one returned
/// broken. Each dialled, shook hands and subscribed, failed at the hand-off to the stopped proxy
/// with an ERROR line, and was tried again after a doubling delay. Here every connection the client
/// holds is cut, a query has its pool replace them, and the client is dropped while the
/// replacements are held in flight; nothing may dial after it
/// ([Resolved #303](../../docs/src/appendix/resolved/dropped-client-dials-nothing.md)).
#[tokio::test]
async fn a_dropped_client_dials_nothing() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start_server(&temp_dir).await?;
    // the client reaches its server only through a forwarder that counts its dials
    let forwarder = Forwarder::start(&addr).await?;
    let client = Shoal::<TestDbClient>::builder()
        .endpoint(&forwarder.addr)
        .routing(Routing::Endpoints)
        .pool(PoolConfig {
            min_idle: 4,
            max_size: 8,
            ..PoolConfig::default()
        })
        .build()
        .await?;
    round_trip(&client, "dropped").await?;
    // every connection from here on is held a second before it reaches the server, so the pool's
    // replacements are still in flight when the client goes
    forwarder.hold(Duration::from_secs(1));
    // the client's connections end, as they would if the server went away
    let cut = Instant::now();
    forwarder.cut();
    // its readers see their connections end
    tokio::time::sleep(Duration::from_millis(200)).await;
    // a query has the pool find its connections broken and replace them toward its minimum
    let client = Arc::new(client);
    let query = {
        let client = client.clone();
        tokio::spawn(async move { round_trip(&client, "replaced").await })
    };
    // wait until replacements are dialled
    let deadline = Instant::now() + Duration::from_secs(10);
    while forwarder.accepted_after(cut) < 2 {
        assert!(
            Instant::now() < deadline,
            "the pool never replaced its broken connections"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // the client goes while they are in flight, the query that held it first
    query.abort();
    let _ = query.await;
    let client = Arc::try_unwrap(client)
        .ok()
        .expect("something still holds the client");
    drop(client);
    let dropped = Instant::now();
    // and nothing it left behind dials again, through bb8's retries of every attempt in flight
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = forwarder.accepted_after(dropped);
    assert_eq!(
        after, 0,
        "a dropped client's pool dialled {after} times in the three seconds after it was dropped"
    );
    Ok(())
}
