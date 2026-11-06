//! Integration tests for what a client logs when it is dropped while its pool opens a connection
//!
//! A client's pool opens a connection over a socket's connect and its handshakes, and the client may
//! be dropped in the middle, which stops the proxy its connections are handed to. Since
//! [Resolved #303](../../docs/src/appendix/resolved/dropped-client-dials-nothing.md) a dropped
//! client's pool dials nothing new, but a connection already past that check went on to its hand-off,
//! met the stopped proxy, and `ShoalConnectionManager::connect_to`'s `err(Debug)` logged it as an
//! ERROR: in every bench run whose client went while its pool was filling
//! ([Resolved #364](../../docs/src/appendix/resolved/dropped-client-connects-quietly.md)).
//!
//! **This binary holds one test.** The counter and the subscriber that feeds it are process wide,
//! and a second test running beside this one would add its own events to the same number.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{PoolConfig, Shoal};
use shoal::tables::EphemeralSortedTable;
use shoal::{Routing, ShoalPool};
use shoal_derive::{db, ShoalSortedTable};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{Event, Level};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

mod utils;

use utils::TestError;

/// The crate a client's events are logged from
const CLIENT_TARGET: &str = "shoal_client";

/// Whether the layer counts yet: only what is logged once the client is dropped
static COUNTING: AtomicBool = AtomicBool::new(false);

/// How many events the client logged at ERROR once it was dropped
static ERRORS: AtomicUsize = AtomicUsize::new(0);

/// A tracing layer that counts the client's errors once counting is on
struct CountClientErrors;

impl<S: tracing::Subscriber> Layer<S> for CountClientErrors {
    /// Count this event if the client logged it at ERROR while counting is on
    ///
    /// # Arguments
    ///
    /// * `event` - The event being logged
    /// * `_ctx` - The context of the subscriber this layer is part of
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // only the client's own crate, and only once its client is gone
        let metadata = event.metadata();
        if !COUNTING.load(Ordering::SeqCst) || !metadata.target().starts_with(CLIENT_TARGET) {
            return;
        }
        if *metadata.level() == Level::ERROR {
            ERRORS.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// A row in the table this test queries
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "DroppedDb")]
pub struct DroppedRecord {
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

/// The schema this test runs against: an ephemeral table, since it is about a client's connections
#[db]
pub struct DroppedDb {
    /// The only table in this schema
    pub dropped_records: EphemeralSortedTable<DroppedRecord>,
}

/// Write one row through a client and read it back
///
/// # Arguments
///
/// * `client` - The client
/// * `key` - The partition key to write under
async fn round_trip(client: &Shoal<DroppedDbClient>, key: &str) -> Result<(), TestError> {
    // a row is its own insert query
    client
        .send_one(DroppedRecord {
            partition_key: key.to_owned(),
            sort_key: "sort".to_owned(),
            data: "data".to_owned(),
        })
        .await?;
    // and read back by its partition
    let response = client
        .send_one(DroppedRecordGet::new(vec![key.to_owned()]))
        .await?;
    assert!(response.access::<DroppedRecord>()?.is_some(), "a get that found nothing");
    Ok(())
}

/// A forwarder between a client and its server that holds each new connection before joining it
///
/// A connection held here has its socket connected and its handshake waiting, which is where a
/// client dropped meanwhile leaves it.
struct Forwarder {
    /// The address the client is given
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
        // forward every connection until the forwarder is dropped
        let acceptor = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                // a dial is counted the moment it arrives
                accepted_by.lock().unwrap().push(Instant::now());
                // and held before it reaches the server, so its handshake waits
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
            .store(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
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

/// A client dropped while its pool opens connections logs no ERROR for them
///
/// Item 364: every connection the client holds is cut, a query has its pool replace them, and the
/// replacements are held in the forwarder with their sockets connected and their handshakes
/// waiting while the client is dropped. Each then finishes its handshake and finds the proxy
/// stopped; that is no failure, and nothing may be logged at ERROR for it.
#[tokio::test]
async fn a_client_dropped_while_its_pool_connects_logs_no_error() -> Result<(), TestError> {
    // every event this process logs passes the counting layer
    tracing_subscriber::registry().with(CountClientErrors).init();
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let mut pool = ShoalPool::<DroppedDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    // the client reaches its server only through the forwarder
    let forwarder = Forwarder::start(&addr).await?;
    let client = Shoal::<DroppedDbClient>::builder()
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
    // every connection from here on is held a second before its handshake is answered
    forwarder.hold(Duration::from_secs(1));
    // the client's connections end, as they would if the server went away
    let cut = Instant::now();
    forwarder.cut();
    tokio::time::sleep(Duration::from_millis(200)).await;
    // a query has the pool find its connections broken and replace them toward its minimum
    let client = Arc::new(client);
    let query = {
        let client = client.clone();
        tokio::spawn(async move { round_trip(&client, "replaced").await })
    };
    // wait until replacements are dialled and held
    let deadline = Instant::now() + Duration::from_secs(10);
    while forwarder.accepted_after(cut) < 2 {
        assert!(Instant::now() < deadline, "the pool never replaced its broken connections");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // the client goes while they are held, the query that held it first
    query.abort();
    let _ = query.await;
    let client = Arc::try_unwrap(client)
        .ok()
        .expect("something still holds the client");
    COUNTING.store(true, Ordering::SeqCst);
    drop(client);
    // the held connections finish their handshakes and reach their hand-offs, well inside this
    tokio::time::sleep(Duration::from_secs(3)).await;
    let errors = ERRORS.load(Ordering::SeqCst);
    assert_eq!(
        errors, 0,
        "a client dropped while its pool connected logged {errors} ERRORs for connections it no longer needed"
    );
    Ok(())
}
