//! Objects in a bucket, end to end through a node ([F80](../../docs/src/features/object-wire.md))
//!
//! M13's acceptance rows that a standalone node can show: the object frames and their capability
//! ([S12](../../docs/src/object-storage/wire-and-client.md#acceptance-tests)), path identity and
//! inline objects ([S3](../../docs/src/object-storage/objects.md#acceptance-tests)), and a bucket
//! bound to nothing refused at start ([S4](../../docs/src/object-storage/pools-and-devices.md#acceptance-tests)).
//! The committed half of S4's policy rows, a joiner's copy ignored, is the cluster fixture's.

// a database struct and a bucket's marker are named as types and never built
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shoal::client::{Deadlines, Errors, PoolConfig, PutOptions, Routing, Shoal, StreamConfig};
use shoal::server::conf::cluster::Cluster;
use shoal::server::ShoalPool;
use shoal::shared::checksum;
use shoal::shared::objects::{object_id, Attributes, BucketRows};
use shoal::shared::pools::{PoolSpec, Redundancy};
use shoal::shared::protocol::auth::AuthMechanisms;
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::protocol::object::{ObjectOp, ObjectOpKind, ObjectOutcome, CLIENT_CAP_OBJECTS};
use shoal::shared::protocol::stream::{self, CLIENT_CAPS, CLIENT_CAP_STREAMS};
use shoal::shared::protocol::{self, handshake, Flags, Header, MessageType};
use shoal::shared::traits::QuerySupport;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

mod utils;

use utils::TestError;

/// A schema of one table and one bucket
mod media {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::{Bucket, PersistentUnsortedTable};
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A movie, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Media")]
    pub struct Movie {
        /// The movie's id
        #[shoal(partition)]
        pub id: u64,
        /// Its title
        #[shoal(update)]
        pub title: String,
    }

    /// Names the bucket our posters are kept in
    pub struct Posters;

    /// The database our media service is built on
    #[db]
    pub struct Media {
        /// The movies we know about
        pub movies: PersistentUnsortedTable<Movie, FileSystem>,
        /// The posters for those movies
        pub posters: Bucket<Posters>,
    }
}

use media::{Media, MediaClient, Posters};

/// Bytes that differ at every offset, so a byte read from the wrong place shows
///
/// # Arguments
///
/// * `len` - How many
/// * `seed` - What makes them this object's
fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|at| (at as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

/// A node of the media schema, a client of it, and its address
struct Node {
    /// The node's storage, removed when the test ends
    _dir: tempfile::TempDir,
    /// The node
    pool: ShoalPool<Media>,
    /// A client of it
    client: Shoal<MediaClient>,
    /// Where it listens
    addr: String,
}

/// Start a node of the media schema with Posters bound to a local pool
async fn node() -> Result<Node, TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]);
    let mut pool = ShoalPool::<Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<MediaClient>::new(&addr).await?;
    Ok(Node {
        _dir: temp_dir,
        pool,
        client,
        addr,
    })
}

/// The outcome an object error names, or none for another error
///
/// # Arguments
///
/// * `error` - The error
fn outcome(error: &Errors) -> Option<ObjectOutcome> {
    match error {
        Errors::Object { outcome, .. } => Some(*outcome),
        _ => None,
    }
}

/// Every connection this process holds that a node accepted on a port, with each one's minimum
/// retransmission timeout in microseconds as the kernel holds it
///
/// The node runs in this process, so its sockets are this process's descriptors: each is matched
/// to its local port through `/proc/self/net/tcp` by its inode, and asked with `getsockopt`.
///
/// # Arguments
///
/// * `port` - The port the node accepted them on
fn accepted_rto_mins(port: u16) -> Vec<i32> {
    use std::os::fd::RawFd;
    // the established sockets on the port, by inode
    let table = std::fs::read_to_string("/proc/self/net/tcp").expect("the process's sockets");
    let mut inodes = std::collections::HashSet::new();
    for line in table.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let local_port = fields
            .get(1)
            .and_then(|local| local.rsplit(':').next())
            .and_then(|hex| u16::from_str_radix(hex, 16).ok());
        if local_port == Some(port) && fields.get(3) == Some(&"01") {
            inodes.insert(fields.get(9).copied().unwrap_or_default().to_string());
        }
    }
    // each descriptor that is one of them, asked for its minimum
    let mut mins = Vec::new();
    for entry in std::fs::read_dir("/proc/self/fd").expect("the process's descriptors").flatten() {
        let Ok(target) = std::fs::read_link(entry.path()) else {
            continue;
        };
        let target = target.to_string_lossy().to_string();
        let Some(inode) = target.strip_prefix("socket:[").and_then(|rest| rest.strip_suffix(']')) else {
            continue;
        };
        if !inodes.contains(inode) {
            continue;
        }
        let Some(fd) = entry.file_name().to_str().and_then(|name| name.parse::<RawFd>().ok()) else {
            continue;
        };
        let mut micros: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: the descriptor is one of this process's open sockets, and the out pointers are
        // valid for their sizes; one closed since it was listed answers an error, which is skipped
        let rc = unsafe {
            libc::getsockopt(fd, libc::IPPROTO_TCP, 45, std::ptr::from_mut(&mut micros).cast(), &raw mut len)
        };
        if rc == 0 {
            mins.push(micros);
        }
    }
    mins
}

/// One option of every established socket of this process on a port, its own end on the port or
/// the other's
///
/// The node runs in this process, so both ends of a client's connection are this process's
/// descriptors, matched to the port through `/proc/self/net/tcp` by inode and asked with
/// `getsockopt`.
///
/// # Arguments
///
/// * `port` - The port
/// * `remote` - Whether the port is the far end's, as a client's connection to the node has it
/// * `level` - The option's level
/// * `name` - The option
fn socket_options_on(
    port: u16,
    remote: bool,
    level: libc::c_int,
    name: libc::c_int,
) -> Vec<libc::c_int> {
    // each descriptor on the port, asked for the option
    let mut values = Vec::new();
    for fd in descriptors_on(port, remote) {
        let mut value: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: the descriptor is one of this process's open sockets and the out pointers are
        // valid for their sizes; one closed since it was listed answers an error, which is skipped
        let rc = unsafe {
            libc::getsockopt(fd, level, name, std::ptr::from_mut(&mut value).cast(), &raw mut len)
        };
        if rc == 0 {
            values.push(value);
        }
    }
    values
}

/// The congestion control of every established socket of this process on a port, its own end on
/// the port or the other's (F112)
///
/// # Arguments
///
/// * `port` - The port
/// * `remote` - Whether the port is the far end's, as a client's connection to the node has it
fn congestion_controls_on(port: u16, remote: bool) -> Vec<String> {
    // each descriptor on the port, asked for its congestion control; one closed since it was
    // listed answers an error, which is skipped
    descriptors_on(port, remote)
        .into_iter()
        .filter_map(|fd| shoal::shared::sockets::congestion_of(fd).ok())
        .collect()
}

/// Every descriptor of this process that is an established socket on a port, its own end on the
/// port or the other's
///
/// The node runs in this process, so both ends of a client's connection are this process's
/// descriptors, matched to the port through `/proc/self/net/tcp` by inode.
///
/// # Arguments
///
/// * `port` - The port
/// * `remote` - Whether the port is the far end's, as a client's connection to the node has it
fn descriptors_on(port: u16, remote: bool) -> Vec<std::os::fd::RawFd> {
    use std::os::fd::RawFd;
    // the established sockets with the port at the end asked for, by inode
    let table = std::fs::read_to_string("/proc/self/net/tcp").expect("the process's sockets");
    let mut inodes = std::collections::HashSet::new();
    for line in table.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let end = fields
            .get(if remote { 2 } else { 1 })
            .and_then(|end| end.rsplit(':').next())
            .and_then(|hex| u16::from_str_radix(hex, 16).ok());
        if end == Some(port) && fields.get(3) == Some(&"01") {
            inodes.insert(fields.get(9).copied().unwrap_or_default().to_string());
        }
    }
    // each descriptor that is one of them
    let mut descriptors = Vec::new();
    for entry in std::fs::read_dir("/proc/self/fd").expect("the process's descriptors").flatten() {
        let Ok(target) = std::fs::read_link(entry.path()) else {
            continue;
        };
        let target = target.to_string_lossy().to_string();
        let Some(inode) = target
            .strip_prefix("socket:[")
            .and_then(|rest| rest.strip_suffix(']'))
        else {
            continue;
        };
        if !inodes.contains(inode) {
            continue;
        }
        if let Some(fd) = entry.file_name().to_str().and_then(|name| name.parse::<RawFd>().ok()) {
            descriptors.push(fd);
        }
    }
    descriptors
}

/// Both ends of a client's connection probe the other once it has carried nothing for a minute, so
/// a node lets go of a client that vanished and a client notices a node that did
/// ([item 310](../../docs/src/appendix/resolved/peer-connections-keepalive.md))
#[tokio::test(flavor = "multi_thread")]
async fn both_ends_of_a_clients_connection_are_kept_alive() -> Result<(), TestError> {
    let Node {
        _dir,
        pool,
        client,
        addr,
    } = node().await?;
    client
        .bucket::<Posters>()
        .put("alive/one", &bytes(64, 1))
        .await?;
    let port: u16 = addr.rsplit(':').next().and_then(|port| port.parse().ok()).expect("a port");
    for (end, remote) in [("the node's", false), ("the client's", true)] {
        let kept = socket_options_on(port, remote, libc::SOL_SOCKET, libc::SO_KEEPALIVE);
        assert!(!kept.is_empty(), "{end} end of no connection was found");
        assert!(kept.iter().all(|kept| *kept == 1), "{end} end of a client's connection: {kept:?}");
        let idle = socket_options_on(port, remote, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE);
        assert!(idle.iter().all(|idle| *idle == 60), "{end} end's idle before a probe: {idle:?}");
    }
    drop(client);
    pool.exit()?;
    Ok(())
}

/// A client's connection holds the node's minimum RTO until it carries an object operation, and the
/// kernel's after (item 304). A table's answer is one segment, whose loss the kernel probes only after
/// the minimum; an object's is bulk, whose connection keeps its window across an idle only on the
/// kernel's. The node runs in this process, so its accepted sockets are read directly
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_holds_the_minimum_rto_until_it_carries_an_object() -> Result<(), TestError> {
    let Node {
        _dir, pool, client, addr,
    } = node().await?;
    let port: u16 = addr.rsplit(':').next().and_then(|port| port.parse().ok()).expect("a port");
    // what a connection nothing set holds, 200 ms unless the host says otherwise
    let kernels = std::fs::read_to_string("/proc/sys/net/ipv4/tcp_rto_min_us")
        .ok()
        .and_then(|text| text.trim().parse::<i32>().ok())
        .unwrap_or(200_000);
    // the client's connections, before any object: every one at the node's minimum
    let before = accepted_rto_mins(port);
    assert!(!before.is_empty(), "the node accepted no connection");
    assert!(before.iter().all(|micros| *micros == 5_000), "before any object: {before:?}");
    // an object put and read back, on a connection set apart for objects
    let posters = client.bucket::<Posters>();
    let body = bytes(10_000, 7);
    posters.put("rto.png", &body).await?;
    assert_eq!(posters.get("rto.png").await?.1, body);
    // the connection that carried it at the kernel's, and those that carried none at the minimum
    let after = accepted_rto_mins(port);
    assert!(after.iter().any(|micros| *micros == kernels), "none at the kernel's: {after:?}");
    assert!(after.iter().any(|micros| *micros == 5_000), "none at the minimum: {after:?}");
    drop(client);
    pool.exit()?;
    Ok(())
}

/// Both ends of a client's connection run under the congestion control named for objects once it
/// carries an object operation, and a connection that carried none keeps the host's (F112). The
/// test names reno, which every kernel has and lets any process take, where a deployment's BBR
/// needs its host to load and allow it; a name no kernel gives is refused before a socket opens.
/// The node runs in this process, so both ends are read directly
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_takes_its_congestion_control_once_it_carries_an_object() -> Result<(), TestError> {
    // what a connection nothing set runs under, which the test cannot tell from reno if it is reno
    let hosts = std::fs::read_to_string("/proc/sys/net/ipv4/tcp_congestion_control")
        .map(|text| text.trim().to_string())
        .unwrap_or_default();
    if hosts == "reno" {
        eprintln!("skipped: this host's own congestion control is reno, the one the test names");
        return Ok(());
    }
    // a node whose client connections take reno once they carry an object
    let temp_dir = utils::test_dir();
    let mut conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]);
    conf.networking.object_congestion = Some("reno".to_string());
    let mut pool = ShoalPool::<Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let port: u16 = addr.rsplit(':').next().and_then(|port| port.parse().ok()).expect("a port");
    // a client naming a word no kernel gives is refused before it connects
    match Shoal::<MediaClient>::builder()
        .endpoint(addr.as_str())
        .object_congestion(Some("not a name"))
        .build()
        .await
    {
        Err(Errors::Config(msg)) => assert!(msg.contains("congestion"), "{msg}"),
        Err(other) => panic!("refused otherwise: {other:?}"),
        Ok(_) => panic!("a client named a congestion control no kernel gives"),
    }
    // and one naming reno, whose connections are the host's until one carries an object
    let client = Shoal::<MediaClient>::builder()
        .endpoint(addr.as_str())
        .object_congestion(Some("reno"))
        .build()
        .await?;
    for (end, remote) in [("the node's", false), ("the client's", true)] {
        let before = congestion_controls_on(port, remote);
        assert!(!before.is_empty(), "{end} end of no connection was found");
        assert!(before.iter().all(|name| *name == hosts), "{end} end before any object: {before:?}");
    }
    // an object put and read back, on a connection set apart for objects
    let posters = client.bucket::<Posters>();
    let body = bytes(10_000, 9);
    posters.put("congestion.png", &body).await?;
    assert_eq!(posters.get("congestion.png").await?.1, body);
    // the connection that carried it runs under reno at both ends, and those that carried none the
    // host's
    for (end, remote) in [("the node's", false), ("the client's", true)] {
        let after = congestion_controls_on(port, remote);
        assert!(after.iter().any(|name| name == "reno"), "{end} end after an object: {after:?}");
        assert!(after.iter().any(|name| *name == hosts), "{end} end kept none the host's: {after:?}");
    }
    drop(client);
    pool.exit()?;
    Ok(())
}

/// An object is put, read whole and in ranges, stated, replaced and deleted
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_object_round_trips() -> Result<(), TestError> {
    let Node {
        _dir, pool, client, ..
    } = node().await?;
    let posters = client.bucket::<Posters>();
    // a put, and a read of all of it
    let first = bytes(10_000, 1);
    let put = posters.put("alien/one-sheet.png", &first).await?;
    assert_eq!(put.size, 10_000);
    assert_eq!(put.content_version, 1);
    let (info, got) = posters.get("alien/one-sheet.png").await?;
    assert_eq!(got, first);
    assert_eq!(info.object, put.object);
    // a range inside it, and one that runs past its end
    let (_, part) = posters
        .read_range("alien/one-sheet.png", 4096, 100, None)
        .await?;
    assert_eq!(part, first[4096..4196]);
    let (_, tail) = posters
        .read_range("alien/one-sheet.png", 9_990, 100, None)
        .await?;
    assert_eq!(tail, first[9_990..]);
    let (_, past) = posters
        .read_range("alien/one-sheet.png", 20_000, 100, None)
        .await?;
    assert!(past.is_empty());
    // a stat with what the put said about the object
    let attributes = Attributes {
        content_type: "image/png".to_string(),
        tags: vec![("kind".to_string(), "poster".to_string())],
        ..Attributes::default()
    };
    let second = bytes(5_000, 2);
    let replaced = posters
        .put_with(
            "alien/one-sheet.png",
            &second,
            &PutOptions::default().attributes(attributes.clone()),
        )
        .await?;
    assert_ne!(
        replaced.object, put.object,
        "a replace keeps the object's id"
    );
    let stat = posters.stat("alien/one-sheet.png").await?;
    assert_eq!(stat.size, 5_000);
    assert_eq!(stat.attributes, Some(attributes));
    assert_eq!(posters.get("alien/one-sheet.png").await?.1, second);
    // an empty object is an object
    posters.put("empty", &[]).await?;
    assert_eq!(posters.get("empty").await?.1, Vec::<u8>::new());
    // a delete, then nothing is there, and a delete of nothing is done
    assert!(posters.delete("alien/one-sheet.png").await?);
    let missing = posters.get("alien/one-sheet.png").await.unwrap_err();
    assert_eq!(
        outcome(&missing),
        Some(ObjectOutcome::NotFound),
        "{missing:?}"
    );
    assert!(!posters.delete("alien/one-sheet.png").await?);
    drop(client);
    pool.exit()?;
    Ok(())
}

/// Open a connection by hand, asking for these capabilities, and say what the server granted
///
/// # Arguments
///
/// * `addr` - The server
/// * `caps` - The capabilities to ask for
async fn raw(addr: &str, caps: u8) -> Result<(TcpStream, handshake::HelloAck), TestError> {
    let mut sock = TcpStream::connect(addr).await?;
    sock.set_nodelay(true)?;
    let hello = handshake::Hello {
        schema_fingerprint: MediaClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        mechanisms: AuthMechanisms::NONE,
        caps,
        max_body_log2: 30,
    };
    sock.write_all(
        &hello
            .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a hello frames"),
    )
    .await?;
    let mut frame = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut frame).await?;
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&frame[protocol::HEADER_LEN..]);
    let ack = handshake::HelloAck::decode(&body);
    assert!(ack.reason.is_accepted());
    Ok((sock, ack))
}

/// Wait for the server to close a connection, and fail if it sends anything instead
///
/// # Arguments
///
/// * `sock` - The connection
async fn assert_closed(sock: &mut TcpStream) {
    let mut scratch = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut scratch))
        .await
        .expect("the server kept a connection it should have closed open");
    match read {
        // a clean end of stream is the server dropping its half
        Ok(0) => (),
        Ok(read) => panic!("the server sent {read} bytes instead of closing the connection"),
        // a reset is the same close, seen through a socket that still had bytes queued
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "{error:?}"
        ),
    }
}

/// Put and read back one object through the client, to show the server still answers
///
/// # Arguments
///
/// * `client` - The client
/// * `path` - A path no other check uses
async fn still_answers(client: &Shoal<MediaClient>, path: &str) -> Result<(), TestError> {
    let posters = client.bucket::<Posters>();
    let want = bytes(3_000, 9);
    posters.put(path, &want).await?;
    assert_eq!(posters.get(path).await?.1, want, "{path}");
    Ok(())
}

/// One data frame of a put, by hand
///
/// # Arguments
///
/// * `id` - The put's id
/// * `offset` - Where its bytes start
/// * `payload` - The bytes
/// * `last` - Whether it is the put's final frame
fn data_frame(id: &Uuid, offset: u64, payload: &[u8], last: bool) -> Vec<u8> {
    let mut frame = stream::data_preamble(
        id,
        offset,
        payload.len(),
        last,
        protocol::DEFAULT_MAX_FRAME_BYTES,
    )
    .expect("a data frame frames")
    .to_vec();
    frame.extend_from_slice(payload);
    frame
}

/// A client sends no object frame to a server that did not grant objects, and a server that is sent
/// one by such a client closes that connection alone
///
/// S12's `object_frames_are_refused_without_the_capability`.
#[tokio::test(flavor = "multi_thread")]
async fn object_frames_are_refused_without_the_capability() -> Result<(), TestError> {
    let Node {
        _dir,
        pool,
        client,
        addr,
    } = node().await?;
    // objects are granted beside streams, never without them, since an object's bytes are a stream
    let (_, ack) = raw(&addr, CLIENT_CAP_OBJECTS).await?;
    assert_eq!(
        ack.caps & CLIENT_CAP_OBJECTS,
        0,
        "objects granted without streams"
    );
    let (_, ack) = raw(&addr, CLIENT_CAPS).await?;
    assert_ne!(
        ack.caps & CLIENT_CAP_OBJECTS,
        0,
        "objects not granted beside streams"
    );
    // an object frame on a connection granted streams and not objects closes that connection
    let (mut sock, ack) = raw(&addr, CLIENT_CAP_STREAMS).await?;
    assert_eq!(ack.caps & CLIENT_CAP_OBJECTS, 0);
    let stat = ObjectOp::bare(Uuid::now_v7(), Posters::id(), ObjectOpKind::Stat, "a");
    sock.write_all(&stat.encode(protocol::DEFAULT_MAX_FRAME_BYTES)?)
        .await?;
    assert_closed(&mut sock).await;
    // and every other connection goes on
    still_answers(&client, "capability/after").await?;
    drop(client);
    pool.exit()?;
    // a client whose server grants streams and no objects sends it none: a server that predates
    // objects, played here by a listener that records the type of every frame it is sent
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fake = listener.local_addr()?.to_string();
    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    let recorded = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let recorded = recorded.clone();
            tokio::spawn(async move {
                // the hello, answered with streams and nothing else
                let mut hello = [0u8; handshake::HANDSHAKE_FRAME_LEN];
                if sock.read_exact(&mut hello).await.is_err() {
                    return;
                }
                let ack = handshake::HelloAck {
                    schema_fingerprint: MediaClient::SCHEMA_FINGERPRINT,
                    max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
                    reason: handshake::RefusalReason::Accepted,
                    mechanism: None,
                    caps: CLIENT_CAP_STREAMS,
                    max_body_log2: 30,
                };
                let frame = ack
                    .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
                    .expect("an ack frames");
                if sock.write_all(&frame).await.is_err() {
                    return;
                }
                // then every frame's type, and nothing answered
                loop {
                    let mut header = [0u8; protocol::HEADER_LEN];
                    if sock.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let raw = protocol::RawHeader::decode(&header);
                    recorded.lock().expect("the record").push(raw.kind);
                    let mut body = vec![0u8; raw.len as usize];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    let pool_config = PoolConfig {
        min_idle: 1,
        max_size: 2,
        ..PoolConfig::default()
    };
    let streams = StreamConfig {
        dedicated_connections: 1,
        ..StreamConfig::default()
    };
    let client = Shoal::<MediaClient>::builder()
        .endpoint(&fake)
        .pool(pool_config)
        .streams(streams)
        .routing(Routing::Endpoints)
        .build()
        .await?;
    let refused = client
        .bucket::<Posters>()
        .put("never/sent", &bytes(100, 3))
        .await
        .expect_err("a client wrote an object frame to a server that did not grant objects");
    assert!(matches!(refused, Errors::Config(_)), "{refused:?}");
    let kinds = seen.lock().expect("the record").clone();
    assert!(
        !kinds.contains(&MessageType::ObjectOp.as_byte()),
        "the client sent an object frame anyway: {kinds:?}"
    );
    Ok(())
}

/// A frame whose length, offset or type cannot be right closes its connection, and every other
/// client is answered
///
/// S12's `malformed_object_frame_ends_one_connection`.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_object_frame_ends_one_connection() -> Result<(), TestError> {
    let Node {
        _dir,
        pool,
        client,
        addr,
    } = node().await?;
    let max = protocol::DEFAULT_MAX_FRAME_BYTES;
    // a put at an offset: a put replaces a whole object
    let mut offset = ObjectOp::put(Uuid::now_v7(), Posters::id(), "bad/offset", 10, 0);
    offset.offset = 4;
    // a stat carrying a length
    let mut length = ObjectOp::bare(Uuid::now_v7(), Posters::id(), ObjectOpKind::Stat, "bad/len");
    length.length = 7;
    // a frame whose path runs past its end
    let mut past = ObjectOp::bare(
        Uuid::now_v7(),
        Posters::id(),
        ObjectOpKind::Stat,
        "bad/past",
    )
    .encode(max)?;
    past[protocol::HEADER_LEN + 26] = 0xff;
    // a type this build does not have
    let mut kind = ObjectOp::bare(
        Uuid::now_v7(),
        Posters::id(),
        ObjectOpKind::Stat,
        "bad/kind",
    )
    .encode(max)?;
    kind[1] = 30;
    // a length no operation can be, refused before anything is allocated for it
    let huge = Header::new(MessageType::ObjectOp, Flags::NONE, 1 << 20, max)?
        .encode()
        .to_vec();
    let mut frames = vec![
        ("an offset", offset.encode(max)?),
        ("a length", length.encode(max)?),
        ("a path past the frame", past),
        ("an unknown type", kind),
        ("an impossible length", huge),
    ];
    // a put's data frame at an offset its stream has not reached
    let id = Uuid::now_v7();
    let mut gap = ObjectOp::put(id, Posters::id(), "bad/gap", 8192, 0).encode(max)?;
    gap.extend_from_slice(&data_frame(&id, 4096, &[1u8; 4096], true));
    frames.push(("a data frame past its stream's bytes", gap));
    for (what, frame) in frames {
        let (mut sock, _) = raw(&addr, CLIENT_CAPS).await?;
        sock.write_all(&frame).await?;
        assert_closed(&mut sock).await;
        // and the client on its own connections is answered after each
        still_answers(&client, &format!("malformed/after {what}")).await?;
    }
    drop(client);
    pool.exit()?;
    Ok(())
}

/// A connection cut in the middle of a put's frames, and the put retried under its identity,
/// leaves the object as one put would; so does a put whose answer was lost and is sent again
///
/// S12's `retried_write_across_frames_is_the_same_write`.
#[tokio::test(flavor = "multi_thread")]
async fn retried_write_across_frames_is_the_same_write() -> Result<(), TestError> {
    let Node {
        _dir,
        pool,
        client,
        addr,
    } = node().await?;
    let posters = client.bucket::<Posters>();
    let path = "retry/one-sheet.png";
    let want = bytes(16 << 10, 5);
    let id = Uuid::now_v7();
    // the put's opener and half its data frames, then the connection cut
    let (mut sock, _) = raw(&addr, CLIENT_CAPS).await?;
    let op = ObjectOp::put(
        id,
        Posters::id(),
        path,
        want.len() as u64,
        checksum::crc64(&want),
    );
    sock.write_all(&op.encode(protocol::DEFAULT_MAX_FRAME_BYTES)?)
        .await?;
    sock.write_all(&data_frame(&id, 0, &want[..4096], false))
        .await?;
    sock.write_all(&data_frame(&id, 4096, &want[4096..8192], false))
        .await?;
    sock.flush().await?;
    drop(sock);
    // a put cut short committed nothing
    tokio::time::sleep(Duration::from_millis(200)).await;
    let nothing = posters.stat(path).await.unwrap_err();
    assert_eq!(
        outcome(&nothing),
        Some(ObjectOutcome::NotFound),
        "{nothing:?}"
    );
    // retried whole under its identity, it is the one put
    let options = PutOptions::default().identity(id);
    let first = posters.put_with(path, &want, &options).await?;
    assert_eq!(first.object, object_id(&id, path));
    assert_eq!(first.content_version, 1);
    // sent again, as a put whose answer was lost would be, it is still the one put
    let again = posters.put_with(path, &want, &options).await?;
    assert_eq!(again, first, "a retried put was written twice");
    let stat = posters.stat(path).await?;
    assert_eq!(stat.object, first.object);
    assert_eq!(stat.content_version, 1);
    assert_eq!(posters.get(path).await?.1, want);
    // and a put under another identity is another write, of another object
    let other = posters.put(path, &want).await?;
    assert_ne!(other.object, first.object);
    drop(client);
    pool.exit()?;
    Ok(())
}

/// Two paths forced onto one key are stored, read, replaced and deleted independently
///
/// S3's `colliding_paths_are_told_apart`. A 64 bit hash does not collide on demand, so the node's
/// object layer is told to put every path under a prefix in one row
/// (`shoal::server::faults::force_path_key`); the row then holds an entry for each path, each
/// compared whole on every read.
#[tokio::test(flavor = "multi_thread")]
async fn colliding_paths_are_told_apart() -> Result<(), TestError> {
    let Node {
        _dir, pool, client, ..
    } = node().await?;
    shoal::server::faults::force_path_key("collide/", 0xc011_1de5);
    let posters = client.bucket::<Posters>();
    // two objects in one row
    let a = bytes(2_000, 11);
    let b = bytes(3_000, 12);
    let put_a = posters.put("collide/a", &a).await?;
    let put_b = posters.put("collide/b", &b).await?;
    assert_ne!(put_a.object, put_b.object);
    assert_eq!(posters.get("collide/a").await?.1, a);
    assert_eq!(posters.get("collide/b").await?.1, b);
    // a path that shares the key and names no entry is not found, never another's object
    let neither = posters.get("collide/c").await.unwrap_err();
    assert_eq!(
        outcome(&neither),
        Some(ObjectOutcome::NotFound),
        "{neither:?}"
    );
    // replacing one leaves the other as it was
    let a2 = bytes(1_500, 13);
    posters.put("collide/a", &a2).await?;
    assert_eq!(posters.get("collide/a").await?.1, a2);
    assert_eq!(posters.get("collide/b").await?.1, b);
    assert_eq!(posters.stat("collide/b").await?.object, put_b.object);
    // and deleting one leaves the other
    assert!(posters.delete("collide/b").await?);
    let gone = posters.get("collide/b").await.unwrap_err();
    assert_eq!(outcome(&gone), Some(ObjectOutcome::NotFound), "{gone:?}");
    assert_eq!(posters.get("collide/a").await?.1, a2);
    assert!(posters.delete("collide/a").await?);
    shoal::server::faults::clear_path_keys();
    drop(client);
    pool.exit()?;
    Ok(())
}

/// An object at or under the threshold is written and read with no device configured, and one
/// over it is refused there by name
///
/// S3's `inline_object_never_touches_a_device`. No pool has a device before M14, so an object is
/// held in its row or not at all.
#[tokio::test(flavor = "multi_thread")]
async fn inline_object_never_touches_a_device() -> Result<(), TestError> {
    // a pool of the default threshold, 16 KiB, and nothing in the configuration names a device
    let Node {
        _dir, pool, client, ..
    } = node().await?;
    let posters = client.bucket::<Posters>();
    let at = bytes(16 << 10, 21);
    posters.put("inline/at", &at).await?;
    assert_eq!(posters.get("inline/at").await?.1, at);
    // one byte past it is refused, naming the threshold and that no write reaches a pool's devices
    let past = bytes((16 << 10) + 1, 22);
    let refused = posters.put("inline/past", &past).await.unwrap_err();
    assert_eq!(
        outcome(&refused),
        Some(ObjectOutcome::TooLarge),
        "{refused:?}"
    );
    let said = format!("{refused:?}");
    assert!(
        said.contains("inline threshold") && said.contains("device"),
        "{said}"
    );
    // nothing was committed for it, and the connection it travelled on goes on
    let nothing = posters.stat("inline/past").await.unwrap_err();
    assert_eq!(
        outcome(&nothing),
        Some(ObjectOutcome::NotFound),
        "{nothing:?}"
    );
    still_answers(&client, "inline/after").await?;
    drop(client);
    pool.exit()?;
    // a pool of a larger threshold holds a larger object inline, and refuses past its own
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_config(&temp_dir);
    let big = PoolSpec::new("ssd", Redundancy::Replicas(1)).inline(1 << 20);
    conf.storage = conf.storage.clone().pool("big", big).bind("Posters", "big");
    let mut pool = ShoalPool::<Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<MediaClient>::new(&addr).await?;
    let posters = client.bucket::<Posters>();
    let mib = bytes(1 << 20, 23);
    posters.put("inline/mib", &mib).await?;
    assert_eq!(posters.get("inline/mib").await?.1, mib);
    let over = posters
        .put("inline/over", &bytes((1 << 20) + 1, 24))
        .await
        .unwrap_err();
    assert_eq!(outcome(&over), Some(ObjectOutcome::TooLarge), "{over:?}");
    drop(client);
    pool.exit()?;
    Ok(())
}

/// A start with a bucket bound to no pool, or to a pool nobody defined, is refused by name, on a
/// standalone node and on a node that bootstraps a cluster
///
/// S4's `unbound_bucket_is_refused_by_name`.
#[test]
fn unbound_bucket_is_refused_by_name() {
    // the refusal a start ends in, as text
    let refusal = |conf| match ShoalPool::<Media>::start(conf) {
        Ok(_) => panic!("a node started with a bucket it cannot keep an object in"),
        Err(error) => format!("{error:?}"),
    };
    // no binding at all, standalone
    let temp_dir = utils::test_dir();
    let said = refusal(utils::build_config(&temp_dir));
    assert!(
        said.contains("bucket Posters is bound to no storage pool"),
        "{said}"
    );
    // no binding, on the node that bootstraps a cluster
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_config(&temp_dir);
    conf.cluster = Some(Cluster::default().bootstrap(true));
    let said = refusal(conf);
    assert!(
        said.contains("bucket Posters is bound to no storage pool"),
        "{said}"
    );
    // a binding to a pool nobody defined
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_config(&temp_dir);
    conf.storage = conf.storage.clone().bind("Posters", "nowhere");
    let said = refusal(conf);
    assert!(
        said.contains("Posters") && said.contains("nowhere"),
        "{said}"
    );
    // a binding of a bucket the schema does not declare
    let temp_dir = utils::test_dir();
    let mut conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]);
    conf.storage = conf.storage.clone().bind("Postres", "local");
    let said = refusal(conf);
    assert!(said.contains("Postres"), "{said}");
}

/// S4's `rotational_pool_of_one_loss_is_refused`. A pool of rotational devices whose redundancy
/// survives one loss - two copies, 2+1 - or none is refused at the start that commits it, unless its
/// configuration accepts the exposure by name, and then it starts named in a warning; its chunks
/// are 4 MiB unless a stripe says otherwise, and a stripe that gives smaller chunks is refused,
/// naming one that does not (X7, X12, F91)
#[test]
fn rotational_pool_of_one_loss_is_refused() {
    use shoal::shared::pools::Media as PoolMedia;
    // the refusal a start ends in, as text
    let refusal = |conf| match ShoalPool::<Media>::start(conf) {
        Ok(pool) => {
            let _ = pool.exit();
            panic!("a node started with a rotational pool that survives no second loss");
        }
        Err(error) => format!("{error:?}"),
    };
    let rotational = |redundancy| PoolSpec::new("hdd", redundancy).media(PoolMedia::Rotational);
    // two copies, 2+1 and one copy are refused, on a standalone node and on a cluster's bootstrapper
    for (redundancy, bootstrap) in [
        (Redundancy::Replicas(2), false),
        (Redundancy::Erasure { data: 2, parity: 1 }, false),
        (Redundancy::Replicas(1), false),
        (Redundancy::Replicas(2), true),
    ] {
        let temp_dir = utils::test_dir();
        let mut conf = utils::build_config(&temp_dir);
        if bootstrap {
            conf.cluster = Some(Cluster::default().bootstrap(true));
        }
        conf.storage = conf.storage.clone().pool("bulk", rotational(redundancy)).bind("Posters", "bulk");
        let said = refusal(conf);
        assert!(said.contains("accept_rebuild_exposure"), "{redundancy}: {said}");
    }
    // a stripe whose chunks are under 4 MiB is refused, naming the stripe of 4 MiB chunks
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_config(&temp_dir);
    let mut narrow = rotational(Redundancy::Erasure { data: 4, parity: 2 });
    narrow.stripe = Some(shoal::shared::pools::ByteSize(4 << 20));
    conf.storage = conf.storage.clone().pool("bulk", narrow).bind("Posters", "bulk");
    let said = refusal(conf);
    assert!(said.contains("16MiB"), "{said}");
    // three copies of 4 MiB chunks start as they are
    assert_eq!(rotational(Redundancy::Replicas(3)).chunk_bytes(), 4 << 20);
    assert_eq!(rotational(Redundancy::Erasure { data: 4, parity: 2 }).chunk_bytes(), 4 << 20);
    // and two copies accepted by name start, the pool's readiness naming the exposure
    let temp_dir = utils::test_dir();
    let mut conf = utils::build_config(&temp_dir);
    let accepted = rotational(Redundancy::Replicas(2)).accept_rebuild_exposure();
    conf.storage = conf.storage.clone().pool("bulk", accepted).bind("Posters", "bulk");
    let mut pool = ShoalPool::<Media>::start(conf).expect("an accepted exposure starts");
    pool.ready(utils::READY_TIMEOUT).expect("the node answers");
    let status = pool.pool_status().expect("a standalone node's readiness");
    assert!(
        status[0].warnings.iter().any(|warning| warning.contains("accepts the exposure")),
        "{status:?}"
    );
    pool.exit().expect("the node stops");
}

/// An object survives its node's restart: its row is replayed from the intent log and read back
#[tokio::test(flavor = "multi_thread")]
async fn an_inline_object_survives_a_restart() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]);
    let mut pool = ShoalPool::<Media>::start(conf.clone())?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<MediaClient>::new(&addr).await?;
    // objects of many lengths, so their rows land at every alignment the log has
    let mut wanted = Vec::new();
    for at in 0..40usize {
        let want = bytes(1_000 + at * 337, at as u8);
        client
            .bucket::<Posters>()
            .put(&format!("restart/{at}"), &want)
            .await?;
        wanted.push(want);
    }
    drop(client);
    pool.exit()?;
    // the same directory, started again
    let mut pool = ShoalPool::<Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<MediaClient>::new(&addr).await?;
    for (at, want) in wanted.iter().enumerate() {
        let got = client
            .bucket::<Posters>()
            .get(&format!("restart/{at}"))
            .await?
            .1;
        assert_eq!(&got, want, "restart/{at}");
    }
    drop(client);
    pool.exit()?;
    Ok(())
}

/// A proxy that holds every byte a server sends for a fixed time before its client sees it
///
/// Returns the address to connect to. What the client sends goes up at once; what comes back
/// arrives `delay` late, so an operation's round trip takes at least that long whatever the
/// server does.
///
/// # Arguments
///
/// * `server` - The server's address
/// * `delay` - How long the server's bytes are held
async fn delay_line(server: String, delay: Duration) -> Result<String, TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            // a connection up to the server for each one the client opens
            let Ok(upstream) = TcpStream::connect(&server).await else {
                continue;
            };
            let (mut client_rx, mut client_tx) = client.into_split();
            let (mut server_rx, mut server_tx) = upstream.into_split();
            // the client's bytes go up at once
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut client_rx, &mut server_tx).await;
            });
            // the server's bytes are stamped with when they may be delivered
            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                while let Ok(read) = server_rx.read(&mut buf).await {
                    if read == 0
                        || tx
                            .send((tokio::time::Instant::now() + delay, buf[..read].to_vec()))
                            .is_err()
                    {
                        break;
                    }
                }
            });
            // and delivered in order, each no sooner than its time
            tokio::spawn(async move {
                while let Some((at, bytes)) = rx.recv().await {
                    tokio::time::sleep_until(at).await;
                    if client_tx.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    Ok(addr)
}

/// Object operations on one connection wait for their answers together, not one after another
///
/// A bundle's connection goes back to the pool once its frames are written. An object operation
/// kept its connection until its answer came back, so the two connections a client sets apart
/// carried two operations at a time however many were in flight, and the rest queued for a
/// connection: on the lab, a put of 4 KiB waited 0.2 to 3.2 seconds for one and took 5 ms once it
/// had it (item 230). Every answer here comes back a quarter of a second late, on one connection
/// set apart, so six stats sent together take about that long, not six times it.
#[tokio::test(flavor = "multi_thread")]
async fn object_operations_share_a_connection_while_they_wait() -> Result<(), TestError> {
    let Node {
        _dir,
        pool,
        client,
        addr,
    } = node().await?;
    client
        .bucket::<Posters>()
        .put("held/one", &bytes(512, 3))
        .await?;
    // a client of one connection set apart, every answer it reads a quarter of a second late
    let delay = Duration::from_millis(250);
    let proxy = delay_line(addr, delay).await?;
    let late = Shoal::<MediaClient>::builder()
        .endpoint(&proxy)
        .streams(StreamConfig {
            dedicated_connections: 1,
            ..StreamConfig::default()
        })
        .routing(Routing::Endpoints)
        .build()
        .await?;
    let posters = late.bucket::<Posters>();
    // one stat opens the connection set apart, so the timed ones do not wait on its hello
    posters.stat("held/one").await?;
    let started = std::time::Instant::now();
    let stats = futures::future::join_all((0..6).map(|_| posters.stat("held/one"))).await;
    let took = started.elapsed();
    for stat in stats {
        stat?;
    }
    assert!(
        took < delay * 3,
        "six stats on one connection took {took:?}, as if each waited for the answer before it"
    );
    drop((late, client));
    pool.exit()?;
    Ok(())
}

/// A line to a server that carries every byte both ways until it is frozen, and then carries none,
/// its connections held open: a server that stopped answering, as a client's connection sees it
///
/// # Arguments
///
/// * `server` - The server's address
async fn freezing_line(server: String) -> Result<(String, Arc<AtomicBool>), TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let frozen = Arc::new(AtomicBool::new(false));
    let lines = frozen.clone();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            // a connection up to the server for each one the client opens
            let Ok(upstream) = TcpStream::connect(&server).await else {
                continue;
            };
            let (client_rx, client_tx) = client.into_split();
            let (server_rx, server_tx) = upstream.into_split();
            // each way carried until the line is frozen, then held where it is
            for (mut from, mut to) in [
                (
                    Box::new(client_rx) as Box<dyn tokio::io::AsyncRead + Unpin + Send>,
                    Box::new(server_tx) as Box<dyn tokio::io::AsyncWrite + Unpin + Send>,
                ),
                (Box::new(server_rx), Box::new(client_tx)),
            ] {
                let frozen = lines.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(read) = from.read(&mut buf).await {
                        if read == 0 {
                            break;
                        }
                        // a frozen line keeps what it read and its connections open
                        while frozen.load(Ordering::Relaxed) {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        if to.write_all(&buf[..read]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    });
    Ok((addr, frozen))
}

/// An object operation's attempt that makes no progress gives up after its bound, by name: a put
/// sent once on a connection whose line stopped carrying bytes is refused `OutcomeUnknown` within a
/// second or two of its client's bound, and a stat `Timeout`. Before it an attempt waited for its
/// answer with no bound, and its retry budget ran only between attempts
/// ([item 308](../../docs/src/appendix/resolved/an-object-attempt-gives-up.md))
#[tokio::test(flavor = "multi_thread")]
async fn an_object_attempt_that_makes_no_progress_gives_up() -> Result<(), TestError> {
    let Node {
        _dir,
        pool,
        client,
        addr,
    } = node().await?;
    client
        .bucket::<Posters>()
        .put("held/two", &bytes(512, 4))
        .await?;
    // a client through a line that can go silent, whose object attempts give up after a second,
    // with two connections set apart for them
    let (line, frozen) = freezing_line(addr).await?;
    let stalled = Shoal::<MediaClient>::builder()
        .endpoint(&line)
        .routing(Routing::Endpoints)
        .streams(StreamConfig {
            dedicated_connections: 2,
            ..StreamConfig::default()
        })
        .deadlines(Deadlines {
            object_attempt: Duration::from_secs(1),
            ..Deadlines::default()
        })
        .build()
        .await?;
    // each operation sent once, so what an attempt does is what the caller is told
    let posters = stalled.objects(Posters::id()).retry(Duration::ZERO);
    // both connections open before the line goes silent: a connection opened after it could not
    // shake hands, which the pool's own checkout bounds
    let (first, second) =
        futures::future::join(posters.stat("held/two"), posters.stat("held/two")).await;
    first?;
    second?;
    // the line carries nothing more, either way, and each operation stalls on a connection of its own
    frozen.store(true, Ordering::Relaxed);
    for (what, expected) in [("a put", ErrorCode::OutcomeUnknown), ("a stat", ErrorCode::Timeout)] {
        let started = std::time::Instant::now();
        let answer = tokio::time::timeout(Duration::from_secs(20), async {
            match what {
                "a stat" => posters.stat("held/two").await.map(|_| ()),
                _ => posters.put("held/three", &bytes(512, 5)).await.map(|_| ()),
            }
        })
        .await;
        let took = started.elapsed();
        match answer {
            Err(_) => panic!("{what} on a silent line waited past twenty seconds"),
            Ok(Ok(())) => panic!("{what} on a silent line was answered"),
            Ok(Err(error)) => {
                assert!(took < Duration::from_secs(5), "{what} gave up only after {took:?}");
                assert!(
                    matches!(&error, Errors::Server { code, .. } if *code == expected),
                    "{what} gave up as {error:?}, not {expected:?}"
                );
            }
        }
    }
    drop((stalled, client));
    pool.exit()?;
    Ok(())
}

/// One put whose task takes none of its bytes no longer stops its whole connection: the reader
/// gives the put up once its task took nothing for the node's query deadline, drops its later bytes
/// and reads on, so a stat behind it on the same connection is answered, and the put is answered by
/// name. Before it every operation on that connection waited for the one put's task
/// ([item 309](../../docs/src/appendix/resolved/a-full-feed-leaves-its-connection-read.md))
#[tokio::test(flavor = "multi_thread")]
async fn a_put_whose_task_takes_nothing_leaves_its_connection_read() -> Result<(), TestError> {
    // a node whose query deadline is two seconds, Posters on a device so a put's bytes are piped
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(1))
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    let striped = striped_node_tuned(1, pool, |mut conf| {
        conf.networking = conf.networking.query_deadline(Duration::from_secs(2));
        conf
    })
    .await?;
    striped
        .node
        .client
        .bucket::<Posters>()
        .put("fed/small", &bytes(512, 6))
        .await?;
    // a client of one connection set apart for objects, so the stat goes where the put went
    let one = Arc::new(
        Shoal::<MediaClient>::builder()
            .endpoint(&striped.node.addr)
            .routing(Routing::Endpoints)
            .streams(StreamConfig {
                dedicated_connections: 1,
                ..StreamConfig::default()
            })
            .build()
            .await?,
    );
    one.objects(Posters::id()).retry(Duration::ZERO).stat("fed/small").await?;
    // every put under fed/held takes none of its bytes for twenty seconds: its four frames of
    // window fill, and the rest of its six MiB sit in the connection behind them
    shoal::server::objects::faults::hold_feed("fed/held", Duration::from_secs(20));
    let putter = tokio::spawn({
        let one = one.clone();
        async move {
            one.objects(Posters::id())
                .retry(Duration::ZERO)
                .put("fed/held", &bytes(6 << 20, 7))
                .await
        }
    });
    // its frames written, a stat behind them on the same connection
    tokio::time::sleep(Duration::from_secs(1)).await;
    let started = std::time::Instant::now();
    let stat = tokio::time::timeout(
        Duration::from_secs(15),
        one.objects(Posters::id()).retry(Duration::ZERO).stat("fed/small"),
    )
    .await;
    let took = started.elapsed();
    shoal::server::objects::faults::clear_feed_holds();
    match stat {
        Err(_) => panic!(
            "a stat behind a put whose task took none of its bytes waited past fifteen seconds"
        ),
        Ok(answered) => {
            answered?;
            assert!(took < Duration::from_secs(8), "a stat behind a held put took {took:?}");
        }
    }
    // and the put itself is answered by name, never left waiting
    let put = tokio::time::timeout(Duration::from_secs(40), putter)
        .await
        .expect("the held put was never answered")
        .expect("the put's task ran");
    assert!(put.is_err(), "a put whose bytes were dropped was made current: {put:?}");
    drop(one);
    striped.node.pool.exit()?;
    Ok(())
}

/// A pooled connection past its lifetime keeps the answers owed on it (item 317)
///
/// An operation gives its pooled connection back once its frames are written, and its answer is
/// routed to it by its id ([Resolved #230](../../docs/src/appendix/resolved/object-connection-held.md)).
/// bb8 checks a connection's age when it is given back, never when it is handed out, and drops one
/// past its pool's lifetime; dropping it shut the connection's write half down, and its server read
/// that as its client gone and dropped what it owed. A get on a connection older than its pool's
/// second of lifetime, held three seconds on its node, is still answered whole
/// ([Resolved #317](../../docs/src/appendix/resolved/owed-connection-kept.md)).
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_past_its_lifetime_keeps_the_answers_it_owes() -> Result<(), TestError> {
    use shoal::server::objects::faults;
    let striped = striped_node(1, 1).await?;
    // an object past the inline threshold, so its get reads stripes, which a hold holds
    let expected = bytes(1 << 20, 31);
    striped.node.client.bucket::<Posters>().put("held.bin", &expected).await?;
    // a client whose connections live a second
    let short = Shoal::<MediaClient>::builder()
        .endpoint(&striped.node.addr)
        .routing(Routing::Endpoints)
        .pool(PoolConfig {
            max_lifetime: Some(Duration::from_secs(1)),
            ..PoolConfig::default()
        })
        .build()
        .await?;
    let objects = short.objects(Posters::id()).retry(Duration::ZERO);
    // a get opens the connection set apart for objects, which goes back to its pool young
    let (_, read) = objects.get("held.bin").await?;
    assert!(read == expected, "a get read back other bytes than were put");
    // and outlives its lifetime there, idle, before the reaper's first pass
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // every striped read held three seconds between its entry and its stripes, so the next get's
    // answer is owed long after its old connection was given back
    faults::hold_read(Duration::from_secs(3));
    let read = objects.get("held.bin").await;
    faults::hold_read(Duration::ZERO);
    // answered once, whole, with no try again to hide a lost one
    let (_, read) = read?;
    assert!(read == expected, "a held get read back other bytes than were put");
    striped.node.pool.exit()?;
    Ok(())
}

/// A node of the media schema whose Posters pool keeps stripes on devices of its own
struct StripedNode {
    /// The node, its client and its address
    node: Node,
    /// The devices' directory, apart from the node's storage
    _devices: tempfile::TempDir,
    /// The device probe's override, held while the node runs
    _facts: std::sync::MutexGuard<'static, ()>,
}

/// Start a node with devices, Posters bound to a pool of `copies` copies over them by device
///
/// # Arguments
///
/// * `devices` - How many devices
/// * `copies` - How many copies the pool keeps
async fn striped_node(devices: usize, copies: u8) -> Result<StripedNode, TestError> {
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(copies))
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    striped_node_with(devices, pool).await
}

/// Start a node with devices, Posters bound to the pool given
///
/// # Arguments
///
/// * `devices` - How many devices
/// * `pool` - The pool
async fn striped_node_with(devices: usize, pool: PoolSpec) -> Result<StripedNode, TestError> {
    striped_node_tuned(devices, pool, |conf| conf).await
}

/// Start a node with devices, Posters bound to the pool given, its config changed first
///
/// # Arguments
///
/// * `devices` - How many devices
/// * `pool` - The pool
/// * `tune` - What changes the node's config before it starts
async fn striped_node_tuned(devices: usize, pool: PoolSpec, tune: impl FnOnce(shoal::server::conf::Conf) -> shoal::server::conf::Conf) -> Result<StripedNode, TestError> {
    let facts = utils::device_facts(&[]);
    let temp_dir = utils::test_dir();
    let device_dir = utils::test_dir();
    let specs: Vec<shoal::shared::devices::DeviceSpec> = (0..devices)
        .map(|index| {
            let path = device_dir.path().join(format!("device-{index}"));
            std::fs::create_dir_all(&path).expect("made");
            shoal::shared::devices::DeviceSpec::new(path, "ssd")
        })
        .collect();
    let mut conf = utils::with_devices(utils::build_config(&temp_dir), &specs);
    conf.storage = conf.storage.clone().pool("local", pool).bind("Posters", "local");
    let conf = tune(conf);
    let mut pool = ShoalPool::<Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<MediaClient>::new(&addr).await?;
    Ok(StripedNode {
        node: Node {
            _dir: temp_dir,
            pool,
            client,
            addr,
        },
        _devices: device_dir,
        _facts: facts,
    })
}

/// An object past its pool's inline threshold is put as stripes, read whole and in ranges,
/// written in place, appended to and truncated, and every read is what its writes left
#[tokio::test(flavor = "multi_thread")]
async fn a_striped_object_round_trips() -> Result<(), TestError> {
    let striped = striped_node(2, 2).await?;
    let posters = striped.node.client.bucket::<Posters>();
    // ten megabytes and three bytes: three stripes of 4 MiB, the last part full
    let mut expected = bytes((10 << 20) + 3, 7);
    let put = posters.put("big.bin", &expected).await?;
    assert_eq!(put.size, expected.len() as u64);
    let (_, read) = posters.get("big.bin").await?;
    assert!(read == expected, "a striped object read back as it was put");
    // a range across a stripe boundary
    let (_, middle) = posters
        .read_range("big.bin", (4 << 20) - 100, 300, None)
        .await?;
    assert_eq!(middle, expected[(4 << 20) - 100..(4 << 20) + 200].to_vec());
    // a write in place across a unit boundary in the middle of a stripe
    let patch = bytes(70_000, 99);
    posters.write_at("big.bin", 5_000_000, &patch).await?;
    expected[5_000_000..5_070_000].copy_from_slice(&patch);
    let (_, read) = posters.get("big.bin").await?;
    assert!(read == expected, "a write in place reads back where it was written");
    // an append at the end
    let tail = bytes(100_000, 3);
    let appended = posters.append("big.bin", &tail).await?;
    expected.extend_from_slice(&tail);
    assert_eq!(appended.size, expected.len() as u64);
    let (_, read) = posters.get("big.bin").await?;
    assert!(read == expected, "an append reads back at the end");
    // a cut inside a stripe, then the object grown again: what was cut reads as zeros
    posters.truncate("big.bin", 6_000_000).await?;
    expected.truncate(6_000_000);
    posters.truncate("big.bin", 9_000_000).await?;
    expected.resize(9_000_000, 0);
    let (info, read) = posters.get("big.bin").await?;
    assert_eq!(info.size, 9_000_000);
    assert!(read == expected, "truncated bytes never return");
    Ok(())
}

/// Every stripe chunk file under a node's devices, by path, with its bytes
///
/// # Arguments
///
/// * `devices` - The devices' directory
fn chunk_files(devices: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    // every file under every slice's chunks directory
    let mut found = std::collections::BTreeMap::new();
    let mut walk = vec![devices.to_path_buf()];
    while let Some(dir) = walk.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk.push(path);
            } else if path.components().any(|part| part.as_os_str() == "chunks") {
                found.insert(path.clone(), std::fs::read(&path).unwrap_or_default());
            }
        }
    }
    found
}

/// The resident memory of this process, from `statm`
fn resident_bytes() -> u64 {
    // the second field, in pages of 4 KiB
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|pages| pages.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

/// A stripe never written, inside the object's size, reads as zeros and holds no stripe chunk
/// (S3, P13's companion)
#[tokio::test(flavor = "multi_thread")]
async fn a_hole_reads_as_zeros() -> Result<(), TestError> {
    let striped = striped_node(2, 2).await?;
    let posters = striped.node.client.bucket::<Posters>();
    // two stripes put, then a write at the fourth, which leaves the third a hole
    let mut expected = bytes(5 << 20, 11);
    let put = posters.put("holey.bin", &expected).await?;
    let far = bytes(1000, 12);
    posters.write_at("holey.bin", 13 << 20, &far).await?;
    expected.resize(13 << 20, 0);
    expected.extend_from_slice(&far);
    let (info, read) = posters.get("holey.bin").await?;
    assert_eq!(info.size, expected.len() as u64);
    assert!(read == expected, "a hole reads as zeros");
    // one read from the second stripe's last bytes into the hole: its first frame is written from a
    // holder's bytes and a run of zeros, the rest from zeros alone (F109)
    let (_, range) = posters.read_range("holey.bin", (5 << 20) - 100, 4 << 20, None).await?;
    assert_eq!(range.len(), 4 << 20);
    assert!(range == expected[(5 << 20) - 100..(9 << 20) - 100], "a range from bytes into a hole");
    // and no device holds a chunk of the third stripe, which nothing wrote
    let object = format!("{:032x}", put.object);
    let hole = format!("{:016x}.", 2u64);
    let chunks = chunk_files(striped._devices.path());
    assert!(chunks.keys().any(|path| path.to_string_lossy().contains(&object)));
    assert!(
        !chunks.keys().any(|path| {
            let path = path.to_string_lossy();
            path.contains(&object) && path.rsplit('/').next().is_some_and(|name| name.starts_with(&hole))
        }),
        "a hole holds no stripe chunk"
    );
    Ok(())
}

/// A stripe committed past a cut by a writer that read the size before the truncate is never
/// shown, after any later extension (S3, P13)
#[tokio::test(flavor = "multi_thread")]
async fn truncated_bytes_never_return() -> Result<(), TestError> {
    use shoal::server::objects::faults;
    let striped = striped_node(2, 2).await?;
    let posters = striped.node.client.bucket::<Posters>().retry(Duration::ZERO);
    // three stripes, every byte known
    let original = bytes(12 << 20, 21);
    posters.put("cut.bin", &original).await?;
    // a writer into the third stripe, held between its stages and its commit
    faults::hold_stager(Duration::from_millis(1500));
    let writer = {
        let client = Shoal::<MediaClient>::new(&striped.node.addr).await?;
        tokio::spawn(async move {
            let posters = client.bucket::<Posters>().retry(Duration::ZERO);
            posters.write_at("cut.bin", 9 << 20, &bytes(4096, 22)).await
        })
    };
    // a truncate to the stripe boundary commits while it is held: once the writer is counted held,
    // its size read and its stages done, never after a time, since a writer a loaded host had not
    // brought to its hold yet wrote after the cut, and its bytes are rightly there (item 332)
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while faults::stagers_held() == 0 {
        assert!(std::time::Instant::now() < deadline, "the writer was never held at its stager");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    posters.truncate("cut.bin", 4 << 20).await?;
    let written = writer.await.expect("the writer ran");
    faults::hold_stager(Duration::ZERO);
    // the held write's stripe committed under the old epoch, or did not; either way the object
    // grown back shows zeros where the cut was, and never the held write's bytes
    let _ = written;
    posters.truncate("cut.bin", 12 << 20).await?;
    let (_, read) = posters.get("cut.bin").await?;
    let mut expected = original[..4 << 20].to_vec();
    expected.resize(12 << 20, 0);
    assert!(read == expected, "the bytes a truncate cut never return");
    Ok(())
}

/// During and after a replace, no read of one object touches a stripe chunk of the other, and a
/// reader sees the old object or the new one, whole (S3, P19)
#[tokio::test(flavor = "multi_thread")]
async fn replace_shares_nothing_with_its_predecessor() -> Result<(), TestError> {
    let striped = striped_node(2, 2).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let old = bytes(6 << 20, 31);
    let first = posters.put("swap.bin", &old).await?;
    let before = chunk_files(striped._devices.path());
    let old_dir = format!("{:032x}", first.object);
    // readers race the replace, each seeing one object or the other whole
    let new = bytes(6 << 20, 32);
    let reader = {
        let client = Shoal::<MediaClient>::new(&striped.node.addr).await?;
        let (old, new) = (old.clone(), new.clone());
        tokio::spawn(async move {
            let posters = client.bucket::<Posters>();
            let mut seen = 0;
            for _ in 0..20 {
                let (_, read) = posters.get("swap.bin").await.expect("read");
                assert!(read == old || read == new, "a reader saw a mixture of two objects");
                seen += 1;
            }
            seen
        })
    };
    let second = posters.put("swap.bin", &new).await?;
    assert!(reader.await.expect("the reader ran") > 0);
    assert_ne!(first.object, second.object);
    // the old object's chunks are as they were, and the new one's are apart from them
    let after = chunk_files(striped._devices.path());
    for (path, bytes) in &before {
        assert_eq!(after.get(path), Some(bytes), "{} changed", path.display());
    }
    let new_dir = format!("{:032x}", second.object);
    assert!(after.keys().any(|path| path.to_string_lossy().contains(&new_dir)));
    assert!(after
        .keys()
        .filter(|path| path.to_string_lossy().contains(&new_dir))
        .all(|path| !path.to_string_lossy().contains(&old_dir)));
    assert_eq!(posters.get("swap.bin").await?.1, new);
    Ok(())
}

/// A node whose reclamation walks every tenth of a second and keeps nothing for readers (M20)
///
/// # Arguments
///
/// * `devices` - How many devices
/// * `copies` - How many copies the pool keeps
async fn reclaiming_node(devices: usize, copies: u8) -> Result<StripedNode, TestError> {
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(copies))
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    striped_node_tuned(devices, pool, |mut conf| {
        conf.storage.object_timings.reclaim_poll =
            shoal::server::conf::cluster::DurationSpec(Duration::from_millis(100));
        conf.storage.object_timings.reclaim_grace = shoal::server::conf::cluster::DurationSpec(Duration::ZERO);
        conf
    })
    .await
}

/// Wait until a check holds, or fail naming what was waited for
///
/// # Arguments
///
/// * `what` - What is waited for
/// * `within` - How long it may take
/// * `check` - Whether it holds
async fn eventually(what: &str, within: Duration, check: impl Fn() -> bool) -> Result<(), TestError> {
    let started = std::time::Instant::now();
    while started.elapsed() < within {
        if check() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(TestError::Io(std::io::Error::other(format!("{what} within {within:?}"))))
}

/// The chunk files under a node's devices that belong to an object, by its directory name
///
/// # Arguments
///
/// * `devices` - The devices' directory
/// * `object` - The object
fn chunks_of(devices: &std::path::Path, object: u128) -> usize {
    let dir = format!("{object:032x}");
    chunk_files(devices)
        .keys()
        .filter(|path| path.to_string_lossy().contains(&dir))
        .count()
}

/// A replaced object's chunks are removed once its grace is over, then a deleted object's, and
/// every chunk file under the node's devices is gone; the object at the path reads as it was put
/// throughout (M20, standalone)
#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_and_a_deleted_object_are_reclaimed_on_one_node() -> Result<(), TestError> {
    let striped = reclaiming_node(2, 2).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let devices = striped._devices.path().to_path_buf();
    // an object of two stripes, then another over it
    let old = bytes(6 << 20, 41);
    let first = posters.put("reclaimed.bin", &old).await?;
    assert!(chunks_of(&devices, first.object) > 0, "the put wrote its chunks");
    let new = bytes(6 << 20, 42);
    let second = posters.put("reclaimed.bin", &new).await?;
    // the replaced object's chunks go, and the current one's stay
    eventually("the replaced object's chunks removed", Duration::from_secs(30), || {
        chunks_of(&devices, first.object) == 0
    })
    .await?;
    assert!(chunks_of(&devices, second.object) > 0, "the current object's chunks stay");
    assert_eq!(posters.get("reclaimed.bin").await?.1, new);
    // a deleted object's chunks go too, and nothing is left under the devices
    posters.delete("reclaimed.bin").await?;
    eventually("every chunk file removed", Duration::from_secs(30), || chunk_files(&devices).is_empty()).await?;
    assert_eq!(
        outcome(&posters.get("reclaimed.bin").await.expect_err("a deleted object is not found")),
        Some(ObjectOutcome::NotFound)
    );
    // and the path takes a new object as any path does
    let again = bytes(5 << 20, 43);
    posters.put("reclaimed.bin", &again).await?;
    assert_eq!(posters.get("reclaimed.bin").await?.1, again);
    Ok(())
}

/// A truncate's floor is reclaimed: the stripes past its cut are removed, and the floor goes once
/// the stripe its cut falls inside is written past it, so an object can be cut far more times
/// than an entry keeps floors (M20, standalone)
#[tokio::test(flavor = "multi_thread")]
async fn a_floors_stripes_are_reclaimed_and_the_floor_goes() -> Result<(), TestError> {
    let striped = reclaiming_node(2, 2).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let devices = striped._devices.path().to_path_buf();
    let stripe = 4u64 << 20;
    // five stripes, cut to one: the four past the cut are removed
    let mut expected = bytes(5 * stripe as usize, 51);
    let put = posters.put("floored.bin", &expected).await?;
    let before = chunks_of(&devices, put.object);
    posters.truncate("floored.bin", stripe).await?;
    expected.truncate(stripe as usize);
    eventually("the stripes past the cut removed", Duration::from_secs(30), || {
        chunks_of(&devices, put.object) * 5 <= before
    })
    .await?;
    assert_eq!(posters.get("floored.bin").await?.1, expected);
    // cut inside the second stripe, a little further each round, then written past the cut and
    // at the end: no later floor subsumes an earlier one, so only reclamation keeps the entry
    // under its sixteen
    for round in 1..=24u64 {
        let cut = stripe + round * (stripe / 32);
        let mut taken = false;
        for _ in 0..400 {
            match posters.truncate("floored.bin", cut).await {
                Ok(_) => {
                    taken = true;
                    break;
                }
                Err(error) if outcome(&error) == Some(ObjectOutcome::TooManyFloors) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        assert!(taken, "truncate {round} was refused for its floors and never taken");
        // a cut past the end grows the object with zeros
        expected.resize(cut as usize, 0);
        // the cut stripe written past the cut, which shows its hidden units again as zeros
        let byte = [round as u8];
        posters.write_at("floored.bin", cut, &byte).await?;
        expected.push(round as u8);
        // and the object grown by a write at its end
        let end = 6 * stripe - 1;
        posters.write_at("floored.bin", end, &byte).await?;
        expected.resize(end as usize, 0);
        expected.push(round as u8);
    }
    let (_, read) = posters.get("floored.bin").await?;
    let first = read.iter().zip(&expected).position(|(got, want)| got != want);
    assert!(
        read.len() == expected.len() && first.is_none(),
        "what was cut returned: {} bytes read against {}, first differing at {first:?}",
        read.len(),
        expected.len()
    );
    Ok(())
}

/// A write and a read of an object far larger than the window complete with this process's memory
/// for the stream bounded (S12)
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_holds_no_more_than_its_window() -> Result<(), TestError> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let striped = striped_node(1, 1).await?;
    let posters = striped.node.client.bucket::<Posters>();
    // the object's bytes are made before anything is measured, and held throughout
    let length = 192usize << 20;
    let object = bytes(length, 41);
    // the most this process held above where it started, sampled while the stream runs
    let sample = || {
        let base = resident_bytes();
        let peak = Arc::new(std::sync::atomic::AtomicU64::new(base));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let sampler = {
            let (peak, running) = (peak.clone(), running.clone());
            std::thread::spawn(move || {
                while running.load(std::sync::atomic::Ordering::Relaxed) {
                    peak.fetch_max(resident_bytes(), std::sync::atomic::Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };
        (base, peak, running, sampler)
    };
    let (base, peak, running, sampler) = sample();
    posters.put("stream.bin", &object).await?;
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sampler.join().expect("sampled");
    let held = peak.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(base);
    assert!(held < 96 << 20, "a put of {length} bytes held {held} more");
    // and a read of it through a file, a buffer at a time
    let (base, peak, running, sampler) = sample();
    let mut file = posters.open("stream.bin").await?;
    let mut buffer = vec![0u8; 1 << 20];
    let mut at = 0usize;
    loop {
        let got = file.read(&mut buffer).await?;
        if got == 0 {
            break;
        }
        assert!(buffer[..got] == object[at..at + got], "the stream read back at {at}");
        at += got;
    }
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sampler.join().expect("sampled");
    assert_eq!(at, length);
    let held = peak.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(base);
    assert!(held < 96 << 20, "a read of {length} bytes held {held} more");
    // a file that writes holds no more than its window behind either
    let mut file = posters.open("stream.bin").await?;
    file.write_all(&bytes(3 << 20, 42)).await?;
    file.flush().await?;
    Ok(())
}

/// A read at an offset deep in a large object transfers that range and no other (S12)
#[tokio::test(flavor = "multi_thread")]
async fn a_seek_reads_only_what_it_asked_for() -> Result<(), TestError> {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
    let striped = striped_node(1, 1).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let object = bytes(64 << 20, 51);
    posters.put("deep.bin", &object).await?;
    let store = striped.node.pool.object_store().expect("a store");
    // a read of a hundred kilobytes fifty megabytes in
    let before = store.read_bytes();
    let (_, range) = posters
        .read_range("deep.bin", 50 << 20, 100_000, None)
        .await?;
    assert_eq!(range, object[50 << 20..(50 << 20) + 100_000].to_vec());
    assert_eq!(store.read_bytes() - before, 100_000, "only the range was read");
    // a seek sends nothing, and a read after it asks for its own range
    let mut file = posters.open("deep.bin").await?;
    let before = store.read_bytes();
    file.seek(std::io::SeekFrom::Start(40 << 20)).await?;
    assert_eq!(store.read_bytes(), before, "a seek reads nothing");
    let mut small = [0u8; 10];
    file.read_exact(&mut small).await?;
    assert_eq!(small, object[40 << 20..(40 << 20) + 10]);
    assert!(store.read_bytes() - before <= (1 << 20) as u64, "a read after a seek reads its window");
    Ok(())
}

/// A schema of two buckets, for consumers sharing one pool
mod gallery {
    use shoal::tables::Bucket;
    use shoal_derive::db;

    /// Names the bucket prints are kept in
    pub struct Prints;

    /// Names the bucket stills are kept in
    pub struct Stills;

    /// Two buckets and no table
    #[db]
    pub struct Gallery {
        /// The prints
        pub prints: Bucket<Prints>,
        /// The stills
        pub stills: Bucket<Stills>,
    }
}

/// Two buckets bound to one pool, with an object id forced equal in both, write the same stripe
/// index; neither reads or holds a chunk of the other (S4)
#[tokio::test(flavor = "multi_thread")]
async fn consumers_share_a_pool_and_nothing_else() -> Result<(), TestError> {
    use gallery::{Gallery, GalleryClient, Prints, Stills};
    use shoal::server::objects::faults;
    let _facts = utils::device_facts(&[]);
    let temp_dir = utils::test_dir();
    let device_dir = utils::test_dir();
    let specs: Vec<shoal::shared::devices::DeviceSpec> = (0..2)
        .map(|index| {
            let path = device_dir.path().join(format!("device-{index}"));
            std::fs::create_dir_all(&path).expect("made");
            shoal::shared::devices::DeviceSpec::new(path, "ssd")
        })
        .collect();
    let mut conf = utils::with_devices(utils::build_config(&temp_dir), &specs);
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(2))
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    conf.storage = conf
        .storage
        .clone()
        .pool("shared", pool)
        .bind("Prints", "shared")
        .bind("Stills", "shared");
    let mut node = ShoalPool::<Gallery>::start(conf)?;
    let addr = node.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<GalleryClient>::new(&addr).await?;
    // one object id in both buckets
    let forced: u128 = 0x0192_6f1e_7a00_7000_8000_0000_0000_beef;
    faults::force_object_id("same/", forced);
    let prints = client.bucket::<Prints>();
    let stills = client.bucket::<Stills>();
    let print = bytes(5 << 20, 61);
    let still = bytes(5 << 20, 62);
    let put_print = prints.put("same/one.bin", &print).await?;
    let put_still = stills.put("same/one.bin", &still).await?;
    faults::clear_object_ids();
    assert_eq!(put_print.object, forced);
    assert_eq!(put_still.object, forced);
    // each reads its own, written in place too
    let patch = bytes(1000, 63);
    prints.write_at("same/one.bin", 100, &patch).await?;
    let mut expected = print.clone();
    expected[100..1100].copy_from_slice(&patch);
    assert!(prints.get("same/one.bin").await?.1 == expected);
    assert!(stills.get("same/one.bin").await?.1 == still, "a consumer's write touched no other's");
    // their chunks are under two consumers' directories, the same stripes in each
    let chunks = chunk_files(device_dir.path());
    let object = format!("{forced:032x}");
    let consumers: std::collections::BTreeSet<String> = chunks
        .keys()
        .filter(|path| path.to_string_lossy().contains(&object))
        .filter_map(|path| {
            let parts: Vec<String> = path.components().map(|part| part.as_os_str().to_string_lossy().into_owned()).collect();
            let at = parts.iter().position(|part| part == "chunks")?;
            parts.get(at + 1).cloned()
        })
        .collect();
    assert_eq!(consumers.len(), 2, "two consumers' chunks: {consumers:?}");
    // deleting one leaves the other whole
    assert!(prints.delete("same/one.bin").await?);
    assert!(stills.get("same/one.bin").await?.1 == still);
    drop(client);
    node.exit()?;
    Ok(())
}

/// Small writes ride their commits: merged into the row's pending bytes, laid over by every read,
/// folded into every holder and cleared, and every read is what the writes left
#[tokio::test(flavor = "multi_thread")]
async fn small_writes_ride_their_commits() -> Result<(), TestError> {
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(2))
        .failure_domain(shoal::shared::pools::FailureDomain::Device)
        .small_writes(64 << 10);
    let striped = striped_node_with(2, pool).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let mut expected = bytes(6 << 20, 71);
    posters.put("small.bin", &expected).await?;
    // writes of a few kilobytes at scattered places, each read back at once
    for round in 0..24u64 {
        let at = (round * 377_011) % ((6 << 20) - 9000);
        let patch = bytes(3000 + (round as usize % 5) * 1000, round as u8);
        posters.write_at("small.bin", at, &patch).await?;
        expected[at as usize..at as usize + patch.len()].copy_from_slice(&patch);
        let (from, to) = (at.saturating_sub(100), at + patch.len() as u64 + 100);
        let (_, read) = posters.read_range("small.bin", from, to - from, None).await?;
        let want = &expected[from as usize..to as usize];
        let first = read.iter().zip(want).position(|(a, b)| a != b);
        assert!(
            read == want,
            "round {round} reads back: {} bytes against {}, first difference at {first:?}",
            read.len(),
            want.len()
        );
    }
    // once the clears have had their time, the whole object is what the writes left
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (_, read) = posters.get("small.bin").await?;
    assert!(read == expected, "every small write is in the object");
    Ok(())
}

/// A put past the threshold that reaches a node before its object executors start is refused
/// `NotReady`, which a client tries again, and never `TooLarge`, which says the node will never
/// write it ([Resolved #237](../../docs/src/appendix/resolved/object-store-starts-after-the-shards.md))
#[tokio::test(flavor = "multi_thread")]
async fn a_put_before_the_object_executors_start_is_asked_again() -> Result<(), TestError> {
    // a node with a device and an object executor, started and not yet told to be ready
    let _facts = utils::device_facts(&[]);
    let temp_dir = utils::test_dir();
    let device_dir = utils::test_dir();
    let path = device_dir.path().join("device-0");
    std::fs::create_dir_all(&path).expect("made");
    let specs = [shoal::shared::devices::DeviceSpec::new(path, "ssd")];
    let mut conf = utils::with_devices(utils::build_config(&temp_dir), &specs);
    let spec = PoolSpec::new("ssd", Redundancy::Replicas(1))
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    conf.storage = conf.storage.clone().pool("local", spec).bind("Posters", "local");
    let mut pool = ShoalPool::<Media>::start(conf)?;
    // the shards answer before `ready` starts the object executors, so a client reaches them
    // in between: connect as soon as one is listening
    let addr = pool.bound_addr().to_string();
    let deadline = std::time::Instant::now() + utils::READY_TIMEOUT;
    let client = loop {
        match Shoal::<MediaClient>::new(&addr).await {
            Ok(client) => break client,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    // a put past the threshold, sent once, is asked again rather than refused for good
    let posters = client.bucket::<Posters>();
    let expected = bytes(100_000, 11);
    let once = PutOptions::default().retry(Duration::ZERO);
    let error = posters
        .put_with("early.bin", &expected, &once)
        .await
        .expect_err("no object executor runs before `ready`");
    assert_eq!(
        outcome(&error),
        Some(ObjectOutcome::NotReady),
        "a put before the object executors start: {error}"
    );
    // and once they run, the same put is done and reads back
    pool.ready(utils::READY_TIMEOUT)?;
    posters.put("early.bin", &expected).await?;
    let (_, read) = posters.get("early.bin").await?;
    assert!(read == expected, "the put done once the executors run reads back");
    drop(client);
    pool.exit()?;
    Ok(())
}

/// Eight writers make small writes in place on one stripe at once, each in a range of its own and
/// each sent once: every write is done, every read meanwhile and after is what the writes before
/// it left, and once the clears have had their time nothing is left staged
/// ([Resolved #239](../../docs/src/appendix/resolved/record-bases.md))
#[tokio::test(flavor = "multi_thread")]
async fn small_writes_from_eight_writers_on_one_stripe() -> Result<(), TestError> {
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(2))
        .failure_domain(shoal::shared::pools::FailureDomain::Device)
        .small_writes(64 << 10);
    let striped = striped_node_with(2, pool).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let base = bytes(1 << 20, 81);
    posters.put("contended.bin", &base).await?;
    // each write and read sent once: a write that loses its row to another is the server's to try
    // again, never the client's
    let once = striped
        .node
        .client
        .objects(<Posters as BucketRows>::id())
        .retry(Duration::ZERO);
    // each writer owns 128 KiB of the object and writes 4 KiB pieces of it in turn
    let region = 128u64 << 10;
    let writers = (0..8u64).map(|writer| {
        let posters = &once;
        let base = &base;
        async move {
            let mut mine = base[(writer * region) as usize..((writer + 1) * region) as usize].to_vec();
            for round in 0..40u64 {
                let at = ((round * 7 + writer) % 32) * 4096;
                let patch = bytes(4096, (writer * 40 + round) as u8);
                posters
                    .write_at("contended.bin", writer * region + at, &patch)
                    .await
                    .map_err(|error| format!("writer {writer} round {round}: {error:?}"))?;
                mine[at as usize..at as usize + 4096].copy_from_slice(&patch);
                // its own range reads back as it left it, whatever the others are doing
                let (_, read) = posters
                    .read_range("contended.bin", writer * region, region, None)
                    .await
                    .map_err(|error| format!("writer {writer} round {round} read: {error:?}"))?;
                if read != mine {
                    return Err(format!("writer {writer} round {round} read back other bytes"));
                }
            }
            Ok::<Vec<u8>, String>(mine)
        }
    });
    let ranges = futures::future::join_all(writers).await;
    let mut expected = Vec::with_capacity(1 << 20);
    for range in ranges {
        expected.extend_from_slice(&range.unwrap_or_else(|why| panic!("{why}")));
    }
    // once every clear has had its time, the object is what the writers left and nothing is staged
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (_, read) = posters.get("contended.bin").await?;
    assert!(read == expected, "the writers' bytes are the object's");
    let store = striped.node.pool.object_store().expect("a store");
    let staged = store.undecided(Duration::ZERO).await;
    assert!(staged.is_empty(), "records left staged: {staged:?}");
    Ok(())
}

/// A write in place that loses its stripe's commit a hundred times in a row is done once it wins,
/// sent once: a write tries its stripes again for its time, never for a count of rounds, which
/// eight writers on one stripe spent on disks (item 312)
#[tokio::test(flavor = "multi_thread")]
async fn a_write_that_keeps_losing_its_stripe_tries_for_its_time() -> Result<(), TestError> {
    use shoal::server::objects::faults;
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(2))
        .failure_domain(shoal::shared::pools::FailureDomain::Device)
        .small_writes(64 << 10);
    let striped = striped_node_with(2, pool).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let base = bytes(1 << 20, 83);
    posters.put("losing.bin", &base).await?;
    // its next hundred commits refused before they are proposed, as another writer's beat each
    faults::refuse_stripe_commits("losing.bin", 100);
    let once = striped
        .node
        .client
        .objects(<Posters as BucketRows>::id())
        .retry(Duration::ZERO);
    let patch = bytes(4096, 84);
    let written = once.write_at("losing.bin", 8192, &patch).await;
    let left = faults::stripe_commits_to_refuse("losing.bin");
    faults::refuse_stripe_commits("losing.bin", 0);
    if let Err(error) = written {
        panic!("a write that lost {} commits: {error:?}", 100 - left);
    }
    assert_eq!(left, 0, "the write was done without losing every commit it was dealt");
    // and the object is the write over what was there
    let mut expected = base.clone();
    expected[8192..8192 + 4096].copy_from_slice(&patch);
    let (_, read) = posters.get("losing.bin").await?;
    assert!(read == expected, "the write that kept losing reads back");
    Ok(())
}

/// A bucket's stripe rows are walked by tablet, read back as stripe rows, and a placement group's
/// range of its tablet holds exactly the stripes `pg_of` puts there (F87)
///
/// What M16's backfill reads a placement group's stripe rows by
/// ([F87](../../docs/src/features/tablet-walk.md)): every row the walk hands out is the stripe its
/// key says, and the walk of a placement group's range is that group's rows and no other's.
#[tokio::test(flavor = "multi_thread")]
async fn a_walked_stripe_row_decodes_and_its_placement_group_holds_it() -> Result<(), TestError> {
    use shoal::server::shard::walk::WalkAsk;
    use shoal::shared::identity::TableId;
    use shoal::shared::objects::{stripe_key, MarkerOps, ObjectOps};
    use shoal::shared::placement::{tablet_of, TABLET_COUNT};
    use shoal::shared::pool_placement::pg_of;
    // four placement groups a tablet, so a group is a quarter of its tablet's keys
    let groups = 4;
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(1))
        .failure_domain(shoal::shared::pools::FailureDomain::Device)
        .placement_groups(groups);
    let striped = striped_node_with(1, pool).await?;
    let posters = striped.node.client.bucket::<Posters>();
    // three objects of three stripes each, every stripe written in place, which gives it a row
    let mut written = std::collections::BTreeSet::new();
    for name in ["a.bin", "b.bin", "c.bin"] {
        posters.put(name, &bytes((12 << 20) - 5, 3)).await?;
        for stripe in 0..3u64 {
            posters.write_at(name, stripe * (4 << 20) + 1_000, &bytes(5_000, 9)).await?;
        }
        let (info, _) = posters.get(name).await?;
        for stripe in 0..3u64 {
            written.insert((info.object, stripe));
        }
    }
    // the bucket names its stripe table
    let ops = MarkerOps::<Posters>::new();
    let table = TableId::of(<MarkerOps<Posters> as ObjectOps<MediaClient>>::stripe_table(&ops));
    // every tablet walked: each row is the stripe its key names, and every written stripe is there
    let node = &striped.node.pool;
    let mut walked = std::collections::BTreeSet::new();
    for tablet in 0..TABLET_COUNT as u16 {
        let page = node.walk(WalkAsk::tablet(table, tablet))?.expect("a page of a tablet");
        for (key, row) in page.stripes::<Media>()? {
            assert_eq!(stripe_key(row.consumer, row.object, row.stripe), key);
            assert_eq!(tablet_of(key), usize::from(tablet));
            assert!(walked.insert((row.object, row.stripe)), "a stripe walked twice");
            // its placement group's range holds it, and a walk of that range is the group's alone
            let pg = pg_of(key, groups);
            let range = pg.range(groups);
            assert!((range.0..=range.1).contains(&key));
            let group = node.walk(WalkAsk::range(table, tablet, range))?.expect("a page of a group");
            assert!(group.partitions.iter().any(|partition| partition.key == key));
            assert!(group.partitions.iter().all(|partition| pg_of(partition.key, groups) == pg));
        }
    }
    assert_eq!(walked, written);
    Ok(())
}

/// Start a node whose Posters pool is erasure coded over as many devices as its stripe is wide
///
/// # Arguments
///
/// * `data` - The data chunks of a stripe
/// * `parity` - The parity chunks of a stripe
/// * `stripe` - The stripe's bytes, a multiple of `data` whole units
async fn erasure_node(data: u8, parity: u8, stripe: u64) -> Result<StripedNode, TestError> {
    let mut pool = PoolSpec::new("ssd", Redundancy::Erasure { data, parity })
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    pool.stripe = Some(shoal::shared::pools::ByteSize(stripe));
    striped_node_with(usize::from(data) + usize::from(parity), pool).await
}

/// Start a node with a device for each chunk of a `data` + `parity` stripe, which waits two minutes
/// for a holder's answer rather than the default ten seconds
///
/// A holder that answers a write's stage past the query deadline is named missed, which is right,
/// and a standalone node rebuilds nothing, so its chunk keeps the bytes of the write before for
/// good. A test of what parity a write leaves gives a starved holder time to answer: on a loaded
/// host its stage's flush once took longer than the deadline, and the test read a stale chunk.
///
/// # Arguments
///
/// * `data` - The pool's data chunks
/// * `parity` - Its parity chunks
/// * `stripe` - Its stripe's bytes
async fn patient_erasure_node(data: u8, parity: u8, stripe: u64) -> Result<StripedNode, TestError> {
    let mut pool = PoolSpec::new("ssd", Redundancy::Erasure { data, parity })
        .failure_domain(shoal::shared::pools::FailureDomain::Device);
    pool.stripe = Some(shoal::shared::pools::ByteSize(stripe));
    striped_node_tuned(usize::from(data) + usize::from(parity), pool, |mut conf| {
        conf.networking = conf.networking.query_deadline(Duration::from_secs(120));
        conf
    })
    .await
}

/// Each stripe chunk under a node's devices, by object, stripe and position, with its header and
/// the bytes its header says it holds
///
/// # Arguments
///
/// * `devices` - The devices' directory
fn erasure_chunks(
    devices: &std::path::Path,
) -> std::collections::BTreeMap<(u128, u64, u8), (shoal::server::objects::store::chunk::ChunkHeader, Vec<u8>)> {
    use shoal::server::objects::store::chunk::{ChunkHeader, BLOCK};
    let mut found = std::collections::BTreeMap::new();
    for (path, bytes) in chunk_files(devices) {
        // a chunk in place, never a staged one or one set aside
        if bytes.len() < BLOCK || path.components().any(|part| part.as_os_str() == "quarantine") {
            continue;
        }
        let Ok(header) = ChunkHeader::decode(&bytes[..BLOCK]) else {
            continue;
        };
        let held = bytes[BLOCK..BLOCK + header.length as usize].to_vec();
        found.insert((header.id.object, header.id.stripe, header.id.position), (header, held));
    }
    found
}

/// Every stripe of an object holds the parity its data encodes to, on the node's devices
///
/// Each stripe's data chunks are read off the slices with zeros past each one's bytes, encoded
/// afresh, and compared with each parity chunk's bytes; a stripe whose applies are still landing
/// is looked at again, for a while.
///
/// # Arguments
///
/// * `devices` - The devices' directory
/// * `object` - The object
/// * `data` - The data chunks of a stripe
/// * `parity` - The parity chunks of a stripe
async fn parity_matches_data(devices: &std::path::Path, object: u128, data: u8, parity: u8) -> Result<usize, String> {
    let mut last = String::new();
    for _ in 0..100 {
        let chunks = erasure_chunks(devices);
        let stripes: std::collections::BTreeSet<u64> = chunks
            .keys()
            .filter(|(at, _, _)| *at == object)
            .map(|(_, stripe, _)| *stripe)
            .collect();
        let mut checked = 0;
        let mut wrong = Vec::new();
        for stripe in stripes {
            // the parity's length, and each data chunk padded with zeros to it
            let Some((parity_header, _)) = chunks.get(&(object, stripe, data)) else {
                wrong.push(format!("stripe {stripe} has no parity chunk"));
                continue;
            };
            let len = parity_header.length as usize;
            let rows: Vec<Vec<u8>> = (0..data)
                .map(|position| {
                    let mut bytes = chunks.get(&(object, stripe, position)).map(|(_, bytes)| bytes.clone()).unwrap_or_default();
                    bytes.resize(len, 0);
                    bytes
                })
                .collect();
            let mut made = vec![vec![0u8; len]; usize::from(parity)];
            {
                let sources: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();
                let mut outs: Vec<&mut [u8]> = made.iter_mut().map(Vec::as_mut_slice).collect();
                shoal::server::objects::codec::encode_now(data, parity, &sources, &mut outs).map_err(|error| error.to_string())?;
            }
            for (row, expected) in made.iter().enumerate() {
                let held = chunks.get(&(object, stripe, data + row as u8)).map(|(_, bytes)| bytes.clone()).unwrap_or_default();
                if &held != expected {
                    // each position's label and length, so a stale chunk shows beside the others
                    let labels: Vec<String> = (0..data + parity)
                        .map(|position| {
                            chunks.get(&(object, stripe, position)).map_or("-".to_string(), |(header, _)| {
                                format!("{}:{}/{:x}+{}", position, header.label.sequence, header.label.tag, header.length)
                            })
                        })
                        .collect();
                    // and the first byte that differs
                    let first = held.iter().zip(expected).position(|(held, made)| held != made);
                    wrong.push(format!("stripe {stripe} parity {row} from byte {first:?}: [{}]", labels.join(" ")));
                }
            }
            checked += 1;
        }
        if wrong.is_empty() {
            return Ok(checked);
        }
        last = wrong.join(", ");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last)
}

/// An erasure coded object is put as stripes of data and parity, read whole and in ranges, written
/// in place inside one data chunk (a delta at 4+2), across two (reconstruct-write), appended to
/// into a chunk that held nothing, and cut and grown again, and every read is what its writes left
/// and every stripe's parity is what its data encodes to (F90)
#[tokio::test(flavor = "multi_thread")]
async fn an_erasure_coded_object_round_trips() -> Result<(), TestError> {
    for (data, parity) in [(4u8, 2u8), (2, 1)] {
        let striped = erasure_node(data, parity, 4 << 20).await?;
        let posters = striped.node.client.bucket::<Posters>();
        let chunk = (4usize << 20) / usize::from(data);
        // ten megabytes and three bytes: three stripes of 4 MiB, the last short
        let mut expected = bytes((10 << 20) + 3, 7);
        let put = posters.put("big.bin", &expected).await?;
        assert_eq!(put.size, expected.len() as u64);
        let (_, read) = posters.get("big.bin").await?;
        assert!(read == expected, "{data}+{parity}: read back as it was put");
        // a range across a data chunk's end and a stripe's
        for (at, len) in [(chunk - 100, 300), ((4 << 20) - 100, 300), (17, 5)] {
            let (_, range) = posters.read_range("big.bin", at as u64, len, None).await?;
            assert_eq!(range, expected[at..at + len as usize].to_vec(), "{data}+{parity} at {at}");
        }
        // inside one data chunk, across a unit's end
        let patch = bytes(70_000, 99);
        posters.write_at("big.bin", chunk as u64 + 1_000, &patch).await?;
        expected[chunk + 1_000..chunk + 71_000].copy_from_slice(&patch);
        // across two data chunks
        let across = bytes(5_000, 98);
        posters.write_at("big.bin", (2 * chunk - 2_500) as u64, &across).await?;
        expected[2 * chunk - 2_500..2 * chunk + 2_500].copy_from_slice(&across);
        let (_, read) = posters.get("big.bin").await?;
        assert!(read == expected, "{data}+{parity}: writes in place read back where they were written");
        // an append from the short last stripe into a data chunk that held nothing
        let tail = bytes(3 << 20, 3);
        let appended = posters.append("big.bin", &tail).await?;
        expected.extend_from_slice(&tail);
        assert_eq!(appended.size, expected.len() as u64);
        let (_, read) = posters.get("big.bin").await?;
        assert!(read == expected, "{data}+{parity}: an append reads back at the end");
        // every stripe's parity is its data's: a whole stripe's, a delta's, a reconstruct-write's
        // and an append's into a chunk that held nothing
        let checked = match parity_matches_data(striped._devices.path(), put.object, data, parity).await {
            Ok(checked) => checked,
            Err(wrong) => panic!("{data}+{parity}: parity disagrees with data: {wrong}"),
        };
        assert_eq!(checked, 4, "{data}+{parity}: {checked} stripes checked");
        // a cut inside a stripe, then the object grown again: what was cut reads as zeros
        posters.truncate("big.bin", 6_000_000).await?;
        expected.truncate(6_000_000);
        posters.truncate("big.bin", 9_000_000).await?;
        expected.resize(9_000_000, 0);
        posters.write_at("big.bin", 5_000_000, &bytes(10, 5)).await?;
        expected[5_000_000..5_000_010].copy_from_slice(&bytes(10, 5));
        let (info, read) = posters.get("big.bin").await?;
        assert_eq!(info.size, 9_000_000);
        assert!(read == expected, "{data}+{parity}: truncated bytes never return");
        // a chunk the cut left past its stripe's bytes keeps its file until reclamation (M20),
        // and is read as nothing: the rewritten stripe reads back whatever its holders hold
        let (_, middle) = posters.read_range("big.bin", 4 << 20, 2 << 20, None).await?;
        assert_eq!(middle, expected[4 << 20..6 << 20].to_vec(), "{data}+{parity}: the rewritten stripe");
        // a small object: one data chunk and the parity, each its own length
        let small = bytes(20_000, 4);
        let put = posters.put("small.bin", &small).await?;
        let (_, read) = posters.get("small.bin").await?;
        assert!(read == small, "{data}+{parity}: a short stripe reads back");
        let chunks = erasure_chunks(striped._devices.path());
        let mine: Vec<(u8, u64)> = chunks
            .iter()
            .filter(|((object, _, _), _)| *object == put.object)
            .map(|((_, _, position), (header, _))| (*position, header.length))
            .collect();
        let mut expected_chunks: Vec<(u8, u64)> = vec![(0, 20_000)];
        expected_chunks.extend((data..data + parity).map(|position| (position, 20_000)));
        assert_eq!(mine, expected_chunks, "{data}+{parity}: a short stripe stores one data chunk and its parity");
    }
    Ok(())
}

/// S8's `short_stripe_stores_no_padding`. A 4+2 stripe shorter than a unit stores one data chunk
/// and its two parity chunks, each of the stripe's own length, and nothing for the three data
/// chunks it does not reach; a stripe of a chunk and a half stores two data chunks, the second half
/// as long, and parity as long as the first (F90)
#[tokio::test(flavor = "multi_thread")]
async fn short_stripe_stores_no_padding() -> Result<(), TestError> {
    let striped = erasure_node(4, 2, 4 << 20).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let mib = 1u64 << 20;
    let (block, unit) = (shoal::server::objects::store::chunk::BLOCK as u64, 64u64 << 10);
    for (path, length, held, files) in [
        (
            "short.bin",
            20_000u64,
            vec![(0u8, 20_000u64), (4, 20_000), (5, 20_000)],
            vec![(0u8, block + unit), (4, block + unit), (5, block + unit)],
        ),
        (
            "half.bin",
            mib + mib / 2,
            vec![(0, mib), (1, mib / 2), (4, mib), (5, mib)],
            vec![(0, block + mib), (1, block + mib / 2), (4, block + mib), (5, block + mib)],
        ),
    ] {
        let bytes = bytes(length as usize, 21);
        let put = posters.put(path, &bytes).await?;
        let (_, read) = posters.get(path).await?;
        assert!(read == bytes, "{path} read back as something else");
        // the positions that hold bytes, each its own length, and no other
        let stored: Vec<(u8, u64)> = erasure_chunks(striped._devices.path())
            .iter()
            .filter(|((object, _, _), _)| *object == put.object)
            .map(|((_, _, position), (header, _))| (*position, header.length))
            .collect();
        assert_eq!(stored, held, "{path} stored padding");
        // and each file the class of units its chunk holds, not the whole chunk (F96)
        let mut sizes: Vec<(u8, u64)> = chunk_files(striped._devices.path())
            .into_iter()
            .filter(|(path, _)| !path.components().any(|part| part.as_os_str() == "quarantine"))
            .filter_map(|(_, bytes)| {
                let header = shoal::server::objects::store::chunk::ChunkHeader::decode(&bytes[..block as usize]).ok()?;
                (header.id.object == put.object).then_some((header.id.position, bytes.len() as u64))
            })
            .collect();
        sizes.sort_unstable();
        assert_eq!(sizes, files, "{path}'s files are not the classes of what they hold");
    }
    Ok(())
}

/// Grow a short object by appends, each taking its chunk past its file's class or keeping it
/// inside, and read it back after every one, then write in place across its first unit's end
///
/// # Arguments
///
/// * `striped` - A node whose bucket's pool the object is written to
async fn grow_by_appends(striped: &StripedNode) -> Result<(), TestError> {
    let unit = 64usize << 10;
    let posters = striped.node.client.bucket::<Posters>();
    // just short of a unit, past the inline threshold, then appends of a quarter unit and a little
    // up to past four units, crossing the first, second and fourth
    let mut expected = bytes(unit - 1000, 31);
    let put = posters.put("grown.bin", &expected).await?;
    let mut seed = 32u8;
    while expected.len() < 5 * unit {
        let tail = bytes(unit / 4 + 100, seed);
        seed = seed.wrapping_add(1);
        posters.append("grown.bin", &tail).await?;
        expected.extend_from_slice(&tail);
        let (info, read) = posters.get("grown.bin").await?;
        assert_eq!(info.size, expected.len() as u64);
        assert!(read == expected, "an append to {} bytes read back as something else", expected.len());
    }
    // and a write in place across the first unit's end, inside the class the chunk reached
    let patch = bytes(3000, 77);
    posters.write_at("grown.bin", (unit - 1500) as u64, &patch).await?;
    expected[unit - 1500..unit + 1500].copy_from_slice(&patch);
    let (_, read) = posters.get("grown.bin").await?;
    assert!(read == expected, "a write in place after the appends read back as something else");
    // every chunk the object holds is a file of its class, short of the whole chunk (F96)
    let block = shoal::server::objects::store::chunk::BLOCK;
    let mut files = 0;
    for (path, bytes) in chunk_files(striped._devices.path()) {
        let Ok(header) = shoal::server::objects::store::chunk::ChunkHeader::decode(&bytes[..block]) else {
            continue;
        };
        if header.id.object != put.object || path.components().any(|part| part.as_os_str() == "quarantine") {
            continue;
        }
        let units = u32::try_from(header.chunk_bytes / u64::from(header.unit_bytes)).expect("units");
        let class = shoal::shared::objects::class_units(header.length, header.unit_bytes, units);
        assert_eq!(bytes.len() as u64, block as u64 + u64::from(class) * u64::from(header.unit_bytes), "{}", path.display());
        assert!((bytes.len() as u64) < block as u64 + header.chunk_bytes, "{} is the whole chunk", path.display());
        files += 1;
    }
    assert!(files > 0, "no chunk of the grown object was found");
    Ok(())
}

/// F96's `a_small_object_grown_by_appends_climbs_its_classes`. A short object grown by appends,
/// each taking its chunk past its file's class or keeping it inside, reads back as written after
/// every one, on copies whose small writes ride their commits: a write past a chunk's class is
/// staged whole, the bytes it does not carry read first, and never rides its commit (F96)
#[tokio::test(flavor = "multi_thread")]
async fn a_small_object_grown_by_appends_climbs_its_classes() -> Result<(), TestError> {
    // two copies whose small writes ride their commits
    let copies = PoolSpec::new("ssd", Redundancy::Replicas(2))
        .failure_domain(shoal::shared::pools::FailureDomain::Device)
        .small_writes(64 << 10);
    let striped = striped_node_with(2, copies).await?;
    grow_by_appends(&striped).await
}

/// F96's `an_erasure_coded_object_grown_by_appends_climbs_its_classes`. The same appends on 4+2:
/// a write that takes a chunk, and so its parity, past its class rewrites the stripe whole (F96)
#[tokio::test(flavor = "multi_thread")]
async fn an_erasure_coded_object_grown_by_appends_climbs_its_classes() -> Result<(), TestError> {
    let striped = erasure_node(4, 2, 4 << 20).await?;
    grow_by_appends(&striped).await
}

/// S9's `range_read_touches_only_the_chunks_it_needs`. A range inside one data chunk of a healthy
/// 4+2 stripe is read from that chunk's slice alone, by exactly its length, and decodes nothing
/// (F90)
#[tokio::test(flavor = "multi_thread")]
async fn range_read_touches_only_the_chunks_it_needs() -> Result<(), TestError> {
    let striped = erasure_node(4, 2, 4 << 20).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let expected = bytes(4 << 20, 22);
    posters.put("ranged.bin", &expected).await?;
    let store = striped.node.pool.object_store().expect("the node runs object executors").clone();
    let before = store.read_bytes_by_slice();
    let decoded = store.codec_figures().decoded_bytes;
    // a range inside data chunk two, of no whole unit
    let (at, len) = ((2usize << 20) + 1_000, 50_000u64);
    let (_, range) = posters.read_range("ranged.bin", at as u64, len, None).await?;
    assert_eq!(range, expected[at..at + len as usize].to_vec());
    let after = store.read_bytes_by_slice();
    let moved: Vec<u64> = after
        .iter()
        .map(|(slice, bytes)| bytes - before.get(slice).copied().unwrap_or(0))
        .filter(|moved| *moved > 0)
        .collect();
    assert_eq!(moved, vec![len], "the range was read from {moved:?}");
    assert_eq!(store.codec_figures().decoded_bytes, decoded, "a healthy range was decoded");
    Ok(())
}

/// S9's `unreadable_stripe_fails_alone`. With three chunks of one 4+2 stripe gone, fewer than `k`
/// remain: a range that needs one of them fails, naming the stripe, while ranges of the object's
/// other stripes read as they were put, and so does a range of that stripe a present data chunk
/// holds (F90)
#[tokio::test(flavor = "multi_thread")]
async fn unreadable_stripe_fails_alone() -> Result<(), TestError> {
    let striped = erasure_node(4, 2, 4 << 20).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let expected = bytes(12 << 20, 23);
    let put = posters.put("lost.bin", &expected).await?;
    // stripe one's first two data chunks and its first parity chunk removed from their slices
    let chunks = chunk_files(striped._devices.path());
    let object = format!("{:032x}", put.object);
    for position in [0u8, 1, 4] {
        let name = format!("{:016x}.{position}", 1u64);
        let path = chunks
            .keys()
            .find(|path| {
                path.parent().and_then(|parent| parent.file_name()).is_some_and(|parent| parent == object.as_str())
                    && path.file_name().is_some_and(|file| file == name.as_str())
            })
            .expect("the chunk is on a slice")
            .clone();
        std::fs::remove_file(&path)?;
    }
    // a range in stripe one's first data chunk fails, by the stripe's name
    let failed = posters.read_range("lost.bin", (4 << 20) + 100, 1_000, None).await;
    let why = format!("{failed:?}");
    assert!(failed.is_err(), "a range with fewer than k chunks read: {why}");
    assert!(why.contains("stripe 1"), "the failure does not name the stripe: {why}");
    // every other stripe reads, and stripe one's third data chunk too
    for (at, len) in [(100usize, 1_000u64), ((8 << 20) + 100, 1_000), ((4 << 20) + (2 << 20) + 100, 1_000)] {
        let (_, range) = posters.read_range("lost.bin", at as u64, len, None).await?;
        assert_eq!(range, expected[at..at + len as usize].to_vec(), "a range at {at}");
    }
    Ok(())
}

/// S8's `partial_overwrite_equals_a_fresh_encode`. After a sequence of writes in place of every size
/// and place - inside one data chunk, across two, across a stripe's end, and past the object's end -
/// at 4+2 (by parity delta and by reconstruct-write) and at 3+1 (XOR), every stripe's parity on its
/// slices is what encoding its data chunks afresh gives, and every read is the writes laid over the
/// put (F90)
#[tokio::test(flavor = "multi_thread")]
async fn partial_overwrite_equals_a_fresh_encode() -> Result<(), TestError> {
    for (data, parity, stripe) in [(4u8, 2u8, 4u64 << 20), (3, 1, 3 << 20)] {
        let striped = patient_erasure_node(data, parity, stripe).await?;
        let posters = striped.node.client.bucket::<Posters>();
        let mut expected = bytes((2 * stripe as usize) + 12_345, 24);
        let put = posters.put("written.bin", &expected).await?;
        // a seeded walk of offsets and lengths
        let mut state = 0x5eed_u64 ^ u64::from(data);
        let mut next = |bound: u64| {
            state = shoal::shared::rendezvous::mix64(state.wrapping_add(0x9e37_79b9_7f4a_7c15));
            state % bound
        };
        for round in 0..24u64 {
            let len = 1 + next(200_000);
            let at = next(expected.len() as u64 + 50_000);
            let patch = bytes(len as usize, (round as u8).wrapping_add(30));
            posters.write_at("written.bin", at, &patch).await?;
            let end = (at + len) as usize;
            if expected.len() < end {
                expected.resize(end, 0);
            }
            expected[at as usize..end].copy_from_slice(&patch);
            if round % 6 == 5 {
                let (_, read) = posters.get("written.bin").await?;
                assert!(read == expected, "{data}+{parity}: round {round} read back as something else");
            }
        }
        let (_, read) = posters.get("written.bin").await?;
        assert!(read == expected, "{data}+{parity}: the writes read back as something else");
        match parity_matches_data(striped._devices.path(), put.object, data, parity).await {
            Ok(checked) => assert!(checked >= 2, "{data}+{parity}: {checked} stripes checked"),
            Err(wrong) => panic!("{data}+{parity}: parity disagrees with a fresh encode: {wrong}"),
        }
    }
    Ok(())
}

/// S8's `parity_stage_refuses_a_base_it_does_not_hold`. A 4+2 parity chunk put back to an older
/// state than its row names refuses a write's change, which expects the row's: the write is
/// acknowledged by the five positions that hold it, the refusing one marked missed, and reads
/// back. With the other parity chunk put back too, a write would leave four, and is refused, and
/// nothing of it is read (F90)
#[tokio::test(flavor = "multi_thread")]
async fn parity_stage_refuses_a_base_it_does_not_hold() -> Result<(), TestError> {
    let striped = erasure_node(4, 2, 4 << 20).await?;
    let posters = striped.node.client.bucket::<Posters>();
    let mut expected = bytes(4 << 20, 25);
    let put = posters.put("based.bin", &expected).await?;
    let chunk_path = |position: u8| -> std::path::PathBuf {
        let object = format!("{:032x}", put.object);
        let name = format!("{:016x}.{position}", 0u64);
        chunk_files(striped._devices.path())
            .into_keys()
            .find(|path| {
                path.components().all(|part| part.as_os_str() != "quarantine")
                    && path.parent().and_then(|parent| parent.file_name()).is_some_and(|parent| parent == object.as_str())
                    && path.file_name().is_some_and(|file| file == name.as_str())
            })
            .expect("the chunk is on a slice")
    };
    // each parity chunk as the put left it
    let put_state: Vec<(std::path::PathBuf, Vec<u8>)> = [4u8, 5]
        .into_iter()
        .map(|position| {
            let path = chunk_path(position);
            let bytes = std::fs::read(&path).expect("a parity chunk");
            (path, bytes)
        })
        .collect();
    // a write into data chunk one moves both parity chunks past the put
    let first = bytes(4_096, 26);
    posters.write_at("based.bin", (1 << 20) + 10, &first).await?;
    expected[(1 << 20) + 10..(1 << 20) + 4_106].copy_from_slice(&first);
    let settled = parity_matches_data(striped._devices.path(), put.object, 4, 2).await;
    assert!(settled.is_ok(), "{settled:?}");
    // parity chunk zero put back to the put's state: it holds a base the row no longer names
    std::fs::write(&put_state[0].0, &put_state[0].1)?;
    let second = bytes(4_096, 27);
    posters.write_at("based.bin", (1 << 20) + 50_000, &second).await?;
    expected[(1 << 20) + 50_000..(1 << 20) + 54_096].copy_from_slice(&second);
    let (_, read) = posters.get("based.bin").await?;
    assert!(read == expected, "a write acknowledged by k + f did not read back");
    // parity chunk one put back too: a write would leave four of six
    std::fs::write(&put_state[1].0, &put_state[1].1)?;
    let refused = posters.write_at("based.bin", (2 << 20) + 10, &bytes(4_096, 28)).await;
    assert!(refused.is_err(), "a write leaving four of six was acknowledged: {refused:?}");
    let (_, read) = posters.get("based.bin").await?;
    assert!(read == expected, "a refused write's bytes were read");
    Ok(())
}
