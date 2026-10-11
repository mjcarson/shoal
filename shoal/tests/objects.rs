//! Objects in a bucket, end to end through a node ([F80](../../docs/src/features/object-wire.md))
//!
//! M13's acceptance rows that a standalone node can show: the object frames and their capability
//! ([S12](../../docs/src/object-storage/wire-and-client.md#acceptance-tests)), path identity and
//! inline objects ([S3](../../docs/src/object-storage/objects.md#acceptance-tests)), and a bucket
//! bound to nothing refused at start ([S4](../../docs/src/object-storage/pools-and-devices.md#acceptance-tests)).
//! The committed half of S4's policy rows, a joiner's copy ignored, is the cluster fixture's.

// a database struct and a bucket's marker are named as types and never built
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use shoal::client::{Errors, PoolConfig, PutOptions, Routing, Shoal, StreamConfig};
use shoal::server::conf::cluster::Cluster;
use shoal::server::ShoalPool;
use shoal::shared::checksum;
use shoal::shared::objects::{object_id, Attributes, BucketRows};
use shoal::shared::pools::{PoolSpec, Redundancy};
use shoal::shared::protocol::auth::AuthMechanisms;
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
    // one byte past it is refused, naming the threshold and that no pool has a device
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
