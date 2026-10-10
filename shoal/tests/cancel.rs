//! Integration tests for a client cancelling a bundle it will never read
//! ([F75](../../docs/src/features/client-cancel.md))
//!
//! A cancel names a bundle and applies to every arrival of it on its connection before it. The
//! server answers it with one `Error` frame of code `Cancelled`, writes nothing more of what the
//! cancelled arrivals owe, cuts a streamed answer it had begun, and answers `Cancelled` instead of
//! running any of their reads not yet run; a write still runs, and only its answer is dropped. Most of these speak the frames by hand on a raw
//! socket, so the order of frames on the wire is what is asserted; the last two drive the client,
//! which sends a cancel when a result stream ends before its answers are all in.
//!
//! The work a cancel stops is made deterministic by holding the shard: a held shard's loop sleeps
//! while its relays go on reading, so the bundle and the cancel both wait on its queue, and the
//! cancel is recorded before the queries it covers are dequeued.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{SendOptions, Shoal};
use shoal::server::conf::Networking;
use shoal::shared::protocol::auth::AuthMechanisms;
use shoal::shared::protocol::cancel::{self, CLIENT_CAP_CANCEL};
use shoal::shared::protocol::error::{self as proto_error, ErrorCode};
use shoal::shared::protocol::stats::CancelCounters;
use shoal::shared::protocol::stream::{self, CLIENT_CAPS};
use shoal::shared::protocol::{self, handshake, Flags, MessageType};
use shoal::shared::queries::Queries;
use shoal::shared::traits::QuerySupport;
use shoal::tables::EphemeralSortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

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
    /// The payload, as long as a test needs a row to be
    #[shoal(update)]
    pub data: String,
}

/// The schema these tests run against
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub test_records: EphemeralSortedTable<TestRecord>,
}

/// How many gets the bundles a held shard is cancelled under carry
const GETS: usize = 32;

/// Start a server under a networking block on some number of shards
///
/// # Arguments
///
/// * `temp_dir` - Where its storage goes
/// * `cores` - How many shards it runs
/// * `networking` - Its networking block, the port left to the pool
async fn start(
    temp_dir: &tempfile::TempDir,
    cores: usize,
    networking: Networking,
) -> Result<(ShoalPool<TestDb>, String), TestError> {
    let conf = utils::build_config(temp_dir)
        .resources(
            shoal::server::conf::Resources::default()
                .cores(cores)
                .memory("512MiB")
                .expect("a memory bound parses"),
        )
        .networking(networking.port(0));
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    Ok((pool, addr.to_string()))
}

/// Write one small row in each of the partitions the held tests read
///
/// # Arguments
///
/// * `addr` - The server
async fn small_rows(addr: &str) -> Result<(), TestError> {
    let writer = Shoal::<TestDbClient>::new(addr).await?;
    for n in 0..GETS {
        writer
            .send_one(TestRecord {
                partition_key: format!("k{n:02}"),
                sort_key: "0".to_owned(),
                data: format!("row {n}"),
            })
            .await?;
    }
    writer
        .send_one(TestRecord {
            partition_key: "probe".to_owned(),
            sort_key: "0".to_owned(),
            data: "probe".to_owned(),
        })
        .await?;
    Ok(())
}

/// Open a raw connection asking for some capabilities, and return it with what was granted
///
/// # Arguments
///
/// * `addr` - The server
/// * `caps` - The capabilities to ask for
/// * `bufs` - The socket buffers to give the connection, if a test needs them small
async fn raw(
    addr: &str,
    caps: u8,
    bufs: Option<u32>,
) -> Result<(TcpStream, handshake::HelloAck), TestError> {
    // small buffers have to be set before the connection opens to stick
    let socket = tokio::net::TcpSocket::new_v4()?;
    if let Some(bytes) = bufs {
        socket.set_recv_buffer_size(bytes)?;
        socket.set_send_buffer_size(bytes)?;
    }
    let mut sock = socket
        .connect(addr.parse().expect("the address parses"))
        .await?;
    sock.set_nodelay(true)?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
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

/// A bundle of queries under an id, framed whole as the client would send it
///
/// # Arguments
///
/// * `id` - The bundle's id
/// * `queries` - Its queries
fn framed(id: Uuid, queries: Vec<<TestDbClient as QuerySupport>::QueryKinds>) -> Vec<u8> {
    let mut bundle = Queries::<TestDbClient>::default();
    bundle.id = id;
    for query in queries {
        bundle = bundle.add(query);
    }
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(&bundle).expect("a bundle archives");
    let mut frame = protocol::request_preamble(body.len(), protocol::DEFAULT_MAX_FRAME_BYTES)
        .expect("a request preamble")
        .to_vec();
    frame.extend_from_slice(&body);
    frame
}

/// A get of each of the small partitions, one query apiece
fn gets() -> Vec<<TestDbClient as QuerySupport>::QueryKinds> {
    (0..GETS)
        .map(|n| TestRecordGet::new(vec![format!("k{n:02}")]).into())
        .collect()
}

/// The cancel of a bundle, as a client writes it
///
/// # Arguments
///
/// * `id` - The bundle
fn cancel_of(id: Uuid) -> [u8; cancel::CANCEL_FRAME_LEN] {
    cancel::cancel_frame(&id, protocol::DEFAULT_MAX_FRAME_BYTES).expect("a cancel frames")
}

/// One frame a server wrote, as much as a test needs of it
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    /// Its type
    kind: MessageType,
    /// Its flags
    flags: Flags,
    /// The id it carried
    id: Uuid,
    /// An error frame's code
    code: Option<ErrorCode>,
}

impl Seen {
    /// Whether this is the acknowledgement of a cancel of a bundle
    ///
    /// # Arguments
    ///
    /// * `id` - The bundle
    fn acknowledges(&self, id: Uuid) -> bool {
        self.kind == MessageType::Error && self.id == id && self.code == Some(ErrorCode::Cancelled)
    }
}

/// Read one frame a server wrote, keeping what a test needs and dropping the rest
///
/// # Arguments
///
/// * `sock` - The connection
async fn read_seen(sock: &mut TcpStream) -> Result<Seen, TestError> {
    let mut preamble = [0u8; protocol::RESPONSE_PREAMBLE_LEN];
    sock.read_exact(&mut preamble).await?;
    let frame = protocol::decode_server_frame(&preamble, protocol::DEFAULT_MAX_FRAME_BYTES)?;
    let mut rest = vec![0u8; frame.rest_len];
    sock.read_exact(&mut rest).await?;
    let code = (frame.header.kind == MessageType::Error)
        .then(|| {
            proto_error::decode_error_tail(&rest)
                .map(|(code, _)| code)
                .ok()
        })
        .flatten();
    Ok(Seen {
        kind: frame.header.kind,
        flags: frame.header.flags,
        id: frame.query_id,
        code,
    })
}

/// Read frames until one matches, within a deadline, returning every frame read
///
/// # Arguments
///
/// * `sock` - The connection
/// * `until` - The frame that ends the read
async fn read_until(
    sock: &mut TcpStream,
    until: impl Fn(&Seen) -> bool,
) -> Result<Vec<Seen>, TestError> {
    let read = async {
        let mut seen = Vec::new();
        loop {
            let frame = read_seen(sock).await?;
            let done = until(&frame);
            seen.push(frame);
            if done {
                return Ok::<_, TestError>(seen);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), read)
        .await
        .expect("the frame never came")
}

/// Ask for the probe partition and read up to its answer, which has to be the next frame
///
/// What proves nothing more of a cancelled bundle came after its acknowledgement: the probe's
/// answer is written after anything the server still had of it.
///
/// # Arguments
///
/// * `sock` - The connection
async fn probe(sock: &mut TcpStream) -> Result<(), TestError> {
    let id = Uuid::now_v7();
    let probe = framed(
        id,
        vec![TestRecordGet::new(vec!["probe".to_owned()]).into()],
    );
    sock.write_all(&probe).await?;
    let next = read_seen(sock).await?;
    assert_eq!(
        (next.kind, next.id),
        (MessageType::Response, id),
        "a frame came after a cancel's acknowledgement: {next:?}"
    );
    Ok(())
}

/// What the server's shards counted of its clients' cancels
///
/// # Arguments
///
/// * `pool` - The server
fn cancels(pool: &ShoalPool<TestDb>) -> Result<CancelCounters, TestError> {
    Ok(pool.replication()?.cancels)
}

/// Wait for the cancel counters to say something, within a deadline
///
/// # Arguments
///
/// * `pool` - The server
/// * `until` - What they have to say
async fn wait_for(
    pool: &ShoalPool<TestDb>,
    until: impl Fn(&CancelCounters) -> bool,
) -> Result<CancelCounters, TestError> {
    let started = Instant::now();
    loop {
        let counted = cancels(pool)?;
        if until(&counted) || started.elapsed() > Duration::from_secs(10) {
            return Ok(counted);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A bundle cancelled while its shard is held is refused before it runs, and only acknowledged
///
/// One shard, held: a bundle of thirty-two gets and its cancel are written together, and both wait
/// on the shard's queue. Once the hold ends the cancel is recorded before any of the gets is
/// dequeued, so each is answered `Cancelled` instead of run, and the relay writes none of those
/// answers - the connection sees one frame, the acknowledgement. Against the tree before F75 the
/// cancel ends the connection.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_bundle_is_refused_before_it_runs() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    small_rows(&addr).await?;
    // a client that asks for cancels is granted them
    let (mut sock, ack) = raw(&addr, CLIENT_CAPS, None).await?;
    assert_ne!(
        ack.caps & CLIENT_CAP_CANCEL,
        0,
        "the server granted no cancels"
    );
    // hold the shard, then write the bundle and its cancel in one go
    pool.hold_shard(0, 1_000)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let id = Uuid::now_v7();
    let mut frames = framed(id, gets());
    frames.extend_from_slice(&cancel_of(id));
    sock.write_all(&frames).await?;
    // the first frame back is the acknowledgement, and nothing of the bundle comes after it
    let first = read_seen(&mut sock).await?;
    assert!(first.acknowledges(id), "the first frame was {first:?}");
    probe(&mut sock).await?;
    // every get was refused rather than run, and every refusal was left unwritten
    let counted = cancels(&pool)?;
    assert_eq!(counted.received, 1);
    assert_eq!(counted.refused, GETS as u64, "{counted:?}");
    assert_eq!(counted.dropped, GETS as u64, "{counted:?}");
    assert_eq!(counted.unrecorded, 0);
    Ok(())
}

/// A client that reads every answer it asks for sends no cancel at all
///
/// A stream that ended because its answers were all in has nothing to cancel, so the cancel
/// bookkeeping stays off the path every ordinary query takes: one at a time, a bundle read to its
/// end, a bundle split into runs, a query stream closed and drained, and writes.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_reads_every_answer_cancels_nothing() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 2, Networking::default()).await?;
    small_rows(&addr).await?;
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    // one at a time, reads and writes
    for n in 0..GETS {
        client
            .send_one(TestRecordGet::new(vec![format!("k{n:02}")]))
            .await?;
        client
            .send_one(TestRecord {
                partition_key: format!("w{n:02}"),
                sort_key: "0".to_owned(),
                data: "w".to_owned(),
            })
            .await?;
    }
    // a bundle of every get, read to its end
    let mut queries = client.query();
    for n in 0..GETS {
        queries = queries.add(TestRecordGet::new(vec![format!("k{n:02}")]));
    }
    let answers = client.exec(queries).await?;
    assert_eq!(answers.len(), GETS);
    // a query stream, closed and drained
    let (mut queries_tx, mut results_rx) = client.stream()?;
    for n in 0..4 {
        let queries = queries_tx
            .query()
            .add(TestRecordGet::new(vec![format!("k{n:02}")]));
        queries_tx.send(queries).await?;
    }
    queries_tx.close().await?;
    let mut streamed = 0;
    while results_rx.next().await?.is_some() {
        streamed += 1;
    }
    assert_eq!(streamed, 4);
    // and the server heard no cancel
    tokio::time::sleep(Duration::from_millis(200)).await;
    let counted = cancels(&pool)?;
    assert_eq!(counted, CancelCounters::default(), "{counted:?}");
    Ok(())
}

/// A write cancelled before it runs is still applied, and only its answer is dropped
///
/// What a write does is what its sender wanted, and a sender that stopped waiting - a timeout
/// around a send, a stream dropped unread - has not taken it back. Refusing it would lose a write
/// the caller sent, which is what the fixture's paused-server test caught when F75 first did.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_write_is_still_applied() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    small_rows(&addr).await?;
    let (mut sock, _) = raw(&addr, CLIENT_CAPS, None).await?;
    // a write and its cancel, while the shard is held
    pool.hold_shard(0, 500)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let id = Uuid::now_v7();
    let write = TestRecord {
        partition_key: "written".to_owned(),
        sort_key: "0".to_owned(),
        data: "kept".to_owned(),
    };
    let mut frames = framed(id, vec![write.into()]);
    frames.extend_from_slice(&cancel_of(id));
    sock.write_all(&frames).await?;
    // only the acknowledgement comes back
    let first = read_seen(&mut sock).await?;
    assert!(first.acknowledges(id), "the first frame was {first:?}");
    probe(&mut sock).await?;
    // the write landed, and its answer was the one thing dropped
    let reader = Shoal::<TestDbClient>::new(&addr).await?;
    let found = reader
        .send_one(TestRecordGet::new(vec!["written".to_owned()]))
        .await?;
    assert!(found
        .access::<TestRecord>()?
        .is_some_and(|rows| rows.len() == 1));
    let counted = cancels(&pool)?;
    assert_eq!((counted.refused, counted.dropped), (0, 1), "{counted:?}");
    Ok(())
}

/// A bundle sent again under its id after a cancel is answered in full
///
/// A cancel covers only the arrivals before it on its connection: the same id sent after it is a
/// retry, and is run. The acknowledgement comes first, then the retry's answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_retry_after_a_cancel_is_answered() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    small_rows(&addr).await?;
    let (mut sock, _) = raw(&addr, CLIENT_CAPS, None).await?;
    // the bundle, its cancel, and the bundle again under the same id, while the shard is held
    pool.hold_shard(0, 500)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let id = Uuid::now_v7();
    let get = || vec![TestRecordGet::new(vec!["k03".to_owned()]).into()];
    let mut frames = framed(id, get());
    frames.extend_from_slice(&cancel_of(id));
    frames.extend_from_slice(&framed(id, get()));
    sock.write_all(&frames).await?;
    // the acknowledgement, then the retry's answer, and nothing else of the id
    let first = read_seen(&mut sock).await?;
    assert!(first.acknowledges(id), "the first frame was {first:?}");
    let second = read_seen(&mut sock).await?;
    assert_eq!(
        (second.kind, second.id),
        (MessageType::Response, id),
        "{second:?}"
    );
    probe(&mut sock).await?;
    // the first arrival was refused and the retry was not
    let counted = cancels(&pool)?;
    assert_eq!(counted.refused, 1, "{counted:?}");
    Ok(())
}

/// A cancel of a streamed answer stops it between two of its frames
///
/// Sixteen rows of a mebibyte answer one get as a stream of 64 KiB frames, into a socket whose
/// buffers are small, so the server's writer is stopped part way through. A cancel then cuts the
/// stream: no frame of it carries `LAST`, the acknowledgement follows its last data frame, and
/// what it had left was never written.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_cuts_a_stream_being_written() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let networking = Networking::default().stream_frame_bytes(64 << 10);
    let (pool, addr) = start(&temp_dir, 2, networking).await?;
    small_rows(&addr).await?;
    let writer = Shoal::<TestDbClient>::new(&addr).await?;
    for n in 0..16u8 {
        writer
            .send_one(TestRecord {
                partition_key: "large".to_owned(),
                sort_key: format!("{n:02}"),
                data: char::from(b'a' + n).to_string().repeat(1 << 20),
            })
            .await?;
    }
    let (mut sock, ack) = raw(&addr, CLIENT_CAPS, Some(64 << 10)).await?;
    assert_ne!(ack.caps & stream::CLIENT_CAP_STREAMS, 0);
    // ask for the large partition, and let its answer back up behind the socket
    let id = Uuid::now_v7();
    sock.write_all(&framed(
        id,
        vec![TestRecordGet::new(vec!["large".to_owned()]).into()],
    ))
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // read the opener and a couple of data frames, so the stream is begun
    let begun = read_until(&mut sock, |frame| frame.kind == MessageType::Data).await?;
    assert!(
        begun
            .iter()
            .any(|frame| frame.id == id && frame.flags.contains(Flags::STREAMED)),
        "the answer was not streamed: {begun:?}"
    );
    // cancel it, and read until the acknowledgement
    sock.write_all(&cancel_of(id)).await?;
    let rest = read_until(&mut sock, |frame| frame.acknowledges(id)).await?;
    // the stream never reached its last frame, and stopped well short of sixteen mebibytes
    let data: Vec<&Seen> = begun
        .iter()
        .chain(rest.iter())
        .filter(|frame| frame.kind == MessageType::Data && frame.id == id)
        .collect();
    assert!(
        data.iter().all(|frame| !frame.flags.contains(Flags::LAST)),
        "the cancelled stream was written to its end"
    );
    assert!(data.len() < 200, "{} data frames were written", data.len());
    probe(&mut sock).await?;
    // and the server says it cut one stream short, with most of its bytes unwritten
    let counted = cancels(&pool)?;
    assert_eq!(counted.cut, 1, "{counted:?}");
    assert!(counted.dropped_bytes > 8 << 20, "{counted:?}");
    Ok(())
}

/// A cancel is read while its connection owes more answers than its bound
///
/// A connection that stops reading stops being read: the read relay waits for its answers to drain
/// before it reads another bundle. A cancel is read anyway, since what the connection owes is what
/// a cancel takes back. Sixty-four answers of a quarter mebibyte are owed past a bound of eight;
/// the cancel is recorded before the client reads a byte, and once it reads, fewer than sixty-four
/// answers come before the acknowledgement.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_is_read_while_answers_are_owed() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let networking = Networking::default().max_queued_replies(8);
    let (pool, addr) = start(&temp_dir, 1, networking).await?;
    small_rows(&addr).await?;
    let writer = Shoal::<TestDbClient>::new(&addr).await?;
    writer
        .send_one(TestRecord {
            partition_key: "wide".to_owned(),
            sort_key: "0".to_owned(),
            data: "w".repeat(256 << 10),
        })
        .await?;
    let (mut sock, _) = raw(&addr, CLIENT_CAPS, Some(64 << 10)).await?;
    // sixty-four gets of the wide row in one bundle, then nothing read for a while
    let id = Uuid::now_v7();
    let wide = (0..64)
        .map(|_| TestRecordGet::new(vec!["wide".to_owned()]).into())
        .collect();
    sock.write_all(&framed(id, wide)).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // the cancel is read and recorded while the client still reads nothing
    sock.write_all(&cancel_of(id)).await?;
    let counted = wait_for(&pool, |counted| counted.received == 1).await?;
    assert_eq!(counted.received, 1, "the cancel was not read: {counted:?}");
    // now read: some answers were already on their way, the rest were taken back
    let seen = read_until(&mut sock, |frame| frame.acknowledges(id)).await?;
    let answers = seen
        .iter()
        .filter(|frame| frame.kind == MessageType::Response && frame.id == id)
        .count();
    assert!(answers < 64, "every answer was written: {answers}");
    probe(&mut sock).await?;
    let counted = cancels(&pool)?;
    assert_eq!(counted.dropped, 64 - answers as u64, "{counted:?}");
    Ok(())
}

/// A connection not granted cancels is ended by one, as every connection was before F75
#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_without_the_capability_ends_the_connection() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    // a client that asks for streams and not cancels is granted no cancels
    let (mut sock, ack) = raw(&addr, stream::CLIENT_CAP_STREAMS, None).await?;
    assert_eq!(ack.caps & CLIENT_CAP_CANCEL, 0);
    // and a cancel from it ends the connection
    sock.write_all(&cancel_of(Uuid::now_v7())).await?;
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut byte))
        .await
        .expect("the connection was not ended");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "the connection went on: {read:?}"
    );
    Ok(())
}

/// Every cancel is answered once, whether its bundle is long answered or was never sent
#[tokio::test(flavor = "multi_thread")]
async fn every_cancel_is_answered_once() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    small_rows(&addr).await?;
    let (mut sock, _) = raw(&addr, CLIENT_CAPS, None).await?;
    // a bundle answered in full, then cancelled
    let answered = Uuid::now_v7();
    sock.write_all(&framed(
        answered,
        vec![TestRecordGet::new(vec!["k01".to_owned()]).into()],
    ))
    .await?;
    let first = read_seen(&mut sock).await?;
    assert_eq!((first.kind, first.id), (MessageType::Response, answered));
    sock.write_all(&cancel_of(answered)).await?;
    let ack = read_seen(&mut sock).await?;
    assert!(ack.acknowledges(answered), "{ack:?}");
    // and an id never sent
    let never = Uuid::now_v7();
    sock.write_all(&cancel_of(never)).await?;
    let ack = read_seen(&mut sock).await?;
    assert!(ack.acknowledges(never), "{ack:?}");
    // each was answered once, and nothing was refused or dropped
    probe(&mut sock).await?;
    let counted = cancels(&pool)?;
    assert_eq!(
        (counted.received, counted.refused, counted.dropped),
        (2, 0, 0),
        "{counted:?}"
    );
    Ok(())
}

/// A get split across both shards settles once cancelled, and leaves no gather behind
///
/// Whichever shard coordinates the connection, every share is refused - one on a held shard, one
/// behind the cancel on the coordinator's own queue - and a refused share completes its gather,
/// which answers the client with nothing the relay writes.
#[tokio::test(flavor = "multi_thread")]
async fn a_split_get_settles_after_a_cancel() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 2, Networking::default()).await?;
    small_rows(&addr).await?;
    let (mut sock, _) = raw(&addr, CLIENT_CAPS, None).await?;
    // one get of every small partition, which the two shards share between them
    let keys = (0..GETS).map(|n| format!("k{n:02}")).collect();
    pool.hold_shard(0, 800)?;
    pool.hold_shard(1, 800)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let id = Uuid::now_v7();
    let mut frames = framed(id, vec![TestRecordGet::new(keys).into()]);
    frames.extend_from_slice(&cancel_of(id));
    sock.write_all(&frames).await?;
    // only the acknowledgement comes back
    let first = read_seen(&mut sock).await?;
    assert!(first.acknowledges(id), "the first frame was {first:?}");
    probe(&mut sock).await?;
    // and no shard holds a gather for it
    let views = pool.read_verb(None, shoal::server::replication::ReadVerb::Gathers)?;
    for view in views {
        let view = view.expect("a shard answered");
        assert_eq!(view["resident"], 0, "a gather was left behind: {view}");
    }
    let counted = cancels(&pool)?;
    assert!(counted.refused >= 1, "{counted:?}");
    Ok(())
}

/// A result stream dropped before its answers come stops the work they would have cost
///
/// The client sends a cancel when a stream it handed out is dropped with answers still owed. With
/// the shard held, the cancel arrives behind the bundle and every get is refused. A client built
/// not to cancel leaves the server to run them all, as every client did before F75.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_result_stream_cancels_its_queued_work() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    small_rows(&addr).await?;
    let bundle = |client: &Shoal<TestDbClient>| {
        let mut queries = client.query();
        for n in 0..GETS {
            queries = queries.add(TestRecordGet::new(vec![format!("k{n:02}")]));
        }
        queries
    };
    // a client that cancels: the stream is dropped while the shard is held
    let cancelling = Shoal::<TestDbClient>::builder()
        .endpoint(&addr)
        .build()
        .await?;
    pool.hold_shard(0, 800)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let stream = cancelling.send(bundle(&cancelling)).await?;
    drop(stream);
    let counted = wait_for(&pool, |counted| counted.refused == GETS as u64).await?;
    assert_eq!(counted.received, 1, "{counted:?}");
    assert_eq!(counted.refused, GETS as u64, "{counted:?}");
    // a client built not to cancel leaves every get to run
    let forgetting = Shoal::<TestDbClient>::builder()
        .endpoint(&addr)
        .cancel_abandoned(false)
        .build()
        .await?;
    pool.hold_shard(0, 800)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let stream = forgetting.send(bundle(&forgetting)).await?;
    drop(stream);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let after = cancels(&pool)?;
    assert_eq!(after.received, 1, "{after:?}");
    assert_eq!(after.refused, GETS as u64, "{after:?}");
    // and the cancelling client goes on being answered
    let found = cancelling
        .send_one(TestRecordGet::new(vec!["probe".to_owned()]))
        .await?;
    assert!(found
        .access::<TestRecord>()?
        .is_some_and(|rows| rows.len() == 1));
    Ok(())
}

/// A try that times out at the client is cancelled, and its retry under the same id is answered
///
/// The shard is held past the client's wait, so the first try ends on the client's deadline and
/// is cancelled; the retry is sent under the same identity, after the cancel on any connection
/// they share, so the server refuses the first and runs the retry.
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_try_is_cancelled_and_its_retry_answered() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (pool, addr) = start(&temp_dir, 1, Networking::default()).await?;
    small_rows(&addr).await?;
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    // a hold longer than the client waits for one try - the deadline and the client's second of
    // slack, about 1.25 seconds - and shorter than it waits for two, so only the first times out
    pool.hold_shard(0, 1_800)?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let options = SendOptions::new()
        .deadline(Duration::from_millis(200))
        .retry(Duration::from_secs(10));
    let found = client
        .send_one_with(TestRecordGet::new(vec!["k05".to_owned()]), &options)
        .await?;
    assert!(found
        .access::<TestRecord>()?
        .is_some_and(|rows| rows.len() == 1));
    // the first try was cancelled and refused, the retry was run
    let counted = wait_for(&pool, |counted| counted.refused >= 1).await?;
    assert_eq!(counted.received, 1, "{counted:?}");
    assert_eq!(counted.refused, 1, "{counted:?}");
    Ok(())
}
