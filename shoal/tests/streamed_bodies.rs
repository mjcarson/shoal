//! Integration tests for bodies carried across frames
//! ([F73](../../docs/src/features/bodies-across-frames.md))
//!
//! A bundle longer than the server's frame is sent as an opener and data frames, and an answer
//! longer than one data frame comes back as one, to a client that asked for streams at its hello.
//! The first two tests drive both through the real client against small frame bounds, so a
//! stream is the only way the bytes could have arrived. The rest open a raw socket and speak the
//! frames by hand: what order frames leave a connection in, and what a stream that breaks its
//! rules or asks for too much is answered with.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{PoolConfig, Shoal, StreamConfig};
use shoal::server::conf::Networking;
use shoal::shared::protocol::auth::AuthMechanisms;
use shoal::shared::protocol::error::{self as proto_error, ErrorCode};
use shoal::shared::protocol::stream::{self, CLIENT_CAPS};
use shoal::shared::protocol::{self, handshake, Flags, MessageType};
use shoal::shared::queries::Queries;
use shoal::shared::traits::QuerySupport;
use shoal::tables::EphemeralSortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable};
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

/// A payload of a given length that no two rows share the bytes of
///
/// # Arguments
///
/// * `len` - Its length
/// * `seed` - What makes it this row's
fn payload(len: usize, seed: u8) -> String {
    (0..len)
        .map(|index| char::from(b'a' + ((index + seed as usize) % 26) as u8))
        .collect()
}

/// Start a server under a networking block, with room in memory for the rows these tests write
///
/// # Arguments
///
/// * `temp_dir` - Where its storage goes
/// * `networking` - Its networking block, the port left to the pool
async fn start(
    temp_dir: &tempfile::TempDir,
    networking: Networking,
) -> Result<(ShoalPool<TestDb>, String), TestError> {
    let conf = utils::build_config(temp_dir)
        .resources(
            shoal::server::conf::Resources::default()
                .cores(2)
                .memory("512MiB")
                .expect("a memory bound parses"),
        )
        .networking(networking.port(0));
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    Ok((pool, addr.to_string()))
}

/// Build a client against a server, taking frames no larger than it is told to
///
/// # Arguments
///
/// * `addr` - The server
/// * `max_frame_bytes` - The largest frame the client accepts
/// * `pool_size` - How many connections it holds
async fn client(
    addr: &str,
    max_frame_bytes: u32,
    pool_size: u32,
) -> Result<Shoal<TestDbClient>, TestError> {
    let streams = StreamConfig {
        max_frame_bytes,
        ..StreamConfig::default()
    };
    let pool = PoolConfig {
        min_idle: pool_size.min(1),
        max_size: pool_size,
        ..PoolConfig::default()
    };
    Ok(Shoal::<TestDbClient>::builder()
        .endpoint(addr)
        .streams(streams)
        .pool(pool)
        .build()
        .await?)
}

/// Read every row of one partition back
///
/// # Arguments
///
/// * `client` - The client
/// * `key` - The partition
async fn read_partition(
    client: &Shoal<TestDbClient>,
    key: &str,
) -> Result<Vec<(String, String)>, TestError> {
    let mut stream = client
        .send(client.query().add(TestRecordGet::new(vec![key.to_owned()])))
        .await?;
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<TestRecord>()? {
            for row in found.iter() {
                rows.push((row.sort_key.to_string(), row.data.to_string()));
            }
        }
    }
    Ok(rows)
}

/// A bundle longer than the server's frame is streamed to it, and its answer streamed back
#[tokio::test]
async fn a_bundle_larger_than_one_frame_round_trips() -> Result<(), TestError> {
    // a server of 64 KiB frames that assembles bundles of a mebibyte
    let temp_dir = utils::test_dir();
    let networking = Networking::default()
        .max_frame_bytes(64 << 10)
        .max_request_body_bytes(1 << 20);
    let (_pool, addr) = start(&temp_dir, networking).await?;
    // and a client of 64 KiB frames too, so the row's answer has to be streamed back as well
    let client = client(&addr, 64 << 10, 4).await?;
    // a row of 400 KiB is a bundle six frames long
    let data = payload(400 << 10, 3);
    client
        .send_one(TestRecord {
            partition_key: "big".to_owned(),
            sort_key: "one".to_owned(),
            data: data.clone(),
        })
        .await?;
    // and it comes back whole
    let rows = read_partition(&client, "big").await?;
    assert_eq!(rows, vec![("one".to_owned(), data)]);
    Ok(())
}

/// A bundle past the server's frame, and a send marked bulk, go on connections set apart for long
/// streams, and every other send on the shared pool
#[tokio::test]
async fn long_streams_go_on_connections_set_apart() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let networking = Networking::default()
        .max_frame_bytes(64 << 10)
        .max_request_body_bytes(1 << 20);
    let (_pool, addr) = start(&temp_dir, networking).await?;
    let streams = StreamConfig {
        max_frame_bytes: 64 << 10,
        dedicated_connections: 1,
        ..StreamConfig::default()
    };
    let client = Shoal::<TestDbClient>::builder()
        .endpoint(&addr)
        .streams(streams)
        .pool(PoolConfig {
            min_idle: 1,
            max_size: 2,
            ..PoolConfig::default()
        })
        .build()
        .await?;
    // nothing is set apart until a long stream needs it
    assert_eq!(client.connections().1, 0);
    client
        .send_one(TestRecord {
            partition_key: "small".to_owned(),
            sort_key: "0".to_owned(),
            data: "x".to_owned(),
        })
        .await?;
    assert_eq!(
        client.connections().1,
        0,
        "a small bundle opened a connection set apart"
    );
    // a bundle past the server's frame is streamed on one
    client
        .send_one(TestRecord {
            partition_key: "long".to_owned(),
            sort_key: "0".to_owned(),
            data: payload(200 << 10, 1),
        })
        .await?;
    assert_eq!(
        client.connections().1,
        1,
        "a streamed bundle went on the shared pool"
    );
    // and a send marked bulk is too, whatever its length
    let options = shoal::client::SendOptions::new().bulk();
    let mut stream = client
        .send_with(
            client
                .query()
                .add(TestRecordGet::new(vec!["long".to_owned()])),
            &options,
        )
        .await?;
    while stream.next().await?.is_some() {}
    assert_eq!(client.connections().1, 1);
    Ok(())
}

/// An answer longer than the client's frame is streamed to it, in data frames that fit
#[tokio::test]
async fn an_answer_larger_than_one_frame_round_trips() -> Result<(), TestError> {
    // a server of the default frame, so every bundle fits one
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, Networking::default()).await?;
    // and a client that takes frames of 64 KiB, so an answer past that has to be streamed
    let client = client(&addr, 64 << 10, 4).await?;
    // eight rows of 20 KiB in one partition, each sent whole
    let mut sent = Vec::new();
    for n in 0..8u8 {
        let row = TestRecord {
            partition_key: "wide".to_owned(),
            sort_key: format!("{n}"),
            data: payload(20 << 10, n),
        };
        sent.push((row.sort_key.clone(), row.data.clone()));
        client.send_one(row).await?;
    }
    // one get answers all of them, in about 160 KiB
    let rows = read_partition(&client, "wide").await?;
    assert_eq!(rows, sent);
    Ok(())
}

/// A server granted streams answers past a client's body bound by name, not with a stream
#[tokio::test]
async fn an_answer_past_the_clients_body_bound_is_refused_by_name() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, Networking::default()).await?;
    // a client that takes frames of 64 KiB and assembles answers of 128 KiB at the most
    let streams = StreamConfig {
        max_frame_bytes: 64 << 10,
        max_body_bytes: 128 << 10,
        ..StreamConfig::default()
    };
    let client = Shoal::<TestDbClient>::builder()
        .endpoint(&addr)
        .streams(streams)
        .build()
        .await?;
    // a partition of 200 KiB
    for n in 0..10u8 {
        let row = TestRecord {
            partition_key: "over".to_owned(),
            sort_key: format!("{n}"),
            data: payload(20 << 10, n),
        };
        client.send_one(row).await?;
    }
    // is refused, naming the bound, and the client goes on
    match read_partition(&client, "over").await {
        Err(TestError::Client(shoal::client::Errors::Server { code, msg, .. })) => {
            assert_eq!(code, ErrorCode::ResponseTooLarge);
            assert!(
                msg.contains("assembles from a stream"),
                "the refusal did not name the bound: {msg}"
            );
        }
        other => panic!("an answer past the body bound was not refused by name: {other:?}"),
    }
    client
        .send_one(TestRecord {
            partition_key: "after".to_owned(),
            sort_key: "0".to_owned(),
            data: "x".to_owned(),
        })
        .await?;
    assert_eq!(read_partition(&client, "after").await?.len(), 1);
    Ok(())
}

/// Open a raw connection that asks for streams, and return it with what the server granted
///
/// # Arguments
///
/// * `addr` - The server
/// * `rcvbuf` - The receive buffer to give the socket, if a test needs it small
async fn raw_streams(
    addr: &str,
    rcvbuf: Option<u32>,
) -> Result<(TcpStream, handshake::HelloAck), TestError> {
    // a small receive buffer has to be set before the connection opens to stick
    let socket = tokio::net::TcpSocket::new_v4()?;
    if let Some(bytes) = rcvbuf {
        socket.set_recv_buffer_size(bytes)?;
    }
    let mut sock = socket
        .connect(addr.parse().expect("the address parses"))
        .await?;
    sock.set_nodelay(true)?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        mechanisms: AuthMechanisms::NONE,
        caps: CLIENT_CAPS,
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
    // a connection subscribes to nothing, so the only frames on it are answers to its bundles
    Ok((sock, ack))
}

/// One bundle's bytes, archived as the client would send them
///
/// # Arguments
///
/// * `id` - The bundle's id
/// * `query` - Its one query
fn bundle(id: Uuid, query: impl Into<<TestDbClient as QuerySupport>::QueryKinds>) -> Vec<u8> {
    let mut queries = Queries::<TestDbClient>::default();
    queries.id = id;
    queries = queries.add(query);
    rkyv::to_bytes::<rkyv::rancor::Error>(&queries)
        .expect("a bundle archives")
        .to_vec()
}

/// One frame a server wrote, as much as a test needs of it
#[derive(Debug, Clone, PartialEq, Eq)]
struct SeenFrame {
    /// Its type
    kind: MessageType,
    /// Its flags
    flags: Flags,
    /// The id it carried
    id: Uuid,
    /// An error frame's code
    code: Option<ErrorCode>,
}

/// Read one frame a server wrote, keeping what a test needs and dropping the rest
///
/// # Arguments
///
/// * `sock` - The connection
async fn read_seen(sock: &mut TcpStream) -> Result<SeenFrame, TestError> {
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
    Ok(SeenFrame {
        kind: frame.header.kind,
        flags: frame.header.flags,
        id: frame.query_id,
        code,
    })
}

/// A small answer queued while a large one streams is written between two of its frames
///
/// The client's socket is given a small receive buffer and is not read for a while, so the large
/// answer's frames back up behind it and its writer is stopped part way through; a small get sent
/// then is answered at once, and its answer has to come out before the large one's last frame.
#[tokio::test]
async fn a_small_answer_is_written_between_the_frames_of_a_large_one() -> Result<(), TestError> {
    // a server streaming in frames of 64 KiB
    let temp_dir = utils::test_dir();
    let networking = Networking::default().stream_frame_bytes(64 << 10);
    let (_pool, addr) = start(&temp_dir, networking).await?;
    // sixteen rows of a mebibyte in one partition, and one small row in another
    let writer = Shoal::<TestDbClient>::new(&addr).await?;
    for n in 0..16u8 {
        let row = TestRecord {
            partition_key: "large".to_owned(),
            sort_key: format!("{n:02}"),
            data: payload(1 << 20, n),
        };
        writer.send_one(row).await?;
    }
    writer
        .send_one(TestRecord {
            partition_key: "small".to_owned(),
            sort_key: "0".to_owned(),
            data: "x".to_owned(),
        })
        .await?;
    // a raw connection asking for streams, with a receive buffer far smaller than the answer
    let (mut sock, ack) = raw_streams(&addr, Some(64 << 10)).await?;
    assert_ne!(
        ack.caps & stream::CLIENT_CAP_STREAMS,
        0,
        "the server granted no streams"
    );
    // ask for the large partition, then wait for its answer to back up behind the socket
    let large = Uuid::new_v4();
    let body = bundle(large, TestRecordGet::new(vec!["large".to_owned()]));
    let head = protocol::request_preamble(body.len(), ack.max_frame_bytes)?;
    sock.write_all(&head).await?;
    sock.write_all(&body).await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // then ask for the small one, and give it time to be answered
    let small = Uuid::new_v4();
    let body = bundle(small, TestRecordGet::new(vec!["small".to_owned()]));
    let head = protocol::request_preamble(body.len(), ack.max_frame_bytes)?;
    sock.write_all(&head).await?;
    sock.write_all(&body).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // read every frame until the large answer's last one
    let mut seen = Vec::new();
    loop {
        let frame = read_seen(&mut sock).await?;
        let done = frame.kind == MessageType::Data
            && frame.flags.contains(Flags::LAST)
            && frame.id == large;
        seen.push(frame);
        if done {
            break;
        }
    }
    // the large answer was streamed, and the small one was written inside it
    let opener = seen
        .iter()
        .position(|frame| frame.id == large && frame.flags.contains(Flags::STREAMED));
    let answer = seen
        .iter()
        .position(|frame| frame.id == small && frame.kind == MessageType::Response);
    assert!(opener.is_some(), "the large answer was not streamed");
    assert!(
        answer.is_some(),
        "the small answer did not come out before the large one ended"
    );
    assert!(
        opener < answer,
        "the small answer was written before the large one began"
    );
    let data_frames = seen
        .iter()
        .filter(|frame| frame.kind == MessageType::Data)
        .count();
    assert!(
        data_frames > 200,
        "the large answer took {data_frames} data frames"
    );
    Ok(())
}

/// What a malformed stream sends, after the bundle that opens it if it has one
#[derive(Debug, Clone, Copy)]
enum Malformed {
    /// A data frame for an id nobody opened
    Unknown,
    /// A data frame at the wrong offset
    Gap,
    /// A data frame past the declared length
    PastEnd,
    /// The last frame short of the declared length
    ShortLast,
}

/// A stream that breaks its rules ends its connection, and every other client is still answered
#[tokio::test]
async fn a_malformed_data_frame_ends_one_connection_and_others_are_answered(
) -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, Networking::default()).await?;
    let healthy = Shoal::<TestDbClient>::new(&addr).await?;
    for malformed in [
        Malformed::Unknown,
        Malformed::Gap,
        Malformed::PastEnd,
        Malformed::ShortLast,
    ] {
        let (mut sock, ack) = raw_streams(&addr, None).await?;
        let id = Uuid::new_v4();
        let max = ack.max_frame_bytes;
        // every case but the first opens a stream of a hundred bytes
        if !matches!(malformed, Malformed::Unknown) {
            let opener = stream::queries_opener(None, None, &id, 100, max)?;
            sock.write_all(opener.as_bytes()).await?;
        }
        let (offset, len, last) = match malformed {
            Malformed::Unknown => (0, 10, true),
            Malformed::Gap => (10, 10, false),
            Malformed::PastEnd => (0, 101, true),
            Malformed::ShortLast => (0, 50, true),
        };
        sock.write_all(&stream::data_preamble(&id, offset, len, last, max)?)
            .await?;
        sock.write_all(&vec![7u8; len]).await?;
        sock.flush().await?;
        // the connection is closed rather than answered
        let mut scratch = [0u8; 64];
        let closed =
            match tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut scratch))
                .await
            {
                Ok(Ok(0)) | Ok(Err(_)) => true,
                Ok(Ok(_)) | Err(_) => false,
            };
        assert!(closed, "a {malformed:?} stream did not end its connection");
        // and the server still answers everybody else
        healthy
            .send_one(TestRecord {
                partition_key: format!("{malformed:?}"),
                sort_key: "0".to_owned(),
                data: "x".to_owned(),
            })
            .await?;
        assert_eq!(
            read_partition(&healthy, &format!("{malformed:?}"))
                .await?
                .len(),
            1
        );
    }
    Ok(())
}

/// A stream longer than the server assembles is refused by name and drained, and its connection
/// goes on serving
#[tokio::test]
async fn a_stream_over_the_request_bound_is_refused_by_name() -> Result<(), TestError> {
    // a server of 64 KiB frames, which assembles nothing longer than a frame
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, Networking::default().max_frame_bytes(64 << 10)).await?;
    let (mut sock, ack) = raw_streams(&addr, None).await?;
    assert_eq!(stream::body_bound(ack.max_body_log2), 64 << 10);
    // open a stream of 128 KiB, past what it said it assembles, and send all of it
    let refused = Uuid::new_v4();
    let max = ack.max_frame_bytes;
    sock.write_all(stream::queries_opener(None, None, &refused, 128 << 10, max)?.as_bytes())
        .await?;
    let mut splitter = stream::Splitter::new(128 << 10, stream::data_body(32 << 10, max));
    while let Some(piece) = splitter.next_piece() {
        sock.write_all(&stream::data_preamble(
            &refused,
            piece.offset,
            piece.len,
            piece.last,
            max,
        )?)
        .await?;
        sock.write_all(&vec![0u8; piece.len]).await?;
    }
    // then a small bundle on the same connection
    let small = Uuid::new_v4();
    let body = bundle(small, TestRecordGet::new(vec!["nothing".to_owned()]));
    sock.write_all(&protocol::request_preamble(body.len(), max)?)
        .await?;
    sock.write_all(&body).await?;
    // the stream is refused by name, and the bundle after it is answered
    let first = read_seen(&mut sock).await?;
    assert_eq!(
        (first.kind, first.id, first.code),
        (
            MessageType::Error,
            refused,
            Some(ErrorCode::RequestTooLarge)
        )
    );
    let second = read_seen(&mut sock).await?;
    assert_eq!((second.kind, second.id), (MessageType::Response, small));
    Ok(())
}

/// The ack grants streams, and says how long a bundle the server assembles, only to a client
/// that asked for them
#[tokio::test]
async fn the_ack_grants_streams_only_to_a_client_that_asked() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, Networking::default()).await?;
    // a client asking for streams is granted them, with the server's bound
    let (_sock, ack) = raw_streams(&addr, None).await?;
    assert_ne!(ack.caps & stream::CLIENT_CAP_STREAMS, 0);
    assert_eq!(
        stream::body_bound(ack.max_body_log2),
        u64::from(protocol::DEFAULT_MAX_FRAME_BYTES)
    );
    // one asking for nothing, as a client from before F73 does, is granted nothing
    let mut sock = TcpStream::connect(&addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        mechanisms: AuthMechanisms::NONE,
        caps: 0,
        max_body_log2: 0,
    };
    sock.write_all(&hello.frame(protocol::DEFAULT_MAX_FRAME_BYTES)?)
        .await?;
    let mut frame = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut frame).await?;
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&frame[protocol::HEADER_LEN..]);
    let ack = handshake::HelloAck::decode(&body);
    assert_eq!((ack.caps, ack.max_body_log2), (0, 0));
    Ok(())
}

/// A data frame on a connection that asked for no streams ends that connection
#[tokio::test]
async fn a_data_frame_without_the_capability_closes_one_connection() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (_pool, addr) = start(&temp_dir, Networking::default()).await?;
    // a connection that asks for nothing
    let mut sock = TcpStream::connect(&addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        mechanisms: AuthMechanisms::NONE,
        caps: 0,
        max_body_log2: 0,
    };
    sock.write_all(&hello.frame(protocol::DEFAULT_MAX_FRAME_BYTES)?)
        .await?;
    let mut frame = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut frame).await?;
    // a data frame on it is refused by closing it
    let id = Uuid::new_v4();
    sock.write_all(&stream::data_preamble(
        &id,
        0,
        4,
        true,
        protocol::DEFAULT_MAX_FRAME_BYTES,
    )?)
    .await?;
    sock.write_all(&[1, 2, 3, 4]).await?;
    let mut scratch = [0u8; 64];
    let closed = matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut scratch)).await,
        Ok(Ok(0)) | Ok(Err(_))
    );
    assert!(
        closed,
        "a data frame without the capability did not end its connection"
    );
    Ok(())
}
