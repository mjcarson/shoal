//! Integration tests for the error channel against a running server
//!
//! These are the tests that establish the difference between a query that failed and a query that
//! found nothing, and between one query failing and a whole connection ending. Before the error
//! channel a server had exactly two ways to report a failure: answer `Get(None)`, which is also
//! how it answers an empty partition, or close the socket, which tells the client nothing at all
//! and takes every other query on that connection down with it.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::shared::protocol::error::{self, ErrorCode};
use shoal::shared::protocol::auth::AuthMechanisms;
use shoal::shared::protocol::{self, handshake};
use shoal::shared::queries::Queries;
use shoal::shared::traits::QuerySupport;
use shoal::tables::EphemeralSortedTable;
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
    /// The payload
    #[shoal(update)]
    pub data: String,
}

/// The schema these tests run against
///
/// An ephemeral table, because none of these tests are about storage — they are about what the
/// server does when it cannot give a client the answer it worked out.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub test_records: EphemeralSortedTable<TestRecord>,
}

/// The frame bound a raw connection advertises so that a real answer will not fit in it
///
/// Large enough to carry an error frame and a get that found nothing, small enough that a single
/// row of a few hundred bytes cannot be framed. That gap is the whole test.
const TINY_FRAME_BYTES: u32 = 512;

/// The size of the row that will not fit in [`TINY_FRAME_BYTES`]
const BIG_ROW_BYTES: usize = 4096;

/// Open a raw connection and complete the client half of the handshake, naming a frame bound
///
/// The real client hardcodes [`protocol::DEFAULT_MAX_FRAME_BYTES`] in its hello, so a raw socket
/// is the only way to tell a server that this connection accepts less than the server can produce.
///
/// # Arguments
///
/// * `addr` - The server to connect too
/// * `max_frame_bytes` - The largest frame to tell the server this connection will accept
async fn handshaken_with_bound(addr: &str, max_frame_bytes: u32) -> Result<TcpStream, TestError> {
    // open a connection and say who we are, and how large a frame we will take
    let mut sock = TcpStream::connect(addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes,
        mechanisms: AuthMechanisms::NONE,
    };
    sock.write_all(
        &hello
            .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("failed to build a hello"),
    )
    .await?;
    sock.flush().await?;
    // read the whole ack back and check that we were let in
    let mut frame = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut frame).await?;
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&frame[protocol::HEADER_LEN..]);
    let ack = handshake::HelloAck::decode(&body);
    assert!(
        ack.reason.is_accepted(),
        "the server refused a well formed handshake: {:?}",
        ack.reason
    );
    Ok(sock)
}

/// Send a bundle of queries down a raw socket, the way the client would
///
/// # Arguments
///
/// * `sock` - The connection to write to
/// * `queries` - The bundle to send
async fn send_bundle(sock: &mut TcpStream, queries: &Queries<TestDbClient>) -> Result<(), TestError> {
    // archive the bundle and build the header that goes ahead of it
    let archived =
        rkyv::to_bytes::<rkyv::rancor::Error>(queries).expect("failed to archive a bundle");
    let preamble = protocol::request_preamble(archived.len(), protocol::DEFAULT_MAX_FRAME_BYTES)
        .expect("failed to build a request preamble");
    // write the two halves, header first
    sock.write_all(&preamble).await?;
    sock.write_all(&archived).await?;
    sock.flush().await?;
    Ok(())
}

/// Read one whole frame back off a raw socket
///
/// Returns the header and the body after it, so a caller can decide what the frame was rather than
/// having to know before it reads.
///
/// # Arguments
///
/// * `sock` - The connection to read from
async fn read_frame(sock: &mut TcpStream) -> Result<(protocol::RawHeader, Vec<u8>), TestError> {
    // every frame starts with the same eight bytes, whatever it turns out to be
    let mut header_bytes = [0u8; protocol::HEADER_LEN];
    sock.read_exact(&mut header_bytes).await?;
    let header = protocol::RawHeader::decode(&header_bytes);
    // and the length says exactly how much follows it
    let mut body = vec![0u8; header.len as usize];
    sock.read_exact(&mut body).await?;
    Ok((header, body))
}

/// A response too large to frame comes back as an error naming the query
///
/// This is item 61. The server worked the answer out and then found it could not write it, and
/// before the error channel the only thing it could do was close the connection — leaving the
/// client with a dead socket and no idea which query had caused it or why.
#[tokio::test]
async fn a_response_too_large_to_frame_is_answered_with_an_error_naming_the_query(
) -> Result<(), TestError> {
    // start a server, and use a real client to put a row in that no tiny frame could carry
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    client
        .send_one(TestRecord {
            partition_key: "big".to_owned(),
            sort_key: "big".to_owned(),
            data: "x".repeat(BIG_ROW_BYTES),
        })
        .await?;
    // open a raw connection that says it will only take a small frame
    let mut sock = handshaken_with_bound(&addr, TINY_FRAME_BYTES).await?;
    // ask it for the row we just wrote, which cannot possibly fit
    let queries = Queries::<TestDbClient>::default().add(TestRecordGet::new(vec!["big".to_owned()]));
    let query_id = queries.id;
    send_bundle(&mut sock, &queries).await?;
    // what comes back is an error frame rather than a closed socket
    let (header, body) = read_frame(&mut sock).await?;
    assert_eq!(
        header.kind,
        protocol::MessageType::Error.as_byte(),
        "a response that could not be framed did not come back as an error"
    );
    // and it is flagged as a failure in the header as well as in the type byte
    assert!(header.flags.contains(protocol::Flags::IS_ERROR));
    // it names the query that was lost, so the client knows which one to give up on
    let mut id_bytes = [0u8; protocol::QUERY_ID_LEN];
    id_bytes.copy_from_slice(&body[..protocol::QUERY_ID_LEN]);
    assert_eq!(Uuid::from_bytes(id_bytes), query_id);
    // and it says why, in terms of the two sizes that did not fit
    let (code, msg) = error::decode_error_tail(&body[protocol::QUERY_ID_LEN..])
        .expect("the error frame's body did not decode");
    assert_eq!(code, ErrorCode::ResponseTooLarge);
    assert!(
        msg.contains(&TINY_FRAME_BYTES.to_string()),
        "the failure did not name the frame bound it hit: {msg}"
    );
    pool.exit()?;
    Ok(())
}

/// A response too large to frame leaves the rest of its connection serving
///
/// The failure is in one answer, not in the socket, so ending the connection would take every
/// other query multiplexed on it down over one oversize row.
#[tokio::test]
async fn a_response_too_large_to_frame_leaves_the_connection_serving() -> Result<(), TestError> {
    // start a server and write the row that will not fit
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, pool) = utils::start_with_conf::<TestDb>(conf).await?;
    client
        .send_one(TestRecord {
            partition_key: "big".to_owned(),
            sort_key: "big".to_owned(),
            data: "x".repeat(BIG_ROW_BYTES),
        })
        .await?;
    // ask for it over a connection that cannot carry it, and take the failure
    let mut sock = handshaken_with_bound(&addr, TINY_FRAME_BYTES).await?;
    let queries = Queries::<TestDbClient>::default().add(TestRecordGet::new(vec!["big".to_owned()]));
    send_bundle(&mut sock, &queries).await?;
    let (header, _) = read_frame(&mut sock).await?;
    assert_eq!(header.kind, protocol::MessageType::Error.as_byte());
    // now ask the same connection for something that does fit
    let queries =
        Queries::<TestDbClient>::default().add(TestRecordGet::new(vec!["nothing".to_owned()]));
    let query_id = queries.id;
    send_bundle(&mut sock, &queries).await?;
    // it is answered normally, which it could not be if the failure had closed the connection
    let (header, body) = read_frame(&mut sock).await?;
    assert_eq!(
        header.kind,
        protocol::MessageType::Response.as_byte(),
        "the connection stopped serving after one response could not be framed"
    );
    let mut id_bytes = [0u8; protocol::QUERY_ID_LEN];
    id_bytes.copy_from_slice(&body[..protocol::QUERY_ID_LEN]);
    assert_eq!(Uuid::from_bytes(id_bytes), query_id);
    pool.exit()?;
    Ok(())
}

/// A get that found nothing is still not a failure
///
/// The point of the error channel is to separate two answers that used to be one, so it is worth
/// pinning the half that did *not* change. An empty partition is not an error and must never
/// start reporting as one.
#[tokio::test]
async fn a_get_that_found_nothing_is_not_reported_as_a_failure() -> Result<(), TestError> {
    // start a server with nothing in it
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<TestDb>(&temp_dir).await?;
    // ask for a partition that was never written
    let mut stream = client
        .send(
            client
                .query()
                .add(TestRecordGet::new(vec!["missing".to_owned()])),
        )
        .await?;
    let response = stream
        .next()
        .await?
        .expect("a get of an empty table answered nothing at all");
    // it found nothing, and finding nothing is not a failure
    assert!(
        response.error().is_none(),
        "an empty partition was reported as a failed query"
    );
    assert!(response.access::<TestRecord>()?.is_none());
    // and the stream still ends cleanly behind it
    assert!(stream.next().await?.is_none());
    pool.exit()?;
    Ok(())
}
