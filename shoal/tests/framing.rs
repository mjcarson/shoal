//! Integration tests for the wire framing against a running server
//!
//! These are the tests that establish the difference between a bad frame ending a connection and a
//! bad frame ending a shard. Every one of them opens a raw socket alongside a healthy client,
//! sends something the server has to refuse, and then asserts two things: that the raw connection
//! was closed, and that **the healthy client still answers**. The second assertion is the one that
//! matters. Before the framing had a header, `client_rx_relay` panicked on anything it could not
//! read, and a panic in a relay takes down the shard it runs on along with every other client that
//! shard was serving — so each of these tests used to be a way for one peer to kill the database
//! for everybody.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::protocol::{self, handshake};
use shoal_core::shared::traits::QuerySupport;
use shoal_core::tables::EphemeralSortedTable;
use shoal_derive::{db, ShoalSortedTable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
/// An ephemeral table, because none of this is about storage — it is about what the server does
/// with bytes that arrive before a query ever reaches a table.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub test_records: EphemeralSortedTable<TestRecord>,
}

/// Build a request header by hand, without going through the encoder
///
/// The encoder refuses to build the frames these tests need to send, which is the whole reason
/// they have to be assembled here. Anything the encoder would produce is already covered by the
/// protocol module's own tests.
///
/// # Arguments
///
/// * `kind` - The message type byte to claim
/// * `len` - The body length to claim
fn hostile_header(kind: u8, len: u32) -> [u8; protocol::HEADER_LEN] {
    // lay the bytes down in the same order the encoder does
    let mut raw = [0u8; protocol::HEADER_LEN];
    raw[0] = protocol::PROTOCOL_VERSION;
    raw[1] = kind;
    raw[4..8].copy_from_slice(&len.to_le_bytes());
    raw
}

/// Open a raw connection and complete the client half of the handshake by hand
///
/// Every hostile frame below has to get past the handshake before it reaches the relay that this
/// file is actually about. A connection that never shakes hands is refused by the handshake and
/// never exercises the framing at all, which would make every test here quietly test the wrong
/// thing.
///
/// # Arguments
///
/// * `addr` - The server to connect too
async fn handshaken(addr: &str) -> Result<TcpStream, TestError> {
    // open a connection and say who we are
    let mut sock = TcpStream::connect(addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
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
    let ack = read_ack(&frame);
    assert!(
        ack.reason.is_accepted(),
        "the server refused a well formed handshake: {:?}",
        ack.reason
    );
    Ok(sock)
}

/// Pull the ack out of a whole handshake frame
///
/// # Arguments
///
/// * `frame` - The header and body the server answered with
fn read_ack(frame: &[u8; handshake::HANDSHAKE_FRAME_LEN]) -> handshake::HelloAck {
    // the header comes first and says what kind of frame this is
    let mut header_bytes = [0u8; protocol::HEADER_LEN];
    header_bytes.copy_from_slice(&frame[..protocol::HEADER_LEN]);
    let header = protocol::RawHeader::decode(&header_bytes);
    assert_eq!(
        header.kind,
        protocol::MessageType::HelloAck.as_byte(),
        "the server answered a hello with something other than an ack"
    );
    // and the body follows it
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&frame[protocol::HEADER_LEN..]);
    handshake::HelloAck::decode(&body)
}

/// Check that a socket has been closed by the far end
///
/// A server that drops a connection can show up either as a clean end of stream or as a reset,
/// depending on how much unread data was still queued, so both count as closed.
///
/// # Arguments
///
/// * `sock` - The socket to check
async fn assert_closed(sock: &mut TcpStream) {
    // try to read anything at all off of this socket
    let mut scratch = [0u8; 64];
    match sock.read(&mut scratch).await {
        // a clean end of stream is the server dropping its half
        Ok(0) => (),
        // any bytes at all means the server answered something it should have refused
        Ok(read) => panic!("the server sent {read} bytes instead of closing the connection"),
        // a reset is the same close, seen through a socket that still had bytes queued
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "the connection failed for a reason other than being closed: {error:?}"
        ),
    }
}

/// Insert a row and read it back, to prove the server is still answering
///
/// # Arguments
///
/// * `client` - The client to query with
/// * `key` - The partition and sort key to use, so two calls do not collide
async fn round_trip(
    client: &shoal_core::client::Shoal<TestDbClient>,
    key: &str,
) -> Result<(), TestError> {
    // insert a single row
    client
        .send_one(TestRecord {
            partition_key: key.to_owned(),
            sort_key: key.to_owned(),
            data: "still here".to_owned(),
        })
        .await?;
    // read it back out of the partition we just wrote
    let mut stream = client
        .send(client.query().add(TestRecordGet::new(vec![key.to_owned()])))
        .await?;
    // collect the keys of every row that get answered with
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<TestRecord>()? {
            for row in found.iter() {
                rows.push((row.partition_key.to_string(), row.sort_key.to_string()));
            }
        }
    }
    assert_eq!(
        rows,
        vec![(key.to_owned(), key.to_owned())],
        "the server stopped answering queries"
    );
    Ok(())
}

/// A frame claiming more bytes than the server will accept ends only that connection
///
/// This is the highest value test in the framing change. The length prefix is used as an
/// allocation size before a byte of the body has arrived, so before the bound existed this frame
/// asked the server to allocate four gibibytes, and the panic that followed took the shard with
/// it. Now it closes one socket.
#[tokio::test]
async fn a_hostile_length_prefix_closes_one_connection_and_the_server_keeps_serving(
) -> Result<(), TestError> {
    // start a server and a healthy client against it
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    // the server answers before anything hostile happens
    round_trip(&client, "before").await?;
    // open a raw connection alongside it and claim the largest frame a u32 can spell
    let mut hostile = handshaken(&addr).await?;
    hostile
        .write_all(&hostile_header(
            protocol::MessageType::Queries.as_byte(),
            u32::MAX,
        ))
        .await?;
    hostile.flush().await?;
    // that connection is closed without a byte of the body ever being asked for
    assert_closed(&mut hostile).await;
    // and the server is still serving every other client it had
    round_trip(&client, "after").await?;
    Ok(())
}

/// A frame of a message type the server does not expect ends only that connection
#[tokio::test]
async fn a_frame_of_an_unknown_type_closes_one_connection() -> Result<(), TestError> {
    // start a server and a healthy client against it
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    round_trip(&client, "before").await?;
    // send a frame whose type byte names nothing this build knows
    let mut hostile = handshaken(&addr).await?;
    hostile.write_all(&hostile_header(200, 0)).await?;
    hostile.flush().await?;
    assert_closed(&mut hostile).await;
    // and the server is still serving
    round_trip(&client, "after").await?;
    Ok(())
}

/// A frame of a type that only travels the other way ends only that connection
///
/// A `Response` is a perfectly valid message type, just not one a client sends. Without the type
/// byte the server would have read its first eight bytes as a length and allocated whatever they
/// spelled, which is the failure the type byte exists to make legible.
#[tokio::test]
async fn a_response_frame_sent_to_the_server_closes_one_connection() -> Result<(), TestError> {
    // start a server and a healthy client against it
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    round_trip(&client, "before").await?;
    // send a frame the server could parse but should never be handed
    let mut hostile = handshaken(&addr).await?;
    hostile
        .write_all(&hostile_header(
            protocol::MessageType::Response.as_byte(),
            0,
        ))
        .await?;
    hostile.flush().await?;
    assert_closed(&mut hostile).await;
    // and the server is still serving
    round_trip(&client, "after").await?;
    Ok(())
}

/// A frame written with a protocol version the server does not speak ends only that connection
#[tokio::test]
async fn a_frame_of_an_unsupported_version_closes_one_connection() -> Result<(), TestError> {
    // start a server and a healthy client against it
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    round_trip(&client, "before").await?;
    // send a well formed frame from a version that does not exist
    let mut hostile = handshaken(&addr).await?;
    let mut header = hostile_header(protocol::MessageType::Queries.as_byte(), 0);
    header[0] = protocol::PROTOCOL_VERSION.wrapping_add(1);
    hostile.write_all(&header).await?;
    hostile.flush().await?;
    assert_closed(&mut hostile).await;
    // and the server is still serving
    round_trip(&client, "after").await?;
    Ok(())
}

/// A hello naming a version the server does not speak is refused with a legible ack
///
/// This is the case that justifies the header layout being fixed across versions. The server has
/// to be able to read a header it cannot interpret well enough to say which version it saw, drain
/// exactly the right number of body bytes, and answer in a header the peer can parse. Without
/// that, a version mismatch would be a reset and the version byte would be decorative.
///
/// The body is drained before the refusal is written for a TCP reason rather than a protocol one:
/// closing a socket that still has unread bytes queued sends a reset, which would throw away the
/// very reply this test is reading.
#[tokio::test]
async fn a_hello_of_an_unsupported_version_is_refused_with_an_ack() -> Result<(), TestError> {
    // start a server and a healthy client against it
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    round_trip(&client, "before").await?;
    // open with a hello from a protocol version that does not exist
    let mut hostile = TcpStream::connect(&addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
    };
    let mut frame = hello
        .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
        .expect("failed to build a hello");
    frame[0] = protocol::PROTOCOL_VERSION.wrapping_add(1);
    hostile.write_all(&frame).await?;
    hostile.flush().await?;
    // the server answers before it closes, in a header written with its own version
    let mut answer = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    hostile.read_exact(&mut answer).await?;
    let mut header_bytes = [0u8; protocol::HEADER_LEN];
    header_bytes.copy_from_slice(&answer[..protocol::HEADER_LEN]);
    let header = protocol::RawHeader::decode(&header_bytes);
    assert_eq!(
        header.version,
        protocol::PROTOCOL_VERSION,
        "the refusal was written with a version the client cannot read"
    );
    assert!(
        header.flags.contains(protocol::Flags::REFUSED),
        "a refusal did not set the refused flag"
    );
    // and the body says which of the two things went wrong
    let ack = read_ack(&answer);
    assert_eq!(ack.reason, handshake::RefusalReason::UnsupportedVersion);
    // the connection is then closed, and the server is still serving
    assert_closed(&mut hostile).await;
    round_trip(&client, "after").await?;
    Ok(())
}

/// A hello naming a different schema is refused with both fingerprints on the wire
#[tokio::test]
async fn a_hello_naming_a_different_schema_is_refused_with_an_ack() -> Result<(), TestError> {
    // start a server and a healthy client against it
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let (client, _pool) = utils::start_with_conf::<TestDb>(conf).await?;
    round_trip(&client, "before").await?;
    // open with a hello claiming a schema this server was not built from
    let mut hostile = TcpStream::connect(&addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT ^ 0xffff_ffff_ffff_ffff,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
    };
    hostile
        .write_all(
            &hello
                .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
                .expect("failed to build a hello"),
        )
        .await?;
    hostile.flush().await?;
    // the refusal names the reason and carries the servers own fingerprint, so a client can
    // report both numbers rather than just its own
    let mut answer = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    hostile.read_exact(&mut answer).await?;
    let ack = read_ack(&answer);
    assert_eq!(ack.reason, handshake::RefusalReason::SchemaMismatch);
    assert_eq!(
        ack.schema_fingerprint,
        TestDbClient::SCHEMA_FINGERPRINT,
        "the refusal did not carry the servers own fingerprint"
    );
    // the connection is then closed, and the server is still serving
    assert_closed(&mut hostile).await;
    round_trip(&client, "after").await?;
    Ok(())
}
