//! Integration tests for authentication against a running server
//!
//! `shoal-core`'s own unit tests establish that the mechanism is SCRAM-SHA-256 and that it agrees
//! with RFC 7677. These establish the things only a socket can: that the exchange happens in the
//! right place relative to the split, that a server which requires nothing still accepts a client
//! that offers nothing, and that a client which fails it is told so rather than being dropped.
//!
//! Several of these take about five seconds to fail on purpose. `bb8` retries `connect` with
//! backoff until its connection timeout elapses, and a refused credential is permanent, so the
//! pool spends the full timeout before it gives up. That is not a hang — `shoal/tests/handshake.rs`
//! says the same thing about a schema mismatch.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{ConnectError, Errors, Shoal};
use shoal::server::ShoalPool;
use shoal::shared::auth::Credentials;
use shoal::shared::protocol::auth::{AuthMechanisms, AuthStatus};
use shoal::shared::protocol::handshake::RefusalReason;
use shoal::shared::protocol::{self, auth as proto_auth, handshake, MessageType};
use shoal::shared::traits::QuerySupport;
use shoal::tables::EphemeralSortedTable;
use shoal_derive::{db, ShoalSortedTable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod utils;

use utils::TestError;

/// The name the server in these tests accepts
const USER: &str = "reader";

/// The password that name authenticates with
const PASSWORD: &str = "hunter2";

/// A row in the table these tests query
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "AuthDb")]
pub struct AuthRecord {
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
/// An ephemeral table, because none of this is about storage — it is about what happens to a
/// connection before a query could reach a table at all.
#[db]
pub struct AuthDb {
    /// The only table in this schema
    pub auth_records: EphemeralSortedTable<AuthRecord>,
}

/// Start a server that requires the one user these tests use
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this server should store its data in, which has to outlive it
async fn start_locked(
    temp_dir: &tempfile::TempDir,
) -> Result<(String, ShoalPool<AuthDb>), TestError> {
    // build a config with a port nobody else in this run is using, and one user
    let conf = utils::build_auth_config(temp_dir, USER, PASSWORD);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    // start the server and give it a moment to bind
    let pool = ShoalPool::<AuthDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    Ok((addr, pool))
}

/// Start a server that requires nothing, which is what every other test in this suite gets
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this server should store its data in, which has to outlive it
async fn start_open(
    temp_dir: &tempfile::TempDir,
) -> Result<(String, ShoalPool<AuthDb>), TestError> {
    // the default config has no auth section at all
    let conf = utils::build_config(temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let pool = ShoalPool::<AuthDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    Ok((addr, pool))
}

/// Write a row through a client and read it back, so a connection is shown to be usable
///
/// # Arguments
///
/// * `client` - The client to query through
async fn round_trip(client: &Shoal<AuthDbClient>) -> Result<Vec<String>, TestError> {
    // write one row
    client
        .send_one(AuthRecord {
            partition_key: "key".to_owned(),
            sort_key: "key".to_owned(),
            data: "value".to_owned(),
        })
        .await?;
    // then read it back
    let mut stream = client
        .send(client.query().add(AuthRecordGet::new(vec!["key".to_owned()])))
        .await?;
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<AuthRecord>()? {
            for row in found.iter() {
                rows.push(row.sort_key.to_string());
            }
        }
    }
    Ok(rows)
}

/// A client with the right credentials connects and can query
///
/// The query half matters as much as the connect half. The exchange happens on the same stream the
/// relays are handed afterwards, so a byte left unread by it would show up here as a client that
/// connects and then cannot do anything.
#[tokio::test]
async fn the_right_credentials_connect_and_query() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    // connect with the credentials this server was given
    let client =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram(USER, PASSWORD)).await?;
    assert_eq!(round_trip(&client).await?, vec!["key".to_owned()]);
    Ok(())
}

/// A client with the wrong password is refused, and told so rather than dropped
#[tokio::test]
async fn the_wrong_password_is_refused() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    let refused =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram(USER, "wrong-password"))
            .await
            .err()
            .expect("a client with the wrong password was let in");
    // the server answered before it closed, which is what makes this legible rather than a reset
    match refused {
        Errors::Handshake(ConnectError::AuthFailed { msg }) => {
            assert!(!msg.is_empty(), "the server refused without saying anything");
        }
        other => panic!("a wrong password was reported as something else: {other:?}"),
    }
    Ok(())
}

/// A user the server has never heard of is refused in exactly the same words
///
/// This is the property that keeps a login from being a way to ask which accounts exist. The unit
/// tests establish it inside the mechanism; this establishes that nothing between the mechanism
/// and the socket puts the difference back.
#[tokio::test]
async fn an_unknown_user_is_refused_identically() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    // ask for a user that does not exist, and for one that does with the wrong password
    let missing =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram("nobody", PASSWORD))
            .await
            .err()
            .expect("a user that does not exist was let in");
    let wrong =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram(USER, "wrong-password"))
            .await
            .err()
            .expect("a client with the wrong password was let in");
    // both are the same variant carrying the same sentence
    match (missing, wrong) {
        (
            Errors::Handshake(ConnectError::AuthFailed { msg: missing }),
            Errors::Handshake(ConnectError::AuthFailed { msg: wrong }),
        ) => assert_eq!(missing, wrong),
        other => panic!("the two refusals were not the same: {other:?}"),
    }
    Ok(())
}

/// A client with no credentials is refused at connect time by a server that requires them
///
/// This one is refused in the `HelloAck` rather than in an `AuthResponse`: the client offered no
/// mechanism, so there is no exchange to have and nothing was ever proved.
#[tokio::test]
async fn a_client_with_no_credentials_is_refused() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    let refused = Shoal::<AuthDbClient>::new(&addr)
        .await
        .err()
        .expect("a client with no credentials was let in");
    match refused {
        Errors::Handshake(ConnectError::Protocol(
            shoal::shared::protocol::ProtocolError::Refused {
                reason: RefusalReason::NoCommonAuthMechanism,
            },
        )) => {}
        other => panic!("a missing credential was reported as something else: {other:?}"),
    }
    Ok(())
}

/// A server that requires nothing accepts a client that offers credentials anyway
///
/// The server picks the mechanism, so a client holding credentials against an open server is let
/// straight in rather than made to use them. Without this, adding credentials to a client would
/// break it against every server that has not turned authentication on.
#[tokio::test]
async fn credentials_against_an_open_server_are_ignored() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_open(&temp_dir).await?;
    let client =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram(USER, PASSWORD)).await?;
    assert_eq!(round_trip(&client).await?, vec!["key".to_owned()]);
    // and a client with nothing still works against the same server
    let plain = Shoal::<AuthDbClient>::new(&addr).await?;
    assert_eq!(round_trip(&plain).await?, vec!["key".to_owned()]);
    Ok(())
}

/// Open a raw socket and shake hands on it, without going through a client
///
/// # Arguments
///
/// * `addr` - The address to connect too
/// * `mechanisms` - The mechanisms to claim this peer can do
async fn raw_handshake(
    addr: &str,
    mechanisms: AuthMechanisms,
) -> Result<(TcpStream, handshake::HelloAck), TestError> {
    // open the socket and say hello, exactly the way a client does
    let mut sock = TcpStream::connect(addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: AuthDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        mechanisms,
    };
    sock.write_all(
        &hello
            .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("failed to build a hello"),
    )
    .await?;
    // read the ack back, which is a fixed size frame
    let mut frame = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut frame).await?;
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&frame[protocol::HEADER_LEN..]);
    Ok((sock, handshake::HelloAck::decode(&body)))
}

/// The server names the mechanism it selected in its ack, and names none when it requires none
#[tokio::test]
async fn the_server_names_the_mechanism_it_selected() -> Result<(), TestError> {
    // a server that requires authentication names the one mechanism it picked
    let locked_dir = utils::test_dir();
    let (locked, _locked_pool) = start_locked(&locked_dir).await?;
    let (_sock, ack) = raw_handshake(&locked, AuthMechanisms::SCRAM_SHA_256).await?;
    assert!(ack.reason.is_accepted());
    assert_eq!(
        ack.mechanism,
        Some(proto_auth::AuthMechanism::ScramSha256),
        "the server accepted a connection without naming a mechanism"
    );
    // and one that requires nothing names none, whatever was offered
    let open_dir = utils::test_dir();
    let (open, _open_pool) = start_open(&open_dir).await?;
    let (_sock, ack) = raw_handshake(&open, AuthMechanisms::SCRAM_SHA_256).await?;
    assert!(ack.reason.is_accepted());
    assert_eq!(ack.mechanism, None);
    Ok(())
}

/// A peer that sends queries instead of proving itself is refused, and the server survives it
///
/// This is the test that pins where the exchange sits. If the stream were split before it, this
/// bundle would reach a relay and be executed by a connection that has authenticated as nobody.
#[tokio::test]
async fn queries_before_the_proof_are_refused() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    // shake hands, get told to prove ourselves, and then send a bundle instead
    let (mut hostile, ack) = raw_handshake(&addr, AuthMechanisms::SCRAM_SHA_256).await?;
    assert_eq!(ack.mechanism, Some(proto_auth::AuthMechanism::ScramSha256));
    let preamble = protocol::request_preamble(4, protocol::DEFAULT_MAX_FRAME_BYTES)
        .expect("failed to build a request preamble");
    hostile.write_all(&preamble).await?;
    hostile.write_all(&[0u8; 4]).await?;
    // the server closes this connection rather than running anything on it
    let mut answer = Vec::new();
    hostile.read_to_end(&mut answer).await?;
    assert!(
        answer.is_empty(),
        "the server answered a bundle from a peer that had not authenticated"
    );
    // and it is still serving the clients that did authenticate
    let client =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram(USER, PASSWORD)).await?;
    assert_eq!(round_trip(&client).await?, vec!["key".to_owned()]);
    Ok(())
}

/// An auth frame claiming more than the auth bound is refused before it is allocated for
///
/// The frame bound this connection agreed on is 64 mebibytes and the auth bound is four kibibytes,
/// and this peer has proved nothing. The tighter of the two is what has to apply.
#[tokio::test]
async fn an_oversized_auth_frame_is_refused() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    let (mut hostile, _ack) = raw_handshake(&addr, AuthMechanisms::SCRAM_SHA_256).await?;
    // claim an auth frame far past the auth bound but well inside the frame bound
    let mut header = [0u8; protocol::HEADER_LEN];
    header[0] = protocol::PROTOCOL_VERSION;
    header[1] = MessageType::Auth.as_byte();
    header[4..8].copy_from_slice(&(1024u32 * 1024).to_le_bytes());
    hostile.write_all(&header).await?;
    // the server closes without waiting for a megabyte that will never arrive
    let mut answer = Vec::new();
    hostile.read_to_end(&mut answer).await?;
    assert!(answer.is_empty());
    // and it is still serving everybody else
    let client =
        Shoal::<AuthDbClient>::with_credentials(&addr, Credentials::scram(USER, PASSWORD)).await?;
    assert_eq!(round_trip(&client).await?, vec!["key".to_owned()]);
    Ok(())
}

/// A refusal is flagged in its header as well as in its status byte
///
/// A client can tell a refusal from a challenge without reading the body at all, the same
/// redundancy a refused `HelloAck` carries.
#[tokio::test]
async fn a_refusal_is_flagged_in_its_header() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_locked(&temp_dir).await?;
    let (mut hostile, _ack) = raw_handshake(&addr, AuthMechanisms::SCRAM_SHA_256).await?;
    // send something that is a valid auth frame and not a valid SCRAM message
    let frame = proto_auth::encode_auth(
        proto_auth::AuthMechanism::ScramSha256,
        b"this is not a scram message",
        protocol::DEFAULT_MAX_FRAME_BYTES,
    )
    .expect("failed to build an auth frame");
    hostile.write_all(&frame).await?;
    // read the answer's header, which says it is a refusal before its body is touched
    let mut header_bytes = [0u8; protocol::HEADER_LEN];
    hostile.read_exact(&mut header_bytes).await?;
    let header = protocol::Header::decode(&header_bytes, proto_auth::MAX_AUTH_FRAME_BODY)
        .expect("the server sent a header we cannot read");
    assert_eq!(header.kind, MessageType::AuthResponse);
    assert!(
        header.flags.contains(protocol::Flags::REFUSED),
        "a refusal was not flagged in its header"
    );
    // and the body says the same thing in its status byte
    let mut body = vec![0u8; header.body_len()];
    hostile.read_exact(&mut body).await?;
    let (status, msg) =
        proto_auth::decode_auth_response_body(&body).expect("the server sent a body we cannot read");
    assert_eq!(status, AuthStatus::Failed);
    assert!(!msg.is_empty());
    Ok(())
}
