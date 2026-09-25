//! What a retried bundle reports when its tries disagree about whether it applied
//!
//! `Shoal::exec_with` sends a bundle again under one identity while each failure says to try
//! again. A try answered `OutcomeUnknown` may have applied; a later try refused by name did not,
//! but that says nothing about the first. Before item 125 the loop returned whatever stopped it,
//! so a write that may well have applied reached its caller as a definite refusal
//! ([Resolved #125](../../docs/src/appendix/resolved/retry-unknown-outcome.md)).
//!
//! These tests answer the client from a scripted server rather than a real one, since the
//! sequence of codes is the whole of the question and a real server only reaches it through a
//! device failing.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::SendOptions;
use shoal::shared::protocol::error::{error_preamble, ErrorCode};
use shoal::shared::protocol::handshake::{self, HelloAck, RefusalReason};
use shoal::shared::protocol::{self, MessageType};
use shoal::shared::queries::Queries;
use shoal::shared::traits::QuerySupport;
use shoal::tables::EphemeralUnsortedTable;
use shoal::uuid::Uuid;
use shoal::{Errors, Shoal};
use shoal_derive::{db, ShoalUnsortedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod utils;

use utils::TestError;

/// A row in the table these tests write
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct TestRecord {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// The payload
    #[shoal(update)]
    pub data: String,
}

/// The schema these tests run against
///
/// Only its client half is used: the server is scripted.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub rows: EphemeralUnsortedTable<TestRecord>,
}

/// Answer one connection's handshake and then every bundle on it from a script
///
/// # Arguments
///
/// * `sock` - The connection a client opened
/// * `identity` - The bundle id every answer names
/// * `script` - The code each bundle is answered with, in order; the last repeats
/// * `sent` - How many bundles every connection together has answered
async fn serve_scripted(
    mut sock: TcpStream,
    identity: Uuid,
    script: Arc<Vec<ErrorCode>>,
    sent: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    // read the client's hello and accept it, asking nothing of it
    let mut hello = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut hello).await?;
    let ack = HelloAck {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        reason: RefusalReason::Accepted,
        mechanism: None,
        caps: 0,
    };
    sock.write_all(
        &ack.frame(protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("an ack frame"),
    )
    .await?;
    // answer every frame the client sends until it goes away
    loop {
        // read the header, which says how much follows it
        let mut raw = [0u8; protocol::REQUEST_PREAMBLE_LEN];
        if sock.read_exact(&mut raw).await.is_err() {
            return Ok(());
        }
        let header = protocol::decode_client_request(&raw, protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a client frame");
        // skip the rest of the frame, whose contents the script does not depend on
        let mut body = vec![0u8; header.len as usize];
        sock.read_exact(&mut body).await?;
        // only a bundle is answered
        if header.kind != MessageType::Queries {
            continue;
        }
        // answer this try with the next code in the script, repeating its last
        let attempt = sent.fetch_add(1, Ordering::SeqCst);
        let code = script[attempt.min(script.len() - 1)];
        let msg = format!("scripted {code} on try {}", attempt + 1);
        let preamble = error_preamble(
            &identity,
            code,
            msg.len(),
            protocol::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("an error preamble");
        sock.write_all(&preamble).await?;
        sock.write_all(msg.as_bytes()).await?;
        sock.flush().await?;
    }
}

/// Start a scripted server and hand back its address and its count of answered bundles
///
/// # Arguments
///
/// * `identity` - The bundle id every answer names
/// * `script` - The code each bundle is answered with, in order; the last repeats
async fn scripted_server(
    identity: Uuid,
    script: Vec<ErrorCode>,
) -> Result<(String, Arc<AtomicUsize>), TestError> {
    // listen on whatever port the kernel hands out, since nothing else needs to find it
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let script = Arc::new(script);
    let sent = Arc::new(AtomicUsize::new(0));
    let counted = sent.clone();
    // serve every connection the client's pool opens on a task of its own
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            tokio::spawn(serve_scripted(
                sock,
                identity,
                script.clone(),
                counted.clone(),
            ));
        }
    });
    Ok((addr, sent))
}

/// Send one insert under a retry budget against a scripted server and return what it came to
///
/// # Arguments
///
/// * `script` - The code each try is answered with, in order; the last repeats
async fn retried_insert(script: Vec<ErrorCode>) -> Result<(Errors, usize), TestError> {
    // every answer has to name the bundle's id, so the bundle is sent under a known one
    let identity = Uuid::now_v7();
    let (addr, sent) = scripted_server(identity, script).await?;
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    let bundle = Queries::<TestDbClient>::default().add(TestRecord {
        key: 1,
        data: "maybe applied".to_owned(),
    });
    let options = SendOptions::new()
        .identity(identity)
        .retry(Duration::from_millis(300));
    // every try fails, so the bundle has to come back as an error
    let error = match client.exec_with(bundle, &options).await {
        Ok(responses) => panic!("a scripted failure was answered with {responses:?}"),
        Err(error) => error,
    };
    Ok((error, sent.load(Ordering::SeqCst)))
}

/// The code a failure carries, if it is a server's failure
///
/// # Arguments
///
/// * `error` - What the bundle came to
fn code_of(error: &Errors) -> Option<ErrorCode> {
    match error {
        Errors::Server { code, .. } => Some(*code),
        _ => None,
    }
}

/// A try that may have applied is not hidden behind a later try's refusal
///
/// The first try is answered `OutcomeUnknown` and every later one `StorageWrite`, which is what a
/// standalone table answers a write behind an intent log that failed and a write after it. The
/// bundle may have applied, so that is what the caller has to be told.
#[tokio::test]
async fn an_unknown_outcome_outlives_a_later_refusal() -> Result<(), TestError> {
    let (error, sent) =
        retried_insert(vec![ErrorCode::OutcomeUnknown, ErrorCode::StorageWrite]).await?;
    // the refusal that stopped the loop was the second try's
    assert_eq!(sent, 2, "the bundle was tried {sent} times");
    assert_eq!(
        code_of(&error),
        Some(ErrorCode::OutcomeUnknown),
        "a bundle that may have applied reached its caller as {error:?}"
    );
    // and the refusal is still in what the caller reads
    assert!(
        error.to_string().contains("StorageWrite"),
        "the last try's refusal is missing from {error}"
    );
    Ok(())
}

/// An unknown outcome also survives a retriable refusal that runs the budget out
///
/// The first try is unknown and every later one is shed, which is tried again until the budget
/// is gone. What stops the loop is a refusal, and the bundle may still have applied.
#[tokio::test]
async fn an_unknown_outcome_outlives_the_budget() -> Result<(), TestError> {
    let (error, sent) =
        retried_insert(vec![ErrorCode::OutcomeUnknown, ErrorCode::Shedding]).await?;
    // shedding is tried again, so the budget is what ended this
    assert!(sent > 2, "the bundle was tried only {sent} times");
    assert_eq!(
        code_of(&error),
        Some(ErrorCode::OutcomeUnknown),
        "a bundle that may have applied reached its caller as {error:?}"
    );
    Ok(())
}

/// A refusal is reported as itself when no try's outcome was unknown
#[tokio::test]
async fn refusals_alone_stay_refusals() -> Result<(), TestError> {
    let (error, sent) = retried_insert(vec![
        ErrorCode::Shedding,
        ErrorCode::Shedding,
        ErrorCode::StorageWrite,
    ])
    .await?;
    // two sheds and then the refusal that is not tried again
    assert_eq!(sent, 3, "the bundle was tried {sent} times");
    assert_eq!(
        code_of(&error),
        Some(ErrorCode::StorageWrite),
        "a bundle nothing applied reached its caller as {error:?}"
    );
    Ok(())
}
