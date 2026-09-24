//! A client that goes away before its answers are written
//!
//! Every shard holds a channel to every client's write relay, and a reply is written into that
//! channel by whichever shard answered. When the client closes its socket the relay ends and the
//! channel with it, and the next reply for that client has nowhere to go. Before
//! [F38](../../docs/src/features/inter-node-transport.md) that reply took the shard with it,
//! and every other client on that shard ([items 32 and 94](../../docs/src/appendix/resolved/disconnected-client-cleanup.md)):
//! `reply_sealed` returned the closed channel's error through `?` in the shard loop, and a shard
//! loop that returns is a shard that has died. With peer links that get cut and reconnect on
//! backoff, that was reachable on every cut.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::shared::protocol::auth::AuthMechanisms;
use shoal::shared::protocol::{self, handshake};
use shoal::shared::queries::Queries;
use shoal::shared::traits::{QuerySupport, RkyvSupport};
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalUnsortedTable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
/// A persistent table on purpose: an insert's answer waits on its fsync, which is what puts the
/// answer after the close.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub rows: PersistentUnsortedTable<TestRecord, FileSystem>,
}

/// Open a connection, shake hands, and hand back the socket
///
/// # Arguments
///
/// * `addr` - The server to connect to
async fn handshaken(addr: &str) -> Result<TcpStream, TestError> {
    let mut sock = TcpStream::connect(addr).await?;
    let hello = handshake::Hello {
        schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
        max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
        mechanisms: AuthMechanisms::NONE,
        caps: 0,
    };
    sock.write_all(
        &hello
            .frame(protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a hello frame"),
    )
    .await?;
    sock.flush().await?;
    // read the ack and check we were let in
    let mut frame = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut frame).await?;
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&frame[protocol::HEADER_LEN..]);
    assert!(handshake::HelloAck::decode(&body).reason.is_accepted());
    Ok(sock)
}

/// A client that closes its socket with answers still owed does not end the shard
///
/// Twenty connections each write a bundle of two hundred inserts and close at once. The inserts
/// are answered after their fsync, into a channel whose relay has already ended. The server has
/// to keep every shard, and a client that connects afterwards has to be answered.
#[tokio::test]
async fn a_client_that_leaves_before_its_answers_does_not_end_the_shard() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::build_config(&temp_dir);
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    // leave with answers owed, twenty times over, so the window is hit on every shard
    for round in 0..20u64 {
        // a handshake that never answers is a shard that died on an earlier round
        let Ok(handshake) =
            tokio::time::timeout(std::time::Duration::from_secs(10), handshaken(&addr)).await
        else {
            panic!(
                "round {round}: the server never answered a handshake; the shards report {:?}",
                pool.failure()
            );
        };
        let mut sock = handshake?;
        let mut bundle = Queries::<TestDbClient>::default();
        for i in 0..200u64 {
            bundle = bundle.add(TestRecord {
                key: round * 1000 + i,
                data: "written and never read".to_owned(),
            });
        }
        let payload = RkyvSupport::serialize(&bundle).expect("a bundle this test built archives");
        let preamble = protocol::request_preamble(payload.len(), protocol::DEFAULT_MAX_FRAME_BYTES)
            .expect("a request preamble");
        sock.write_all(&preamble).await?;
        sock.write_all(&payload).await?;
        sock.flush().await?;
        // gone before a single answer could have been written
        drop(sock);
    }
    // give every fsync time to land and every reply time to find nowhere to go, watching for
    // the shard death the whole while so the failure is a message rather than a hang
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            pool.failure(),
            None,
            "a shard died answering a client that had left"
        );
    }
    // and a client that arrives afterwards is answered by every shard
    let connected = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        shoal::client::Shoal::<TestDbClient>::new(&addr),
    )
    .await;
    let Ok(client) = connected else {
        panic!(
            "a client could not connect within twenty seconds; the shards report {:?}",
            pool.failure()
        );
    };
    let client = client?;
    let served = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for key in 0..16u64 {
            client
                .send_one(TestRecord {
                    key: u64::MAX - key,
                    data: "still here".to_owned(),
                })
                .await?;
            let response = client
                .send_one(TestRecordGet::new(vec![u64::MAX - key]))
                .await?;
            let rows = response
                .access::<TestRecord>()?
                .expect("a get that found nothing");
            assert_eq!(rows.len(), 1);
        }
        Ok::<(), TestError>(())
    })
    .await;
    match served {
        Ok(outcome) => outcome?,
        Err(_) => panic!(
            "a client was not answered within twenty seconds; the shards report {:?}",
            pool.failure()
        ),
    }
    drop(client);
    assert_eq!(pool.failure(), None);
    pool.exit()?;
    Ok(())
}
