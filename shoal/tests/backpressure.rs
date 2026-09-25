//! The admission bound on the shard mesh
//!
//! Every channel between a node's shards is unbounded, so a shard that fell behind grew its
//! queue until the process was killed and nothing throttled a client
//! ([Resolved #15](../../docs/src/appendix/resolved/shard-mesh-admission.md)). The bound is at
//! admission: a client's query bound for a shard whose queue already holds
//! `networking.max_queued_queries` messages is answered `Shedding` by the shard that accepted
//! it, at once, and never enqueued. This is the test that holds one shard and watches the other
//! turn queries away.
//!
//! The mesh counts only what waits on a queue, so the remainder of the item was what leaves it
//! ([Resolved #15](../../docs/src/appendix/resolved/backlog-bounds.md)). The answers owed to a
//! client that stops reading them are the one half of that a whole server can show: a connection
//! owing `networking.max_queued_replies` answers is not read again until they drain. The two
//! table bounds are tested in `table_backlog.rs`, where a slow device can be held still.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{Errors, PoolConfig, SendOptions, Shoal};
use shoal::shared::protocol::auth::AuthMechanisms;
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::protocol::{self, handshake};
use shoal::shared::queries::Queries;
use shoal::shared::traits::{QuerySupport, RkyvSupport};
use shoal::tables::EphemeralUnsortedTable;
use shoal::ShoalPool;
use shoal_derive::{db, ShoalUnsortedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;

mod utils;

use utils::TestError;

/// A row in the table this test queries
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

/// The schema this test runs against
///
/// An ephemeral table: the bound is about the mesh, not about storage.
#[db]
pub struct TestDb {
    /// The only table in this schema
    pub test_records: EphemeralUnsortedTable<TestRecord>,
}

/// A query bound for a shard that has fallen behind is shed at once, and answered by name
///
/// Two shards, a bound of eight, shard one held for two seconds. Two hundred gets over distinct
/// keys through a pool of twenty connections: the shares that reach shard zero's coordinator
/// bound for shard one are shed once its queue holds eight - `Shedding`, inside a second, never
/// two seconds on a queue nothing drains - and the rest are answered, some after the hold. Then
/// the held shard answers again, and the transport view counts what was shed. Against the tree
/// before the bound nothing is shed and every query to shard one waits the hold out.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_for_a_shard_that_fell_behind_is_shed() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // two shards, and a bound low enough that a held shard reaches it inside the hold
    let conf = utils::build_config(&temp_dir).networking(
        shoal::server::conf::Networking::default()
            .port(0)
            .max_queued_queries(8),
    );
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    // rows on every key, so a get has something to find once it runs
    let client = std::sync::Arc::new(
        Shoal::<TestDbClient>::builder()
            .endpoints(vec![addr.to_string()])
            .pool(PoolConfig {
                min_idle: 20,
                max_size: 20,
                ..PoolConfig::default()
            })
            .build()
            .await?,
    );
    for key in 0..200u64 {
        client
            .send_one(TestRecord {
                key,
                data: format!("row {key}"),
            })
            .await?;
    }
    // hold shard one where it stands: its queue grows behind it
    pool.hold_shard(1, 2_000)?;
    let held = Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // two hundred gets at once, each on its own connection when one is free
    let mut tasks = tokio::task::JoinSet::new();
    for key in 0..200u64 {
        let client = client.clone();
        tasks.spawn(async move {
            let sent = Instant::now();
            let outcome = client.send_one(TestRecordGet::new(vec![key])).await;
            (key, sent.elapsed(), outcome)
        });
    }
    let mut shed = 0u32;
    let mut answered = 0u32;
    let mut fastest_shed = Duration::MAX;
    while let Some(done) = tasks.join_next().await {
        let (key, took, outcome) = done.expect("a get task panicked");
        match outcome {
            Ok(_) => answered += 1,
            Err(Errors::Server { code, .. }) if code == ErrorCode::Shedding => {
                shed += 1;
                fastest_shed = fastest_shed.min(took);
            }
            Err(error) => panic!("get {key} failed for another reason: {error:?}"),
        }
    }
    eprintln!(
        "{shed} gets shed, the fastest in {fastest_shed:?}; {answered} answered; {:?} since the hold",
        held.elapsed()
    );
    // the bound turned queries away - and at once, for a query that had a connection to be
    // turned away on; a task's elapsed time includes its wait for one of the twenty, half of
    // which sit on the held shard's coordinator for the whole hold
    assert!(shed > 0, "no query was shed while shard one was held");
    assert!(
        fastest_shed < Duration::from_millis(500),
        "the fastest refusal took {fastest_shed:?}"
    );
    assert!(answered > 0, "every query was shed");
    // the held shard is back, and answers
    if let Some(left) = Duration::from_millis(2_500).checked_sub(held.elapsed()) {
        tokio::time::sleep(left).await;
    }
    let found = client.send_one(TestRecordGet::new(vec![7])).await?;
    assert!(
        found
            .access::<TestRecord>()?
            .is_some_and(|rows| rows.len() == 1),
        "the held shard did not answer once released"
    );
    // a shed query is safe to ask again, and the client does when told to
    let retried = client
        .send_one_with(
            TestRecordGet::new(vec![8]),
            &SendOptions::new().retry(Duration::from_secs(5)),
        )
        .await?;
    assert!(retried.access::<TestRecord>()?.is_some());
    // and the transport view says how many were turned away
    let views = pool.transport()?;
    let counted: u64 = views.iter().map(|view| view.shed).sum();
    assert_eq!(
        counted,
        u64::from(shed),
        "the transport view counts the shed queries"
    );
    pool.exit()?;
    Ok(())
}

/// The size of the row a slow reader asks for, and of every insert it writes
const WIDE: usize = 256 * 1024;

/// How many gets a slow reader asks for before it stops reading
const GETS: usize = 64;

/// How many inserts a slow reader writes after that, which is sixty-four mebibytes of them
const INSERTS: usize = 256;

/// Frame one query as a whole request, preamble and bundle
///
/// # Arguments
///
/// * `bundle` - The bundle to frame
fn request_frame(bundle: &Queries<TestDbClient>) -> Vec<u8> {
    // the bundle's archive, behind the preamble that says how long it is
    let payload = RkyvSupport::serialize(bundle).expect("a bundle this test built archives");
    let mut frame = protocol::request_preamble(payload.len(), protocol::DEFAULT_MAX_FRAME_BYTES)
        .expect("a request preamble")
        .to_vec();
    frame.extend_from_slice(&payload);
    frame
}

/// A client that stops reading its answers stops being read, and loses nothing
///
/// A raw connection with small socket buffers asks for sixty-four answers of a quarter mebibyte
/// each and reads none of them, so the server's write relay stalls on a full socket with most of
/// them still owed, past the bound of eight. It then writes sixty-four mebibytes of inserts. A
/// server that still reads it takes every byte, holding every answer; one that stops reading it
/// stalls the client's writes at what the socket buffers hold. Then the client reads, and every
/// query it sent is answered - nothing was refused or dropped, only held back.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_reading_stops_being_read() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // a bound of eight answers per connection
    let conf = utils::build_config(&temp_dir).networking(
        shoal::server::conf::Networking::default()
            .port(0)
            .max_queued_replies(8),
    );
    let mut pool = ShoalPool::<TestDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?;
    // one wide row for the gets to find
    let client = Shoal::<TestDbClient>::new(&addr.to_string()).await?;
    client
        .send_one(TestRecord {
            key: 0,
            data: "g".repeat(WIDE),
        })
        .await?;
    // a raw connection whose own buffers are small, so what the kernel holds for it is too
    let socket = TcpSocket::new_v4()?;
    socket.set_send_buffer_size(64 * 1024)?;
    socket.set_recv_buffer_size(64 * 1024)?;
    let mut sock = socket.connect(addr).await?;
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
    let mut ack = [0u8; handshake::HANDSHAKE_FRAME_LEN];
    sock.read_exact(&mut ack).await?;
    let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
    body.copy_from_slice(&ack[protocol::HEADER_LEN..]);
    assert!(handshake::HelloAck::decode(&body).reason.is_accepted());
    let (mut reader, mut writer) = sock.into_split();
    // the gets first, then the inserts, written by a task that counts what the socket took
    let get = request_frame(&Queries::<TestDbClient>::default().add(TestRecordGet::new(vec![0])));
    let written = Arc::new(AtomicUsize::new(0));
    let progress = written.clone();
    let writes = tokio::spawn(async move {
        for _ in 0..GETS {
            writer.write_all(&get).await?;
            progress.fetch_add(get.len(), Ordering::SeqCst);
        }
        for key in 1..=INSERTS as u64 {
            let insert = request_frame(&Queries::<TestDbClient>::default().add(TestRecord {
                key,
                data: "i".repeat(WIDE),
            }));
            writer.write_all(&insert).await?;
            progress.fetch_add(insert.len(), Ordering::SeqCst);
        }
        writer.flush().await?;
        Ok::<_, std::io::Error>(writer)
    });
    // wait for the writes to finish or to stop moving for two seconds
    let total = GETS * 64 + INSERTS * WIDE;
    let mut last = 0;
    let mut still = Instant::now();
    let stalled_at = loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = written.load(Ordering::SeqCst);
        if writes.is_finished() {
            break None;
        }
        if now != last {
            last = now;
            still = Instant::now();
        } else if still.elapsed() > Duration::from_secs(2) {
            break Some(now);
        }
    };
    eprintln!(
        "the client's writes stalled at {stalled_at:?} of about {total} bytes, owing {GETS} answers"
    );
    // the server stopped reading a client that stopped reading, well short of what it sent
    let Some(stalled_at) = stalled_at else {
        panic!("the server read all {total} bytes from a client that read none of its answers");
    };
    assert!(
        stalled_at < 32 * 1024 * 1024,
        "the server read {stalled_at} bytes before it stopped reading"
    );
    // now read: every query the client sent is answered, one frame each, and the writes finish
    let answered = tokio::time::timeout(Duration::from_secs(60), async {
        let mut frames = 0;
        let mut header = [0u8; protocol::HEADER_LEN];
        let mut body = Vec::new();
        while frames < GETS + INSERTS {
            reader.read_exact(&mut header).await?;
            let raw = protocol::RawHeader::decode(&header);
            body.resize(raw.len as usize, 0);
            reader.read_exact(&mut body).await?;
            frames += 1;
        }
        Ok::<_, std::io::Error>(frames)
    })
    .await
    .expect("the answers did not all arrive within a minute")?;
    assert_eq!(answered, GETS + INSERTS, "a query was never answered");
    writes.await.expect("the writer panicked")?;
    pool.exit()?;
    Ok(())
}
