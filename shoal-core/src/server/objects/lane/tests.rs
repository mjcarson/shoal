//! The object lane between two stores over loopback: dialled by one, accepted and handed to the
//! other's slice owner, and a whole chunk written, confirmed and read back across it

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::AsyncWriteExt;
use glommio::net::TcpListener;

use super::LanePlan;
use crate::server::conf::cluster::Transport;
use crate::server::control::types::{MemberHealth, MemberPhase, MemberRole};
use crate::server::devices::{DeviceState, Facts};
use crate::server::map::{MapCell, MapMember, TabletMap};
use crate::server::objects::budget::Budget;
use crate::server::objects::cpus::ObjectCpu;
use crate::server::objects::executor::{answer_of, Holder, ObjectPlan, ObjectStore};
use crate::server::objects::serve::Streams;
use crate::server::objects::store::{ChunkId, SliceOptions};
use crate::server::peer::handoff::HandedLane;
use crate::server::peer::handshake::{self, Local, PeerAddr};
use crate::server::peer::Lane;
use crate::shared::checksum::crc64;
use crate::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;
use crate::shared::protocol::peer::chunk::{ChunkBytes, ChunkReply, ChunkRequest, Piece, StoreCode, UnitBytes};
use crate::shared::protocol::peer::CAP_OBJECT_LANE_V1;
use crate::shared::protocol::{MessageType, CLASS_WIRE_VERSION, OBJECT_WIRE_VERSION, PROTOCOL_VERSION};
use crate::shared::tls::{PeerIdentity, PeerTlsHolder};

/// A unit of 64 KiB
const UNIT: u32 = 64 << 10;
/// A chunk of four units
const CHUNK: u64 = 4 * UNIT as u64;

/// A directory on the repository's own filesystem, where direct I/O is real
fn dir() -> tempfile::TempDir {
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/store-tests");
    std::fs::create_dir_all(&target).expect("made");
    tempfile::tempdir_in(target).expect("a directory")
}

/// What a node says about itself in its hellos
///
/// # Arguments
///
/// * `node` - The node
/// * `cluster` - Its cluster
fn local(node: NodeId, cluster: ClusterId) -> Local {
    Local {
        node,
        cluster: Some(cluster),
        shards: 2,
        schema_id: 0x0b1e_c71a,
        lineage: vec![0x0b1e_c71a],
        committed: None,
        max_frame_bytes: 8 << 20,
        incarnation: 1,
        wire_max: PROTOCOL_VERSION,
    }
}

/// A map of two members, the second listening at an address, with the object wire activated
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `a` - The first member
/// * `b` - The second member
/// * `at` - Where the second listens
fn map(cluster: ClusterId, a: NodeId, b: NodeId, at: SocketAddr) -> TabletMap {
    let member = |node: NodeId, data: String| MapMember {
        node,
        client: "127.0.0.1:0".to_string(),
        data,
        control: "127.0.0.1:1".to_string(),
        shards: 2,
        role: MemberRole::Voter,
        health: MemberHealth::Up,
        incarnation: 1,
        shards_failed: Vec::new(),
        quarantined: Vec::new(),
        phase: MemberPhase::Member,
        lead_weight: 0,
    };
    TabletMap {
        version: 1,
        cluster: Some(cluster),
        members: [
            (a, member(a, "127.0.0.1:2".to_string())),
            (b, member(b, at.to_string())),
        ]
        .into_iter()
        .collect(),
        placement: vec![a, b],
        activated_wire: OBJECT_WIRE_VERSION,
        ..TabletMap::default()
    }
}

/// A node's object store of one executor and one slice under a directory
///
/// # Arguments
///
/// * `root` - The device's directory
/// * `node` - The node
/// * `slice` - The slice's id
/// * `lanes` - How it dials an object lane
fn store(root: &Path, node: NodeId, slice: SliceId, lanes: Option<LanePlan>) -> ObjectStore {
    store_with(root, node, slice, lanes, 0, None)
}

/// A node's object store of one executor and one slice under a directory, with a cache of read
/// units and its device's recovery ceiling as given
///
/// # Arguments
///
/// * `root` - The device's directory
/// * `node` - The node
/// * `slice` - The slice's id
/// * `lanes` - How it dials an object lane
/// * `cache` - The node's cache of read units, in bytes, none at 0
/// * `ceiling` - What the device's recovery and scrubs may move a second, the default if none
fn store_with(
    root: &Path,
    node: NodeId,
    slice: SliceId,
    lanes: Option<LanePlan>,
    cache: u64,
    ceiling: Option<u64>,
) -> ObjectStore {
    // the device's spec, its ceiling named when the test sets one
    let spec = match ceiling {
        Some(bytes) => DeviceSpec::new(root, "ssd").with_recovery_ceiling(bytes),
        None => DeviceSpec::new(root, "ssd"),
    };
    let device = Arc::new(DeviceState::new(
        DeviceId::mint(),
        spec,
        Facts::default(),
        0,
        ApplySync::Shared,
        vec![(slice, root.join("slice-0"))],
        Vec::new(),
    ));
    ObjectStore::start(ObjectPlan {
        cpus: vec![ObjectCpu { cpu: 0, blocking: 0 }],
        budget: 64 << 20,
        cache,
        // the least glommio's rings take: every ring's registered buffers are charged to the user's
        // locked memory, 8 MiB at a login, and a mebibyte registered in each of three rings for each
        // of two stores a test, six tests at once, spent it alone
        io_memory: 64 << 10,
        slices: vec![(
            device.clone(),
            device.slices[0].clone(),
            SliceOptions {
                segment_bytes: 1 << 20,
                free_files: 1,
                chunk_lengths: vec![CHUNK],
                budget: None,
                keep_previous: true,
                apply_queue: None,
            },
        )],
        node,
        lanes,
    })
    .expect("the store starts")
}

/// Seeded bytes
///
/// # Arguments
///
/// * `len` - How many
fn bytes(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|index| (crate::shared::rendezvous::mix64(index) >> 24) as u8)
        .collect()
}

/// A piece of a chunk with its units' checksums
///
/// # Arguments
///
/// * `chunk` - The chunk
/// * `label` - Its label
/// * `offset` - Where the piece starts
/// * `bytes` - Its bytes, whole units
/// * `last` - Whether it is the chunk's last
fn piece(chunk: ChunkId, label: Label, offset: u64, bytes: Vec<u8>, last: bool) -> Piece {
    Piece {
        stream: 7,
        chunk,
        label,
        chunk_bytes: CHUNK,
        unit_bytes: UNIT,
        length: CHUNK,
        offset,
        last,
        stripe_length: CHUNK,
        owner: 0,
        crcs: bytes.chunks(UNIT as usize).map(crc64).collect(),
        bytes: bytes.into(),
    }
}

/// Accept one object lane on a listener of its own once told the map, and hand it to a store
///
/// Returns the acceptor, where it listens, and where the map is sent.
///
/// # Arguments
///
/// * `store` - The store whose slice the lane names
/// * `local` - What its node says in its hellos
/// * `slice` - The slice the lane must name
fn accept_one(
    store: ObjectStore,
    local: Local,
    slice: SliceId,
) -> (
    glommio::ExecutorJoinHandle<()>,
    SocketAddr,
    std::sync::mpsc::Sender<Arc<TabletMap>>,
) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let (map_tx, map_rx) = std::sync::mpsc::channel::<Arc<TabletMap>>();
    let acceptor = glommio::LocalExecutorBuilder::default()
        .spawn(move || async move {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            addr_tx
                .send(listener.local_addr().expect("an address"))
                .expect("the test waits");
            let map = loop {
                match map_rx.try_recv() {
                    Ok(map) => break MapCell::new(map),
                    Err(_) => glommio::timer::sleep(Duration::from_millis(1)).await,
                }
            };
            let mut stream = listener.accept().await.expect("accept");
            let accepted = handshake::accept(
                &mut stream,
                &local,
                &[Lane::Object],
                &map,
                &PeerIdentity::Plaintext,
                true,
            )
            .await
            .expect("let in");
            // the hello named the slice and agreed the lane
            assert_eq!(accepted.lane, Lane::Object);
            assert_eq!(accepted.slice, Some(slice));
            assert!(accepted.negotiated.has(CAP_OBJECT_LANE_V1));
            let lane =
                HandedLane::release(stream, accepted.node, accepted.negotiated, None, accepted.slice);
            assert!(store.adopt(lane), "the slice's owner takes the lane");
        })
        .expect("the acceptor");
    let at = addr_rx.recv().expect("an address");
    (acceptor, at, map_tx)
}

/// A whole chunk written to a slice on another node arrives through its owner's lane, and reads
/// back the same; a lane naming a slice its node does not serve is never served
#[test]
fn a_chunk_crosses_the_object_lane_to_its_slices_owner() {
    let cluster = ClusterId::mint();
    let (a, b) = (NodeId::mint(), NodeId::mint());
    let slice_a = SliceId::mint();
    let slice_b = SliceId::mint();
    let (root_a, root_b) = (dir(), dir());
    // B listens, and once told the map, accepts one object lane and hands it to its store
    let store_b = store(root_b.path(), b, slice_b, None);
    let (acceptor, at, map_tx) = accept_one(store_b.clone(), local(b, cluster), slice_b);
    let map = Arc::new(map(cluster, a, b, at));
    map_tx.send(map.clone()).expect("the acceptor waits");
    // A dials B's slice over the lane its executors keep
    let store_a = store(
        root_a.path(),
        a,
        slice_a,
        Some(LanePlan {
            local: local(a, cluster),
            dial: std::collections::BTreeMap::new(),
            tls: PeerTlsHolder::build(None).expect("plaintext"),
            transport: Transport::default(),
        }),
    );
    store_a.install_map(&map);
    let holder = Holder { node: b, slice: slice_b };
    let chunk = ChunkId {
        consumer: 1,
        pg: PlacementGroup { tablet: 1, sub: 0 },
        object: 99,
        stripe: 0,
        position: 1,
    };
    let label = Label { sequence: 0, tag: 0x7a6 };
    let written = bytes(CHUNK as usize);
    futures::executor::block_on(async {
        // two pieces, each answered, the last once the chunk is durable on B
        let half = (CHUNK / 2) as usize;
        let first = store_a
            .send(holder, ChunkRequest::Write(piece(chunk, label, 0, written[..half].to_vec(), false)))
            .await
            .expect("sent");
        let last = store_a
            .send(holder, ChunkRequest::Write(piece(chunk, label, half as u64, written[half..].to_vec(), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(first).await, ChunkReply::Done);
        assert_eq!(answer_of(last).await, ChunkReply::Done);
        // B's slice holds it at its label, read on B's own store
        assert_eq!(
            store_b.chunk_label(slice_b, chunk).await.expect("asked"),
            Some(label)
        );
        // and a read across the lane gives back every byte
        let read = store_a
            .ask_holder(
                holder,
                ChunkRequest::Read {
                    chunk,
                    offset: 0,
                    len: CHUNK,
                    labels: vec![label],
                },
            )
            .await;
        assert_eq!(read, ChunkReply::Bytes { label, bytes: written.clone().into() });
        // a label it cannot make is answered with the one it holds
        let other = Label { sequence: 3, tag: 1 };
        let confirm = store_a
            .ask_holder(holder, ChunkRequest::Confirm { chunk, labels: vec![other] })
            .await;
        assert_eq!(confirm, ChunkReply::Held { label: Some(label) });
        // a holder on a node the map does not name is refused as unavailable, at once
        let nowhere = store_a
            .ask_holder(
                Holder { node: NodeId::mint(), slice: slice_b },
                ChunkRequest::Confirm { chunk, labels: vec![label] },
            )
            .await;
        assert!(
            matches!(nowhere, ChunkReply::Refused { code: StoreCode::Unavailable, .. }),
            "{nowhere:?}"
        );
        // one lane dialled and served, every request over it answered on B
        let (sent, served) = (store_a.lane_figures().await, store_b.lane_figures().await);
        assert_eq!((sent.links, sent.sent, sent.shed), (1, 4, 0));
        assert_eq!((served.adopted, served.served), (1, 4));
    });
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A lane whose queue is at its bound holds its sender until the holder takes the bytes ahead of
/// it, rather than refusing a request a live holder would have taken
/// ([Resolved #238](../../../../../docs/src/appendix/resolved/object-lane-backpressure.md))
#[test]
fn a_full_lane_holds_its_sender_until_the_holder_takes_its_bytes() {
    let cluster = ClusterId::mint();
    let (a, b) = (NodeId::mint(), NodeId::mint());
    let (slice_a, slice_b) = (SliceId::mint(), SliceId::mint());
    let (root_a, root_b) = (dir(), dir());
    let store_b = store(root_b.path(), b, slice_b, None);
    let (acceptor, at, map_tx) = accept_one(store_b.clone(), local(b, cluster), slice_b);
    let map = Arc::new(map(cluster, a, b, at));
    map_tx.send(map.clone()).expect("the acceptor waits");
    // A's lane holds two chunks' requests at most
    let transport = Transport {
        object_queue_bytes: 2 * CHUNK as usize + (64 << 10),
        ..Transport::default()
    };
    let store_a = store(
        root_a.path(),
        a,
        slice_a,
        Some(LanePlan {
            local: local(a, cluster),
            dial: std::collections::BTreeMap::new(),
            tls: PeerTlsHolder::build(None).expect("plaintext"),
            transport,
        }),
    );
    store_a.install_map(&map);
    let holder = Holder { node: b, slice: slice_b };
    // eight whole chunks sent back to back from A's executor, as a put's pipeline sends them,
    // without waiting for any answer before the next
    let (done_tx, done_rx) = std::sync::mpsc::channel::<Vec<ChunkReply>>();
    store_a
        .run(Box::new(move |store| {
            Box::pin(async move {
                let mut answers = Vec::new();
                for object in 0..8u128 {
                    let chunk = ChunkId {
                        consumer: 1,
                        pg: PlacementGroup { tablet: 1, sub: 0 },
                        object,
                        stripe: 0,
                        position: 1,
                    };
                    let label = Label { sequence: 0, tag: 0x7a6 };
                    answers.push(
                        store
                            .send(holder, ChunkRequest::Write(piece(chunk, label, 0, bytes(CHUNK as usize), true)))
                            .await,
                    );
                }
                // then every answer, a refusal at the send included
                let mut replies = Vec::new();
                for answer in answers {
                    replies.push(match answer {
                        Ok(answer) => answer_of(answer).await,
                        Err(refused) => refused,
                    });
                }
                let _ = done_tx.send(replies);
            })
        }))
        .expect("the job runs");
    let replies = done_rx.recv_timeout(Duration::from_secs(60)).expect("every answer");
    // every chunk is written: B was taking bytes the whole time, so nothing was refused
    assert!(
        replies.iter().all(|reply| *reply == ChunkReply::Done),
        "a live holder's requests were refused at a full lane: {replies:?}"
    );
    futures::executor::block_on(async {
        let sent = store_a.lane_figures().await;
        assert_eq!((sent.sent, sent.shed), (8, 0), "{sent:?}");
        // and the sender waited for room rather than queueing past the bound
        assert!(sent.waited > 0, "no request waited at a lane of two chunks: {sent:?}");
    });
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A holder sizes a whole chunk's file by its class at once on a node of no cluster, and on a
/// cluster node only once the map it installed has activated wire 16: never before its first map,
/// which holds no member and the floor's activation (F96)
#[test]
fn a_cluster_node_sizes_files_by_class_only_past_wire_16() {
    glommio::LocalExecutorBuilder::default()
        .spawn(|| async {
            let cluster = ClusterId::mint();
            let (a, b) = (NodeId::mint(), NodeId::mint());
            // a node of no cluster has no plan to dial by, and sizes by class from its start
            let alone = super::Lanes::start(
                None,
                Streams::default(),
                Budget::new(1 << 20),
                Default::default(),
                None,
                None,
            );
            assert!(alone.classed(), "a node of no cluster waited for an activation it never has");
            alone.stop();
            // a cluster node's lanes, before their first map and at each activation after it
            let plan = LanePlan {
                local: local(a, cluster),
                dial: std::collections::BTreeMap::new(),
                tls: PeerTlsHolder::build(None).expect("plaintext"),
                transport: Transport::default(),
            };
            let lanes = super::Lanes::start(
                Some(plan),
                Streams::default(),
                Budget::new(1 << 20),
                Default::default(),
                None,
                None,
            );
            assert!(!lanes.classed(), "a cluster node sized files by class before its first map");
            let mut installed = map(cluster, a, b, "127.0.0.1:9".parse().expect("an address"));
            for (activated, classed) in [(OBJECT_WIRE_VERSION, false), (CLASS_WIRE_VERSION - 1, false), (CLASS_WIRE_VERSION, true)] {
                installed.activated_wire = activated;
                lanes.install(Arc::new(installed.clone()));
                assert_eq!(lanes.classed(), classed, "at wire {activated}");
            }
            lanes.stop();
        })
        .expect("an executor")
        .join()
        .expect("the test ran");
}

/// Two stores, A dialling B's slice over one object lane, B keeping a cache of read units and its
/// device's ceiling as given
///
/// Returns A's store, B's store, B's slice as a holder, and B's acceptor to join.
///
/// # Arguments
///
/// * `root_a` - A's device's directory
/// * `root_b` - B's device's directory
/// * `cache` - B's cache of read units, in bytes, none at 0
/// * `ceiling` - What B's device's recovery and scrubs may move a second, the default if none
fn pair(
    root_a: &Path,
    root_b: &Path,
    cache: u64,
    ceiling: Option<u64>,
) -> (ObjectStore, ObjectStore, Holder, glommio::ExecutorJoinHandle<()>) {
    pair_over(root_a, root_b, cache, ceiling, Transport::default())
}

/// Two stores, A dialling B's slice over one object lane under the bounds and timers given, B
/// keeping a cache of read units and its device's ceiling as given
///
/// Returns A's store, B's store, B's slice as a holder, and B's acceptor to join.
///
/// # Arguments
///
/// * `root_a` - A's device's directory
/// * `root_b` - B's device's directory
/// * `cache` - B's cache of read units, in bytes, none at 0
/// * `ceiling` - What B's device's recovery and scrubs may move a second, the default if none
/// * `transport` - The bounds and timers A's lane keeps
fn pair_over(
    root_a: &Path,
    root_b: &Path,
    cache: u64,
    ceiling: Option<u64>,
    transport: Transport,
) -> (ObjectStore, ObjectStore, Holder, glommio::ExecutorJoinHandle<()>) {
    let cluster = ClusterId::mint();
    let (a, b) = (NodeId::mint(), NodeId::mint());
    let (slice_a, slice_b) = (SliceId::mint(), SliceId::mint());
    // B listens, and once told the map, accepts one object lane and hands it to its store
    let store_b = store_with(root_b, b, slice_b, None, cache, ceiling);
    let (acceptor, at, map_tx) = accept_one(store_b.clone(), local(b, cluster), slice_b);
    let map = Arc::new(map(cluster, a, b, at));
    map_tx.send(map.clone()).expect("the acceptor waits");
    // A dials B's slice over the lane its executors keep
    let store_a = store(
        root_a,
        a,
        slice_a,
        Some(LanePlan {
            local: local(a, cluster),
            dial: std::collections::BTreeMap::new(),
            tls: PeerTlsHolder::build(None).expect("plaintext"),
            transport,
        }),
    );
    store_a.install_map(&map);
    (store_a, store_b, Holder { node: b, slice: slice_b }, acceptor)
}

/// A chunk of one placement group, by its object
///
/// # Arguments
///
/// * `object` - The object it is a chunk of
fn chunk_of(object: u128) -> ChunkId {
    ChunkId {
        consumer: 1,
        pg: PlacementGroup { tablet: 1, sub: 0 },
        object,
        stripe: 0,
        position: 1,
    }
}

/// Ask a holder for a chunk's bytes, and say what its node's cache of read units counted while it
/// answered: its hits, its misses and the units it filled
///
/// # Arguments
///
/// * `asker` - The store that asks
/// * `holder` - The holder asked
/// * `request` - The read, wrapped in its class or not
/// * `cached` - The holder's store, whose cache is counted
/// * `expected` - The answer it must give
async fn read_counted(
    asker: &ObjectStore,
    holder: Holder,
    request: ChunkRequest,
    cached: &ObjectStore,
    expected: &ChunkReply,
) -> (u64, u64, u64) {
    // the cache's counts before and after one answered read
    let before = cached.cache_figures();
    let answer = asker.ask_holder(holder, request).await;
    assert_eq!(&answer, expected, "the read was not answered in full");
    let after = cached.cache_figures();
    (
        after.hits - before.hits,
        after.misses - before.misses,
        after.filled - before.filled,
    )
}

/// A rebuild's or a move's read is served as recovery's, through its executor's own dispatch and
/// across the lane alike: it neither takes from its holder's cache of read units nor fills it,
/// where a reader's read fills it (F102,
/// [item 299](../../../../../docs/src/appendix/resolved/a-gate-keeps-a-requests-class.md))
#[test]
fn a_background_read_leaves_the_cache_as_it_was() {
    let (root_a, root_b) = (dir(), dir());
    // B keeps a cache that holds a chunk four times over
    let (store_a, store_b, holder, acceptor) = pair(root_a.path(), root_b.path(), 1 << 20, None);
    let chunk = chunk_of(99);
    let label = Label { sequence: 0, tag: 0x7a6 };
    let written = bytes(CHUNK as usize);
    let read = ChunkRequest::Read {
        chunk,
        offset: 0,
        len: CHUNK,
        labels: vec![label],
    };
    let background = ChunkRequest::Background(Box::new(read.clone()));
    let expected = ChunkReply::Bytes {
        label,
        bytes: written.clone().into(),
    };
    futures::executor::block_on(async {
        // the chunk written whole to B's slice, on B's own store
        let wrote = store_b
            .send(holder, ChunkRequest::Write(piece(chunk, label, 0, written.clone(), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        // a rebuild's read through B's executor's own dispatch, and one across the lane
        let own = read_counted(&store_b, holder, background.clone(), &store_b, &expected).await;
        let across = read_counted(&store_a, holder, background.clone(), &store_b, &expected).await;
        // a reader's read across the lane looks, misses, and fills the cache with the chunk's units
        let reader = read_counted(&store_a, holder, read.clone(), &store_b, &expected).await;
        // and a rebuild's reads still never take what it holds
        let own_after = read_counted(&store_b, holder, background.clone(), &store_b, &expected).await;
        let across_after = read_counted(&store_a, holder, background.clone(), &store_b, &expected).await;
        assert_eq!(
            [own, across, own_after, across_after],
            [(0, 0, 0); 4],
            "a background read was served as a reader's, through the executor and across the lane, \
             before and after a reader's read (hits, misses, units filled)"
        );
        assert_eq!(reader, (0, 1, 4), "a reader's read did not fill the cache with the chunk's four units");
    });
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A light scrub's listing draws a block of its device's recovery ceiling for every chunk header
/// it reads, through its executor's own dispatch and across the lane alike, so a scrub keeps to
/// its device's pace however many chunks a slice holds (F89,
/// [item 299](../../../../../docs/src/appendix/resolved/a-gate-keeps-a-requests-class.md))
#[test]
fn a_scrub_listing_draws_on_its_devices_ceiling() {
    let (root_a, root_b) = (dir(), dir());
    // B's device may move one 4 KiB block a second, so a listing of three chunks waits for two
    let (store_a, store_b, holder, acceptor) = pair(root_a.path(), root_b.path(), 0, Some(4096));
    let label = Label { sequence: 0, tag: 0x7a6 };
    let chunks: Vec<ChunkId> = (100..103).map(chunk_of).collect();
    // the group's whole listing, from its start
    let listing = ChunkRequest::Scrub(Box::new(ChunkRequest::List {
        after: ChunkId {
            object: 0,
            ..chunks[0]
        },
        start: true,
        limit: 16,
    }));
    futures::executor::block_on(async {
        // three chunks of one placement group written whole to B's slice
        for chunk in &chunks {
            let wrote = store_b
                .send(holder, ChunkRequest::Write(piece(*chunk, label, 0, bytes(CHUNK as usize), true)))
                .await
                .expect("sent");
            assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        }
        // a scrub's listing through B's executor's own dispatch, then one across the lane
        let mut took = Vec::new();
        for asker in [&store_b, &store_a] {
            let begun = std::time::Instant::now();
            let answer = asker.ask_holder(holder, listing.clone()).await;
            took.push(begun.elapsed());
            // every chunk listed, at the label it was written at
            match answer {
                ChunkReply::Listed { chunks: listed, more } => {
                    assert_eq!(listed.iter().map(|entry| entry.chunk).collect::<Vec<_>>(), chunks);
                    assert!(listed.iter().all(|entry| entry.label == label));
                    assert!(!more, "the listing said more were left");
                }
                other => panic!("a scrub's listing was answered {other:?}"),
            }
        }
        // each read its three headers at a block a second, after the one block it had in hand
        assert!(
            took.iter().all(|took| *took >= Duration::from_millis(1800)),
            "a scrub's listing read its headers without drawing on its device's ceiling: {took:?}"
        );
    });
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A local read's bytes cross to their caller in the heap buffer the read filled, never copied,
/// and hold the read's draw on the holder's budget until the caller drops them (F110)
#[test]
fn a_local_reads_bytes_hold_their_draw_until_dropped() {
    // one node's store, asked by its own shards, with no lane
    let root = dir();
    let (node, slice) = (NodeId::mint(), SliceId::mint());
    let store = store_with(root.path(), node, slice, None, 0, None);
    let holder = Holder { node, slice };
    let chunk = chunk_of(78);
    let base = Label { sequence: 0, tag: 0x7b6 };
    let written = bytes(CHUNK as usize);
    futures::executor::block_on(async {
        // the chunk written whole to the slice
        let wrote = store
            .send(holder, ChunkRequest::Write(piece(chunk, base, 0, written.clone(), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        let (idle, _, _) = store.budgets()[0];
        // the whole chunk read through its own executor, whose answer crosses to its caller
        let range = ChunkRequest::Read {
            chunk,
            offset: 0,
            len: CHUNK,
            labels: vec![base],
        };
        let ChunkReply::Bytes { bytes, .. } = store.ask_holder(holder, range).await else {
            panic!("a local read was not answered with its bytes");
        };
        assert!(
            matches!(bytes, ChunkBytes::Shared(_)),
            "a local read's bytes were copied to cross"
        );
        assert_eq!(bytes, written);
        // the read's draw held while its caller holds the bytes, and given back once it drops them
        let (held, _, _) = store.budgets()[0];
        assert!(
            held >= idle + CHUNK,
            "{held} drawn while a local read's {CHUNK} bytes are held, {idle} before"
        );
        drop(bytes);
        let (after, _, _) = store.budgets()[0];
        assert_eq!(after, idle, "a local read's draw outlived its bytes");
    });
    store.stop();
}

/// Answers ready together are written in one call, as many as the byte bound lets a call carry,
/// each the bytes its read asked for, and every one gives back its place among the answers owed
/// once it is written (F111)
#[test]
fn answers_ready_together_go_in_one_write_within_a_byte_bound() {
    let (root_a, root_b) = (dir(), dir());
    let (store_a, store_b, holder, acceptor) = pair(root_a.path(), root_b.path(), 0, None);
    let chunk = chunk_of(79);
    let base = Label { sequence: 0, tag: 0x7c6 };
    let written = bytes(CHUNK as usize);
    let read = ChunkRequest::Read {
        chunk,
        offset: 0,
        len: CHUNK,
        labels: vec![base],
    };
    futures::executor::block_on(async {
        // the chunk written whole to B's slice, and read once across the lane to open it
        let wrote = store_b
            .send(holder, ChunkRequest::Write(piece(chunk, base, 0, written.clone(), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        assert!(matches!(store_a.ask_holder(holder, read.clone()).await, ChunkReply::Bytes { .. }));
        let before = store_b.lane_figures().await;
        // forty reads of the whole chunk owed while B's answers are held, then let go
        crate::server::objects::faults::hold_answers(holder.slice, true);
        let asks = (0..40).map(|_| store_a.ask_holder(holder, read.clone()));
        let reads = futures::future::join_all(asks);
        let release = async {
            // every read owed and none still reading its slice, so all forty answers wait together
            let deadline = Instant::now() + Duration::from_secs(20);
            let queued = || {
                let work = store_b.work();
                work.owed >= 40 && work.foreground == 0
            };
            while !queued() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            crate::server::objects::faults::hold_answers(holder.slice, false);
        };
        let (answers, ()) = futures::join!(reads, release);
        // every read answered with its bytes
        for answer in answers {
            match answer {
                ChunkReply::Bytes { label, bytes } => {
                    assert_eq!(label, base);
                    assert_eq!(bytes, written);
                }
                other => panic!("a read was answered {other:?}"),
            }
        }
        // forty answers of 256 KiB in calls of at most 4 MiB: sixteen, sixteen and eight
        let after = store_b.lane_figures().await;
        assert_eq!(after.answers_written - before.answers_written, 40);
        assert_eq!(
            after.answer_writes - before.answer_writes,
            3,
            "forty answers ready together went in other than three calls"
        );
        // and none is owed once written
        assert_eq!(store_b.work().owed, 0, "an answer written kept its place among those owed");
    });
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A whole chunk's pieces sent across the lane are written from the buffers the lane read their
/// units into, and the chunk's file is byte for byte the one the same pieces make on a store of
/// their own, written through its copying path (F111)
#[test]
fn a_whole_chunk_across_the_lane_is_the_file_its_own_store_writes() {
    let (root_a, root_b, root_c) = (dir(), dir(), dir());
    let (store_a, store_b, holder, acceptor) = pair(root_a.path(), root_b.path(), 0, None);
    // a third store, written on its own executor, with no lane
    let (node_c, slice_c) = (NodeId::mint(), SliceId::mint());
    let store_c = store_with(root_c.path(), node_c, slice_c, None, 0, None);
    let holder_c = Holder {
        node: node_c,
        slice: slice_c,
    };
    let chunk = chunk_of(80);
    let base = Label { sequence: 0, tag: 0x7d6 };
    let written = bytes(CHUNK as usize);
    let half = CHUNK as usize / 2;
    let pieces = [
        piece(chunk, base, 0, written[..half].to_vec(), false),
        piece(chunk, base, half as u64, written[half..].to_vec(), true),
    ];
    futures::executor::block_on(async {
        // the chunk in two pieces across the lane to B, and the same on C's own executor
        let before = store_b.lane_figures().await;
        for piece in &pieces {
            let across = store_a.ask_holder(holder, ChunkRequest::Write(piece.clone())).await;
            assert_eq!(across, ChunkReply::Done);
            let own = store_c
                .send(holder_c, ChunkRequest::Write(piece.clone()))
                .await
                .expect("sent");
            assert_eq!(answer_of(own).await, ChunkReply::Done);
        }
        // both of B's pieces were read straight into its device's buffers
        let after = store_b.lane_figures().await;
        let aligned = after.aligned_pieces - before.aligned_pieces;
        assert_eq!(aligned, 2, "a piece's units were not read apart");
        // and B reads back what was written
        let read = ChunkRequest::Read {
            chunk,
            offset: 0,
            len: CHUNK,
            labels: vec![base],
        };
        match store_a.ask_holder(holder, read).await {
            ChunkReply::Bytes { bytes, .. } => assert_eq!(bytes, written),
            other => panic!("the chunk read back across the lane as {other:?}"),
        }
    });
    // the two chunk files, byte for byte
    let at = |root: &Path| root.join("slice-0").join("chunks").join(chunk.relative());
    let (across, own) = (std::fs::read(at(root_b.path())), std::fs::read(at(root_c.path())));
    let (across, own) = (across.expect("B's chunk file"), own.expect("C's chunk file"));
    assert!(
        across == own,
        "a chunk written from a lane's buffers is not the file its own store writes"
    );
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
    store_c.stop();
}

/// A read across the lane is answered in the frame its link read, never copied out of it: a range
/// from part way into a unit, a chunk a staged unit is laid over, and a rebuild's read, each the
/// label's bytes; the same range through the holder's own executor is a buffer of its own (F106)
#[test]
fn a_read_across_the_lane_stays_in_its_frame() {
    let (root_a, root_b) = (dir(), dir());
    let (store_a, store_b, holder, acceptor) = pair(root_a.path(), root_b.path(), 0, None);
    let chunk = chunk_of(77);
    let base = Label { sequence: 0, tag: 0x7a6 };
    let staged = Label { sequence: 1, tag: 0x7a7 };
    let written = bytes(CHUNK as usize);
    let unit = UNIT as usize;
    futures::executor::block_on(async {
        // the chunk written whole to B's slice, on B's own store
        let wrote = store_b
            .send(holder, ChunkRequest::Write(piece(chunk, base, 0, written.clone(), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        // a range from part way into a unit, across the next, read across the lane at the base
        let range = ChunkRequest::Read {
            chunk,
            offset: 1000,
            len: 2 * u64::from(UNIT),
            labels: vec![base],
        };
        match store_a.ask_holder(holder, range.clone()).await {
            ChunkReply::Bytes { label, bytes } => {
                assert_eq!(label, base);
                assert!(matches!(bytes, ChunkBytes::Framed { .. }), "a read across the lane was copied out of its frame");
                assert_eq!(bytes, written[1000..1000 + 2 * unit].to_vec());
            }
            other => panic!("a range read across the lane was answered {other:?}"),
        }
        // the same through B's own executor, which crosses to its caller ~~in a buffer of its own~~
        // in the heap buffer its read filled, never copied (F110)
        match store_b.ask_holder(holder, range).await {
            ChunkReply::Bytes { bytes, .. } => {
                assert!(
                    matches!(bytes, ChunkBytes::Shared(_)),
                    "a local read's bytes were copied to cross"
                );
                assert_eq!(bytes, written[1000..1000 + 2 * unit].to_vec());
            }
            other => panic!("a range read on its holder was answered {other:?}"),
        }
        // a unit staged over the base, read across the lane at the label it makes, is laid over
        let new = vec![9u8; unit];
        let stage = ChunkRequest::StageUnits {
            chunk,
            makes: staged,
            expects: vec![base],
            length: CHUNK,
            unit_bytes: UNIT,
            units: vec![UnitBytes {
                index: 1,
                crc: crc64(&new),
                bytes: new.clone().into(),
            }],
        };
        assert_eq!(store_a.ask_holder(holder, stage).await, ChunkReply::Done);
        let mut overlaid = written.clone();
        overlaid[unit..2 * unit].copy_from_slice(&new);
        let read = ChunkRequest::Read {
            chunk,
            offset: 0,
            len: CHUNK,
            labels: vec![staged],
        };
        assert_eq!(
            store_a.ask_holder(holder, read).await,
            ChunkReply::Bytes {
                label: staged,
                bytes: overlaid.into(),
            }
        );
        // and a rebuild's read of the base, served in its class across the lane
        let background = ChunkRequest::Background(Box::new(ChunkRequest::Read {
            chunk,
            offset: 0,
            len: CHUNK,
            labels: vec![base],
        }));
        assert_eq!(
            store_a.ask_holder(holder, background).await,
            ChunkReply::Bytes {
                label: base,
                bytes: written.clone().into(),
            }
        );
    });
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A served lane whose answer cannot be written ends, so its asker's requests are refused by name.
/// A writer that stopped left the lane reading: every answer after it was dropped, its asker's link
/// stayed up, and each request it had sent waited with no bound
/// ([item 307](../../../../../docs/src/appendix/resolved/a-lane-whose-writer-stops-ends.md))
#[test]
fn a_lane_whose_answers_cannot_be_written_ends() {
    let (root_a, root_b) = (dir(), dir());
    let cluster = ClusterId::mint();
    let (a, b) = (NodeId::mint(), NodeId::mint());
    let (slice_a, slice_b) = (SliceId::mint(), SliceId::mint());
    // B listens, and serves its slice to one lane
    let store_b = store(root_b.path(), b, slice_b, None);
    let (acceptor, at, map_tx) = accept_one(store_b.clone(), local(b, cluster), slice_b);
    let map = Arc::new(map(cluster, a, b, at));
    map_tx.send(map.clone()).expect("the acceptor waits");
    // A takes frames of 128 KiB at most, half of B's chunk and its head
    let store_a = store(
        root_a.path(),
        a,
        slice_a,
        Some(LanePlan {
            local: Local {
                max_frame_bytes: 128 << 10,
                ..local(a, cluster)
            },
            dial: std::collections::BTreeMap::new(),
            tls: PeerTlsHolder::build(None).expect("plaintext"),
            transport: Transport::default(),
        }),
    );
    store_a.install_map(&map);
    let holder = Holder {
        node: b,
        slice: slice_b,
    };
    let chunk = chunk_of(78);
    let base = Label {
        sequence: 0,
        tag: 0x7a8,
    };
    let written = bytes(CHUNK as usize);
    futures::executor::block_on(async {
        // the chunk written whole to B's slice, on B's own store
        let wrote = store_b
            .send(holder, ChunkRequest::Write(piece(chunk, base, 0, written.clone(), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        // a unit's read fits A's frames, and is answered across the lane
        let unit = ChunkRequest::Read {
            chunk,
            offset: 0,
            len: u64::from(UNIT),
            labels: vec![base],
        };
        assert!(matches!(store_a.ask_holder(holder, unit).await, ChunkReply::Bytes { .. }));
    });
    // the whole chunk's answer does not fit A's frames: the lane ends, and the read is refused by
    // the lane's end within a bound, never left waiting
    let (tx, rx) = std::sync::mpsc::channel();
    let asker = store_a.clone();
    std::thread::spawn(move || {
        let whole = ChunkRequest::Read {
            chunk,
            offset: 0,
            len: CHUNK,
            labels: vec![base],
        };
        let _ = tx.send(futures::executor::block_on(asker.ask_holder(holder, whole)));
    });
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(ChunkReply::Refused {
            code: StoreCode::Unavailable,
            message,
        }) => assert!(
            message.contains("dropped"),
            "refused, but not by the lane's end: {message}"
        ),
        Ok(other) => panic!("a read whose answer could not be written was answered {other:?}"),
        Err(_) => panic!("a read whose answer could not be written was left waiting, its lane up"),
    }
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A peer that sends reads and never reads their answers is read from only while its lane has
/// room, each answer it has not taken holding its read's draw on the holder's budget until it is
/// written; once the peer goes, every draw comes back. Before F106 an answer gave its room and its
/// draw back once it was queued, so the holder read every request and held the answers outside its
/// budget (F106)
#[test]
fn a_peer_that_reads_no_answers_holds_a_bounded_budget() {
    let cluster = ClusterId::mint();
    let (a, b) = (NodeId::mint(), NodeId::mint());
    let slice_b = SliceId::mint();
    let root_b = dir();
    let store_b = store(root_b.path(), b, slice_b, None);
    let (acceptor, at, map_tx) = accept_one(store_b.clone(), local(b, cluster), slice_b);
    map_tx.send(Arc::new(map(cluster, a, b, at))).expect("the acceptor waits");
    let holder = Holder { node: b, slice: slice_b };
    let chunk = chunk_of(55);
    let label = Label { sequence: 0, tag: 0x7a6 };
    futures::executor::block_on(async {
        let wrote = store_b
            .send(holder, ChunkRequest::Write(piece(chunk, label, 0, bytes(CHUNK as usize), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
    });
    // a peer dials B's slice and sends three times the reads a lane takes ahead of its answers, each
    // of the whole chunk, and reads none of their answers until told to go
    let requests = 3 * super::MAX_SERVED;
    let (sent_tx, sent_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let peer_local = local(a, cluster);
    let peer = glommio::LocalExecutorBuilder::default()
        .spawn(move || async move {
            let mut stream = glommio::net::TcpStream::connect(at).await.expect("connect");
            // whoever answers, so the hello's shard count is not judged
            let target = PeerAddr {
                node: None,
                data: at.to_string(),
                control: String::new(),
                shards: 0,
            };
            let (_, negotiated) = handshake::dial(
                &mut stream,
                &peer_local,
                crate::server::peer::Lane::Object,
                None,
                Some(slice_b),
                &target,
                &crate::shared::tls::PeerIdentity::Plaintext,
                false,
            )
            .await
            .expect("the hello");
            for id in 0..requests {
                let body = ChunkRequest::Read {
                    chunk,
                    offset: 0,
                    len: CHUNK,
                    labels: vec![label],
                }
                .encode_body(id);
                let header = crate::server::peer::codec::header_at(
                    negotiated.version,
                    MessageType::ChunkRequest,
                    body.len(),
                    negotiated.max_frame_bytes,
                )
                .expect("a header");
                stream.write_all(&header).await.expect("written");
                stream.write_all(&body).await.expect("written");
            }
            stream.flush().await.expect("flushed");
            sent_tx.send(()).expect("the test waits");
            while go_rx.try_recv().is_err() {
                glommio::timer::sleep(Duration::from_millis(5)).await;
            }
            drop(stream);
        })
        .expect("the peer");
    sent_rx.recv_timeout(Duration::from_secs(30)).expect("every request sent");
    // B reads while its lane has room, and stops once the answers it cannot write hold all of it
    let mut served = u64::MAX;
    loop {
        std::thread::sleep(Duration::from_millis(500));
        let now = futures::executor::block_on(store_b.lane_figures()).served;
        if now == served {
            break;
        }
        served = now;
    }
    let (used, limit, _) = store_b.budgets()[0];
    assert!(
        served < requests,
        "the holder read all {requests} requests of a peer that reads no answers"
    );
    assert!(used > 0, "the answers the peer did not take hold no draw on the budget");
    assert!(used <= limit, "{used} drawn of a budget of {limit}");
    // once the peer goes, its lane ends and every answer it left gives its draw back
    go_tx.send(()).expect("the peer waits");
    peer.join().expect("the peer ran");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (used, _, _) = store_b.budgets()[0];
        if used == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "{used} bytes stayed drawn after the peer went");
        std::thread::sleep(Duration::from_millis(50));
    }
    acceptor.join().expect("the acceptor ran");
    store_b.stop();
}

/// A node's caches give their units back when it is past its memory: each bound falls below what
/// it holds by its share of the excess and an eighth more, its executor gives back what is past it,
/// and caches that hold nothing have nothing to give, so a shard evicts rows (F107)
#[test]
fn the_caches_give_their_units_back_before_rows() {
    let root = dir();
    let (node, slice) = (NodeId::mint(), SliceId::mint());
    // a cache that holds the chunk four times over
    let store = store_with(root.path(), node, slice, None, 1 << 20, None);
    let holder = Holder { node, slice };
    let chunk = chunk_of(33);
    let label = Label { sequence: 0, tag: 0x7a6 };
    futures::executor::block_on(async {
        let wrote = store
            .send(holder, ChunkRequest::Write(piece(chunk, label, 0, bytes(CHUNK as usize), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        // a cache that holds nothing has nothing to give
        assert!(!store.yield_caches(1 << 20), "a cache that holds nothing gave");
        // a reader's read fills it with the chunk's four units
        let read = ChunkRequest::Read {
            chunk,
            offset: 0,
            len: CHUNK,
            labels: vec![label],
        };
        assert!(matches!(store.ask_holder(holder, read).await, ChunkReply::Bytes { .. }));
    });
    assert_eq!(store.cache_figures().held, CHUNK, "the read did not fill the cache");
    // past the node's memory by a unit: the bound falls below what it holds by that and an eighth
    // more, and the executor gives the units past it back
    assert!(store.yield_caches(u64::from(UNIT)), "a cache holding units did not give");
    let bound = CHUNK - (u64::from(UNIT) + CHUNK / 8);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let figures = store.cache_figures();
        if figures.held <= bound {
            assert_eq!(figures.limit, bound, "the bound is not what it held less the excess and an eighth");
            break;
        }
        assert!(Instant::now() < deadline, "the cache kept {} past its bound of {bound}", figures.held);
        std::thread::sleep(Duration::from_millis(10));
    }
    // asked again while it holds any, it gives everything, and then has nothing to give
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.yield_caches(CHUNK) {
        assert!(Instant::now() < deadline, "the cache never gave everything back");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(store.cache_figures().held, 0);
    store.stop();
}

/// Ask a holder from a thread of its own, so a test waits for the answer only as long as it chooses
///
/// Returns where the answer arrives.
///
/// # Arguments
///
/// * `asker` - The store that asks
/// * `holder` - The holder asked
/// * `request` - The request
fn ask_apart(asker: &ObjectStore, holder: Holder, request: ChunkRequest) -> std::sync::mpsc::Receiver<ChunkReply> {
    let (tx, rx) = std::sync::mpsc::channel();
    let asker = asker.clone();
    std::thread::spawn(move || {
        let _ = tx.send(futures::executor::block_on(asker.ask_holder(holder, request)));
    });
    rx
}

/// A read across the lane whose answer does not come is refused by name once the lane's bound on
/// a read's answer passes, so its reader asks another holder or decodes without it: it is no
/// longer counted waiting, its answer is let go when it comes, and the lane answers the next read.
/// A rebuild's read waits behind its holder's foreground, and is not bounded. Before, a reader's
/// read waited for its answer with no bound
/// ([item 316](../../../../../docs/src/appendix/resolved/unanswered-read-bound.md))
#[test]
fn a_read_whose_answer_does_not_come_is_refused_at_its_bound() {
    let (root_a, root_b) = (dir(), dir());
    // A waits half a second for a read's answer
    let transport = Transport {
        object_read_timeout: crate::server::conf::cluster::DurationSpec(Duration::from_millis(500)),
        ..Transport::default()
    };
    let (store_a, store_b, holder, acceptor) = pair_over(root_a.path(), root_b.path(), 0, None, transport);
    let chunk = chunk_of(80);
    let base = Label { sequence: 0, tag: 0x7d6 };
    let read = ChunkRequest::Read {
        chunk,
        offset: 0,
        len: CHUNK,
        labels: vec![base],
    };
    futures::executor::block_on(async {
        // the chunk written whole to B's slice, and read once across the lane to open it
        let wrote = store_b
            .send(holder, ChunkRequest::Write(piece(chunk, base, 0, bytes(CHUNK as usize), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
        // the setup's read asks again until it is answered: under load its answer can outrun the
        // half second this store gives every read, which is what the row holds later, not here
        let mut last = None;
        for _ in 0..20 {
            let reply = store_a.ask_holder(holder, read.clone()).await;
            if matches!(reply, ChunkReply::Bytes { .. }) {
                last = None;
                break;
            }
            last = Some(reply);
        }
        assert!(last.is_none(), "the setup's read across the lane was never answered: {last:?}");
    });
    // B's answers held, a read across the lane waits for one that does not come
    crate::server::objects::faults::hold_answers(holder.slice, true);
    let begun = Instant::now();
    let answer = ask_apart(&store_a, holder, read.clone()).recv_timeout(Duration::from_secs(12));
    let took = begun.elapsed();
    crate::server::objects::faults::hold_answers(holder.slice, false);
    match answer {
        Ok(ChunkReply::Refused {
            code: StoreCode::Unavailable,
            message,
        }) => assert!(message.contains("no answer"), "refused, but not for want of an answer: {message}"),
        Ok(ChunkReply::Bytes { label, .. }) => panic!("a read whose answer was held was answered at {label:?} after {took:?}"),
        Ok(other) => panic!("a read whose answer was held was answered {other:?} after {took:?}"),
        Err(_) => panic!("a read whose answer was held was still waiting after {took:?}, its lane up"),
    }
    assert!(
        took >= Duration::from_millis(500) && took < Duration::from_secs(3),
        "a read whose answer was held was refused after {took:?}, where its bound is 500 ms"
    );
    // no longer counted waiting on A, before its reader heard
    assert_eq!(store_a.work().waiting, 0, "a read refused at its bound was still counted waiting");
    // its answer, let go, comes late and is dropped, and the lane answers the next read
    futures::executor::block_on(async {
        match store_a.ask_holder(holder, read.clone()).await {
            ChunkReply::Bytes { label, .. } => assert_eq!(label, base),
            other => panic!("the read after one refused at its bound was answered {other:?}"),
        }
    });
    // a rebuild's read held past the bound waits, and is answered once let go
    crate::server::objects::faults::hold_answers(holder.slice, true);
    let rebuild = ask_apart(&store_a, holder, ChunkRequest::Background(Box::new(read.clone())));
    let early = rebuild.recv_timeout(Duration::from_millis(1500));
    crate::server::objects::faults::hold_answers(holder.slice, false);
    assert!(early.is_err(), "a rebuild's read was answered while its answer was held, a reader's bound past");
    match rebuild.recv_timeout(Duration::from_secs(10)) {
        Ok(ChunkReply::Bytes { label, .. }) => assert_eq!(label, base),
        Ok(other) => panic!("a rebuild's read held past a reader's bound was answered {other:?}"),
        Err(_) => panic!("a rebuild's read was still waiting after its answer was let go"),
    }
    acceptor.join().expect("the acceptor ran");
    store_a.stop();
    store_b.stop();
}

/// A read of a slice on its own node counts in that slice's queue from the moment it chooses the
/// slice, not only once the slice's executor takes it from its mailbox. Under load sixteen reads of
/// a pool of copies on disks each chose while their slice's executor had taken none of them, so each
/// saw an empty queue, and one disk served every one
/// ([item 314](../../../../../docs/src/appendix/resolved/local-reads-counted-at-choice.md))
#[test]
fn a_local_read_counts_in_its_queue_from_its_choice() {
    let root = dir();
    let (node, slice) = (NodeId::mint(), SliceId::mint());
    let store = store_with(root.path(), node, slice, None, 0, None);
    let holder = Holder { node, slice };
    let chunk = chunk_of(81);
    let base = Label { sequence: 0, tag: 0x7e6 };
    futures::executor::block_on(async {
        let wrote = store
            .send(holder, ChunkRequest::Write(piece(chunk, base, 0, bytes(CHUNK as usize), true)))
            .await
            .expect("sent");
        assert_eq!(answer_of(wrote).await, ChunkReply::Done);
    });
    // the slice's executor held busy, as a loaded host holds it, so no read reaches the slice
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    store
        .run(Box::new(move |_store| {
            Box::pin(async move {
                let _ = busy_tx.send(());
                std::thread::sleep(Duration::from_millis(800));
            })
        }))
        .expect("a job");
    busy_rx.recv_timeout(Duration::from_secs(10)).expect("the executor took the job");
    // eight reads of the slice chosen meanwhile, each waiting for its answer on a thread of its own
    let read = ChunkRequest::Read {
        chunk,
        offset: 0,
        len: u64::from(UNIT),
        labels: vec![base],
    };
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let (store, read) = (store.clone(), read.clone());
            std::thread::spawn(move || futures::executor::block_on(store.read_from(holder, read)))
        })
        .collect();
    // every one counted in the slice's queue, though none has reached the slice
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut queued = store.queue_of(&holder);
    while queued < 8 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
        queued = store.queue_of(&holder);
    }
    assert_eq!(
        queued, 8,
        "eight reads chosen while their slice's executor was busy were counted as {queued} in its queue"
    );
    // and each answered, its place given back
    for reader in readers {
        assert!(matches!(reader.join().expect("a reader"), ChunkReply::Bytes { .. }));
    }
    assert_eq!(store.queue_of(&holder), 0, "a read answered kept its place in its slice's queue");
    store.stop();
}
