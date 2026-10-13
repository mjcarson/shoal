//! The object lane between two stores over loopback: dialled by one, accepted and handed to the
//! other's slice owner, and a whole chunk written, confirmed and read back across it

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use glommio::net::TcpListener;

use super::LanePlan;
use crate::server::conf::cluster::Transport;
use crate::server::control::types::{MemberHealth, MemberPhase, MemberRole};
use crate::server::devices::{DeviceState, Facts};
use crate::server::map::{MapCell, MapMember, TabletMap};
use crate::server::objects::cpus::ObjectCpu;
use crate::server::objects::executor::{answer_of, Holder, ObjectPlan, ObjectStore};
use crate::server::objects::store::{ChunkId, SliceOptions};
use crate::server::peer::handoff::HandedLane;
use crate::server::peer::handshake::{self, Local};
use crate::server::peer::Lane;
use crate::shared::checksum::crc64;
use crate::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece, StoreCode};
use crate::shared::protocol::peer::CAP_OBJECT_LANE_V1;
use crate::shared::protocol::{OBJECT_WIRE_VERSION, PROTOCOL_VERSION};
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
    let device = Arc::new(DeviceState::new(
        DeviceId::mint(),
        DeviceSpec::new(root, "ssd"),
        Facts::default(),
        0,
        ApplySync::Shared,
        vec![(slice, root.join("slice-0"))],
        Vec::new(),
    ));
    ObjectStore::start(ObjectPlan {
        cpus: vec![ObjectCpu { cpu: 0, blocking: 0 }],
        budget: 64 << 20,
        io_memory: 1 << 20,
        slices: vec![(
            device.clone(),
            device.slices[0].clone(),
            SliceOptions {
                segment_bytes: 1 << 20,
                free_files: 1,
                chunk_lengths: vec![CHUNK],
                budget: None,
                keep_previous: true,
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
        crcs: bytes.chunks(UNIT as usize).map(crc64).collect(),
        bytes,
    }
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
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let (map_tx, map_rx) = std::sync::mpsc::channel::<Arc<TabletMap>>();
    let handed_to = store_b.clone();
    let local_b = local(b, cluster);
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
                &local_b,
                &[Lane::Object],
                &map,
                &PeerIdentity::Plaintext,
                true,
            )
            .await
            .expect("let in");
            // the hello named the slice and agreed the lane
            assert_eq!(accepted.lane, Lane::Object);
            assert_eq!(accepted.slice, Some(slice_b));
            assert!(accepted.negotiated.has(CAP_OBJECT_LANE_V1));
            let lane =
                HandedLane::release(stream, accepted.node, accepted.negotiated, None, accepted.slice);
            assert!(handed_to.adopt(lane), "the slice's owner takes the lane");
        })
        .expect("the acceptor");
    let at = addr_rx.recv().expect("an address");
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
        assert_eq!(read, ChunkReply::Bytes { label, bytes: written.clone() });
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
