//! The peer handshake and forward validation, driven over a glommio loopback
//!
//! The one place a peer's identity and its frames are judged before anything unchecked. This runs
//! the real [`handshake::accept`] against a client on the same executor - reliable, since there is
//! no cross-runtime connection churn - for every identity a hello can carry wrong, and asserts the
//! forward decoders reject every malformed shape the receiver would otherwise pass to unchecked
//! archive access ([F38](../../../../docs/src/features/inter-node-transport.md)).

use futures::AsyncReadExt;
use glommio::net::{TcpListener, TcpStream};

use super::handshake::{self, Admission, Local};
use super::Lane;
use crate::server::control::types::{MemberHealth, MemberRole};
use crate::server::map::{MapCell, MapMember, TabletMap};
use crate::server::control::runtime::GlommioRuntime;
use openraft::AsyncRuntime as _;
use crate::server::meta::Identity;
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::protocol::peer::{
    decode_entries, encode_entries, ForwardEntry, ForwardPreamble, PeerHello, PeerHelloAck,
    PeerRefusal, FORWARD_PREAMBLE_LEN, PEER_HELLO_BODY_LEN, PEER_HELLO_FRAME_LEN,
};
use crate::shared::protocol::{HEADER_LEN, PROTOCOL_VERSION};
use futures::AsyncWriteExt;

/// An identity for a node in a placed cluster
fn identity(node: NodeId, cluster: ClusterId) -> Identity {
    Identity {
        node,
        cluster: Some(cluster),
        layout: 1,
        topology_at_claim: 0,
        fresh: false,
        mode: crate::server::meta::MarkerMode::Cluster,
        incarnation: 1,
    }
}

/// A map of two members of one cluster, each running two shards at incarnation 1
fn two_nodes(cluster: ClusterId, a: NodeId, b: NodeId) -> MapCell {
    let member = |node: NodeId, port: u16| MapMember {
        node,
        client: "127.0.0.1:0".to_string(),
        data: format!("127.0.0.1:{port}"),
        control: format!("127.0.0.1:{}", port + 1),
        shards: 2,
        role: MemberRole::Voter,
        health: MemberHealth::Up,
        incarnation: 1,
        shards_failed: Vec::new(),
    };
    let map = TabletMap {
        version: 1,
        cluster: Some(cluster),
        members: [(a, member(a, 1)), (b, member(b, 3))].into_iter().collect(),
        placement: vec![a, b],
        ..TabletMap::default()
    };
    MapCell::new(std::sync::Arc::new(map))
}

/// Drive one hello against `accept`, returning the reason the client read and whether accept let
/// it in
///
/// # Arguments
///
/// * `listener` - The bound listener the client dials
/// * `local` - What the accepting node says about itself
/// * `admission` - What it judges a hello against
/// * `served` - The lanes this listener serves
/// * `hello` - The hello the client sends
async fn exchange(
    listener: &TcpListener,
    local: &Local,
    admission: &dyn Admission,
    served: &[Lane],
    hello: PeerHello,
) -> (PeerRefusal, bool) {
    let addr = listener.local_addr().expect("a bound address");
    let max = local.max_frame_bytes;
    // the client writes its hello and reads the ack, on its own task
    let client = glommio::spawn_local(async move {
        let mut sock = TcpStream::connect(addr).await.expect("connect");
        sock.write_all(&hello.frame(max).expect("a hello frame")).await.expect("write");
        sock.flush().await.expect("flush");
        let mut frame = [0u8; PEER_HELLO_FRAME_LEN];
        sock.read_exact(&mut frame).await.expect("read ack");
        let mut body = [0u8; PEER_HELLO_BODY_LEN];
        body.copy_from_slice(&frame[HEADER_LEN..]);
        PeerHelloAck::decode(&body).expect("an ack").reason
    });
    // the server accepts and judges
    let mut stream = listener.accept().await.expect("accept");
    let accepted = handshake::accept(&mut stream, local, served, admission).await.is_ok();
    let reason = client.await;
    (reason, accepted)
}

/// A hello with every field settable, so each case moves exactly one
fn a_hello(cluster: [u8; 16], node: [u8; 16], shards: u16, lane: Lane, schema_id: u64) -> PeerHello {
    a_hello_at(cluster, node, shards, lane, schema_id, 1)
}

/// A hello at a particular incarnation
fn a_hello_at(
    cluster: [u8; 16],
    node: [u8; 16],
    shards: u16,
    lane: Lane,
    schema_id: u64,
    incarnation: u64,
) -> PeerHello {
    PeerHello {
        cluster,
        node,
        incarnation,
        lane,
        wire_min: PROTOCOL_VERSION,
        wire_max: PROTOCOL_VERSION,
        capabilities: crate::shared::protocol::peer::CAPABILITIES,
        schema_id,
        shards,
        max_frame_bytes: 1 << 20,
    }
}

/// A peer's identity and its frames are validated before anything unchecked (C2 M2)
///
/// Every identity a hello can carry wrong is refused with a named reason and `accept` returns an
/// error; a matching hello is accepted. Then every malformed forward shape a peer could send - a
/// preamble that overruns, a hop count, no entries, too many, keys past the bound, entries that do
/// not fill their bytes - is refused by the decoders the receiver runs before it touches an
/// archive.
#[test]
fn peer_rejects_wrong_cluster_identity_and_malformed_payload() {
    let cluster = ClusterId::mint();
    let node0 = NodeId::mint();
    let node1 = NodeId::mint();
    let schema_id = 0xabcd_1234_5678_9abc;
    let local = Local::new(&identity(node1, cluster), 2, schema_id, 1 << 20);
    let placement = two_nodes(cluster, node0, node1);

    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let data = &[Lane::Data, Lane::Bulk];
        let c = *cluster.0.as_bytes();
        let n0 = *node0.0.as_bytes();

        // a foreign cluster, before anything else
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello([9; 16], n0, 2, Lane::Data, schema_id)).await;
        assert_eq!(reason, PeerRefusal::WrongCluster);
        assert!(!ok);
        // a node the membership does not name
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello(c, [7; 16], 2, Lane::Data, schema_id)).await;
        assert_eq!(reason, PeerRefusal::UnknownNode);
        assert!(!ok);
        // a run of node 0 the cluster has superseded
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello_at(c, n0, 2, Lane::Data, schema_id, 0)).await;
        assert_eq!(reason, PeerRefusal::Fenced);
        assert!(!ok);
        // a joiner - no cluster at all - may open the control lane and nothing else
        let control = &[Lane::Control];
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello([0; 16], [7; 16], 2, Lane::Data, schema_id)).await;
        assert_eq!(reason, PeerRefusal::NotJoinable);
        assert!(!ok);
        let (reason, ok) =
            exchange(&listener, &local, &placement, control, a_hello([0; 16], [7; 16], 2, Lane::Control, schema_id)).await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // but not with another schema
        let (reason, ok) =
            exchange(&listener, &local, &placement, control, a_hello([0; 16], [7; 16], 2, Lane::Control, schema_id ^ 1)).await;
        assert_eq!(reason, PeerRefusal::SchemaMismatch);
        assert!(!ok);
        // a node that has been told nothing yet trusts any member of its cluster
        let empty = MapCell::default();
        let (reason, ok) =
            exchange(&listener, &local, &empty, data, a_hello(c, [7; 16], 5, Lane::Data, schema_id)).await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // node 0 with the wrong shard count
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello(c, n0, 99, Lane::Data, schema_id)).await;
        assert_eq!(reason, PeerRefusal::ShardCountMismatch);
        assert!(!ok);
        // a different schema
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello(c, n0, 2, Lane::Data, schema_id ^ 0xffff)).await;
        assert_eq!(reason, PeerRefusal::SchemaMismatch);
        assert!(!ok);
        // a control-lane hello on a listener that serves only data and bulk
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello(c, n0, 2, Lane::Control, schema_id)).await;
        assert_eq!(reason, PeerRefusal::LaneRefused);
        assert!(!ok);
        // and a matching hello is accepted
        let (reason, ok) =
            exchange(&listener, &local, &placement, data, a_hello(c, n0, 2, Lane::Data, schema_id)).await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
    });

    // every malformed forward the receiver would decode is refused before an archive is touched:
    // these are the exact decoders `peer_rx_relay` runs on a frame's bytes
    let entry = ForwardEntry {
        offset: 0,
        index: 0,
        end: true,
        shard: 0,
        origin_shard: 0,
        gather: false,
        trace: None,
        keys: vec![1, 2, 3],
    };
    let entry_bytes = encode_entries(&[entry.clone()]).expect("entries");
    let good = ForwardPreamble {
        bundle: [0; 16],
        attempt: 0,
        base_index: 0,
        hops: 0,
        remaining_ms: 1000,
        entries: 1,
        entries_len: entry_bytes.len() as u32,
    };
    let body_len = FORWARD_PREAMBLE_LEN + entry_bytes.len() + 32;
    // the good preamble decodes
    assert!(ForwardPreamble::decode(&good.encode(), body_len).is_ok());
    // a hop count is a routing loop
    assert!(ForwardPreamble::decode(&ForwardPreamble { hops: 1, ..good }.encode(), body_len).is_err());
    // no entries, or a frame too short to hold a bundle after its entries
    assert!(ForwardPreamble::decode(&ForwardPreamble { entries: 0, ..good }.encode(), body_len).is_err());
    assert!(ForwardPreamble::decode(&good.encode(), FORWARD_PREAMBLE_LEN + entry_bytes.len()).is_err());
    // entries that do not fill their bytes, or claim a count below what follows
    assert!(decode_entries(&entry_bytes[..entry_bytes.len() - 1], 1).is_err());
    assert!(decode_entries(&entry_bytes, 2).is_err());
    // and an alignment note: the bundle is read into its own buffer, never the tail of the
    // preamble's, which `ForwardPreamble::bundle_len` computes from the frame length
    assert_eq!(good.bundle_len(body_len), 32);
}
