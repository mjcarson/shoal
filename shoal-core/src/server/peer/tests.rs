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
use crate::server::control::runtime::GlommioRuntime;
use crate::server::control::types::{MemberHealth, MemberPhase, MemberRole};
use crate::server::map::{MapCell, MapMember, TabletMap};
use crate::server::meta::Identity;
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::protocol::peer::{
    decode_entries, encode_entries, ForwardEntry, ForwardPreamble, PeerHello, PeerHelloAck,
    PeerRefusal, FORWARD_PREAMBLE_LEN, PEER_HELLO_BODY_LEN, PEER_HELLO_FRAME_LEN,
};
use crate::shared::protocol::{HEADER_LEN, MIN_PEER_VERSION, PROTOCOL_VERSION};
use crate::shared::tls::PeerIdentity;
use futures::AsyncWriteExt;
use openraft::AsyncRuntime as _;

/// An identity for a node in a placed cluster
fn identity(node: NodeId, cluster: ClusterId) -> Identity {
    Identity {
        node,
        slots: 2,
        physical: 2,
        rehome: None,
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
        quarantined: Vec::new(),
        phase: MemberPhase::Member,
        lead_weight: 0,
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
        sock.write_all(&hello.frame(max).expect("a hello frame"))
            .await
            .expect("write");
        sock.flush().await.expect("flush");
        let mut frame = [0u8; PEER_HELLO_FRAME_LEN];
        sock.read_exact(&mut frame).await.expect("read ack");
        let mut body = [0u8; PEER_HELLO_BODY_LEN];
        body.copy_from_slice(&frame[HEADER_LEN..]);
        PeerHelloAck::decode(&body).expect("an ack").reason
    });
    // the server accepts and judges, as a plaintext lane would
    let mut stream = listener.accept().await.expect("accept");
    let accepted = handshake::accept(
        &mut stream,
        local,
        served,
        admission,
        &PeerIdentity::Plaintext,
        true,
    )
    .await
    .is_ok();
    let reason = client.await;
    (reason, accepted)
}

/// The same exchange under a certificate that said something about the peer
///
/// # Arguments
///
/// * `listener` - The bound listener the client dials
/// * `local` - What the accepting node says about itself
/// * `admission` - What it judges a hello against
/// * `served` - The lanes this listener serves
/// * `hello` - The hello the client sends
/// * `certified` - What the peer's certificate said
/// * `bind` - Whether the lanes bind certificates to identities
async fn exchange_certified(
    listener: &TcpListener,
    local: &Local,
    admission: &dyn Admission,
    served: &[Lane],
    hello: PeerHello,
    certified: &PeerIdentity,
    bind: bool,
) -> (PeerRefusal, bool) {
    let addr = listener.local_addr().expect("a bound address");
    let max = local.max_frame_bytes;
    let client = glommio::spawn_local(async move {
        let mut sock = TcpStream::connect(addr).await.expect("connect");
        sock.write_all(&hello.frame(max).expect("a hello frame"))
            .await
            .expect("write");
        sock.flush().await.expect("flush");
        let mut frame = [0u8; PEER_HELLO_FRAME_LEN];
        sock.read_exact(&mut frame).await.expect("read ack");
        let mut body = [0u8; PEER_HELLO_BODY_LEN];
        body.copy_from_slice(&frame[HEADER_LEN..]);
        PeerHelloAck::decode(&body).expect("an ack").reason
    });
    let mut stream = listener.accept().await.expect("accept");
    let accepted = handshake::accept(&mut stream, local, served, admission, certified, bind)
        .await
        .is_ok();
    let reason = client.await;
    (reason, accepted)
}

/// A hello with every field settable, so each case moves exactly one
fn a_hello(
    cluster: [u8; 16],
    node: [u8; 16],
    shards: u16,
    lane: Lane,
    schema_id: u64,
) -> PeerHello {
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
        slot: None,
        slice: None,
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
    let local = Local::new(&identity(node1, cluster), 2, schema_id, 1 << 20, None);
    let placement = two_nodes(cluster, node0, node1);

    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let data = &[Lane::Data, Lane::Bulk];
        let c = *cluster.0.as_bytes();
        let n0 = *node0.0.as_bytes();

        // a foreign cluster, before anything else
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello([9; 16], n0, 2, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::WrongCluster);
        assert!(!ok);
        // a node the membership does not name
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello(c, [7; 16], 2, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::UnknownNode);
        assert!(!ok);
        // a run of node 0 the cluster has superseded
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello_at(c, n0, 2, Lane::Data, schema_id, 0),
        )
        .await;
        assert_eq!(reason, PeerRefusal::Fenced);
        assert!(!ok);
        // a joiner - no cluster at all - may open the control lane and nothing else
        let control = &[Lane::Control];
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello([0; 16], [7; 16], 2, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::NotJoinable);
        assert!(!ok);
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            control,
            a_hello([0; 16], [7; 16], 2, Lane::Control, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // but not with another schema
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            control,
            a_hello([0; 16], [7; 16], 2, Lane::Control, schema_id ^ 1),
        )
        .await;
        assert_eq!(reason, PeerRefusal::SchemaMismatch);
        assert!(!ok);
        // a node that has been told nothing yet trusts any member of its cluster
        let empty = MapCell::default();
        let (reason, ok) = exchange(
            &listener,
            &local,
            &empty,
            data,
            a_hello(c, [7; 16], 5, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // node 0 with the wrong shard count
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello(c, n0, 99, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::ShardCountMismatch);
        assert!(!ok);
        // a different schema
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello(c, n0, 2, Lane::Data, schema_id ^ 0xffff),
        )
        .await;
        assert_eq!(reason, PeerRefusal::SchemaMismatch);
        assert!(!ok);
        // a control-lane hello on a listener that serves only data and bulk
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello(c, n0, 2, Lane::Control, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::LaneRefused);
        assert!(!ok);
        // and a matching hello is accepted
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello(c, n0, 2, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // a peer whose range shares no version, and one without a required capability
        // ([F48](../../../../docs/src/features/rolling-compatibility.md))
        let mut apart = a_hello(c, n0, 2, Lane::Data, schema_id);
        apart.wire_min = PROTOCOL_VERSION + 1;
        apart.wire_max = PROTOCOL_VERSION + 2;
        let (reason, ok) = exchange(&listener, &local, &placement, data, apart).await;
        assert_eq!(reason, PeerRefusal::NoCommonVersion);
        assert!(!ok);
        let mut lacking = a_hello(c, n0, 2, Lane::Data, schema_id);
        lacking.capabilities &= !crate::shared::protocol::peer::CAP_READ_CONSISTENCY_V1;
        let (reason, ok) = exchange(&listener, &local, &placement, data, lacking).await;
        assert_eq!(reason, PeerRefusal::CapabilityMissing);
        assert!(!ok);
        // a peer pinned at the floor is accepted, and the two speak the floor
        let mut pinned = a_hello(c, n0, 2, Lane::Data, schema_id);
        pinned.wire_min = MIN_PEER_VERSION;
        pinned.wire_max = MIN_PEER_VERSION;
        let (reason, ok) = exchange(&listener, &local, &placement, data, pinned).await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // but not once the cluster has activated a version above what it speaks
        let mut activated = (*placement.get()).clone();
        activated.activated_wire = PROTOCOL_VERSION;
        activated.version += 1;
        assert!(placement.install(std::sync::Arc::new(activated)));
        let (reason, ok) = exchange(&listener, &local, &placement, data, pinned).await;
        assert_eq!(reason, PeerRefusal::BelowActivatedWire);
        assert!(!ok);
        // while one that speaks it still is
        let (reason, ok) = exchange(
            &listener,
            &local,
            &placement,
            data,
            a_hello(c, n0, 2, Lane::Data, schema_id),
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // the certificate's name against the hello's, under the binding: the same node is
        // accepted, another node is a mismatch, no name is unauthorized, and with the binding
        // off the chain alone is trusted ([F50](../../../../docs/src/features/cluster-operations.md))
        let named = PeerIdentity::Node(NodeId(uuid::Uuid::from_bytes(n0)));
        let other = PeerIdentity::Node(NodeId::from(77));
        let hello = a_hello(c, n0, 2, Lane::Data, schema_id);
        let (reason, ok) = exchange_certified(
            &listener,
            &local,
            &placement,
            data,
            hello.clone(),
            &named,
            true,
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        let (reason, ok) = exchange_certified(
            &listener,
            &local,
            &placement,
            data,
            hello.clone(),
            &other,
            true,
        )
        .await;
        assert_eq!(reason, PeerRefusal::IdentityMismatch);
        assert!(!ok);
        let (reason, ok) = exchange_certified(
            &listener,
            &local,
            &placement,
            data,
            hello.clone(),
            &PeerIdentity::Unnamed,
            true,
        )
        .await;
        assert_eq!(reason, PeerRefusal::Unauthorized);
        assert!(!ok);
        let (reason, ok) = exchange_certified(
            &listener,
            &local,
            &placement,
            data,
            hello.clone(),
            &other,
            false,
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        let (reason, ok) = exchange_certified(
            &listener,
            &local,
            &placement,
            data,
            hello,
            &PeerIdentity::Unnamed,
            false,
        )
        .await;
        assert_eq!(reason, PeerRefusal::Accepted);
        assert!(ok);
        // a joiner's certificate is bound too: its hello names a node before it has a cluster
        let joining = a_hello([0; 16], [7; 16], 2, Lane::Control, schema_id);
        let (reason, ok) = exchange_certified(
            &listener, &local, &placement, control, joining, &other, true,
        )
        .await;
        assert_eq!(reason, PeerRefusal::IdentityMismatch);
        assert!(!ok);
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
        read: None,
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
    assert!(
        ForwardPreamble::decode(&ForwardPreamble { hops: 1, ..good }.encode(), body_len).is_err()
    );
    // no entries, or a frame too short to hold a bundle after its entries
    assert!(
        ForwardPreamble::decode(&ForwardPreamble { entries: 0, ..good }.encode(), body_len)
            .is_err()
    );
    assert!(
        ForwardPreamble::decode(&good.encode(), FORWARD_PREAMBLE_LEN + entry_bytes.len()).is_err()
    );
    // entries that do not fill their bytes, or claim a count below what follows
    assert!(decode_entries(&entry_bytes[..entry_bytes.len() - 1], 1).is_err());
    assert!(decode_entries(&entry_bytes, 2).is_err());
    // and an alignment note: the bundle is read into its own buffer, never the tail of the
    // preamble's, which `ForwardPreamble::bundle_len` computes from the frame length
    assert_eq!(good.bundle_len(body_len), 32);
}

/// A frame naming a slot reaches the executor hosting it, and a slot past the count is refused
///
/// Four slots on two executors: slots zero and two are executor zero's, one and three are
/// executor one's, and slot four is not one this node has, whatever the frame kind
/// ([F47](../../../../docs/src/features/local-rehome.md)).
#[test]
fn the_listener_dispatches_a_slot_to_its_host() {
    use crate::server::hosting::Hosting;
    use crate::server::peer::listener::dispatch_target;
    use crate::server::shard::ShardContact;
    let hosting = Hosting::identity(4).plan(2, true).expect("a plan");
    for (slot, executor) in [(0u16, 0usize), (1, 1), (2, 0), (3, 1)] {
        let target = dispatch_target(&hosting, slot, "refused").expect("a slot this node has");
        assert_eq!(target, ShardContact::Local(executor), "slot {slot}");
    }
    // the identity hosting dispatches a slot to the executor of its number, as always
    let plain = Hosting::identity(4);
    for slot in 0..4u16 {
        assert_eq!(
            dispatch_target(&plain, slot, "refused").expect("a slot"),
            ShardContact::Local(usize::from(slot))
        );
    }
    // a slot past the count is malformed, under either hosting
    let error = dispatch_target(&hosting, 4, "a frame names a shard this node does not run")
        .expect_err("a slot past the count");
    assert!(format!("{error:?}").contains("does not run"), "{error:?}");
    assert!(dispatch_target(&plain, 4, "refused").is_err());
}

/// A dial refused on who the peer is waits out its backoff; one refused for now does not
///
/// A certificate or hello naming another node, and a refusal as mismatched, removed or of another
/// cluster, are verdicts no queued frame changes, so the link waits its whole grown backoff. A
/// refused connection, a peer that does not know us yet and a fenced run are retried as soon as a
/// frame wants to go ([Resolved #172](../../../../docs/src/appendix/resolved/identity-refusal-redials.md)).
#[test]
fn identity_verdicts_wait_out_the_backoff() {
    use super::link::is_identity_verdict;
    use crate::server::errors::{ServerError, ShoalError};
    use crate::shared::protocol::peer::PeerRefusal;
    let (a, b) = (NodeId::mint(), NodeId::mint());
    let refused = |reason| {
        ServerError::Shoal(ShoalError::PeerRefused { node: a, reason })
    };
    // verdicts on identity
    for error in [
        ServerError::Shoal(ShoalError::CertificateIdentity {
            claimed: a,
            certified: Some(b),
        }),
        ServerError::Shoal(ShoalError::PeerIdentity {
            expected: a,
            found: b,
        }),
        refused(PeerRefusal::IdentityMismatch),
        refused(PeerRefusal::Removed),
        refused(PeerRefusal::WrongCluster),
    ] {
        assert!(is_identity_verdict(&error), "{error:?} is retried at once");
    }
    // and what may change with the next start or the next map
    for error in [
        ServerError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)),
        refused(PeerRefusal::UnknownNode),
        refused(PeerRefusal::Fenced),
    ] {
        assert!(!is_identity_verdict(&error), "{error:?} waits out its backoff");
    }
}

/// A peer connection is told to give up on data unacknowledged for the transport's timeout
///
/// What turns a link a long partition left waiting on a backed-off retransmission timer into a
/// link that is aborted and dialled again ([Resolved #181](../../../../docs/src/appendix/resolved/partition-retransmit-backoff.md)).
#[test]
fn unacked_timeout_is_set_on_the_socket() {
    use std::os::fd::AsRawFd;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
    let stream =
        std::net::TcpStream::connect(listener.local_addr().expect("its address")).expect("a dial");
    // set, and read back from the kernel
    super::set_unacked_timeout(&stream, std::time::Duration::from_millis(5000))
        .expect("the option is set");
    let mut millis: libc::c_uint = 0;
    let mut len = std::mem::size_of::<libc::c_uint>() as libc::socklen_t;
    // SAFETY: the descriptor is an open TCP socket and the out pointers are valid for their sizes
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_USER_TIMEOUT,
            std::ptr::from_mut(&mut millis).cast(),
            &raw mut len,
        )
    };
    assert_eq!(rc, 0, "{}", std::io::Error::last_os_error());
    assert_eq!(millis, 5000);
}

/// A connection the network cut is judged cut off once its timer fired and nothing came back
/// ([Resolved #143](../../../../docs/src/appendix/resolved/silent-partition-hops.md))
#[test]
fn a_cut_connection_is_judged_cut_off() {
    let cut = super::TcpSample {
        unacked: 12,
        backoff: 2,
        retransmits: 2,
        last_ack_recv_ms: 600,
        rtt_us: 150,
    };
    assert!(cut.cut_off());
}

/// Nothing the kernel says about a slow or lossy peer is taken for a cut
#[test]
fn a_slow_or_lossy_peer_is_not_cut_off() {
    use super::{TcpSample, KERNEL_SILENCE};
    let base = TcpSample {
        unacked: 12,
        backoff: 2,
        retransmits: 2,
        last_ack_recv_ms: 600,
        rtt_us: 150,
    };
    // a single unanswered retransmission is what a lossy link does, not a cut
    assert!(!TcpSample {
        backoff: 1,
        retransmits: 1,
        ..base
    }
    .cut_off());
    // nothing outstanding is nothing unanswered
    assert!(!TcpSample { unacked: 0, ..base }.cut_off());
    // loss recovered by fast retransmit never fires the timer
    assert!(!TcpSample {
        backoff: 0,
        retransmits: 0,
        ..base
    }
    .cut_off());
    // a peer acknowledging, however slowly its process answers, is heard
    assert!(!TcpSample {
        last_ack_recv_ms: 20,
        ..base
    }
    .cut_off());
    // a long round trip needs four of them without an acknowledgement, not the floor
    let far = TcpSample {
        rtt_us: 200_000,
        last_ack_recv_ms: 600,
        ..base
    };
    assert!(!far.cut_off());
    assert!(TcpSample {
        last_ack_recv_ms: 801,
        ..far
    }
    .cut_off());
    // and the floor itself
    let floor = u32::try_from(KERNEL_SILENCE.as_millis()).expect("a small floor");
    assert!(!TcpSample {
        last_ack_recv_ms: floor - 1,
        ..base
    }
    .cut_off());
    assert!(TcpSample {
        last_ack_recv_ms: floor,
        ..base
    }
    .cut_off());
}

/// A real socket's figures can be read, and a fresh idle one is not cut off
#[test]
fn a_socket_can_be_sampled() {
    use std::os::fd::AsRawFd;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let client = std::net::TcpStream::connect(listener.local_addr().expect("an address"))
        .expect("a connection");
    let sample = super::TcpSample::read(client.as_raw_fd()).expect("tcp_info");
    assert!(!sample.cut_off(), "{sample:?}");
    // a descriptor that is not a socket is an error, never a verdict
    assert!(super::TcpSample::read(-1).is_err());
}

/// A replication lane naming a slot another executor hosts is handed to that executor, and every
/// other lane is served where it landed (F84)
///
/// The pure half of the listener's choice: a slot hosted elsewhere names its host, one hosted
/// here or past the count is served here, and a lane that is not a replication lane, or names no
/// slot, is never handed anywhere ([F84](../../../../docs/src/features/replication-lane-handoff.md)).
#[test]
fn the_listener_hands_a_replication_lane_to_its_slots_host() {
    use super::handoff::{destination, Destination};
    use crate::server::hosting::Hosting;
    // four slots on two executors: slots 0 and 2 on executor 0, slots 1 and 3 on executor 1
    let hosting = Hosting::identity(4).plan(2, true).expect("a plan");
    // a slot hosted elsewhere is its host's to serve
    assert_eq!(
        destination(&hosting, 0, Lane::Replication, Some(1)),
        Destination::Host(1)
    );
    assert_eq!(
        destination(&hosting, 1, Lane::Replication, Some(2)),
        Destination::Host(0)
    );
    // one hosted by the executor that accepted it stays
    assert_eq!(
        destination(&hosting, 0, Lane::Replication, Some(2)),
        Destination::Here
    );
    assert_eq!(
        destination(&hosting, 1, Lane::Replication, Some(3)),
        Destination::Here
    );
    // a replication lane naming none, from a build before the slot, stays
    assert_eq!(
        destination(&hosting, 0, Lane::Replication, None),
        Destination::Here
    );
    // and every other lane stays, whatever it names
    for lane in [Lane::Data, Lane::Bulk, Lane::Control] {
        assert_eq!(
            destination(&hosting, 0, lane, Some(1)),
            Destination::Here,
            "{lane}"
        );
    }
    // a slot past the count stays, where each of its requests is refused by name
    assert_eq!(
        destination(&hosting, 0, Lane::Replication, Some(4)),
        Destination::Here
    );
}

/// A dialler's hello names the slot its lane is for, the acceptor learns it, and the ack names
/// none (F84)
#[test]
fn a_hello_names_its_slot_to_the_acceptor() {
    let cluster = ClusterId::mint();
    let node0 = NodeId::mint();
    let node1 = NodeId::mint();
    let schema_id = 0x5107_5107_5107_5107;
    let acceptor = Local::new(&identity(node1, cluster), 2, schema_id, 1 << 20, None);
    let dialler = Local::new(&identity(node0, cluster), 2, schema_id, 1 << 20, None);
    let placement = two_nodes(cluster, node0, node1);
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("a bound address");
        // the dialler names slot one on a replication lane, on its own task
        let expected = handshake::PeerAddr {
            node: Some(node1),
            data: addr.to_string(),
            control: addr.to_string(),
            shards: 2,
        };
        let client = glommio::spawn_local(async move {
            let mut sock = TcpStream::connect(addr).await.expect("connect");
            handshake::dial(
                &mut sock,
                &dialler,
                Lane::Replication,
                Some(1),
                None,
                &expected,
                &PeerIdentity::Plaintext,
                true,
            )
            .await
            .expect("let in")
        });
        let mut stream = listener.accept().await.expect("accept");
        let accepted = handshake::accept(
            &mut stream,
            &acceptor,
            &[Lane::Replication],
            &placement,
            &PeerIdentity::Plaintext,
            true,
        )
        .await
        .expect("accepted");
        // the acceptor learned the slot, and its ack named none
        assert_eq!(accepted.lane, Lane::Replication);
        assert_eq!(accepted.slot, Some(1));
        let (ack, _) = client.await;
        assert_eq!(ack.slot, None);
    });
}

/// A lane given up after its hello loses no byte the dialler sent behind it (F84)
///
/// The dialler writes its hello naming slot one and a replication request straight behind it,
/// before it reads the ack. Executor A accepts, shakes hands and gives the connection up; B
/// adopts the same descriptor and reads the request byte for byte, then answers it; the dialler
/// reads the ack and then the answer, in that order. This is the invariant the handoff rests on:
/// nothing is read past the hello before the lane leaves the executor that accepted it
/// ([F84](../../../../docs/src/features/replication-lane-handoff.md)).
#[test]
fn a_lane_given_up_after_its_hello_loses_no_frame() {
    use super::codec;
    use super::handoff::HandedLane;
    use crate::shared::protocol::peer::{
        ReplicateKind, ReplicateRequestHead, ReplicateResponseHead, ReplicateStatus,
        REPLICATE_HEAD_LEN, REPLICATE_RESPONSE_HEAD_LEN,
    };
    use crate::shared::protocol::{Header, MessageType};
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::time::Duration;
    let cluster = ClusterId::mint();
    let node0 = NodeId::mint();
    let node1 = NodeId::mint();
    let schema_id = 0x5107_5107_5107_5108;
    // the request's payload, large enough that it waits in the socket while the lane moves
    let pattern: Vec<u8> = (0..256 * 1024u32).map(|i| (i % 253) as u8).collect();
    let expected = pattern.clone();
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (lane_tx, lane_rx) = std::sync::mpsc::channel::<(HandedLane, i32)>();
    // A accepts, shakes hands, and gives the lane up before reading anything else
    let a = glommio::LocalExecutorBuilder::default()
        .spawn(move || async move {
            let local = Local::new(&identity(node1, cluster), 2, schema_id, 1 << 20, None);
            let placement = two_nodes(cluster, node0, node1);
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            addr_tx
                .send(listener.local_addr().expect("a bound address"))
                .expect("the dialler is waiting");
            let mut stream = listener.accept().await.expect("accept");
            let accepted = handshake::accept(
                &mut stream,
                &local,
                &[Lane::Replication],
                &placement,
                &PeerIdentity::Plaintext,
                true,
            )
            .await
            .expect("accepted");
            assert_eq!(accepted.slot, Some(1));
            let fd = stream.as_raw_fd();
            let lane = HandedLane::release(stream, accepted.node, accepted.negotiated, Some(1), None);
            lane_tx.send((lane, fd)).expect("B is waiting");
        })
        .expect("executor A");
    // B adopts the descriptor and serves the request the dialler sent behind its hello
    let b = glommio::LocalExecutorBuilder::default()
        .spawn(move || async move {
            let (lane, fd) = loop {
                match lane_rx.try_recv() {
                    Ok(handed) => break handed,
                    Err(_) => glommio::timer::sleep(Duration::from_millis(1)).await,
                }
            };
            let version = lane.negotiated.version;
            assert_eq!((lane.origin, lane.slot), (node0, Some(1)));
            let stream = lane.adopt();
            assert_eq!(
                stream.as_raw_fd(),
                fd,
                "the same descriptor, never duplicated"
            );
            let (mut rx, mut tx) = futures::AsyncReadExt::split(stream);
            let header = codec::read_header(&mut rx, 1 << 20, version)
                .await
                .expect("a header")
                .expect("not the end of the stream");
            assert_eq!(header.kind, MessageType::Replicate);
            let raw: [u8; REPLICATE_HEAD_LEN] = codec::read_array(&mut rx).await.expect("a head");
            let head = ReplicateRequestHead::decode(&raw).expect("a valid head");
            assert_eq!(head.target_shard, 1);
            let payload = codec::read_vec(&mut rx, header.body_len() - REPLICATE_HEAD_LEN)
                .await
                .expect("the payload");
            assert!(
                payload == expected,
                "every byte arrived on the adopting executor"
            );
            // the answer goes back on the same connection, from this executor
            let answer = ReplicateResponseHead {
                id: head.id,
                status: ReplicateStatus::Ok,
            }
            .encode();
            let header = codec::header_at(
                version,
                MessageType::ReplicateResponse,
                answer.len() + payload.len(),
                1 << 20,
            )
            .expect("a header");
            codec::write_frame(&mut tx, &header, &[&answer, &payload])
                .await
                .expect("the answer is written");
        })
        .expect("executor B");
    // the dialler, on this thread: a hello naming slot one and a request straight behind it
    let addr = addr_rx.recv().expect("A's address");
    let mut sock = std::net::TcpStream::connect(addr).expect("connect");
    let hello = PeerHello {
        slot: Some(1),
        ..a_hello(
            *cluster.0.as_bytes(),
            *node0.0.as_bytes(),
            2,
            Lane::Replication,
            schema_id,
        )
    };
    sock.write_all(&hello.frame(1 << 20).expect("a hello frame"))
        .expect("the hello");
    let head = ReplicateRequestHead {
        id: 77,
        group: 5,
        target_shard: 1,
        kind: ReplicateKind::AppendEntries,
        deadline_ms: 5_000,
    }
    .encode();
    let header = codec::header_at(
        PROTOCOL_VERSION,
        MessageType::Replicate,
        head.len() + pattern.len(),
        1 << 20,
    )
    .expect("a header");
    sock.write_all(&header).expect("the request's header");
    sock.write_all(&head).expect("the request's head");
    sock.write_all(&pattern).expect("the request's payload");
    // the ack first, which names no slot of its own
    let mut ack = [0u8; PEER_HELLO_FRAME_LEN];
    sock.read_exact(&mut ack).expect("the ack");
    let mut body = [0u8; PEER_HELLO_BODY_LEN];
    body.copy_from_slice(&ack[HEADER_LEN..]);
    let ack = PeerHelloAck::decode(&body).expect("an ack");
    assert_eq!(ack.reason, PeerRefusal::Accepted);
    assert_eq!(ack.hello.slot, None);
    // then the answer, from the executor that adopted the lane
    let mut raw = [0u8; HEADER_LEN];
    sock.read_exact(&mut raw).expect("the answer's header");
    let header = Header::decode(&raw, 1 << 20).expect("a valid header");
    assert_eq!(header.kind, MessageType::ReplicateResponse);
    let mut head = [0u8; REPLICATE_RESPONSE_HEAD_LEN];
    sock.read_exact(&mut head).expect("the answer's head");
    assert_eq!(ReplicateResponseHead::decode(&head).id, 77);
    let mut echoed = vec![0u8; header.body_len() - REPLICATE_RESPONSE_HEAD_LEN];
    sock.read_exact(&mut echoed).expect("the answer's payload");
    assert!(echoed == pattern, "the answer came back whole");
    a.join().expect("A finished");
    b.join().expect("B finished");
}
