//! The peer hello, from both ends
//!
//! The dialler writes its record and reads the acceptor's; the acceptor reads, judges, and
//! writes its own record with the verdict. Every check the acceptor makes, the dialler makes
//! too against the ack - a node that accepted us is still checked to be the node we dialled -
//! so neither end trusts the other's say-so about who it is.
//!
//! Since [F39](../../../../docs/src/features/membership.md) the judge reads the committed
//! membership rather than a static placement, through the [`Admission`] trait: a shard judges
//! against the map its control plane pushed it, the control thread against its applied state.
//! Two things the placement never had to allow are allowed here. A **joiner** dials a seed's
//! control lane with no cluster at all - the nil id - because the cluster it will adopt is
//! what the seed's ack proves; the acceptor lets that hello in on the control lane alone, and
//! marks the connection as one that may only ask to join. And a node that has been **told
//! nothing yet** - a joiner between admission and its first log entry - accepts any member of
//! the cluster it adopted, since refusing the leader that is trying to replicate to it would
//! be refusing the only thing that could ever tell it who its members are.
//!
//! What a hello proves is bounded. The cluster and node identities in it are what the peer
//! *claims*; what makes the claim worth anything is the transport underneath. Under
//! `cluster.tls` the peer holds a certificate the cluster's authority signed, and the claim is a
//! member's claim; in plaintext it is a claim from inside whatever boundary the deployment drew.
//!
//! Since [F48](../../../../docs/src/features/rolling-compatibility.md) the hello is a
//! negotiation rather than a match: each end advertises the range of wire versions it reads,
//! the two speak the highest both hold ([`Negotiated`]), and every frame after the hello is
//! written at that version or below it. A peer whose newest version is below the one the
//! cluster has **activated** is refused at this door too, whatever it could otherwise speak,
//! which is what makes an activation the boundary past which no member rolls back.

use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;

use crate::server::errors::ShoalError;
use crate::server::meta::Identity;
use crate::server::ServerError;
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::protocol::peer::{
    Lane, PeerHello, PeerHelloAck, PeerRefusal, CAPABILITIES, PEER_HELLO_BODY_LEN, REQUIRED_CAPABILITIES,
};
use crate::shared::protocol::{self, MessageType, ProtocolError, HEADER_LEN, MIN_PEER_VERSION};

/// What this node says about itself in every hello
#[derive(Debug, Clone)]
pub struct Local {
    /// This node's identity
    pub node: NodeId,
    /// The cluster it belongs to, or none for a joiner that has not adopted one yet
    pub cluster: Option<ClusterId>,
    /// How many shards it runs
    pub shards: u16,
    /// The structural fingerprint of the schema it serves
    pub schema_id: u64,
    /// The largest frame it accepts
    pub max_frame_bytes: u32,
    /// Which start of this node this is, from the marker
    ///
    /// Carried in every hello, so a peer can tell a restart from a duplicate and the control
    /// plane can fence the lower of two runs of one directory.
    pub incarnation: u64,
    /// The newest wire version this node advertises
    ///
    /// The build's `PROTOCOL_VERSION` unless `cluster.transport.wire_version` pins it lower, which is
    /// how a node is held at the version it spoke before an upgrade until the operator
    /// activates the new one ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    pub wire_max: u8,
}

impl Local {
    /// Build this node's record from its identity
    ///
    /// # Arguments
    ///
    /// * `identity` - The identity the marker holds
    /// * `shards` - How many shards this node runs
    /// * `schema_id` - The structural fingerprint of the schema it serves
    /// * `max_frame_bytes` - The largest frame it accepts
    /// * `wire_pin` - The newest wire version to advertise, or none for this build's newest
    #[must_use]
    pub fn new(identity: &Identity, shards: usize, schema_id: u64, max_frame_bytes: u32, wire_pin: Option<u8>) -> Self {
        // the range this node advertises, bounded above by the pin
        let (_, wire_max) = PeerHello::range(wire_pin);
        // a node runs fewer shards than a u16 holds; the ring refuses more
        #[allow(clippy::cast_possible_truncation)]
        Local {
            node: identity.node,
            cluster: identity.cluster,
            shards: shards as u16,
            schema_id,
            max_frame_bytes,
            incarnation: identity.incarnation,
            wire_max,
        }
    }

    /// The hello this node writes on a lane
    ///
    /// # Arguments
    ///
    /// * `lane` - Which lane the connection carries
    #[must_use]
    pub fn hello(&self, lane: Lane) -> PeerHello {
        PeerHello {
            cluster: *self.cluster.unwrap_or_default().0.as_bytes(),
            node: *self.node.0.as_bytes(),
            incarnation: self.incarnation,
            lane,
            wire_min: MIN_PEER_VERSION,
            wire_max: self.wire_max,
            capabilities: CAPABILITIES,
            schema_id: self.schema_id,
            shards: self.shards,
            max_frame_bytes: self.max_frame_bytes,
        }
    }
}

/// What two ends of a lane agreed at the hello
///
/// Every frame after the hello is written at `version` or below it and decoded at the version
/// its own header names; a frame above `version` is refused by version. `capabilities` is
/// the intersection of the two words, and is what either end may act on
/// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negotiated {
    /// The highest wire version both ends read
    pub version: u8,
    /// The capabilities both ends act on
    pub capabilities: u64,
    /// The largest frame the peer accepts
    pub max_frame_bytes: u32,
    /// The newest wire version the peer's own build speaks, whatever was negotiated
    ///
    /// What a node knows of each member's build from its own hellos, which is one of the two
    /// live sources an activation is judged by.
    pub peer_wire_max: u8,
}

impl Negotiated {
    /// What was agreed with a peer, given both hellos
    ///
    /// # Arguments
    ///
    /// * `peer` - The peer's record
    /// * `ours` - This end's record
    /// * `version` - The version [`PeerHello::negotiate`] chose
    #[must_use]
    pub const fn of(peer: &PeerHello, ours: &PeerHello, version: u8) -> Self {
        Negotiated {
            version,
            capabilities: peer.common_capabilities(ours),
            max_frame_bytes: peer.max_frame_bytes,
            peer_wire_max: peer.wire_max,
        }
    }

    /// What a link that is not up yet may safely assume: the floor, and nothing acted on
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The largest frame to assume the peer accepts
    #[must_use]
    pub const fn floor(max_frame_bytes: u32) -> Self {
        Negotiated {
            version: MIN_PEER_VERSION,
            capabilities: 0,
            max_frame_bytes,
            peer_wire_max: MIN_PEER_VERSION,
        }
    }

    /// Whether the peer acts on a capability
    ///
    /// # Arguments
    ///
    /// * `capability` - The capability bit
    #[must_use]
    pub const fn has(&self, capability: u64) -> bool {
        self.capabilities & capability == capability
    }
}

/// Where to dial a peer, and who to expect there
///
/// Built from a committed member record by whoever dials: the map on a shard, the record
/// openraft hands the control network. A joiner dialling a seed knows only an address, and says
/// so with no node: whoever answers is accepted, and its cluster is learned from the ack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddr {
    /// The node expected to answer, or none to accept whoever does
    pub node: Option<NodeId>,
    /// Where its data and bulk lanes are dialled
    pub data: String,
    /// Where its control lane is dialled
    pub control: String,
    /// How many shards it runs, checked against its hello when the node is expected
    pub shards: u16,
}

impl PeerAddr {
    /// A seed to discover a cluster through: a control address and nothing else
    ///
    /// # Arguments
    ///
    /// * `control` - The seed's control address
    #[must_use]
    pub fn seed(control: &str) -> Self {
        PeerAddr {
            node: None,
            data: String::new(),
            control: control.to_string(),
            shards: 0,
        }
    }

    /// The node this addresses, or the nil id for a seed
    #[must_use]
    pub fn node_or_nil(&self) -> NodeId {
        self.node.unwrap_or_default()
    }
}

/// What an acceptor decides about a peer's identity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// A member, at an incarnation the cluster has not superseded
    Member,
    /// Not a member this node knows
    Unknown,
    /// A member at an incarnation below the committed one
    Fenced {
        /// The incarnation the cluster holds for it
        committed: u64,
    },
    /// A member running a different number of shards than the cluster recorded
    ShardCount {
        /// The count the cluster recorded
        expected: u16,
    },
}

/// What a listener judges a peer's identity against
///
/// A shard implements it over the map its control plane pushed; the control thread over its
/// applied state. Neither is a static file.
pub trait Admission {
    /// Judge a peer by its identity, incarnation and shard count
    ///
    /// # Arguments
    ///
    /// * `node` - The peer's identity
    /// * `incarnation` - Which start of it this is
    /// * `shards` - How many shards it says it runs
    fn judge(&self, node: NodeId, incarnation: u64, shards: u16) -> Verdict;

    /// The cluster this listener serves, or none for a joiner that has not adopted one
    fn cluster(&self) -> Option<ClusterId>;

    /// The wire version the cluster has activated, which every member has to speak
    ///
    /// [`MIN_PEER_VERSION`] until an operator activates a newer one
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    fn activated_wire(&self) -> u8;
}

/// What the acceptor learned about a peer it let in
#[derive(Debug, Clone)]
pub struct Accepted {
    /// The peer's identity
    pub node: NodeId,
    /// Which run of it this is
    pub incarnation: u64,
    /// Which lane this connection carries
    pub lane: Lane,
    /// The largest frame the peer accepts
    pub max_frame_bytes: u32,
    /// Whether the peer is a joiner with no cluster, let in to ask for one
    ///
    /// Such a connection may ask to join and ping, and nothing else.
    pub joining: bool,
    /// What the two ends agreed to speak
    pub negotiated: Negotiated,
}

/// The most bytes a hello may carry past the ones this build reads
///
/// A hello from a build that grew the record is read for the fields this build knows and the
/// rest drained, so a longer hello is a negotiation rather than a `BodyTooShort`; one longer
/// than this is not a hello at all.
const HELLO_GROWTH: usize = 256;

/// Read one hello or ack frame off a stream that has not been split
///
/// # Arguments
///
/// * `stream` - The connection
/// * `expected` - Which of the two frames has to be here
async fn read_body(
    stream: &mut TcpStream,
    expected: MessageType,
) -> Result<[u8; PEER_HELLO_BODY_LEN], ServerError> {
    // the header first, judged before its length sizes anything
    let mut raw = [0u8; HEADER_LEN];
    stream.read_exact(&mut raw).await?;
    let header = protocol::RawHeader::decode(&raw);
    // a hello of a version we do not read is refused by version, which the header always
    // says; the range is the floor up to this build's newest, since nothing is negotiated yet
    let header = header.validate(u32::try_from(PEER_HELLO_BODY_LEN + HELLO_GROWTH).unwrap_or(u32::MAX))?;
    let header = header.expect(expected)?;
    // a hello is at least the record this build reads; a longer one is read for that record
    // and the rest drained, a shorter one is not a hello
    if header.body_len() < PEER_HELLO_BODY_LEN {
        return Err(ProtocolError::BodyTooShort {
            need: PEER_HELLO_BODY_LEN,
            got: header.len,
        }
        .into());
    }
    let mut body = [0u8; PEER_HELLO_BODY_LEN];
    stream.read_exact(&mut body).await?;
    let extra = header.body_len() - PEER_HELLO_BODY_LEN;
    if extra > 0 {
        let mut rest = vec![0u8; extra];
        stream.read_exact(&mut rest).await?;
    }
    Ok(body)
}

/// Open a lane to a peer: write our hello, read its ack, and check the ack is the peer we meant
///
/// # Arguments
///
/// * `stream` - The freshly connected socket, after TLS if there is any
/// * `local` - What this node says about itself
/// * `lane` - Which lane this connection carries
/// * `expected` - Who this connection was dialled for
pub async fn dial(
    stream: &mut TcpStream,
    local: &Local,
    lane: Lane,
    expected: &PeerAddr,
) -> Result<(PeerHello, Negotiated), ServerError> {
    // the dialler speaks first
    let hello = local.hello(lane);
    stream.write_all(&hello.frame(local.max_frame_bytes)?).await?;
    stream.flush().await?;
    // and reads the verdict, which carries the acceptor's own record
    let body = read_body(stream, MessageType::PeerHelloAck).await?;
    let ack = PeerHelloAck::decode(&body)?;
    if !ack.reason.is_accepted() {
        return Err(ServerError::Shoal(ShoalError::PeerRefused {
            node: expected.node_or_nil(),
            reason: ack.reason,
        }));
    }
    // an acceptance from the wrong node is still the wrong node
    let negotiated = check_peer(&ack.hello, &hello, local, expected)?;
    Ok((ack.hello, negotiated))
}

/// Check a peer's record against ours and against who we dialled
///
/// Shared by both ends, so the dialler judges an ack by exactly the rules the acceptor judged
/// its hello by. A dialler with no cluster yet - a joiner at its seed - learns the cluster from
/// the ack rather than checking it; a dial that named no node accepts whoever answered.
///
/// # Arguments
///
/// * `peer` - The record the peer sent
/// * `ours` - The hello this end wrote
/// * `local` - What this node says about itself
/// * `expected` - Who was dialled
fn check_peer(peer: &PeerHello, ours: &PeerHello, local: &Local, expected: &PeerAddr) -> Result<Negotiated, ServerError> {
    let found = NodeId(uuid::Uuid::from_bytes(peer.node));
    let cluster = ClusterId(uuid::Uuid::from_bytes(peer.cluster));
    // the same cluster, before anything else about the peer is believed
    if let Some(ours) = local.cluster {
        if cluster != ours {
            return Err(ServerError::Shoal(ShoalError::WrongCluster {
                found: cluster,
                expected: Some(ours),
            }));
        }
    }
    // the node we dialled, and the shards the cluster recorded for it
    if let Some(node) = expected.node {
        if found != node {
            return Err(ServerError::Shoal(ShoalError::PeerIdentity {
                expected: node,
                found,
            }));
        }
        if peer.shards != expected.shards {
            return Err(ServerError::Shoal(ShoalError::PeerShardCount {
                node: found,
                placed: expected.shards,
                claimed: peer.shards,
            }));
        }
    }
    // built from the same schema
    if peer.schema_id != local.schema_id {
        return Err(ServerError::Shoal(ShoalError::PeerSchema {
            node: found,
            ours: local.schema_id,
            theirs: peer.schema_id,
        }));
    }
    // and sharing a wire version, which every frame after this is written at or below
    let Some(version) = peer.negotiate(ours) else {
        return Err(ProtocolError::UnsupportedVersion {
            got: peer.wire_max,
            ours: ours.wire_max,
        }
        .into());
    };
    let negotiated = Negotiated::of(peer, ours, version);
    // and acting on everything a member has to
    if !negotiated.has(REQUIRED_CAPABILITIES) {
        return Err(ServerError::Shoal(ShoalError::PeerRefused {
            node: found,
            reason: PeerRefusal::CapabilityMissing,
        }));
    }
    Ok(negotiated)
}

/// Accept a lane from a peer: read its hello, judge it, and answer with our record and a verdict
///
/// A refusal is written before the error is returned, so the peer's log says why.
///
/// # Arguments
///
/// * `stream` - The freshly accepted socket, after TLS if there is any
/// * `local` - What this node says about itself
/// * `served` - The lanes this listener serves
/// * `admission` - What this listener judges a peer's identity against
pub async fn accept(
    stream: &mut TcpStream,
    local: &Local,
    served: &[Lane],
    admission: &dyn Admission,
) -> Result<Accepted, ServerError> {
    // the peer speaks first
    let body = read_body(stream, MessageType::PeerHello).await?;
    let hello = PeerHello::decode(&body)?;
    // judge it in the order the refusals are documented
    let ours = local.hello(hello.lane);
    let (verdict, outcome) = judge(&hello, &ours, local, served, admission);
    // answer with our record and the verdict, on the lane the peer asked for
    let ack = PeerHelloAck {
        hello: ours,
        reason: verdict,
    };
    stream.write_all(&ack.frame(local.max_frame_bytes)?).await?;
    stream.flush().await?;
    // and only then act on it
    let (joining, negotiated) = outcome?;
    Ok(Accepted {
        node: NodeId(uuid::Uuid::from_bytes(hello.node)),
        incarnation: hello.incarnation,
        lane: hello.lane,
        max_frame_bytes: hello.max_frame_bytes,
        joining,
        negotiated,
    })
}

/// Decide whether a hello is let in, and what to say either way
///
/// Returns the refusal to write and, on the other side, whether the peer is a joiner and what
/// the two ends agreed to speak.
///
/// # Arguments
///
/// * `hello` - The peer's record
/// * `ours` - The record this end answers with
/// * `local` - What this node says about itself
/// * `served` - The lanes this listener serves
/// * `admission` - What this listener judges a peer's identity against
fn judge(
    hello: &PeerHello,
    ours: &PeerHello,
    local: &Local,
    served: &[Lane],
    admission: &dyn Admission,
) -> (PeerRefusal, Result<(bool, Negotiated), ServerError>) {
    let found = NodeId(uuid::Uuid::from_bytes(hello.node));
    let cluster = ClusterId(uuid::Uuid::from_bytes(hello.cluster));
    // the wire version, since nothing after the hello can be read otherwise
    let Some(version) = hello.negotiate(ours) else {
        return (
            PeerRefusal::NoCommonVersion,
            Err(ProtocolError::UnsupportedVersion {
                got: hello.wire_max,
                ours: ours.wire_max,
            }
            .into()),
        );
    };
    let negotiated = Negotiated::of(hello, ours, version);
    // the capabilities every member acts on, which a peer in the range is never without
    if !negotiated.has(REQUIRED_CAPABILITIES) {
        return (
            PeerRefusal::CapabilityMissing,
            Err(ServerError::Shoal(ShoalError::PeerRefused {
                node: found,
                reason: PeerRefusal::CapabilityMissing,
            })),
        );
    }
    // the version the cluster activated, which a member has to speak whatever it could
    // negotiate with this one node: an activation is the boundary no member rolls back past
    let activated = admission.activated_wire();
    if hello.wire_max < activated {
        return (
            PeerRefusal::BelowActivatedWire,
            Err(ServerError::Shoal(ShoalError::BelowActivatedWire {
                node: found,
                activated,
                offered: hello.wire_max,
            })),
        );
    }
    // the lane, which this listener may not serve at all
    if !served.contains(&hello.lane) {
        return (
            PeerRefusal::LaneRefused,
            Err(ServerError::Shoal(ShoalError::PeerLane {
                node: found,
                lane: hello.lane,
            })),
        );
    }
    // a joiner has no cluster, and may ask for one on the control lane alone
    if cluster == ClusterId::default() {
        if hello.lane != Lane::Control {
            return (
                PeerRefusal::NotJoinable,
                Err(ServerError::Shoal(ShoalError::PeerRefused {
                    node: found,
                    reason: PeerRefusal::NotJoinable,
                })),
            );
        }
        // built from the same schema, even before it belongs to anything
        if hello.schema_id != local.schema_id {
            return (
                PeerRefusal::SchemaMismatch,
                Err(ServerError::Shoal(ShoalError::PeerSchema {
                    node: found,
                    ours: local.schema_id,
                    theirs: hello.schema_id,
                })),
            );
        }
        return (PeerRefusal::Accepted, Ok((true, negotiated)));
    }
    // the cluster, which is what `Identity::verify_cluster` was written to check; a listener
    // with no cluster yet refuses every member's hello, since it cannot know whose
    let ours = admission.cluster().or(local.cluster);
    if ours != Some(cluster) {
        return (
            PeerRefusal::WrongCluster,
            Err(ServerError::Shoal(ShoalError::WrongCluster {
                found: cluster,
                expected: ours,
            })),
        );
    }
    // the node, judged against the committed membership
    match admission.judge(found, hello.incarnation, hello.shards) {
        Verdict::Member => {}
        Verdict::Unknown => {
            return (
                PeerRefusal::UnknownNode,
                Err(ServerError::Shoal(ShoalError::PeerIdentity {
                    expected: local.node,
                    found,
                })),
            );
        }
        Verdict::Fenced { committed } => {
            return (
                PeerRefusal::Fenced,
                Err(ServerError::Shoal(ShoalError::PeerFenced {
                    node: found,
                    committed,
                    offered: hello.incarnation,
                })),
            );
        }
        Verdict::ShardCount { expected } => {
            return (
                PeerRefusal::ShardCountMismatch,
                Err(ServerError::Shoal(ShoalError::PeerShardCount {
                    node: found,
                    placed: expected,
                    claimed: hello.shards,
                })),
            );
        }
    }
    // built from the same schema
    if hello.schema_id != local.schema_id {
        return (
            PeerRefusal::SchemaMismatch,
            Err(ServerError::Shoal(ShoalError::PeerSchema {
                node: found,
                ours: local.schema_id,
                theirs: hello.schema_id,
            })),
        );
    }
    (PeerRefusal::Accepted, Ok((false, negotiated)))
}
