//! The peer hello, from both ends
//!
//! The dialler writes its record and reads the acceptor's; the acceptor reads, judges, and
//! writes its own record with the verdict. Every check the acceptor makes, the dialler makes
//! too against the ack - a node that accepted us is still checked to be the node the placement
//! said lives at that address - so neither end trusts the other's say-so about who it is.
//!
//! What a hello proves is bounded. The cluster and node identities in it are what the peer
//! *claims*; what makes the claim worth anything is the transport underneath. Under
//! `cluster.tls` the peer holds a certificate the cluster's authority signed, and the claim is a
//! member's claim; in plaintext it is a claim from inside whatever boundary the deployment drew.
//! The binding of a certificate to one node identity is Q11's contract and is enforced when a
//! joiner exists, which is recorded on the F page and nowhere hidden.

use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;

use super::incarnation;
use crate::server::conf::cluster::{PlacedNode, Placement};
use crate::server::errors::ShoalError;
use crate::server::meta::Identity;
use crate::server::ServerError;
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::protocol::peer::{
    Lane, PeerHello, PeerHelloAck, PeerRefusal, CAPABILITIES, PEER_HELLO_BODY_LEN,
};
use crate::shared::protocol::{self, MessageType, ProtocolError, HEADER_LEN, PROTOCOL_VERSION};

/// What this node says about itself in every hello
#[derive(Debug, Clone)]
pub struct Local {
    /// This node's identity
    pub node: NodeId,
    /// The cluster it belongs to
    pub cluster: ClusterId,
    /// How many shards it runs
    pub shards: u16,
    /// The structural fingerprint of the schema it serves
    pub schema_id: u64,
    /// The largest frame it accepts
    pub max_frame_bytes: u32,
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
    ///
    /// # Errors
    ///
    /// A standalone identity has no cluster, and a node with no cluster has no peers.
    pub fn new(
        identity: &Identity,
        shards: usize,
        schema_id: u64,
        max_frame_bytes: u32,
    ) -> Result<Self, ServerError> {
        let Some(cluster) = identity.cluster else {
            return Err(ServerError::Shoal(ShoalError::NotClustered));
        };
        // a node runs fewer shards than a u16 holds; the ring refuses more
        #[allow(clippy::cast_possible_truncation)]
        Ok(Local {
            node: identity.node,
            cluster,
            shards: shards as u16,
            schema_id,
            max_frame_bytes,
        })
    }

    /// The hello this node writes on a lane
    ///
    /// # Arguments
    ///
    /// * `lane` - Which lane the connection carries
    #[must_use]
    pub fn hello(&self, lane: Lane) -> PeerHello {
        PeerHello {
            cluster: *self.cluster.0.as_bytes(),
            node: *self.node.0.as_bytes(),
            incarnation: incarnation(),
            lane,
            wire_min: PROTOCOL_VERSION,
            wire_max: PROTOCOL_VERSION,
            capabilities: CAPABILITIES,
            schema_id: self.schema_id,
            shards: self.shards,
            max_frame_bytes: self.max_frame_bytes,
        }
    }
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
}

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
    // a hello of a version we do not read is refused by version, which the header always says
    if header.version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion {
            got: header.version,
            ours: PROTOCOL_VERSION,
        }
        .into());
    }
    let header = header.validate(u32::try_from(PEER_HELLO_BODY_LEN).unwrap_or(u32::MAX))?;
    let header = header.expect(expected)?;
    // a hello is a fixed size, so one of any other size is not a hello
    if header.body_len() != PEER_HELLO_BODY_LEN {
        return Err(ProtocolError::BodyTooShort {
            need: PEER_HELLO_BODY_LEN,
            got: header.len,
        }
        .into());
    }
    let mut body = [0u8; PEER_HELLO_BODY_LEN];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

/// Open a lane to a peer: write our hello, read its ack, and check the ack is the peer we meant
///
/// # Arguments
///
/// * `stream` - The freshly connected socket, after TLS if there is any
/// * `local` - What this node says about itself
/// * `lane` - Which lane this connection carries
/// * `expected` - The placement entry this connection was dialled for
pub async fn dial(
    stream: &mut TcpStream,
    local: &Local,
    lane: Lane,
    expected: &PlacedNode,
) -> Result<PeerHello, ServerError> {
    // the dialler speaks first
    let hello = local.hello(lane);
    stream.write_all(&hello.frame(local.max_frame_bytes)?).await?;
    stream.flush().await?;
    // and reads the verdict, which carries the acceptor's own record
    let body = read_body(stream, MessageType::PeerHelloAck).await?;
    let ack = PeerHelloAck::decode(&body)?;
    if !ack.reason.is_accepted() {
        return Err(ServerError::Shoal(ShoalError::PeerRefused {
            node: expected.node,
            reason: ack.reason,
        }));
    }
    // an acceptance from the wrong node is still the wrong node
    check_peer(&ack.hello, local, expected)?;
    Ok(ack.hello)
}

/// Check a peer's record against ours and against its placement entry
///
/// Shared by both ends, so the dialler judges an ack by exactly the rules the acceptor judged
/// its hello by.
///
/// # Arguments
///
/// * `peer` - The record the peer sent
/// * `local` - What this node says about itself
/// * `expected` - The placement entry the peer has to match
fn check_peer(peer: &PeerHello, local: &Local, expected: &PlacedNode) -> Result<(), ServerError> {
    let found = NodeId(uuid::Uuid::from_bytes(peer.node));
    let cluster = ClusterId(uuid::Uuid::from_bytes(peer.cluster));
    // the same cluster, before anything else about the peer is believed
    if cluster != local.cluster {
        return Err(ServerError::Shoal(ShoalError::WrongCluster {
            found: cluster,
            expected: Some(local.cluster),
        }));
    }
    // the node the placement said lives there
    if found != expected.node {
        return Err(ServerError::Shoal(ShoalError::PeerIdentity {
            expected: expected.node,
            found,
        }));
    }
    // running the shards the placement routes to
    if peer.shards != expected.shards {
        return Err(ServerError::Shoal(ShoalError::PeerShardCount {
            node: found,
            placed: expected.shards,
            claimed: peer.shards,
        }));
    }
    // built from the same schema
    if peer.schema_id != local.schema_id {
        return Err(ServerError::Shoal(ShoalError::PeerSchema {
            node: found,
            ours: local.schema_id,
            theirs: peer.schema_id,
        }));
    }
    // and reading the wire version every frame after this is written at
    if !peer.speaks_our_version() {
        return Err(ProtocolError::UnsupportedVersion {
            got: peer.wire_max,
            ours: PROTOCOL_VERSION,
        }
        .into());
    }
    Ok(())
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
/// * `placement` - Every node this listener will accept a hello from
pub async fn accept(
    stream: &mut TcpStream,
    local: &Local,
    served: &[Lane],
    placement: &Placement,
) -> Result<Accepted, ServerError> {
    // the peer speaks first
    let body = read_body(stream, MessageType::PeerHello).await?;
    let hello = PeerHello::decode(&body)?;
    // judge it in the order the refusals are documented
    let (verdict, outcome) = judge(&hello, local, served, placement);
    // answer with our record and the verdict, on the lane the peer asked for
    let ack = PeerHelloAck {
        hello: local.hello(hello.lane),
        reason: verdict,
    };
    stream.write_all(&ack.frame(local.max_frame_bytes)?).await?;
    stream.flush().await?;
    // and only then act on it
    outcome?;
    Ok(Accepted {
        node: NodeId(uuid::Uuid::from_bytes(hello.node)),
        incarnation: hello.incarnation,
        lane: hello.lane,
        max_frame_bytes: hello.max_frame_bytes,
    })
}

/// Decide whether a hello is let in, and what to say either way
///
/// # Arguments
///
/// * `hello` - The peer's record
/// * `local` - What this node says about itself
/// * `served` - The lanes this listener serves
/// * `placement` - Every node this listener will accept a hello from
fn judge(
    hello: &PeerHello,
    local: &Local,
    served: &[Lane],
    placement: &Placement,
) -> (PeerRefusal, Result<(), ServerError>) {
    let found = NodeId(uuid::Uuid::from_bytes(hello.node));
    let cluster = ClusterId(uuid::Uuid::from_bytes(hello.cluster));
    // the wire version, since nothing after the hello can be read otherwise
    if !hello.speaks_our_version() {
        return (
            PeerRefusal::NoCommonVersion,
            Err(ProtocolError::UnsupportedVersion {
                got: hello.wire_max,
                ours: PROTOCOL_VERSION,
            }
            .into()),
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
    // the cluster, which is what `Identity::verify_cluster` was written to check
    if cluster != local.cluster {
        return (
            PeerRefusal::WrongCluster,
            Err(ServerError::Shoal(ShoalError::WrongCluster {
                found: cluster,
                expected: Some(local.cluster),
            })),
        );
    }
    // the node, which has to be one the placement names
    let Some(placed) = placement.peer(found) else {
        return (
            PeerRefusal::UnknownNode,
            Err(ServerError::Shoal(ShoalError::PeerIdentity {
                expected: local.node,
                found,
            })),
        );
    };
    // with the shards the placement routes to
    if placed.shards != hello.shards {
        return (
            PeerRefusal::ShardCountMismatch,
            Err(ServerError::Shoal(ShoalError::PeerShardCount {
                node: found,
                placed: placed.shards,
                claimed: hello.shards,
            })),
        );
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
    (PeerRefusal::Accepted, Ok(()))
}
