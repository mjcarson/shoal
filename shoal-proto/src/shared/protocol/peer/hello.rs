//! The two frames a peer connection opens with
//!
//! ```text
//!  ┌────────────┬────────────┬─────────────┬──────┬─────────┬─────────┬────────┬──────────────┬───────────┬────────┬──────────┬─────────────────┐
//!  │ cluster id │  node id   │ incarnation │ lane │ wire min│ wire max│ reason │ capabilities │ schema id │ shards │ reserved │ max frame bytes │
//!  │   (16 B)   │   (16 B)   │  (u64, 8 B) │ (1 B)│  (1 B)  │  (1 B)  │ (1 B)  │  (u64, 8 B)  │(u64, 8 B) │(u16,2B)│  (2 B)   │   (u32, 4 B)    │
//!  └────────────┴────────────┴─────────────┴──────┴─────────┴─────────┴────────┴──────────────┴───────────┴────────┴──────────┴─────────────────┘
//! ```
//!
//! A hello and its ack have one layout. The dialler writes its own record with `reason` zero;
//! the acceptor answers with *its* record and the reason it accepted or refused, so both ends
//! learn the other's identity, incarnation, shard count and capabilities from one frame each.
//!
//! **Schema identity and wire version are separate fields**, which is the Q10 contract
//! ([C13](../../../../../docs/src/distributed/protocol.md)): `schema_id` is the structural
//! fingerprint of the schema alone, `wire_min..=wire_max` the versions of this framing the peer
//! reads, and `capabilities` the peer features it can act on. A client hello folds the protocol
//! version into its fingerprint and that stays as it is; a peer keeps the three apart so that a
//! rolling upgrade can tell "different schema" from "newer transport" from "same everything".
//! ~~At M2 all three are required to match exactly - the contract is defined here, and the
//! codecs for an older version are what M10 owes before n−1 is accepted.~~ Since
//! [F48](../../../../../docs/src/features/rolling-compatibility.md) the three are judged
//! apart: the schema id exactly, the wire version as the highest both ranges hold
//! ([`PeerHello::negotiate`]), and the capabilities as the intersection
//! ([`PeerHello::common_capabilities`]). The hello frame itself is written at
//! [`MIN_PEER_VERSION`] so that any peer in the range reads it.

use super::super::{
    Flags, Header, MessageType, ProtocolError, HEADER_LEN, MIN_PEER_VERSION, PROTOCOL_VERSION,
};
use super::{bytes16_at, u16_at, u32_at, u64_at, Lane, PeerRefusal};

/// The size of a peer hello body in bytes
pub const PEER_HELLO_BODY_LEN: usize = 68;

/// The size of a whole peer hello frame, header included
pub const PEER_HELLO_FRAME_LEN: usize = HEADER_LEN + PEER_HELLO_BODY_LEN;

/// This peer forwards bundles and answers them
pub const CAP_FORWARD_V1: u64 = 1 << 0;

/// This peer carries the control group's consensus RPCs on its control lane
pub const CAP_CONTROL_RAFT_V1: u64 = 1 << 1;

/// This peer carries snapshot streams on its bulk lane
pub const CAP_BULK_SNAPSHOT_V1: u64 = 1 << 2;

/// This peer admits joiners over its control lane and carries membership RPCs and status reports
pub const CAP_MEMBERSHIP_V1: u64 = 1 << 3;

/// This peer hosts tablet groups and carries their consensus RPCs on its replication lane
pub const CAP_REPLICATION_V1: u64 = 1 << 4;

/// This peer carries read plans on its forward entries, attempts and tokens on its answers, and
/// read barriers on its replication lane ([F41](../../../../../docs/src/features/read-consistency.md))
pub const CAP_READ_CONSISTENCY_V1: u64 = 1 << 5;

/// This peer answers the consensus library's pre-vote on its replication lane
///
/// Optional: a peer without it is still a member, and a pre-vote to it is granted locally, the
/// library's own default, rather than sent as a kind it would refuse to decode
/// ([Resolved #144](../../../../../docs/src/appendix/resolved/post-heal-elections.md)).
pub const CAP_PRE_VOTE_V1: u64 = 1 << 6;

/// Everything this build can act on
pub const CAPABILITIES: u64 = CAP_FORWARD_V1
    | CAP_CONTROL_RAFT_V1
    | CAP_BULK_SNAPSHOT_V1
    | CAP_MEMBERSHIP_V1
    | CAP_REPLICATION_V1
    | CAP_READ_CONSISTENCY_V1
    | CAP_PRE_VOTE_V1;

/// The capabilities a peer has to act on to be a member at all
///
/// Every bit above but [`CAP_PRE_VOTE_V1`]: each is what some version in [`MIN_PEER_VERSION`]`..=`[`PROTOCOL_VERSION`]
/// carries, so a peer in the range without one is a build this one does not know how to
/// half serve, and is refused ([`PeerRefusal::CapabilityMissing`]). A capability a future
/// build adds as optional is left out of this set and gated by `Negotiated::has` at the one
/// place it is acted on, the way `CLIENT_CAP_READ_OPTIONS` gates a client
/// ([F48](../../../../../docs/src/features/rolling-compatibility.md)).
pub const REQUIRED_CAPABILITIES: u64 = CAP_FORWARD_V1
    | CAP_CONTROL_RAFT_V1
    | CAP_BULK_SNAPSHOT_V1
    | CAP_MEMBERSHIP_V1
    | CAP_REPLICATION_V1
    | CAP_READ_CONSISTENCY_V1;

/// Where each field sits in the body
const CLUSTER_AT: usize = 0;
const NODE_AT: usize = 16;
const INCARNATION_AT: usize = 32;
const LANE_AT: usize = 40;
const WIRE_MIN_AT: usize = 41;
const WIRE_MAX_AT: usize = 42;
const REASON_AT: usize = 43;
const CAPABILITIES_AT: usize = 44;
const SCHEMA_AT: usize = 52;
const SHARDS_AT: usize = 60;
const MAX_FRAME_AT: usize = 64;

/// What a node says about itself when it opens, or answers, a peer connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHello {
    /// The cluster this node belongs to, as the uuid's bytes
    pub cluster: [u8; 16],
    /// This node's identity, as the uuid's bytes
    pub node: [u8; 16],
    /// Which run of this node this is
    ///
    /// Strictly greater for a later start of the same node, so a peer can tell a restart from a
    /// duplicate. At M2 it is the process start time in nanoseconds since the epoch, which is
    /// recorded as provisional under Q11.
    pub incarnation: u64,
    /// Which lane this connection carries
    pub lane: Lane,
    /// The oldest wire version this build reads
    pub wire_min: u8,
    /// The newest wire version this build reads
    pub wire_max: u8,
    /// What this build can act on, as [`CAPABILITIES`] bits
    pub capabilities: u64,
    /// The structural fingerprint of the schema this node serves
    pub schema_id: u64,
    /// How many shards this node runs, which is what the placement has to agree with
    pub shards: u16,
    /// The largest frame this node will accept on this connection
    pub max_frame_bytes: u32,
}

impl PeerHello {
    /// Write this hello's body, with a reason byte of zero
    ///
    /// # Arguments
    ///
    /// * `reason` - The reason byte to write, which is zero for a hello
    fn encode_with(&self, reason: PeerRefusal) -> [u8; PEER_HELLO_BODY_LEN] {
        let mut body = [0u8; PEER_HELLO_BODY_LEN];
        body[CLUSTER_AT..CLUSTER_AT + 16].copy_from_slice(&self.cluster);
        body[NODE_AT..NODE_AT + 16].copy_from_slice(&self.node);
        body[INCARNATION_AT..INCARNATION_AT + 8].copy_from_slice(&self.incarnation.to_le_bytes());
        body[LANE_AT] = self.lane.as_byte();
        body[WIRE_MIN_AT] = self.wire_min;
        body[WIRE_MAX_AT] = self.wire_max;
        body[REASON_AT] = reason.as_byte();
        body[CAPABILITIES_AT..CAPABILITIES_AT + 8]
            .copy_from_slice(&self.capabilities.to_le_bytes());
        body[SCHEMA_AT..SCHEMA_AT + 8].copy_from_slice(&self.schema_id.to_le_bytes());
        body[SHARDS_AT..SHARDS_AT + 2].copy_from_slice(&self.shards.to_le_bytes());
        // two reserved bytes stay zero
        body[MAX_FRAME_AT..MAX_FRAME_AT + 4].copy_from_slice(&self.max_frame_bytes.to_le_bytes());
        body
    }

    /// Write this hello's body
    #[must_use]
    pub fn encode(&self) -> [u8; PEER_HELLO_BODY_LEN] {
        self.encode_with(PeerRefusal::Accepted)
    }

    /// Read a hello's fields out of a body, leaving the reason byte to the caller
    ///
    /// # Arguments
    ///
    /// * `body` - The body to read
    fn decode_fields(body: &[u8; PEER_HELLO_BODY_LEN]) -> Result<Self, ProtocolError> {
        Ok(PeerHello {
            cluster: bytes16_at(body, CLUSTER_AT),
            node: bytes16_at(body, NODE_AT),
            incarnation: u64_at(body, INCARNATION_AT),
            lane: Lane::from_byte(body[LANE_AT])?,
            wire_min: body[WIRE_MIN_AT],
            wire_max: body[WIRE_MAX_AT],
            capabilities: u64_at(body, CAPABILITIES_AT),
            schema_id: u64_at(body, SCHEMA_AT),
            shards: u16_at(body, SHARDS_AT),
            max_frame_bytes: u32_at(body, MAX_FRAME_AT),
        })
    }

    /// Read a hello out of its body
    ///
    /// # Arguments
    ///
    /// * `body` - The body to read
    pub fn decode(body: &[u8; PEER_HELLO_BODY_LEN]) -> Result<Self, ProtocolError> {
        Self::decode_fields(body)
    }

    /// Build the whole frame for this hello
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The largest frame the peer will accept, which a hello always fits
    pub fn frame(&self, max_frame_bytes: u32) -> Result<[u8; PEER_HELLO_FRAME_LEN], ProtocolError> {
        frame_of(MessageType::PeerHello, self.encode(), max_frame_bytes)
    }

    /// The version two peers speak, if their ranges share one
    ///
    /// The highest version both read: the smaller of the two maxima, provided it is at or
    /// above both minima. Every frame after the hello is written at this version or below it
    /// ([F48](../../../../../docs/src/features/rolling-compatibility.md)).
    ///
    /// # Arguments
    ///
    /// * `ours` - This end's hello, whose range is what it advertised
    #[must_use]
    pub const fn negotiate(&self, ours: &PeerHello) -> Option<u8> {
        // the highest version both read
        let version = if self.wire_max < ours.wire_max {
            self.wire_max
        } else {
            ours.wire_max
        };
        // which has to be one both read
        if version >= self.wire_min && version >= ours.wire_min {
            Some(version)
        } else {
            None
        }
    }

    /// The capabilities both peers can act on
    ///
    /// # Arguments
    ///
    /// * `ours` - This end's hello
    #[must_use]
    pub const fn common_capabilities(&self, ours: &PeerHello) -> u64 {
        self.capabilities & ours.capabilities
    }

    /// The range this build advertises, bounded above by a pin
    ///
    /// # Arguments
    ///
    /// * `pin` - The newest version to advertise, or none for [`PROTOCOL_VERSION`]
    #[must_use]
    pub const fn range(pin: Option<u8>) -> (u8, u8) {
        let max = match pin {
            Some(pin) if pin < PROTOCOL_VERSION => pin,
            _ => PROTOCOL_VERSION,
        };
        let max = if max < MIN_PEER_VERSION {
            MIN_PEER_VERSION
        } else {
            max
        };
        (MIN_PEER_VERSION, max)
    }
}

/// A node accepting or refusing a peer hello, with its own record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHelloAck {
    /// The accepting node's own record
    pub hello: PeerHello,
    /// Whether it accepted, and if not why
    pub reason: PeerRefusal,
}

impl PeerHelloAck {
    /// Write this ack's body
    #[must_use]
    pub fn encode(&self) -> [u8; PEER_HELLO_BODY_LEN] {
        self.hello.encode_with(self.reason)
    }

    /// Read an ack out of its body
    ///
    /// # Arguments
    ///
    /// * `body` - The body to read
    pub fn decode(body: &[u8; PEER_HELLO_BODY_LEN]) -> Result<Self, ProtocolError> {
        Ok(PeerHelloAck {
            hello: PeerHello::decode_fields(body)?,
            reason: PeerRefusal::from_byte(body[REASON_AT]),
        })
    }

    /// Build the whole frame for this ack
    ///
    /// A refusal carries [`Flags::REFUSED`] so a peer reading only the header knows the answer.
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The largest frame the peer will accept
    pub fn frame(&self, max_frame_bytes: u32) -> Result<[u8; PEER_HELLO_FRAME_LEN], ProtocolError> {
        frame_of(MessageType::PeerHelloAck, self.encode(), max_frame_bytes)
    }
}

/// Put a header in front of a hello body
///
/// # Arguments
///
/// * `kind` - Which of the two frames this is
/// * `body` - The body to frame
/// * `max_frame_bytes` - The largest frame the peer will accept
fn frame_of(
    kind: MessageType,
    body: [u8; PEER_HELLO_BODY_LEN],
    max_frame_bytes: u32,
) -> Result<[u8; PEER_HELLO_FRAME_LEN], ProtocolError> {
    // a refused ack says so in its header too
    let flags = if kind == MessageType::PeerHelloAck
        && !PeerRefusal::from_byte(body[REASON_AT]).is_accepted()
    {
        Flags::REFUSED
    } else {
        Flags::NONE
    };
    // the hello is written at the floor, so a peer anywhere in the range reads it
    let header = Header::at(
        MIN_PEER_VERSION,
        kind,
        flags,
        PEER_HELLO_BODY_LEN,
        max_frame_bytes,
    )?;
    let mut frame = [0u8; PEER_HELLO_FRAME_LEN];
    frame[..HEADER_LEN].copy_from_slice(&header.encode());
    frame[HEADER_LEN..].copy_from_slice(&body);
    Ok(frame)
}
