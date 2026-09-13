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
//! At M2 all three are required to match exactly - the contract is defined here, and the codecs
//! for an older version are what M10 owes before n−1 is accepted.

use super::super::{Flags, Header, MessageType, ProtocolError, HEADER_LEN, PROTOCOL_VERSION};
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

/// Everything this build can act on
pub const CAPABILITIES: u64 = CAP_FORWARD_V1
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

    /// Whether this peer reads the wire version this build speaks
    ///
    /// Exact at M2: the peer's range has to contain [`PROTOCOL_VERSION`], and every frame after
    /// the hello is written at that version. A wider intersection is what M10's codecs are for.
    #[must_use]
    pub const fn speaks_our_version(&self) -> bool {
        self.wire_min <= PROTOCOL_VERSION && PROTOCOL_VERSION <= self.wire_max
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
    let header = Header::new(kind, flags, PEER_HELLO_BODY_LEN, max_frame_bytes)?;
    let mut frame = [0u8; PEER_HELLO_FRAME_LEN];
    frame[..HEADER_LEN].copy_from_slice(&header.encode());
    frame[HEADER_LEN..].copy_from_slice(&body);
    Ok(frame)
}
