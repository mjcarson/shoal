//! The frames two nodes of one cluster exchange
//!
//! This is the peer protocol ([F38](../../../../../docs/src/features/inter-node-transport.md)),
//! spoken only between nodes that have proved to each other that they belong to the same cluster.
//! It reuses the eight byte header every client frame starts with, and adds ten message types
//! above the twelve a client knows. Every body here is fixed bytes, for the same reason the
//! client handshake's are: the hello is what detects a peer built from a different schema, and a
//! detector that used the thing it detects to decode itself would report "corrupt archive"
//! instead of "different schema".
//!
//! # Invariants
//!
//! **Nothing ahead of an rkyv payload shares its buffer.** A `Forward` frame carries a bundle of
//! queries, and that bundle is accessed in place as an archive. Its fixed fields and its entries
//! are read into buffers of their own, and the bundle into a fresh allocation at offset zero,
//! exactly as a request frame's trace context is kept out of the front of the request body.
//! The decoders here therefore hand back *how many bytes follow* and never a slice into one
//! buffer that another decoder should read.
//!
//! **Every length is judged before anything is sized by it.** The header's length bounds the
//! frame; the preamble's entry byte count is bounded by [`MAX_FORWARD_ENTRIES_BYTES`] and its
//! entry count by [`MAX_FORWARD_ENTRIES`]; an entry's key count by [`MAX_FORWARD_KEYS`]. A peer
//! that has passed the hello is still a process on another machine, and a length is the one
//! field a decoder acts on before it has checked anything else.
//!
//! **The dialler speaks first.** It writes a [`PeerHello`] and then reads; the accepting node
//! reads and then writes a [`PeerHelloAck`]. A refusal is still an ack, written before the
//! close, so a misconfigured node sees why in its log rather than a reset.
//!
//! **This module names no runtime, no socket and no shoal type but the trace context.** It is
//! pure encode and decode over byte arrays, so that the server's glommio relays and a test's
//! tokio socket read it the same way.

pub mod control;
pub mod forward;
pub mod hello;
pub mod replicate;
pub mod snapshot;

#[cfg(test)]
mod tests;

pub use control::{
    ControlKind, ControlRequestHead, ControlResponseHead, ControlStatus, StatusReport,
    CONTROL_HEAD_LEN,
};
pub use forward::{
    decode_entries, decode_error_payload, encode_entries, encode_error_payload, EntryRead,
    ForwardEntry, ForwardPreamble, ForwardedKind, ForwardedPreamble, FORWARDED_PREAMBLE_LEN,
    FORWARD_PREAMBLE_LEN, MAX_FORWARD_ENTRIES, MAX_FORWARD_ENTRIES_BYTES, MAX_FORWARD_KEYS,
};
pub use hello::{
    PeerHello, PeerHelloAck, CAPABILITIES, CAP_BULK_SNAPSHOT_V1, CAP_CONTROL_RAFT_V1,
    CAP_FORWARD_V1, CAP_MEMBERSHIP_V1, CAP_PRE_VOTE_V1, CAP_READ_CONSISTENCY_V1,
    CAP_REPLICATION_V1, PEER_HELLO_BODY_LEN, PEER_HELLO_FRAME_LEN, REQUIRED_CAPABILITIES,
};
pub use replicate::{
    Command, ReplicateKind, ReplicateRequestHead, ReplicateResponseHead, ReplicateStatus,
    RequestId, COMMAND_HEAD_LEN, REPLICATE_HEAD_LEN, REPLICATE_RESPONSE_HEAD_LEN,
};
pub use snapshot::{
    checksum, SnapshotBegin, SnapshotChunk, SnapshotEnd, SnapshotStatus, SNAPSHOT_BEGIN_LEN,
    SNAPSHOT_CHUNK_LEN, SNAPSHOT_END_LEN,
};

use super::ProtocolError;

/// Which of the four lanes a peer connection carries
///
/// Each lane is a socket of its own, so that bytes queued on one can never sit ahead of bytes on
/// another: a snapshot on the bulk lane cannot delay a vote on the control lane, and a stalled
/// data peer cannot hold a heartbeat. The lane is named in the hello, and a listener refuses a
/// lane it does not serve - the control listener takes only control, the shard listeners take
/// data, bulk and replication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Lane {
    /// Forwarded queries and their answers, owned by a shard
    Data = 1,
    /// Consensus RPCs and pings, owned by the control thread
    Control = 2,
    /// Snapshot streams, owned by a shard, separate so they cannot block the other two
    Bulk = 3,
    /// A tablet group's consensus RPCs and proposals, owned by the shard that hosts the group
    ///
    /// Served on the data port beside the data lane, a socket of its own so that a forwarded
    /// bundle queued ahead of an append cannot delay a heartbeat
    /// ([F40](../../../../../docs/src/features/replication.md)).
    Replication = 4,
}

impl Lane {
    /// Get the byte this lane is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a lane from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a lane from
    #[inline]
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            1 => Ok(Lane::Data),
            2 => Ok(Lane::Control),
            3 => Ok(Lane::Bulk),
            4 => Ok(Lane::Replication),
            // anything else, zero included, is a lane this build does not serve
            unknown => Err(ProtocolError::UnknownLane(unknown)),
        }
    }

    /// Get the name of this lane
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            Lane::Data => "data",
            Lane::Control => "control",
            Lane::Bulk => "bulk",
            Lane::Replication => "replication",
        }
    }
}

impl std::fmt::Display for Lane {
    /// Write this lane's name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Why a node refused a peer connection, or that it did not
///
/// Every refusal names the check that failed, in the order the accepting node runs them: the
/// wire version first, because nothing after it can be read otherwise; then the lane; then the
/// cluster, the node and its shard count, which is what the static placement says about it; then
/// the schema. A refusal this build does not recognize reads as a refusal
/// ([`PeerRefusal::Unrecognized`]), never as an acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PeerRefusal {
    /// The connection was accepted
    Accepted = 0,
    /// The peer belongs to a different cluster
    WrongCluster = 1,
    /// The peer's node id is not a member this node knows
    UnknownNode = 2,
    /// The peer's node id is known, but not at the address it connected from, it answered a
    /// dial with an identity other than the one dialled, or its certificate names another node
    IdentityMismatch = 3,
    /// The two builds share no wire version
    NoCommonVersion = 4,
    /// The peer was built from a different schema
    SchemaMismatch = 5,
    /// The peer runs a different number of shards than the placement says it does
    ShardCountMismatch = 6,
    /// The peer asked for a lane this listener does not serve
    LaneRefused = 7,
    /// The peer's transport credentials did not prove membership
    Unauthorized = 8,
    /// The peer's incarnation is below the one the cluster has committed for its identity
    ///
    /// A later start of the same directory has been admitted, so this one is a run the cluster
    /// has already replaced ([F39](../../../../../docs/src/features/membership.md), Q11).
    Fenced = 10,
    /// A joiner presented an identity the cluster already holds at the same or a higher incarnation
    DuplicateIdentity = 11,
    /// A join was asked of a node that cannot admit one - not a member, or not on the control lane
    NotJoinable = 12,
    /// The peer's newest wire version is below the one the cluster has activated
    ///
    /// Once an operator activates a version, a member that cannot speak it is refused at every
    /// door, which is what makes the activation the rollback boundary
    /// ([F48](../../../../../docs/src/features/rolling-compatibility.md)). A build from before
    /// the byte reads it as `Unrecognized`, which is still a refusal.
    BelowActivatedWire = 13,
    /// The peer's identity was removed from the cluster and can never return
    ///
    /// A tombstoned member, at any incarnation, and a member of a cluster this one was restored
    /// from ([F49](../../../../../docs/src/features/backup-and-recovery.md)).
    Removed = 14,
    /// The peer lacks a capability every member has to act on
    ///
    /// The capability words are intersected at the hello and what both act on is what either
    /// may send; a bit in [`hello::REQUIRED_CAPABILITIES`] is one no version in the range is
    /// without, so a peer missing it is refused rather than half served
    /// ([F48](../../../../../docs/src/features/rolling-compatibility.md)).
    CapabilityMissing = 15,
    /// A reason this build does not know, which is still a refusal
    Unrecognized = 255,
}

impl PeerRefusal {
    /// Get the byte this reason is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a refusal from the byte it was written as, failing closed
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a refusal from
    #[inline]
    pub const fn from_byte(raw: u8) -> Self {
        match raw {
            0 => PeerRefusal::Accepted,
            1 => PeerRefusal::WrongCluster,
            2 => PeerRefusal::UnknownNode,
            3 => PeerRefusal::IdentityMismatch,
            4 => PeerRefusal::NoCommonVersion,
            5 => PeerRefusal::SchemaMismatch,
            6 => PeerRefusal::ShardCountMismatch,
            7 => PeerRefusal::LaneRefused,
            8 => PeerRefusal::Unauthorized,
            10 => PeerRefusal::Fenced,
            11 => PeerRefusal::DuplicateIdentity,
            12 => PeerRefusal::NotJoinable,
            13 => PeerRefusal::BelowActivatedWire,
            14 => PeerRefusal::Removed,
            15 => PeerRefusal::CapabilityMissing,
            // a reason we cannot name is still a node that would not have us
            _ => PeerRefusal::Unrecognized,
        }
    }

    /// Check whether this reason means the connection was accepted
    #[inline]
    pub const fn is_accepted(self) -> bool {
        matches!(self, PeerRefusal::Accepted)
    }
}

impl std::fmt::Display for PeerRefusal {
    /// Write a legible description of this refusal
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerRefusal::Accepted => write!(f, "accepted"),
            PeerRefusal::WrongCluster => write!(f, "wrong cluster"),
            PeerRefusal::UnknownNode => write!(f, "node not a known member"),
            PeerRefusal::IdentityMismatch => write!(f, "identity does not match the membership"),
            PeerRefusal::NoCommonVersion => write!(f, "no wire version in common"),
            PeerRefusal::SchemaMismatch => write!(f, "schema mismatch"),
            PeerRefusal::ShardCountMismatch => {
                write!(f, "shard count does not match the membership")
            }
            PeerRefusal::LaneRefused => write!(f, "lane not served on this listener"),
            PeerRefusal::Unauthorized => write!(f, "not authorized"),
            PeerRefusal::Fenced => write!(f, "fenced by a later start of the same node"),
            PeerRefusal::DuplicateIdentity => {
                write!(
                    f,
                    "the cluster already holds this identity at this incarnation or later"
                )
            }
            PeerRefusal::BelowActivatedWire => {
                write!(f, "the newest wire version this build speaks is below the one the cluster activated")
            }
            PeerRefusal::Removed => write!(
                f,
                "this identity was removed from the cluster and cannot return"
            ),
            PeerRefusal::CapabilityMissing => {
                write!(f, "a capability every member acts on is missing")
            }
            PeerRefusal::NotJoinable => write!(f, "this node cannot admit a joiner here"),
            PeerRefusal::Unrecognized => write!(f, "refused for a reason this build does not know"),
        }
    }
}

/// Read a little endian `u16` at an offset of a body that is known to be long enough
#[inline]
pub(crate) const fn u16_at(raw: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([raw[at], raw[at + 1]])
}

/// Read a little endian `u32` at an offset of a body that is known to be long enough
#[inline]
pub(crate) const fn u32_at(raw: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]])
}

/// Read a little endian `u64` at an offset of a body that is known to be long enough
#[inline]
pub(crate) const fn u64_at(raw: &[u8], at: usize) -> u64 {
    u64::from_le_bytes([
        raw[at],
        raw[at + 1],
        raw[at + 2],
        raw[at + 3],
        raw[at + 4],
        raw[at + 5],
        raw[at + 6],
        raw[at + 7],
    ])
}

/// Read sixteen bytes at an offset of a body that is known to be long enough
#[inline]
pub(crate) fn bytes16_at(raw: &[u8], at: usize) -> [u8; 16] {
    let mut out = [0u8; 16];
    out.copy_from_slice(&raw[at..at + 16]);
    out
}
