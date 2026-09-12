//! The control lane's request and response frames
//!
//! ```text
//!  request  : [header][id u64][kind u8][reserved 3 B][deadline ms u32][json]
//!  response : [header][id u64][status u8][reserved 7 B][json]
//! ```
//!
//! The control group's RPCs - an append, a vote, a snapshot - and its pings ride here, and
//! since [F39](../../../../../docs/src/features/membership.md) so do a joiner's admission, a
//! member's status report and a proposal forwarded to the leader. The payload is JSON because
//! the control store is JSON already: these are a few hundred bytes a heartbeat, read by people
//! as often as by the server, and the types are openraft's, which derive serde. The correlation
//! id is what lets one connection carry many requests in flight and answer them in any order,
//! and the deadline is what the sender will still wait, so a receiver that cannot answer in
//! time need not try.
//!
//! `MessageType::StatusReport` (19) stays reserved: a report is one JSON body under a
//! correlation id like every other control RPC, and a second framing for it would be a second
//! reader to keep equivalent to this one for nothing.

use serde::{Deserialize, Serialize};

use super::super::ProtocolError;
use super::{u32_at, u64_at};
use crate::shared::identity::NodeId;

/// The size of both heads in bytes
pub const CONTROL_HEAD_LEN: usize = 16;

/// What a control request asks for
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ControlKind {
    /// openraft's `AppendEntriesRequest`
    AppendEntries = 1,
    /// openraft's `VoteRequest`
    Vote = 2,
    /// A whole snapshot with its vote and metadata
    Snapshot = 3,
    /// A liveness probe carrying the sender's incarnation and topology version
    Ping = 4,
    /// A joiner asking the leader to admit it, answered with the cluster or a redirect
    Join = 5,
    /// A member's bounded status report to the leader: freshness, incarnation, shard health
    StatusReport = 6,
    /// A command a member proposes through the leader, answered with what applying it produced
    Propose = 7,
}

impl ControlKind {
    /// Get the byte this kind is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a kind from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse
    #[inline]
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            1 => Ok(ControlKind::AppendEntries),
            2 => Ok(ControlKind::Vote),
            3 => Ok(ControlKind::Snapshot),
            4 => Ok(ControlKind::Ping),
            5 => Ok(ControlKind::Join),
            6 => Ok(ControlKind::StatusReport),
            7 => Ok(ControlKind::Propose),
            unknown => Err(ProtocolError::UnknownControlKind(unknown)),
        }
    }

    /// Get the name of this kind
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            ControlKind::AppendEntries => "append_entries",
            ControlKind::Vote => "vote",
            ControlKind::Snapshot => "snapshot",
            ControlKind::Ping => "ping",
            ControlKind::Join => "join",
            ControlKind::StatusReport => "status_report",
            ControlKind::Propose => "propose",
        }
    }
}

/// Whether a control response carries an answer or a failure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ControlStatus {
    /// The payload is the answer
    Ok = 0,
    /// The payload is a message saying why there is no answer
    Error = 1,
}

impl ControlStatus {
    /// Get the byte this status is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a status from the byte it was written as, reading anything unknown as a failure
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse
    #[inline]
    pub const fn from_byte(raw: u8) -> Self {
        match raw {
            0 => ControlStatus::Ok,
            _ => ControlStatus::Error,
        }
    }
}

/// The fixed head of a control request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlRequestHead {
    /// The id the response will carry back
    pub id: u64,
    /// What is asked
    pub kind: ControlKind,
    /// How many milliseconds the sender will still wait for the answer
    pub deadline_ms: u32,
}

impl ControlRequestHead {
    /// Write this head
    #[must_use]
    pub fn encode(&self) -> [u8; CONTROL_HEAD_LEN] {
        let mut body = [0u8; CONTROL_HEAD_LEN];
        body[..8].copy_from_slice(&self.id.to_le_bytes());
        body[8] = self.kind.as_byte();
        body[12..16].copy_from_slice(&self.deadline_ms.to_le_bytes());
        body
    }

    /// Read a head
    ///
    /// # Arguments
    ///
    /// * `raw` - The head bytes
    pub fn decode(raw: &[u8; CONTROL_HEAD_LEN]) -> Result<Self, ProtocolError> {
        Ok(ControlRequestHead {
            id: u64_at(raw, 0),
            kind: ControlKind::from_byte(raw[8])?,
            deadline_ms: u32_at(raw, 12),
        })
    }
}

/// The fixed head of a control response
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlResponseHead {
    /// The id of the request this answers
    pub id: u64,
    /// Whether the payload is an answer or a failure
    pub status: ControlStatus,
}

impl ControlResponseHead {
    /// Write this head
    #[must_use]
    pub fn encode(&self) -> [u8; CONTROL_HEAD_LEN] {
        let mut body = [0u8; CONTROL_HEAD_LEN];
        body[..8].copy_from_slice(&self.id.to_le_bytes());
        body[8] = self.status.as_byte();
        body
    }

    /// Read a head
    ///
    /// # Arguments
    ///
    /// * `raw` - The head bytes
    #[must_use]
    pub fn decode(raw: &[u8; CONTROL_HEAD_LEN]) -> Self {
        ControlResponseHead {
            id: u64_at(raw, 0),
            status: ControlStatus::from_byte(raw[8]),
        }
    }
}

/// What a member tells the control leader about itself, on every detector tick
///
/// Freshness is `(incarnation, seq)`: a report at a lower incarnation than the cluster has
/// committed for the node, or at a sequence the leader has already seen, is ignored rather than
/// acted on ([C3](../../../../../docs/src/distributed/membership.md)). The shard health is what a
/// healthy control lane must never mask: a node whose control thread answers every ping can
/// still have a dead shard, and this is where it says so.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusReport {
    /// The reporting node
    pub node: NodeId,
    /// Which start of that node this is
    pub incarnation: u64,
    /// The report's sequence within this start, strictly increasing
    pub seq: u64,
    /// The topology version the node has applied
    pub topology_version: u64,
    /// The control log index the node has applied
    pub applied_index: u64,
    /// The shards that have failed on the node, by index
    pub shards_failed: Vec<u16>,
    /// The peers this node can currently reach over its control lane, with the round trip in
    /// microseconds, as a local observation and nothing more
    pub reachability: Vec<(NodeId, u32)>,
}
