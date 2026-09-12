//! The control lane's request and response frames
//!
//! ```text
//!  request  : [header][id u64][kind u8][reserved 3 B][deadline ms u32][json]
//!  response : [header][id u64][status u8][reserved 7 B][json]
//! ```
//!
//! The control group's RPCs - an append, a vote, a snapshot - and its pings ride here. The
//! payload is JSON because the control store is JSON already: these are a few hundred bytes a
//! heartbeat, read by people as often as by the server, and the types are openraft's, which
//! derive serde. The correlation id is what lets one connection carry many requests in flight
//! and answer them in any order, and the deadline is what the sender will still wait, so a
//! receiver that cannot answer in time need not try.

use super::super::ProtocolError;
use super::{u32_at, u64_at};

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
