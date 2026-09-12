//! The replication lane's frames, and the command every tablet group replicates
//!
//! ```text
//!  request  : [header][id u64][group u64][target shard u16][kind u8][reserved 1 B][deadline ms u32][body]
//!  response : [header][id u64][status u8][reserved 7 B][body]
//!  command  : [table u64][tablet u16][bundle 16 B][index u64][len u32][payload]
//! ```
//!
//! A tablet group's members exchange the consensus library's RPCs - an append, a vote - and a
//! proposal a non-leader forwards to the leader, each under a correlation id like a control
//! request, and additionally under the identity of the group and the shard on the receiving
//! node that hosts it, so the peer's listener can hand the frame to that shard without decoding
//! the body ([F40](../../../../../docs/src/features/replication.md)). The body is the library's
//! own request type, serialized by the engine; this module frames it and nothing more.
//!
//! [`Command`] is what the log replicates: a table, the tablet the write names, the request's
//! identity for deduplication, and the table's serialized intent, forwarded as bytes and never
//! re-serialized between the node that accepted the write and the shard that applies it
//! ([C5](../../../../../docs/src/distributed/replication.md), "the bytes are forwarded, not
//! re-serialized"). Its encoding is fixed and checked, so a peer's command is validated before
//! anything is sized by it; the payload inside is validated again by the table that applies it,
//! since it is an rkyv archive that crossed a process boundary.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::ProtocolError;
use super::{bytes16_at, u16_at, u32_at, u64_at};
use crate::shared::identity::TableId;

/// The size of a replication request head in bytes
pub const REPLICATE_HEAD_LEN: usize = 24;

/// The size of a replication response head in bytes
pub const REPLICATE_RESPONSE_HEAD_LEN: usize = 16;

/// The bytes ahead of a command's payload
pub const COMMAND_HEAD_LEN: usize = 38;

/// The most bytes a command's payload may carry
///
/// A row is bounded by the frame it arrives in, and a frame by the connection's bound, so this
/// is a ceiling against a corrupt length rather than a limit anybody reaches.
pub const MAX_COMMAND_PAYLOAD: usize = 256 * 1024 * 1024;

/// The seed a command's digest is hashed under, frozen like every other persisted hash
const DIGEST_SEED: i64 = 0;

/// What a replication request asks for
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ReplicateKind {
    /// The consensus library's append entries RPC
    AppendEntries = 1,
    /// The consensus library's vote RPC
    Vote = 2,
    /// A command a member proposes through the group's leader, answered with what applying it produced
    Propose = 3,
    /// A whole snapshot with its vote and metadata, which M7 delivers and this build refuses
    Snapshot = 4,
}

impl ReplicateKind {
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
            1 => Ok(ReplicateKind::AppendEntries),
            2 => Ok(ReplicateKind::Vote),
            3 => Ok(ReplicateKind::Propose),
            4 => Ok(ReplicateKind::Snapshot),
            unknown => Err(ProtocolError::UnknownReplicateKind(unknown)),
        }
    }

    /// Get the name of this kind
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            ReplicateKind::AppendEntries => "append_entries",
            ReplicateKind::Vote => "vote",
            ReplicateKind::Propose => "propose",
            ReplicateKind::Snapshot => "snapshot",
        }
    }
}

/// Whether a replication response carries an answer or a failure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ReplicateStatus {
    /// The payload is the answer
    Ok = 0,
    /// The payload is a message saying why there is no answer
    Error = 1,
}

impl ReplicateStatus {
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
            0 => ReplicateStatus::Ok,
            _ => ReplicateStatus::Error,
        }
    }
}

/// The fixed head of a replication request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateRequestHead {
    /// The id the response will carry back
    pub id: u64,
    /// The group the request is for
    pub group: u64,
    /// The shard on the receiving node that hosts the group
    pub target_shard: u16,
    /// What is asked
    pub kind: ReplicateKind,
    /// How many milliseconds the sender will still wait for the answer
    pub deadline_ms: u32,
}

impl ReplicateRequestHead {
    /// Write this head
    #[must_use]
    pub fn encode(&self) -> [u8; REPLICATE_HEAD_LEN] {
        let mut body = [0u8; REPLICATE_HEAD_LEN];
        body[..8].copy_from_slice(&self.id.to_le_bytes());
        body[8..16].copy_from_slice(&self.group.to_le_bytes());
        body[16..18].copy_from_slice(&self.target_shard.to_le_bytes());
        body[18] = self.kind.as_byte();
        // one reserved byte stays zero
        body[20..24].copy_from_slice(&self.deadline_ms.to_le_bytes());
        body
    }

    /// Read a head
    ///
    /// # Arguments
    ///
    /// * `raw` - The head bytes
    pub fn decode(raw: &[u8; REPLICATE_HEAD_LEN]) -> Result<Self, ProtocolError> {
        Ok(ReplicateRequestHead {
            id: u64_at(raw, 0),
            group: u64_at(raw, 8),
            target_shard: u16_at(raw, 16),
            kind: ReplicateKind::from_byte(raw[18])?,
            deadline_ms: u32_at(raw, 20),
        })
    }
}

/// The fixed head of a replication response
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateResponseHead {
    /// The id of the request this answers
    pub id: u64,
    /// Whether the payload is an answer or a failure
    pub status: ReplicateStatus,
}

impl ReplicateResponseHead {
    /// Write this head
    #[must_use]
    pub fn encode(&self) -> [u8; REPLICATE_RESPONSE_HEAD_LEN] {
        let mut body = [0u8; REPLICATE_RESPONSE_HEAD_LEN];
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
    pub fn decode(raw: &[u8; REPLICATE_RESPONSE_HEAD_LEN]) -> Self {
        ReplicateResponseHead {
            id: u64_at(raw, 0),
            status: ReplicateStatus::from_byte(raw[8]),
        }
    }
}

/// The identity of one write within one bundle, which is what a retry repeats
///
/// The bundle's id and the query's index in it. A group remembers the result it produced for an
/// identity and answers a repeat with it rather than applying the command again
/// ([C5](../../../../../docs/src/distributed/replication.md), "retry identity").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RequestId {
    /// The bundle the write arrived in, as the uuid's bytes
    pub bundle: [u8; 16],
    /// The write's index in that bundle
    pub index: u64,
}

/// What a tablet group replicates: one write, as the table serialized it
///
/// The payload is the table's own intent, archived once by the node that accepted the write
/// and carried as bytes from there to every replica's log and state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// The table the write names
    pub table: TableId,
    /// The tablet the write's partition belongs to
    pub tablet: u16,
    /// Which write this is, for deduplication
    pub request: RequestId,
    /// The table's serialized intent
    pub payload: Vec<u8>,
}

impl Command {
    /// Write this command as bytes
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COMMAND_HEAD_LEN + self.payload.len());
        out.extend_from_slice(&self.table.0.to_le_bytes());
        out.extend_from_slice(&self.tablet.to_le_bytes());
        out.extend_from_slice(&self.request.bundle);
        out.extend_from_slice(&self.request.index.to_le_bytes());
        // truncation cannot happen: the payload is bounded by a frame, which a u32 holds
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Read a command back out of its bytes, checking every length first
    ///
    /// # Arguments
    ///
    /// * `raw` - The bytes `encode` wrote
    pub fn decode(raw: &[u8]) -> Result<Self, ProtocolError> {
        // the head has to be there whole before any of it is read
        if raw.len() < COMMAND_HEAD_LEN {
            return Err(ProtocolError::MalformedCommand("a command is shorter than its head"));
        }
        let len = u32_at(raw, 34) as usize;
        // the payload's length has to fit the ceiling and the bytes that actually follow
        if len > MAX_COMMAND_PAYLOAD {
            return Err(ProtocolError::MalformedCommand("a command's payload passes the ceiling"));
        }
        if raw.len() != COMMAND_HEAD_LEN + len {
            return Err(ProtocolError::MalformedCommand(
                "a command's payload length does not match its bytes",
            ));
        }
        Ok(Command {
            table: TableId(u64_at(raw, 0)),
            tablet: u16_at(raw, 8),
            request: RequestId {
                bundle: bytes16_at(raw, 10),
                index: u64_at(raw, 26),
            },
            payload: raw[COMMAND_HEAD_LEN..].to_vec(),
        })
    }

    /// The hash of the payload, which is what a retry with a different payload is refused by
    #[must_use]
    pub fn digest(&self) -> u64 {
        gxhash::gxhash64(&self.payload, DIGEST_SEED)
    }

    /// How many bytes this command takes encoded
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        COMMAND_HEAD_LEN + self.payload.len()
    }
}

impl std::fmt::Display for Command {
    /// Name the table, the tablet and the request, and how big the payload is
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}/{} ({} B)",
            self.table,
            self.tablet,
            uuid::Uuid::from_bytes(self.request.bundle),
            self.request.index,
            self.payload.len()
        )
    }
}

impl Serialize for Command {
    /// Serialize as the fixed encoding, as one byte string
    ///
    /// One byte string rather than a struct, so that a binary format carries the payload as it is
    /// and a text one carries it once, and so that the wire form and the log form are one encoding.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.encode())
    }
}

/// Reads a command's bytes back for serde
struct CommandVisitor;

impl<'de> Visitor<'de> for CommandVisitor {
    type Value = Command;

    /// Say what is expected, for an error message
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("the bytes of a replicated command")
    }

    /// Decode a borrowed byte string
    fn visit_bytes<E: de::Error>(self, raw: &[u8]) -> Result<Command, E> {
        Command::decode(raw).map_err(|error| E::custom(error.to_string()))
    }

    /// Decode an owned byte string
    fn visit_byte_buf<E: de::Error>(self, raw: Vec<u8>) -> Result<Command, E> {
        self.visit_bytes(&raw)
    }

    /// Decode a sequence of bytes, which is how a text format carries a byte string
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Command, A::Error> {
        let mut raw = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(byte) = seq.next_element::<u8>()? {
            raw.push(byte);
        }
        self.visit_bytes(&raw)
    }
}

impl<'de> Deserialize<'de> for Command {
    /// Deserialize from the byte string `Serialize` wrote
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_byte_buf(CommandVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A command of some payload, under a fixed identity
    fn a_command(payload: &[u8]) -> Command {
        Command {
            table: TableId::of("Row"),
            tablet: 1234,
            request: RequestId {
                bundle: [7u8; 16],
                index: 42,
            },
            payload: payload.to_vec(),
        }
    }

    /// A command round trips through its bytes, through serde, and refuses a length that lies
    #[test]
    fn a_command_round_trips_and_checks_its_lengths() {
        let command = a_command(&[1, 2, 3, 200, 255]);
        let raw = command.encode();
        assert_eq!(raw.len(), COMMAND_HEAD_LEN + 5);
        assert_eq!(Command::decode(&raw).unwrap(), command);
        // an empty payload is a legal command
        assert_eq!(Command::decode(&a_command(&[]).encode()).unwrap(), a_command(&[]));
        // the digest follows the payload alone
        assert_eq!(command.digest(), a_command(&[1, 2, 3, 200, 255]).digest());
        assert_ne!(command.digest(), a_command(&[1, 2, 3]).digest());
        // a head too short, a length past its bytes and bytes past the length are all refused
        assert!(Command::decode(&raw[..COMMAND_HEAD_LEN - 1]).is_err());
        let mut lying = raw.clone();
        lying[34] = 9;
        assert!(Command::decode(&lying).is_err());
        let mut long = raw.clone();
        long.push(0);
        assert!(Command::decode(&long).is_err());
        // through json the bytes are a sequence, and come back the same command
        let json = serde_json::to_string(&command).unwrap();
        let back: Command = serde_json::from_str(&json).unwrap();
        assert_eq!(back, command);
    }

    /// Both heads round trip, and an unknown kind is refused
    #[test]
    fn replication_heads_round_trip() {
        let head = ReplicateRequestHead {
            id: 9,
            group: 0xdead_beef,
            target_shard: 3,
            kind: ReplicateKind::Propose,
            deadline_ms: 5000,
        };
        assert_eq!(ReplicateRequestHead::decode(&head.encode()).unwrap(), head);
        let mut bad = head.encode();
        bad[18] = 0;
        assert_eq!(
            ReplicateRequestHead::decode(&bad),
            Err(ProtocolError::UnknownReplicateKind(0))
        );
        for kind in [
            ReplicateKind::AppendEntries,
            ReplicateKind::Vote,
            ReplicateKind::Propose,
            ReplicateKind::Snapshot,
        ] {
            assert_eq!(ReplicateKind::from_byte(kind.as_byte()).unwrap(), kind);
        }
        let response = ReplicateResponseHead {
            id: 9,
            status: ReplicateStatus::Error,
        };
        assert_eq!(ReplicateResponseHead::decode(&response.encode()), response);
        assert_eq!(ReplicateStatus::from_byte(7), ReplicateStatus::Error);
    }
}
