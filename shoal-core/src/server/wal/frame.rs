//! The format 2 frame: one record of a shard's shared WAL
//!
//! ```text
//!  [len u32][gxhash32 u32] [kind u8][version u8][flag u8][reserved u8][group u64][index u64][term u64][leader ShardAddr 18 B] [body]
//!   <- 8 B, unhashed ->   <----------------------------- 46 B, hashed with the body ----------------------------->
//! ```
//!
//! Every logical stream a shard hosts - one per tablet group - is multiplexed into one physical
//! file, and the frame is what keeps them apart ([C5](../../../../docs/src/distributed/replication.md),
//! "the intent record, format 2"; [Q2](../../../../docs/src/distributed/protocol.md)): every
//! frame names its group, and a group's history is the subsequence of the file that names it.
//! The header carries the entry's log id whole so that an index over the file can be rebuilt
//! from headers alone, and the vote, committed, purged and truncate records reuse the same
//! fields rather than defining bodies of their own.
//!
//! The checksum covers the header after the first eight bytes and the whole body, so a frame
//! whose length runs past the end of the file or whose hash does not match is the tail of a
//! write that never finished, and reading stops there. Format 1 is the per-table intent log
//! standalone nodes still write ([`IntentLogReader`](crate::server::tables::storage::fs::reader::IntentLogReader)),
//! and the two are told apart by directory and never by sniffing.

use std::io;

use openraft::entry::RaftEntry as _;
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::vote::{RaftLeaderId as _, RaftVote as _};
use openraft::{EntryPayload, LogId, Membership};

use crate::server::replication::DataConfig;
use crate::shared::identity::{GroupId, ShardAddr};
use crate::shared::protocol::peer::Command;

/// The bytes ahead of the hashed part of a frame: the length and the checksum
pub const FRAME_PREFIX: usize = 8;

/// The hashed header: kind, version, flag, reserved, group, index, term and leader
pub const FRAME_HEADER: usize = 46;

/// A whole frame's fixed part
pub const FRAME_FIXED: usize = FRAME_PREFIX + FRAME_HEADER;

/// The format every frame is written in
pub const FRAME_VERSION: u8 = 2;

/// The seed every frame checksum is taken with, frozen like the control log's
const FRAME_SEED: i64 = 0;

/// An entry of a tablet group's log
pub type Entry = EntryOf<DataConfig>;

/// A log id of a tablet group's log
pub type WalLogId = LogIdOf<DataConfig>;

/// A vote in a tablet group
pub type Vote = VoteOf<DataConfig>;

/// The leader id a log id or a vote carries
pub type LeaderId = <DataConfig as openraft::RaftTypeConfig>::LeaderId;

/// What a frame records
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// A blank entry, which a new leader commits to establish its term
    Blank = 1,
    /// A command, whose body is the command's bytes
    Normal = 2,
    /// A membership entry, whose body is the membership in `postcard`
    ///
    /// Not JSON, though the control log's is: a membership's nodes are keyed by shard address,
    /// and a JSON map cannot be keyed by a struct.
    Membership = 3,
    /// The vote granted, whose term and leader are in the header and whose flag is `committed`
    Vote = 4,
    /// The committed log id, in the header; a flag of zero means none
    Committed = 5,
    /// The purged log id, in the header; a flag of zero means none
    Purged = 6,
    /// Everything after the header's log id is dropped; a flag of zero means everything is
    Truncate = 7,
}

impl FrameKind {
    /// Parse a kind from its byte
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte
    fn from_byte(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(FrameKind::Blank),
            2 => Some(FrameKind::Normal),
            3 => Some(FrameKind::Membership),
            4 => Some(FrameKind::Vote),
            5 => Some(FrameKind::Committed),
            6 => Some(FrameKind::Purged),
            7 => Some(FrameKind::Truncate),
            _ => None,
        }
    }
}

/// One decoded frame
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// An entry of the group's log
    Entry {
        /// The group
        group: GroupId,
        /// The entry
        entry: Entry,
    },
    /// The vote the group granted
    Vote {
        /// The group
        group: GroupId,
        /// The vote
        vote: Vote,
    },
    /// The committed log id the group recorded
    Committed {
        /// The group
        group: GroupId,
        /// The log id, or none
        log_id: Option<WalLogId>,
    },
    /// The purged log id the group recorded
    Purged {
        /// The group
        group: GroupId,
        /// The log id
        log_id: WalLogId,
    },
    /// Everything after a log id was dropped
    Truncate {
        /// The group
        group: GroupId,
        /// The last log id kept, or none for everything dropped
        keep_after: Option<WalLogId>,
    },
}

impl Frame {
    /// The group this frame belongs to
    #[must_use]
    pub fn group(&self) -> GroupId {
        match self {
            Frame::Entry { group, .. }
            | Frame::Vote { group, .. }
            | Frame::Committed { group, .. }
            | Frame::Purged { group, .. }
            | Frame::Truncate { group, .. } => *group,
        }
    }
}

/// Write the hashed header into a frame buffer
///
/// # Arguments
///
/// * `out` - The buffer, positioned after the prefix
/// * `kind` - What the frame records
/// * `flag` - The kind's flag byte
/// * `group` - The group
/// * `log_id` - The log id the header carries, or the default for a frame that carries none
fn put_header(out: &mut Vec<u8>, kind: FrameKind, flag: u8, group: GroupId, log_id: Option<&WalLogId>) {
    out.push(kind as u8);
    out.push(FRAME_VERSION);
    out.push(flag);
    out.push(0);
    out.extend_from_slice(&group.0.to_le_bytes());
    // the log id's parts, or zeros for a frame that carries none
    let (index, term, leader) = match log_id {
        Some(log_id) => (
            log_id.index,
            log_id.leader_id.term(),
            *log_id.leader_id.node_id(),
        ),
        None => (0, 0, ShardAddr::default()),
    };
    out.extend_from_slice(&index.to_le_bytes());
    out.extend_from_slice(&term.to_le_bytes());
    out.extend_from_slice(&leader.to_bytes());
}

/// Seal a frame: prefix its length and checksum
///
/// # Arguments
///
/// * `hashed` - The header and body, which the checksum covers
fn seal(hashed: Vec<u8>) -> io::Result<Vec<u8>> {
    let len = u32::try_from(hashed.len())
        .map_err(|_| io::Error::other("a wal frame does not fit in a u32"))?;
    let mut frame = Vec::with_capacity(FRAME_PREFIX + hashed.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&gxhash::gxhash32(&hashed, FRAME_SEED).to_le_bytes());
    frame.extend_from_slice(&hashed);
    Ok(frame)
}

/// Encode an entry as a frame
///
/// # Arguments
///
/// * `group` - The group the entry belongs to
/// * `entry` - The entry
pub fn encode_entry(group: GroupId, entry: &Entry) -> io::Result<Vec<u8>> {
    let mut hashed = Vec::with_capacity(FRAME_HEADER + 64);
    match &entry.payload {
        EntryPayload::Blank => put_header(&mut hashed, FrameKind::Blank, 0, group, Some(&entry.log_id)),
        EntryPayload::Normal(command) => {
            put_header(&mut hashed, FrameKind::Normal, 0, group, Some(&entry.log_id));
            hashed.extend_from_slice(&command.encode());
        }
        EntryPayload::Membership(membership) => {
            put_header(&mut hashed, FrameKind::Membership, 0, group, Some(&entry.log_id));
            hashed.extend_from_slice(
                &postcard::to_allocvec(membership)
                    .map_err(|error| io::Error::other(format!("encoding a membership: {error}")))?,
            );
        }
    }
    seal(hashed)
}

/// Encode a vote as a frame
///
/// # Arguments
///
/// * `group` - The group
/// * `vote` - The vote
pub fn encode_vote(group: GroupId, vote: &Vote) -> io::Result<Vec<u8>> {
    let mut hashed = Vec::with_capacity(FRAME_HEADER);
    // the vote's term and leader ride in the log id fields, at index zero
    let log_id = LogId::new(vote.leader_id().clone(), 0);
    put_header(&mut hashed, FrameKind::Vote, u8::from(vote.is_committed()), group, Some(&log_id));
    seal(hashed)
}

/// Encode a committed, purged or truncate marker as a frame
///
/// # Arguments
///
/// * `kind` - Which of the three
/// * `group` - The group
/// * `log_id` - The log id, or none
pub fn encode_marker(kind: FrameKind, group: GroupId, log_id: Option<&WalLogId>) -> io::Result<Vec<u8>> {
    let mut hashed = Vec::with_capacity(FRAME_HEADER);
    put_header(&mut hashed, kind, u8::from(log_id.is_some()), group, log_id);
    seal(hashed)
}

/// Read a little endian `u64` at an offset of a buffer known to be long enough
fn u64_at(raw: &[u8], at: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&raw[at..at + 8]);
    u64::from_le_bytes(bytes)
}

/// Decode one frame's hashed part
///
/// # Arguments
///
/// * `hashed` - The header and body, checksum already verified
fn decode_hashed(hashed: &[u8]) -> Option<Frame> {
    if hashed.len() < FRAME_HEADER {
        return None;
    }
    let kind = FrameKind::from_byte(hashed[0])?;
    if hashed[1] != FRAME_VERSION {
        return None;
    }
    let flag = hashed[2];
    let group = GroupId(u64_at(hashed, 4));
    let index = u64_at(hashed, 12);
    let term = u64_at(hashed, 20);
    let mut leader = [0u8; 18];
    leader.copy_from_slice(&hashed[28..46]);
    let leader = ShardAddr::from_bytes(&leader);
    let log_id = LogId::new(LeaderId::new(term, leader), index);
    let body = &hashed[FRAME_HEADER..];
    let frame = match kind {
        FrameKind::Blank => Frame::Entry {
            group,
            entry: Entry::new_blank(log_id),
        },
        FrameKind::Normal => Frame::Entry {
            group,
            entry: Entry::new_normal(log_id, Command::decode(body).ok()?),
        },
        FrameKind::Membership => {
            let membership: Membership<ShardAddr, ShardAddr> = postcard::from_bytes(body).ok()?;
            Frame::Entry {
                group,
                entry: Entry::new_membership(log_id, membership),
            }
        }
        FrameKind::Vote => Frame::Vote {
            group,
            vote: Vote::from_leader_id(LeaderId::new(term, leader), flag != 0),
        },
        FrameKind::Committed => Frame::Committed {
            group,
            log_id: (flag != 0).then_some(log_id),
        },
        FrameKind::Purged => Frame::Purged { group, log_id },
        FrameKind::Truncate => Frame::Truncate {
            group,
            keep_after: (flag != 0).then_some(log_id),
        },
    };
    Some(frame)
}

/// Decode one frame at an offset, saying how many bytes it took
///
/// `None` means the frame at that offset is torn: its prefix or body runs past the end, its
/// checksum does not match, or its header does not parse. Everything after a torn frame is
/// discarded by the caller.
///
/// # Arguments
///
/// * `bytes` - The file, or the part of it the frame is in
/// * `offset` - Where the frame begins
pub fn decode_at(bytes: &[u8], offset: usize) -> Option<(Frame, usize)> {
    // the prefix has to be there whole
    if offset + FRAME_PREFIX > bytes.len() {
        return None;
    }
    let len = u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]) as usize;
    let checksum = u32::from_le_bytes([
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ]);
    // a body that runs past the end is a write that never finished
    let start = offset + FRAME_PREFIX;
    let end = start.checked_add(len).filter(|end| *end <= bytes.len())?;
    let hashed = &bytes[start..end];
    // a body that does not match its checksum was half written
    if gxhash::gxhash32(hashed, FRAME_SEED) != checksum {
        return None;
    }
    let frame = decode_hashed(hashed)?;
    Some((frame, FRAME_PREFIX + len))
}

/// Decode every whole frame of a file, and say where the first torn one begins
///
/// # Arguments
///
/// * `bytes` - The whole file
pub fn decode_all(bytes: &[u8]) -> (Vec<(u64, u32, Frame)>, u64) {
    let mut frames = Vec::new();
    let mut offset = 0usize;
    while let Some((frame, took)) = decode_at(bytes, offset) {
        // truncation cannot happen: a frame's length is a u32
        #[allow(clippy::cast_possible_truncation)]
        frames.push((offset as u64, took as u32, frame));
        offset += took;
    }
    (frames, offset as u64)
}

/// The command a normal entry frame carries, read straight out of a frame's bytes
///
/// For the compactor, which reads the frames a segment holds for its table and needs the
/// command inside each and nothing else. A frame that is not a normal entry, or is torn, is
/// `None`.
///
/// # Arguments
///
/// * `bytes` - The frame's bytes, prefix included
pub fn command_of(bytes: &[u8]) -> Option<Command> {
    match decode_at(bytes, 0)? {
        (
            Frame::Entry {
                entry: Entry {
                    payload: EntryPayload::Normal(command),
                    ..
                },
                ..
            },
            _,
        ) => Some(command),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::identity::TableId;
    use crate::shared::protocol::peer::RequestId;
    use std::collections::BTreeSet;

    /// A log id under a leader on shard two of some node
    fn log_id(term: u64, index: u64) -> WalLogId {
        LogId::new(LeaderId::new(term, ShardAddr::from(2)), index)
    }

    /// Every frame kind round trips through its bytes, and a torn or corrupt tail stops a decode
    #[test]
    fn frames_round_trip_and_a_torn_tail_stops_the_decode() {
        let group = GroupId(0xabcd);
        let command = Command {
            table: TableId::of("Row"),
            tablet: 5,
            request: RequestId {
                bundle: [1u8; 16],
                index: 3,
            },
            payload: vec![9, 8, 7, 200],
        };
        let membership = Membership::<ShardAddr, ShardAddr>::new_with_defaults(
            vec![BTreeSet::from([ShardAddr::from(1), ShardAddr::from(2)])],
            [ShardAddr::from(3)],
        );
        let frames = vec![
            Frame::Entry {
                group,
                entry: Entry::new_blank(log_id(1, 1)),
            },
            Frame::Entry {
                group,
                entry: Entry::new_normal(log_id(1, 2), command.clone()),
            },
            Frame::Entry {
                group,
                entry: Entry::new_membership(log_id(2, 3), membership),
            },
            Frame::Vote {
                group,
                vote: Vote::from_leader_id(LeaderId::new(4, ShardAddr::from(9)), true),
            },
            Frame::Committed {
                group,
                log_id: Some(log_id(2, 3)),
            },
            Frame::Committed {
                group,
                log_id: None,
            },
            Frame::Purged {
                group,
                log_id: log_id(1, 1),
            },
            Frame::Truncate {
                group,
                keep_after: Some(log_id(1, 2)),
            },
            Frame::Truncate {
                group,
                keep_after: None,
            },
        ];
        // encode every one into one buffer, the way a segment holds them
        let mut bytes = Vec::new();
        let mut lens = Vec::new();
        for frame in &frames {
            let encoded = match frame {
                Frame::Entry { group, entry } => encode_entry(*group, entry).unwrap(),
                Frame::Vote { group, vote } => encode_vote(*group, vote).unwrap(),
                Frame::Committed { group, log_id } => {
                    encode_marker(FrameKind::Committed, *group, log_id.as_ref()).unwrap()
                }
                Frame::Purged { group, log_id } => {
                    encode_marker(FrameKind::Purged, *group, Some(log_id)).unwrap()
                }
                Frame::Truncate { group, keep_after } => {
                    encode_marker(FrameKind::Truncate, *group, keep_after.as_ref()).unwrap()
                }
            };
            lens.push(encoded.len());
            bytes.extend_from_slice(&encoded);
        }
        let (decoded, whole) = decode_all(&bytes);
        assert_eq!(whole, bytes.len() as u64);
        assert_eq!(decoded.len(), frames.len());
        let mut at = 0u64;
        for ((offset, len, frame), (expected, expected_len)) in decoded.iter().zip(frames.iter().zip(&lens)) {
            assert_eq!(*offset, at);
            assert_eq!(*len as usize, *expected_len);
            assert_eq!(frame, expected);
            at += u64::from(*len);
        }
        // the command reads straight out of its frame
        let normal = &bytes[lens[0]..lens[0] + lens[1]];
        assert_eq!(command_of(normal), Some(command));
        assert_eq!(command_of(&bytes[..lens[0]]), None);
        // a cut tail keeps every whole frame before it and nothing after
        let cut = bytes.len() - 3;
        let (decoded, whole) = decode_all(&bytes[..cut]);
        assert_eq!(decoded.len(), frames.len() - 1);
        assert_eq!(whole, (bytes.len() - lens[lens.len() - 1]) as u64);
        // a corrupt checksum stops the decode at that frame, whatever follows
        let mut corrupt = bytes.clone();
        corrupt[lens[0] + FRAME_PREFIX + 3] ^= 0xff;
        let (decoded, whole) = decode_all(&corrupt);
        assert_eq!(decoded.len(), 1);
        assert_eq!(whole, lens[0] as u64);
        // and an unknown version does too
        let mut wrong = bytes.clone();
        wrong[FRAME_PREFIX + 1] = 9;
        assert!(decode_at(&wrong, 0).is_none());
    }
}
