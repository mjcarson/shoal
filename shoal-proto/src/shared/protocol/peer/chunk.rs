//! The object lane's frames: what one node asks of a slice on another, and its answer
//!
//! ```text
//!  request  : [header][id u64][kind u8][flags u8][reserved 6 B][body]
//!  piece    : [stream u64][chunk][label][chunk bytes u64][unit bytes u32][crcs u32][length u64][offset u64][last u8][reserved 3 B][stripe length u32][crcs][units]
//!  response : [header][id u64][answer u8][code u8][reserved 6 B][body]
//!  chunk    : [consumer u64][tablet u16][sub u16][reserved 4 B][object u128][stripe u64][position u8][reserved 7 B]
//!  label    : [sequence u64][tag u64]
//! ```
//!
//! An object lane carries one slice's chunks, which its hello names, and is served on the executor
//! that owns the slice ([F86](../../../../../docs/src/features/replicated-pools.md)). Its frames
//! name a chunk and a label and carry bytes that are never an archive: a put's whole chunk in
//! pieces, a stage of new values, an apply, a discard, a confirmation, a fold, a read, a listing,
//! and since [F89](../../../../../docs/src/features/scrub-and-repair.md) a verification of some of a
//! chunk's units where they lie. Every request is answered once, under its correlation id: a piece
//! of a whole chunk before its last once it is written, the last once the chunk is durable.
//!
//! **A unit's checksum crosses with its bytes.** The coordinating node computes each unit's
//! CRC-64/NVME as the bytes arrive and folds them into the put's; a holder binds them to the
//! unit's place without reading the bytes again ([X5](../../../../../docs/src/object-storage/checksums.md)).
//!
//! # Invariants
//!
//! **A frame that cannot be right ends its connection**: a head too short for its own counts, a
//! kind or an answer this build does not read, a marked label, units that do not fill the frame.
//! A request a slice refuses is answered by name with its [`StoreCode`], and the lane goes on.
//!
//! **No label on this lane is marked.** A marked label names a state no holder holds; a request
//! carrying one is malformed.

use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use rkyv::util::AlignedVec;

use super::super::ProtocolError;
use super::{u16_at, u32_at, u64_at};
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;
use crate::shared::stripes::ChunkId;

/// The bytes ahead of a request's or a response's body
pub const CHUNK_HEAD_LEN: usize = 16;

/// The bytes a chunk's identity is written as
pub const CHUNK_ID_LEN: usize = 48;

/// The bytes a label is written as
pub const LABEL_LEN: usize = 16;

/// The bytes of a piece's fixed fields, after its request's head and before its checksums: what a
/// reader reads to learn how many checksums, and so how many bytes of head, come before its units
/// ([F111](../../../../../docs/src/features/lane-buffers.md))
pub const PIECE_FIXED_LEN: usize = 120;

/// Where a piece's count of checksums is in its fixed fields
const PIECE_CRCS_AT: usize = 84;

/// Whether a request is a piece of a whole chunk, from its head's [`CHUNK_HEAD_LEN`] bytes (F111)
///
/// # Arguments
///
/// * `head` - The request body's first bytes
#[must_use]
pub fn is_piece(head: &[u8]) -> bool {
    head.get(8).is_some_and(|kind| {
        matches!(ChunkKind::from_byte(*kind), Ok(ChunkKind::Write | ChunkKind::Stage))
    })
}

/// How many checksums a piece carries, from its [`PIECE_FIXED_LEN`] bytes of fixed fields (F111)
///
/// # Arguments
///
/// * `fixed` - The piece's fixed fields
#[must_use]
pub fn piece_crcs(fixed: &[u8]) -> usize {
    fixed
        .get(PIECE_CRCS_AT..PIECE_CRCS_AT + 4)
        .map_or(0, |count| u32_at(count, 0) as usize)
}

/// The bytes ahead of each unit's bytes in a stage, a fold or a piece: its index and its checksum
pub const UNIT_HEAD_LEN: usize = 16;

/// The most labels one request names
pub const MAX_LABELS: usize = 64;

/// The most chunks one listing answers
pub const MAX_LISTED: u32 = 4096;

/// The flag a background request carries in its head: a rebuild's or a move's, which its slice
/// serves only while no foreground request is in flight on it, under its device's ceiling (F88)
const FLAG_BACKGROUND: u8 = 1;

/// The flag a scrub's request carries in its head: served only while its slice has no foreground
/// request and no rebuild's or move's in flight, one at a time, under its device's ceiling (F89)
const FLAG_SCRUB: u8 = 2;

/// The most units one verdict names: a chunk's whole header of checksums
pub const MAX_VERIFIED_UNITS: u32 = 480;

/// What a request asks of a slice
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ChunkKind {
    /// A piece of a put's whole chunk, written into place once its last piece arrives
    Write = 1,
    /// A piece of a whole chunk to stage, applied once its write commits
    Stage = 2,
    /// New values of some units of a chunk, staged over a label the slice can make
    StageUnits = 3,
    /// Apply a staged label that committed
    Apply = 4,
    /// Discard a staged label that can never commit
    Discard = 5,
    /// Say which of some labels the slice's chunk is at, or can make
    Confirm = 6,
    /// Journal a small write's committed bytes over a label they fold from, and apply them
    Fold = 7,
    /// Read a range of a chunk at a label
    Read = 8,
    /// List the chunks a slice holds of one placement group, from a place in it (F88)
    List = 9,
    /// Verify some of a chunk's units where they lie, against the checksums its header binds
    /// (F89)
    Verify = 10,
    /// Fold the change to some data chunks into a parity chunk read at a label, and stage the
    /// result as its new values (F90)
    StageParity = 11,
    /// Verify some of a chunk's units as [`ChunkKind::Verify`] does, and answer their summary for
    /// the parity check beside their checksums (F90)
    VerifySummary = 12,
    /// Seal an object's chunks in a placement group, or a whole group, on a slice: every request
    /// for what is sealed waits until the seal ends (M20)
    Seal = 13,
    /// End a seal without reclaiming anything
    Unseal = 14,
    /// Remove what a seal covers, under that seal: a retired object's chunks, or a group a slice
    /// no longer holds (M20)
    Reclaim = 15,
    /// Remove a chunk if its label is at or below a sequence a reclamation committed past: a
    /// stripe a truncate's floor hides (M20)
    DiscardThrough = 16,
}

impl ChunkKind {
    /// Parse a kind from its byte
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte
    ///
    /// # Errors
    ///
    /// A byte naming no kind this build reads.
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            1 => Ok(ChunkKind::Write),
            2 => Ok(ChunkKind::Stage),
            3 => Ok(ChunkKind::StageUnits),
            4 => Ok(ChunkKind::Apply),
            5 => Ok(ChunkKind::Discard),
            6 => Ok(ChunkKind::Confirm),
            7 => Ok(ChunkKind::Fold),
            8 => Ok(ChunkKind::Read),
            9 => Ok(ChunkKind::List),
            10 => Ok(ChunkKind::Verify),
            11 => Ok(ChunkKind::StageParity),
            12 => Ok(ChunkKind::VerifySummary),
            13 => Ok(ChunkKind::Seal),
            14 => Ok(ChunkKind::Unseal),
            15 => Ok(ChunkKind::Reclaim),
            16 => Ok(ChunkKind::DiscardThrough),
            _ => Err(ProtocolError::MalformedObject(
                "an object lane request names a kind this build does not read",
            )),
        }
    }
}

/// Why a slice refused a request, as the lane carries it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StoreCode {
    /// The device under the slice failed
    DeviceFailed = 1,
    /// A stage expects a label the slice cannot make
    CannotMake = 2,
    /// An apply or a discard of a label no record on the slice makes
    NotStaged = 3,
    /// No space for a stage without breaching the device's reserve
    NoSpace = 4,
    /// Every journal segment is held; retry once applies catch up
    JournalFull = 5,
    /// A unit fails its checksum and nothing can write it again
    Corrupt = 6,
    /// The executor's memory budget had no room; retry later
    Budget = 7,
    /// The slice is not served here, or no longer
    UnknownSlice = 8,
    /// A request no slice can serve
    Invalid = 9,
    /// The node's object executors are not running yet, or have stopped
    Unavailable = 10,
    /// A reclamation whose seal lapsed or was taken by another, so nothing was removed (M20)
    NotSealed = 11,
    /// A write of what a reclamation removed from the slice moments ago, refused so a late piece
    /// does not bring back what the reclamation freed (item 253)
    Removed = 12,
}

impl StoreCode {
    /// Parse a code from its byte
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte
    ///
    /// # Errors
    ///
    /// A byte naming no code this build reads.
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            1 => Ok(StoreCode::DeviceFailed),
            2 => Ok(StoreCode::CannotMake),
            3 => Ok(StoreCode::NotStaged),
            4 => Ok(StoreCode::NoSpace),
            5 => Ok(StoreCode::JournalFull),
            6 => Ok(StoreCode::Corrupt),
            7 => Ok(StoreCode::Budget),
            8 => Ok(StoreCode::UnknownSlice),
            9 => Ok(StoreCode::Invalid),
            10 => Ok(StoreCode::Unavailable),
            11 => Ok(StoreCode::NotSealed),
            12 => Ok(StoreCode::Removed),
            _ => Err(ProtocolError::MalformedObject(
                "an object lane answer names a code this build does not read",
            )),
        }
    }

    /// Whether the same request, sent again later, may be served
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            StoreCode::NoSpace | StoreCode::JournalFull | StoreCode::Budget | StoreCode::Unavailable
        )
    }
}

/// One unit of a chunk with its checksum: in a stage of new values, a fold, or a piece
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitBytes {
    /// The unit's index in its chunk
    pub index: u32,
    /// The CRC-64/NVME of its bytes, not yet bound to its place
    pub crc: u64,
    /// Its bytes, a whole unit, shared with every request and frame that carries them rather than
    /// copied into each ([F106](../../../../../docs/src/features/lane-copies.md))
    pub bytes: Bytes,
}

/// The change to one data chunk's units that a parity holder folds into its own parity (F90)
///
/// Each unit is the old bytes XOR the new, at the unit's index in the data chunk, which is its
/// unit row's index in every chunk; its checksum crosses with it, as a stage's does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParityChange {
    /// The data chunk that changed, its position in the stripe
    pub data: u8,
    /// The change to each of its units
    pub units: Vec<UnitBytes>,
}

/// A piece of a whole chunk, written in order from the start, its units' checksums beside it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Piece {
    /// The stream the piece belongs to, minted by its sender, so two writers of one chunk never
    /// have their pieces taken for one stream
    pub stream: u64,
    /// The chunk
    pub chunk: ChunkId,
    /// The label the chunk is written or staged under
    pub label: Label,
    /// The chunk's size in bytes, every unit of it
    pub chunk_bytes: u64,
    /// The bytes of one unit
    pub unit_bytes: u32,
    /// How much of the chunk holds bytes; the rest reads as zeros
    pub length: u64,
    /// Where in the chunk this piece starts, a multiple of the unit
    pub offset: u64,
    /// Whether this is the chunk's last piece
    pub last: bool,
    /// How much of the chunk's stripe holds bytes, zero where not recorded
    ///
    /// An erasure coded chunk's length is its share of its stripe, and a stripe its put wrote has
    /// no row to say the rest: a chunk's header keeps it, so a node that finds the chunk with no
    /// row knows the stripe's length from any one chunk ([F90](../../../../../docs/src/features/erasure-coded-pools.md)).
    /// Written in four bytes that were reserved: a stripe is at most 4 GiB.
    pub stripe_length: u64,
    /// The key the chunk's consumer records its owner under, for the chunk's header: for a
    /// bucket, the object's path's `ObjectMeta` key; zero where it is not known (M20)
    pub owner: u64,
    /// The CRC-64/NVME of each unit this piece covers, not yet bound to its place
    pub crcs: Vec<u64>,
    /// The piece's bytes, whole units, shared with every request and frame that carries them rather
    /// than copied into each ([F106](../../../../../docs/src/features/lane-copies.md))
    pub bytes: Bytes,
}

/// What one node asks of a slice on another
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkRequest {
    /// A piece of a put's whole chunk, written into place once the last arrives
    Write(Piece),
    /// A piece of a whole chunk to stage, applied once its write commits
    Stage(Piece),
    /// New values of some units, staged over the first of `expects` the slice can make
    StageUnits {
        /// The chunk
        chunk: ChunkId,
        /// The label the stage makes
        makes: Label,
        /// The labels it may be laid over: the row's, then any pending bytes fold from
        expects: Vec<Label>,
        /// How much of the chunk holds bytes after it
        length: u64,
        /// The bytes of one unit
        unit_bytes: u32,
        /// The units and their new values
        units: Vec<UnitBytes>,
    },
    /// Apply a staged label that committed
    Apply {
        /// The chunk
        chunk: ChunkId,
        /// The label
        label: Label,
    },
    /// Discard a staged label that can never commit
    Discard {
        /// The chunk
        chunk: ChunkId,
        /// The label
        label: Label,
    },
    /// Which of some labels the slice's chunk is at or can make, first match first
    Confirm {
        /// The chunk
        chunk: ChunkId,
        /// The labels, in the order they are wanted
        labels: Vec<Label>,
    },
    /// A small write's committed bytes, journalled over a label they fold from and applied
    Fold {
        /// The chunk
        chunk: ChunkId,
        /// The label the bytes make
        makes: Label,
        /// The labels they fold from: the base, then each earlier small write merged
        sources: Vec<Label>,
        /// How much of the chunk holds bytes after them
        length: u64,
        /// The bytes of one unit
        unit_bytes: u32,
        /// The units and their values
        units: Vec<UnitBytes>,
    },
    /// A range of a chunk at the first of some labels the slice can make
    Read {
        /// The chunk
        chunk: ChunkId,
        /// Where the range starts in the chunk
        offset: u64,
        /// How long it is
        len: u64,
        /// The labels, the row's first, then any its pending bytes fold from
        labels: Vec<Label>,
    },
    /// The chunks a slice holds of one placement group, in order, after a place in it
    ///
    /// What a move's copier asks every old holder of a group, since a stripe its put wrote whole
    /// has no row to find it by (F88). A staged whole chunk is listed with the label it stages.
    List {
        /// The consumer and the placement group, with the place to list after in its object,
        /// stripe and position; listing starts at the first chunk past it
        after: ChunkId,
        /// Whether `after` is the group's start rather than a chunk listed already
        start: bool,
        /// The most chunks to answer
        limit: u32,
    },
    /// Some of a chunk's units read where they lie and verified against its header, if the
    /// chunk is at a label (F89)
    ///
    /// A deep scrub's, one piece at a time: the units' bytes stay on the holder, and only its
    /// verdict and each unit's checksum cross the lane.
    Verify {
        /// The chunk
        chunk: ChunkId,
        /// The label the row names for it
        label: Label,
        /// Where the range starts in the chunk; every unit it touches is verified
        offset: u64,
        /// How long it is
        len: u64,
    },
    /// The change to some data chunks of a stripe, which a parity holder folds into its own
    /// parity at the label it expects and stages as new values (F90)
    ///
    /// Parity delta: the writer sends the change, never the result, and the holder stages the
    /// result, never the change, so a record replayed twice is the same parity (X1's
    /// `parity_staged_as_a_patch`). A holder not at the label expected refuses it `CannotMake`.
    StageParity {
        /// The parity chunk
        chunk: ChunkId,
        /// The label the stage makes
        makes: Label,
        /// The labels it may be laid over, the row's first
        expects: Vec<Label>,
        /// How much of the chunk holds bytes after it
        length: u64,
        /// The bytes of one unit
        unit_bytes: u32,
        /// The stripe's data chunks, `k`
        data: u8,
        /// The stripe's parity chunks, `m`
        parity: u8,
        /// Each data chunk's change
        changes: Vec<ParityChange>,
    },
    /// Some of a chunk's units verified as [`ChunkRequest::Verify`] does, answered with their
    /// summary beside their checksums: a deep scrub's piece of a placement group whose parity it
    /// checks (F90)
    VerifySummary {
        /// The chunk
        chunk: ChunkId,
        /// The label the row names for it
        label: Label,
        /// Where the range starts in the chunk; every unit it touches is verified
        offset: u64,
        /// How long it is
        len: u64,
    },
    /// Seal what a scope covers on the slice under an id: every other request for it waits until
    /// the seal ends, by a reclamation under it, an unseal, or its lapse on the slice's own clock
    /// (M20)
    Seal {
        /// What it covers
        scope: SealScope,
        /// The seal's id, which its reclamation names
        seal: u64,
    },
    /// End a seal, reclaiming nothing: what it covers is still named
    Unseal {
        /// What it covered
        scope: SealScope,
        /// The seal's id
        seal: u64,
    },
    /// Remove everything a scope covers on the slice, under the seal named; refused `NotSealed`
    /// when that seal lapsed or another took its place (M20)
    Reclaim {
        /// What it covers
        scope: SealScope,
        /// The seal it is removed under
        seal: u64,
    },
    /// Remove a chunk if the label it is at is at or below a sequence: a stripe a reclamation's
    /// commit tombstoned past it, which no write staged before that commit can name again (M20)
    DiscardThrough {
        /// The chunk
        chunk: ChunkId,
        /// The sequence the reclamation committed past
        through: u64,
    },
    /// A request served only while no foreground request is in flight on its slice, its bytes
    /// drawn from its device's recovery ceiling first: a rebuild's or a move's (F88)
    Background(Box<ChunkRequest>),
    /// A request served only while its slice has no foreground request and no rebuild's or
    /// move's in flight, one at a time, its bytes drawn from its device's recovery ceiling first:
    /// a scrub's (F89)
    Scrub(Box<ChunkRequest>),
}

/// One chunk a listing names
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListedChunk {
    /// The chunk
    pub chunk: ChunkId,
    /// The label it is at, or for a whole chunk staged and not yet applied the label it stages
    pub label: Label,
    /// How much of it holds bytes
    pub length: u64,
    /// Whether it is set aside for failing a checksum: evidence, never a chunk a read or a
    /// rebuild is offered (F89)
    pub quarantined: bool,
    /// Whether it is a whole chunk staged and not yet applied, a write still deciding (F89)
    pub staged: bool,
    /// How much of its stripe holds bytes, as its header recorded it; zero where not recorded
    /// ([F90](../../../../../docs/src/features/erasure-coded-pools.md))
    pub stripe_length: u64,
    /// The key its consumer records its owner under, as its header recorded it; zero where not
    /// recorded, so a light scrub can ask the consumer whether the object is named (M20)
    pub owner: u64,
}

/// What a seal or a reclamation covers on one slice (M20)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SealScope {
    /// One object's chunks in one placement group: a retired object's
    Object {
        /// The consumer
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
        /// The object
        object: u128,
    },
    /// Every chunk of one placement group: a group the slice no longer holds
    Group {
        /// The consumer
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
    },
}

impl SealScope {
    /// Whether a chunk falls under this scope
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk
    #[must_use]
    pub fn covers(&self, chunk: &ChunkId) -> bool {
        match self {
            SealScope::Object { consumer, pg, object } => {
                chunk.consumer == *consumer && chunk.pg == *pg && chunk.object == *object
            }
            SealScope::Group { consumer, pg } => chunk.consumer == *consumer && chunk.pg == *pg,
        }
    }

    /// The scope as a chunk identity, its object zero for a group, which the frame carries
    #[must_use]
    pub fn as_chunk(&self) -> ChunkId {
        match self {
            SealScope::Object { consumer, pg, object } => ChunkId {
                consumer: *consumer,
                pg: *pg,
                object: *object,
                stripe: 0,
                position: 0,
            },
            SealScope::Group { consumer, pg } => ChunkId {
                consumer: *consumer,
                pg: *pg,
                object: 0,
                stripe: 0,
                position: 0,
            },
        }
    }
}

/// The byte a scope is written with
const SCOPE_OBJECT: u8 = 1;

/// The byte a group's scope is written with
const SCOPE_GROUP: u8 = 2;

/// The flag a listed chunk set aside carries
const LISTED_QUARANTINED: u8 = 1;

/// The flag a listed chunk staged and not yet applied carries
const LISTED_STAGED: u8 = 2;

/// A slice's answer to a request
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkReply {
    /// It did what it was asked, durably
    Done,
    /// It refused, and why
    Refused {
        /// Why, for a caller to branch on
        code: StoreCode,
        /// Why, for a person
        message: String,
    },
    /// For a confirmation or a read: the label the chunk is at, none for no chunk, when it cannot
    /// make any label asked for, or which of them it makes when it is a confirmation
    Held {
        /// The label, or none
        label: Option<Label>,
    },
    /// A read's bytes, at the label named
    Bytes {
        /// The label the bytes are at, one of those asked for
        label: Label,
        /// The bytes, where the answer that brought them left them (F106)
        bytes: ChunkBytes,
    },
    /// A listing's chunks, each with the label it is at or stages and how much of it holds
    /// bytes, and whether more follow
    Listed {
        /// The chunks, in order
        chunks: Vec<ListedChunk>,
        /// Whether the listing stopped at its limit with more to come
        more: bool,
    },
    /// A verification's units all verified at the label asked for, with each unit's checksum of
    /// its bytes, not bound to its place, so a chunk's copies can be compared (F89)
    Verified {
        /// The label the chunk is at
        label: Label,
        /// Each unit's CRC-64/NVME, in order from the first asked for; zero for a unit that
        /// holds no bytes
        crcs: Vec<u64>,
    },
    /// A summarised verification's units all verified at the label asked for: each unit's
    /// checksum as [`ChunkReply::Verified`] has them, and their summary, the XOR of their bytes in
    /// 4 KiB blocks where they lie in the chunk (F90)
    Summarised {
        /// The label the chunk is at
        label: Label,
        /// Each unit's CRC-64/NVME, in order from the first asked for
        crcs: Vec<u64>,
        /// The units' summary
        summary: Vec<u8>,
    },
    /// A verification found units that fail their checksums and nothing could write again: the
    /// chunk is set aside on its slice, and is evidence until a replacement verifies (F89)
    Corrupt {
        /// The label the chunk was at
        label: Label,
        /// The units that failed; none for a header that failed its own
        units: Vec<u32>,
    },
}

/// A read's bytes as an answer carries them: in a buffer of their own, in the body of the frame
/// that brought them across a lane, never copied out of it ([F106](../../../../../docs/src/features/lane-copies.md)),
/// or in the buffer a read on this node filled, handed over whole
/// ([F110](../../../../../docs/src/features/store-checks-in-place.md))
///
/// Two are equal when their bytes are, wherever each is, and one prints as its length. Bytes
/// another owns are copied into a buffer of their own before they are changed.
#[derive(Clone)]
pub enum ChunkBytes {
    /// A buffer of their own
    Owned(Vec<u8>),
    /// The body of the frame they arrived in, from a place in it on
    Framed {
        /// The frame's body, as the link read it
        body: AlignedVec,
        /// Where the bytes start in it
        at: usize,
    },
    /// The buffer a read on this node filled, with whatever its owner holds beside it, given back
    /// once every holder of it is dropped
    Shared(Bytes),
}

impl ChunkBytes {
    /// Its bytes in a buffer of their own: moved where they are one already, copied out of a
    /// frame's body
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            ChunkBytes::Owned(bytes) => bytes,
            ChunkBytes::Framed { body, at } => body[at..].to_vec(),
            ChunkBytes::Shared(bytes) => Vec::from(bytes),
        }
    }

    /// Lengthen or shorten it where it is, any byte it gains set to a value
    ///
    /// # Arguments
    ///
    /// * `len` - Its new length
    /// * `value` - What a byte it gains is
    pub fn resize(&mut self, len: usize, value: u8) {
        match self {
            ChunkBytes::Owned(bytes) => bytes.resize(len, value),
            ChunkBytes::Framed { body, at } => body.resize(*at + len, value),
            // bytes another owns are copied into a buffer of their own to be changed
            ChunkBytes::Shared(bytes) => {
                let mut owned = bytes.to_vec();
                owned.resize(len, value);
                *self = ChunkBytes::Owned(owned);
            }
        }
    }
}

impl Default for ChunkBytes {
    /// No bytes, in a buffer of their own
    fn default() -> Self {
        ChunkBytes::Owned(Vec::new())
    }
}

impl From<Vec<u8>> for ChunkBytes {
    /// Bytes in a buffer of their own
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn from(bytes: Vec<u8>) -> Self {
        ChunkBytes::Owned(bytes)
    }
}

impl Deref for ChunkBytes {
    type Target = [u8];

    /// The bytes, wherever they are
    fn deref(&self) -> &[u8] {
        match self {
            ChunkBytes::Owned(bytes) => bytes,
            ChunkBytes::Framed { body, at } => &body[*at..],
            ChunkBytes::Shared(bytes) => bytes,
        }
    }
}

impl DerefMut for ChunkBytes {
    /// The bytes, to change where they are: bytes another owns copied into a buffer of their own
    /// first, which only a read laying a pending write over them does
    fn deref_mut(&mut self) -> &mut [u8] {
        if let ChunkBytes::Shared(bytes) = self {
            let owned = bytes.to_vec();
            *self = ChunkBytes::Owned(owned);
        }
        match self {
            ChunkBytes::Owned(bytes) => bytes,
            ChunkBytes::Framed { body, at } => &mut body[*at..],
            ChunkBytes::Shared(_) => unreachable!("shared bytes were copied out to be changed"),
        }
    }
}

impl PartialEq for ChunkBytes {
    /// Equal when their bytes are, wherever each is
    ///
    /// # Arguments
    ///
    /// * `other` - The other bytes
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for ChunkBytes {}

impl PartialEq<Vec<u8>> for ChunkBytes {
    /// Equal when its bytes are those
    ///
    /// # Arguments
    ///
    /// * `other` - The bytes
    fn eq(&self, other: &Vec<u8>) -> bool {
        **self == other[..]
    }
}

impl std::fmt::Debug for ChunkBytes {
    /// Its length, never its bytes, which may be a mebibyte
    ///
    /// # Arguments
    ///
    /// * `f` - Where it is written
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes", self.len())
    }
}

/// The answer byte of each [`ChunkReply`] variant
const ANSWER_DONE: u8 = 0;
const ANSWER_REFUSED: u8 = 1;
const ANSWER_HELD: u8 = 2;
const ANSWER_BYTES: u8 = 3;
const ANSWER_LISTED: u8 = 4;
const ANSWER_VERIFIED: u8 = 5;
const ANSWER_CORRUPT: u8 = 6;
const ANSWER_SUMMARISED: u8 = 7;

/// The bytes a summary is: X12's block, which every chunk's units fold into for the parity check
pub const SUMMARY_LEN: usize = 4096;

/// The most data chunks one parity stage carries a change of
pub const MAX_PARITY_CHANGES: usize = 32;

/// Where a body's bytes are written: one buffer, or parts that keep the bytes a request carries
/// apart, shared rather than copied ([F106](../../../../../docs/src/features/lane-copies.md))
trait Sink {
    /// Write fixed bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn put(&mut self, bytes: &[u8]);

    /// Write the bytes a request carries: a piece's or a unit's
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn share(&mut self, bytes: &Bytes);
}

impl Sink for Vec<u8> {
    /// Copy them onto the buffer
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn put(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    /// Copy them onto the buffer, as fixed bytes are
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn share(&mut self, bytes: &Bytes) {
        self.extend_from_slice(bytes);
    }
}

/// A body written as parts: fixed bytes gathered into the part being written, the bytes a request
/// carries a part of their own, the same allocation as the request's ([F106](../../../../../docs/src/features/lane-copies.md))
#[derive(Default)]
struct Parts {
    /// The parts written, in order
    done: Vec<Bytes>,
    /// The part being written
    open: Vec<u8>,
}

impl Parts {
    /// The parts, in order: their bytes, side by side, are the body
    fn finish(mut self) -> Vec<Bytes> {
        if !self.open.is_empty() || self.done.is_empty() {
            self.done.push(Bytes::from(self.open));
        }
        self.done
    }
}

impl Sink for Parts {
    /// Gather them into the part being written
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn put(&mut self, bytes: &[u8]) {
        self.open.extend_from_slice(bytes);
    }

    /// End the part being written and keep these as a part of their own, shared
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn share(&mut self, bytes: &Bytes) {
        if bytes.is_empty() {
            return;
        }
        if !self.open.is_empty() {
            self.done.push(Bytes::from(std::mem::take(&mut self.open)));
        }
        self.done.push(bytes.clone());
    }
}

/// Write a chunk's identity
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `chunk` - The chunk
fn put_chunk(out: &mut impl Sink, chunk: &ChunkId) {
    out.put(&chunk.consumer.to_le_bytes());
    out.put(&chunk.pg.tablet.to_le_bytes());
    out.put(&chunk.pg.sub.to_le_bytes());
    out.put(&[0u8; 4]);
    out.put(&chunk.object.to_le_bytes());
    out.put(&chunk.stripe.to_le_bytes());
    out.put(&[chunk.position]);
    out.put(&[0u8; 7]);
}

/// Write a label
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `label` - The label
fn put_label(out: &mut impl Sink, label: Label) {
    out.put(&label.sequence.to_le_bytes());
    out.put(&label.tag.to_le_bytes());
}

/// Write a count of labels and the labels
///
/// # Arguments
///
/// * `out` - Where to write them
/// * `labels` - The labels
fn put_labels(out: &mut impl Sink, labels: &[Label]) {
    out.put(&(labels.len() as u16).to_le_bytes());
    out.put(&[0u8; 6]);
    for label in labels {
        put_label(out, *label);
    }
}

/// Write a count of units and each unit with its head
///
/// # Arguments
///
/// * `out` - Where to write them
/// * `units` - The units
fn put_units(out: &mut impl Sink, units: &[UnitBytes]) {
    out.put(&(units.len() as u32).to_le_bytes());
    out.put(&[0u8; 4]);
    for unit in units {
        out.put(&unit.index.to_le_bytes());
        out.put(&[0u8; 4]);
        out.put(&unit.crc.to_le_bytes());
        out.share(&unit.bytes);
    }
}

/// Write a seal's scope: the chunk identity it reduces to, and whether it is an object's or a group's
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `scope` - The scope
fn put_scope(out: &mut impl Sink, scope: &SealScope) {
    put_chunk(out, &scope.as_chunk());
    out.put(&[match scope {
        SealScope::Object { .. } => SCOPE_OBJECT,
        SealScope::Group { .. } => SCOPE_GROUP,
    }]);
    out.put(&[0u8; 7]);
}

/// Write a piece's fields after its chunk
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `piece` - The piece
fn put_piece(out: &mut impl Sink, piece: &Piece) {
    out.put(&piece.stream.to_le_bytes());
    put_chunk(out, &piece.chunk);
    put_label(out, piece.label);
    out.put(&piece.chunk_bytes.to_le_bytes());
    out.put(&piece.unit_bytes.to_le_bytes());
    out.put(&(piece.crcs.len() as u32).to_le_bytes());
    out.put(&piece.length.to_le_bytes());
    out.put(&piece.offset.to_le_bytes());
    out.put(&[u8::from(piece.last)]);
    out.put(&[0u8; 3]);
    out.put(&u32::try_from(piece.stripe_length).unwrap_or(0).to_le_bytes());
    out.put(&piece.owner.to_le_bytes());
    for crc in &piece.crcs {
        out.put(&crc.to_le_bytes());
    }
    out.share(&piece.bytes);
}

/// The chunk a scoped request answers [`ChunkRequest::chunk`] with: its scope is not one chunk
const UNSCOPED: ChunkId = ChunkId {
    consumer: 0,
    pg: PlacementGroup { tablet: 0, sub: 0 },
    object: 0,
    stripe: 0,
    position: 0,
};

impl ChunkRequest {
    /// The scope a seal, an unseal or a reclamation covers, none for any other request (M20)
    #[must_use]
    pub fn scope(&self) -> Option<SealScope> {
        match self {
            ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => inner.scope(),
            ChunkRequest::Seal { scope, .. }
            | ChunkRequest::Unseal { scope, .. }
            | ChunkRequest::Reclaim { scope, .. } => Some(*scope),
            _ => None,
        }
    }

    /// The kind this request is
    #[must_use]
    pub fn kind(&self) -> ChunkKind {
        match self {
            ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => inner.kind(),
            ChunkRequest::List { .. } => ChunkKind::List,
            ChunkRequest::Verify { .. } => ChunkKind::Verify,
            ChunkRequest::StageParity { .. } => ChunkKind::StageParity,
            ChunkRequest::VerifySummary { .. } => ChunkKind::VerifySummary,
            ChunkRequest::Seal { .. } => ChunkKind::Seal,
            ChunkRequest::Unseal { .. } => ChunkKind::Unseal,
            ChunkRequest::Reclaim { .. } => ChunkKind::Reclaim,
            ChunkRequest::DiscardThrough { .. } => ChunkKind::DiscardThrough,
            ChunkRequest::Write(_) => ChunkKind::Write,
            ChunkRequest::Stage(_) => ChunkKind::Stage,
            ChunkRequest::StageUnits { .. } => ChunkKind::StageUnits,
            ChunkRequest::Apply { .. } => ChunkKind::Apply,
            ChunkRequest::Discard { .. } => ChunkKind::Discard,
            ChunkRequest::Confirm { .. } => ChunkKind::Confirm,
            ChunkRequest::Fold { .. } => ChunkKind::Fold,
            ChunkRequest::Read { .. } => ChunkKind::Read,
        }
    }

    /// The chunk this request names; a listing's place in its group
    ///
    /// A seal's and a reclamation's scope names no one chunk, and is answered by [`Self::scope`].
    #[must_use]
    pub fn chunk(&self) -> &ChunkId {
        match self {
            ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => inner.chunk(),
            ChunkRequest::List { after, .. } => after,
            ChunkRequest::Seal { .. } | ChunkRequest::Unseal { .. } | ChunkRequest::Reclaim { .. } => {
                &UNSCOPED
            }
            ChunkRequest::DiscardThrough { chunk, .. } => chunk,
            ChunkRequest::Write(piece) | ChunkRequest::Stage(piece) => &piece.chunk,
            ChunkRequest::StageUnits { chunk, .. }
            | ChunkRequest::Apply { chunk, .. }
            | ChunkRequest::Discard { chunk, .. }
            | ChunkRequest::Confirm { chunk, .. }
            | ChunkRequest::Fold { chunk, .. }
            | ChunkRequest::Read { chunk, .. }
            | ChunkRequest::Verify { chunk, .. }
            | ChunkRequest::StageParity { chunk, .. }
            | ChunkRequest::VerifySummary { chunk, .. } => chunk,
        }
    }

    /// The bytes of this request's body, after the frame header
    ///
    /// # Arguments
    ///
    /// * `id` - The correlation id its answer comes back under
    #[must_use]
    pub fn encode_body(&self, id: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(CHUNK_HEAD_LEN + 256);
        self.encode_into(id, 0, &mut out);
        out
    }

    /// The bytes of this request's body as parts, side by side the body
    /// [`ChunkRequest::encode_body`] makes: a piece's and a unit's bytes are parts of their own,
    /// the same allocation the request holds, so a link writes them where they are and a request
    /// sent to several holders shares one copy of them ([F106](../../../../../docs/src/features/lane-copies.md))
    ///
    /// # Arguments
    ///
    /// * `id` - The correlation id the sender waits on
    #[must_use]
    pub fn encode_parts(&self, id: u64) -> Vec<Bytes> {
        let mut parts = Parts::default();
        self.encode_into(id, 0, &mut parts);
        parts.finish()
    }

    /// Write this request's body to a sink
    ///
    /// # Arguments
    ///
    /// * `id` - The correlation id the sender waits on
    /// * `flags` - The flags of the requests it is wrapped in
    /// * `out` - Where its bytes go
    fn encode_into(&self, id: u64, flags: u8, out: &mut impl Sink) {
        // a background request is the request it wraps, with the flag in its head, and a scrub's
        // the same with its own
        if let ChunkRequest::Background(inner) = self {
            return inner.encode_into(id, flags | FLAG_BACKGROUND, out);
        }
        if let ChunkRequest::Scrub(inner) = self {
            return inner.encode_into(id, flags | FLAG_SCRUB, out);
        }
        // the head: the correlation id, the kind and its flags
        out.put(&id.to_le_bytes());
        out.put(&[self.kind() as u8, flags]);
        out.put(&[0u8; 6]);
        // then the kind's own fields
        match self {
            ChunkRequest::Background(_) | ChunkRequest::Scrub(_) => {
                unreachable!("encoded as the request it wraps")
            }
            ChunkRequest::Verify {
                chunk,
                label,
                offset,
                len,
            }
            | ChunkRequest::VerifySummary {
                chunk,
                label,
                offset,
                len,
            } => {
                put_chunk(out, chunk);
                put_label(out, *label);
                out.put(&offset.to_le_bytes());
                out.put(&len.to_le_bytes());
            }
            ChunkRequest::StageParity {
                chunk,
                makes,
                expects,
                length,
                unit_bytes,
                data,
                parity,
                changes,
            } => {
                put_chunk(out, chunk);
                put_label(out, *makes);
                out.put(&length.to_le_bytes());
                out.put(&unit_bytes.to_le_bytes());
                out.put(&[*data]);
                out.put(&[*parity]);
                out.put(&[0u8; 2]);
                put_labels(out, expects);
                out.put(&(changes.len() as u32).to_le_bytes());
                out.put(&[0u8; 4]);
                for change in changes {
                    out.put(&[change.data]);
                    out.put(&[0u8; 7]);
                    put_units(out, &change.units);
                }
            }
            ChunkRequest::List { after, start, limit } => {
                put_chunk(out, after);
                out.put(&[u8::from(*start)]);
                out.put(&[0u8; 3]);
                out.put(&limit.to_le_bytes());
            }
            ChunkRequest::Write(piece) | ChunkRequest::Stage(piece) => put_piece(out, piece),
            ChunkRequest::StageUnits {
                chunk,
                makes,
                expects,
                length,
                unit_bytes,
                units,
            }
            | ChunkRequest::Fold {
                chunk,
                makes,
                sources: expects,
                length,
                unit_bytes,
                units,
            } => {
                put_chunk(out, chunk);
                put_label(out, *makes);
                out.put(&length.to_le_bytes());
                out.put(&unit_bytes.to_le_bytes());
                out.put(&[0u8; 4]);
                put_labels(out, expects);
                put_units(out, units);
            }
            ChunkRequest::Apply { chunk, label } | ChunkRequest::Discard { chunk, label } => {
                put_chunk(out, chunk);
                put_label(out, *label);
            }
            ChunkRequest::Confirm { chunk, labels } => {
                put_chunk(out, chunk);
                put_labels(out, labels);
            }
            ChunkRequest::Seal { scope, seal }
            | ChunkRequest::Unseal { scope, seal }
            | ChunkRequest::Reclaim { scope, seal } => {
                put_scope(out, scope);
                out.put(&seal.to_le_bytes());
            }
            ChunkRequest::DiscardThrough { chunk, through } => {
                put_chunk(out, chunk);
                out.put(&through.to_le_bytes());
            }
            ChunkRequest::Read {
                chunk,
                offset,
                len,
                labels,
            } => {
                put_chunk(out, chunk);
                out.put(&offset.to_le_bytes());
                out.put(&len.to_le_bytes());
                put_labels(out, labels);
            }
        }
    }

    /// Read a request from its body
    ///
    /// Returns the correlation id and the request.
    ///
    /// # Arguments
    ///
    /// * `body` - Every byte after the frame header
    ///
    /// # Errors
    ///
    /// A body that cannot be right, which ends its connection.
    pub fn decode(body: &[u8]) -> Result<(u64, Self), ProtocolError> {
        Self::decode_from(Reader::new(body))
    }

    /// Read a request from a body it shares: a piece's and a unit's bytes are slices of it, never
    /// copied out ([F106](../../../../../docs/src/features/lane-copies.md))
    ///
    /// Returns the correlation id and the request.
    ///
    /// # Arguments
    ///
    /// * `body` - Every byte after the frame header
    ///
    /// # Errors
    ///
    /// A body that cannot be right, which ends its connection.
    pub fn decode_shared(body: &Bytes) -> Result<(u64, Self), ProtocolError> {
        Self::decode_from(Reader::sharing(body))
    }

    /// Read a piece of a whole chunk from its body's bytes before its units, which its reader read
    /// apart into a buffer of their own (F111)
    ///
    /// The head and every field are judged as [`ChunkRequest::decode_shared`] judges them, and the
    /// units must be exactly the bytes the piece's checksums cover; the piece's `bytes` are empty,
    /// since the units are where its reader put them. Anything but a piece is refused.
    ///
    /// # Arguments
    ///
    /// * `head` - Every byte of the body before the units
    /// * `units` - How many bytes of units its reader read apart
    ///
    /// # Errors
    ///
    /// A body that cannot be right, or a request that is not a piece, which ends its connection.
    pub fn decode_piece_head(head: &[u8], units: usize) -> Result<(u64, Self), ProtocolError> {
        let mut reader = Reader::new(head);
        reader.units_apart = Some(units);
        let (id, request) = Self::decode_from(reader)?;
        // only a piece's units are ever read apart, in its class or not
        let piece = match &request {
            ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => inner.as_ref(),
            other => other,
        };
        match piece {
            ChunkRequest::Write(_) | ChunkRequest::Stage(_) => Ok((id, request)),
            _ => Err(ProtocolError::MalformedObject(
                "an object lane request read apart that is not a piece",
            )),
        }
    }

    /// Read a request with a cursor over its body
    ///
    /// # Arguments
    ///
    /// * `reader` - The cursor, at the body's start
    ///
    /// # Errors
    ///
    /// A body that cannot be right, which ends its connection.
    fn decode_from(mut reader: Reader<'_>) -> Result<(u64, Self), ProtocolError> {
        // the head
        let id = reader.u64()?;
        let kind = ChunkKind::from_byte(reader.u8()?)?;
        let flags = reader.u8()?;
        if flags & !(FLAG_BACKGROUND | FLAG_SCRUB) != 0 || flags == FLAG_BACKGROUND | FLAG_SCRUB {
            return Err(ProtocolError::MalformedObject(
                "an object lane request carries a flag this build does not read",
            ));
        }
        reader.skip(6)?;
        let request = match kind {
            ChunkKind::List => {
                let after = reader.chunk()?;
                let start = reader.u8()? != 0;
                reader.skip(3)?;
                let limit = reader.u32()?;
                ChunkRequest::List {
                    after,
                    start,
                    limit: limit.min(MAX_LISTED),
                }
            }
            ChunkKind::Write | ChunkKind::Stage => {
                let piece = reader.piece()?;
                if kind == ChunkKind::Write {
                    ChunkRequest::Write(piece)
                } else {
                    ChunkRequest::Stage(piece)
                }
            }
            ChunkKind::StageUnits | ChunkKind::Fold => {
                let chunk = reader.chunk()?;
                let makes = reader.label()?;
                let length = reader.u64()?;
                let unit_bytes = reader.u32()?;
                reader.skip(4)?;
                let labels = reader.labels()?;
                let units = reader.units(unit_bytes)?;
                if kind == ChunkKind::StageUnits {
                    ChunkRequest::StageUnits {
                        chunk,
                        makes,
                        expects: labels,
                        length,
                        unit_bytes,
                        units,
                    }
                } else {
                    ChunkRequest::Fold {
                        chunk,
                        makes,
                        sources: labels,
                        length,
                        unit_bytes,
                        units,
                    }
                }
            }
            ChunkKind::Apply | ChunkKind::Discard => {
                let chunk = reader.chunk()?;
                let label = reader.label()?;
                if kind == ChunkKind::Apply {
                    ChunkRequest::Apply { chunk, label }
                } else {
                    ChunkRequest::Discard { chunk, label }
                }
            }
            ChunkKind::Confirm => ChunkRequest::Confirm {
                chunk: reader.chunk()?,
                labels: reader.labels()?,
            },
            ChunkKind::Seal | ChunkKind::Unseal | ChunkKind::Reclaim => {
                let scope = reader.scope()?;
                let seal = reader.u64()?;
                match kind {
                    ChunkKind::Seal => ChunkRequest::Seal { scope, seal },
                    ChunkKind::Unseal => ChunkRequest::Unseal { scope, seal },
                    _ => ChunkRequest::Reclaim { scope, seal },
                }
            }
            ChunkKind::DiscardThrough => ChunkRequest::DiscardThrough {
                chunk: reader.chunk()?,
                through: reader.u64()?,
            },
            ChunkKind::Read => ChunkRequest::Read {
                chunk: reader.chunk()?,
                offset: reader.u64()?,
                len: reader.u64()?,
                labels: reader.labels()?,
            },
            ChunkKind::Verify => ChunkRequest::Verify {
                chunk: reader.chunk()?,
                label: reader.label()?,
                offset: reader.u64()?,
                len: reader.u64()?,
            },
            ChunkKind::VerifySummary => ChunkRequest::VerifySummary {
                chunk: reader.chunk()?,
                label: reader.label()?,
                offset: reader.u64()?,
                len: reader.u64()?,
            },
            ChunkKind::StageParity => {
                let chunk = reader.chunk()?;
                let makes = reader.label()?;
                let length = reader.u64()?;
                let unit_bytes = reader.u32()?;
                let data = reader.u8()?;
                let parity = reader.u8()?;
                reader.skip(2)?;
                let expects = reader.labels()?;
                let count = reader.u32()? as usize;
                reader.skip(4)?;
                if count > MAX_PARITY_CHANGES {
                    return Err(ProtocolError::MalformedObject(
                        "an object lane parity stage carries more changes than a stripe has data chunks",
                    ));
                }
                let mut changes = Vec::with_capacity(count);
                for _ in 0..count {
                    let position = reader.u8()?;
                    reader.skip(7)?;
                    changes.push(ParityChange {
                        data: position,
                        units: reader.units(unit_bytes)?,
                    });
                }
                ChunkRequest::StageParity {
                    chunk,
                    makes,
                    expects,
                    length,
                    unit_bytes,
                    data,
                    parity,
                    changes,
                }
            }
        };
        // and nothing past the request
        reader.finish()?;
        let request = if flags & FLAG_BACKGROUND != 0 {
            ChunkRequest::Background(Box::new(request))
        } else if flags & FLAG_SCRUB != 0 {
            ChunkRequest::Scrub(Box::new(request))
        } else {
            request
        };
        Ok((id, request))
    }
}

impl ChunkReply {
    /// The bytes of this answer's body, after the frame header
    ///
    /// # Arguments
    ///
    /// * `id` - The correlation id of the request it answers
    #[must_use]
    pub fn encode_body(&self, id: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(CHUNK_HEAD_LEN + 32);
        out.extend_from_slice(&id.to_le_bytes());
        match self {
            ChunkReply::Done => {
                out.push(ANSWER_DONE);
                out.extend_from_slice(&[0u8; 7]);
            }
            ChunkReply::Refused { code, message } => {
                out.push(ANSWER_REFUSED);
                out.push(*code as u8);
                out.extend_from_slice(&[0u8; 6]);
                out.extend_from_slice(message.as_bytes());
            }
            ChunkReply::Held { label } => {
                out.push(ANSWER_HELD);
                out.push(u8::from(label.is_some()));
                out.extend_from_slice(&[0u8; 6]);
                put_label(&mut out, label.unwrap_or_default());
            }
            ChunkReply::Bytes { label, bytes } => {
                // the head a holder writes ahead of bytes it sends from where they lie, then the
                // bytes, so the two ways of sending a read's answer are one body (F106)
                out = Self::bytes_head(id, *label);
                out.extend_from_slice(bytes);
            }
            ChunkReply::Listed { chunks, more } => {
                out.push(ANSWER_LISTED);
                out.push(u8::from(*more));
                out.extend_from_slice(&[0u8; 6]);
                out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
                out.extend_from_slice(&[0u8; 4]);
                for listed in chunks {
                    put_chunk(&mut out, &listed.chunk);
                    put_label(&mut out, listed.label);
                    out.extend_from_slice(&listed.length.to_le_bytes());
                    let mut flags = 0u8;
                    if listed.quarantined {
                        flags |= LISTED_QUARANTINED;
                    }
                    if listed.staged {
                        flags |= LISTED_STAGED;
                    }
                    out.push(flags);
                    out.extend_from_slice(&[0u8; 3]);
                    out.extend_from_slice(&u32::try_from(listed.stripe_length).unwrap_or(0).to_le_bytes());
                    out.extend_from_slice(&listed.owner.to_le_bytes());
                }
            }
            ChunkReply::Verified { label, crcs } => {
                out.push(ANSWER_VERIFIED);
                out.extend_from_slice(&[0u8; 7]);
                put_label(&mut out, *label);
                out.extend_from_slice(&(crcs.len() as u32).to_le_bytes());
                out.extend_from_slice(&[0u8; 4]);
                for crc in crcs {
                    out.extend_from_slice(&crc.to_le_bytes());
                }
            }
            ChunkReply::Summarised { label, crcs, summary } => {
                out.push(ANSWER_SUMMARISED);
                out.extend_from_slice(&[0u8; 7]);
                put_label(&mut out, *label);
                out.extend_from_slice(&(crcs.len() as u32).to_le_bytes());
                out.extend_from_slice(&(summary.len() as u32).to_le_bytes());
                for crc in crcs {
                    out.extend_from_slice(&crc.to_le_bytes());
                }
                out.extend_from_slice(summary);
            }
            ChunkReply::Corrupt { label, units } => {
                out.push(ANSWER_CORRUPT);
                out.extend_from_slice(&[0u8; 7]);
                put_label(&mut out, *label);
                out.extend_from_slice(&(units.len() as u32).to_le_bytes());
                out.extend_from_slice(&[0u8; 4]);
                for unit in units {
                    out.extend_from_slice(&unit.to_le_bytes());
                }
            }
        }
        out
    }

    /// Read an answer from its body
    ///
    /// Returns the correlation id and the answer.
    ///
    /// # Arguments
    ///
    /// * `body` - Every byte after the frame header
    ///
    /// # Errors
    ///
    /// A body that cannot be right, which ends its connection.
    pub fn decode(body: &[u8]) -> Result<(u64, Self), ProtocolError> {
        // the head is the body's first bytes, and the rest follows it
        if body.len() < CHUNK_HEAD_LEN {
            return Err(ProtocolError::BodyTooShort {
                need: CHUNK_HEAD_LEN,
                got: body.len() as u32,
            });
        }
        Self::decode_parts(&body[..CHUNK_HEAD_LEN], &body[CHUNK_HEAD_LEN..])
    }

    /// The bytes of a read's answer ahead of its bytes: its head and its label
    ///
    /// A holder writes these, then the bytes from the buffers its store read them into, as one
    /// frame, so they are never copied into a body to be sent. These and the bytes are the body
    /// [`ChunkReply::encode_body`] makes of the same answer ([F106](../../../../../docs/src/features/lane-copies.md)).
    ///
    /// # Arguments
    ///
    /// * `id` - The correlation id of the request it answers
    /// * `label` - The label the bytes are at
    #[must_use]
    pub fn bytes_head(id: u64, label: Label) -> Vec<u8> {
        let mut out = Vec::with_capacity(CHUNK_HEAD_LEN + LABEL_LEN);
        out.extend_from_slice(&id.to_le_bytes());
        out.push(ANSWER_BYTES);
        out.extend_from_slice(&[0u8; 7]);
        put_label(&mut out, label);
        out
    }

    /// Read an answer from its head and the rest of its body, read apart, a read's bytes left in
    /// the body they were read into
    ///
    /// Returns the correlation id and the answer. A link reads a frame's head into a buffer of its
    /// own and the rest into another, and a read's bytes stay in that one, never copied out of it
    /// ([F106](../../../../../docs/src/features/lane-copies.md)). Any other answer is read as [`ChunkReply::decode_parts`]
    /// reads it.
    ///
    /// # Arguments
    ///
    /// * `head` - The body's first [`CHUNK_HEAD_LEN`] bytes
    /// * `rest` - Every byte after them, as the link read them
    ///
    /// # Errors
    ///
    /// A body that cannot be right, which ends its connection.
    pub fn decode_owned(head: &[u8], rest: AlignedVec) -> Result<(u64, Self), ProtocolError> {
        // anything but a read's bytes is read as it always was
        if head.get(8) != Some(&ANSWER_BYTES) {
            return Self::decode_parts(head, &rest);
        }
        // the head and the label judged as any answer's are, the bytes after them aside
        let label_end = LABEL_LEN.min(rest.len());
        let (id, reply) = Self::decode_parts(head, &rest[..label_end])?;
        let ChunkReply::Bytes { label, .. } = reply else {
            return Err(ProtocolError::MalformedObject(
                "an object lane read's answer that is not bytes",
            ));
        };
        // and the bytes left where the link read them
        Ok((
            id,
            ChunkReply::Bytes {
                label,
                bytes: ChunkBytes::Framed {
                    body: rest,
                    at: LABEL_LEN,
                },
            },
        ))
    }

    /// Read an answer from its head and the rest of its body, read apart
    ///
    /// Returns the correlation id and the answer. A read's bytes are copied out of `rest` into a
    /// buffer of their own; a link that owns the buffer it read them into keeps them there with
    /// [`ChunkReply::decode_owned`] instead (F106).
    ///
    /// # Arguments
    ///
    /// * `head` - The body's first [`CHUNK_HEAD_LEN`] bytes
    /// * `rest` - Every byte after them
    ///
    /// # Errors
    ///
    /// A body that cannot be right, which ends its connection.
    pub fn decode_parts(head: &[u8], rest: &[u8]) -> Result<(u64, Self), ProtocolError> {
        let mut front = Reader::new(head);
        let id = front.u64()?;
        let answer = front.u8()?;
        let code = front.u8()?;
        front.skip(6)?;
        front.finish()?;
        let mut reader = Reader::new(rest);
        let reply = match answer {
            ANSWER_DONE => ChunkReply::Done,
            ANSWER_REFUSED => ChunkReply::Refused {
                code: StoreCode::from_byte(code)?,
                message: String::from_utf8_lossy(reader.rest()).into_owned(),
            },
            ANSWER_HELD => {
                let label = reader.label_any()?;
                ChunkReply::Held {
                    label: (code != 0).then_some(label),
                }
            }
            ANSWER_BYTES => ChunkReply::Bytes {
                label: reader.label()?,
                bytes: reader.rest().to_vec().into(),
            },
            ANSWER_LISTED => {
                let count = reader.u32()?;
                reader.skip(4)?;
                if count > MAX_LISTED {
                    return Err(ProtocolError::MalformedObject(
                        "an object lane listing longer than any is",
                    ));
                }
                let mut chunks = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let chunk = reader.chunk()?;
                    let label = reader.label()?;
                    let length = reader.u64()?;
                    let flags = reader.u8()?;
                    if flags & !(LISTED_QUARANTINED | LISTED_STAGED) != 0 {
                        return Err(ProtocolError::MalformedObject(
                            "an object lane listing carries a flag this build does not read",
                        ));
                    }
                    reader.skip(3)?;
                    let stripe_length = u64::from(reader.u32()?);
                    let owner = reader.u64()?;
                    chunks.push(ListedChunk {
                        chunk,
                        label,
                        length,
                        quarantined: flags & LISTED_QUARANTINED != 0,
                        staged: flags & LISTED_STAGED != 0,
                        stripe_length,
                        owner,
                    });
                }
                ChunkReply::Listed {
                    chunks,
                    more: code != 0,
                }
            }
            ANSWER_SUMMARISED => {
                let label = reader.label()?;
                let count = reader.u32()?;
                let summary_len = reader.u32()? as usize;
                if count > MAX_VERIFIED_UNITS || summary_len != SUMMARY_LEN {
                    return Err(ProtocolError::MalformedObject(
                        "an object lane summary is not one a chunk's units make",
                    ));
                }
                let crcs = (0..count).map(|_| reader.u64()).collect::<Result<_, _>>()?;
                let summary = reader.rest().to_vec();
                if summary.len() != summary_len {
                    return Err(ProtocolError::MalformedObject(
                        "an object lane summary is not the length it says",
                    ));
                }
                ChunkReply::Summarised { label, crcs, summary }
            }
            ANSWER_VERIFIED | ANSWER_CORRUPT => {
                let label = reader.label()?;
                let count = reader.u32()?;
                reader.skip(4)?;
                if count > MAX_VERIFIED_UNITS {
                    return Err(ProtocolError::MalformedObject(
                        "an object lane verdict names more units than a chunk has",
                    ));
                }
                if answer == ANSWER_VERIFIED {
                    let crcs = (0..count).map(|_| reader.u64()).collect::<Result<_, _>>()?;
                    ChunkReply::Verified { label, crcs }
                } else {
                    let units = (0..count).map(|_| reader.u32()).collect::<Result<_, _>>()?;
                    ChunkReply::Corrupt { label, units }
                }
            }
            _ => {
                return Err(ProtocolError::MalformedObject(
                    "an object lane answer this build does not read",
                ))
            }
        };
        reader.finish()?;
        Ok((id, reply))
    }
}

/// A cursor over a body that refuses to read past it
struct Reader<'a> {
    /// The body
    raw: &'a [u8],
    /// Where the next read starts
    at: usize,
    /// The body as shared bytes, which carried bytes are sliced from, if it is one (F106)
    shared: Option<&'a Bytes>,
    /// A piece's units, read apart from the body into a buffer of their own, by their length (F111)
    units_apart: Option<usize>,
}

impl<'a> Reader<'a> {
    /// A cursor at a body's start
    ///
    /// # Arguments
    ///
    /// * `raw` - The body
    const fn new(raw: &'a [u8]) -> Self {
        Reader {
            raw,
            at: 0,
            shared: None,
            units_apart: None,
        }
    }

    /// A cursor at the start of a body it shares, whose carried bytes are slices of it (F106)
    ///
    /// # Arguments
    ///
    /// * `body` - The body
    fn sharing(body: &'a Bytes) -> Self {
        Reader {
            raw: body,
            at: 0,
            shared: Some(body),
            units_apart: None,
        }
    }

    /// The next `len` bytes as bytes a request carries: a slice of the body where it is shared,
    /// else a copy of them (F106)
    ///
    /// # Arguments
    ///
    /// * `len` - How many
    fn carried(&mut self, len: usize) -> Result<Bytes, ProtocolError> {
        let start = self.at;
        let taken = self.take(len)?;
        Ok(match self.shared {
            Some(body) => body.slice(start..start + len),
            None => Bytes::copy_from_slice(taken),
        })
    }

    /// Take the next `len` bytes, or refuse a body too short for them
    ///
    /// # Arguments
    ///
    /// * `len` - How many
    fn take(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self.at.checked_add(len).filter(|end| *end <= self.raw.len());
        let Some(end) = end else {
            return Err(ProtocolError::BodyTooShort {
                need: self.at.saturating_add(len),
                got: self.raw.len() as u32,
            });
        };
        let taken = &self.raw[self.at..end];
        self.at = end;
        Ok(taken)
    }

    /// Skip reserved bytes
    ///
    /// # Arguments
    ///
    /// * `len` - How many
    fn skip(&mut self, len: usize) -> Result<(), ProtocolError> {
        self.take(len).map(|_| ())
    }

    /// The next byte
    fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }

    /// The next little endian `u32`
    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32_at(self.take(4)?, 0))
    }

    /// The next little endian `u64`
    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64_at(self.take(8)?, 0))
    }

    /// The next chunk identity
    fn chunk(&mut self) -> Result<ChunkId, ProtocolError> {
        let raw = self.take(CHUNK_ID_LEN)?;
        let mut object = [0u8; 16];
        object.copy_from_slice(&raw[16..32]);
        Ok(ChunkId {
            consumer: u64_at(raw, 0),
            pg: PlacementGroup {
                tablet: u16_at(raw, 8),
                sub: u16_at(raw, 10),
            },
            object: u128::from_le_bytes(object),
            stripe: u64_at(raw, 32),
            position: raw[40],
        })
    }

    /// The next seal scope: a chunk identity and the byte saying whose it is
    fn scope(&mut self) -> Result<SealScope, ProtocolError> {
        let chunk = self.chunk()?;
        let kind = self.u8()?;
        self.skip(7)?;
        match kind {
            SCOPE_OBJECT => Ok(SealScope::Object {
                consumer: chunk.consumer,
                pg: chunk.pg,
                object: chunk.object,
            }),
            SCOPE_GROUP => Ok(SealScope::Group {
                consumer: chunk.consumer,
                pg: chunk.pg,
            }),
            _ => Err(ProtocolError::MalformedObject(
                "an object lane seal names a scope this build does not read",
            )),
        }
    }

    /// The next label, which may be marked: only a holder's own answer carries one
    fn label_any(&mut self) -> Result<Label, ProtocolError> {
        let raw = self.take(LABEL_LEN)?;
        Ok(Label {
            sequence: u64_at(raw, 0),
            tag: u64_at(raw, 8),
        })
    }

    /// The next label, refused if marked: no request names a state no holder holds
    fn label(&mut self) -> Result<Label, ProtocolError> {
        let label = self.label_any()?;
        if label.is_missed() {
            return Err(ProtocolError::MalformedObject(
                "an object lane frame names a marked label",
            ));
        }
        Ok(label)
    }

    /// The next count of labels and the labels
    fn labels(&mut self) -> Result<Vec<Label>, ProtocolError> {
        let count = usize::from(u16_at(self.take(2)?, 0));
        self.skip(6)?;
        if count > MAX_LABELS {
            return Err(ProtocolError::MalformedObject(
                "an object lane frame names more labels than any request needs",
            ));
        }
        (0..count).map(|_| self.label()).collect()
    }

    /// The next count of units and each unit
    ///
    /// # Arguments
    ///
    /// * `unit_bytes` - The bytes of one unit
    fn units(&mut self, unit_bytes: u32) -> Result<Vec<UnitBytes>, ProtocolError> {
        let count = self.u32()? as usize;
        self.skip(4)?;
        // every unit is whole, so the count has to fit what is left
        let each = UNIT_HEAD_LEN + unit_bytes as usize;
        if unit_bytes == 0 || count.saturating_mul(each) > self.raw.len() - self.at {
            return Err(ProtocolError::MalformedObject(
                "an object lane frame's units do not fill it",
            ));
        }
        let mut units = Vec::with_capacity(count);
        for _ in 0..count {
            let index = self.u32()?;
            self.skip(4)?;
            let crc = self.u64()?;
            let bytes = self.carried(unit_bytes as usize)?;
            units.push(UnitBytes { index, crc, bytes });
        }
        Ok(units)
    }

    /// The next piece of a whole chunk
    fn piece(&mut self) -> Result<Piece, ProtocolError> {
        let stream = self.u64()?;
        let chunk = self.chunk()?;
        let label = self.label()?;
        let chunk_bytes = self.u64()?;
        let unit_bytes = self.u32()?;
        let crcs = self.u32()? as usize;
        let length = self.u64()?;
        let offset = self.u64()?;
        let last = self.u8()? != 0;
        self.skip(3)?;
        let stripe_length = u64::from(self.u32()?);
        let owner = self.u64()?;
        // the checksums, then exactly the units they cover
        if crcs.saturating_mul(8) > self.raw.len() - self.at {
            return Err(ProtocolError::MalformedObject(
                "an object lane piece names more checksums than it carries",
            ));
        }
        let crcs: Vec<u64> = (0..crcs).map(|_| self.u64()).collect::<Result<_, _>>()?;
        // the units, after the checksums or read apart into a buffer of their own (F111)
        let (units, bytes) = match self.units_apart {
            Some(units) => (units, Bytes::new()),
            None => {
                let bytes = self.carried(self.raw.len() - self.at)?;
                (bytes.len(), bytes)
            }
        };
        if unit_bytes == 0 || units != crcs.len() * unit_bytes as usize {
            return Err(ProtocolError::MalformedObject(
                "an object lane piece's units do not match its checksums",
            ));
        }
        Ok(Piece {
            stream,
            chunk,
            label,
            chunk_bytes,
            unit_bytes,
            length,
            offset,
            last,
            stripe_length,
            owner,
            crcs,
            bytes,
        })
    }

    /// Everything left
    fn rest(&mut self) -> &'a [u8] {
        let rest = &self.raw[self.at..];
        self.at = self.raw.len();
        rest
    }

    /// Refuse a body with bytes past what was read
    fn finish(&self) -> Result<(), ProtocolError> {
        if self.at == self.raw.len() {
            Ok(())
        } else {
            Err(ProtocolError::MalformedObject(
                "an object lane frame carries bytes past its fields",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChunkBytes, ChunkKind, ChunkReply, ChunkRequest, ListedChunk, ParityChange, Piece, SealScope, StoreCode, UnitBytes, CHUNK_HEAD_LEN,
        SUMMARY_LEN, CHUNK_ID_LEN, LABEL_LEN,
    };
    use crate::shared::objects::Label;
    use crate::shared::poolmap::PlacementGroup;
    use crate::shared::protocol::ProtocolError;
    use crate::shared::stripes::ChunkId;

    /// A chunk with every field set
    fn chunk() -> ChunkId {
        ChunkId {
            consumer: 0x0102_0304_0506_0708,
            pg: PlacementGroup {
                tablet: 0x7ab,
                sub: 3,
            },
            object: 0x0192_6f1e_7a00_7000_8000_0000_0000_0001,
            stripe: 41,
            position: 2,
        }
    }

    /// A label at a sequence with a tag
    fn label(sequence: u64, tag: u64) -> Label {
        Label { sequence, tag }
    }

    /// A unit of four bytes
    fn unit(index: u32) -> UnitBytes {
        UnitBytes {
            index,
            crc: u64::from(index) * 7,
            bytes: vec![index as u8; 4].into(),
        }
    }

    /// Every request reads back as it was written
    #[test]
    fn every_request_round_trips() {
        let piece = Piece {
            stream: 3,
            chunk: chunk(),
            label: label(0, 9),
            chunk_bytes: 16,
            unit_bytes: 4,
            length: 13,
            offset: 8,
            last: true,
            stripe_length: 40,
            owner: 0x5eed_0000_0000_0042,
            crcs: vec![5, 6],
            bytes: vec![1; 8].into(),
        };
        let requests = [
            ChunkRequest::Write(piece.clone()),
            ChunkRequest::Stage(Piece {
                last: false,
                ..piece
            }),
            ChunkRequest::StageUnits {
                chunk: chunk(),
                makes: label(3, 4),
                expects: vec![label(2, 1), label(0, 0)],
                length: 16,
                unit_bytes: 4,
                units: vec![unit(0), unit(3)],
            },
            ChunkRequest::Apply {
                chunk: chunk(),
                label: label(3, 4),
            },
            ChunkRequest::Discard {
                chunk: chunk(),
                label: label(3, 4),
            },
            ChunkRequest::Confirm {
                chunk: chunk(),
                labels: vec![label(3, 4), label(2, 1)],
            },
            ChunkRequest::Fold {
                chunk: chunk(),
                makes: label(5, 6),
                sources: vec![label(3, 4)],
                length: 16,
                unit_bytes: 4,
                units: vec![unit(1)],
            },
            ChunkRequest::Read {
                chunk: chunk(),
                offset: 4,
                len: 8,
                labels: vec![label(5, 6), label(3, 4)],
            },
            // a parity holder's change of two data chunks, and a summarised verification (F90)
            ChunkRequest::StageParity {
                chunk: chunk(),
                makes: label(5, 6),
                expects: vec![label(3, 4)],
                length: 16,
                unit_bytes: 4,
                data: 4,
                parity: 2,
                changes: vec![
                    ParityChange {
                        data: 0,
                        units: vec![unit(1), unit(2)],
                    },
                    ParityChange {
                        data: 3,
                        units: vec![unit(2)],
                    },
                ],
            },
            ChunkRequest::VerifySummary {
                chunk: chunk(),
                label: label(3, 4),
                offset: 0,
                len: 16,
            },
            // a seal of an object and of a group, its end, a reclamation under one, and a
            // discard through a sequence (M20)
            ChunkRequest::Seal {
                scope: SealScope::Object {
                    consumer: chunk().consumer,
                    pg: chunk().pg,
                    object: chunk().object,
                },
                seal: 0xabcd,
            },
            ChunkRequest::Unseal {
                scope: SealScope::Group {
                    consumer: chunk().consumer,
                    pg: chunk().pg,
                },
                seal: 7,
            },
            ChunkRequest::Reclaim {
                scope: SealScope::Group {
                    consumer: chunk().consumer,
                    pg: chunk().pg,
                },
                seal: 8,
            },
            ChunkRequest::DiscardThrough {
                chunk: chunk(),
                through: 12,
            },
        ];
        for request in requests {
            let body = request.encode_body(77);
            assert_eq!(ChunkRequest::decode(&body).unwrap(), (77, request.clone()));
            // every body starts with the head, and its kind's byte
            assert_eq!(body[8], request.kind() as u8);
            assert!(body.len() >= CHUNK_HEAD_LEN + CHUNK_ID_LEN);
        }
    }

    /// A seal's scope covers what it names and nothing else: an object's chunks in its group, or
    /// every chunk of a group, and a scope this build does not read ends the connection (M20)
    #[test]
    fn a_seals_scope_covers_its_object_or_its_group() {
        let object = SealScope::Object {
            consumer: chunk().consumer,
            pg: chunk().pg,
            object: chunk().object,
        };
        let group = SealScope::Group {
            consumer: chunk().consumer,
            pg: chunk().pg,
        };
        let other_object = ChunkId {
            object: chunk().object + 1,
            ..chunk()
        };
        let other_group = ChunkId {
            pg: PlacementGroup { tablet: 9, sub: 0 },
            ..chunk()
        };
        assert!(object.covers(&chunk()) && !object.covers(&other_object));
        assert!(group.covers(&chunk()) && group.covers(&other_object) && !group.covers(&other_group));
        assert_eq!(
            ChunkRequest::Seal { scope: group, seal: 1 }.scope(),
            Some(group)
        );
        let mut body = ChunkRequest::Seal { scope: object, seal: 1 }.encode_body(2);
        // the scope's byte follows its chunk identity
        body[CHUNK_HEAD_LEN + CHUNK_ID_LEN] = 9;
        assert!(ChunkRequest::decode(&body).is_err());
    }

    /// A listing and its answer read back as written, and any request flagged background reads
    /// back as the same request in the background, its kind's byte unchanged (F88)
    #[test]
    fn a_listing_and_a_background_request_round_trip() {
        let listing = ChunkRequest::List {
            after: chunk(),
            start: true,
            limit: 512,
        };
        let body = listing.encode_body(9);
        assert_eq!(ChunkRequest::decode(&body).unwrap(), (9, listing.clone()));
        assert_eq!(body[8], ChunkKind::List as u8);
        // a background read and a background piece keep their kinds, and say so in their flags
        let requests = [
            ChunkRequest::Read {
                chunk: chunk(),
                offset: 0,
                len: 16,
                labels: vec![label(4, 2)],
            },
            listing,
        ];
        for request in requests {
            let background = ChunkRequest::Background(Box::new(request.clone()));
            let body = background.encode_body(11);
            assert_eq!(body[8], request.kind() as u8);
            assert_eq!(body[9], 1);
            assert_eq!(ChunkRequest::decode(&body).unwrap(), (11, background));
        }
        // a flag this build does not read ends the connection
        let mut body = ChunkRequest::Apply {
            chunk: chunk(),
            label: label(1, 1),
        }
        .encode_body(3);
        body[9] = 4;
        assert!(ChunkRequest::decode(&body).is_err());
        // and the listing's answer
        let listed = ChunkReply::Listed {
            chunks: vec![
                ListedChunk {
                    chunk: chunk(),
                    label: label(4, 2),
                    length: 9,
                    quarantined: false,
                    staged: true,
                    stripe_length: 33,
                    owner: 0x1234_5678,
                },
                ListedChunk {
                    chunk: chunk(),
                    label: label(0, 0),
                    length: 16,
                    quarantined: true,
                    staged: false,
                    stripe_length: 0,
                    owner: 0,
                },
            ],
            more: true,
        };
        assert_eq!(ChunkReply::decode(&listed.encode_body(5)).unwrap(), (5, listed));
    }

    /// A verification and its two verdicts read back as written, a scrub's request says so in
    /// its flags, and a request flagged both background and scrub ends the connection (F89)
    #[test]
    fn a_verification_and_its_verdicts_round_trip() {
        let verify = ChunkRequest::Verify {
            chunk: chunk(),
            label: label(0, 0),
            offset: 1 << 20,
            len: 1 << 20,
        };
        let body = verify.encode_body(4);
        assert_eq!(body[8], ChunkKind::Verify as u8);
        assert_eq!(ChunkRequest::decode(&body).unwrap(), (4, verify.clone()));
        // a scrub's verification and a scrub's listing keep their kinds, flagged
        let listing = ChunkRequest::List {
            after: chunk(),
            start: false,
            limit: 256,
        };
        for request in [verify.clone(), listing] {
            let scrub = ChunkRequest::Scrub(Box::new(request.clone()));
            let body = scrub.encode_body(6);
            assert_eq!(body[8], request.kind() as u8);
            assert_eq!(body[9], 2);
            assert_eq!(ChunkRequest::decode(&body).unwrap(), (6, scrub));
        }
        // both flags at once is no request
        let mut body = verify.encode_body(4);
        body[9] = 3;
        assert!(ChunkRequest::decode(&body).is_err());
        // a verdict naming more units than a chunk has is no answer
        let mut wide = ChunkReply::Corrupt {
            label: label(1, 1),
            units: vec![0; 2],
        }
        .encode_body(1);
        wide[CHUNK_HEAD_LEN + 16..CHUNK_HEAD_LEN + 20].copy_from_slice(&481u32.to_le_bytes());
        assert!(ChunkReply::decode(&wide).is_err());
        // the verdicts
        let answers = [
            ChunkReply::Verified {
                label: label(3, 4),
                crcs: vec![1, 0, u64::MAX],
            },
            ChunkReply::Corrupt {
                label: label(3, 4),
                units: vec![2, 17],
            },
            ChunkReply::Corrupt {
                label: label(0, 0),
                units: Vec::new(),
            },
            ChunkReply::Summarised {
                label: label(3, 4),
                crcs: vec![1, 2],
                summary: vec![7; SUMMARY_LEN],
            },
        ];
        for answer in answers {
            assert_eq!(ChunkReply::decode(&answer.encode_body(8)).unwrap(), (8, answer));
        }
        // a summary of another length is no answer
        let mut short = ChunkReply::Summarised {
            label: label(3, 4),
            crcs: vec![1],
            summary: vec![7; SUMMARY_LEN],
        }
        .encode_body(8);
        short.truncate(short.len() - 1);
        assert!(ChunkReply::decode(&short).is_err());
    }

    /// Every request's parts, side by side, are its body, a piece's and a unit's bytes parts of
    /// their own sharing the request's allocation; and a request decoded from a shared body takes
    /// those bytes as slices of it, never copies (F106)
    #[test]
    fn every_requests_parts_are_its_body() {
        let piece = Piece {
            stream: 3,
            chunk: chunk(),
            label: label(0, 9),
            chunk_bytes: 16,
            unit_bytes: 4,
            length: 13,
            offset: 8,
            last: true,
            stripe_length: 40,
            owner: 77,
            crcs: vec![1, 2],
            bytes: (1..=8u8).collect::<Vec<u8>>().into(),
        };
        let requests = [
            ChunkRequest::Write(piece.clone()),
            ChunkRequest::Background(Box::new(ChunkRequest::Stage(piece.clone()))),
            ChunkRequest::StageUnits {
                chunk: chunk(),
                makes: label(3, 4),
                expects: vec![label(2, 1)],
                length: 16,
                unit_bytes: 4,
                units: vec![unit(0), unit(2)],
            },
            ChunkRequest::Fold {
                chunk: chunk(),
                makes: label(3, 4),
                sources: vec![label(2, 1)],
                length: 16,
                unit_bytes: 4,
                units: vec![unit(1)],
            },
            ChunkRequest::StageParity {
                chunk: chunk(),
                makes: label(3, 4),
                expects: vec![label(2, 1)],
                length: 16,
                unit_bytes: 4,
                data: 2,
                parity: 1,
                changes: vec![ParityChange {
                    data: 0,
                    units: vec![unit(1), unit(3)],
                }],
            },
            ChunkRequest::Read {
                chunk: chunk(),
                offset: 0,
                len: 16,
                labels: vec![label(2, 1)],
            },
        ];
        for request in requests {
            let body = request.encode_body(9);
            let parts = request.encode_parts(9);
            assert_eq!(parts.concat(), body, "{request:?}");
            // decoded from a shared body, it is the request, and a carried byte is the body's own
            let shared = bytes::Bytes::from(body.clone());
            let (id, decoded) = ChunkRequest::decode_shared(&shared).unwrap();
            assert_eq!((id, &decoded), (9, &request));
            assert_eq!(ChunkRequest::decode(&body).unwrap(), (9, request.clone()));
            let carried: Vec<&[u8]> = match &decoded {
                ChunkRequest::Write(piece) => vec![&piece.bytes[..]],
                ChunkRequest::Background(inner) => match &**inner {
                    ChunkRequest::Stage(piece) => vec![&piece.bytes[..]],
                    other => panic!("{other:?}"),
                },
                ChunkRequest::StageUnits { units, .. } | ChunkRequest::Fold { units, .. } => {
                    units.iter().map(|unit| &unit.bytes[..]).collect()
                }
                ChunkRequest::StageParity { changes, .. } => {
                    changes.iter().flat_map(|change| change.units.iter()).map(|unit| &unit.bytes[..]).collect()
                }
                _ => Vec::new(),
            };
            let range = shared.as_ptr_range();
            for bytes in carried {
                assert!(range.contains(&bytes.as_ptr()), "{request:?}: carried bytes were copied out of their body");
            }
        }
    }

    /// Every answer reads back as it was written, a marked label included where a holder says it
    #[test]
    fn every_answer_round_trips() {
        let answers = [
            ChunkReply::Done,
            ChunkReply::Refused {
                code: StoreCode::CannotMake,
                message: "a stage over a label it cannot make".to_string(),
            },
            ChunkReply::Held { label: None },
            ChunkReply::Held {
                label: Some(label(2, 1)),
            },
            ChunkReply::Bytes {
                label: label(3, 4),
                bytes: vec![9; 33].into(),
            },
        ];
        for answer in answers {
            let body = answer.encode_body(5);
            assert_eq!(ChunkReply::decode(&body).unwrap(), (5, answer.clone()));
            // and kept where the link read it, every answer the same
            let mut rest = rkyv::util::AlignedVec::<16>::new();
            rest.extend_from_slice(&body[CHUNK_HEAD_LEN..]);
            assert_eq!(ChunkReply::decode_owned(&body[..CHUNK_HEAD_LEN], rest).unwrap(), (5, answer));
        }
        // a code's retry is its meaning's
        assert!(StoreCode::JournalFull.retryable());
        assert!(!StoreCode::CannotMake.retryable());
    }

    /// A read's head and its bytes, written apart, are the body its answer encodes to, and a read's
    /// bytes decoded from the body a link read them into stay in it, never copied; a body too short
    /// for a label is refused as before (F106)
    #[test]
    fn a_reads_bytes_stay_where_their_frame_was_read() {
        let bytes: Vec<u8> = (0..70_000u32).map(|at| (at % 251) as u8).collect();
        let answer = ChunkReply::Bytes {
            label: label(7, 2),
            bytes: bytes.clone().into(),
        };
        // the head and the bytes, side by side, are the body
        let mut written = ChunkReply::bytes_head(11, label(7, 2));
        assert_eq!(written.len(), CHUNK_HEAD_LEN + LABEL_LEN);
        written.extend_from_slice(&bytes);
        assert_eq!(written, answer.encode_body(11));
        // decoded where the link read them, the bytes are the body's own, past the label
        let mut rest = rkyv::util::AlignedVec::<16>::new();
        rest.extend_from_slice(&written[CHUNK_HEAD_LEN..]);
        let start = rest.as_ptr();
        let (id, decoded) = ChunkReply::decode_owned(&written[..CHUNK_HEAD_LEN], rest).unwrap();
        assert_eq!((id, &decoded), (11, &answer));
        let ChunkReply::Bytes { bytes: kept, .. } = decoded else {
            panic!("a read's answer decoded as {decoded:?}");
        };
        assert!(matches!(kept, ChunkBytes::Framed { at: LABEL_LEN, .. }), "{kept:?}");
        assert_eq!(kept.as_ptr(), start.wrapping_add(LABEL_LEN), "the bytes were copied out of their body");
        // a body that ends inside its label is refused
        let mut short = rkyv::util::AlignedVec::<16>::new();
        short.extend_from_slice(&written[CHUNK_HEAD_LEN..CHUNK_HEAD_LEN + LABEL_LEN - 1]);
        assert!(ChunkReply::decode_owned(&written[..CHUNK_HEAD_LEN], short).is_err());
    }

    /// Bytes are equal wherever each is, resize where they are, print as their length, and come out
    /// as a buffer of their own (F106)
    #[test]
    fn chunk_bytes_are_their_bytes_wherever_they_are() {
        let owned = ChunkBytes::from(vec![1, 2, 3]);
        let mut body = rkyv::util::AlignedVec::<16>::new();
        body.extend_from_slice(&[0, 0, 1, 2, 3]);
        let mut framed = ChunkBytes::Framed { body, at: 2 };
        assert_eq!(owned, framed);
        assert_eq!(framed, vec![1, 2, 3]);
        assert_eq!(format!("{framed:?}"), "3 bytes");
        // lengthened and shortened in place
        framed.resize(5, 9);
        assert_eq!(framed, vec![1, 2, 3, 9, 9]);
        framed.resize(2, 0);
        assert_eq!(framed, vec![1, 2]);
        // changed where it is
        framed[0] = 7;
        assert_eq!(framed.into_vec(), vec![7, 2]);
        assert_eq!(owned.into_vec(), vec![1, 2, 3]);
        assert!(ChunkBytes::default().is_empty());
    }

    /// A frame that cannot be right is malformed: a marked label, an unknown kind, units that do
    /// not fill it, or bytes past its fields
    #[test]
    fn a_frame_that_cannot_be_right_is_malformed() {
        let apply = ChunkRequest::Apply {
            chunk: chunk(),
            label: label(3, 4),
        };
        // a marked label names a state no holder holds
        let marked = ChunkRequest::Apply {
            chunk: chunk(),
            label: label(3, 4).marked(),
        };
        assert!(matches!(
            ChunkRequest::decode(&marked.encode_body(1)),
            Err(ProtocolError::MalformedObject(_))
        ));
        // a kind this build does not read
        let mut body = apply.encode_body(1);
        body[8] = ChunkKind::Verify as u8 + 1;
        assert!(ChunkRequest::decode(&body).is_err());
        // bytes past its fields
        let mut body = apply.encode_body(1);
        body.push(0);
        assert!(ChunkRequest::decode(&body).is_err());
        // cut short
        let body = apply.encode_body(1);
        assert!(matches!(
            ChunkRequest::decode(&body[..body.len() - 1]),
            Err(ProtocolError::BodyTooShort { .. })
        ));
        // units claiming more than the frame holds
        let stage = ChunkRequest::StageUnits {
            chunk: chunk(),
            makes: label(3, 4),
            expects: vec![label(2, 1)],
            length: 16,
            unit_bytes: 4,
            units: vec![unit(0)],
        };
        let mut body = stage.encode_body(1);
        let count_at = body.len() - 4 - 16 - 8;
        body[count_at] = 200;
        assert!(ChunkRequest::decode(&body).is_err());
    }
}
