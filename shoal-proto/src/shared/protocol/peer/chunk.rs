//! The object lane's frames: what one node asks of a slice on another, and its answer
//!
//! ```text
//!  request  : [header][id u64][kind u8][reserved 7 B][body]
//!  piece    : [stream u64][chunk][label][chunk bytes u64][unit bytes u32][crcs u32][length u64][offset u64][last u8][reserved 7 B][crcs][units]
//!  response : [header][id u64][answer u8][code u8][reserved 6 B][body]
//!  chunk    : [consumer u64][tablet u16][sub u16][reserved 4 B][object u128][stripe u64][position u8][reserved 7 B]
//!  label    : [sequence u64][tag u64]
//! ```
//!
//! An object lane carries one slice's chunks, which its hello names, and is served on the executor
//! that owns the slice ([F86](../../../../../docs/src/features/replicated-pools.md)). Its frames
//! name a chunk and a label and carry bytes that are never an archive: a put's whole chunk in
//! pieces, a stage of new values, an apply, a discard, a confirmation, a fold and a read. Every
//! request is answered once, under its correlation id: a piece of a whole chunk before its last once
//! it is written, the last once the chunk is durable.
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

/// The bytes ahead of each unit's bytes in a stage, a fold or a piece: its index and its checksum
pub const UNIT_HEAD_LEN: usize = 16;

/// The most labels one request names
pub const MAX_LABELS: usize = 64;

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
    /// Its bytes, a whole unit
    pub bytes: Vec<u8>,
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
    /// The CRC-64/NVME of each unit this piece covers, not yet bound to its place
    pub crcs: Vec<u64>,
    /// The piece's bytes, whole units
    pub bytes: Vec<u8>,
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
}

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
        /// The bytes
        bytes: Vec<u8>,
    },
}

/// The answer byte of each [`ChunkReply`] variant
const ANSWER_DONE: u8 = 0;
const ANSWER_REFUSED: u8 = 1;
const ANSWER_HELD: u8 = 2;
const ANSWER_BYTES: u8 = 3;

/// Write a chunk's identity
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `chunk` - The chunk
fn put_chunk(out: &mut Vec<u8>, chunk: &ChunkId) {
    out.extend_from_slice(&chunk.consumer.to_le_bytes());
    out.extend_from_slice(&chunk.pg.tablet.to_le_bytes());
    out.extend_from_slice(&chunk.pg.sub.to_le_bytes());
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&chunk.object.to_le_bytes());
    out.extend_from_slice(&chunk.stripe.to_le_bytes());
    out.push(chunk.position);
    out.extend_from_slice(&[0u8; 7]);
}

/// Write a label
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `label` - The label
fn put_label(out: &mut Vec<u8>, label: Label) {
    out.extend_from_slice(&label.sequence.to_le_bytes());
    out.extend_from_slice(&label.tag.to_le_bytes());
}

/// Write a count of labels and the labels
///
/// # Arguments
///
/// * `out` - Where to write them
/// * `labels` - The labels
fn put_labels(out: &mut Vec<u8>, labels: &[Label]) {
    out.extend_from_slice(&(labels.len() as u16).to_le_bytes());
    out.extend_from_slice(&[0u8; 6]);
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
fn put_units(out: &mut Vec<u8>, units: &[UnitBytes]) {
    out.extend_from_slice(&(units.len() as u32).to_le_bytes());
    out.extend_from_slice(&[0u8; 4]);
    for unit in units {
        out.extend_from_slice(&unit.index.to_le_bytes());
        out.extend_from_slice(&[0u8; 4]);
        out.extend_from_slice(&unit.crc.to_le_bytes());
        out.extend_from_slice(&unit.bytes);
    }
}

/// Write a piece's fields after its chunk
///
/// # Arguments
///
/// * `out` - Where to write it
/// * `piece` - The piece
fn put_piece(out: &mut Vec<u8>, piece: &Piece) {
    out.extend_from_slice(&piece.stream.to_le_bytes());
    put_chunk(out, &piece.chunk);
    put_label(out, piece.label);
    out.extend_from_slice(&piece.chunk_bytes.to_le_bytes());
    out.extend_from_slice(&piece.unit_bytes.to_le_bytes());
    out.extend_from_slice(&(piece.crcs.len() as u32).to_le_bytes());
    out.extend_from_slice(&piece.length.to_le_bytes());
    out.extend_from_slice(&piece.offset.to_le_bytes());
    out.push(u8::from(piece.last));
    out.extend_from_slice(&[0u8; 7]);
    for crc in &piece.crcs {
        out.extend_from_slice(&crc.to_le_bytes());
    }
    out.extend_from_slice(&piece.bytes);
}

impl ChunkRequest {
    /// The kind this request is
    #[must_use]
    pub const fn kind(&self) -> ChunkKind {
        match self {
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

    /// The chunk this request names
    #[must_use]
    pub const fn chunk(&self) -> &ChunkId {
        match self {
            ChunkRequest::Write(piece) | ChunkRequest::Stage(piece) => &piece.chunk,
            ChunkRequest::StageUnits { chunk, .. }
            | ChunkRequest::Apply { chunk, .. }
            | ChunkRequest::Discard { chunk, .. }
            | ChunkRequest::Confirm { chunk, .. }
            | ChunkRequest::Fold { chunk, .. }
            | ChunkRequest::Read { chunk, .. } => chunk,
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
        // the head: the correlation id and the kind
        out.extend_from_slice(&id.to_le_bytes());
        out.push(self.kind() as u8);
        out.extend_from_slice(&[0u8; 7]);
        // then the kind's own fields
        match self {
            ChunkRequest::Write(piece) | ChunkRequest::Stage(piece) => put_piece(&mut out, piece),
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
                put_chunk(&mut out, chunk);
                put_label(&mut out, *makes);
                out.extend_from_slice(&length.to_le_bytes());
                out.extend_from_slice(&unit_bytes.to_le_bytes());
                out.extend_from_slice(&[0u8; 4]);
                put_labels(&mut out, expects);
                put_units(&mut out, units);
            }
            ChunkRequest::Apply { chunk, label } | ChunkRequest::Discard { chunk, label } => {
                put_chunk(&mut out, chunk);
                put_label(&mut out, *label);
            }
            ChunkRequest::Confirm { chunk, labels } => {
                put_chunk(&mut out, chunk);
                put_labels(&mut out, labels);
            }
            ChunkRequest::Read {
                chunk,
                offset,
                len,
                labels,
            } => {
                put_chunk(&mut out, chunk);
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&len.to_le_bytes());
                put_labels(&mut out, labels);
            }
        }
        out
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
        let mut reader = Reader::new(body);
        // the head
        let id = reader.u64()?;
        let kind = ChunkKind::from_byte(reader.u8()?)?;
        reader.skip(7)?;
        let request = match kind {
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
            ChunkKind::Read => ChunkRequest::Read {
                chunk: reader.chunk()?,
                offset: reader.u64()?,
                len: reader.u64()?,
                labels: reader.labels()?,
            },
        };
        // and nothing past the request
        reader.finish()?;
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
                out.push(ANSWER_BYTES);
                out.extend_from_slice(&[0u8; 7]);
                put_label(&mut out, *label);
                out.extend_from_slice(bytes);
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

    /// Read an answer from its head and the rest of its body, read apart
    ///
    /// Returns the correlation id and the answer. A link reads a frame's head into a buffer of
    /// its own and the rest into another, so a read's bytes are never copied to join them.
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
                bytes: reader.rest().to_vec(),
            },
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
}

impl<'a> Reader<'a> {
    /// A cursor at a body's start
    ///
    /// # Arguments
    ///
    /// * `raw` - The body
    const fn new(raw: &'a [u8]) -> Self {
        Reader { raw, at: 0 }
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
            let bytes = self.take(unit_bytes as usize)?.to_vec();
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
        self.skip(7)?;
        // the checksums, then exactly the units they cover
        if crcs.saturating_mul(8) > self.raw.len() - self.at {
            return Err(ProtocolError::MalformedObject(
                "an object lane piece names more checksums than it carries",
            ));
        }
        let crcs: Vec<u64> = (0..crcs).map(|_| self.u64()).collect::<Result<_, _>>()?;
        let bytes = self.rest().to_vec();
        if unit_bytes == 0 || bytes.len() != crcs.len() * unit_bytes as usize {
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
        ChunkKind, ChunkReply, ChunkRequest, Piece, StoreCode, UnitBytes, CHUNK_HEAD_LEN,
        CHUNK_ID_LEN,
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
            bytes: vec![index as u8; 4],
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
            crcs: vec![5, 6],
            bytes: vec![1; 8],
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
        ];
        for request in requests {
            let body = request.encode_body(77);
            assert_eq!(ChunkRequest::decode(&body).unwrap(), (77, request.clone()));
            // every body starts with the head, and its kind's byte
            assert_eq!(body[8], request.kind() as u8);
            assert!(body.len() >= CHUNK_HEAD_LEN + CHUNK_ID_LEN);
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
                bytes: vec![9; 33],
            },
        ];
        for answer in answers {
            let body = answer.encode_body(5);
            assert_eq!(ChunkReply::decode(&body).unwrap(), (5, answer));
        }
        // a code's retry is its meaning's
        assert!(StoreCode::JournalFull.retryable());
        assert!(!StoreCode::CannotMake.retryable());
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
        body[8] = ChunkKind::Read as u8 + 1;
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
