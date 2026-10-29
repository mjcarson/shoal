//! The device store: stripe chunks in a slice's directory, staged, applied and read
//!
//! A slice does three things to a stripe chunk under S7's write path: it **stages** an update
//! durably without disturbing the chunk, **applies** a staged update once the stripe's row says it
//! committed, and **reads** a range at a label a reader names, so that a crash at any instant leaves
//! the old bytes or the new ones, verifiably
//! ([S6](../../../../../docs/src/object-storage/device-store.md)). X6 chose its shape on the lab's
//! SSDs: a file a chunk, ~~written into a file the slice wrote ahead~~ since
//! [F96](../../../../../docs/src/features/chunk-file-classes.md) a file of the chunk's class made for
//! it and synced by itself; part of a chunk journalled and applied in place; no clone; XFS
//! preferred, ext4 accepted, btrfs and tmpfs refused
//! ([Q22](../../../../../docs/src/object-storage/contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04)).
//!
//! # Invariants
//!
//! The page's, which every operation here keeps:
//!
//! - A stage is durable before it is reported, holds new values only, and ~~needs no new space~~
//!   takes any new space it needs before it is reported, so an apply needs none (F96).
//! - A chunk changes in place only by applying a staged write, and the staged copy is dropped only
//!   after that apply is durable.
//! - No unit that fails its checksum is returned, merged into, or used as a source.
//! - A read and an apply of one chunk never overlap.
//! - A slice's files are touched by one executor, the one that owns it.
//! - Every loop yields in steps of [`STEP`] bytes, on a queue with a latency goal.

pub mod applier;
pub mod cache;
pub mod chunk;
pub mod free;
pub mod journal;
pub mod record;
pub mod slice;
pub mod sync;

use std::path::Path;
use std::rc::Rc;

use glommio::io::{DmaFile, OwnedDmaBuffer, ReadResult};

use crate::shared::objects::Label;

pub use chunk::{ChunkHeader, ChunkId, BLOCK};
pub use slice::{ReadAnswer, Slice, SliceOptions, StageParity, StageUnits, StageWhole, Verdict, WholeWriter};

/// The most input an object loop takes before it offers its executor back
///
/// X9 found a step of 64 KiB under a 100 µs goal held a table beside it where a step of 1 MiB did
/// not ([X9](../../../../../docs/src/object-storage/table-latency.md#4-steps-inside-a-unit-the-supplement)).
pub const STEP: usize = 64 << 10;

/// The bytes one write of a file's zeros or a chunk's units takes
const PIECE: usize = 1 << 20;

/// Why a slice refused or failed an operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The device under the slice failed; every slice on it refuses from now on
    DeviceFailed {
        /// What failed
        reason: String,
    },
    /// A stage expects a label the slice cannot make from its chunk or the records beneath
    CannotMake {
        /// The label the stage expects, none for a chunk not there yet
        expects: Option<Label>,
        /// The label the chunk carries, none when there is no chunk
        have: Option<Label>,
    },
    /// An apply or a discard of a label no record on the slice makes
    NotStaged {
        /// The label
        label: Label,
    },
    /// No space for a stage without breaching the device's reserve
    NoSpace {
        /// The device's free bytes
        free: u64,
        /// What it keeps
        reserve: u64,
    },
    /// Every journal segment holds a record not yet applied; retry once applies catch up
    JournalFull,
    /// A unit fails its checksum and no staged record can write it again
    Corrupt {
        /// The unit
        unit: u32,
    },
    /// The executor's memory budget had no room for the operation's buffers; retry later
    Budget {
        /// The bytes it needed
        needed: u64,
        /// The executor's whole budget
        limit: u64,
    },
    /// A slice this node does not serve, or no longer does
    UnknownSlice(crate::shared::devices::SliceId),
    /// A request no slice can serve
    Invalid(String),
    /// A reclamation whose seal lapsed or was taken by another, so nothing was removed (M20)
    NotSealed,
    /// A write of what a reclamation removed moments ago (item 253)
    Removed,
}

impl std::fmt::Display for StoreError {
    /// Write the refusal for a person
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::DeviceFailed { reason } => write!(f, "the device failed: {reason}"),
            StoreError::CannotMake { expects, have } => write!(
                f,
                "the stage expects {expects:?} and the slice holds {have:?}, from which it cannot make it"
            ),
            StoreError::NotStaged { label } => write!(f, "no record on the slice makes {label:?}"),
            StoreError::NoSpace { free, reserve } => write!(
                f,
                "the device has {free} bytes free and keeps {reserve}; a stage would breach its reserve"
            ),
            StoreError::JournalFull => write!(
                f,
                "every journal segment holds a record not yet applied; retry once applies catch up"
            ),
            StoreError::Corrupt { unit } => write!(
                f,
                "unit {unit} fails its checksum and no staged record can write it again"
            ),
            StoreError::Budget { needed, limit } => write!(
                f,
                "object work had no room for {needed} bytes of buffers in its {limit} byte budget; retry"
            ),
            StoreError::UnknownSlice(slice) => write!(f, "slice {slice} is not served by this node"),
            StoreError::Invalid(why) => write!(f, "{why}"),
            StoreError::NotSealed => write!(
                f,
                "the seal this reclamation names lapsed or was taken by another; nothing was removed"
            ),
            StoreError::Removed => write!(
                f,
                "a reclamation removed this chunk's object or group from the slice moments ago; a late write of it is refused"
            ),
        }
    }
}

impl std::error::Error for StoreError {}

impl StoreError {
    /// Whether a caller may retry: a full journal or a full device may drain
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            StoreError::JournalFull | StoreError::NoSpace { .. } | StoreError::Budget { .. }
        )
    }
}

/// An I/O error as the device failing, naming what was done
///
/// # Arguments
///
/// * `what` - What was being done
/// * `error` - What the os said
pub(crate) fn failed(what: &str, error: impl std::fmt::Display) -> StoreError {
    StoreError::DeviceFailed {
        reason: format!("{what}: {error}"),
    }
}

/// Open a file for direct reads and writes, creating it when asked
///
/// # Arguments
///
/// * `path` - The file
/// * `create` - Whether to create it
pub(crate) async fn open(path: &Path, create: bool) -> Result<Rc<DmaFile>, StoreError> {
    let mut options = glommio::io::OpenOptions::new();
    options.read(true).write(true).create(create);
    options
        .dma_open(path)
        .await
        .map(Rc::new)
        .map_err(|error| failed(&format!("opening {}", path.display()), error))
}

/// Close a file now, rather than when the reactor next turns
///
/// A dropped glommio file is closed on the executor's next I/O cycle, and one dropped as its
/// executor stops is never closed at all, so every file the store is done with on its way out is
/// closed this way. A file another holder still references is closed by the last.
///
/// # Arguments
///
/// * `file` - The file
pub(crate) async fn close(file: Rc<DmaFile>) {
    // a failure to close leaves nothing the store reads again; the bytes were synced or not before
    let _ = file.close_rc().await;
}

/// Write bytes at an aligned offset of a file, in pieces, every byte landing
///
/// # Arguments
///
/// * `file` - The file
/// * `bytes` - The bytes, a whole number of blocks
/// * `at` - Where, a whole number of blocks in
pub(crate) async fn write_all(file: &DmaFile, bytes: &[u8], at: u64) -> Result<(), StoreError> {
    let mut written = 0usize;
    for piece in bytes.chunks(PIECE) {
        let mut buffer = file.alloc_dma_buffer(piece.len());
        buffer.as_bytes_mut().copy_from_slice(piece);
        let landed = file
            .write_at(buffer, at + written as u64)
            .await
            .map_err(|error| failed(&format!("writing {:?}", file.path()), error))?;
        if landed != piece.len() {
            return Err(failed(
                &format!("writing {:?}", file.path()),
                format!("{landed} of {} bytes landed", piece.len()),
            ));
        }
        written += piece.len();
        glommio::yield_if_needed().await;
    }
    Ok(())
}

/// Read an aligned range of a file whole
///
/// # Arguments
///
/// * `file` - The file
/// * `at` - Where, a whole number of blocks in
/// * `len` - How many bytes, a whole number of blocks
pub(crate) async fn read_exact(file: &DmaFile, at: u64, len: usize) -> Result<Vec<u8>, StoreError> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let want = (len - bytes.len()).min(PIECE);
        let read = file
            .read_at_aligned(at + bytes.len() as u64, want)
            .await
            .map_err(|error| failed(&format!("reading {:?}", file.path()), error))?;
        if read.is_empty() {
            return Err(failed(
                &format!("reading {:?}", file.path()),
                format!("it ended {} bytes short", len - bytes.len()),
            ));
        }
        bytes.extend_from_slice(&read[..read.len().min(want)]);
    }
    Ok(bytes)
}

/// Read an aligned range of a file in one read, left in the buffer the device put it in, for a
/// check that reads its bytes where they lie and keeps none of them
/// ([F110](../../../../../docs/src/features/store-checks-in-place.md))
///
/// # Arguments
///
/// * `file` - The file
/// * `at` - Where, a whole number of blocks in
/// * `len` - How many bytes, a whole number of blocks
pub(crate) async fn read_in_place(
    file: &DmaFile,
    at: u64,
    len: usize,
) -> Result<ReadResult, StoreError> {
    let read = file
        .read_at_aligned(at, len)
        .await
        .map_err(|error| failed(&format!("reading {:?}", file.path()), error))?;
    // a short read is a file that ended early
    if read.len() < len {
        return Err(failed(
            &format!("reading {:?}", file.path()),
            format!("it ended {} bytes short", len - read.len()),
        ));
    }
    // cut to what was asked, since a device may answer past it
    ReadResult::slice(&read, 0, len).ok_or_else(|| {
        failed(&format!("reading {:?}", file.path()), "a read could not be cut to its length")
    })
}

/// Read an aligned range of a file in one read into a buffer of the heap's, which may cross to
/// another thread once it is filled (F110)
///
/// A read whose answer crosses to a shard reads here, so its bytes cross in the buffer the device
/// filled. The ring's registered buffers cannot be lent to another thread, so such a read cannot
/// use them.
///
/// # Arguments
///
/// * `file` - The file
/// * `at` - Where, a whole number of blocks in
/// * `len` - How many bytes, a whole number of blocks
pub(crate) async fn read_run(
    file: &DmaFile,
    at: u64,
    len: usize,
) -> Result<OwnedDmaBuffer, StoreError> {
    let buffer = OwnedDmaBuffer::new(len).ok_or_else(|| {
        failed(&format!("reading {:?}", file.path()), format!("no buffer of {len} bytes"))
    })?;
    let read = file
        .read_into_at(buffer, at)
        .await
        .map_err(|error| failed(&format!("reading {:?}", file.path()), error))?;
    // a short read is a file that ended early
    if read.len() < len {
        return Err(failed(
            &format!("reading {:?}", file.path()),
            format!("it ended {} bytes short", len - read.len()),
        ));
    }
    Ok(read)
}

/// Write a read's buffer back at an aligned offset of a file as it is, every byte landing (F110)
///
/// An apply writes a record's units from the buffer the journal's device read them into, never a
/// copy of it.
///
/// # Arguments
///
/// * `file` - The file
/// * `bytes` - The bytes, a whole number of blocks where the device put them
/// * `at` - Where, a whole number of blocks in
pub(crate) async fn write_in_place(
    file: &DmaFile,
    bytes: ReadResult,
    at: u64,
) -> Result<(), StoreError> {
    let len = bytes.len();
    let landed = file
        .write_at(glommio::io::DmaBuffer::from(bytes), at)
        .await
        .map_err(|error| failed(&format!("writing {:?}", file.path()), error))?;
    if landed != len {
        return Err(failed(
            &format!("writing {:?}", file.path()),
            format!("{landed} of {len} bytes landed"),
        ));
    }
    Ok(())
}

/// How long a read's pieces are: the most whole units [`PIECE`] holds, at least one
///
/// # Arguments
///
/// * `unit` - A unit's length
#[must_use]
pub(crate) fn piece_bytes(unit: usize) -> usize {
    let unit = unit.max(1);
    (PIECE / unit).max(1) * unit
}

/// Read an aligned range of a file as pieces of whole units, every piece in flight at once and
/// each left in the buffer the device put it in (F105)
///
/// A piece is a whole number of units, so no unit straddles two and each is verified where it
/// lies. Every piece in flight at once is one span to a disk's queue: a rotational disk reaches
/// half its sequential rate only past 4 MiB a read
/// ([X7](../../../../../docs/src/object-storage/device-store-hdd.md),
/// [F91](../../../../../docs/src/features/rotational-devices.md)), and an SSD answers the pieces
/// together where a read of one unit at a time waited for each
/// ([F105](../../../../../docs/src/features/store-reads-in-place.md)).
///
/// # Arguments
///
/// * `file` - The file
/// * `at` - Where, a whole number of blocks in
/// * `len` - How many bytes, a whole number of units
/// * `unit` - A unit's length
pub(crate) async fn read_pieces(file: &DmaFile, at: u64, len: usize, unit: usize) -> Result<Vec<ReadResult>, StoreError> {
    // every piece asked for at once
    let piece = piece_bytes(unit);
    let pieces = (0..len.div_ceil(piece)).map(|index| {
        let from = index * piece;
        let want = (len - from).min(piece);
        async move {
            let read = file
                .read_at_aligned(at + from as u64, want)
                .await
                .map_err(|error| failed(&format!("reading {:?}", file.path()), error))?;
            // a short read is a file that ended early
            if read.len() < want {
                return Err(failed(
                    &format!("reading {:?}", file.path()),
                    format!("it ended {} bytes short", want - read.len()),
                ));
            }
            // cut to what was asked, since a device may answer past it
            ReadResult::slice(&read, 0, want).ok_or_else(|| {
                failed(&format!("reading {:?}", file.path()), "a piece could not be cut to its length")
            })
        }
    });
    futures::future::try_join_all(pieces).await
}

/// The pieces of a run cut to a range of it, each still the buffer the device put it in (F105)
///
/// # Arguments
///
/// * `run` - The pieces, in order
/// * `from` - Where the range starts in the run
/// * `to` - Where it ends
#[must_use]
pub(crate) fn cut(run: &[ReadResult], from: usize, to: usize) -> Vec<ReadResult> {
    let mut cut = Vec::with_capacity(run.len());
    let mut start = 0;
    for piece in run {
        // the part of this piece the range covers, if any
        let end = start + piece.len();
        let (lo, hi) = (from.max(start), to.min(end));
        if lo < hi {
            if let Some(part) = ReadResult::slice(piece, lo - start, hi - lo) {
                cut.push(part);
            }
        }
        start = end;
    }
    cut
}

/// A run of a chunk's units as a read left them: the device's buffers, each a piece of whole units,
/// or one buffer of the heap's that a read whose answer crosses to a shard filled (F110)
pub(crate) enum Run {
    /// Pieces in the device's buffers, each a whole number of units
    Pieces(Vec<ReadResult>),
    /// One buffer of the heap's
    Heap(OwnedDmaBuffer),
}

impl Run {
    /// A unit's bytes where they lie, by where the unit starts in the run
    ///
    /// # Arguments
    ///
    /// * `at` - Where the unit starts in the run
    /// * `piece` - How long a piece of the device's is
    /// * `len` - How long a unit is
    #[must_use]
    pub(crate) fn unit(&self, at: usize, piece: usize, len: usize) -> Option<&[u8]> {
        match self {
            Run::Pieces(pieces) => {
                // a piece is a whole number of units, so no unit straddles two
                let within = at % piece;
                pieces.get(at / piece)?.get(within..within + len)
            }
            Run::Heap(buffer) => buffer.as_bytes().get(at..at + len),
        }
    }

    /// The run cut to a range of it, held where the read left it
    ///
    /// # Arguments
    ///
    /// * `from` - Where the range starts in the run
    /// * `to` - Where it ends
    #[must_use]
    pub(crate) fn span(self, from: usize, to: usize) -> Span {
        match self {
            Run::Pieces(pieces) => Span::read(cut(&pieces, from, to)),
            Run::Heap(buffer) => Span {
                bytes: SpanBytes::Heap { buffer, from, to },
                _drawn: None,
            },
        }
    }
}

/// A buffer of the heap's a read filled, the range of it the read answers, and the read's draw,
/// given back once whoever the bytes crossed to drops them (F110)
struct HeapRange {
    /// The buffer
    buffer: OwnedDmaBuffer,
    /// Where the range starts in it
    from: usize,
    /// Where it ends
    to: usize,
    /// The read's draw on its executor's budget
    _drawn: Option<crate::server::objects::budget::Permit>,
}

impl AsRef<[u8]> for HeapRange {
    /// The range's bytes
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_bytes()[self.from..self.to]
    }
}

/// A range's bytes as a read made them: in the buffers the device put them in, every unit
/// verified there, or laid out once in a buffer of their own where any came from a record, from
/// memory or from nowhere ([F105](../../../../../docs/src/features/store-reads-in-place.md))
///
/// Not `Send`: a device's buffer belongs to the executor that read it, so a span never leaves it,
/// and an answer that crosses to another thread ~~copies it once ([`Span::into_vec`])~~ takes it
/// as [`Span::into_bytes`] gives it: the heap buffer a read whose answer crosses filled, handed
/// over whole, or a copy of the device's buffers (F110). It holds its read's draw on its
/// executor's budget until it is dropped, and a heap buffer's crosses with it.
pub(crate) struct Span {
    /// The bytes, where the read left them
    bytes: SpanBytes,
    /// The read's draw on its executor's budget, given back once the span is dropped
    _drawn: Option<crate::server::objects::budget::Permit>,
}

/// Where a span's bytes are
enum SpanBytes {
    /// A buffer of their own
    Owned(Vec<u8>),
    /// The device's buffers, in order, each cut to the range
    Read(Vec<ReadResult>),
    /// Slices of the units a cache holds, in order (F111)
    Cached(Vec<bytes::Bytes>),
    /// A buffer of the heap's a read whose answer crosses to a shard filled, and the range of it
    /// the read answers (F110)
    Heap {
        /// The buffer
        buffer: OwnedDmaBuffer,
        /// Where the range starts in it
        from: usize,
        /// Where it ends
        to: usize,
    },
}

impl Span {
    /// A span of bytes in a buffer of their own
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    #[must_use]
    pub(crate) fn owned(bytes: Vec<u8>) -> Self {
        Span {
            bytes: SpanBytes::Owned(bytes),
            _drawn: None,
        }
    }

    /// A span of the buffers the device put a range in, each cut to it
    ///
    /// # Arguments
    ///
    /// * `pieces` - The buffers, in order
    #[must_use]
    pub(crate) fn read(pieces: Vec<ReadResult>) -> Self {
        Span {
            bytes: SpanBytes::Read(pieces),
            _drawn: None,
        }
    }

    /// A span of the slices a cache answered a read with, never copied (F111)
    ///
    /// # Arguments
    ///
    /// * `parts` - The slices, in order
    #[must_use]
    pub(crate) fn cached(parts: Vec<bytes::Bytes>) -> Self {
        Span {
            bytes: SpanBytes::Cached(parts),
            _drawn: None,
        }
    }

    /// This span, holding a read's draw until it is dropped
    ///
    /// # Arguments
    ///
    /// * `drawn` - The read's draw, if its executor has a budget
    #[must_use]
    pub(crate) fn holding(mut self, drawn: Option<crate::server::objects::budget::Permit>) -> Self {
        self._drawn = drawn;
        self
    }

    /// How many bytes it holds
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        match &self.bytes {
            SpanBytes::Owned(bytes) => bytes.len(),
            SpanBytes::Read(pieces) => pieces.iter().map(|piece| piece.len()).sum(),
            SpanBytes::Cached(parts) => parts.iter().map(bytes::Bytes::len).sum(),
            SpanBytes::Heap { from, to, .. } => to - from,
        }
    }

    /// Whether its bytes are still where the device put them, never copied
    #[cfg(test)]
    #[must_use]
    pub(crate) fn in_place(&self) -> bool {
        matches!(self.bytes, SpanBytes::Read(_) | SpanBytes::Heap { .. })
    }

    /// Its bytes part by part, in order: one part where they are a buffer of their own
    #[must_use]
    pub(crate) fn parts(&self) -> Vec<&[u8]> {
        match &self.bytes {
            SpanBytes::Owned(bytes) => vec![bytes.as_slice()],
            SpanBytes::Read(pieces) => pieces.iter().map(|piece| &piece[..]).collect(),
            SpanBytes::Cached(parts) => parts.iter().map(|part| &part[..]).collect(),
            SpanBytes::Heap { buffer, from, to } => vec![&buffer.as_bytes()[*from..*to]],
        }
    }

    /// Its bytes in one buffer of their own: moved where they are one already, and otherwise
    /// copied a step at a time, yielding between steps as every long copy on an executor does
    pub(crate) async fn into_vec(self) -> Vec<u8> {
        let Span { bytes, _drawn } = self;
        let vec = match bytes {
            SpanBytes::Owned(bytes) => bytes,
            SpanBytes::Read(pieces) => {
                let mut out = Vec::with_capacity(pieces.iter().map(|piece| piece.len()).sum());
                for piece in &pieces {
                    for step in piece.chunks(STEP) {
                        out.extend_from_slice(step);
                        glommio::yield_if_needed().await;
                    }
                }
                out
            }
            SpanBytes::Cached(parts) => {
                let mut out = Vec::with_capacity(parts.iter().map(bytes::Bytes::len).sum());
                for part in &parts {
                    for step in part.chunks(STEP) {
                        out.extend_from_slice(step);
                        glommio::yield_if_needed().await;
                    }
                }
                out
            }
            SpanBytes::Heap { buffer, from, to } => {
                let mut out = Vec::with_capacity(to - from);
                for step in buffer.as_bytes()[from..to].chunks(STEP) {
                    out.extend_from_slice(step);
                    glommio::yield_if_needed().await;
                }
                out
            }
        };
        // the draw is given back once the copy no longer needs the device's buffers
        drop(_drawn);
        vec
    }

    /// Its bytes as an answer that crosses to another thread carries them (F110)
    ///
    /// The heap buffer a read whose answer crosses filled is handed over whole, its draw with it,
    /// given back once the bytes are dropped wherever they went. A buffer of their own is moved,
    /// a cache's one slice is handed over as it is (F111), and the device's buffers, which never
    /// leave their executor, and several of a cache's slices are copied once.
    pub(crate) async fn into_bytes(self) -> crate::shared::protocol::peer::chunk::ChunkBytes {
        use crate::shared::protocol::peer::chunk::ChunkBytes;
        let Span { bytes, _drawn } = self;
        match bytes {
            SpanBytes::Heap { buffer, from, to } => {
                let owner = HeapRange {
                    buffer,
                    from,
                    to,
                    _drawn,
                };
                ChunkBytes::Shared(bytes::Bytes::from_owner(owner))
            }
            SpanBytes::Cached(mut parts) if parts.len() == 1 => {
                ChunkBytes::Shared(parts.pop().unwrap_or_default())
            }
            bytes => ChunkBytes::Owned(Span { bytes, _drawn }.into_vec().await),
        }
    }
}

/// How many units a chunk's file has room for past its header block
///
/// A chunk is never extended in place: every unit a stage or an apply writes lies inside its file,
/// and a write that needs more is staged whole into a larger one
/// ([F96](../../../../../docs/src/features/chunk-file-classes.md)).
///
/// # Arguments
///
/// * `file` - The chunk's file
/// * `unit_bytes` - The bytes of a unit
pub(crate) async fn file_units(file: &DmaFile, unit_bytes: u32) -> Result<u32, StoreError> {
    let size = file
        .file_size()
        .await
        .map_err(|error| failed(&format!("reading the size of {:?}", file.path()), error))?;
    // the whole units past the header block
    let room = size.saturating_sub(chunk::BLOCK as u64) / u64::from(unit_bytes.max(1));
    Ok(u32::try_from(room).unwrap_or(u32::MAX))
}

/// Allocate a fresh chunk file's blocks ahead of its writes, so its writes land inside it
///
/// The allocation, and the unwritten extents its writes convert, are made durable by the file's
/// own `fdatasync` once every byte of it is written; nothing reads it as a chunk before then
/// ([F96](../../../../../docs/src/features/chunk-file-classes.md)). A filesystem that cannot
/// allocate ahead has the file grown by its writes instead, and a device with no room for it is
/// `NoSpace`, for the caller to say how full it is.
///
/// # Arguments
///
/// * `file` - The fresh file
/// * `len` - Its whole length
pub(crate) async fn allocate(file: &DmaFile, len: u64) -> Result<(), StoreError> {
    let Err(error) = file.pre_allocate(len, false).await else {
        return Ok(());
    };
    // what the kernel said, beneath glommio's wrapping
    let error = std::io::Error::from(error);
    match error.raw_os_error() {
        Some(libc::EOPNOTSUPP) => Ok(()),
        Some(libc::ENOSPC) => Err(StoreError::NoSpace { free: 0, reserve: 0 }),
        _ => Err(failed(&format!("allocating {:?}", file.path()), error)),
    }
}

/// Write zeros over a range of a file, in pieces, every byte landing
///
/// What a fresh chunk file's units past its bytes hold, so no extent of the file is left
/// unwritten and a later write in place stays inside written ones (F96).
///
/// # Arguments
///
/// * `file` - The file
/// * `at` - Where the zeros start, a whole number of blocks in
/// * `len` - How many, a whole number of blocks
pub(crate) async fn write_zeros(file: &DmaFile, at: u64, len: u64) -> Result<(), StoreError> {
    let mut written = 0u64;
    while written < len {
        let piece = (len - written).min(PIECE as u64);
        let mut buffer = file.alloc_dma_buffer(piece as usize);
        buffer.as_bytes_mut().fill(0);
        let landed = file
            .write_at(buffer, at + written)
            .await
            .map_err(|error| failed(&format!("writing zeros into {:?}", file.path()), error))?;
        if landed as u64 != piece {
            return Err(failed(&format!("writing zeros into {:?}", file.path()), format!("{landed} of {piece} bytes landed")));
        }
        written += piece;
        glommio::yield_if_needed().await;
    }
    Ok(())
}

/// Write zeros over a file's whole length and sync it: a file written ahead
///
/// Allocated ahead without its zeros, a file's overwrite commits its allocation as well as its
/// data, which X6 measured at no faster than appending, so the zeros are written
/// ([S6](../../../../../docs/src/object-storage/device-store.md#staging-two-cases)).
///
/// # Arguments
///
/// * `file` - The file
/// * `len` - Its length
pub(crate) async fn zero_fill(file: &DmaFile, len: u64) -> Result<(), StoreError> {
    let mut at = 0u64;
    while at < len {
        let piece = (len - at).min(PIECE as u64) as usize;
        let buffer = file.alloc_dma_buffer(piece);
        let mut buffer = buffer;
        buffer.as_bytes_mut().fill(0);
        file.write_at(buffer, at)
            .await
            .map_err(|error| failed(&format!("writing ahead {:?}", file.path()), error))?;
        at += piece as u64;
        glommio::yield_if_needed().await;
    }
    file.fdatasync()
        .await
        .map_err(|error| failed(&format!("syncing {:?}", file.path()), error))
}

/// Sync a directory, so the renames and creations under it are durable
///
/// # Arguments
///
/// * `dir` - The directory
pub(crate) async fn sync_dir(dir: &Path) -> Result<(), StoreError> {
    let directory = glommio::io::Directory::open(dir)
        .await
        .map_err(|error| failed(&format!("opening {}", dir.display()), error))?;
    directory
        .sync()
        .await
        .map_err(|error| failed(&format!("syncing {}", dir.display()), error))?;
    let _ = directory.close().await;
    Ok(())
}
