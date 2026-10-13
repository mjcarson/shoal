//! The device store: stripe chunks in a slice's directory, staged, applied and read
//!
//! A slice does three things to a stripe chunk under S7's write path: it **stages** an update
//! durably without disturbing the chunk, **applies** a staged update once the stripe's row says it
//! committed, and **reads** a range at a label a reader names, so that a crash at any instant leaves
//! the old bytes or the new ones, verifiably
//! ([S6](../../../../../docs/src/object-storage/device-store.md)). X6 chose its shape on the lab's
//! SSDs: a file a chunk, written into a file the slice wrote ahead; part of a chunk journalled and
//! applied in place; no clone; XFS preferred, ext4 accepted, btrfs and tmpfs refused
//! ([Q22](../../../../../docs/src/object-storage/contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04)).
//!
//! # Invariants
//!
//! The page's, which every operation here keeps:
//!
//! - A stage is durable before it is reported, holds new values only, and needs no new space.
//! - A chunk changes in place only by applying a staged write, and the staged copy is dropped only
//!   after that apply is durable.
//! - No unit that fails its checksum is returned, merged into, or used as a source.
//! - A read and an apply of one chunk never overlap.
//! - A slice's files are touched by one executor, the one that owns it.
//! - Every loop yields in steps of [`STEP`] bytes, on a queue with a latency goal.

pub mod chunk;
pub mod free;
pub mod journal;
pub mod record;
pub mod slice;
pub mod sync;

use std::path::Path;
use std::rc::Rc;

use glommio::io::DmaFile;

use crate::shared::objects::Label;

pub use chunk::{ChunkHeader, ChunkId, BLOCK};
pub use slice::{ReadAnswer, Slice, SliceOptions, StageUnits, StageWhole, WholeWriter};

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
