//! One slice of a device: its chunks, its journal and its pool, driven by the executor that owns it
//!
//! Everything a slice does is here: a whole chunk staged into a file of its pool, part of a chunk
//! staged into its journal, either applied once its row says it committed, discarded once a
//! committed fact excludes it, and a range read at a label, overlaid from the records a reader's
//! label says committed ([S6](../../../../../docs/src/object-storage/device-store.md)).
//!
//! # Invariants
//!
//! **A slice's files are touched by one executor**: a slice is opened on its owner and never
//! leaves it, which is what lets a read and an apply of one chunk take turns on a lock of that
//! executor's and no other.
//!
//! **An apply needs no new space**: part of a chunk is written in place over a chunk that exists,
//! and a whole chunk was written into ~~its pool's file~~ a file of its class, taken from the pool or
//! made for it, at its stage; a chunk's first write stages it whole, so an apply never makes a file.
//! And since [F96](../../../../../docs/src/features/chunk-file-classes.md) an apply never writes past
//! its file: a write that takes a chunk past its class is staged whole, and a stage of units past a
//! file is refused.
//!
//! **A record stays while a later one stands on it**: an apply of a record whose base another record
//! makes applies that one first, since a commit naming a label names the commit beneath it; nothing
//! here drops a record but its apply, its discard, or a start finding it applied.
//!
//! **No unit that fails its checksum is returned or written**: a read verifies every unit before a
//! byte of it is returned, and an apply verifies every unit of its record before it writes one.
//!
//! **A chunk that fails its own checksum is set aside, and kept until its replacement verifies**
//! ([F89](../../../../../docs/src/features/scrub-and-repair.md)). A read, an apply or a deep scrub
//! that finds a unit of a chunk's own file failing, or its header failing its own checksum, and no
//! record that can write it again, moves the chunk into `quarantine/`: from then on no read, no
//! confirmation and no rebuild is offered it, and its count goes against its device. A whole chunk
//! placed at its id is verified before the evidence is removed.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use glommio::io::DmaFile;
use glommio::sync::RwLock;
use glommio::TaskQueueHandle;
use tracing::{event, Level};

use super::applier::Applier;
use super::cache::UnitCache;
use super::chunk::{bound_crc, bytes_crc, unit_crc, ChunkHeader, ChunkId, HeaderError, BLOCK};
use super::free::FreePool;
use super::journal::{Journal, Location};
use super::record::{classify, Outcome, Record, RecordKind, MAX_ALSO, MAX_RECORD_UNITS};
use super::sync::Committer;
use super::{
    allocate, close, failed, file_units, open, piece_bytes, read_exact, read_in_place, read_pieces,
    read_run, sync_dir, write_all, write_in_place, write_zeros, zero_fill, Run, Span, StoreError,
    STEP,
};
use crate::server::devices::{take_from, DeviceState, SliceState};
use crate::shared::devices::ApplySync;
use crate::shared::objects::{class_units, Label};
use crate::shared::pools::MAX_CHUNK_UNITS;
use crate::shared::protocol::peer::chunk::ListedChunk;

/// What one open of a slice found
enum Opened {
    /// The slice, open and serving
    Ready(Rc<Slice>),
    /// Its journal held no record at another length than asked, and was removed to be made again
    Resize,
}

/// How a slice is kept
#[derive(Debug, Clone)]
pub struct SliceOptions {
    /// The bytes of one journal segment
    pub segment_bytes: u64,
    /// ~~The files written ahead it keeps for each chunk length~~ The files removed chunks give
    /// back it keeps of each length (F96)
    pub free_files: u32,
    /// The chunk lengths of the pools its device serves, each named to its pool at the open
    pub chunk_lengths: Vec<u64>,
    /// The budget its read and apply buffers are drawn from, its owning executor's
    pub budget: Option<Arc<crate::server::objects::budget::Budget>>,
    /// Whether a chunk's state before its last apply is kept, for a reader one write behind
    /// ([F86](../../../../../docs/src/features/replicated-pools.md)); off only to price it
    pub keep_previous: bool,
    /// The queue its applies run on, below its foreground, or the current one (S13, F91)
    pub apply_queue: Option<TaskQueueHandle>,
}

/// How long a chunk's previous state is kept when no apply replaces it first
///
/// A reader one write behind asks within its bundle's deadline; past this nobody is.
pub const PREVIOUS_FOR: std::time::Duration = std::time::Duration::from_secs(30);

/// The bytes of a chunk's state before its last apply
enum PreviousBytes {
    /// A whole chunk's file, linked aside before the apply renamed another over it, whose own
    /// header is read only when a reader asks for it
    File(PathBuf),
    /// The units a partial apply overwrote, held in memory under a draw from the budget
    Units {
        /// The header the chunk had: its label, length and every unit's checksum
        header: ChunkHeader,
        /// Each overwritten unit's bytes before the apply
        units: BTreeMap<u32, Vec<u8>>,
        /// The draw they are held under, given back when they are dropped
        _permit: Option<crate::server::objects::budget::Permit>,
    },
}

/// A chunk's state before its last apply, kept until its next or for [`PREVIOUS_FOR`]
///
/// X1 found readers of a stripe being written failing by name without it: a reader that read
/// its row before a commit asks a holder that applied since, and is served the label it named
/// rather than sent to read its row again ([X1](../../../../../docs/src/object-storage/stripe-model.md)).
struct Previous {
    /// Where its bytes are
    bytes: PreviousBytes,
    /// When it was kept
    kept: std::time::Instant,
}

/// A whole chunk to stage
#[derive(Debug, Clone)]
pub struct StageWhole {
    /// The chunk
    pub id: ChunkId,
    /// The label the write makes
    pub label: Label,
    /// The bytes of the whole chunk
    pub chunk_bytes: u64,
    /// The bytes of a unit
    pub unit_bytes: u32,
    /// How many of its bytes hold data
    pub length: u64,
    /// Its bytes, `length` of them
    pub bytes: Vec<u8>,
}

/// A parity chunk's stage of a change to some data chunks: folded into its parity at the label it
/// expects, and staged as the new values (F90)
#[derive(Debug, Clone)]
pub struct StageParity {
    /// The parity chunk
    pub id: ChunkId,
    /// The label the write makes
    pub makes: Label,
    /// The label the change was computed against, which the parity is read at
    pub expects: Label,
    /// How many of the chunk's bytes hold data once it applies
    pub length: u64,
    /// The stripe's data chunks, `k`
    pub data: u8,
    /// The stripe's parity chunks, `m`
    pub parity: u8,
    /// Each data chunk's change: its position, and each unit's old bytes XOR its new
    pub changes: Vec<(u8, Vec<(u32, Vec<u8>)>)>,
}

/// Part of a chunk to stage: whole units' new values over a label the slice can make
#[derive(Debug, Clone)]
pub struct StageUnits {
    /// The chunk, which a whole chunk was staged as first
    pub id: ChunkId,
    /// The label the write makes
    pub makes: Label,
    /// The label it was computed against
    pub expects: Label,
    /// Labels it may be laid over instead, tried in order after `expects`: those pending bytes
    /// fold from into it, when the stage carries every pending unit with its own
    /// ([F86](../../../../../docs/src/features/replicated-pools.md))
    pub also: Vec<Label>,
    /// How many of the chunk's bytes hold data once it applies, at least what they held
    pub length: u64,
    /// Each unit's index and its new bytes, a whole unit each
    pub units: Vec<(u32, Vec<u8>)>,
}

/// What a verification of some of a chunk's units found (F89)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every unit asked for verified at the label, each with the CRC-64/NVME of its bytes alone,
    /// zero for a unit that holds none
    Verified {
        /// The label the chunk is at
        label: Label,
        /// Each unit's checksum, unbound, in order from the first asked for
        crcs: Vec<u64>,
        /// The units' summary, the XOR of their bytes in 4 KiB blocks where they lie, when one
        /// was asked for: a parity check's (F90)
        summary: Option<Vec<u8>>,
    },
    /// Units failed their checksums, or the header its own, and nothing could write them again:
    /// the chunk is in quarantine now, or was found there
    Corrupt {
        /// The label the chunk was at
        label: Label,
        /// The units that failed; none for a header, or a chunk found in quarantine already
        units: Vec<u32>,
    },
    /// The chunk's file is not at the label: the label asked for when records make it and the
    /// file is not there yet, else the label its file carries, none for no chunk
    Held(Option<Label>),
}

/// A chunk set aside for failing a checksum: where its file is, and the label its header carried
#[derive(Debug, Clone)]
struct Quarantined {
    /// Its file, under the slice's `quarantine/`
    path: PathBuf,
    /// The label its header carried, the default for a header that did not read
    label: Label,
}

/// What a read answers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAnswer {
    /// The range's bytes, every unit of them verified
    Bytes(Vec<u8>),
    /// The slice cannot make the label asked for; this is the label its chunk carries
    Label(Option<Label>),
}

/// What a read found, on the executor that read it ([F105](../../../../../docs/src/features/store-reads-in-place.md))
pub(crate) enum Found {
    /// The range's bytes, every unit of them verified, where the read left them
    Bytes(Span),
    /// The slice cannot make the label asked for; this is the label its chunk carries
    Label(Option<Label>),
}

/// Where one unit of a read's range comes from (F105)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// No bytes under the label's header: zeros
    Zeros,
    /// A chunk's previous state, kept in memory
    Held,
    /// A record laid over the base
    Laid,
    /// The base's file
    Base,
}

/// A unit of a run of pieces, where the device put it (F105)
///
/// # Arguments
///
/// * `run` - The pieces, each a whole number of units
/// * `at` - Where the unit starts in the run
/// * `piece` - How long every piece but the last is
/// * `len` - A unit's length

/// A record the slice holds, and where it is in the journal
#[derive(Debug, Clone)]
struct Staged {
    /// The record
    record: Record,
    /// Where it is
    location: Location,
    /// When it was staged here, or when the slice opened for one a start recovered
    at: std::time::Instant,
    /// Whether a start recovered it, so nobody is waiting on it and its row decides it at once
    recovered: bool,
}

/// A whole chunk being written into a file of its slice's pool, in order, a piece at a time
///
/// Bytes leave a coordinator as they arrive from its client, so a holder takes a whole chunk in
/// pieces: each piece is written where it falls in the file, its units' checksums bound to their
/// place, and the chunk is staged or placed once its last piece is in
/// ([F86](../../../../../docs/src/features/replicated-pools.md)). A writer not finished is
/// aborted, which gives a file of the pool back, or removes one made for it (F96).
pub struct WholeWriter {
    /// The slice it writes into
    slice: Rc<Slice>,
    /// The chunk's header as it will be written, its unit checksums filled as pieces arrive
    header: ChunkHeader,
    /// The length of its file
    file_bytes: u64,
    /// The number of that file
    number: u64,
    /// The file, open
    file: Rc<DmaFile>,
    /// Where the next piece starts in the chunk
    next: u64,
    /// Whether its file was made for it, under a temporary name, rather than taken from the pool:
    /// it is synced by itself and renamed whole, never given back half written (F96)
    fresh: bool,
}

impl WholeWriter {
    /// Record in the chunk's header how much of its stripe holds bytes (F90)
    ///
    /// # Arguments
    ///
    /// * `stripe_length` - The stripe's length, zero where the writer does not know it
    pub fn record_stripe_length(&mut self, stripe_length: u64) {
        self.header.stripe_length = stripe_length;
    }

    /// Record in the chunk's header the key its consumer records its owner under (M20)
    ///
    /// # Arguments
    ///
    /// * `owner` - The key, zero where the writer does not know it
    pub fn record_owner(&mut self, owner: u64) {
        self.header.owner = owner;
    }
}

impl Staged {
    /// The bytes it stages, for the slice's figures
    fn bytes(&self) -> u64 {
        match &self.record.kind {
            RecordKind::Units { units, header } => units.len() as u64 * u64::from(header.unit_bytes),
            RecordKind::Whole { file_bytes, .. } => file_bytes.saturating_sub(BLOCK as u64),
        }
    }
}

/// What a chunk's file holds
enum ChunkState {
    /// No file
    Missing,
    /// A file whose header does not read
    Torn,
    /// A file and its header
    At(ChunkHeader, Rc<DmaFile>),
}

/// Where a label's bytes come from: a base and the records laid over it
struct Chain {
    /// The header the label's chunk has
    header: ChunkHeader,
    /// The base: the chunk's file, or a staged whole chunk's, and its header
    base: Option<(Rc<DmaFile>, ChunkHeader)>,
    /// The records over the base, oldest first
    overlays: Vec<Staged>,
    /// Units held in memory over everything else: a chunk's previous state's
    held: BTreeMap<u32, Vec<u8>>,
}

/// One slice, opened on the executor that owns it
pub struct Slice {
    /// The device it is on, which a failure marks
    device: Arc<DeviceState>,
    /// Its figures and directory
    state: Arc<SliceState>,
    /// How it is kept
    options: SliceOptions,
    /// Its journal
    journal: Journal,
    /// Its pool of files removed chunks gave back, where fresh files are made (F96)
    free: FreePool,
    /// The committer of the file a shared flush syncs
    flush: Rc<Committer>,
    /// The records it holds, by chunk, oldest first
    staged: RefCell<BTreeMap<ChunkId, Vec<Staged>>>,
    /// Each chunk's lock, so a read and an apply never overlap
    locks: RefCell<HashMap<ChunkId, Rc<RwLock<()>>>>,
    /// The queue its background work runs on: the count of its chunks
    background: Option<TaskQueueHandle>,
    /// Each chunk's state before its last apply, for a reader one write behind (F86)
    previous: RefCell<HashMap<ChunkId, Previous>>,
    /// When kept states past [`PREVIOUS_FOR`] were last let go
    previous_swept: Cell<std::time::Instant>,
    /// The chunks set aside for failing a checksum, each kept until a replacement verifies (F89)
    quarantine: RefCell<BTreeMap<ChunkId, Vec<Quarantined>>>,
    /// A rotational slice's applier, which issues its applies a batch at a time (F91)
    applier: RefCell<Option<Applier>>,
    /// The one arm a test stands in for a rotational slice's disk ([`faults::slow_arm`])
    ///
    /// [`faults::slow_arm`]: crate::server::objects::faults::slow_arm
    arm: glommio::sync::Semaphore,
    /// The seals a reclaimer set, each until it reclaims, unseals or lapses (M20)
    seals: RefCell<HashMap<crate::shared::protocol::peer::chunk::SealScope, reclaim::Seal>>,
    /// What a reclamation removed, each until a seal's lapse after it, whose late writes are refused
    /// (item 253)
    removed: RefCell<HashMap<crate::shared::protocol::peer::chunk::SealScope, std::time::Instant>>,
    /// Its executor's cache of the units its reads verified, when the node keeps one (F102)
    cache: std::cell::OnceCell<Rc<UnitCache>>,
}

impl Slice {
    /// Open a slice on the executor that owns it: its journal replayed, its records recovered
    ///
    /// # Arguments
    ///
    /// * `device` - The device it is on
    /// * `state` - Its figures and directory
    /// * `options` - How it is kept
    /// * `background` - The queue its background work runs on, or the current one
    ///
    /// # Errors
    ///
    /// When its files cannot be made or read, which fails its device.
    pub async fn open(
        device: Arc<DeviceState>,
        state: Arc<SliceState>,
        options: SliceOptions,
        background: Option<TaskQueueHandle>,
    ) -> Result<Rc<Slice>, StoreError> {
        let opened = Self::open_inner(device.clone(), state, options, background).await;
        if let Err(StoreError::DeviceFailed { reason }) = &opened {
            device.fail(reason.clone());
        }
        opened
    }

    /// Open a slice, failing nothing itself
    ///
    /// A journal found at another length than its device asks is resized once a start leaves it
    /// holding no record, by closing the slice, removing its segments and opening it again; until
    /// then it keeps the length it has, and says so (F91).
    ///
    /// # Arguments
    ///
    /// * `device` - The device it is on
    /// * `state` - Its figures and directory
    /// * `options` - How it is kept
    /// * `background` - The queue its background work runs on
    async fn open_inner(
        device: Arc<DeviceState>,
        state: Arc<SliceState>,
        options: SliceOptions,
        background: Option<TaskQueueHandle>,
    ) -> Result<Rc<Slice>, StoreError> {
        match Self::open_once(device.clone(), state.clone(), options.clone(), background, true).await? {
            Opened::Ready(slice) => Ok(slice),
            // its journal held nothing and was removed: made again at the length asked
            Opened::Resize => match Self::open_once(device, state, options, background, false).await? {
                Opened::Ready(slice) => Ok(slice),
                Opened::Resize => Err(StoreError::Invalid(
                    "a journal made again at the length asked was found at another".to_string(),
                )),
            },
        }
    }

    /// Open a slice once, answering that its journal is to be made again at the length asked when
    /// it holds no record at another
    ///
    /// # Arguments
    ///
    /// * `device` - The device it is on
    /// * `state` - Its figures and directory
    /// * `options` - How it is kept
    /// * `background` - The queue its background work runs on
    /// * `resize` - Whether a journal of another length holding no record is to be made again
    async fn open_once(
        device: Arc<DeviceState>,
        state: Arc<SliceState>,
        options: SliceOptions,
        background: Option<TaskQueueHandle>,
        resize: bool,
    ) -> Result<Opened, StoreError> {
        if let Some(reason) = device.failed() {
            return Err(StoreError::DeviceFailed { reason });
        }
        let dir = state.path.clone();
        crate::server::faults::guard(&dir).map_err(|error| failed("opening the slice", error))?;
        for sub in ["chunks", "quarantine"] {
            std::fs::create_dir_all(dir.join(sub)).map_err(|error| failed("making the slice", error))?;
        }
        // its journal, under it or on an SSD of the node, whose directory the claim made (F91)
        let journal_dir = state.journal.clone();
        crate::server::faults::guard(&journal_dir).map_err(|error| failed("opening the slice's journal", error))?;
        std::fs::create_dir_all(&journal_dir).map_err(|error| failed("making the slice's journal", error))?;
        // the chunks a run before this one set aside, which are evidence until replaced (F89)
        let quarantine = quarantined_under(&dir.join("quarantine"));
        state.quarantined.store(
            quarantine.values().map(|list| list.len() as u64).sum(),
            Ordering::Relaxed,
        );
        // a previous state is a reader's convenience and never outlives its start (F86)
        let previous = dir.join("previous");
        if previous.exists() {
            std::fs::remove_dir_all(&previous).map_err(|error| failed("clearing previous states", error))?;
        }
        std::fs::create_dir_all(&previous).map_err(|error| failed("making the slice", error))?;
        sync_dir(&dir).await?;
        // the file a shared flush syncs, written ahead so a sync of it writes nothing of its own
        let flush_path = dir.join("flush");
        if std::fs::metadata(&flush_path).map(|meta| meta.len()).unwrap_or(0) != BLOCK as u64 {
            let file = open(&flush_path, true).await?;
            zero_fill(&file, BLOCK as u64).await?;
            close(file).await;
            sync_dir(&dir).await?;
        }
        let flush = Committer::start(open(&flush_path, false).await?);
        // the journal, and every record a start finds in it
        let (journal, found) = Journal::open(state.id, &journal_dir, options.segment_bytes).await?;
        let held: BTreeSet<(u64, u64)> = found
            .iter()
            .filter_map(|(record, _)| match record.kind {
                RecordKind::Whole { file_bytes, file } => Some((file_bytes, file)),
                RecordKind::Units { .. } => None,
            })
            .collect();
        let free = FreePool::open(&dir.join("free"), options.free_files, &held)?;
        for length in &options.chunk_lengths {
            free.serve(BLOCK as u64 + length);
        }
        let slice = Rc::new(Slice {
            device,
            state,
            options,
            journal,
            free,
            flush,
            staged: RefCell::new(BTreeMap::new()),
            locks: RefCell::new(HashMap::new()),
            background,
            previous: RefCell::new(HashMap::new()),
            previous_swept: Cell::new(std::time::Instant::now()),
            quarantine: RefCell::new(quarantine),
            applier: RefCell::new(None),
            arm: glommio::sync::Semaphore::new(1),
            seals: RefCell::new(HashMap::new()),
            removed: RefCell::new(HashMap::new()),
            cache: std::cell::OnceCell::new(),
        });
        slice.recover(found).await?;
        // a journal at another length than asked: made again once it holds nothing, kept until then
        if slice.journal.segment_bytes() != slice.options.segment_bytes {
            if resize && slice.journal.live() == 0 {
                slice.close().await;
                drop(slice);
                for index in 0..crate::shared::devices::JOURNAL_SEGMENTS {
                    let segment = journal_dir.join(format!("segment-{index}"));
                    if segment.exists() {
                        std::fs::remove_file(&segment).map_err(|error| failed("removing a journal segment", error))?;
                    }
                }
                sync_dir(&journal_dir).await?;
                return Ok(Opened::Resize);
            }
            tracing::event!(
                tracing::Level::WARN,
                slice = %slice.state.id,
                journal = %journal_dir.display(),
                kept = slice.journal.segment_bytes(),
                asked = slice.options.segment_bytes,
                msg = "a slice's journal holds records at another segment length than its device asks; \
                       it keeps the length it has until a start finds it holding none",
            );
        }
        slice.count_chunks();
        // a disk's applies wait for their batch, on the apply queue (F91)
        if slice.device.rotational() {
            let applier = Applier::start(&slice, slice.options.apply_queue);
            *slice.applier.borrow_mut() = Some(applier);
        }
        Ok(Opened::Ready(slice))
    }

    /// Mark the device failed when an operation found it failing, and pass the error on
    ///
    /// # Arguments
    ///
    /// * `result` - What the operation came to
    fn guard<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        if let Err(StoreError::DeviceFailed { reason }) = &result {
            self.device.fail(reason.clone());
        }
        result
    }

    /// Draw an operation's buffers from the owning executor's budget, if it has one
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    async fn draw(
        &self,
        bytes: u64,
    ) -> Result<Option<crate::server::objects::budget::Permit>, StoreError> {
        match &self.options.budget {
            Some(budget) => budget
                .draw(bytes, crate::server::objects::budget::DRAW_WAIT)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// Refuse if the device has failed
    fn serving(&self) -> Result<(), StoreError> {
        match self.device.failed() {
            Some(reason) => Err(StoreError::DeviceFailed { reason }),
            None => Ok(()),
        }
    }

    /// The lock of one chunk
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    fn lock_of(&self, id: &ChunkId) -> Rc<RwLock<()>> {
        let mut locks = self.locks.borrow_mut();
        // a lock nobody holds is let go, so the table holds only chunks in use
        locks.retain(|_, lock| Rc::strong_count(lock) > 1);
        locks
            .entry(*id)
            .or_insert_with(|| Rc::new(RwLock::new(())))
            .clone()
    }

    /// Where a chunk's file is
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    #[must_use]
    pub fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        self.state.path.join("chunks").join(id.relative())
    }

    /// What a chunk's file holds
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    async fn chunk_state(&self, id: &ChunkId) -> Result<ChunkState, StoreError> {
        let path = self.chunk_path(id);
        if !path.exists() {
            return Ok(ChunkState::Missing);
        }
        let file = open(&path, false).await?;
        let block = read_in_place(&file, 0, BLOCK).await?;
        match ChunkHeader::decode(&block) {
            Ok(header) if header.id == *id => Ok(ChunkState::At(header, file)),
            Ok(_) | Err(HeaderError::Checksum | HeaderError::NotAHeader) => Ok(ChunkState::Torn),
            Err(error) => Err(StoreError::Invalid(format!(
                "chunk {} has a header this build does not read: {error:?}",
                path.display()
            ))),
        }
    }

    /// Whether every unit of some held under a header verifies in a file
    ///
    /// # Arguments
    ///
    /// * `file` - The chunk's file
    /// * `header` - Its header
    /// * `units` - The units
    async fn verifies(&self, file: &DmaFile, header: &ChunkHeader, units: &[u32]) -> Result<bool, StoreError> {
        for unit in units {
            // a unit holding no bytes under the header has nothing to verify
            if !header.holds(*unit) {
                continue;
            }
            // checked where the device put it (F110)
            let bytes =
                read_in_place(file, header.offset_of(*unit), header.unit_bytes as usize).await?;
            if unit_crc(&bytes, &header.id, *unit).await != header.crcs[*unit as usize] {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Recover the records a start found: drop the applied, apply the torn again, keep the rest
    ///
    /// # Arguments
    ///
    /// * `found` - Every record the journal found, and where
    async fn recover(&self, found: Vec<(Record, Location)>) -> Result<(), StoreError> {
        let mut by_chunk: BTreeMap<ChunkId, Vec<Staged>> = BTreeMap::new();
        for (record, location) in found {
            by_chunk.entry(record.id).or_default().push(Staged {
                record,
                location,
                at: std::time::Instant::now(),
                recovered: true,
            });
        }
        for (id, mut list) in by_chunk {
            list.sort_by_key(|staged| staged.record.seq);
            // a header that does not read was an apply's: the oldest record's header restores it
            if let ChunkState::Torn = self.chunk_state(&id).await? {
                if let Some(first) = list.iter().find(|staged| matches!(staged.record.kind, RecordKind::Units { .. })) {
                    self.write_units(first).await?;
                }
            }
            let mut kept: Vec<Staged> = Vec::new();
            for staged in list {
                let state = self.chunk_state(&id).await?;
                let keep = match &staged.record.kind {
                    RecordKind::Units { units, .. } => {
                        let (chunk, covered) = match &state {
                            ChunkState::At(header, file) => (Some(header.label), self.verifies(file, header, units).await?),
                            ChunkState::Missing => (None, true),
                            ChunkState::Torn => (None, false),
                        };
                        let beneath = kept.iter().any(|other| staged.record.stands_on(other.record.makes));
                        match classify(
                            staged.record.makes,
                            staged.record.expects,
                            &staged.record.also,
                            chunk,
                            covered,
                            beneath,
                        ) {
                            Outcome::Applied => false,
                            Outcome::Torn => {
                                self.write_units(&staged).await?;
                                false
                            }
                            Outcome::Staged | Outcome::Inert => true,
                        }
                    }
                    RecordKind::Whole { file_bytes, file } => {
                        // applied: renamed into place and whole there
                        let applied = match &state {
                            ChunkState::At(header, chunk) if header.label == staged.record.makes => {
                                let all: Vec<u32> = (0..header.units()).collect();
                                self.verifies(chunk, header, &all).await?
                            }
                            _ => false,
                        };
                        if applied {
                            false
                        } else if self.whole_holds(&staged.record, *file_bytes, *file).await? {
                            true
                        } else {
                            // never acknowledged: its file goes back to the pool, if it is there at
                            // all, so no number the pool hands out names no file
                            if self.free.path(*file_bytes, *file).exists() {
                                self.free.give_back(*file_bytes, *file);
                            }
                            false
                        }
                    }
                };
                if keep {
                    self.state.staged_bytes.fetch_add(staged.bytes(), Ordering::Relaxed);
                    kept.push(staged);
                } else {
                    self.journal.release(staged.location);
                }
            }
            if !kept.is_empty() {
                self.staged.borrow_mut().insert(id, kept);
            }
        }
        Ok(())
    }

    /// Whether a whole chunk's record's file holds the chunk it names, whole and verified
    ///
    /// # Arguments
    ///
    /// * `record` - The record
    /// * `file_bytes` - Its file's length
    /// * `number` - Its file's number
    async fn whole_holds(&self, record: &Record, file_bytes: u64, number: u64) -> Result<bool, StoreError> {
        let path = self.free.path(file_bytes, number);
        if !path.exists() {
            return Ok(false);
        }
        let file = open(&path, false).await?;
        let block = read_in_place(&file, 0, BLOCK).await?;
        let Ok(header) = ChunkHeader::decode(&block) else {
            return Ok(false);
        };
        if header.id != record.id || header.label != record.makes {
            return Ok(false);
        }
        let all: Vec<u32> = (0..header.units()).collect();
        let holds = self.verifies(&file, &header, &all).await;
        close(file).await;
        holds
    }

    /// Count the chunks the slice holds, once, at its open, for its report
    ///
    /// The count lists every directory under `chunks/`, which X6 timed at 37.5 s cold for a
    /// million chunks, so it runs on the executor's blocking pool in the background and never on
    /// the reactor: a start serves at once and its report's figure catches up. Recovery never
    /// reads it. A chunk applied while the count lists its directory may be counted twice, until
    /// the slice opens again; it is a figure an operator reads, not one anything decides by.
    fn count_chunks(self: &Rc<Self>) {
        let path = self.state.path.join("chunks");
        let state = self.state.clone();
        let counting = async move {
            let (count, bytes) = glommio::executor().spawn_blocking(move || chunks_under(&path)).await;
            state.chunks.fetch_add(count, Ordering::Relaxed);
            state.chunk_bytes.fetch_add(bytes, Ordering::Relaxed);
        };
        // on the background queue, below every foreground read and stage
        let _ = match self.background {
            Some(queue) => glommio::spawn_local_into(counting, queue).map(glommio::Task::detach),
            None => Ok::<_, glommio::GlommioError<()>>(glommio::spawn_local(counting).detach()),
        };
    }

    /// Let a whole chunk's file go: back to the pool when it was the pool's, removed when it was made
    /// for the chunk and never whole (F96)
    ///
    /// # Arguments
    ///
    /// * `file_bytes` - The file's length
    /// * `number` - The file's number
    /// * `fresh` - Whether it was made for the chunk, under its temporary name
    fn release_whole(&self, file_bytes: u64, number: u64, fresh: bool) {
        match fresh {
            // half written or never synced: no pool may take it
            true => {
                let _ = std::fs::remove_file(self.free.temporary(file_bytes, number));
            }
            false => self.free.give_back(file_bytes, number),
        }
    }

    /// Whether the device has room for a file of a length above its reserve
    ///
    /// # Arguments
    ///
    /// * `length` - The file's length
    fn has_room(&self, length: u64) -> bool {
        let free = crate::server::control::capacity::free_bytes(&self.state.path).unwrap_or(0);
        free > self.device.reserve.saturating_add(length)
    }

    /// Validate a chunk's geometry
    ///
    /// # Arguments
    ///
    /// * `chunk_bytes` - The bytes of the chunk
    /// * `unit_bytes` - The bytes of a unit
    fn check_geometry(chunk_bytes: u64, unit_bytes: u32) -> Result<(), StoreError> {
        if unit_bytes == 0
            || !(unit_bytes as usize).is_multiple_of(BLOCK)
            || !chunk_bytes.is_multiple_of(u64::from(unit_bytes))
            || chunk_bytes / u64::from(unit_bytes) > MAX_CHUNK_UNITS
        {
            return Err(StoreError::Invalid(format!(
                "a chunk of {chunk_bytes} bytes in units of {unit_bytes} is no chunk a slice holds"
            )));
        }
        Ok(())
    }

    /// Stage a whole chunk: written into a file of the pool, durable before this answers
    ///
    /// Staging the same write twice is staging it once.
    ///
    /// # Arguments
    ///
    /// * `stage` - The chunk and its bytes
    ///
    /// # Errors
    ///
    /// A failed device, a pool empty past the device's reserve, a full journal, or a request no
    /// slice serves.
    pub async fn stage_whole(self: &Rc<Self>, stage: StageWhole) -> Result<(), StoreError> {
        let staged = self.stage_whole_inner(stage).await;
        self.guard(staged)
    }

    /// Stage a whole chunk, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `stage` - The chunk and its bytes
    async fn stage_whole_inner(self: &Rc<Self>, stage: StageWhole) -> Result<(), StoreError> {
        // the same write staged again is staged
        if self.staged_making(&stage.id, stage.label).is_some() {
            return Ok(());
        }
        self.write_whole(stage, false).await
    }

    /// Write a whole chunk straight into place: a put's chunk, which no row names until its
    /// object's entry commits, so it needs no record ([S7](../../../../../docs/src/object-storage/write-path.md#a-whole-object-in-one-commit))
    ///
    /// Durable before this answers: written into a file of the pool, synced (or covered by the
    /// device's shared flush), then renamed over the chunk's path.
    ///
    /// # Arguments
    ///
    /// * `stage` - The chunk and its bytes
    ///
    /// # Errors
    ///
    /// A failed device, a pool empty past the device's reserve, or a request no slice serves.
    pub async fn place_whole(self: &Rc<Self>, stage: StageWhole) -> Result<(), StoreError> {
        let placed = self.write_whole(stage, true).await;
        self.guard(placed)
    }

    /// Write a whole chunk held in memory through a writer, staged or placed
    ///
    /// # Arguments
    ///
    /// * `stage` - The chunk and its bytes
    /// * `place` - Whether it goes straight into place rather than being staged
    async fn write_whole(self: &Rc<Self>, stage: StageWhole, place: bool) -> Result<(), StoreError> {
        if stage.length > stage.chunk_bytes || stage.bytes.len() as u64 != stage.length {
            return Err(StoreError::Invalid(format!(
                "a whole chunk of {} bytes holding {} carries {}",
                stage.chunk_bytes,
                stage.length,
                stage.bytes.len()
            )));
        }
        // a file of its class once every member sizes files so, as a piece's chunk is (F96)
        let classed = crate::server::objects::lane::current().is_none_or(|lanes| lanes.classed());
        let mut writer = self
            .begin_whole(stage.id, stage.label, stage.chunk_bytes, stage.unit_bytes, stage.length, classed)
            .await?;
        // every unit holding bytes, the last padded with zeros, in pieces of a step at most
        let unit = stage.unit_bytes as usize;
        let mut padded = stage.bytes;
        padded.resize(padded.len().div_ceil(unit) * unit, 0);
        let per_piece = (STEP / unit).max(1) * unit;
        let mut offset = 0u64;
        for piece in padded.chunks(per_piece) {
            if let Err(error) = writer.piece(offset, &[], piece).await {
                writer.abort().await;
                return Err(error);
            }
            offset += piece.len() as u64;
        }
        writer.finish(place).await
    }

    /// Begin a whole chunk, to be written in pieces into a file of the pool
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label it is written under
    /// * `chunk_bytes` - The bytes of the whole chunk
    /// * `unit_bytes` - The bytes of a unit
    /// * `length` - How many of its bytes hold data
    /// * `classed` - Whether its file is sized to the class of units it holds rather than to the
    ///   whole chunk, which a holder does once its cluster has activated every member's doing so
    ///   ([F96](../../../../../docs/src/features/chunk-file-classes.md))
    ///
    /// # Errors
    ///
    /// A failed device, a device without room above its reserve for a fresh file, or a geometry
    /// no slice serves.
    pub async fn begin_whole(
        self: &Rc<Self>,
        id: ChunkId,
        label: Label,
        chunk_bytes: u64,
        unit_bytes: u32,
        length: u64,
        classed: bool,
    ) -> Result<WholeWriter, StoreError> {
        self.serving()?;
        Self::check_geometry(chunk_bytes, unit_bytes)?;
        if length > chunk_bytes {
            return Err(StoreError::Invalid(format!(
                "a whole chunk of {chunk_bytes} bytes cannot hold {length}"
            )));
        }
        // its file holds the class of units its bytes need, a power of two of them, or the whole
        // chunk until its cluster sizes files by class (F96)
        let unit = u64::from(unit_bytes);
        let units = u32::try_from(chunk_bytes / unit).unwrap_or(u32::MAX);
        let file_bytes = match classed {
            true => BLOCK as u64 + u64::from(class_units(length, unit_bytes, units)) * unit,
            false => BLOCK as u64 + chunk_bytes,
        };
        // a file a removed chunk gave back, written whole, or one made now for this chunk alone
        // while the device has room above its reserve: nothing is written ahead (F96)
        self.free.serve(file_bytes);
        if let Some(number) = self.free.take(file_bytes) {
            let file = match open(&self.free.path(file_bytes, number), false).await {
                Ok(file) => file,
                Err(error) => {
                    self.free.give_back(file_bytes, number);
                    return Err(self.guard::<()>(Err(error)).unwrap_err());
                }
            };
            return Ok(self.whole_writer(id, label, chunk_bytes, unit_bytes, length, file_bytes, number, file, false));
        }
        let full = || StoreError::NoSpace {
            free: crate::server::control::capacity::free_bytes(&self.state.path).unwrap_or(0),
            reserve: self.device.reserve,
        };
        if !self.has_room(file_bytes) {
            return Err(full());
        }
        // a fresh file under a temporary name, its blocks allocated before its writes
        let (number, temporary) = self.guard(self.free.fresh(file_bytes).await)?;
        let file = self.guard(open(&temporary, true).await)?;
        if let Err(error) = allocate(&file, file_bytes).await {
            close(file).await;
            let _ = std::fs::remove_file(&temporary);
            return match error {
                StoreError::NoSpace { .. } => Err(full()),
                other => Err(self.guard::<()>(Err(other)).unwrap_err()),
            };
        }
        Ok(self.whole_writer(id, label, chunk_bytes, unit_bytes, length, file_bytes, number, file, true))
    }

    /// A writer of a whole chunk into a file it was given
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label it is written under
    /// * `chunk_bytes` - The bytes of the whole chunk
    /// * `unit_bytes` - The bytes of a unit
    /// * `length` - How many of its bytes hold data
    /// * `file_bytes` - The file's length
    /// * `number` - The file's number
    /// * `file` - The file, open
    /// * `fresh` - Whether the file was made for this chunk rather than taken from the pool
    #[allow(clippy::too_many_arguments)]
    fn whole_writer(
        self: &Rc<Self>,
        id: ChunkId,
        label: Label,
        chunk_bytes: u64,
        unit_bytes: u32,
        length: u64,
        file_bytes: u64,
        number: u64,
        file: Rc<DmaFile>,
        fresh: bool,
    ) -> WholeWriter {
        let mut header = ChunkHeader::empty(id, chunk_bytes, unit_bytes);
        header.label = label;
        header.length = length;
        WholeWriter {
            slice: self.clone(),
            header,
            file_bytes,
            number,
            file,
            next: 0,
            fresh,
        }
    }

    /// The record of a chunk that makes a label, if the slice holds one
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label
    fn staged_making(&self, id: &ChunkId, label: Label) -> Option<Staged> {
        self.staged
            .borrow()
            .get(id)?
            .iter()
            .find(|staged| staged.record.makes == label)
            .cloned()
    }

    /// Where the bytes of a chunk at a label come from, if the slice can make it
    ///
    /// The chunk's own file at that label; a staged whole chunk at it; or either with the records
    /// that chain from its label to the one wanted laid over it. A record stands on the first label
    /// it may be laid over that the slice can make, so the walk tries each in turn and goes back
    /// from one that leads nowhere. It is a loop over the records on its path, never a call nested
    /// for each, and the chunk's own header is read once (item 313).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label
    /// * `depth` - How many records deep the search begins, to bound it
    async fn chain(
        &self,
        id: &ChunkId,
        label: Label,
        depth: usize,
    ) -> Result<Option<Chain>, StoreError> {
        // the chunk's own label and file, read once for the whole walk
        let chunk = match self.chunk_state(id).await? {
            ChunkState::At(header, file) => Some((header, file)),
            _ => None,
        };
        // the records on the walk's path, from the label asked for down, each with the labels it
        // may be laid over and how many of them were tried
        let mut path: Vec<(Staged, Vec<Label>, usize)> = Vec::new();
        let mut current = label;
        loop {
            // a label past the bound leads nowhere, as one no record makes does
            let carried = chunk.as_ref().filter(|(header, _)| header.label == current);
            let base = if depth + path.len() > MAX_RECORD_UNITS {
                None
            } else if let Some((header, file)) = carried {
                // the chunk itself at the label
                Some(Chain {
                    header: header.clone(),
                    base: Some((file.clone(), header.clone())),
                    overlays: Vec::new(),
                    held: BTreeMap::new(),
                })
            } else {
                match self.staged_making(id, current) {
                    None => None,
                    Some(staged) => match &staged.record.kind {
                        // a staged whole chunk stands alone
                        RecordKind::Whole { file_bytes, file } => {
                            let path = self.free.path(*file_bytes, *file);
                            let file = open(&path, false).await?;
                            let block = read_in_place(&file, 0, BLOCK).await?;
                            let header = ChunkHeader::decode(&block).map_err(|error| {
                                StoreError::Invalid(format!(
                                    "a staged chunk's header does not read: {error:?}"
                                ))
                            })?;
                            Some(Chain {
                                header: header.clone(),
                                base: Some((file, header)),
                                overlays: Vec::new(),
                                held: BTreeMap::new(),
                            })
                        }
                        // part of a chunk goes on the path, and the walk tries what it stands on
                        RecordKind::Units { .. } => {
                            let bases: Vec<Label> = staged.record.bases().collect();
                            path.push((staged, bases, 0));
                            None
                        }
                    },
                }
            };
            // a base found: every record on the path laid over it, the deepest first, the chain's
            // header the one the label asked for makes
            if let Some(mut chain) = base {
                for (staged, _, _) in path.into_iter().rev() {
                    if let RecordKind::Units { header, .. } = &staged.record.kind {
                        chain.header = header.clone();
                    }
                    chain.overlays.push(staged);
                }
                return Ok(Some(chain));
            }
            // the next label to try: the deepest record's next base, going back past records with
            // none left
            loop {
                let Some((_, bases, tried)) = path.last_mut() else {
                    return Ok(None);
                };
                if let Some(next) = bases.get(*tried) {
                    current = *next;
                    *tried += 1;
                    break;
                }
                path.pop();
            }
        }
    }

    /// Stage part of a chunk: whole units' new values over a label the slice can make
    ///
    /// Units between what the chunk held and the new length that the write does not carry are
    /// staged as zeros. Staging the same write twice is staging it once while the slice can make
    /// its label; a new stage of a label whose record lost its base replaces the record.
    ///
    /// # Arguments
    ///
    /// * `stage` - The units and the labels
    ///
    /// # Errors
    ///
    /// A failed device, a label the slice cannot make, a full journal, or a request no slice serves.
    pub async fn stage_units(&self, stage: StageUnits) -> Result<(), StoreError> {
        let staged = self.stage_units_inner(stage).await;
        self.guard(staged)
    }

    /// Stage a parity chunk's new values from a change to some data chunks (F90)
    ///
    /// The holder reads its own parity units the change reaches at the label it expects, each
    /// verified before it is merged into (P15), folds `c · change` into each, and stages the result
    /// as any stage of units is staged: the new values in its journal, never the change, so a
    /// record replayed is the same parity. A chunk not at the label expected refuses it
    /// `CannotMake`, and one whose unit fails its checksum is set aside and refuses it `Corrupt`.
    ///
    /// # Arguments
    ///
    /// * `stage` - The parity chunk, its labels and the change
    ///
    /// # Errors
    ///
    /// A failed device, a full journal, a base the slice cannot make, a corrupt unit, or a change no
    /// parity chunk of this stripe takes.
    pub async fn stage_parity(self: &Rc<Self>, stage: StageParity) -> Result<(), StoreError> {
        let staged = self.stage_parity_inner(stage).await;
        self.guard(staged)
    }

    /// Stage a parity chunk's new values from a change, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `stage` - The parity chunk, its labels and the change
    async fn stage_parity_inner(self: &Rc<Self>, stage: StageParity) -> Result<(), StoreError> {
        self.serving()?;
        // a parity position of the stripe, and changes of its data chunks
        let position = stage.id.position;
        if position < stage.data
            || position >= stage.data.saturating_add(stage.parity)
            || stage.changes.iter().any(|(data, _)| *data >= stage.data)
        {
            return Err(StoreError::Invalid(format!(
                "a change of {} data chunks staged on position {position} of a {}+{} stripe is none a parity chunk takes",
                stage.changes.len(),
                stage.data,
                stage.parity
            )));
        }
        // the same write again, over the base it was computed against, is staged already: a
        // change folded twice is no parity at all
        if let Some(existing) = self.staged_making(&stage.id, stage.makes) {
            if existing.record.expects == Some(stage.expects) {
                return Ok(());
            }
        }
        // the base, judged under the chunk's lock as every stage's is: an apply of the label
        // expected drops its record and rewrites the chunk under the write lock, and a chain read
        // between the two finds neither
        let header = {
            let lock = self.lock_of(&stage.id);
            let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
            let Some(header) = self.chain(&stage.id, stage.expects, 0).await?.map(|chain| chain.header) else {
                let have = match self.chunk_state(&stage.id).await? {
                    ChunkState::At(header, _) => Some(header.label),
                    _ => None,
                };
                return Err(StoreError::CannotMake {
                    expects: Some(stage.expects),
                    have,
                });
            };
            header
        };
        // every unit the change reaches, read at the label expected and verified, each read under
        // the lock again: an apply between them leaves the bytes at that label as they were
        let unit = u64::from(header.unit_bytes);
        let mut indexes: Vec<u32> = stage
            .changes
            .iter()
            .flat_map(|(_, units)| units.iter().map(|(index, _)| *index))
            .collect();
        indexes.sort_unstable();
        indexes.dedup();
        let mut units: Vec<(u32, Vec<u8>)> = Vec::with_capacity(indexes.len());
        for index in indexes {
            if index >= header.units() {
                return Err(StoreError::Invalid(format!(
                    "unit {index} is past the {} units of a chunk",
                    header.units()
                )));
            }
            let (_, read) = self
                .read_inner(
                    stage.id,
                    u64::from(index) * unit,
                    unit,
                    &[stage.expects],
                    true,
                    false,
                    false,
                )
                .await?;
            // the unit copied once out of where the device put it (F105)
            let mut bytes = match read {
                Found::Bytes(span) => span.into_vec().await,
                Found::Label(have) => {
                    return Err(StoreError::CannotMake {
                        expects: Some(stage.expects),
                        have,
                    })
                }
            };
            bytes.resize(unit as usize, 0);
            units.push((index, bytes));
        }
        // each data chunk's change folded into the units it reaches, on this executor
        let row = position - stage.data;
        for (data, changes) in &stage.changes {
            for (index, change) in changes {
                let Some((_, bytes)) = units.iter_mut().find(|(at, _)| at == index) else {
                    continue;
                };
                if change.len() != bytes.len() {
                    return Err(StoreError::Invalid(format!("a change of unit {index} is not a whole unit")));
                }
                crate::server::objects::codec::fold(stage.data, stage.parity, row, *data, change, bytes)
                    .await
                    .map_err(|error| StoreError::Invalid(error.to_string()))?;
            }
        }
        // and staged as their new values, over the label expected
        self.stage_units_inner(StageUnits {
            id: stage.id,
            makes: stage.makes,
            expects: stage.expects,
            also: Vec::new(),
            length: stage.length,
            units,
        })
        .await
    }

    /// Stage part of a chunk, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `stage` - The units and the labels
    async fn stage_units_inner(&self, mut stage: StageUnits) -> Result<(), StoreError> {
        self.serving()?;
        let lock = self.lock_of(&stage.id);
        let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
        // the base: what the first label it may be laid over holds, from the chunk or the
        // records beneath; a stage carrying every pending unit may be laid over any of their
        // sources (F86)
        let mut found = None;
        for expects in std::iter::once(stage.expects).chain(stage.also.iter().copied()) {
            if let Some(chain) = self.chain(&stage.id, expects, 0).await? {
                found = Some((expects, chain));
                break;
            }
        }
        let Some((expects, base)) = found else {
            let have = match self.chunk_state(&stage.id).await? {
                ChunkState::At(header, _) => Some(header.label),
                _ => None,
            };
            return Err(StoreError::CannotMake {
                expects: Some(stage.expects),
                have,
            });
        };
        // every other label it may be laid over stays one: the chunk can reach another of them
        // before this applies, as a fold applied after this was staged does (item 239)
        let mut also: Vec<Label> = std::iter::once(stage.expects)
            .chain(stage.also.iter().copied())
            .filter(|label| *label != expects)
            .collect();
        also.sort_by_key(|label| std::cmp::Reverse(label.sequence));
        also.dedup();
        also.truncate(MAX_ALSO);
        stage.expects = expects;
        // the same write again, over a base the slice still makes, is staged
        if let Some(existing) = self.staged_making(&stage.id, stage.makes) {
            if existing.record.expects == Some(stage.expects) {
                return Ok(());
            }
        }
        let header = &base.header;
        let unit = header.unit_bytes;
        stage.units.sort_by_key(|(index, _)| *index);
        stage.units.dedup_by_key(|(index, _)| *index);
        let length = stage.length.max(header.length).min(header.chunk_bytes);
        for (index, bytes) in &stage.units {
            if *index >= header.units() || bytes.len() != unit as usize {
                return Err(StoreError::Invalid(format!(
                    "unit {index} of {} bytes is no whole unit of a chunk of {} units of {unit}",
                    bytes.len(),
                    header.units()
                )));
            }
            if u64::from(*index) * u64::from(unit) >= length {
                return Err(StoreError::Invalid(format!(
                    "unit {index} is past the {length} bytes the chunk holds once the write applies"
                )));
            }
        }
        // units the new length brings in and the write does not carry are zeros
        let held_end = u32::try_from(header.length.div_ceil(u64::from(unit))).unwrap_or(u32::MAX);
        let new_end = u32::try_from(length.div_ceil(u64::from(unit))).unwrap_or(u32::MAX);
        // a chunk is never extended in place: a write that takes it past its file is staged whole
        // by its writer, so a stage of units past the file it would apply into is refused by name
        // (F96)
        if let Some((file, _)) = &base.base {
            let room = file_units(file, unit).await?;
            if new_end > room {
                return Err(StoreError::Invalid(format!(
                    "a stage taking chunk {:?} to {length} bytes needs {new_end} units and its file \
                     has room for {room}: a write past a chunk's file is staged whole",
                    stage.id
                )));
            }
        }
        for gap in held_end..new_end {
            if stage.units.iter().all(|(index, _)| *index != gap) {
                stage.units.push((gap, vec![0; unit as usize]));
            }
        }
        stage.units.sort_by_key(|(index, _)| *index);
        if stage.units.len() > MAX_RECORD_UNITS {
            return Err(StoreError::Invalid(format!(
                "a write of {} units is past the {MAX_RECORD_UNITS} one record carries",
                stage.units.len()
            )));
        }
        // the header the chunk has once this applies
        let mut result = header.clone();
        result.label = stage.makes;
        result.length = length;
        for (index, bytes) in &stage.units {
            result.crcs[*index as usize] = unit_crc(bytes, &stage.id, *index).await;
        }
        let mut record = Record {
            seq: 0,
            id: stage.id,
            makes: stage.makes,
            expects: Some(stage.expects),
            also,
            kind: RecordKind::Units {
                units: stage.units.iter().map(|(index, _)| *index).collect(),
                header: result,
            },
        };
        let payload: Vec<&[u8]> = stage.units.iter().map(|(_, bytes)| bytes.as_slice()).collect();
        let location = self.journal.append(&mut record, &payload).await?;
        let staged = Staged {
            record,
            location,
            at: std::time::Instant::now(),
            recovered: false,
        };
        self.state.staged_bytes.fetch_add(staged.bytes(), Ordering::Relaxed);
        let mut by_chunk = self.staged.borrow_mut();
        let list = by_chunk.entry(stage.id).or_default();
        // a record of this label over a base it no longer stands on is replaced
        if let Some(position) = list.iter().position(|other| other.record.makes == stage.makes) {
            let old = list.remove(position);
            self.journal.release(old.location);
            self.state.staged_bytes.fetch_sub(old.bytes(), Ordering::Relaxed);
        }
        list.push(staged);
        Ok(())
    }

    /// Apply a staged write its row says committed, and drop its record once that is durable
    ///
    /// A record whose base another record makes applies that one first: a commit naming a label
    /// names the commit beneath it. Applying a label the chunk already carries, whole and verified,
    /// changes nothing. On a rotational device the apply waits for its batch (F91).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label the committed write makes
    ///
    /// # Errors
    ///
    /// A failed device, or a label no record makes and the chunk does not carry.
    pub async fn apply(self: &Rc<Self>, id: ChunkId, label: Label) -> Result<(), StoreError> {
        // a disk's apply waits for its batch, issued whole with the others, and runs as below (F91)
        let applier = self.applier.borrow().clone();
        match applier {
            Some(applier) => applier.apply(id, label).await,
            None => self.apply_now(id, label).await,
        }
    }

    /// Apply a staged write now, counted as an apply in flight on the slice until it is done
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label the committed write makes
    ///
    /// # Errors
    ///
    /// A failed device, or a label no record makes and the chunk does not carry.
    pub(super) async fn apply_now(self: &Rc<Self>, id: ChunkId, label: Label) -> Result<(), StoreError> {
        let _applying = self.state.applying();
        self.hold_arm().await;
        let applied = self.apply_inner(id, label).await;
        self.guard(applied)
    }

    /// The bytes an apply of a label draws from the budget: its record's units, none for a whole
    /// chunk's, which is renamed into place (F91)
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label its committed write makes
    #[must_use]
    pub(super) fn apply_bytes(&self, id: &ChunkId, label: Label) -> u64 {
        self.staged
            .borrow()
            .get(id)
            .and_then(|records| records.iter().find(|staged| staged.record.makes == label))
            .map_or(0, |staged| match &staged.record.kind {
                RecordKind::Units { units, header } => units.len() as u64 * u64::from(header.unit_bytes),
                RecordKind::Whole { .. } => 0,
            })
    }

    /// The bytes one batch of applies may draw at once: a quarter of the budget, so a batch never
    /// starves the reads beside it of buffers (F91)
    #[must_use]
    pub(super) fn batch_room(&self) -> u64 {
        self.options
            .budget
            .as_ref()
            .map_or(u64::MAX, |budget| budget.limit() / 4)
    }

    /// Hold the one arm a test stands in for a rotational slice's disk, if one does, for as long as
    /// the test says, in arrival order
    async fn hold_arm(&self) {
        if !self.device.rotational() {
            return;
        }
        if let Some(hold) = crate::server::objects::faults::arm_hold() {
            if let Ok(_arm) = self.arm.acquire_permit(1).await {
                glommio::timer::sleep(hold).await;
            }
        }
    }

    /// Apply a staged write, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label it makes
    async fn apply_inner(self: &Rc<Self>, id: ChunkId, label: Label) -> Result<(), StoreError> {
        self.serving()?;
        let lock = self.lock_of(&id);
        let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
        self.apply_locked(&id, label).await
    }

    /// Apply a staged write with its chunk's lock held, and every record beneath it first
    ///
    /// The records beneath it, those it stands on that the chunk does not carry yet, are found
    /// first and applied from the bottom in a loop. An apply that awaited the apply of the record
    /// beneath it nested a poll for each one, and a chain of a few hundred records a disk's
    /// batched applies left behind overflowed its executor's stack (item 313).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label it makes
    async fn apply_locked(&self, id: &ChunkId, label: Label) -> Result<(), StoreError> {
        // the chunk's label, which nothing changes until the first record below is applied
        let at = match self.chunk_state(id).await? {
            ChunkState::At(header, _) => Some(header.label),
            _ => None,
        };
        // the labels to apply, the one asked for first and each record beneath it after: the
        // chunk at any label a record may be laid over takes it (item 239), and otherwise the
        // record beneath one of them is applied before it
        let mut chain = vec![label];
        while chain.len() <= MAX_RECORD_UNITS {
            let top = chain[chain.len() - 1];
            let Some(staged) = self.staged_making(id, top) else {
                break;
            };
            let kind = (&staged.record.kind, staged.record.expects);
            let (RecordKind::Units { .. }, Some(expects)) = kind else {
                break;
            };
            let on_base = at.is_some_and(|at| staged.record.stands_on(at));
            if on_base || at == Some(top) {
                break;
            }
            let beneath = staged
                .record
                .bases()
                .find(|base| self.staged_making(id, *base).is_some());
            match beneath {
                Some(beneath) => chain.push(beneath),
                None if at.is_some() => {
                    return Err(StoreError::CannotMake {
                        expects: Some(expects),
                        have: at,
                    });
                }
                None => break,
            }
        }
        // the bottom first, each made durable before the one above it is written
        while let Some(next) = chain.pop() {
            self.apply_one(id, next).await?;
        }
        Ok(())
    }

    /// Apply one staged write over a chunk that carries what it stands on, with its chunk's lock
    /// held
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label it makes
    async fn apply_one(&self, id: &ChunkId, label: Label) -> Result<(), StoreError> {
        let Some(staged) = self.staged_making(id, label) else {
            // a label the chunk carries whole is applied already
            if let ChunkState::At(header, file) = self.chunk_state(id).await? {
                if header.label == label {
                    let all: Vec<u32> = (0..header.units()).collect();
                    if self.verifies(&file, &header, &all).await? {
                        return Ok(());
                    }
                }
            }
            return Err(StoreError::NotStaged { label });
        };
        match &staged.record.kind {
            RecordKind::Whole { file_bytes, file } => {
                // a whole chunk is never applied over a newer one: a rebuild may have made the
                // chunk a later label in place since this was staged (X1's
                // `whole_apply_older_than_the_chunk`, F88); nor over another label at its
                // sequence, which a row behind its chunks names a lost try by (F93)
                let newer = match self.chunk_state(id).await? {
                    ChunkState::At(header, _) => header.label.ahead_of(label),
                    _ => false,
                };
                if newer {
                    event!(Level::DEBUG, msg = "a staged whole chunk older than the chunk was dropped rather than applied", chunk = ?id, label = ?label);
                    self.free.give_back(*file_bytes, *file);
                } else {
                    self.apply_whole(id, *file_bytes, &self.free.path(*file_bytes, *file)).await?;
                }
            }
            RecordKind::Units { .. } => self.write_units(&staged).await?,
        }
        // a test can end the holder once the apply is durable and before its record goes
        crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::AppliedNotDropped);
        // durable, so the record goes, and every one it passed with it
        self.drop_record(id, label);
        self.drop_passed(id, label);
        Ok(())
    }

    /// Rename a staged whole chunk's file over its chunk, durably
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `file_bytes` - The file's length
    /// * `source` - Where the whole file is: the pool's, or a fresh one's temporary name (F96)
    async fn apply_whole(&self, id: &ChunkId, file_bytes: u64, source: &Path) -> Result<(), StoreError> {
        let target = self.chunk_path(id);
        let parent = target
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| StoreError::Invalid("a chunk's path has no parent".to_string()))?;
        // every directory down to the chunk's, each made durable in its parent
        let chunks = self.state.path.join("chunks");
        let mut made = Vec::new();
        let mut walk = parent.clone();
        while !walk.exists() && walk != chunks {
            made.push(walk.clone());
            walk = walk.parent().map(Path::to_path_buf).unwrap_or_else(|| chunks.clone());
        }
        for dir in made.iter().rev() {
            std::fs::create_dir(dir).or_else(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(failed("making a chunk's directory", error))
                }
            })?;
            if let Some(up) = dir.parent() {
                sync_dir(up).await?;
            }
        }
        let fresh = !target.exists();
        // the bytes the old file held, which the new one's take the place of in the figure (F96)
        let replaced = if fresh { 0 } else { std::fs::metadata(&target).map_or(0, |meta| meta.len()) };
        // the chunk as it was, linked aside before the rename replaces it, for a reader one write
        // behind; the rename itself stays the one atomic step (F86)
        let kept = match fresh {
            true => None,
            false => self.link_previous(id, &target).await,
        };
        glommio::io::rename(source, &target)
            .await
            .map_err(|error| failed("renaming a staged chunk into place", error))?;
        match kept {
            Some(path) => self.keep_previous(*id, PreviousBytes::File(path)),
            None => self.drop_previous(id),
        }
        // and what its executor's cache held of it, at a label it no longer carries (F102)
        self.uncache(id);
        sync_dir(&parent).await?;
        if let Some(pool) = source.parent() {
            sync_dir(pool).await?;
        }
        if fresh {
            self.state.chunks.fetch_add(1, Ordering::Relaxed);
        }
        self.state.chunk_bytes.fetch_add(file_bytes, Ordering::Relaxed);
        take_from(&self.state.chunk_bytes, replaced);
        // a chunk set aside at this id goes only once its replacement verifies (F89)
        self.settle_quarantine(id).await
    }

    /// Write part of a chunk's record into the chunk in place, its header after, made durable
    ///
    /// Every unit of the record is verified against the header it carries before one is written:
    /// a damaged record is never applied.
    ///
    /// # Arguments
    ///
    /// * `staged` - The record
    async fn write_units(&self, staged: &Staged) -> Result<(), StoreError> {
        let RecordKind::Units { units, header } = &staged.record.kind else {
            return Ok(());
        };
        let _drawn = self
            .draw(units.len() as u64 * u64::from(header.unit_bytes))
            .await?;
        let record = self
            .journal
            .read_units(staged.location, units.len(), header.unit_bytes)
            .await?;
        for (index, unit) in units.iter().enumerate() {
            let Some(bytes) = record.get(index) else {
                return Err(StoreError::Invalid(format!(
                    "a staged record holds {} of its units",
                    record.len()
                )));
            };
            if unit_crc(bytes, &header.id, *unit).await != header.crcs[*unit as usize] {
                // the journal returned bytes it was never given: counted against the device (F89)
                event!(
                    Level::WARN,
                    msg = "a staged record's unit failed its checksum and was not applied",
                    chunk = ?staged.record.id,
                    label = ?staged.record.makes,
                    unit,
                );
                self.device.note_checksum_failure(&self.state);
                return Err(StoreError::Corrupt { unit: *unit });
            }
        }
        let path = self.chunk_path(&staged.record.id);
        if !path.exists() {
            return Err(StoreError::Invalid(format!(
                "part of chunk {} is staged and the chunk is not there; a chunk's first write stages it whole",
                path.display()
            )));
        }
        let file = open(&path, false).await?;
        // a chunk is never extended in place: a record writes only inside its file (F96)
        let room = file_units(&file, header.unit_bytes).await?;
        if let Some(past) = units.iter().find(|unit| **unit >= room) {
            close(file).await;
            return Err(StoreError::Invalid(format!(
                "unit {past} of {} is past the {room} units its file has room for, and a chunk is \
                 never extended in place",
                path.display()
            )));
        }
        // the units about to be overwritten and the header they verify under, for a reader one
        // write behind, when the budget has room for them (F86)
        let kept = self.read_previous_units(&file, units).await;
        for (written, unit) in units.iter().enumerate() {
            // a test can end the holder half way through an apply's units
            if written * 2 >= units.len() && written > 0 {
                crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::MidApply);
            }
            // from the buffer the journal's device read it into, never a copy (F110)
            let Some(bytes) = record.buffer(written) else {
                return Err(StoreError::Invalid(format!(
                    "a staged record holds {} of its units",
                    record.len()
                )));
            };
            write_in_place(&file, bytes, header.offset_of(*unit)).await?;
        }
        write_all(&file, &header.encode(), 0).await?;
        // durable before the record goes: by the chunk's own sync, or a device flush begun after
        match self.device.apply_sync {
            ApplySync::Each => file
                .fdatasync()
                .await
                .map_err(|error| failed("syncing an applied chunk", error))?,
            ApplySync::Shared => self
                .flush
                .durable()
                .await
                .map_err(|error| StoreError::DeviceFailed { reason: error })?,
        }
        // and closed now, not on the reactor's next turn
        close(file).await;
        match kept {
            Some((header, units, permit)) => self.keep_previous(
                staged.record.id,
                PreviousBytes::Units {
                    header,
                    units,
                    _permit: permit,
                },
            ),
            None => self.drop_previous(&staged.record.id),
        }
        // and what its executor's cache held of it, at a label it no longer carries (F102)
        self.uncache(&staged.record.id);
        Ok(())
    }

    /// Link a chunk's file aside before a whole apply replaces it
    ///
    /// Nothing is read: the linked file carries its own header, which only a reader that asks
    /// for its label reads. Answers nothing when the link cannot be made: a previous state is a
    /// reader's convenience, and an apply never waits on it.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `target` - Its file
    async fn link_previous(&self, id: &ChunkId, target: &Path) -> Option<PathBuf> {
        if !self.options.keep_previous {
            return None;
        }
        let aside = self.previous_path(id);
        let _ = std::fs::remove_file(&aside);
        std::fs::hard_link(target, &aside).ok()?;
        Some(aside)
    }

    /// Read the units a partial apply is about to overwrite, and the header they verify under
    ///
    /// Answers nothing when the budget has no room for them or they do not read: the state is
    /// then not kept, and a reader one write behind reads its row again.
    ///
    /// # Arguments
    ///
    /// * `file` - The chunk's file
    /// * `units` - The units the apply writes
    async fn read_previous_units(
        &self,
        file: &Rc<DmaFile>,
        units: &[u32],
    ) -> Option<(ChunkHeader, BTreeMap<u32, Vec<u8>>, Option<crate::server::objects::budget::Permit>)> {
        if !self.options.keep_previous {
            return None;
        }
        let block = read_in_place(file, 0, BLOCK).await.ok()?;
        let header = ChunkHeader::decode(&block).ok()?;
        // the memory first, never waited for: a previous state is dropped first under pressure
        let bytes = units.len() as u64 * u64::from(header.unit_bytes);
        let permit = match &self.options.budget {
            Some(budget) => Some(budget.try_draw(bytes)?),
            None => None,
        };
        let mut held = BTreeMap::new();
        for unit in units {
            // a unit past what the chunk held reads as zeros at its old label, and is not kept
            if !header.holds(*unit) {
                continue;
            }
            let value = read_exact(file, header.offset_of(*unit), header.unit_bytes as usize).await.ok()?;
            held.insert(*unit, value);
        }
        Some((header, held, permit))
    }

    /// Where a chunk's whole previous state is linked
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    fn previous_path(&self, id: &ChunkId) -> PathBuf {
        self.state.path.join("previous").join(format!(
            "{:016x}-{:04x}-{:04x}-{:032x}-{:016x}-{:02x}",
            id.consumer, id.pg.tablet, id.pg.sub, id.object, id.stripe, id.position
        ))
    }

    /// Keep a chunk's state before an apply, replacing whatever was kept before it
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `bytes` - Where its bytes are
    fn keep_previous(&self, id: ChunkId, bytes: PreviousBytes) {
        self.sweep_previous();
        let replaced = self.previous.borrow_mut().insert(
            id,
            Previous {
                bytes,
                kept: std::time::Instant::now(),
            },
        );
        // a whole state replaced by units leaves its link behind
        if let Some(Previous {
            bytes: PreviousBytes::File(path),
            ..
        }) = replaced
        {
            if !matches!(self.previous.borrow().get(&id), Some(Previous { bytes: PreviousBytes::File(kept), .. }) if *kept == path)
            {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    /// Let a chunk's kept state go
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    fn drop_previous(&self, id: &ChunkId) {
        let gone = self.previous.borrow_mut().remove(id);
        if let Some(Previous {
            bytes: PreviousBytes::File(path),
            ..
        }) = gone
        {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Give the slice its executor's cache of the units its reads verify, once, as the executor
    /// opens it ([F102](../../../../../docs/src/features/read-unit-cache.md))
    ///
    /// # Arguments
    ///
    /// * `cache` - The executor's cache
    pub fn set_cache(&self, cache: Rc<UnitCache>) {
        // a slice is given one cache for its life; a second is the same executor's
        let _ = self.cache.set(cache);
    }

    /// Drop what its executor's cache holds of a chunk, which the slice is changing (F102)
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    fn uncache(&self, id: &ChunkId) {
        // a slice whose node keeps no cache has nothing to drop
        if let Some(cache) = self.cache.get() {
            cache.drop_chunk(self.state.id, id);
        }
    }

    /// Let every state kept past [`PREVIOUS_FOR`] go, at most once a second
    fn sweep_previous(&self) {
        if self.previous_swept.get().elapsed() < std::time::Duration::from_secs(1) {
            return;
        }
        self.previous_swept.set(std::time::Instant::now());
        let expired: Vec<ChunkId> = self
            .previous
            .borrow()
            .iter()
            .filter(|(_, previous)| previous.kept.elapsed() >= PREVIOUS_FOR)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.drop_previous(&id);
        }
    }

    /// The chain a reader one write behind is served from: the first label it names that is a
    /// chunk's kept previous state
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `labels` - The labels the reader names
    async fn previous_chain(&self, id: &ChunkId, labels: &[Label]) -> Result<Option<(Label, Chain)>, StoreError> {
        self.sweep_previous();
        let kept = {
            let previous = self.previous.borrow();
            let Some(kept) = previous.get(id) else {
                return Ok(None);
            };
            match &kept.bytes {
                PreviousBytes::File(path) => Err(path.clone()),
                PreviousBytes::Units { header, units, .. } => Ok((header.clone(), units.clone())),
            }
        };
        // a whole state's own file and the header it carries, or the chunk's file with the
        // overwritten units held over it
        let (header, base, held) = match kept {
            Err(path) => {
                let file = open(&path, false).await?;
                let block = read_in_place(&file, 0, BLOCK).await?;
                let Ok(header) = ChunkHeader::decode(&block) else {
                    return Ok(None);
                };
                (header, file, BTreeMap::new())
            }
            Ok((header, units)) => match self.chunk_state(id).await? {
                ChunkState::At(_, file) => (header, file, units),
                _ => return Ok(None),
            },
        };
        if !labels.contains(&header.label) {
            return Ok(None);
        }
        let label = header.label;
        Ok(Some((
            label,
            Chain {
                header: header.clone(),
                base: Some((base, header)),
                overlays: Vec::new(),
                held,
            },
        )))
    }

    /// Forget a record whose write is applied or discarded, and free its journal room
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label its record makes
    fn drop_record(&self, id: &ChunkId, label: Label) -> Option<Staged> {
        let mut staged = self.staged.borrow_mut();
        let list = staged.get_mut(id)?;
        let position = list.iter().position(|staged| staged.record.makes == label)?;
        let gone = list.remove(position);
        if list.is_empty() {
            staged.remove(id);
        }
        self.journal.release(gone.location);
        self.state.staged_bytes.fetch_sub(gone.bytes(), Ordering::Relaxed);
        Some(gone)
    }

    /// Drop every record of a chunk at or below the sequence of a label it just applied
    ///
    /// A label is applied only once its write committed, so its row has reached its sequence and
    /// no other write at or below it can commit; and the chunk only moves forward, so none of
    /// their records can apply. A record laid over a further label it may stand on leaves the one
    /// that made its expected label unapplied, and this is what drops it (item 239).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `applied` - The label it applied
    fn drop_passed(&self, id: &ChunkId, applied: Label) {
        // every record the applied label passed
        let passed: Vec<Label> = self
            .staged
            .borrow()
            .get(id)
            .map(|list| {
                list.iter()
                    .map(|staged| staged.record.makes)
                    .filter(|makes| makes.sequence <= applied.sequence)
                    .collect()
            })
            .unwrap_or_default();
        for label in passed {
            // a whole chunk's file goes back to the pool, as a discard's does
            if let Some(gone) = self.drop_record(id, label) {
                if let RecordKind::Whole { file_bytes, file } = gone.record.kind {
                    self.free.give_back(file_bytes, file);
                }
            }
        }
    }

    /// Discard a staged write a committed fact excludes
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label its record makes
    ///
    /// # Errors
    ///
    /// A label no record on the slice makes.
    pub async fn discard(&self, id: ChunkId, label: Label) -> Result<(), StoreError> {
        let lock = self.lock_of(&id);
        let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
        let gone = self.drop_record(&id, label).ok_or(StoreError::NotStaged { label })?;
        // a whole chunk's file goes back to the pool
        if let RecordKind::Whole { file_bytes, file } = gone.record.kind {
            self.free.give_back(file_bytes, file);
        }
        Ok(())
    }

    /// Read a range of a chunk at a label, every unit it touches verified
    ///
    /// Answers the bytes if the slice can make the label - its chunk carries it, or the records a
    /// reader's label says committed chain to it - and otherwise the label its chunk carries.
    /// A unit that fails its checksum under a record that can write it again is written again
    /// first; one that cannot is refused, never returned.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `label` - The label the reader wants
    ///
    /// # Errors
    ///
    /// A failed device, or a unit that fails its checksum with no record to write it again.
    pub async fn read(
        self: &Rc<Self>,
        id: ChunkId,
        offset: u64,
        len: u64,
        label: Label,
    ) -> Result<ReadAnswer, StoreError> {
        let batches = self.batches_now();
        let read = self.read_inner(id, offset, len, &[label], true, false, false).await;
        self.note_batches_waited(batches);
        // its bytes copied once into the buffer an answer to another thread needs (F105)
        Ok(match self.guard(read)?.1 {
            Found::Bytes(span) => ReadAnswer::Bytes(span.into_vec().await),
            Found::Label(have) => ReadAnswer::Label(have),
        })
    }

    /// The batches of applies a read beginning now waits behind already: the one in flight, if
    /// any, and how many were issued before it (F91)
    fn batches_now(&self) -> (u64, u64) {
        (
            self.state.apply_batches.load(Ordering::Acquire),
            self.state.apply_batch_in_flight.load(Ordering::Acquire),
        )
    }

    /// Count the batches of applies a read waited behind, the one in flight when it began and
    /// every one issued while it ran, into the most any read has (F91)
    ///
    /// # Arguments
    ///
    /// * `began` - What [`Slice::batches_now`] said when the read began
    fn note_batches_waited(&self, began: (u64, u64)) {
        if !self.device.rotational() {
            return;
        }
        let (issued, in_flight) = began;
        let since = self.state.apply_batches.load(Ordering::Acquire).saturating_sub(issued);
        // a batch in flight when it began is counted again by `since` only if it was issued after
        let waited = since + in_flight;
        self.state.read_batches_peak.fetch_max(waited, Ordering::Relaxed);
    }

    /// Read a range, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `labels` - The labels the reader wants, the first the slice makes read
    /// * `repair` - Whether a torn unit may be written again and the read tried once more
    /// * `cached` - Whether its executor's cache may answer it, and keeps what it verifies (F102)
    /// * `crosses` - Whether its answer crosses to a shard, so its run is read into a buffer that
    ///   can cross with it (F110)
    #[allow(clippy::too_many_arguments)]
    async fn read_inner(
        self: &Rc<Self>,
        id: ChunkId,
        offset: u64,
        len: u64,
        labels: &[Label],
        repair: bool,
        cached: bool,
        crosses: bool,
    ) -> Result<(Option<Label>, Found), StoreError> {
        self.serving()?;
        // what a read holds: the range, and the units it is cut from, a hit's range as much as a
        // device read's, held by the span it answers until that is dropped (F105)
        let drawn = self.draw(len.saturating_add(2 * STEP as u64)).await?;
        // units a read verified before, every one the range covers held at a label it asks
        // for, are answered from its executor's cache without the device: a label's bytes never
        // change, so they are the bytes the device holds at it (F102)
        let cache = self.cache.get().filter(|_| cached).cloned();
        if let Some(cache) = &cache {
            if let Some((label, range)) = cache.read(self.state.id, &id, labels, offset, len) {
                self.state.read_bytes.fetch_add(range.len() as u64, Ordering::Relaxed);
                // slices of the cache's units, never copied: a lane writes them as they are (F111)
                return Ok((Some(label), Found::Bytes(Span::cached(range.parts).holding(drawn))));
            }
        }
        let torn = {
            // which label is made is judged under the chunk's lock, as the read is, so an apply
            // rewriting the header between the two is never seen half written
            let lock = self.lock_of(&id);
            let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
            let mut found = None;
            for label in labels {
                if let Some(chain) = self.chain(&id, *label, 0).await? {
                    found = Some((*label, chain));
                    break;
                }
            }
            let Some((label, chain)) = found else {
                // a label the chunk was at before its last apply, for a reader one write behind
                if let Some((label, chain)) = self.previous_chain(&id, labels).await? {
                    if let Ok(span) = self.read_chain(&chain, offset, len, None, crosses).await? {
                        self.state.read_bytes.fetch_add(span.len() as u64, Ordering::Relaxed);
                        return Ok((Some(label), Found::Bytes(span.holding(drawn))));
                    }
                }
                let have = match self.chunk_state(&id).await? {
                    ChunkState::At(header, _) => Some(header.label),
                    _ => None,
                };
                return Ok((None, Found::Label(have)));
            };
            // a reader's read keeps every unit it verifies, whole, for the next read of them
            let mut keep = cache.as_ref().map(|_| BTreeMap::new());
            match self.read_chain(&chain, offset, len, keep.as_mut(), crosses).await? {
                Ok(span) => {
                    self.state.read_bytes.fetch_add(span.len() as u64, Ordering::Relaxed);
                    // put in under the chunk's lock, so no apply drops the chunk's units first and
                    // leaves these behind it (F102)
                    if let (Some(cache), Some(units)) = (&cache, keep) {
                        cache.fill(self.state.id, &id, label, chain.header.unit_bytes, chain.header.chunk_bytes, units);
                    }
                    return Ok((Some(label), Found::Bytes(span.holding(drawn))));
                }
                Err(unit) => unit,
            }
        };
        // a torn unit of the chunk is written again from the record that makes its label
        let at = match self.chunk_state(&id).await? {
            ChunkState::At(header, _) => Some(header.label),
            _ => None,
        };
        // the record that made the chunk's label tore after its header; the one standing on that
        // label tore before it, its write committed and begun
        let rewrite = at.and_then(|at| {
            self.staged_making(&id, at).or_else(|| {
                self.staged.borrow().get(&id).and_then(|list| {
                    list.iter()
                        .find(|staged| {
                            staged.record.stands_on(at)
                                && matches!(&staged.record.kind, RecordKind::Units { units, .. } if units.contains(&torn))
                        })
                        .cloned()
                })
            })
        });
        match rewrite {
            Some(staged) if repair => {
                {
                    let lock = self.lock_of(&id);
                    let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
                    self.write_units(&staged).await?;
                }
                Box::pin(self.read_inner(id, offset, len, labels, false, cached, crosses)).await
            }
            _ => {
                // nothing writes it again: the read's failure is reported, and the chunk set
                // aside if its own file is what failed (F89)
                self.condemn(&id, at, torn, "a read").await;
                Err(StoreError::Corrupt { unit: torn })
            }
        }
    }

    /// Report a unit a read found failing: set the chunk aside if its own file's unit fails at its
    /// label, and count the failure against the device either way
    ///
    /// A read's unit may have come from a record or a kept previous state rather than the chunk's
    /// file, so the file's unit is verified again before the chunk is condemned. Fails nothing
    /// itself: a read's answer is its refusal whatever this finds.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `at` - The label the chunk's file was at when the read failed, none for no file that read
    /// * `unit` - The unit that failed
    /// * `found_by` - What found it, for the log
    async fn condemn(&self, id: &ChunkId, at: Option<Label>, unit: u32, found_by: &str) {
        let condemned = self.quarantine(id, at, &[unit], found_by).await;
        match condemned {
            Ok(true) => {}
            // the file's own unit verifies: a record or a held state is what failed
            Ok(false) => {
                event!(
                    Level::WARN,
                    msg = "a unit failed its checksum outside its chunk's file; the chunk is kept",
                    chunk = ?id,
                    unit,
                    found_by,
                );
                self.device.note_checksum_failure(&self.state);
            }
            Err(error) => event!(
                Level::WARN,
                msg = "a chunk that failed its checksum could not be set aside",
                chunk = ?id,
                error = %error,
            ),
        }
    }

    /// Where a chunk's file goes when it is set aside, the `n`th time it is
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `n` - How many copies of it are set aside already
    fn quarantine_path(&self, id: &ChunkId, n: usize) -> PathBuf {
        let relative = id.relative();
        self.state
            .path
            .join("quarantine")
            .join(format!("{}.{n}", relative.display()))
    }

    /// Set a chunk aside if it still fails: at the label given, or a header that does not read
    /// when none is given
    ///
    /// The units named are verified again under the chunk's write lock first, so a chunk an apply
    /// mended or moved on since it was found is kept. Answers whether it was set aside.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `at` - The label its file was found failing at, none for a header that did not read
    /// * `units` - The units that failed
    /// * `found_by` - What found it, for the log
    ///
    /// # Errors
    ///
    /// A failed device.
    pub(crate) async fn quarantine(
        &self,
        id: &ChunkId,
        at: Option<Label>,
        units: &[u32],
        found_by: &str,
    ) -> Result<bool, StoreError> {
        let lock = self.lock_of(id);
        let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
        let label = match (self.chunk_state(id).await?, at) {
            // a header that does not read, found so
            (ChunkState::Torn, None) => Label::default(),
            // the same label, and a unit of it still failing
            (ChunkState::At(header, file), Some(at)) if header.label == at => {
                if self.verifies(&file, &header, units).await? {
                    return Ok(false);
                }
                at
            }
            _ => return Ok(false),
        };
        self.quarantine_locked(id, label, units, found_by).await?;
        Ok(true)
    }

    /// Move a chunk's file into `quarantine/`, with its lock held, and count it against its device
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label its header carried
    /// * `units` - The units that failed, none for its header
    /// * `found_by` - What found it, for the log
    async fn quarantine_locked(
        &self,
        id: &ChunkId,
        label: Label,
        units: &[u32],
        found_by: &str,
    ) -> Result<(), StoreError> {
        let source = self.chunk_path(id);
        let n = self.quarantine.borrow().get(id).map_or(0, Vec::len);
        let target = self.quarantine_path(id, n);
        // its directories, each made durable in its parent
        let root = self.state.path.join("quarantine");
        if let Some(parent) = target.parent() {
            let mut made = Vec::new();
            let mut walk = parent.to_path_buf();
            while !walk.exists() && walk != root {
                made.push(walk.clone());
                walk = walk.parent().map(Path::to_path_buf).unwrap_or_else(|| root.clone());
            }
            for dir in made.iter().rev() {
                std::fs::create_dir(dir).or_else(|error| {
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        Ok(())
                    } else {
                        Err(failed("making a quarantine directory", error))
                    }
                })?;
                if let Some(up) = dir.parent() {
                    sync_dir(up).await?;
                }
            }
        }
        // one rename, then both directories durable
        glommio::io::rename(&source, &target)
            .await
            .map_err(|error| failed("setting a chunk aside", error))?;
        for dir in [source.parent(), target.parent()].into_iter().flatten() {
            sync_dir(dir).await?;
        }
        // a reader one write behind is never served from a condemned chunk's file either
        self.drop_previous(id);
        // nor from its executor's cache, whose units were read from it (F102)
        self.uncache(id);
        // its bytes no longer a chunk's held, but evidence's (M20)
        let bytes = std::fs::metadata(&target).map_or(0, |meta| meta.len());
        self.quarantine
            .borrow_mut()
            .entry(*id)
            .or_default()
            .push(Quarantined { path: target, label });
        crate::server::devices::take_from(&self.state.chunks, 1);
        crate::server::devices::take_from(&self.state.chunk_bytes, bytes);
        self.state.quarantined.fetch_add(1, Ordering::Relaxed);
        event!(
            Level::WARN,
            msg = "a chunk failed its checksum and was set aside; its position is rebuilt from a copy that verifies",
            chunk = ?id,
            label = ?label,
            units = ?units,
            found_by,
        );
        self.device.note_checksum_failure(&self.state);
        Ok(())
    }

    /// Remove the chunks set aside at an id once the chunk placed there verifies, with its lock
    /// held; a replacement that fails is set aside beside them
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    async fn settle_quarantine(&self, id: &ChunkId) -> Result<(), StoreError> {
        if !self.quarantine.borrow().contains_key(id) {
            return Ok(());
        }
        let ChunkState::At(header, file) = self.chunk_state(id).await? else {
            return Ok(());
        };
        // every unit of the replacement verified before the evidence goes
        let all: Vec<u32> = (0..header.units()).collect();
        if !self.verifies(&file, &header, &all).await? {
            return self
                .quarantine_locked(id, header.label, &[], "its replacement's check")
                .await;
        }
        let gone = self.quarantine.borrow_mut().remove(id).unwrap_or_default();
        for set_aside in &gone {
            std::fs::remove_file(&set_aside.path).map_err(|error| failed("removing a replaced chunk", error))?;
        }
        if let Some(dir) = gone.first().and_then(|first| first.path.parent()) {
            sync_dir(dir).await?;
        }
        crate::server::devices::take_from(&self.state.quarantined, gone.len() as u64);
        event!(
            Level::INFO,
            msg = "a chunk set aside was replaced by one that verifies, and removed",
            chunk = ?id,
            label = ?header.label,
        );
        Ok(())
    }

    /// Verify some of a chunk's units where they lie, against the checksums its header binds
    ///
    /// A deep scrub's piece (F89): the units' bytes stay here, and the verdict answers each unit's
    /// checksum unbound, so a chunk's copies can be compared. A unit that fails and that a record
    /// can write again is written again, as a read's is; one nothing can write sets the chunk
    /// aside. A chunk whose file is not at the label is answered with the label it is at, or the
    /// one asked for when records make it, which is not damage.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label the row names for it
    /// * `offset` - Where the range starts; every unit it touches is verified
    /// * `len` - How long it is
    ///
    /// # Errors
    ///
    /// A failed device.
    pub async fn verify(self: &Rc<Self>, id: ChunkId, label: Label, offset: u64, len: u64) -> Result<Verdict, StoreError> {
        let verdict = self.verify_inner(id, label, offset, len, true, false).await;
        self.guard(verdict)
    }

    /// Verify some of a chunk's units where they lie, as [`Slice::verify`] does, and fold the bytes
    /// of those that verify into their summary for the parity check (F90)
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label the row names for it
    /// * `offset` - Where the range starts; every unit it touches is verified
    /// * `len` - How long it is
    ///
    /// # Errors
    ///
    /// A failed device, or a header or a unit that cannot be read at all.
    pub async fn verify_summarised(
        self: &Rc<Self>,
        id: ChunkId,
        label: Label,
        offset: u64,
        len: u64,
    ) -> Result<Verdict, StoreError> {
        let verdict = self.verify_inner(id, label, offset, len, true, true).await;
        self.guard(verdict)
    }

    /// Verify some of a chunk's units, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `repair` - Whether a failing unit may be written again from a record and verified again
    async fn verify_inner(
        self: &Rc<Self>,
        id: ChunkId,
        label: Label,
        offset: u64,
        len: u64,
        repair: bool,
        summarise: bool,
    ) -> Result<Verdict, StoreError> {
        self.serving()?;
        let failing = {
            // the header and the units judged under the chunk's lock, so an apply between them
            // is never taken for damage
            let lock = self.lock_of(&id);
            let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
            match self.chunk_state(&id).await? {
                ChunkState::Missing => {
                    // set aside already, found before
                    let held = self
                        .quarantine
                        .borrow()
                        .get(&id)
                        .and_then(|list| list.last().map(|set_aside| set_aside.label));
                    if let Some(held) = held {
                        return Ok(Verdict::Corrupt { label: held, units: Vec::new() });
                    }
                    let makes = self.chain(&id, label, 0).await?.is_some();
                    return Ok(Verdict::Held(makes.then_some(label)));
                }
                ChunkState::Torn => None,
                ChunkState::At(header, file) => {
                    if header.label != label {
                        // records that make the label are not applied yet: not damage
                        let makes = self.chain(&id, label, 0).await?.is_some();
                        return Ok(Verdict::Held(Some(if makes { label } else { header.label })));
                    }
                    let units = header.units_of(offset, len);
                    let (first, end) = (units.start.min(header.units()), units.end.min(header.units()));
                    let mut crcs = Vec::with_capacity(end.saturating_sub(first) as usize);
                    let mut failing = Vec::new();
                    let mut read = 0u64;
                    let mut summary = summarise.then(|| Box::new([0u8; crate::server::objects::codec::SUMMARY_BYTES]));
                    for unit in first..end {
                        // a unit holding no bytes under the header has nothing to verify
                        if !header.holds(unit) {
                            crcs.push(0);
                            continue;
                        }
                        // checked where the device put it, a unit at a time as before (F110)
                        let bytes =
                            read_in_place(&file, header.offset_of(unit), header.unit_bytes as usize)
                                .await?;
                        read += bytes.len() as u64;
                        let crc = bytes_crc(&bytes).await;
                        if bound_crc(crc, &id, unit) != header.crcs[unit as usize] {
                            failing.push(unit);
                        }
                        // folded where it lies in the chunk, for the parity check (F90)
                        if let Some(summary) = summary.as_mut() {
                            let at = u64::from(unit) * u64::from(header.unit_bytes);
                            crate::server::objects::codec::summarise(at, &bytes, summary);
                            glommio::yield_if_needed().await;
                        }
                        crcs.push(crc);
                    }
                    self.state.scrub_bytes.fetch_add(read, Ordering::Relaxed);
                    if failing.is_empty() {
                        return Ok(Verdict::Verified {
                            label,
                            crcs,
                            summary: summary.map(|summary| summary.to_vec()),
                        });
                    }
                    Some(failing)
                }
            }
        };
        let Some(failing) = failing else {
            // a header that does not read under the chunk's lock is damage, unless an apply mended
            // it meanwhile
            if self.quarantine(&id, None, &[], "a deep scrub").await? {
                return Ok(Verdict::Corrupt { label, units: Vec::new() });
            }
            return Ok(Verdict::Held(self.chunk_label(&id).await?));
        };
        // a unit a record can write again is written again and the piece verified once more
        let rewrite = self.staged_making(&id, label).or_else(|| {
            self.staged.borrow().get(&id).and_then(|list| {
                list.iter()
                    .find(|staged| {
                        staged.record.stands_on(label)
                            && matches!(&staged.record.kind, RecordKind::Units { units, .. } if failing.iter().all(|unit| units.contains(unit)))
                    })
                    .cloned()
            })
        });
        if let (Some(staged), true) = (rewrite, repair) {
            {
                let lock = self.lock_of(&id);
                let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
                self.write_units(&staged).await?;
            }
            return Box::pin(self.verify_inner(id, label, offset, len, false, summarise)).await;
        }
        // nothing can: the chunk is set aside, unless it moved on meanwhile
        if self.quarantine(&id, Some(label), &failing, "a deep scrub").await? {
            Ok(Verdict::Corrupt { label, units: failing })
        } else {
            Ok(Verdict::Held(self.chunk_label(&id).await?))
        }
    }

    /// Read a range through a chain, every unit verified; the unit that failed if one did
    ///
    /// The range's units the base holds are read from its file as one run of pieces in flight
    /// together, on SSDs as on disks, and each is verified where the device put it. The answer is
    /// those buffers when every unit of the range came from them, and otherwise the range laid out
    /// once in a buffer of its own, from the run, a record, a kept previous state or zeros
    /// ([F105](../../../../../docs/src/features/store-reads-in-place.md)). The run spans the units that need the base,
    /// overlaid ones between them, since reading those costs a device little more than skipping
    /// them; it holds no more than the range and two units, inside the draw a read makes (F91).
    ///
    /// # Arguments
    ///
    /// * `chain` - Where the label's bytes come from
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `keep` - Where every unit it verified goes, whole, if a cache is to keep them (F102)
    async fn read_chain(
        &self,
        chain: &Chain,
        offset: u64,
        len: u64,
        mut keep: Option<&mut BTreeMap<u32, Vec<u8>>>,
        crosses: bool,
    ) -> Result<Result<Span, u32>, StoreError> {
        let header = &chain.header;
        let end = (offset + len).min(header.chunk_bytes);
        let units: Vec<u32> = if offset < end {
            header.units_of(offset, end - offset).collect()
        } else {
            Vec::new()
        };
        if units.is_empty() {
            return Ok(Ok(Span::owned(Vec::new())));
        }
        let unit_bytes = u64::from(header.unit_bytes);
        let unit_len = header.unit_bytes as usize;
        // each record's units, read once, by unit, where the journal's device put them (F110)
        let mut laid: BTreeMap<u32, (glommio::io::ReadResult, u64)> = BTreeMap::new();
        for staged in &chain.overlays {
            if let RecordKind::Units { units: written, header: made } = &staged.record.kind {
                if written.iter().all(|unit| !units.contains(unit)) {
                    continue;
                }
                let record = self
                    .journal
                    .read_units(staged.location, written.len(), made.unit_bytes)
                    .await?;
                for (index, unit) in written.iter().enumerate() {
                    if let Some(bytes) = record.buffer(index) {
                        laid.insert(*unit, (bytes, made.crcs[*unit as usize]));
                    }
                }
            }
        }
        // where each unit's bytes come from: nowhere, a kept previous state, a record, or the base
        let sources: Vec<(u32, Source)> = units
            .iter()
            .map(|unit| {
                let from = if !header.holds(*unit) {
                    Source::Zeros
                } else if chain.held.contains_key(unit) {
                    Source::Held
                } else if laid.contains_key(unit) {
                    Source::Laid
                } else {
                    Source::Base
                };
                (*unit, from)
            })
            .collect();
        // the base's units the range needs, one run of its file: a unit it does not hold, or past
        // its file's room, is nowhere, since a record that lengthens a chunk carries every unit it
        // brings in (F96)
        let needed: Vec<u32> = sources
            .iter()
            .filter(|(_, from)| *from == Source::Base)
            .map(|(unit, _)| *unit)
            .collect();
        let mut run = Run::Pieces(Vec::new());
        let run_first = needed.first().copied().unwrap_or(units[0]);
        if let (Some(&lo), Some(&hi)) = (needed.first(), needed.last()) {
            let Some((file, base)) = chain.base.as_ref() else {
                return Ok(Err(lo));
            };
            let base_end = u32::try_from(base.length.div_ceil(unit_bytes)).unwrap_or(u32::MAX);
            if let Some(missing) = needed.iter().find(|unit| **unit >= base_end || !base.holds(**unit)) {
                return Ok(Err(*missing));
            }
            self.hold_arm().await;
            let run_len = usize::try_from(u64::from(hi - lo + 1) * unit_bytes).unwrap_or(0);
            // a read whose answer crosses to a shard reads into a buffer that can cross with it,
            // in one read; one answered on this executor keeps the device's pieces (F110)
            run = if crosses {
                Run::Heap(read_run(file, base.offset_of(lo), run_len).await?)
            } else {
                Run::Pieces(read_pieces(file, base.offset_of(lo), run_len, unit_len).await?)
            };
            self.state.read_ios.fetch_add(1, Ordering::Relaxed);
        }
        let piece = piece_bytes(unit_len);
        // where a unit of the run starts in it
        let at_in_run = |unit: u32| usize::try_from(u64::from(unit - run_first) * unit_bytes).unwrap_or(usize::MAX);
        // every unit verified where it lies, wherever it comes from
        for (unit, from) in &sources {
            let (bytes, crc) = match from {
                Source::Zeros => continue,
                Source::Held => (chain.held[unit].as_slice(), header.crcs[*unit as usize]),
                Source::Laid => (&laid[unit].0[..], laid[unit].1),
                Source::Base => {
                    let (Some(bytes), Some((_, base))) = (run.unit(at_in_run(*unit), piece, unit_len), chain.base.as_ref()) else {
                        return Ok(Err(*unit));
                    };
                    (bytes, base.crcs[*unit as usize])
                }
            };
            if unit_crc(bytes, &header.id, *unit).await != crc {
                return Ok(Err(*unit));
            }
        }
        // every unit it verified, whole, kept if a cache is to hold them (F102): copies, since a
        // cache never holds a device's buffer and the registered memory it is drawn from (F105)
        if let Some(keep) = keep.as_deref_mut() {
            for (unit, from) in &sources {
                let bytes = match from {
                    Source::Zeros => vec![0u8; unit_len],
                    Source::Held => chain.held[unit].clone(),
                    Source::Laid => laid[unit].0.to_vec(),
                    Source::Base => run.unit(at_in_run(*unit), piece, unit_len).map(<[u8]>::to_vec).unwrap_or_default(),
                };
                keep.insert(*unit, bytes);
            }
        }
        // the range where the device put it, when every unit of it came from the base
        let run_start = u64::from(run_first) * unit_bytes;
        if needed.len() == units.len() {
            let from = usize::try_from(offset - run_start).unwrap_or(0);
            let to = usize::try_from(end - run_start).unwrap_or(0);
            return Ok(Ok(run.span(from, to)));
        }
        // or laid out once in a buffer of its own, a unit at a time
        let mut out = Vec::with_capacity(usize::try_from(end - offset).unwrap_or(0));
        for (unit, from) in &sources {
            // the part of the unit the range covers
            let unit_start = u64::from(*unit) * unit_bytes;
            let lo = usize::try_from(offset.max(unit_start) - unit_start).unwrap_or(0);
            let hi = usize::try_from(end.min(unit_start + unit_bytes) - unit_start).unwrap_or(0);
            match from {
                Source::Zeros => out.resize(out.len() + (hi - lo), 0),
                Source::Held => out.extend_from_slice(&chain.held[unit][lo..hi]),
                Source::Laid => out.extend_from_slice(&laid[unit].0[lo..hi]),
                Source::Base => match run.unit(at_in_run(*unit), piece, unit_len) {
                    Some(bytes) => out.extend_from_slice(&bytes[lo..hi]),
                    None => return Ok(Err(*unit)),
                },
            }
            glommio::yield_if_needed().await;
        }
        Ok(Ok(Span::owned(out)))
    }

    /// Fold a small write's committed bytes into a chunk: journalled over a label they fold from,
    /// durable before this answers, then applied
    ///
    /// The bytes are committed, so the record they make is one a row names: applying it waits for
    /// nothing. A chunk that already makes their label answers at once
    /// ([S6](../../../../../docs/src/object-storage/device-store.md#folding-a-small-writes-bytes)).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `makes` - The label the bytes make
    /// * `sources` - The labels they fold from: the base, then each earlier small write merged
    /// * `length` - How many of the chunk's bytes hold data once they apply
    /// * `units` - The units and their values
    ///
    /// # Errors
    ///
    /// A chunk at none of the sources, a failed device or a full journal.
    pub async fn fold(
        self: &Rc<Self>,
        id: ChunkId,
        makes: Label,
        sources: Vec<Label>,
        length: u64,
        units: Vec<(u32, Vec<u8>)>,
    ) -> Result<(), StoreError> {
        // a chunk that makes the label already has nothing to fold, judged under its lock
        let made = {
            let lock = self.lock_of(&id);
            let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
            self.chain(&id, makes, 0).await?.is_some()
        };
        if made {
            return Ok(());
        }
        let Some((first, rest)) = sources.split_first() else {
            return Err(StoreError::Invalid("a fold names no label it folds from".to_string()));
        };
        self.stage_units(StageUnits {
            id,
            makes,
            expects: *first,
            also: rest.to_vec(),
            length,
            units,
        })
        .await?;
        // durable as a committed record; applied behind the answer, on this executor
        let slice = self.clone();
        glommio::spawn_local(async move {
            if let Err(error) = slice.apply(id, makes).await {
                // a later apply that passed it applied it beneath its own and dropped it (item 239)
                let passed = matches!(error, StoreError::NotStaged { .. })
                    && matches!(
                        slice.chunk_state(&id).await,
                        Ok(ChunkState::At(header, _)) if header.label.sequence >= makes.sequence
                    );
                if passed {
                    return;
                }
                tracing::event!(
                    tracing::Level::WARN,
                    msg = "a folded small write did not apply; its record stays for its row to decide",
                    chunk = ?id,
                    error = %error,
                );
            }
        })
        .detach();
        Ok(())
    }

    /// Read a range of a chunk at the first of some labels the slice can make
    ///
    /// The labels a reader names are the row's, then those its pending bytes fold from, so a
    /// holder that has not folded answers with its base and the reader lays the bytes over
    /// ([S9](../../../../../docs/src/object-storage/read-path.md#a-stripe-chunk-under-another-label)).
    /// Answers the label made with the bytes, or the label the chunk carries when it makes none.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `labels` - The labels, in the order they are wanted
    ///
    /// # Errors
    ///
    /// A failed device, or a unit that fails its checksum with no record to write it again.
    pub async fn read_any(
        self: &Rc<Self>,
        id: ChunkId,
        offset: u64,
        len: u64,
        labels: &[Label],
    ) -> Result<Result<(Label, Vec<u8>), Option<Label>>, StoreError> {
        // a reader's read, which its executor's cache may answer
        self.read_any_as(id, offset, len, labels, true).await
    }

    /// Read a range of a chunk at the first of some labels the slice can make, through its
    /// executor's cache or past it
    ///
    /// A reader's read takes what the cache holds and fills it with what it verifies; a
    /// rebuild's, a move's and a scrub's read the device and leave the cache as it was, so
    /// recovery never pushes out what readers read ([F102](../../../../../docs/src/features/read-unit-cache.md)).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `labels` - The labels, in the order they are wanted
    /// * `cached` - Whether the read may be answered from the cache, and fills it
    ///
    /// # Errors
    ///
    /// A failed device, or a unit that fails its checksum with no record to write it again.
    pub async fn read_any_as(
        self: &Rc<Self>,
        id: ChunkId,
        offset: u64,
        len: u64,
        labels: &[Label],
        cached: bool,
    ) -> Result<Result<(Label, Vec<u8>), Option<Label>>, StoreError> {
        // its bytes copied once into the buffer an answer to another thread needs (F105)
        Ok(match self.read_span_as(id, offset, len, labels, cached, false).await? {
            Ok((label, span)) => Ok((label, span.into_vec().await)),
            Err(have) => Err(have),
        })
    }

    /// Read a range of a chunk at the first of some labels the slice can make, its bytes left in
    /// the buffers the read put them in, for an answer written on this executor
    /// ([F106](../../../../../docs/src/features/lane-copies.md))
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `labels` - The labels, in the order they are wanted
    /// * `cached` - Whether the read may be answered from the cache, and fills it
    ///
    /// # Errors
    ///
    /// A failed device, or a unit that fails its checksum with no record to write it again.
    pub(crate) async fn read_span_as(
        self: &Rc<Self>,
        id: ChunkId,
        offset: u64,
        len: u64,
        labels: &[Label],
        cached: bool,
        crosses: bool,
    ) -> Result<Result<(Label, Span), Option<Label>>, StoreError> {
        // the first the slice can make is read; a torn unit is mended as a read of it would be
        let batches = self.batches_now();
        let read = self.read_inner(id, offset, len, labels, true, cached, crosses).await;
        self.note_batches_waited(batches);
        match self.guard(read)? {
            (Some(label), Found::Bytes(span)) => Ok(Ok((label, span))),
            (_, Found::Label(have)) => Ok(Err(have)),
            (None, Found::Bytes(_)) => Err(StoreError::Invalid(
                "a read answered bytes at no label".to_string(),
            )),
        }
    }

    /// Which of some labels the slice can make, the first it can, or the label its chunk carries
    ///
    /// A holder's word in a write's round that an untouched chunk is current
    /// ([S7](../../../../../docs/src/object-storage/write-path.md#the-acknowledgement-rule)).
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `labels` - The labels, in the order they are wanted
    ///
    /// # Errors
    ///
    /// A failed device.
    pub async fn confirm(&self, id: ChunkId, labels: &[Label]) -> Result<Result<Label, Option<Label>>, StoreError> {
        self.serving()?;
        // judged under the chunk's lock, so an apply's header is never seen half written
        let lock = self.lock_of(&id);
        let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
        for label in labels {
            if self.can_make(&id, *label).await? {
                return Ok(Ok(*label));
            }
        }
        Ok(Err(self.chunk_label(&id).await?))
    }

    /// Whether the slice can make a label of a chunk: its chunk carries it, or records chain to it
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label
    async fn can_make(&self, id: &ChunkId, label: Label) -> Result<bool, StoreError> {
        Ok(self.chain(id, label, 0).await?.is_some())
    }

    /// The records no row has decided yet: staged at least `older_than` ago, or found by a start,
    /// and none another record of its chunk stands on
    ///
    /// What the node asks each one's row about, so a stage whose stager died is decided by a
    /// committed fact ([P16](../../../../../docs/src/object-storage/contract.md)). A record a later
    /// one stands on is committed, since a writer stages only over a label its row names or its
    /// pending bytes fold from. Its row may have cleared past it while its apply waits, as a disk's
    /// applies wait behind their batches, and judged alone it would be discarded and every record
    /// above it left with nothing to apply over. So it is the later record's to decide: applied
    /// beneath it, or asked about once that record is gone
    /// ([Resolved #283](../../../../../docs/src/appendix/resolved/chained-records-decided-together.md)).
    ///
    /// # Arguments
    ///
    /// * `older_than` - How long a stage is left to its stager
    #[must_use]
    pub fn undecided(&self, older_than: std::time::Duration) -> Vec<(ChunkId, Label)> {
        self.staged
            .borrow()
            .iter()
            .flat_map(|(id, list)| {
                list.iter()
                    .filter(|staged| staged.recovered || staged.at.elapsed() >= older_than)
                    // a record a later one of its chunk stands on is decided with that one
                    .filter(|staged| {
                        !list.iter().any(|other| {
                            other.record.makes != staged.record.makes
                                && other.record.stands_on(staged.record.makes)
                        })
                    })
                    .map(|staged| (*id, staged.record.makes))
            })
            .collect()
    }

    /// The records the slice holds, for a test or a figure
    #[must_use]
    pub fn staged_count(&self) -> usize {
        self.staged.borrow().values().map(Vec::len).sum()
    }

    /// The labels the slice holds records making, for a chunk
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    #[must_use]
    pub fn staged_labels(&self, id: &ChunkId) -> Vec<Label> {
        self.staged
            .borrow()
            .get(id)
            .map(|list| list.iter().map(|staged| staged.record.makes).collect())
            .unwrap_or_default()
    }

    /// The label a chunk carries on the device, if it is there and its header reads
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    ///
    /// # Errors
    ///
    /// A failed device.
    pub async fn chunk_label(&self, id: &ChunkId) -> Result<Option<Label>, StoreError> {
        Ok(match self.chunk_state(id).await? {
            ChunkState::At(header, _) => Some(header.label),
            _ => None,
        })
    }

    /// The header a chunk carries on the device, if it is there and reads: its label, its length,
    /// its stripe's and whose it is
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    ///
    /// # Errors
    ///
    /// A failed device.
    pub async fn chunk_header(&self, id: &ChunkId) -> Result<Option<ChunkHeader>, StoreError> {
        Ok(match self.chunk_state(id).await? {
            ChunkState::At(header, _) => Some(header),
            _ => None,
        })
    }

    /// The chunks this slice holds of one placement group, past a place in it, with the label
    /// each is at, or for a whole chunk staged and not yet applied, the label it stages (F88)
    ///
    /// What a move's copier asks of every old holder: a stripe whose put wrote it whole has no row
    /// to be found by, so its holders' inventories are how it is found. A failed device's listing
    /// is an error, never an empty answer, which a copier would take for a hole. Since F89 it is a
    /// light scrub's inventory too: a chunk set aside is listed as quarantined, and a chunk whose
    /// header does not read under its lock is set aside as it is found and listed so.
    ///
    /// # Arguments
    ///
    /// * `after` - The consumer and the placement group, and the place to list after
    /// * `start` - Whether `after` is the group's start rather than a chunk listed already
    /// * `limit` - The most chunks to answer
    /// * `paced` - Whether each header read draws a block from the device's ceiling first, as a
    ///   scrub's listing does
    ///
    /// # Errors
    ///
    /// A failed device, or a directory that cannot be read.
    pub async fn list(
        &self,
        after: ChunkId,
        start: bool,
        limit: u32,
        paced: bool,
    ) -> Result<(Vec<ListedChunk>, bool), StoreError> {
        self.serving()?;
        // every chunk of the group named on the device, by its path
        let group = ChunkId {
            object: 0,
            stripe: 0,
            position: 0,
            ..after
        };
        let dir = self.chunk_path(&group);
        let dir = dir
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .ok_or_else(|| StoreError::Invalid("a chunk's path has no group directory".to_string()))?;
        let mut found: BTreeMap<ChunkId, Listing> = BTreeMap::new();
        if dir.exists() {
            let objects = std::fs::read_dir(&dir).map_err(|error| failed("listing a placement group", error))?;
            for object in objects {
                let object = object.map_err(|error| failed("listing a placement group", error))?;
                let Some(id) = object
                    .file_name()
                    .to_str()
                    .and_then(|name| u128::from_str_radix(name, 16).ok())
                else {
                    continue;
                };
                let files = std::fs::read_dir(object.path()).map_err(|error| failed("listing an object's chunks", error))?;
                for file in files {
                    let file = file.map_err(|error| failed("listing an object's chunks", error))?;
                    let name = file.file_name();
                    // a chunk's file is its stripe and its position
                    let Some((stripe, position)) = name.to_str().and_then(|name| name.split_once('.')) else {
                        continue;
                    };
                    let (Ok(stripe), Ok(position)) = (u64::from_str_radix(stripe, 16), position.parse::<u8>()) else {
                        continue;
                    };
                    found.insert(
                        ChunkId {
                            object: id,
                            stripe,
                            position,
                            ..group
                        },
                        Listing::Placed,
                    );
                }
                glommio::yield_if_needed().await;
            }
        }
        // and every whole chunk of it staged here, with the label it stages
        for (id, staged) in self.staged.borrow().iter() {
            if id.consumer != group.consumer || id.pg != group.pg {
                continue;
            }
            if let Some(whole) = staged
                .iter()
                .filter(|staged| matches!(staged.record.kind, RecordKind::Whole { .. }))
                .max_by_key(|staged| staged.record.makes.sequence)
            {
                found.entry(*id).or_insert(Listing::Staged(whole.record.makes));
            }
        }
        // and every chunk of it set aside, where nothing was placed since (F89)
        for (id, list) in self.quarantine.borrow().iter() {
            if id.consumer != group.consumer || id.pg != group.pg {
                continue;
            }
            if let Some(last) = list.last() {
                found.entry(*id).or_insert(Listing::Quarantined(last.label));
            }
        }
        // the page past the place asked for
        let cursor = (after.object, after.stripe, after.position);
        let mut page: Vec<(ChunkId, Listing)> = found
            .into_iter()
            .filter(|(id, _)| {
                let at = (id.object, id.stripe, id.position);
                if start {
                    at >= cursor
                } else {
                    at > cursor
                }
            })
            .collect();
        page.sort_by_key(|(id, _)| (id.object, id.stripe, id.position));
        let more = page.len() > limit as usize;
        page.truncate(limit as usize);
        // each placed chunk's label and length, from its header read under its lock
        let mut listed = Vec::with_capacity(page.len());
        for (id, listing) in page {
            let entry = match listing {
                Listing::Placed => {
                    // a scrub's header read is a block of its device's ceiling
                    if paced {
                        self.pace(BLOCK as u64).await;
                    }
                    let state = {
                        let lock = self.lock_of(&id);
                        let _read = lock.read().await.map_err(|error| failed("locking a chunk", error))?;
                        match self.chunk_state(&id).await? {
                            ChunkState::At(header, _) => Some((header.label, header.length, header.stripe_length, header.owner)),
                            ChunkState::Torn => None,
                            ChunkState::Missing => continue,
                        }
                    };
                    match state {
                        Some((label, length, stripe_length, owner)) => ListedChunk {
                            chunk: id,
                            label,
                            length,
                            quarantined: false,
                            staged: false,
                            stripe_length,
                            owner,
                        },
                        // a header that does not read under its lock is damage, set aside as found
                        None => {
                            if !self.quarantine(&id, None, &[], "a listing").await? {
                                continue;
                            }
                            ListedChunk {
                                chunk: id,
                                label: Label::default(),
                                length: 0,
                                quarantined: true,
                                staged: false,
                                stripe_length: 0,
                                owner: 0,
                            }
                        }
                    }
                }
                // a staged whole chunk's length is read with it, from its file's header
                Listing::Staged(label) => ListedChunk {
                    chunk: id,
                    label,
                    length: 0,
                    quarantined: false,
                    staged: true,
                    stripe_length: 0,
                    owner: 0,
                },
                Listing::Quarantined(label) => ListedChunk {
                    chunk: id,
                    label,
                    length: 0,
                    quarantined: true,
                    staged: false,
                    stripe_length: 0,
                    owner: 0,
                },
            };
            listed.push(entry);
        }
        Ok((listed, more))
    }

    /// Wait until the device's ceiling lets some bytes through, and take them (F89)
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    async fn pace(&self, bytes: u64) {
        while let Err(wait) = self.device.ceiling.take(bytes) {
            glommio::timer::sleep(wait.max(std::time::Duration::from_millis(1))).await;
        }
    }

    /// The chunks set aside on this slice, by chunk, for a test or a figure (F89)
    #[must_use]
    pub fn quarantined(&self) -> Vec<(ChunkId, Label)> {
        self.quarantine
            .borrow()
            .iter()
            .flat_map(|(id, list)| list.iter().map(|set_aside| (*id, set_aside.label)))
            .collect()
    }

    /// The files of the pool ready, across every length
    #[must_use]
    pub fn free_files(&self) -> usize {
        self.free.available()
    }

    /// How many syncs the shared flush has made, and the applies they covered
    #[must_use]
    pub fn flushes(&self) -> (u64, u64) {
        (self.flush.counts.syncs.get(), self.flush.counts.waiters.get())
    }

    /// How the slice is kept
    #[must_use]
    pub fn options(&self) -> &SliceOptions {
        &self.options
    }

    /// The device this slice is on
    #[must_use]
    pub fn device(&self) -> &Arc<DeviceState> {
        &self.device
    }

    /// The slice's own state
    #[must_use]
    pub fn state(&self) -> &Arc<SliceState> {
        &self.state
    }

    /// Close the slice once every sync asked of it is answered, and every file it holds
    ///
    /// A chunk file dropped on an error's way out is closed on the reactor's next turn, so the
    /// slice gives it one before it returns: a file still queued when its executor stops is never
    /// closed, which a device being unmounted after its node stops would meet.
    pub async fn close(&self) {
        // a disk's applier ends with its sender, answering what it never ran
        self.applier.borrow_mut().take();
        self.journal.close().await;
        self.flush.stop().await;
        glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
    }
}

impl WholeWriter {
    /// Write the next piece of the chunk: whole units, in order from the chunk's start
    ///
    /// Each unit's checksum is the one its sender computed as the bytes arrived, bound here to the
    /// unit's place without reading the bytes again; a piece sent with none has them computed.
    ///
    /// # Arguments
    ///
    /// * `offset` - Where in the chunk the piece starts, where the last one ended
    /// * `crcs` - The CRC-64/NVME of each unit's bytes, unbound, or none to compute them here
    /// * `bytes` - The piece's units
    ///
    /// # Errors
    ///
    /// A piece out of order or of no whole units, or a failed device.
    pub async fn piece(&mut self, offset: u64, crcs: &[u64], bytes: &[u8]) -> Result<(), StoreError> {
        let written = self.piece_inner(offset, crcs, bytes).await;
        self.slice.guard(written)
    }

    /// Write the next piece from the buffer its lane read its units into, as it is (F111)
    ///
    /// What [`WholeWriter::piece`] does, without copying the units into the device's buffers: the
    /// lane read them into one already.
    ///
    /// # Arguments
    ///
    /// * `offset` - Where in the chunk the piece starts, where the last one ended
    /// * `crcs` - The CRC-64/NVME of each unit's bytes, unbound, or none to compute them here
    /// * `units` - The piece's units, in a buffer aligned for the device
    ///
    /// # Errors
    ///
    /// A piece out of order or of no whole units, or a failed device.
    pub async fn piece_aligned(
        &mut self,
        offset: u64,
        crcs: &[u64],
        units: glommio::io::DmaBuffer,
    ) -> Result<(), StoreError> {
        let written = match self.bind_piece(offset, crcs, units.as_bytes()).await {
            Ok(first) => self.write_aligned(first, units).await,
            Err(error) => Err(error),
        };
        self.slice.guard(written)
    }

    /// Write a piece's units at their place from the buffer they are in, every byte landing
    ///
    /// # Arguments
    ///
    /// * `first` - The piece's first unit
    /// * `units` - Its units
    async fn write_aligned(
        &mut self,
        first: u32,
        units: glommio::io::DmaBuffer,
    ) -> Result<(), StoreError> {
        let len = units.len();
        let landed = self
            .file
            .write_at(units, self.header.offset_of(first))
            .await
            .map_err(|error| super::failed(&format!("writing {:?}", self.file.path()), error))?;
        if landed != len {
            return Err(super::failed(
                &format!("writing {:?}", self.file.path()),
                format!("{landed} of {len} bytes landed"),
            ));
        }
        self.next += len as u64;
        Ok(())
    }

    /// Write the next piece, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `offset` - Where in the chunk the piece starts
    /// * `crcs` - The units' unbound checksums, or none
    /// * `bytes` - The piece's units
    async fn piece_inner(&mut self, offset: u64, crcs: &[u64], bytes: &[u8]) -> Result<(), StoreError> {
        let first = self.bind_piece(offset, crcs, bytes).await?;
        write_all(&self.file, bytes, self.header.offset_of(first)).await?;
        self.next += bytes.len() as u64;
        Ok(())
    }

    /// Check that a piece is the chunk's next whole units and bind each unit's checksum to its
    /// place; the piece's first unit
    ///
    /// # Arguments
    ///
    /// * `offset` - Where in the chunk the piece starts
    /// * `crcs` - The units' unbound checksums, or none
    /// * `bytes` - The piece's units
    async fn bind_piece(
        &mut self,
        offset: u64,
        crcs: &[u64],
        bytes: &[u8],
    ) -> Result<u32, StoreError> {
        let unit = u64::from(self.header.unit_bytes);
        // pieces arrive in order and carry whole units inside the chunk's file, which is never
        // extended (F96)
        let room = self.file_bytes - BLOCK as u64;
        if offset != self.next
            || bytes.is_empty()
            || !(bytes.len() as u64).is_multiple_of(unit)
            || offset + bytes.len() as u64 > room
        {
            return Err(StoreError::Invalid(format!(
                "a piece of {} bytes at {offset} is not the next whole units of a chunk whose file \
                 has room for {room} bytes, written to {}",
                bytes.len(),
                self.next
            )));
        }
        let units = bytes.len() as u64 / unit;
        if !crcs.is_empty() && crcs.len() as u64 != units {
            return Err(StoreError::Invalid(format!(
                "a piece of {units} units carries {} checksums",
                crcs.len()
            )));
        }
        let first = u32::try_from(offset / unit).unwrap_or(u32::MAX);
        // each unit's checksum bound to its place: the sender's, or computed now
        for (index, bytes) in bytes.chunks(unit as usize).enumerate() {
            let at = first + index as u32;
            self.header.crcs[at as usize] = match crcs.get(index) {
                Some(crc) => super::chunk::bound_crc(*crc, &self.header.id, at),
                None => unit_crc(bytes, &self.header.id, at).await,
            };
        }
        Ok(first)
    }

    /// Finish the chunk: its header written, made durable, then staged under a record or placed
    ///
    /// # Arguments
    ///
    /// * `place` - Whether it goes straight into place, a put's chunk, rather than being staged
    ///
    /// # Errors
    ///
    /// A chunk missing units that hold bytes, a failed device, or a full journal.
    pub async fn finish(self, place: bool) -> Result<(), StoreError> {
        let slice = self.slice.clone();
        let finished = self.finish_inner(place).await;
        slice.guard(finished)
    }

    /// Finish the chunk, failing nothing itself, giving its file back on any failure
    ///
    /// # Arguments
    ///
    /// * `place` - Whether it goes straight into place
    async fn finish_inner(self, place: bool) -> Result<(), StoreError> {
        let WholeWriter {
            slice,
            header,
            file_bytes,
            number,
            file,
            next,
            fresh,
        } = self;
        // every unit holding bytes has to have arrived
        let unit = u64::from(header.unit_bytes);
        let held = header.length.div_ceil(unit) * unit;
        if next < held {
            close(file).await;
            slice.release_whole(file_bytes, number, fresh);
            return Err(StoreError::Invalid(format!(
                "a whole chunk holding {} bytes was given {next}",
                header.length
            )));
        }
        let made = async {
            // a fresh file's units past its bytes, to its class, written as zeros, so no extent of
            // it is left unwritten and a later write in place stays inside written ones (F96)
            let padding = file_bytes.saturating_sub(BLOCK as u64 + held);
            if fresh && padding > 0 {
                write_zeros(&file, BLOCK as u64 + held, padding).await?;
            }
            write_all(&file, &header.encode(), 0).await?;
            // its bytes durable: a fresh file by its own sync, which commits its allocation too;
            // a file of the pool by its own sync, by the device's shared flush, or for a stage by
            // the record's sync, which flushes the device on XFS and ext4 only while the journal is
            // on it; a journal on another device flushes that device, so a stage beside one joins
            // its own device's shared flush first (F91)
            match (fresh, slice.device.apply_sync, place) {
                (true, _, _) | (false, ApplySync::Each, _) => file
                    .fdatasync()
                    .await
                    .map_err(|error| failed("syncing a whole chunk", error)),
                (false, ApplySync::Shared, true) => slice
                    .flush
                    .durable()
                    .await
                    .map_err(|error| StoreError::DeviceFailed { reason: error }),
                (false, ApplySync::Shared, false) if slice.device.journal_elsewhere() => slice
                    .flush
                    .durable()
                    .await
                    .map_err(|error| StoreError::DeviceFailed { reason: error }),
                (false, ApplySync::Shared, false) => Ok(()),
            }
        }
        .await;
        close(file).await;
        if let Err(error) = made {
            slice.release_whole(file_bytes, number, fresh);
            return Err(error);
        }
        // where the whole file is now: a fresh one under its temporary name
        let source = match fresh {
            true => slice.free.temporary(file_bytes, number),
            false => slice.free.path(file_bytes, number),
        };
        if place {
            // a put's chunk, or a rebuild's, is renamed over its path with its chunk's lock held,
            // and never over a newer one: a rebuild that lost a race with a write's apply leaves
            // the write in place (F88)
            let lock = slice.lock_of(&header.id);
            let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
            if let ChunkState::At(have, _) = slice.chunk_state(&header.id).await? {
                // never over a newer one, nor over another label at its sequence (F93)
                if have.label.ahead_of(header.label) {
                    slice.release_whole(file_bytes, number, fresh);
                    return Err(StoreError::CannotMake {
                        expects: Some(header.label),
                        have: Some(have.label),
                    });
                }
            }
            if let Err(error) = slice.apply_whole(&header.id, file_bytes, &source).await {
                slice.release_whole(file_bytes, number, fresh);
                return Err(error);
            }
        } else {
            // a fresh file is taken into the pool, whole and synced, before a record names it
            if fresh {
                if let Err(error) = slice.free.adopt(file_bytes, number).await {
                    slice.release_whole(file_bytes, number, fresh);
                    return Err(error);
                }
            }
            // a stage's record, naming the file, is what makes it durable as a stage
            let mut record = Record {
                seq: 0,
                id: header.id,
                makes: header.label,
                expects: None,
                also: Vec::new(),
                kind: RecordKind::Whole {
                    file_bytes,
                    file: number,
                },
            };
            let location = match slice.journal.append(&mut record, &[]).await {
                Ok(location) => location,
                Err(error) => {
                    // the file is the pool's now, whole, whichever way it came
                    slice.free.give_back(file_bytes, number);
                    return Err(error);
                }
            };
            let staged = Staged {
                record,
                location,
                at: std::time::Instant::now(),
                recovered: false,
            };
            slice.state.staged_bytes.fetch_add(staged.bytes(), Ordering::Relaxed);
            slice.staged.borrow_mut().entry(header.id).or_default().push(staged);
        }
        Ok(())
    }

    /// Give up on the chunk: its file closed, and given back to the pool or, made for it, removed
    pub async fn abort(self) {
        close(self.file).await;
        self.slice.release_whole(self.file_bytes, self.number, self.fresh);
    }

    /// The chunk being written
    #[must_use]
    pub fn chunk(&self) -> &ChunkId {
        &self.header.id
    }
}

/// What a listing found of one chunk before its header is read
#[derive(Debug, Clone, Copy)]
enum Listing {
    /// A file in `chunks/`, whose header says its label and length
    Placed,
    /// A whole chunk staged and not yet applied, with the label it stages
    Staged(Label),
    /// A chunk set aside, with the label its header carried (F89)
    Quarantined(Label),
}

/// Every chunk a slice's `quarantine/` holds, read with blocking calls at its open (F89)
///
/// A file there is `<consumer>/<tablet>-<sub>/<object>/<stripe>.<position>.<n>`, a chunk's own
/// path with the count of copies set aside before it; its label is its header's, the default for
/// a header that does not read.
///
/// # Arguments
///
/// * `root` - The slice's `quarantine/` directory
fn quarantined_under(root: &Path) -> BTreeMap<ChunkId, Vec<Quarantined>> {
    // the four levels a chunk's path has, then its files
    let mut found: BTreeMap<ChunkId, Vec<(usize, Quarantined)>> = BTreeMap::new();
    let entries = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| entries.filter_map(Result::ok).map(|entry| entry.path()).collect())
            .unwrap_or_default()
    };
    for consumer_dir in entries(root) {
        let Some(consumer) = name_of(&consumer_dir).and_then(|name| u64::from_str_radix(&name, 16).ok()) else {
            continue;
        };
        for pg_dir in entries(&consumer_dir) {
            let Some((tablet, sub)) = name_of(&pg_dir).and_then(|name| {
                let (tablet, sub) = name.split_once('-')?;
                Some((u16::from_str_radix(tablet, 16).ok()?, u16::from_str_radix(sub, 16).ok()?))
            }) else {
                continue;
            };
            for object_dir in entries(&pg_dir) {
                let Some(object) = name_of(&object_dir).and_then(|name| u128::from_str_radix(&name, 16).ok()) else {
                    continue;
                };
                for file in entries(&object_dir) {
                    // `<stripe>.<position>.<n>`
                    let Some((stripe, position, n)) = name_of(&file).and_then(|name| {
                        let mut parts = name.splitn(3, '.');
                        Some((
                            u64::from_str_radix(parts.next()?, 16).ok()?,
                            parts.next()?.parse::<u8>().ok()?,
                            parts.next()?.parse::<usize>().ok()?,
                        ))
                    }) else {
                        continue;
                    };
                    // its header block alone
                    let label = std::fs::File::open(&file)
                        .ok()
                        .and_then(|mut opened| {
                            let mut block = vec![0u8; BLOCK];
                            std::io::Read::read_exact(&mut opened, &mut block).ok()?;
                            ChunkHeader::decode(&block).ok()
                        })
                        .map(|header| header.label)
                        .unwrap_or_default();
                    let id = ChunkId {
                        consumer,
                        pg: crate::shared::poolmap::PlacementGroup { tablet, sub },
                        object,
                        stripe,
                        position,
                    };
                    found.entry(id).or_default().push((n, Quarantined { path: file, label }));
                }
            }
        }
    }
    // each chunk's copies in the order they were set aside
    found
        .into_iter()
        .map(|(id, mut list)| {
            list.sort_by_key(|(n, _)| *n);
            (id, list.into_iter().map(|(_, set_aside)| set_aside).collect())
        })
        .collect()
}

/// A path's last component, as text
///
/// # Arguments
///
/// * `path` - The path
fn name_of(path: &Path) -> Option<String> {
    path.file_name().and_then(|name| name.to_str()).map(str::to_string)
}

/// The chunk files under a slice's `chunks/` directory and their bytes, listed with blocking calls
///
/// Consumer, placement group, object, then the chunks: four levels.
///
/// # Arguments
///
/// * `chunks` - The slice's `chunks/` directory
fn chunks_under(chunks: &std::path::Path) -> (u64, u64) {
    // the three levels of directories, then the files in the last
    let mut level = vec![chunks.to_path_buf()];
    for _ in 0..3 {
        level = level
            .iter()
            .filter_map(|dir| std::fs::read_dir(dir).ok())
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
    }
    // each file counted, with its length
    level
        .into_iter()
        .filter_map(|dir| std::fs::read_dir(dir).ok())
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .fold((0, 0), |(count, bytes), meta| (count + 1, bytes + meta.len()))
}

mod reclaim;

pub use reclaim::SEAL_LAPSE;

#[cfg(test)]
mod tests;
