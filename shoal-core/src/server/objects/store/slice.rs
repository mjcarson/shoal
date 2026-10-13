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
//! and a whole chunk was written into its pool's file at its stage; a chunk's first write stages it
//! whole, so an apply never makes a file.
//!
//! **A record stays while a later one stands on it**: an apply of a record whose base another record
//! makes applies that one first, since a commit naming a label names the commit beneath it; nothing
//! here drops a record but its apply, its discard, or a start finding it applied.
//!
//! **No unit that fails its checksum is returned or written**: a read verifies every unit before a
//! byte of it is returned, and an apply verifies every unit of its record before it writes one.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use glommio::io::DmaFile;
use glommio::sync::RwLock;
use glommio::TaskQueueHandle;

use super::chunk::{unit_crc, ChunkHeader, ChunkId, HeaderError, BLOCK};
use super::free::FreePool;
use super::journal::{Journal, Location};
use super::record::{classify, Outcome, Record, RecordKind, MAX_RECORD_UNITS};
use super::sync::Committer;
use super::{close, failed, open, read_exact, sync_dir, write_all, zero_fill, StoreError, STEP};
use crate::server::devices::{DeviceState, SliceState};
use crate::shared::devices::ApplySync;
use crate::shared::objects::Label;
use crate::shared::pools::MAX_CHUNK_UNITS;

/// How a slice is kept
#[derive(Debug, Clone)]
pub struct SliceOptions {
    /// The bytes of one journal segment
    pub segment_bytes: u64,
    /// The files written ahead it keeps for each chunk length
    pub free_files: u32,
    /// The chunk lengths it keeps files ready for, from the pools its device serves
    pub chunk_lengths: Vec<u64>,
    /// The budget its read and apply buffers are drawn from, its owning executor's
    pub budget: Option<Arc<crate::server::objects::budget::Budget>>,
    /// Whether a chunk's state before its last apply is kept, for a reader one write behind
    /// ([F86](../../../../../docs/src/features/replicated-pools.md)); off only to price it
    pub keep_previous: bool,
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

/// What a read answers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAnswer {
    /// The range's bytes, every unit of them verified
    Bytes(Vec<u8>),
    /// The slice cannot make the label asked for; this is the label its chunk carries
    Label(Option<Label>),
}

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
/// aborted, which gives its file back to the pool.
pub struct WholeWriter {
    /// The slice it writes into
    slice: Rc<Slice>,
    /// The chunk's header as it will be written, its unit checksums filled as pieces arrive
    header: ChunkHeader,
    /// The length of the pool's file it was given
    file_bytes: u64,
    /// The number of that file
    number: u64,
    /// The file, open
    file: Rc<DmaFile>,
    /// Where the next piece starts in the chunk
    next: u64,
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
    /// Its pool of files written ahead
    free: FreePool,
    /// The committer of the file a shared flush syncs
    flush: Rc<Committer>,
    /// The records it holds, by chunk, oldest first
    staged: RefCell<BTreeMap<ChunkId, Vec<Staged>>>,
    /// Each chunk's lock, so a read and an apply never overlap
    locks: RefCell<HashMap<ChunkId, Rc<RwLock<()>>>>,
    /// Whether a refill of the pool is running
    refilling: Cell<bool>,
    /// The queue a refill runs on
    background: Option<TaskQueueHandle>,
    /// Each chunk's state before its last apply, for a reader one write behind (F86)
    previous: RefCell<HashMap<ChunkId, Previous>>,
    /// When kept states past [`PREVIOUS_FOR`] were last let go
    previous_swept: Cell<std::time::Instant>,
}

impl Slice {
    /// Open a slice on the executor that owns it: its journal replayed, its records recovered
    ///
    /// # Arguments
    ///
    /// * `device` - The device it is on
    /// * `state` - Its figures and directory
    /// * `options` - How it is kept
    /// * `background` - The queue its pool is refilled on, or the current one
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
    /// # Arguments
    ///
    /// * `device` - The device it is on
    /// * `state` - Its figures and directory
    /// * `options` - How it is kept
    /// * `background` - The queue its pool is refilled on
    async fn open_inner(
        device: Arc<DeviceState>,
        state: Arc<SliceState>,
        options: SliceOptions,
        background: Option<TaskQueueHandle>,
    ) -> Result<Rc<Slice>, StoreError> {
        if let Some(reason) = device.failed() {
            return Err(StoreError::DeviceFailed { reason });
        }
        let dir = state.path.clone();
        crate::server::faults::guard(&dir).map_err(|error| failed("opening the slice", error))?;
        for sub in ["journal", "chunks"] {
            std::fs::create_dir_all(dir.join(sub)).map_err(|error| failed("making the slice", error))?;
        }
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
        let (journal, found) = Journal::open(state.id, &dir.join("journal"), options.segment_bytes).await?;
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
            refilling: Cell::new(false),
            background,
            previous: RefCell::new(HashMap::new()),
            previous_swept: Cell::new(std::time::Instant::now()),
        });
        slice.recover(found).await?;
        slice.count_chunks();
        slice.refill();
        Ok(slice)
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
        let block = read_exact(&file, 0, BLOCK).await?;
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
            let bytes = read_exact(file, header.offset_of(*unit), header.unit_bytes as usize).await?;
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
                        let beneath = kept
                            .iter()
                            .any(|other| Some(other.record.makes) == staged.record.expects);
                        match classify(staged.record.makes, staged.record.expects, chunk, covered, beneath) {
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
                            // never acknowledged: its file goes back to the pool
                            self.free.give_back(*file_bytes, *file);
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
        let block = read_exact(&file, 0, BLOCK).await?;
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
            let count = glommio::executor().spawn_blocking(move || chunks_under(&path)).await;
            state.chunks.fetch_add(count, Ordering::Relaxed);
        };
        // on the background queue, below every foreground read and stage
        let _ = match self.background {
            Some(queue) => glommio::spawn_local_into(counting, queue).map(glommio::Task::detach),
            None => Ok::<_, glommio::GlommioError<()>>(glommio::spawn_local(counting).detach()),
        };
    }

    /// Fill the pool to its target in the background, while the device is above its reserve
    pub fn refill(self: &Rc<Self>) {
        if self.refilling.replace(true) {
            return;
        }
        let slice = self.clone();
        let work = async move {
            loop {
                let wanted = slice.free.wanted();
                let Some((length, _)) = wanted.first().copied() else {
                    break;
                };
                if slice.serving().is_err() || !slice.has_room(length) {
                    break;
                }
                if let Err(error) = slice.free.make_one(length).await {
                    let _ = slice.guard::<()>(Err(error));
                    break;
                }
            }
            slice.refilling.set(false);
        };
        let spawned = match self.background {
            Some(queue) => glommio::spawn_local_into(work, queue).map(glommio::Task::detach),
            None => Ok::<_, glommio::GlommioError<()>>(glommio::spawn_local(work).detach()),
        };
        if spawned.is_err() {
            self.refilling.set(false);
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
        let mut writer = self
            .begin_whole(stage.id, stage.label, stage.chunk_bytes, stage.unit_bytes, stage.length)
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
    ///
    /// # Errors
    ///
    /// A failed device, a pool empty past the device's reserve, or a geometry no slice serves.
    pub async fn begin_whole(
        self: &Rc<Self>,
        id: ChunkId,
        label: Label,
        chunk_bytes: u64,
        unit_bytes: u32,
        length: u64,
    ) -> Result<WholeWriter, StoreError> {
        self.serving()?;
        Self::check_geometry(chunk_bytes, unit_bytes)?;
        if length > chunk_bytes {
            return Err(StoreError::Invalid(format!(
                "a whole chunk of {chunk_bytes} bytes cannot hold {length}"
            )));
        }
        // a file of the pool, or one made now for this chunk alone while the device has room
        // above its reserve, which no other stage can take before it is written
        let file_bytes = BLOCK as u64 + chunk_bytes;
        self.free.serve(file_bytes);
        let number = match self.free.take(file_bytes) {
            Some(number) => number,
            None if self.has_room(file_bytes) => self.guard(self.free.make_held(file_bytes).await)?,
            None => {
                return Err(StoreError::NoSpace {
                    free: crate::server::control::capacity::free_bytes(&self.state.path).unwrap_or(0),
                    reserve: self.device.reserve,
                })
            }
        };
        let file = match open(&self.free.path(file_bytes, number), false).await {
            Ok(file) => file,
            Err(error) => {
                self.free.give_back(file_bytes, number);
                return Err(self.guard::<()>(Err(error)).unwrap_err());
            }
        };
        let mut header = ChunkHeader::empty(id, chunk_bytes, unit_bytes);
        header.label = label;
        header.length = length;
        Ok(WholeWriter {
            slice: self.clone(),
            header,
            file_bytes,
            number,
            file,
            next: 0,
        })
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
    /// that chain from its label to the one wanted laid over it.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label
    /// * `depth` - How many records deep the search is, to bound it
    fn chain<'a>(
        &'a self,
        id: &'a ChunkId,
        label: Label,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Chain>, StoreError>> + 'a>> {
        Box::pin(async move {
            if depth > MAX_RECORD_UNITS {
                return Ok(None);
            }
            // the chunk itself
            if let ChunkState::At(header, file) = self.chunk_state(id).await? {
                if header.label == label {
                    return Ok(Some(Chain {
                        header: header.clone(),
                        base: Some((file, header)),
                        overlays: Vec::new(),
                        held: BTreeMap::new(),
                    }));
                }
            }
            let Some(staged) = self.staged_making(id, label) else {
                return Ok(None);
            };
            match &staged.record.kind {
                // a staged whole chunk stands alone
                RecordKind::Whole { file_bytes, file } => {
                    let path = self.free.path(*file_bytes, *file);
                    let file = open(&path, false).await?;
                    let block = read_exact(&file, 0, BLOCK).await?;
                    let header = ChunkHeader::decode(&block).map_err(|error| {
                        StoreError::Invalid(format!("a staged chunk's header does not read: {error:?}"))
                    })?;
                    Ok(Some(Chain {
                        header: header.clone(),
                        base: Some((file, header)),
                        overlays: Vec::new(),
                        held: BTreeMap::new(),
                    }))
                }
                // part of a chunk stands on the label it expects
                RecordKind::Units { header, .. } => {
                    let Some(expects) = staged.record.expects else {
                        return Ok(None);
                    };
                    let Some(mut beneath) = self.chain(id, expects, depth + 1).await? else {
                        return Ok(None);
                    };
                    beneath.header = header.clone();
                    beneath.overlays.push(staged);
                    Ok(Some(beneath))
                }
            }
        })
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
    /// changes nothing.
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
        let applied = self.apply_inner(id, label).await;
        self.guard(applied)
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
        self.apply_locked(&id, label, 0).await
    }

    /// Apply a staged write with its chunk's lock held
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label it makes
    /// * `depth` - How many records beneath the first this is
    fn apply_locked<'a>(
        self: &'a Rc<Self>,
        id: &'a ChunkId,
        label: Label,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + 'a>> {
        Box::pin(async move {
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
                    self.apply_whole(id, *file_bytes, *file).await?;
                }
                RecordKind::Units { .. } => {
                    // the record beneath first, when the chunk is not yet at its base
                    let at = match self.chunk_state(id).await? {
                        ChunkState::At(header, _) => Some(header.label),
                        _ => None,
                    };
                    if let Some(expects) = staged.record.expects {
                        if at != Some(expects) && at != Some(label) && depth < MAX_RECORD_UNITS {
                            if self.staged_making(id, expects).is_some() {
                                self.apply_locked(id, expects, depth + 1).await?;
                            } else if at.is_some() {
                                return Err(StoreError::CannotMake {
                                    expects: Some(expects),
                                    have: at,
                                });
                            }
                        }
                    }
                    self.write_units(&staged).await?;
                }
            }
            // a test can end the holder once the apply is durable and before its record goes
            crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::AppliedNotDropped);
            // durable, so the record goes
            self.drop_record(id, label);
            Ok(())
        })
    }

    /// Rename a staged whole chunk's file over its chunk, durably
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `file_bytes` - The file's length
    /// * `number` - The file's number
    async fn apply_whole(&self, id: &ChunkId, file_bytes: u64, number: u64) -> Result<(), StoreError> {
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
        // the chunk as it was, linked aside before the rename replaces it, for a reader one write
        // behind; the rename itself stays the one atomic step (F86)
        let kept = match fresh {
            true => None,
            false => self.link_previous(id, &target).await,
        };
        let source = self.free.path(file_bytes, number);
        glommio::io::rename(&source, &target)
            .await
            .map_err(|error| failed("renaming a staged chunk into place", error))?;
        match kept {
            Some(path) => self.keep_previous(*id, PreviousBytes::File(path)),
            None => self.drop_previous(id),
        }
        sync_dir(&parent).await?;
        if let Some(pool) = source.parent() {
            sync_dir(pool).await?;
        }
        if fresh {
            self.state.chunks.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
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
        let bytes = self
            .journal
            .read_units(staged.location, units.len(), header.unit_bytes)
            .await?;
        for (unit, bytes) in units.iter().zip(&bytes) {
            if unit_crc(bytes, &header.id, *unit).await != header.crcs[*unit as usize] {
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
        // the units about to be overwritten and the header they verify under, for a reader one
        // write behind, when the budget has room for them (F86)
        let kept = self.read_previous_units(&file, units).await;
        for (written, (unit, bytes)) in units.iter().zip(&bytes).enumerate() {
            // a test can end the holder half way through an apply's units
            if written * 2 >= units.len() && written > 0 {
                crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::MidApply);
            }
            write_all(&file, bytes, header.offset_of(*unit)).await?;
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
        let block = read_exact(file, 0, BLOCK).await.ok()?;
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
                let block = read_exact(&file, 0, BLOCK).await?;
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
        let read = self.read_inner(id, offset, len, &[label], true).await;
        self.guard(read).map(|(_, answer)| answer)
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
    async fn read_inner(
        self: &Rc<Self>,
        id: ChunkId,
        offset: u64,
        len: u64,
        labels: &[Label],
        repair: bool,
    ) -> Result<(Option<Label>, ReadAnswer), StoreError> {
        self.serving()?;
        // what a read holds: the range, and the units it is cut from
        let _drawn = self.draw(len.saturating_add(2 * STEP as u64)).await?;
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
                    if let Ok(bytes) = self.read_chain(&chain, offset, len).await? {
                        self.state.read_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        return Ok((Some(label), ReadAnswer::Bytes(bytes)));
                    }
                }
                let have = match self.chunk_state(&id).await? {
                    ChunkState::At(header, _) => Some(header.label),
                    _ => None,
                };
                return Ok((None, ReadAnswer::Label(have)));
            };
            match self.read_chain(&chain, offset, len).await? {
                Ok(bytes) => {
                    self.state.read_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok((Some(label), ReadAnswer::Bytes(bytes)));
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
                            staged.record.expects == Some(at)
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
                Box::pin(self.read_inner(id, offset, len, labels, false)).await
            }
            _ => Err(StoreError::Corrupt { unit: torn }),
        }
    }

    /// Read a range through a chain, every unit verified; the unit that failed if one did
    ///
    /// # Arguments
    ///
    /// * `chain` - Where the label's bytes come from
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    async fn read_chain(&self, chain: &Chain, offset: u64, len: u64) -> Result<Result<Vec<u8>, u32>, StoreError> {
        let header = &chain.header;
        let end = (offset + len).min(header.chunk_bytes);
        if offset >= end {
            return Ok(Ok(Vec::new()));
        }
        let unit_bytes = u64::from(header.unit_bytes);
        let mut out = Vec::with_capacity(usize::try_from(end - offset).unwrap_or(0));
        // each record's units, read once, by unit
        let mut laid: BTreeMap<u32, (Vec<u8>, u64)> = BTreeMap::new();
        for staged in &chain.overlays {
            if let RecordKind::Units { units, header: made } = &staged.record.kind {
                let wanted: Vec<u32> = header.units_of(offset, end - offset).collect();
                if units.iter().all(|unit| !wanted.contains(unit)) {
                    continue;
                }
                let bytes = self
                    .journal
                    .read_units(staged.location, units.len(), made.unit_bytes)
                    .await?;
                for (unit, bytes) in units.iter().zip(bytes) {
                    laid.insert(*unit, (bytes, made.crcs[*unit as usize]));
                }
            }
        }
        for unit in header.units_of(offset, end - offset) {
            // a unit holding no bytes under the label's header reads as zeros
            let bytes = if !header.holds(unit) {
                vec![0u8; header.unit_bytes as usize]
            } else if let Some(bytes) = chain.held.get(&unit) {
                // a unit held in memory, verified as one read from a file is
                if unit_crc(bytes, &header.id, unit).await != header.crcs[unit as usize] {
                    return Ok(Err(unit));
                }
                bytes.clone()
            } else if let Some((bytes, crc)) = laid.get(&unit) {
                if unit_crc(bytes, &header.id, unit).await != *crc {
                    return Ok(Err(unit));
                }
                bytes.clone()
            } else {
                let Some((file, base)) = &chain.base else {
                    return Ok(Err(unit));
                };
                let bytes = read_exact(file, base.offset_of(unit), header.unit_bytes as usize).await?;
                if unit_crc(&bytes, &header.id, unit).await != base.crcs[unit as usize] {
                    return Ok(Err(unit));
                }
                bytes
            };
            // the part of the unit the range covers
            let unit_start = u64::from(unit) * unit_bytes;
            let from = offset.max(unit_start) - unit_start;
            let to = end.min(unit_start + unit_bytes) - unit_start;
            out.extend_from_slice(&bytes[from as usize..to as usize]);
        }
        Ok(Ok(out))
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
        // the first the slice can make is read; a torn unit is mended as a read of it would be
        let read = self.read_inner(id, offset, len, labels, true).await;
        match self.guard(read)? {
            (Some(label), ReadAnswer::Bytes(bytes)) => Ok(Ok((label, bytes))),
            (_, ReadAnswer::Label(have)) => Ok(Err(have)),
            (None, ReadAnswer::Bytes(_)) => Err(StoreError::Invalid(
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

    /// The records no row has decided yet: staged at least `older_than` ago, or found by a start
    ///
    /// What the node asks each one's row about, so a stage whose stager died is decided by a
    /// committed fact ([P16](../../../../../docs/src/object-storage/contract.md)).
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

    /// The label and length a chunk's header carries on the device, if it is there and reads
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    ///
    /// # Errors
    ///
    /// A failed device.
    pub async fn chunk_header(&self, id: &ChunkId) -> Result<Option<(Label, u64)>, StoreError> {
        Ok(match self.chunk_state(id).await? {
            ChunkState::At(header, _) => Some((header.label, header.length)),
            _ => None,
        })
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

    /// Write the next piece, failing nothing itself
    ///
    /// # Arguments
    ///
    /// * `offset` - Where in the chunk the piece starts
    /// * `crcs` - The units' unbound checksums, or none
    /// * `bytes` - The piece's units
    async fn piece_inner(&mut self, offset: u64, crcs: &[u64], bytes: &[u8]) -> Result<(), StoreError> {
        let unit = u64::from(self.header.unit_bytes);
        // pieces arrive in order and carry whole units inside the chunk
        if offset != self.next
            || bytes.is_empty()
            || !(bytes.len() as u64).is_multiple_of(unit)
            || offset + bytes.len() as u64 > self.header.chunk_bytes
        {
            return Err(StoreError::Invalid(format!(
                "a piece of {} bytes at {offset} is not the next whole units of a chunk of {} \
                 written to {}",
                bytes.len(),
                self.header.chunk_bytes,
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
        write_all(&self.file, bytes, self.header.offset_of(first)).await?;
        self.next += bytes.len() as u64;
        Ok(())
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
        } = self;
        // every unit holding bytes has to have arrived
        let unit = u64::from(header.unit_bytes);
        if next < header.length.div_ceil(unit) * unit {
            close(file).await;
            slice.free.give_back(file_bytes, number);
            return Err(StoreError::Invalid(format!(
                "a whole chunk holding {} bytes was given {next}",
                header.length
            )));
        }
        let made = async {
            write_all(&file, &header.encode(), 0).await?;
            // its bytes durable: by its own sync, by the device's shared flush, or for a stage by
            // the record's sync, which flushes the device on XFS and ext4
            match (slice.device.apply_sync, place) {
                (ApplySync::Each, _) => file
                    .fdatasync()
                    .await
                    .map_err(|error| failed("syncing a whole chunk", error)),
                (ApplySync::Shared, true) => slice
                    .flush
                    .durable()
                    .await
                    .map_err(|error| StoreError::DeviceFailed { reason: error }),
                (ApplySync::Shared, false) => Ok(()),
            }
        }
        .await;
        close(file).await;
        if let Err(error) = made {
            slice.free.give_back(file_bytes, number);
            return Err(error);
        }
        if place {
            // a put's chunk is renamed over its path with its chunk's lock held
            let lock = slice.lock_of(&header.id);
            let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
            if let Err(error) = slice.apply_whole(&header.id, file_bytes, number).await {
                slice.free.give_back(file_bytes, number);
                return Err(error);
            }
        } else {
            // a stage's record, naming the file, is what makes it durable as a stage
            let mut record = Record {
                seq: 0,
                id: header.id,
                makes: header.label,
                expects: None,
                kind: RecordKind::Whole {
                    file_bytes,
                    file: number,
                },
            };
            let location = match slice.journal.append(&mut record, &[]).await {
                Ok(location) => location,
                Err(error) => {
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
        slice.refill();
        Ok(())
    }

    /// Give up on the chunk: its file closed and given back to the pool
    pub async fn abort(self) {
        close(self.file).await;
        self.slice.free.give_back(self.file_bytes, self.number);
    }

    /// The chunk being written
    #[must_use]
    pub fn chunk(&self) -> &ChunkId {
        &self.header.id
    }
}

/// The chunk files under a slice's `chunks/` directory, listed with blocking calls
///
/// Consumer, placement group, object, then the chunks: four levels.
///
/// # Arguments
///
/// * `chunks` - The slice's `chunks/` directory
fn chunks_under(chunks: &std::path::Path) -> u64 {
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
    level
        .into_iter()
        .map(|dir| std::fs::read_dir(dir).map_or(0, |entries| entries.count() as u64))
        .sum()
}

#[cfg(test)]
mod tests;
