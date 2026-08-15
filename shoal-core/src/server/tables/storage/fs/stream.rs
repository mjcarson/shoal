//! Utilities to efficiently and performantly streams data to and from  disk
use glommio::io::{DmaBuffer, DmaFile, OpenOptions};
use glommio::task::JoinHandle;
use kanal::AsyncSender;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::Rc;
use tracing::instrument;

use super::conf::Durability;
use crate::server::messages::ServerMsg;
#[cfg(feature = "stage-profile")]
use crate::server::stage_profile::{StageStamps, Stamp};
use crate::server::ServerError;
use crate::server::database::ShoalDatabase;

/// The sentinel written in place of a size header at the start of a pad region
///
/// O_DIRECT requires every write to have a block aligned offset and length, but
/// records are arbitrarily sized. So when a partial buffer is flushed we pad it up
/// to the next block boundary. That padding has to be distinguishable from the
/// zeroed tail of a partly filled log, since [`IntentLogReader`] treats a zero size
/// header as the end of the log. This sentinel marks "skip to the next block
/// boundary and keep reading" instead.
///
/// [`IntentLogReader`]: super::reader::IntentLogReader
pub const PAD_SENTINEL: u64 = u64::MAX;

/// The number of bytes our pad sentinel takes up
pub const PAD_SENTINEL_SIZE: usize = 8;

/// Round a value up to the next multiple of an alignment
///
/// # Arguments
///
/// * `value` - The value to round up
/// * `alignment` - The alignment to round up too
pub fn align_up(value: usize, alignment: usize) -> usize {
    // round up to the next alignment boundary
    value.div_ceil(alignment) * alignment
}

/// Pad a staged buffer up to an aligned length so it can be written with O_DIRECT
///
/// Returns the aligned length to write. When any padding is required the pad region
/// starts with [`PAD_SENTINEL`] so the reader can tell it apart from the zeroed tail
/// of a partly filled log.
///
/// The caller must guarantee `bytes` has at least one alignment block of slack past
/// `buff_pos`, which [`StreamWriter::usable`] enforces.
///
/// # Arguments
///
/// * `bytes` - The buffer holding our staged data
/// * `buff_pos` - The number of bytes of staged data in that buffer
/// * `alignment` - The direct IO alignment to pad up too
pub fn pad_region(bytes: &mut [u8], buff_pos: usize, alignment: usize) -> usize {
    // round the data we have staged up to the next alignment boundary
    let mut padded = align_up(buff_pos, alignment);
    // bail early if we happen to already be aligned since there is nothing to pad
    if padded == buff_pos {
        return padded;
    }
    // make sure our pad region is big enough to hold our sentinel
    if padded - buff_pos < PAD_SENTINEL_SIZE {
        padded += alignment;
    }
    // zero our pad region since DMA buffers are pooled and may hold stale data
    bytes[buff_pos..padded].fill(0);
    // mark this pad region so the reader skips it instead of stopping
    bytes[buff_pos..buff_pos + PAD_SENTINEL_SIZE].copy_from_slice(&PAD_SENTINEL.to_le_bytes());
    padded
}

pub struct StreamWriterBuilder<D: ShoalDatabase> {
    /// The path to write data too
    pub path: PathBuf,
    /// The channel to send write messages over
    pub shard_local_tx: AsyncSender<ServerMsg<D>>,
    /// The default/minumum size to make our DMA buffer
    pub buffer_size: usize,
    /// Maximum number of in-flight write tasks
    pub write_behind: usize,
    /// How durable a write has to be before it can be acknowledged
    pub durability: Durability,
}

impl<D: ShoalDatabase> StreamWriterBuilder<D> {
    /// Create a new [`StreamWriterBuilder`]
    ///
    /// # Arguments
    ///
    /// * `path` - The path this stream writer should write data too when built
    pub fn new(path: impl Into<PathBuf>, shard_local_tx: AsyncSender<ServerMsg<D>>) -> Self {
        StreamWriterBuilder {
            path: path.into(),
            shard_local_tx,
            buffer_size: 4096,
            write_behind: 4,
            durability: Durability::Fsync,
        }
    }

    /// Set the buffer size to use by default
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - The buffer size to set in bytes
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the number of write behind buffers to use
    ///
    /// # Arguments
    ///
    /// * `write_behind` - The number of write behind buffers to use
    pub fn write_behind(mut self, write_behind: usize) -> Self {
        self.write_behind = write_behind;
        self
    }

    /// Set how durable a write has to be before it can be acknowledged
    ///
    /// # Arguments
    ///
    /// * `durability` - The durability level to use
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Build a [`StreamWriter`] from this [`StreamWriterBuilder`]
    #[instrument(name = "StreamWriterBuilder::build", skip_all)]
    pub async fn build(self) -> Result<StreamWriter<D>, ServerError> {
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&self.path)
            .await?;
        // get the direct IO alignment this file requires
        let alignment = file.alignment() as usize;
        // clamp our usable buffer size up to at least one aligned block
        let default_buffer_size = align_up(std::cmp::max(self.buffer_size, alignment), alignment);
        // get a buffer to write, reserving a block of slack for padding
        let buffer = file.alloc_dma_buffer(default_buffer_size + alignment);
        // build this stream writer
        let writer = StreamWriter {
            path: self.path,
            file: Rc::new(file),
            shard_local_tx: self.shard_local_tx,
            buffer,
            alignment,
            default_buffer_size,
            file_pos: 0,
            buff_pos: 0,
            state: Rc::new(RefCell::new(FlushState::default())),
            durability: self.durability,
            max_write_behind: self.write_behind,
            pending_writes: VecDeque::with_capacity(self.write_behind),
        };
        Ok(writer)
    }
}

/// How many recent writes a [`FlushState`] keeps timings for
///
/// Bounded so a long run does not grow an unbounded timeline. A window has to survive from
/// the moment its write is submitted until the last response it carries is released, which
/// is at most a couple of fdatasync round trips, so this is far more headroom than the
/// invariant needs. Records whose window had already been evicted are flagged and counted
/// rather than guessed at.
#[cfg(feature = "stage-profile")]
const TIMELINE_LEN: usize = 4096;

/// One write and the fdatasync that made it durable
///
/// A pending response knows the intent log offset it becomes durable at and nothing else, so
/// this is what turns that offset back into times. Windows are appended in submission order
/// and are therefore already sorted by `end_pos`, which is what makes the lookup at release a
/// binary search rather than a scan.
#[cfg(feature = "stage-profile")]
pub struct DurabilityWindow {
    /// The offset one past the last byte this write covers
    pub end_pos: u64,
    /// When this write was handed to io_uring
    pub submitted: Stamp,
    /// When this writes completion was observed, if it has landed
    pub completed: Option<Stamp>,
    /// When the fdatasync that covers this write claimed its slot
    ///
    /// Not the sync issued immediately after this write. [`start_sync`] group commits, so the
    /// covering sync is the first one whose target reaches `end_pos`, which can be several
    /// writes later.
    pub sync_issued: Option<Stamp>,
    /// When that fdatasync returned
    pub sync_completed: Option<Stamp>,
}

/// The write completion state shared between a [`StreamWriter`] and its detached IO tasks
///
/// Buffer writes run as detached tasks that cannot reach `&mut StreamWriter`, and
/// glommio's [`JoinHandle`] has no non blocking poll, so shared state is the only way
/// to reconcile completions without blocking the shard on device latency.
#[derive(Default)]
pub struct FlushState {
    /// The submitted but not yet retired writes, in submission order
    inflight: VecDeque<(u64, bool)>,
    /// When each recent write was submitted, landed, and was covered by an fdatasync
    ///
    /// Kept apart from `inflight` because a write leaves that queue as soon as it retires,
    /// while the response it carries is still waiting on the sync that makes it durable.
    #[cfg(feature = "stage-profile")]
    timeline: VecDeque<DurabilityWindow>,
    /// The position that all data below has been write_at completed for
    written_pos: u64,
    /// The position that all data below has been fdatasync completed for
    synced_pos: u64,
    /// The target position of our single in flight fdatasync if one exists
    syncing_to: Option<u64>,
    /// The handle for our in flight fdatasync task
    sync_handle: Option<JoinHandle<()>>,
    /// The first IO error observed by any of our background tasks
    error: Option<ServerError>,
}

impl FlushState {
    /// Track that a write has been submitted
    ///
    /// # Arguments
    ///
    /// * `end` - The position one past the last byte this write covers
    pub fn on_start(&mut self, end: u64) {
        // add this write to our in flight queue in submission order
        self.inflight.push_back((end, false));
        // open a durability window for this write so the responses it carries can later
        // find out when it was submitted, landed, and was synced
        #[cfg(feature = "stage-profile")]
        {
            // drop the oldest window if we are at our bound
            //
            // a record whose window is gone is flagged rather than interpolated, so the
            // profile says it does not know instead of inventing a number
            if self.timeline.len() >= TIMELINE_LEN {
                self.timeline.pop_front();
            }
            self.timeline.push_back(DurabilityWindow {
                end_pos: end,
                submitted: Stamp::now(),
                completed: None,
                sync_issued: None,
                sync_completed: None,
            });
        }
    }

    /// Record that a covering fdatasync reached a stage for every write it covers
    ///
    /// [`start_sync`] group commits, so one fdatasync makes every write below its target
    /// durable at once. Stamping only the newest window would attribute the whole group's
    /// wait to one write and report the rest as having no sync stage at all.
    ///
    /// # Arguments
    ///
    /// * `target` - The position this fdatasync covers
    /// * `at` - The stamp to record
    /// * `pick` - The field of a window to stamp
    #[cfg(feature = "stage-profile")]
    fn mark_sync_stage(
        &mut self,
        target: u64,
        at: Stamp,
        pick: fn(&mut DurabilityWindow) -> &mut Option<Stamp>,
    ) {
        // walk every window this sync covers
        for window in self.timeline.iter_mut() {
            // windows are in submission order, so the first one past our target ends this
            if window.end_pos > target {
                break;
            }
            // only the first sync to reach a window covers it, so never overwrite
            let slot = pick(window);
            if slot.is_none() {
                *slot = Some(at);
            }
        }
    }

    /// Look up when the write covering an intent log offset was made durable
    ///
    /// Returns `None` when the window has already been evicted, which the caller flags on
    /// the record rather than filling in.
    ///
    /// # Arguments
    ///
    /// * `pos` - The offset a response becomes durable at
    #[cfg(feature = "stage-profile")]
    pub fn window_for(&self, pos: u64) -> Option<&DurabilityWindow> {
        // windows are appended in submission order, so they are sorted by end position and
        // the covering write is the first one that reaches this offset
        let found = self.timeline.partition_point(|window| window.end_pos < pos);
        self.timeline.get(found)
    }

    /// Retire a completed write and advance our contiguous written watermark
    ///
    /// io_uring completions are not ordered, so a later write can land before an
    /// earlier one. Taking the max of what has completed would advance our watermark
    /// past data that is still in flight, so instead we only advance over writes that
    /// have completed contiguously from the front of the queue.
    ///
    /// # Arguments
    ///
    /// * `end` - The position one past the last byte this write covered
    pub fn on_complete(&mut self, end: u64) {
        // find this writes slot and mark it as complete
        if let Some(slot) = self.inflight.iter_mut().find(|(pos, _)| *pos == end) {
            slot.1 = true;
        }
        // close out the write half of this writes durability window
        #[cfg(feature = "stage-profile")]
        if let Some(window) = self
            .timeline
            .iter_mut()
            .find(|window| window.end_pos == end)
        {
            window.completed = Some(Stamp::now());
        }
        // pop completed writes from the front so our watermark stays contiguous
        while matches!(self.inflight.front(), Some((_, true))) {
            // retire this completed write
            if let Some((pos, _)) = self.inflight.pop_front() {
                // advance our watermark, which is monotonic by construction
                self.written_pos = pos;
            }
        }
    }

    /// Record the first IO error one of our background tasks hit
    ///
    /// # Arguments
    ///
    /// * `error` - The error to record
    pub fn record_error(&mut self, error: ServerError) {
        // only keep the first error since later ones are likely fallout from it
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// Get the position that all data below has been written to disk for
    pub fn written_pos(&self) -> u64 {
        self.written_pos
    }

    /// Record that all data below a position is now durable
    ///
    /// # Arguments
    ///
    /// * `synced_pos` - The position to advance our synced watermark too
    pub fn mark_synced(&mut self, synced_pos: u64) {
        // our synced watermark only ever moves forwards
        self.synced_pos = self.synced_pos.max(synced_pos);
    }

    /// Get the durable watermark for a durability level
    ///
    /// # Arguments
    ///
    /// * `durability` - The durability level to get a watermark for
    pub fn durable_pos(&self, durability: Durability) -> u64 {
        match durability {
            Durability::Fsync => self.synced_pos,
            Durability::Async => self.written_pos,
        }
    }
}

/// Start a background fdatasync covering all currently written data
///
/// At most one fdatasync is in flight at a time, so every write that retires while
/// one is running is covered by the next one. That makes the cost per write
/// `fdatasync latency / batch size`, and batch size grows on its own with load.
///
/// Capturing our target as `written_pos` is what makes this sound while other writes
/// are still in flight on the same ring: an fdatasync only has to cover writes that
/// completed before it was issued, and every byte below `written_pos` has.
///
/// # Arguments
///
/// * `file` - The intent log file to sync
/// * `state` - The flush state shared with our writer
/// * `shard_local_tx` - The channel to wake our shard up on
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn start_sync<D: ShoalDatabase>(
    file: Rc<DmaFile>,
    state: Rc<RefCell<FlushState>>,
    shard_local_tx: AsyncSender<ServerMsg<D>>,
) {
    // claim the sync slot and capture the position we are syncing up too
    let target = {
        // borrow our shared flush state
        let mut flush_state = state.borrow_mut();
        // bail if a sync is already in flight or we have nothing new to sync
        if flush_state.syncing_to.is_some() || flush_state.written_pos <= flush_state.synced_pos {
            return;
        }
        // claim the sync slot for our current written watermark
        flush_state.syncing_to = Some(flush_state.written_pos);
        // note that every write below this watermark is now waiting on this one sync
        //
        // this is the moment a group commit forms, so it is what separates "waiting for a
        // sync to start" from "waiting for a sync to finish"
        #[cfg(feature = "stage-profile")]
        {
            let written_pos = flush_state.written_pos;
            flush_state
                .mark_sync_stage(written_pos, Stamp::now(), |window| &mut window.sync_issued);
        }
        flush_state.written_pos
    };
    // get local copies for our background task
    let sync_file = file.clone();
    let sync_state = state.clone();
    let sync_tx = shard_local_tx.clone();
    // spawn our fdatasync as a background task
    let handle = glommio::spawn_local(async move {
        // sync our files data to stable storage
        let synced = sync_file.fdatasync().await;
        // update our shared state and check if more data landed while we synced
        let resync = {
            // borrow our shared flush state, never holding it across an await
            let mut flush_state = sync_state.borrow_mut();
            // record either our new synced watermark or the error we hit
            match synced {
                Ok(_) => {
                    flush_state.synced_pos = flush_state.synced_pos.max(target);
                    // close out the sync half of every window this one sync covered
                    #[cfg(feature = "stage-profile")]
                    flush_state
                        .mark_sync_stage(target, Stamp::now(), |window| &mut window.sync_completed);
                }
                Err(error) => flush_state.record_error(error.into()),
            }
            // release the sync slot
            flush_state.syncing_to = None;
            flush_state.written_pos > flush_state.synced_pos
        };
        // wake our shard so it can release any newly durable responses
        sync_tx.send(ServerMsg::DataFlushed).await.unwrap();
        // start another sync if more data landed while we were syncing
        if resync {
            start_sync::<D>(sync_file, sync_state, sync_tx);
        }
    })
    .detach();
    // store our sync handle so shutdown and rotation can await it
    state.borrow_mut().sync_handle = Some(handle);
}

/// Helps a StreamWriter write a block of data to disk
///
/// # Arguments
///
/// * `file` - The intent log file to write too
/// * `buff` - The buffer to write
/// * `pos` - The offset to write this buffer at
/// * `end` - The offset one past the last byte this write covers
/// * `state` - The flush state shared with our writer
/// * `durability` - How durable a write has to be before it can be acknowledged
/// * `shard_local_tx` - The channel to tell our shard this write landed on
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn write_helper<D: ShoalDatabase>(
    file: Rc<DmaFile>,
    buff: DmaBuffer,
    pos: u64,
    end: u64,
    state: Rc<RefCell<FlushState>>,
    durability: Durability,
    shard_local_tx: AsyncSender<ServerMsg<D>>,
) {
    // write this buffer to disk
    let written = file.write_at(buff, pos).await;
    {
        // borrow our shared flush state, never holding it across an await
        let mut flush_state = state.borrow_mut();
        // either retire this write or record the error it hit
        match written {
            Ok(_) => flush_state.on_complete(end),
            Err(error) => flush_state.record_error(error.into()),
        }
    }
    // if we only acknowledge synced data then let our sync task wake our shard
    if durability == Durability::Fsync {
        // fdatasync everything that has landed so far, group committing with any
        // other writes that retired while a sync was already running
        start_sync::<D>(file, state, shard_local_tx);
        return;
    }
    // tell our shard some data has been written to disk so it releases responses
    shard_local_tx.send(ServerMsg::DataFlushed).await.unwrap()
}

/// A streaming writer that utilizes high queue depth DMA to have efficient
/// and performant IO.
pub struct StreamWriter<D: ShoalDatabase> {
    /// The path to write data too
    pub path: PathBuf,
    /// The file we are streaming data too
    file: Rc<DmaFile>,
    /// The channel to send write messages over
    shard_local_tx: AsyncSender<ServerMsg<D>>,
    /// The current buffer we are writting too
    buffer: DmaBuffer,
    /// The direct IO alignment our backing file requires
    alignment: usize,
    /// The default/minumum amount of usable space to make in our DMA buffer
    ///
    /// Buffers are allocated one alignment block larger than this so a partial
    /// flush always has room for its pad region.
    default_buffer_size: usize,
    /// The current position we have written data in our file up too
    file_pos: u64,
    /// The current position we have written data in our buffer up too
    buff_pos: usize,
    /// The write completion state shared with our detached IO tasks
    state: Rc<RefCell<FlushState>>,
    /// How durable a write has to be before it can be acknowledged
    durability: Durability,
    /// Maximum number of in-flight write tasks
    max_write_behind: usize,
    /// Handles for in-flight write tasks
    pending_writes: VecDeque<JoinHandle<()>>,
}

impl<D: ShoalDatabase> StreamWriter<D> {
    /// Create a new stream writer
    pub fn builder(
        path: impl Into<PathBuf>,
        shard_local_tx: AsyncSender<ServerMsg<D>>,
    ) -> StreamWriterBuilder<D> {
        StreamWriterBuilder::new(path, shard_local_tx)
    }

    /// If at write-behind capacity, await the oldest pending write
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn flush_oldest_write(&mut self) {
        if self.pending_writes.len() >= self.max_write_behind {
            if let Some(handle) = self.pending_writes.pop_front() {
                handle.await;
            }
        }
    }

    /// Await all pending write tasks
    async fn drain_pending_writes(&mut self) {
        while let Some(handle) = self.pending_writes.pop_front() {
            handle.await;
        }
    }

    /// Get the amount of space in our current buffer that records may use
    ///
    /// This is one alignment block short of the buffers real length, since a partial
    /// flush needs somewhere to put its pad region.
    fn usable(&self) -> usize {
        self.buffer.len() - self.alignment
    }

    /// Allocate a buffer with at least this much usable space
    ///
    /// # Arguments
    ///
    /// * `usable` - The amount of usable space this buffer needs
    fn alloc_buffer(&self, usable: usize) -> DmaBuffer {
        // allocate an aligned buffer with a block of slack for our pad region
        self.file
            .alloc_dma_buffer(align_up(usable, self.alignment) + self.alignment)
    }

    /// Pad our current buffer up to an aligned length so it can be written with O_DIRECT
    ///
    /// Returns the aligned length to write.
    fn pad_buffer(&mut self) -> usize {
        // get our staged length and alignment before we borrow our buffer mutably
        let buff_pos = self.buff_pos;
        let alignment = self.alignment;
        // pad our staged data up to an aligned length
        pad_region(self.buffer.as_bytes_mut(), buff_pos, alignment)
    }

    /// Write our current buffer to our WAL via a background task
    ///
    /// # Arguments
    ///
    /// * `new_usable` - The amount of usable space our next buffer needs
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn write(&mut self, new_usable: usize) -> Result<(), ServerError> {
        // if we have nothing staged then just make sure our buffer is big enough
        if self.buff_pos == 0 {
            // grow our buffer if it can't fit our next write
            if self.usable() < new_usable {
                self.buffer = self.alloc_buffer(new_usable);
            }
            return Ok(());
        }
        // back-pressure: if at capacity, await the oldest pending write
        self.flush_oldest_write().await;
        // pad our staged data up to an aligned length
        let padded = self.pad_buffer();
        // allocate our next buffer
        let mut buff = self.alloc_buffer(new_usable);
        // swap our new buffer with our old one
        std::mem::swap(&mut self.buffer, &mut buff);
        // trim our buffer down to the aligned region we wrote data too
        buff.trim_to_size(padded);
        // get a local copy of our file, position, and shard channel
        let file = self.file.clone();
        let pos = self.file_pos;
        let shard_local_tx = self.shard_local_tx.clone();
        let state = self.state.clone();
        let durability = self.durability;
        // get the offset one past the region we are about to write
        let end = pos + padded as u64;
        // O_DIRECT requires both our offset and our length to be block aligned
        debug_assert_eq!(pos as usize % self.alignment, 0, "unaligned write offset");
        debug_assert_eq!(padded % self.alignment, 0, "unaligned write length");
        // track this write so our watermark only advances over contiguous completions
        self.state.borrow_mut().on_start(end);
        // spawn the write as a background task
        let handle = glommio::spawn_local(async move {
            // write this data to disk and tell our shard when its done
            write_helper::<D>(file, buff, pos, end, state, durability, shard_local_tx).await
        })
        .detach();
        self.pending_writes.push_back(handle);
        // increment our file position past the region we just wrote
        self.file_pos = end;
        // reset our buff position
        self.buff_pos = 0;
        Ok(())
    }

    /// Make sure we have enough space to fully store this next write
    ///
    /// # Arguments
    ///
    /// * `size` - The size to prep for
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn prep(&mut self, size: usize) -> &mut [u8] {
        // if we don't have enough usable space then write our current buffer out
        if self.usable() < size + self.buff_pos {
            // we won't have enough space to write this new data to out buffer so get a new one
            // make this new buffer big enough for our next write or bigger
            let new_usable = std::cmp::max(self.default_buffer_size, size);
            // write but not sync our current buffer to disk
            self.write(new_usable).await.unwrap();
        }
        // get a mutable ref to our buffer
        &mut self.buffer.as_bytes_mut()[self.buff_pos..self.buff_pos + size]
    }

    /// Consume some data from our buffer
    ///
    /// # Arguments
    ///
    /// * `size` - The number of bytes that have been consumed
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn consume(&mut self, size: usize) {
        // increment our buffer position by the amount of data consumed
        self.buff_pos += size;
        // if we have consumed all of our usable space then write it to disk
        if self.usable() <= self.buff_pos {
            // write but not sync our current buffer to disk
            self.write(self.default_buffer_size).await.unwrap();
        }
    }

    /// Get the current flushed position for this stream writer
    ///
    /// Every byte below this offset has been written to disk. This comes from state
    /// shared with our detached write tasks rather than from a message, so it is
    /// correct the instant an IO completes.
    pub fn get_flushed_pos(&mut self) -> u64 {
        self.state.borrow().durable_pos(self.durability)
    }

    /// Fill in the durability stages for a response that has just been released
    ///
    /// The four phases between a commit returning and its response coming back are
    /// intervals of the intent log rather than properties of a query, so they are looked up
    /// here by the offset the response parked at instead of being stamped as they happen.
    ///
    /// # Arguments
    ///
    /// * `stamps` - The stamps to fill in, already carrying their commit offset
    #[cfg(feature = "stage-profile")]
    pub fn fill_durability(&self, stamps: &mut StageStamps) {
        // find the write that carried this response
        let state = self.state.borrow();
        let Some(window) = state.window_for(stamps.commit_pos()) else {
            // this writes window has already aged out of our bounded timeline, so say we do
            // not know rather than filling in a number we would be guessing at
            stamps.set_window_missing(true);
            return;
        };
        // record when the write carrying this response was submitted
        //
        // the gap between this and `exec_done` is time the query spent staged in the DMA
        // buffer, unsubmitted, which nothing before this measured
        stamps.set_write_submitted(window.submitted);
        // record the rest of the phases, each of which a response may not have reached
        if let Some(completed) = window.completed {
            stamps.set_write_completed(completed);
        }
        if let Some(issued) = window.sync_issued {
            stamps.set_sync_issued(issued);
        }
        if let Some(synced) = window.sync_completed {
            stamps.set_sync_completed(synced);
        }
    }

    /// Check if any of our background IO tasks have failed
    ///
    /// Takes the error, so a caller that swallows it will not see it again.
    pub fn check_error(&mut self) -> Result<(), ServerError> {
        // take any error one of our background tasks recorded
        match self.state.borrow_mut().error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Get the current unflushed position for this stream writer
    pub fn get_unflushed_pos(&self) -> u64 {
        // unflushed is our file position + buffer position
        self.file_pos + self.buff_pos as u64
    }

    /// Sync any in flight buffers to disk in a non blocking manner
    ///
    /// All writes will not be durably written until the corresponding
    /// [`ServerMsg::DataFlushed`] is recieved by this shard.
    pub async fn sync(&mut self) -> Result<(), ServerError> {
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await.unwrap();
        }
        Ok(())
    }

    /// Sync our files data, blocking until it is durably on disk
    ///
    /// Unlike [`StreamWriter::sync`] this waits: it writes our staged tail, drains
    /// every in flight write, waits out any group commit already running, and then
    /// issues its own fdatasync. On return everything ever handed to this writer is
    /// durable.
    #[instrument(name = "StreamWriter::sync_blocking", skip_all, err(Debug))]
    pub async fn sync_blocking(&mut self) -> Result<(), ServerError> {
        // write out whatever is still staged in our buffer
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await?;
        }
        // wait for every in flight write to land
        self.drain_pending_writes().await;
        // wait out any group commit that was already running
        self.drain_pending_sync().await;
        // sync our files data to stable storage
        self.file.fdatasync().await?;
        // record that everything written so far is now durable
        {
            // borrow our shared flush state
            let mut flush_state = self.state.borrow_mut();
            let written = flush_state.written_pos();
            flush_state.mark_synced(written);
        }
        // surface any error our background tasks hit along the way
        self.check_error()
    }

    /// Await any in flight fdatasync task
    async fn drain_pending_sync(&mut self) {
        // take the handle for any group commit currently running
        let handle = self.state.borrow_mut().sync_handle.take();
        // wait for it to finish so its result lands in our shared state
        if let Some(handle) = handle {
            handle.await;
        }
    }

    /// Rename our current backing file and start writing to a new one
    ///
    /// # Arguments
    ///
    /// * `inactive_path` - The path to rename our current backing file to
    #[instrument(name = "StreamWriter::refresh", skip_all, err(Debug))]
    pub async fn refresh(&mut self, rename_to: &PathBuf) -> Result<u64, ServerError> {
        // flush this intent log
        self.sync_blocking().await?;
        // get the offset one past everything we ever wrote to this file
        //
        // sync_blocking just made all of it durable, so this is the right watermark
        // to report. reading our flushed watermark here would report a stale value,
        // since the completions that advance it may not have been observed yet
        let flushed_pos = self.get_unflushed_pos();
        // rename our old intent log
        glommio::io::rename(&self.path, &rename_to).await?;
        // get our parent dir and sync it so this rename is durable
        if let Some(parent) = rename_to.parent() {
            // open our parent dir
            let dir = glommio::io::Directory::open(parent).await?;
            // fsync the parent directory to ensure the rename is durable
            dir.sync().await?;
            // close our now durably synced parent dir
            dir.close().await?;
        }
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&self.path)
            .await?;
        // replace our old file with out new one
        let old_file = std::mem::replace(&mut self.file, Rc::new(file));
        // get a buffer to write
        self.buffer = self.alloc_buffer(self.default_buffer_size);
        // close our old file
        old_file.close_rc().await?;
        // reset our position counters
        self.file_pos = 0;
        self.buff_pos = 0;
        // install a fresh flush state for our new file
        //
        // positions restart at 0 in the new file, so any completion still holding the
        // old state mutates an object nothing reads rather than corrupting our watermark
        self.state = Rc::new(RefCell::new(FlushState::default()));
        Ok(flushed_pos)
    }

    /// Close this writer
    ///
    /// This fdatasyncs before closing, so a clean shutdown leaves the intent log
    /// durably on disk rather than only in the drives write cache.
    #[instrument(name = "StreamWriter::close", skip_all, err(Debug))]
    pub async fn close(mut self) -> Result<AsyncSender<ServerMsg<D>>, ServerError> {
        // wait out any group commit that was already running
        self.drain_pending_sync().await;
        // wait for every in flight write to land
        self.drain_pending_writes().await;
        // write out whatever is still staged in our buffer
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await?;
            self.drain_pending_writes().await;
        }
        // sync our files data to stable storage before we let go of it
        self.file.fdatasync().await?;
        self.file.close_rc().await?;
        Ok(self.shard_local_tx)
    }
}
