//! The shard's shared WAL: one physical log, every tablet group's logical log inside it
//!
//! On a cluster node the Raft log *is* the WAL ([F40](../../../../docs/src/features/replication.md)).
//! Every tablet group a shard hosts appends its entries here, one [`frame`] each, into segments
//! under `<latency_sensitive.path>/wal/Shard-N/`:
//!
//! ```text
//! wal/Shard-0/
//!   000000000000000001.wal   frames, appended in batches, one fdatasync per batch
//!   000000000000000002.wal   the active segment, once the first rotated
//!   checkpoint.json          per group, the log id its table's archives are complete to
//! ```
//!
//! # Group commit across groups
//!
//! An append from any group lands in the batch that is open; the writer task takes a whole
//! batch, writes it with one `write_at`, syncs it with one `fdatasync`, and only then completes
//! every [`IOFlushed`] in it - which is what openraft's quorum counts, and what
//! [P3](../../../../docs/src/distributed/protocol.md) requires of a durable vote. One batch is in
//! flight at a time; everything appended meanwhile becomes the next one. That is
//! [Q2](../../../../docs/src/distributed/protocol.md)'s answer to fan-in: a shard hosting
//! sixty-four groups pays one sync per batch, not sixty-four.
//!
//! # Invariants
//!
//! **`append` returns before the flush, and the cache holds the unflushed tail.** openraft reads
//! an entry the moment `append` returns - to replicate it, and on a follower to apply it once the
//! cluster commits it, which can happen before the follower's own sync - so every entry stays in
//! the cache at least until its bytes are durable, and is only ever read from the file below the
//! durable watermark.
//!
//! **Frames are replayed in file order, and a later frame wins.** A truncate frame drops every
//! index above its log id at the point it appears; an entry appended after it at one of those
//! indexes supersedes the old frame. An open rebuilds the index by walking every segment in
//! generation order, which is why nothing is ever edited in place.
//!
//! **A segment is deleted by the shard loop and nobody else.** The store records what was purged
//! and drops index entries; whether a file can go depends on every group in it and on the
//! compactor, which only the loop sees.
//!
//! **Nothing here holds a `RefCell` borrow across an `.await`.** The writer task, openraft's
//! core, its state machine worker and the shard loop all share one executor and this one cell.

pub mod frame;
pub mod memory;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, Bound, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures_channel::oneshot;
use glommio::io::{BufferedFile, Directory, OpenOptions};
use openraft::entry::RaftEntry as _;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::type_config::alias::StoredMembershipOf;
use openraft::{EntryPayload, Membership, OptionalSend, StoredMembership};
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

use crate::server::replication::{DataConfig, Remembered};
use crate::shared::identity::{GroupId, ShardAddr};
use crate::shared::protocol::peer::RequestId;
pub use frame::{Entry, LeaderId, Vote, WalLogId};
pub use memory::MemoryWal;

/// The directory under the latency sensitive path the WAL lives in
pub const WAL_DIR: &str = "wal";

/// The checkpoint file's name
pub const CHECKPOINT_FILE: &str = "checkpoint.json";

/// The retry sidecar's name, beside the checkpoint file
///
/// Every persistent group's remembered requests as of the checkpoint, written before the
/// checkpoint that names them ([F42](../../../../docs/src/features/primary-failover.md)).
pub const RETRIES_FILE: &str = "retries.bin";

/// The magic a checksummed retry sidecar begins with
///
/// A sidecar from before [F44](../../../../docs/src/features/repair.md) begins with postcard's
/// count of its groups, which is never these eight bytes, so the first bytes of the file say
/// whether a checksum follows them.
pub const RETRIES_MAGIC: &[u8; 8] = b"SHOALRTY";

/// Hash bytes the way every checksummed file on this node does
///
/// # Arguments
///
/// * `bytes` - The bytes to hash
#[must_use]
pub fn checksum_of(bytes: &[u8]) -> u64 {
    // one hasher describes every checksum on disk
    let mut hasher = gxhash::GxHasher::default();
    std::hash::Hasher::write(&mut hasher, bytes);
    std::hash::Hasher::finish(&hasher)
}

/// Turn a glommio error into the io error openraft wants
///
/// # Arguments
///
/// * `error` - The glommio error
fn io<T>(error: glommio::GlommioError<T>) -> io::Error {
    match error {
        glommio::GlommioError::IoError(error) => error,
        glommio::GlommioError::EnhancedIoError {
            source, op, path, ..
        } => io::Error::new(
            source.kind(),
            format!(
                "{op} {}: {source}",
                path.map(|p| p.display().to_string()).unwrap_or_default()
            ),
        ),
        other => io::Error::other(other.to_string()),
    }
}

/// Where a frame lies: which segment, at what offset, and how long
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Loc {
    /// The segment's generation
    pub generation: u64,
    /// The frame's offset in it
    pub offset: u64,
    /// How many bytes the frame takes, prefix included
    pub len: u32,
}

/// One frame the compactor is to read: which entry it is and where it lies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameRef {
    /// The group the entry belongs to
    pub group: GroupId,
    /// The entry's index
    pub index: u64,
    /// The frame's offset in the segment
    pub offset: u64,
    /// How many bytes the frame takes
    pub len: u32,
}

/// One index entry of a group's log: where the frame is and who led when it was written
#[derive(Debug, Clone)]
struct Slot {
    /// Where the frame lies
    loc: Loc,
    /// The leader id of the entry's log id, so the log id can be rebuilt without the frame
    leader: LeaderId,
    /// Whether the entry carries a command, which is what a compactor is handed
    ///
    /// A blank or a membership entry is log alone: it moves a checkpoint and reaches no archive.
    command: bool,
}

/// One group's logical log within the shared file
#[derive(Debug, Default)]
struct GroupLog {
    /// Every entry, by index, with where its frame lies
    index: BTreeMap<u64, Slot>,
    /// The last vote granted
    vote: Option<Vote>,
    /// The last committed log id recorded
    committed: Option<WalLogId>,
    /// The last purged log id recorded
    purged: Option<WalLogId>,
    /// The entries held in memory, by index
    cache: BTreeMap<u64, Entry>,
}

impl GroupLog {
    /// The log id of an index, from the index's slot
    ///
    /// # Arguments
    ///
    /// * `index` - The index
    fn log_id_at(&self, index: u64) -> Option<WalLogId> {
        self.index
            .get(&index)
            .map(|slot| openraft::LogId::new(slot.leader.clone(), index))
    }

    /// The last log id, from the index or from what was purged
    fn last_log_id(&self) -> Option<WalLogId> {
        self.index
            .keys()
            .next_back()
            .and_then(|index| self.log_id_at(*index))
            .or_else(|| self.purged.clone())
    }
}

/// What one sealed or active segment holds, as far as the shard loop needs to know
#[derive(Debug, Clone, Default)]
pub struct SegmentView {
    /// The segment's generation
    pub generation: u64,
    /// Whether it is sealed: the writer moved on, and nothing more lands in it
    pub sealed: bool,
    /// Whether the loop has handed it to the compactors
    pub handed: bool,
    /// The last entry of each group with frames in it, as the index stands now
    pub last: HashMap<GroupId, WalLogId>,
    /// How many bytes the file holds, as of its seal or its open
    ///
    /// What the retention budget is judged against ([F43](../../../../docs/src/features/node-recovery.md)):
    /// a sealed segment's size is fixed, and the active one's is not counted.
    pub bytes: u64,
}

/// A batch of frames waiting to be written, or being written
struct Batch {
    /// The segment the batch goes into
    generation: u64,
    /// Where in that segment it begins
    base: u64,
    /// The frames, back to back
    bytes: Vec<u8>,
    /// The flush callbacks to complete once the batch is durable, each with its group
    callbacks: Vec<(GroupId, IOFlushed<DataConfig>)>,
    /// Whoever is waiting for this batch to be durable
    waiters: Vec<oneshot::Sender<io::Result<()>>>,
}

impl Batch {
    /// A fresh batch at the tail of a segment
    ///
    /// # Arguments
    ///
    /// * `generation` - The segment
    /// * `base` - Where in it the batch begins
    fn new(generation: u64, base: u64) -> Self {
        Batch {
            generation,
            base,
            bytes: Vec::with_capacity(64 * 1024),
            callbacks: Vec::new(),
            waiters: Vec::new(),
        }
    }

    /// Where the batch ends
    fn end(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

/// The store's state, shared by every handle on this shard
struct WalInner {
    /// The directory the segments are in
    dir: PathBuf,
    /// Every group's logical log
    groups: HashMap<GroupId, GroupLog>,
    /// The generation appends go to
    generation: u64,
    /// The offset the next frame is assigned in it
    next_offset: u64,
    /// The batch accepting appends, if one is open
    open: Option<Batch>,
    /// Closed batches waiting for the writer, in order
    queued: VecDeque<Batch>,
    /// Whether the writer has a batch in flight
    writing: bool,
    /// Everything below this is durable: the generation and the offset within it
    durable: (u64, u64),
    /// Every segment, by generation
    segments: BTreeMap<u64, SegmentView>,
    /// How many bytes the cache may hold before durable entries are evicted
    cache_bound: usize,
    /// How many bytes the cache holds
    cache_bytes: usize,
    /// The cached entries in insertion order, for eviction
    cache_order: VecDeque<(GroupId, u64, usize)>,
    /// How many bytes a segment grows to before the next append opens a new one
    segment_bytes: u64,
    /// The groups whose flush completions are held back, for a test
    stalled: HashSet<GroupId>,
    /// The completions held back, with their groups
    held: Vec<(GroupId, IOFlushed<DataConfig>)>,
    /// Who to wake when a batch is ready for the writer
    waker: Option<Waker>,
    /// Whoever is waiting for the queue to drain
    idle_waiters: Vec<oneshot::Sender<()>>,
    /// Whether the store is closing
    closed: bool,
    /// The first write error, which fails every append after it
    error: Option<String>,
    /// Who to tell when a segment is sealed
    on_sealed: Option<Rc<dyn Fn(u64)>>,
    /// Open read handles on segments, by generation
    readers: HashMap<u64, BufferedFile>,
}

impl WalInner {
    /// The group's log, creating an empty one on first sight
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    fn group(&mut self, group: GroupId) -> &mut GroupLog {
        self.groups.entry(group).or_default()
    }

    /// Whether a location is durable
    ///
    /// # Arguments
    ///
    /// * `loc` - The location
    fn is_durable(&self, loc: &Loc) -> bool {
        loc.generation < self.durable.0
            || (loc.generation == self.durable.0
                && loc.offset + u64::from(loc.len) <= self.durable.1)
    }

    /// The batch to append into, opening one at the tail if none is open
    ///
    /// Rotation happens here: once the active segment has grown past its bound the next append
    /// opens a batch in the next generation, and the writer seals the old one when it gets there.
    fn open_batch(&mut self) -> &mut Batch {
        if self.next_offset >= self.segment_bytes {
            self.rotate();
        }
        if self.open.is_none() {
            self.open = Some(Batch::new(self.generation, self.next_offset));
        }
        self.open.as_mut().expect("a batch was just opened")
    }

    /// Move appends to the next generation
    ///
    /// The open batch, if any, is closed so nothing more lands in the old segment; the writer
    /// seals the file when it reaches the first batch of the new generation. Every group's
    /// vote, committed and purged markers are written again at the head of the new segment,
    /// so a sealed segment is never the only place a marker lives: a segment that held only
    /// a group's newest purge marker would otherwise be deleted as empty, and the group would
    /// open with its purge point forgotten and its log starting at an index it no longer has
    /// ([F43](../../../../docs/src/features/node-recovery.md)).
    fn rotate(&mut self) {
        self.close_open();
        // the segment being left is as large as it will ever be
        let size = self.next_offset;
        if let Some(segment) = self.segments.get_mut(&self.generation) {
            segment.bytes = size;
        }
        self.generation += 1;
        self.next_offset = 0;
        self.segments.insert(
            self.generation,
            SegmentView {
                generation: self.generation,
                ..SegmentView::default()
            },
        );
        self.carry_markers();
    }

    /// Write every group's current markers into the active segment
    ///
    /// Called right after a rotation, so the new segment carries the state the old one held
    /// whatever happens to the old one afterwards.
    fn carry_markers(&mut self) {
        let mut frames = Vec::new();
        for (group, log) in &self.groups {
            if let Some(vote) = &log.vote {
                if let Ok(frame) = frame::encode_vote(*group, vote) {
                    frames.push(frame);
                }
            }
            if log.committed.is_some() {
                if let Ok(frame) = frame::encode_marker(
                    frame::FrameKind::Committed,
                    *group,
                    log.committed.as_ref(),
                ) {
                    frames.push(frame);
                }
            }
            if log.purged.is_some() {
                if let Ok(frame) =
                    frame::encode_marker(frame::FrameKind::Purged, *group, log.purged.as_ref())
                {
                    frames.push(frame);
                }
            }
        }
        // straight into the open batch of the new generation, ahead of every append
        for frame in frames {
            self.put(&frame);
        }
    }

    /// Close the open batch, handing it to the writer
    fn close_open(&mut self) {
        if let Some(batch) = self.open.take() {
            if batch.bytes.is_empty() && batch.waiters.is_empty() {
                return;
            }
            self.queued.push_back(batch);
        }
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Put a frame into the open batch, assigning it a location
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame's bytes
    fn put(&mut self, frame: &[u8]) -> Loc {
        // truncation cannot happen: a frame's length is a u32 by construction
        #[allow(clippy::cast_possible_truncation)]
        let len = frame.len() as u32;
        let batch = self.open_batch();
        let loc = Loc {
            generation: batch.generation,
            offset: batch.end(),
            len,
        };
        batch.bytes.extend_from_slice(frame);
        self.next_offset = loc.offset + u64::from(len);
        loc
    }

    /// Record an entry's slot in its group's index and the segment's last log id
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `log_id` - The entry's log id
    /// * `command` - Whether the entry carries a command
    /// * `loc` - Where its frame lies
    fn index_entry(&mut self, group: GroupId, log_id: &WalLogId, command: bool, loc: Loc) {
        self.group(group).index.insert(
            log_id.index,
            Slot {
                loc,
                leader: log_id.leader_id.clone(),
                command,
            },
        );
        let segment = self
            .segments
            .entry(loc.generation)
            .or_insert_with(|| SegmentView {
                generation: loc.generation,
                ..SegmentView::default()
            });
        let last = segment.last.entry(group).or_insert_with(|| log_id.clone());
        if log_id.index >= last.index {
            *last = log_id.clone();
        }
    }

    /// Put an entry in the cache, evicting durable entries past the bound
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `entry` - The entry
    /// * `len` - How many bytes its frame takes
    fn cache_entry(&mut self, group: GroupId, entry: Entry, len: usize) {
        let index = entry.index();
        if self.group(group).cache.insert(index, entry).is_none() {
            self.cache_bytes += len;
            self.cache_order.push_back((group, index, len));
        }
        self.evict();
    }

    /// Evict the oldest durable entries until the cache is under its bound
    fn evict(&mut self) {
        while self.cache_bytes > self.cache_bound {
            let Some((group, index, len)) = self.cache_order.front().copied() else {
                break;
            };
            let Some(log) = self.groups.get(&group) else {
                self.cache_order.pop_front();
                continue;
            };
            // an entry still in the cache has to be durable to go; one already dropped by a
            // truncate or a purge only has its order record left to drop
            match log.index.get(&index) {
                Some(slot) if log.cache.contains_key(&index) => {
                    if !self.is_durable(&slot.loc) {
                        break;
                    }
                }
                _ => {}
            }
            self.cache_order.pop_front();
            if self
                .groups
                .get_mut(&group)
                .and_then(|log| log.cache.remove(&index))
                .is_some()
            {
                self.cache_bytes = self.cache_bytes.saturating_sub(len);
            }
        }
    }

    /// Drop a group's index and cache entries above a log id, and fix the segments' lasts
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `keep_after` - The last index kept, or none for nothing kept
    fn truncate_index(&mut self, group: GroupId, keep_after: Option<u64>) {
        let log = self.group(group);
        let dropped: Vec<u64> = log
            .index
            .keys()
            .filter(|index| keep_after.is_none_or(|keep| **index > keep))
            .copied()
            .collect();
        let mut touched = HashSet::new();
        for index in &dropped {
            if let Some(slot) = log.index.remove(index) {
                touched.insert(slot.loc.generation);
            }
            log.cache.remove(index);
        }
        // the segments those frames were in have to say what the group's last entry is now
        for generation in touched {
            let last = self.groups.get(&group).and_then(|log| {
                log.index
                    .iter()
                    .filter(|(_, slot)| slot.loc.generation == generation)
                    .map(|(index, _)| *index)
                    .max()
                    .and_then(|index| log.log_id_at(index))
            });
            if let Some(segment) = self.segments.get_mut(&generation) {
                match last {
                    Some(last) => {
                        segment.last.insert(group, last);
                    }
                    None => {
                        segment.last.remove(&group);
                    }
                }
            }
        }
    }

    /// Drop a group's index and cache entries up to a log id
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `upto` - The last index dropped
    fn purge_index(&mut self, group: GroupId, upto: u64) {
        let log = self.group(group);
        let dropped: Vec<u64> = log.index.range(..=upto).map(|(index, _)| *index).collect();
        for index in dropped {
            log.index.remove(&index);
            log.cache.remove(&index);
        }
    }

    /// Apply one decoded frame to the state, as a replay at open does
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame
    /// * `loc` - Where it lies
    fn replay(&mut self, frame: frame::Frame, loc: Loc) {
        match frame {
            frame::Frame::Entry { group, entry } => {
                // a replayed entry supersedes whatever the index held at that position
                let log_id = entry.log_id();
                // a scrub is log alone, like a blank: it reaches no archive
                // ([F44](../../../../docs/src/features/repair.md))
                let command = matches!(&entry.payload, EntryPayload::Normal(command) if command.scrub_op().is_none());
                self.index_entry(group, &log_id, command, loc);
            }
            frame::Frame::Vote { group, vote } => self.group(group).vote = Some(vote),
            frame::Frame::Committed { group, log_id } => self.group(group).committed = log_id,
            frame::Frame::Purged { group, log_id } => {
                let index = log_id.index;
                self.group(group).purged = Some(log_id);
                self.purge_index(group, index);
            }
            frame::Frame::Truncate { group, keep_after } => {
                self.truncate_index(group, keep_after.map(|log_id| log_id.index));
            }
            // a forgotten group: its state and every segment's memory of it are gone, and
            // the frames before this one are dead ([F45](../../../../docs/src/features/replica-migration.md))
            frame::Frame::Forget { group } => self.forget_group(group),
        }
    }

    /// Drop a group's state whole, and every segment's memory of its frames
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    fn forget_group(&mut self, group: GroupId) {
        self.groups.remove(&group);
        for segment in self.segments.values_mut() {
            segment.last.remove(&group);
        }
    }
}

/// The name of a segment file
///
/// # Arguments
///
/// * `generation` - The segment's generation
fn segment_name(generation: u64) -> String {
    format!("{generation:018}.wal")
}

/// Open a segment for reading and appending, creating it if it is not there
///
/// # Arguments
///
/// * `path` - The segment
async fn open_segment(path: &Path) -> io::Result<BufferedFile> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .buffered_open(path)
        .await
        .map_err(io)
}

/// Read a whole file
///
/// # Arguments
///
/// * `path` - The file
async fn read_whole(path: &Path) -> io::Result<Vec<u8>> {
    let file = BufferedFile::open(path).await.map_err(io)?;
    let size = file.file_size().await.map_err(io)?;
    let bytes = if size == 0 {
        Vec::new()
    } else {
        // `usize` from `u64` is a lossless conversion on every target this runs on
        file.read_at(0, size as usize).await.map_err(io)?.to_vec()
    };
    file.close().await.map_err(io)?;
    Ok(bytes)
}

/// Replace a file so that a crash leaves the old one or the new one
///
/// # Arguments
///
/// * `dir` - The directory the file is in
/// * `name` - The file's name
/// * `bytes` - What it should hold
pub async fn write_atomic(dir: &Path, name: &str, bytes: Vec<u8>) -> io::Result<()> {
    let staged = dir.join(format!("{name}.tmp"));
    let mut file = BufferedFile::create(&staged).await.map_err(io)?;
    if !bytes.is_empty() {
        file.write_at(bytes, 0).await.map_err(io)?;
    }
    file.fdatasync().await.map_err(io)?;
    file.rename(dir.join(name)).await.map_err(io)?;
    file.close().await.map_err(io)?;
    let directory = Directory::open(dir).await.map_err(io)?;
    directory.sync().await.map_err(io)?;
    directory.close().await.map_err(io)?;
    Ok(())
}

/// What the checkpoint file records for one group
///
/// The membership is spelled out as lists rather than as openraft's type, whose node map is
/// keyed by a shard address and so has no JSON form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupCheckpoint {
    /// The last log id whose effect the table's archives hold
    pub applied: Option<WalLogId>,
    /// The log id of the membership as of that point, if one was ever committed
    pub membership_at: Option<WalLogId>,
    /// The joint configuration then: the voters of each config
    pub configs: Vec<Vec<ShardAddr>>,
    /// Every member then, learners included
    pub members: Vec<ShardAddr>,
    /// The log index the retry sidecar's entries for this group are complete to
    ///
    /// The checkpoint's own index once a sidecar was written for it; zero from a file written
    /// before there was one, which seeds nothing.
    #[serde(default)]
    pub retries_at: u64,
    /// The lowest applied index the retry table still remembered, or zero
    ///
    /// The low-water mark: a retry of an identity applied below it is applied as new
    /// ([F45](../../../../docs/src/features/replica-migration.md) refuses by the identity's
    /// own time instead, since an index says nothing a client can compare its retry to).
    #[serde(default)]
    pub retry_floor: u64,
    /// The newest time-ordered identity the retry table had forgotten, in milliseconds since
    /// the epoch; zero for none, and from a file written before there was one
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub expired_before: u64,
}

impl GroupCheckpoint {
    /// Record a checkpoint and the membership as of it
    ///
    /// # Arguments
    ///
    /// * `applied` - The checkpoint
    /// * `membership` - The membership as of it
    #[must_use]
    pub fn new(applied: Option<WalLogId>, membership: &StoredMembershipOf<DataConfig>) -> Self {
        GroupCheckpoint {
            applied,
            membership_at: membership.log_id().clone(),
            configs: membership
                .membership()
                .get_joint_config()
                .iter()
                .map(|config| config.iter().copied().collect())
                .collect(),
            members: membership
                .membership()
                .nodes()
                .map(|(addr, _)| *addr)
                .collect(),
            retries_at: 0,
            retry_floor: 0,
            expired_before: 0,
        }
    }

    /// Record the newest time-ordered identity the retry table had forgotten
    ///
    /// # Arguments
    ///
    /// * `expired_before` - Its timestamp, in milliseconds since the epoch
    #[must_use]
    pub fn expired_before(mut self, expired_before: u64) -> Self {
        self.expired_before = expired_before;
        self
    }

    /// Record which retry sidecar goes with this checkpoint, and the table's low-water mark
    ///
    /// # Arguments
    ///
    /// * `retries_at` - The index the sidecar's entries are complete to
    /// * `retry_floor` - The lowest applied index still remembered
    #[must_use]
    pub fn retries(mut self, retries_at: u64, retry_floor: u64) -> Self {
        self.retries_at = retries_at;
        self.retry_floor = retry_floor;
        self
    }

    /// The membership as openraft holds it
    #[must_use]
    pub fn membership(&self) -> StoredMembershipOf<DataConfig> {
        let configs: Vec<BTreeSet<ShardAddr>> = self
            .configs
            .iter()
            .map(|config| config.iter().copied().collect())
            .collect();
        let nodes: BTreeMap<ShardAddr, ShardAddr> =
            self.members.iter().map(|addr| (*addr, *addr)).collect();
        // a configuration read back from a file this node wrote is one openraft accepted
        let membership = Membership::new(configs, nodes).unwrap_or_default();
        StoredMembership::new(self.membership_at.clone(), membership)
    }
}

/// The checkpoint file: every group's boundary
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Per group, by its identity rendered in hex
    pub groups: BTreeMap<String, GroupCheckpoint>,
}

/// The checkpoint file as it lies on disk: the groups and a checksum over them
///
/// The checksum is gxhash64 over the compact JSON of `groups`, which is canonical because the
/// map is ordered and nothing in it is skipped. Zero is a file from before
/// [F44](../../../../docs/src/features/repair.md), which is read unchecked; any other value has
/// to match, since a checkpoint that cannot be trusted is not a state to start a group from.
#[derive(Debug, Serialize, Deserialize)]
struct CheckpointFile {
    /// The checkpoint itself
    #[serde(flatten)]
    checkpoint: Checkpoint,
    /// The checksum over the groups, or zero
    #[serde(default)]
    checksum: u64,
}

impl Checkpoint {
    /// The checksum a checkpoint file carries for these groups
    fn checksum(&self) -> io::Result<u64> {
        // the compact JSON of the ordered map is the canonical form
        Ok(checksum_of(&serde_json::to_vec(&self.groups)?))
    }

    /// Read the checkpoint file, or an empty one if there is none
    ///
    /// # Arguments
    ///
    /// * `dir` - The WAL directory
    pub async fn read(dir: &Path) -> io::Result<Self> {
        let path = dir.join(CHECKPOINT_FILE);
        if !path.exists() {
            return Ok(Checkpoint::default());
        }
        let bytes = read_whole(&path).await?;
        let file: CheckpointFile = serde_json::from_slice(&bytes)?;
        // a file with a checksum has to hash to it; one without is from before there was one
        if file.checksum != 0 {
            let found = file.checkpoint.checksum()?;
            if found != file.checksum {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} does not hash to its checksum: expected {:016x}, found {found:016x}",
                        path.display(),
                        file.checksum
                    ),
                ));
            }
        } else {
            event!(Level::WARN, msg = "read a checkpoint file with no checksum", path = %path.display());
        }
        Ok(file.checkpoint)
    }

    /// Write the checkpoint file atomically, with a checksum over its groups
    ///
    /// # Arguments
    ///
    /// * `dir` - The WAL directory
    pub async fn write(&self, dir: &Path) -> io::Result<()> {
        let file = CheckpointFile {
            checksum: self.checksum()?,
            checkpoint: self.clone(),
        };
        write_atomic(dir, CHECKPOINT_FILE, serde_json::to_vec_pretty(&file)?).await
    }

    /// One group's boundary, if it has one
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    #[must_use]
    pub fn get(&self, group: GroupId) -> Option<&GroupCheckpoint> {
        self.groups.get(&group.to_string())
    }
}

/// One group's remembered requests as of a checkpoint
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GroupRetries {
    /// The log index the entries are complete to: the checkpoint they were written for
    pub retries_at: u64,
    /// The entries, oldest first
    pub entries: Vec<(RequestId, Remembered)>,
}

/// The retry sidecar: every persistent group's remembered requests as of its checkpoint
///
/// Written before the checkpoint file on the same trigger, so a checkpoint whose `retries_at`
/// names an index always has a sidecar complete to it; a crash between the two leaves a sidecar
/// ahead of its checkpoint, which the seed rule ignores. Postcard rather than JSON: a request
/// identity is sixteen bytes and an index, and there are up to four thousand a group.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Retries {
    /// Per group, by its identity rendered in hex
    pub groups: BTreeMap<String, GroupRetries>,
}

impl Retries {
    /// Read the retry sidecar, or an empty one if there is none
    ///
    /// # Arguments
    ///
    /// * `dir` - The WAL directory
    pub async fn read(dir: &Path) -> io::Result<Self> {
        let path = dir.join(RETRIES_FILE);
        if !path.exists() {
            return Ok(Retries::default());
        }
        let bytes = read_whole(&path).await?;
        // a sidecar from before F44 has no magic and no checksum, and is read as it was
        let Some(rest) = bytes.strip_prefix(RETRIES_MAGIC) else {
            event!(Level::WARN, msg = "read a retry sidecar with no checksum", path = %path.display());
            return postcard::from_bytes(&bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
        };
        // the checksum follows the magic, and the payload has to hash to it
        if rest.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is too short to hold its checksum", path.display()),
            ));
        }
        let expected = u64::from_le_bytes(rest[..8].try_into().expect("eight bytes"));
        let found = checksum_of(&rest[8..]);
        if expected != found {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} does not hash to its checksum: expected {expected:016x}, found {found:016x}", path.display()),
            ));
        }
        postcard::from_bytes(&rest[8..])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Write the retry sidecar atomically: the magic, a checksum, then the entries
    ///
    /// # Arguments
    ///
    /// * `dir` - The WAL directory
    pub async fn write(&self, dir: &Path) -> io::Result<()> {
        let payload = postcard::to_allocvec(self)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        // the magic says a checksum follows, and the checksum says what the payload has to be
        let mut bytes = Vec::with_capacity(16 + payload.len());
        bytes.extend_from_slice(RETRIES_MAGIC);
        bytes.extend_from_slice(&checksum_of(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        write_atomic(dir, RETRIES_FILE, bytes).await
    }

    /// The entries to seed a group's retry table with at open
    ///
    /// Only when the sidecar was written for exactly the checkpoint the group starts from, and
    /// only the entries applied at or below it: an entry above the checkpoint is re-derived by
    /// the replay of the log, and seeding it would make the replay answer `Duplicate` and skip
    /// the apply the table needs.
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `point` - The checkpoint it starts from
    #[must_use]
    pub fn seed_for(
        &self,
        group: GroupId,
        point: &GroupCheckpoint,
    ) -> Vec<(RequestId, Remembered)> {
        // a checkpoint from before the sidecar existed names no sidecar
        if point.retries_at == 0 {
            return Vec::new();
        }
        let Some(retries) = self.groups.get(&group.to_string()) else {
            return Vec::new();
        };
        // a sidecar written for another checkpoint is not this one's
        if retries.retries_at != point.retries_at {
            return Vec::new();
        }
        retries
            .entries
            .iter()
            .filter(|(_, remembered)| remembered.applied <= point.retries_at)
            .copied()
            .collect()
    }
}

/// A future that resolves with the next batch for the writer, or none once the store closed
struct NextBatch {
    /// The store
    inner: Rc<RefCell<WalInner>>,
}

impl Future for NextBatch {
    type Output = Option<Batch>;

    /// Take a queued batch, else the open one, else park until something is queued
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();
        if let Some(batch) = inner.queued.pop_front() {
            inner.writing = true;
            return Poll::Ready(Some(batch));
        }
        if inner
            .open
            .as_ref()
            .is_some_and(|batch| !batch.bytes.is_empty() || !batch.waiters.is_empty())
        {
            let batch = inner.open.take().expect("checked above");
            inner.writing = true;
            return Poll::Ready(Some(batch));
        }
        // nothing to write: whoever waits for idleness can go, and a close ends the task
        for waiter in inner.idle_waiters.drain(..) {
            let _ = waiter.send(());
        }
        if inner.closed {
            return Poll::Ready(None);
        }
        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// The writer task: write and sync one batch at a time, sealing segments as it crosses them
///
/// # Arguments
///
/// * `inner` - The store
async fn writer(inner: Rc<RefCell<WalInner>>) {
    // the file the writer holds, and which generation it is
    let mut file: Option<(u64, BufferedFile)> = None;
    while let Some(batch) = (NextBatch {
        inner: inner.clone(),
    })
    .await
    {
        let dir = inner.borrow().dir.clone();
        // a batch in a new generation seals the file before it
        if file
            .as_ref()
            .is_some_and(|(generation, _)| *generation != batch.generation)
        {
            let (sealed, old) = file.take().expect("checked above");
            let synced = old.fdatasync().await.map_err(io);
            let _ = old.close().await;
            if let Err(error) = synced {
                fail_batch(&inner, batch, error);
                continue;
            }
            let hook = {
                let mut guard = inner.borrow_mut();
                if let Some(segment) = guard.segments.get_mut(&sealed) {
                    segment.sealed = true;
                }
                guard.on_sealed.clone()
            };
            if let Some(hook) = hook {
                hook(sealed);
            }
        }
        // open the file for this generation if the writer does not hold it
        if file.is_none() {
            match open_segment(&dir.join(segment_name(batch.generation))).await {
                Ok(opened) => file = Some((batch.generation, opened)),
                Err(error) => {
                    fail_batch(&inner, batch, error);
                    continue;
                }
            }
        }
        let (_, handle) = file.as_ref().expect("opened above");
        // the write, at the batch's base, and the sync that makes it durable
        let written = if batch.bytes.is_empty() {
            Ok(())
        } else {
            handle
                .write_at(batch.bytes.clone(), batch.base)
                .await
                .map(|_| ())
                .map_err(io)
        };
        let synced = match written {
            Ok(()) => handle.fdatasync().await.map_err(io),
            Err(error) => Err(error),
        };
        match synced {
            Ok(()) => complete_batch(&inner, batch),
            Err(error) => fail_batch(&inner, batch, error),
        }
    }
    // closing: sync whatever file is open and let it go
    if let Some((_, handle)) = file.take() {
        let _ = handle.fdatasync().await;
        let _ = handle.close().await;
    }
}

/// Complete a batch that is durable: move the watermark, fire the callbacks, evict
///
/// # Arguments
///
/// * `inner` - The store
/// * `batch` - The batch
fn complete_batch(inner: &Rc<RefCell<WalInner>>, batch: Batch) {
    let mut fire = Vec::new();
    {
        let mut guard = inner.borrow_mut();
        guard.durable = (batch.generation, batch.end());
        guard.writing = false;
        // a stalled group's completions are held; everybody else's fire now
        for (group, callback) in batch.callbacks {
            if guard.stalled.contains(&group) {
                guard.held.push((group, callback));
            } else {
                fire.push(callback);
            }
        }
        guard.evict();
    }
    for callback in fire {
        callback.io_completed(Ok(()));
    }
    for waiter in batch.waiters {
        let _ = waiter.send(Ok(()));
    }
}

/// Fail a batch: record the error, fail the callbacks and the waiters
///
/// # Arguments
///
/// * `inner` - The store
/// * `batch` - The batch
/// * `error` - What went wrong
fn fail_batch(inner: &Rc<RefCell<WalInner>>, batch: Batch, error: io::Error) {
    event!(Level::ERROR, msg = "a wal batch failed", ?error);
    {
        let mut guard = inner.borrow_mut();
        guard.writing = false;
        guard.error.get_or_insert_with(|| error.to_string());
    }
    for (_, callback) in batch.callbacks {
        callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
    }
    for waiter in batch.waiters {
        let _ = waiter.send(Err(io::Error::new(error.kind(), error.to_string())));
    }
}

/// The shard's WAL, as one handle every group store on the shard clones
#[derive(Clone)]
pub struct ShardWal {
    /// The shared state
    inner: Rc<RefCell<WalInner>>,
}

impl ShardWal {
    /// Open the WAL under a shard's directory, recovering every segment in it
    ///
    /// # Arguments
    ///
    /// * `dir` - The shard's WAL directory
    /// * `segment_bytes` - How large a segment grows before the next append rotates
    /// * `cache_bound` - How many bytes of entries to hold in memory past the durable tail
    ///
    /// # Errors
    ///
    /// Fails if the directory cannot be created or a segment cannot be read.
    pub async fn open(dir: &Path, segment_bytes: u64, cache_bound: usize) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        // every segment, in generation order
        let mut generations = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".wal") {
                if let Ok(generation) = stem.parse::<u64>() {
                    generations.push(generation);
                }
            }
        }
        generations.sort_unstable();
        let mut inner = WalInner {
            dir: dir.to_path_buf(),
            groups: HashMap::new(),
            generation: 1,
            next_offset: 0,
            open: None,
            queued: VecDeque::new(),
            writing: false,
            durable: (1, 0),
            segments: BTreeMap::new(),
            cache_bound,
            cache_bytes: 0,
            cache_order: VecDeque::new(),
            segment_bytes,
            stalled: HashSet::new(),
            held: Vec::new(),
            waker: None,
            idle_waiters: Vec::new(),
            closed: false,
            error: None,
            on_sealed: None,
            readers: HashMap::new(),
        };
        // replay every frame of every segment, in file order, cutting a torn tail
        let last = generations.last().copied();
        for generation in &generations {
            let path = dir.join(segment_name(*generation));
            let bytes = read_whole(&path).await?;
            let (frames, whole) = frame::decode_all(&bytes);
            inner.segments.insert(
                *generation,
                SegmentView {
                    generation: *generation,
                    sealed: Some(*generation) != last,
                    bytes: whole,
                    ..SegmentView::default()
                },
            );
            for (offset, len, decoded) in frames {
                inner.replay(
                    decoded,
                    Loc {
                        generation: *generation,
                        offset,
                        len,
                    },
                );
            }
            if whole < bytes.len() as u64 {
                event!(
                    Level::WARN,
                    msg = "truncating a torn tail off a wal segment",
                    generation,
                    torn_from = whole,
                    length = bytes.len()
                );
                let file = open_segment(&path).await?;
                file.truncate(whole).await.map_err(io)?;
                file.fdatasync().await.map_err(io)?;
                file.close().await.map_err(io)?;
            }
            if Some(*generation) == last {
                inner.generation = *generation;
                inner.next_offset = whole;
                inner.durable = (*generation, whole);
            }
        }
        if generations.is_empty() {
            inner.segments.insert(
                1,
                SegmentView {
                    generation: 1,
                    ..SegmentView::default()
                },
            );
        }
        let inner = Rc::new(RefCell::new(inner));
        // the writer, which lives until the store closes
        glommio::spawn_local(writer(inner.clone())).detach();
        Ok(ShardWal { inner })
    }

    /// The directory the segments are in
    #[must_use]
    pub fn dir(&self) -> PathBuf {
        self.inner.borrow().dir.clone()
    }

    /// The generation appends currently go to
    #[must_use]
    pub fn active_generation(&self) -> u64 {
        self.inner.borrow().generation
    }

    /// Hear about every sealed segment, by generation
    ///
    /// # Arguments
    ///
    /// * `hook` - Called on the writer's task once the old file is synced and closed
    pub fn on_sealed(&self, hook: Rc<dyn Fn(u64)>) {
        self.inner.borrow_mut().on_sealed = Some(hook);
    }

    /// The segment an entry's frame lies in, if the index knows the entry
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `index` - The entry's index
    #[must_use]
    pub fn generation_of(&self, group: GroupId, index: u64) -> Option<u64> {
        self.inner
            .borrow()
            .groups
            .get(&group)
            .and_then(|log| log.index.get(&index))
            .map(|slot| slot.loc.generation)
    }

    /// Every segment, in generation order
    #[must_use]
    pub fn segments(&self) -> Vec<SegmentView> {
        self.inner.borrow().segments.values().cloned().collect()
    }

    /// Forget a group's log whole: a marker frame, then its state and every segment's memory of it
    ///
    /// A retired copy's log is dead history: its frames stay in the sealed segments until they
    /// are reclaimed with everything else in them, are never handed to a compactor again, and
    /// a replay past the marker rebuilds nothing from them, so a copy of the same group added
    /// to this shard later starts with no log, no vote and no committed position of its own
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    ///
    /// # Errors
    ///
    /// Fails if the marker could not be staged.
    pub fn forget(&self, group: GroupId) -> io::Result<()> {
        let frame = frame::encode_marker(frame::FrameKind::Forget, group, None)?;
        self.stage(&frame, group, None)?;
        self.inner.borrow_mut().forget_group(group);
        Ok(())
    }

    /// Note that the loop handed a segment to the compactors
    ///
    /// # Arguments
    ///
    /// * `generation` - The segment
    pub fn mark_handed(&self, generation: u64) {
        if let Some(segment) = self.inner.borrow_mut().segments.get_mut(&generation) {
            segment.handed = true;
        }
    }

    /// The command frames of some groups that lie in a segment, in index order per group
    ///
    /// Only the frames above each group's checkpoint: a frame at or below it is one the
    /// archives already hold the effect of, and merging it again would put an older write over
    /// a newer one once the partition is read back from disk
    /// ([Resolved #104](../../../../docs/src/appendix/resolved/segments-recompacted-after-restart.md)).
    ///
    /// # Arguments
    ///
    /// * `generation` - The segment
    /// * `groups` - The groups, each with the index its archives are complete to
    #[must_use]
    pub fn frames_in(&self, generation: u64, groups: &[(GroupId, u64)]) -> Vec<FrameRef> {
        let inner = self.inner.borrow();
        let mut frames = Vec::new();
        for (group, since) in groups {
            if let Some(log) = inner.groups.get(group) {
                for (index, slot) in log.index.range((Bound::Excluded(*since), Bound::Unbounded)) {
                    // a blank or a membership entry has nothing for an archive
                    if slot.command && slot.loc.generation == generation {
                        frames.push(FrameRef {
                            group: *group,
                            index: *index,
                            offset: slot.loc.offset,
                            len: slot.loc.len,
                        });
                    }
                }
            }
        }
        frames
    }

    /// The path of a segment
    ///
    /// # Arguments
    ///
    /// * `generation` - The segment
    #[must_use]
    pub fn segment_path(&self, generation: u64) -> PathBuf {
        self.inner.borrow().dir.join(segment_name(generation))
    }

    /// Delete a sealed segment, forgetting it
    ///
    /// # Arguments
    ///
    /// * `generation` - The segment
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be removed.
    pub async fn delete_segment(&self, generation: u64) -> io::Result<()> {
        let (path, reader) = {
            let mut inner = self.inner.borrow_mut();
            if inner
                .segments
                .get(&generation)
                .is_none_or(|segment| !segment.sealed)
            {
                return Ok(());
            }
            inner.segments.remove(&generation);
            (
                inner.dir.join(segment_name(generation)),
                inner.readers.remove(&generation),
            )
        };
        if let Some(reader) = reader {
            let _ = reader.close().await;
        }
        glommio::io::remove(&path).await.map_err(io)
    }

    /// Force the next append into a new segment, for a test
    pub fn rotate(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.rotate();
        // the markers carried into the new generation are a batch in it, which is what makes
        // the writer seal the old one; a store with no groups yet gets an empty batch for it
        if inner.open.is_none() {
            let generation = inner.generation;
            inner.queued.push_back(Batch::new(generation, 0));
        } else {
            inner.close_open();
        }
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }

    /// Hold back a group's flush completions, for a test of a slow follower
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub fn stall(&self, group: GroupId) {
        self.inner.borrow_mut().stalled.insert(group);
    }

    /// Release a group's held completions
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub fn release(&self, group: GroupId) {
        let held = {
            let mut inner = self.inner.borrow_mut();
            inner.stalled.remove(&group);
            let (release, keep): (Vec<_>, Vec<_>) =
                inner.held.drain(..).partition(|(held, _)| *held == group);
            inner.held = keep;
            release
        };
        for (_, callback) in held {
            callback.io_completed(Ok(()));
        }
    }

    /// How many completions are held back
    #[must_use]
    pub fn held(&self) -> usize {
        self.inner.borrow().held.len()
    }

    /// Wait until every queued batch is durable
    pub async fn flush(&self) -> io::Result<()> {
        let rx = {
            let mut inner = self.inner.borrow_mut();
            inner.close_open();
            if inner.queued.is_empty() && !inner.writing {
                return match &inner.error {
                    Some(error) => Err(io::Error::other(error.clone())),
                    None => Ok(()),
                };
            }
            let (tx, rx) = oneshot::channel();
            inner.idle_waiters.push(tx);
            rx
        };
        let _ = rx.await;
        match &self.inner.borrow().error {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }

    /// Close the store: flush what is queued and stop the writer
    pub async fn close(&self) -> io::Result<()> {
        let outcome = self.flush().await;
        {
            let mut inner = self.inner.borrow_mut();
            inner.closed = true;
            if let Some(waker) = inner.waker.take() {
                waker.wake();
            }
        }
        outcome
    }

    /// A store for one group over this WAL
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// The last log id this WAL holds for a group, from its index or from what was purged
    ///
    /// None is a group the WAL has no frame of at all - not an entry, not a purge, not a
    /// vote's log - which for a group whose checkpoint names an applied index is a log that
    /// was lost ([Resolved #99](../../../../docs/src/appendix/resolved/durable-log-reversion.md)).
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    #[must_use]
    pub fn last_log_id_of(&self, group: GroupId) -> Option<WalLogId> {
        self.inner
            .borrow()
            .groups
            .get(&group)
            .and_then(GroupLog::last_log_id)
    }

    #[must_use]
    pub fn store(&self, group: GroupId) -> GroupStore {
        GroupStore {
            backend: Backend::Shared(self.clone()),
            group,
        }
    }

    /// Every group this WAL holds a log, a vote or a marker for
    ///
    /// What a rehome moves out of a vanishing executor's WAL
    /// ([F47](../../../../docs/src/features/local-rehome.md)).
    #[must_use]
    pub fn groups(&self) -> Vec<GroupId> {
        let mut groups: Vec<GroupId> = self.inner.borrow().groups.keys().copied().collect();
        groups.sort_unstable();
        groups
    }

    /// The last vote a group granted, for a test
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    #[must_use]
    pub fn vote_of(&self, group: GroupId) -> Option<Vote> {
        self.inner
            .borrow()
            .groups
            .get(&group)
            .and_then(|log| log.vote.clone())
    }

    /// The indexes a group's log holds, for a test
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    #[must_use]
    pub fn indexes_of(&self, group: GroupId) -> Vec<u64> {
        self.inner
            .borrow()
            .groups
            .get(&group)
            .map(|log| log.index.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Put a frame into the open batch and wake the writer
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame
    /// * `callback` - A flush callback to complete once the frame is durable, if any
    fn stage(
        &self,
        frame: &[u8],
        group: GroupId,
        callback: Option<IOFlushed<DataConfig>>,
    ) -> io::Result<Loc> {
        let mut inner = self.inner.borrow_mut();
        if let Some(error) = &inner.error {
            return Err(io::Error::other(error.clone()));
        }
        if inner.closed {
            return Err(io::Error::other("the wal is closed"));
        }
        let loc = inner.put(frame);
        if let Some(callback) = callback {
            inner
                .open
                .as_mut()
                .expect("put opened a batch")
                .callbacks
                .push((group, callback));
        }
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Ok(loc)
    }

    /// Put a frame into the open batch and wait until it is durable
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame
    async fn stage_and_wait(&self, frame: &[u8], group: GroupId) -> io::Result<Loc> {
        let (tx, rx) = oneshot::channel();
        let loc = {
            let loc = self.stage(frame, group, None)?;
            let mut inner = self.inner.borrow_mut();
            inner
                .open
                .as_mut()
                .expect("stage opened a batch")
                .waiters
                .push(tx);
            inner.close_open();
            loc
        };
        match rx.await {
            Ok(outcome) => outcome.map(|()| loc),
            Err(_) => Err(io::Error::other("the wal writer went away")),
        }
    }

    /// Read a frame's entry from its segment
    ///
    /// # Arguments
    ///
    /// * `loc` - Where the frame lies
    async fn read_entry(&self, loc: Loc) -> io::Result<Entry> {
        // take a reader on the segment, or open one
        let taken = self.inner.borrow_mut().readers.remove(&loc.generation);
        let path = self.segment_path(loc.generation);
        let reader = match taken {
            Some(reader) => reader,
            None => BufferedFile::open(&path).await.map_err(io)?,
        };
        let read = reader
            .read_at(loc.offset, loc.len as usize)
            .await
            .map_err(io);
        self.inner
            .borrow_mut()
            .readers
            .insert(loc.generation, reader);
        let bytes = read?;
        match frame::decode_at(&bytes, 0) {
            Some((frame::Frame::Entry { entry, .. }, _)) => Ok(entry),
            _ => Err(io::Error::other(format!(
                "the frame at {}:{} is not a whole entry",
                loc.generation, loc.offset
            ))),
        }
    }
}

/// Which log a group store writes to
#[derive(Clone)]
enum Backend {
    /// The shard's shared file
    Shared(ShardWal),
    /// A volatile log in memory, for an ephemeral table's group
    Memory(MemoryWal),
}

/// One group's log store: openraft's log storage over the shard's WAL, or over memory
///
/// Cloneable, and every clone is the same log: openraft asks for a reader and a store and
/// drives them from different tasks, and on one thread an `Rc` is the whole of what that needs.
#[derive(Clone)]
pub struct GroupStore {
    /// Where the log lives
    backend: Backend,
    /// The group
    group: GroupId,
}

impl GroupStore {
    /// The group this store is for
    #[must_use]
    pub fn group(&self) -> GroupId {
        self.group
    }

    /// Whether this store keeps nothing across a restart
    #[must_use]
    pub fn is_volatile(&self) -> bool {
        matches!(self.backend, Backend::Memory(_))
    }

    /// The index this group's log is purged to, if it was ever purged
    #[must_use]
    pub fn purged_index(&self) -> Option<u64> {
        match &self.backend {
            Backend::Shared(wal) => wal
                .inner
                .borrow()
                .groups
                .get(&self.group)
                .and_then(|log| log.purged.as_ref().map(|log_id| log_id.index)),
            Backend::Memory(memory) => memory
                .log_state(self.group)
                .last_purged_log_id
                .map(|log_id| log_id.index),
        }
    }

    /// How many bytes of entries this group holds, for admission against a volatile bound
    #[must_use]
    pub fn bytes(&self) -> usize {
        match &self.backend {
            Backend::Shared(_) => 0,
            Backend::Memory(memory) => memory.bytes(),
        }
    }
}

/// A reader over a group's log, which is the store itself
pub type GroupLogReader = GroupStore;

impl RaftLogReader<DataConfig> for GroupStore {
    /// The entries in a range of indexes
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        // the range as the maps take it
        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => start.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(end) => Some(*end),
            Bound::Excluded(end) => end.checked_sub(1),
            Bound::Unbounded => None,
        };
        // an empty or inverted range names nothing
        if end.is_some_and(|end| end < start) {
            return Ok(Vec::new());
        }
        match &self.backend {
            Backend::Memory(memory) => Ok(memory.entries(self.group, start, end)),
            Backend::Shared(wal) => {
                // every index in range, from the cache where it is there
                let (indexes, cached): (Vec<(u64, Loc)>, HashMap<u64, Entry>) = {
                    let inner = wal.inner.borrow();
                    let Some(log) = inner.groups.get(&self.group) else {
                        return Ok(Vec::new());
                    };
                    let upper = match end {
                        Some(end) => Bound::Included(end),
                        None => Bound::Unbounded,
                    };
                    let indexes = log
                        .index
                        .range((Bound::Included(start), upper))
                        .map(|(index, slot)| (*index, slot.loc))
                        .collect::<Vec<_>>();
                    let cached = indexes
                        .iter()
                        .filter_map(|(index, _)| {
                            log.cache.get(index).map(|entry| (*index, entry.clone()))
                        })
                        .collect();
                    (indexes, cached)
                };
                let mut entries = Vec::with_capacity(indexes.len());
                for (index, loc) in indexes {
                    match cached.get(&index) {
                        Some(entry) => entries.push(entry.clone()),
                        None => entries.push(wal.read_entry(loc).await?),
                    }
                }
                Ok(entries)
            }
        }
    }

    /// The last vote granted
    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(match &self.backend {
            Backend::Memory(memory) => memory.vote(self.group),
            Backend::Shared(wal) => wal
                .inner
                .borrow()
                .groups
                .get(&self.group)
                .and_then(|log| log.vote.clone()),
        })
    }
}

impl RaftLogStorage<DataConfig> for GroupStore {
    type LogReader = GroupLogReader;

    /// Where the log begins and ends
    async fn get_log_state(&mut self) -> Result<LogState<DataConfig>, io::Error> {
        Ok(match &self.backend {
            Backend::Memory(memory) => memory.log_state(self.group),
            Backend::Shared(wal) => {
                let inner = wal.inner.borrow();
                match inner.groups.get(&self.group) {
                    Some(log) => LogState {
                        last_purged_log_id: log.purged.clone(),
                        last_log_id: log.last_log_id(),
                    },
                    None => LogState {
                        last_purged_log_id: None,
                        last_log_id: None,
                    },
                }
            }
        })
    }

    /// A reader, which shares the log
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Record a vote, durably, before it is answered
    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        match &self.backend {
            Backend::Memory(memory) => {
                memory.save_vote(self.group, vote.clone());
                Ok(())
            }
            Backend::Shared(wal) => {
                let frame = frame::encode_vote(self.group, vote)?;
                wal.stage_and_wait(&frame, self.group).await?;
                wal.inner.borrow_mut().group(self.group).vote = Some(vote.clone());
                Ok(())
            }
        }
    }

    /// Record the committed log id, folded into the next batch
    async fn save_committed(&mut self, committed: Option<WalLogId>) -> Result<(), io::Error> {
        match &self.backend {
            Backend::Memory(memory) => memory.save_committed(self.group, committed),
            Backend::Shared(wal) => {
                let frame = frame::encode_marker(
                    frame::FrameKind::Committed,
                    self.group,
                    committed.as_ref(),
                )?;
                wal.stage(&frame, self.group, None)?;
                wal.inner.borrow_mut().group(self.group).committed = committed;
            }
        }
        Ok(())
    }

    /// The committed log id last recorded
    async fn read_committed(&mut self) -> Result<Option<WalLogId>, io::Error> {
        Ok(match &self.backend {
            Backend::Memory(memory) => memory.committed(self.group),
            Backend::Shared(wal) => wal
                .inner
                .borrow()
                .groups
                .get(&self.group)
                .and_then(|log| log.committed.clone()),
        })
    }

    /// Append entries into the open batch, completing the callback once they are durable
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<DataConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        match &self.backend {
            Backend::Memory(memory) => {
                memory.append(self.group, entries);
                // memory is as durable as it will ever be, so the callback fires at once
                callback.io_completed(Ok(()));
                Ok(())
            }
            Backend::Shared(wal) => {
                // the callback rides with the last frame, so it fires once all are durable
                let mut callback_slot = Some(callback);
                // every frame into the batch, each indexed and cached as it goes
                let mut frames = Vec::with_capacity(entries.len());
                for entry in &entries {
                    frames.push(frame::encode_entry(self.group, entry)?);
                }
                let last = entries.len().saturating_sub(1);
                for (at, (entry, encoded)) in entries.into_iter().zip(frames).enumerate() {
                    let callback = (at == last).then(|| callback_slot.take()).flatten();
                    let loc = wal.stage(&encoded, self.group, callback)?;
                    let log_id = entry.log_id();
                    // a scrub is log alone, like a blank: it reaches no archive
                    let command = matches!(&entry.payload, EntryPayload::Normal(command) if command.scrub_op().is_none());
                    let mut inner = wal.inner.borrow_mut();
                    inner.index_entry(self.group, &log_id, command, loc);
                    inner.cache_entry(self.group, entry, encoded.len());
                }
                // an empty append is durable already
                if let Some(callback) = callback_slot.take() {
                    callback.io_completed(Ok(()));
                }
                Ok(())
            }
        }
    }

    /// Drop every entry after a log id
    async fn truncate_after(&mut self, last_log_id: Option<WalLogId>) -> Result<(), io::Error> {
        match &self.backend {
            Backend::Memory(memory) => {
                memory.truncate_after(self.group, last_log_id.map(|log_id| log_id.index));
                Ok(())
            }
            Backend::Shared(wal) => {
                let frame = frame::encode_marker(
                    frame::FrameKind::Truncate,
                    self.group,
                    last_log_id.as_ref(),
                )?;
                wal.stage_and_wait(&frame, self.group).await?;
                wal.inner
                    .borrow_mut()
                    .truncate_index(self.group, last_log_id.map(|log_id| log_id.index));
                Ok(())
            }
        }
    }

    /// Drop every entry up to and including a log id
    async fn purge(&mut self, log_id: WalLogId) -> Result<(), io::Error> {
        match &self.backend {
            Backend::Memory(memory) => {
                memory.purge(self.group, log_id);
                Ok(())
            }
            Backend::Shared(wal) => {
                // a purge never moves the boundary backwards
                let purged = {
                    let inner = wal.inner.borrow();
                    match inner
                        .groups
                        .get(&self.group)
                        .and_then(|log| log.purged.clone())
                    {
                        Some(existing) if existing.index >= log_id.index => existing,
                        _ => log_id,
                    }
                };
                let frame =
                    frame::encode_marker(frame::FrameKind::Purged, self.group, Some(&purged))?;
                wal.stage(&frame, self.group, None)?;
                let mut inner = wal.inner.borrow_mut();
                let index = purged.index;
                inner.group(self.group).purged = Some(purged);
                inner.purge_index(self.group, index);
                Ok(())
            }
        }
    }
}
