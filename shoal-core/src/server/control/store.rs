//! The control group's durable storage: a log and a state machine under `control/`
//!
//! Everything openraft has to be able to recover lives under `<latency_sensitive.path>/control/`:
//!
//! ```text
//! control/
//!   vote.json        the last vote granted, written before the grant is answered
//!   committed.json   the last committed log id, a hint that speeds recovery
//!   purged.json      the last purged log id, which is where the log begins
//!   log              [u32 len][u32 gxhash32][json entry] frames, appended and fdatasynced
//!   state.json       the applied state: last applied log id, membership, ControlState
//!   snapshot.json    the last snapshot built or installed, meta and data
//! ```
//!
//! Every file but the log is replaced whole - staged as `<name>.tmp`, fdatasynced, renamed over
//! the old one, and the directory synced - so a crash leaves the old file or the new one. The
//! log is appended in place, one fdatasync per append, and its frames are what make a torn
//! append harmless: a frame whose length runs past the end of the file or whose checksum does not
//! match is the tail of a write that never finished, and opening the log truncates it there.
//! An [`IOFlushed`] callback completes only after the fdatasync, which is the durability
//! openraft's quorum counts on ([P3](../../../../docs/src/distributed/protocol.md)).
//!
//! The whole log is held in memory as well. A control log is small - a member record per
//! start and a policy change per admin action - and openraft's storage contract is written for
//! stores that answer reads without IO. `truncate_after` and `purge` rewrite the log file from
//! the map rather than editing it in place, for the same reason the marker is never edited.
//!
//! JSON rather than rkyv because these files are read by a person as often as by the server:
//! `cat control/state.json` is the topology view a node has of its cluster.
//!
//! `openraft::testing::log::Suite` is the contract and `control_store_passes_the_openraft_storage_suite`
//! runs the whole of it; `control_store_recovers_from_a_torn_append` is the crash test
//! [M1](../../../../docs/src/distributed/milestones.md) asks for.

use std::cell::RefCell;
use std::collections::{BTreeMap, Bound};
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use futures::{Stream, StreamExt as _};
use glommio::io::{BufferedFile, Directory, OpenOptions};
use openraft::type_config::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use openraft::entry::RaftEntry as _;
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use serde::{Deserialize, Serialize};

use super::types::{ControlConfig, ControlResponse, ControlState};

/// The directory under the latency sensitive path the control files live in
pub const CONTROL_DIR: &str = "control";

/// The log file's name
const LOG_FILE: &str = "log";

/// The vote file's name
const VOTE_FILE: &str = "vote.json";

/// The committed file's name
const COMMITTED_FILE: &str = "committed.json";

/// The purged file's name
const PURGED_FILE: &str = "purged.json";

/// The applied state file's name
const STATE_FILE: &str = "state.json";

/// The snapshot file's name
const SNAPSHOT_FILE: &str = "snapshot.json";

/// The seed every frame checksum is taken with
///
/// Any constant would do; what matters is that it never changes, since a checksum taken under
/// another seed makes every frame in an existing log look torn.
const FRAME_SEED: i64 = 0;

/// The bytes a frame's header takes: the length and the checksum
const FRAME_HEADER: usize = 8;

/// A snapshot's data: the bytes of `state.json`
pub type SnapshotData = Cursor<Vec<u8>>;

/// An entry of the control log
type Entry = EntryOf<ControlConfig>;

/// A log id of the control log
type LogId = LogIdOf<ControlConfig>;

/// A vote in the control group
type Vote = VoteOf<ControlConfig>;

/// Turn a glommio error into the io error openraft wants
///
/// # Arguments
///
/// * `error` - The glommio error
fn io<T>(error: glommio::GlommioError<T>) -> io::Error {
    match error {
        glommio::GlommioError::IoError(error) => error,
        glommio::GlommioError::EnhancedIoError { source, op, path, .. } => io::Error::new(
            source.kind(),
            format!("{op} {}: {source}", path.map(|p| p.display().to_string()).unwrap_or_default()),
        ),
        other => io::Error::other(other.to_string()),
    }
}

/// Open the log file for reading and appending, creating it if it is not there
///
/// `BufferedFile::open` is read only and `create` truncates, and a log wants neither.
///
/// # Arguments
///
/// * `path` - The log file
async fn open_log(path: &Path) -> io::Result<BufferedFile> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .buffered_open(path)
        .await
        .map_err(io)
}

/// Read a whole file, or none if it does not exist
///
/// # Arguments
///
/// * `path` - The file
async fn read_file(path: &Path) -> io::Result<Option<Vec<u8>>> {
    // a file that is not there is a value that was never written, which is not an error
    if !path.exists() {
        return Ok(None);
    }
    let file = BufferedFile::open(path).await.map_err(io)?;
    let size = file.file_size().await.map_err(io)?;
    let bytes = if size == 0 {
        Vec::new()
    } else {
        // `usize` from `u64` is a lossless conversion on every target this runs on
        let read = file.read_at(0, size as usize).await.map_err(io)?;
        read.to_vec()
    };
    file.close().await.map_err(io)?;
    Ok(Some(bytes))
}

/// Read a json file into a value, or none if it does not exist
///
/// # Arguments
///
/// * `path` - The file
async fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Option<T>> {
    match read_file(path).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

/// Replace a file so that a crash leaves the old one or the new one
///
/// Staged beside the target, fdatasynced, renamed over it, and then the directory is synced so
/// the rename itself is durable.
///
/// # Arguments
///
/// * `dir` - The directory the file is in
/// * `name` - The file's name
/// * `bytes` - What it should hold
async fn write_atomic(dir: &Path, name: &str, bytes: Vec<u8>) -> io::Result<()> {
    // stage the new contents beside the old
    let staged = dir.join(format!("{name}.tmp"));
    let mut file = BufferedFile::create(&staged).await.map_err(io)?;
    if !bytes.is_empty() {
        file.write_at(bytes, 0).await.map_err(io)?;
    }
    file.fdatasync().await.map_err(io)?;
    // swap it in, which is the atomic step
    file.rename(dir.join(name)).await.map_err(io)?;
    file.close().await.map_err(io)?;
    // and make the swap durable
    let directory = Directory::open(dir).await.map_err(io)?;
    directory.sync().await.map_err(io)?;
    directory.close().await.map_err(io)?;
    Ok(())
}

/// Write a value as json, atomically
///
/// # Arguments
///
/// * `dir` - The directory the file is in
/// * `name` - The file's name
/// * `value` - What to write
async fn write_json<T: Serialize>(dir: &Path, name: &str, value: &T) -> io::Result<()> {
    write_atomic(dir, name, serde_json::to_vec_pretty(value)?).await
}

/// Encode one entry as a frame
///
/// # Arguments
///
/// * `entry` - The entry
fn encode_frame(entry: &Entry) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(entry)?;
    // the length first, then the checksum over the body, then the body
    let len = u32::try_from(body.len())
        .map_err(|_| io::Error::other("a log entry does not fit in a frame"))?;
    let mut frame = Vec::with_capacity(FRAME_HEADER + body.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&gxhash::gxhash32(&body, FRAME_SEED).to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Decode every whole frame in a log, and say where the first torn one begins
///
/// A frame is torn if its header runs past the end, its body runs past the end, or its checksum
/// does not match. Everything before the first torn frame is kept; nothing after it is, since a
/// frame after a torn one was written after a write that never finished.
///
/// # Arguments
///
/// * `bytes` - The whole log file
fn decode_frames(bytes: &[u8]) -> (Vec<Entry>, u64) {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset + FRAME_HEADER <= bytes.len() {
        // the header
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
        let start = offset + FRAME_HEADER;
        let Some(end) = start.checked_add(len).filter(|end| *end <= bytes.len()) else {
            break;
        };
        let body = &bytes[start..end];
        // a body that does not match its checksum is one that was half written
        if gxhash::gxhash32(body, FRAME_SEED) != checksum {
            break;
        }
        // a body that does not parse is treated the same way rather than taking the log down
        let Ok(entry) = serde_json::from_slice::<Entry>(body) else {
            break;
        };
        entries.push(entry);
        offset = end;
    }
    (entries, offset as u64)
}

/// The log's state, shared by the store and its readers
struct LogInner {
    /// The directory the files are in
    dir: PathBuf,
    /// Every entry, by index
    entries: BTreeMap<u64, Entry>,
    /// The last vote granted
    vote: Option<Vote>,
    /// The last committed log id
    committed: Option<LogId>,
    /// The last purged log id
    purged: Option<LogId>,
    /// The open log file
    file: Option<BufferedFile>,
    /// Where the next frame goes in it
    offset: u64,
}

impl LogInner {
    /// Rewrite the log file from the map, so that what is on disk is exactly what is in memory
    ///
    /// Staged and renamed the way every other file is, and then reopened for appends.
    async fn rewrite(&mut self) -> io::Result<()> {
        // every frame, in index order
        let mut bytes = Vec::new();
        for entry in self.entries.values() {
            bytes.extend_from_slice(&encode_frame(entry)?);
        }
        let offset = bytes.len() as u64;
        // close the old file before the rename replaces it
        if let Some(file) = self.file.take() {
            file.close().await.map_err(io)?;
        }
        write_atomic(&self.dir, LOG_FILE, bytes).await?;
        // and reopen the new one for the appends that follow
        self.file = Some(open_log(&self.dir.join(LOG_FILE)).await?);
        self.offset = offset;
        Ok(())
    }
}

/// The control log
///
/// Cloneable, and every clone is the same log: openraft asks for a reader and a store and
/// drives them from different tasks, and on one thread an `Rc` is the whole of what that needs.
#[derive(Clone)]
pub struct ControlLog {
    /// The shared state
    inner: Rc<RefCell<LogInner>>,
}

impl ControlLog {
    /// Open the log under a control directory, recovering whatever is there
    ///
    /// A torn tail is truncated here, at open, so that every append that follows lands after a
    /// frame that is whole.
    ///
    /// # Arguments
    ///
    /// * `dir` - The control directory
    ///
    /// # Errors
    ///
    /// Fails if the directory cannot be created or a file cannot be read.
    pub async fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        // the small files, each of which may not exist yet
        let vote = read_json(&dir.join(VOTE_FILE)).await?;
        let committed = read_json(&dir.join(COMMITTED_FILE)).await?;
        let purged = read_json(&dir.join(PURGED_FILE)).await?;
        // the log, decoded up to its first torn frame
        let path = dir.join(LOG_FILE);
        let bytes = read_file(&path).await?.unwrap_or_default();
        let (decoded, whole) = decode_frames(&bytes);
        let file = open_log(&path).await?;
        // cut the torn tail off, so the next append does not land after it
        if whole < bytes.len() as u64 {
            tracing::warn!(
                torn_from = whole,
                length = bytes.len(),
                "truncating a torn tail off the control log"
            );
            file.truncate(whole).await.map_err(io)?;
            file.fdatasync().await.map_err(io)?;
        }
        let entries = decoded
            .into_iter()
            .map(|entry| (entry.index(), entry))
            .collect();
        Ok(ControlLog {
            inner: Rc::new(RefCell::new(LogInner {
                dir: dir.to_path_buf(),
                entries,
                vote,
                committed,
                purged,
                file: Some(file),
                offset: whole,
            })),
        })
    }

    /// How many entries the log holds in memory
    pub fn len(&self) -> usize {
        self.inner.borrow().entries.len()
    }

    /// Whether the log holds nothing
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A reader over the control log, which is the log itself
pub type ControlLogReader = ControlLog;

impl RaftLogReader<ControlConfig> for ControlLog {
    /// The entries in a range of indexes
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let inner = self.inner.borrow();
        // the map's own range, with the bounds copied out of the caller's
        let start = match range.start_bound() {
            Bound::Included(start) => Bound::Included(*start),
            Bound::Excluded(start) => Bound::Excluded(*start),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(end) => Bound::Included(*end),
            Bound::Excluded(end) => Bound::Excluded(*end),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(inner
            .entries
            .range((start, end))
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    /// The last vote granted
    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.borrow().vote.clone())
    }
}

impl RaftLogStorage<ControlConfig> for ControlLog {
    type LogReader = ControlLogReader;

    /// Where the log begins and ends
    async fn get_log_state(&mut self) -> Result<LogState<ControlConfig>, io::Error> {
        let inner = self.inner.borrow();
        // the last entry if there is one, else the last purged: a fully purged log still ends
        // where it was purged to
        let last_log_id = inner
            .entries
            .values()
            .next_back()
            .map(|entry| entry.log_id())
            .or_else(|| inner.purged.clone());
        Ok(LogState {
            last_purged_log_id: inner.purged.clone(),
            last_log_id,
        })
    }

    /// A reader, which shares the log
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Record a vote, durably, before it is answered
    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let dir = self.inner.borrow().dir.clone();
        write_json(&dir, VOTE_FILE, vote).await?;
        self.inner.borrow_mut().vote = Some(vote.clone());
        Ok(())
    }

    /// Record the committed log id, which speeds a recovery
    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        let dir = self.inner.borrow().dir.clone();
        write_json(&dir, COMMITTED_FILE, &committed).await?;
        self.inner.borrow_mut().committed = committed;
        Ok(())
    }

    /// The committed log id last recorded
    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.inner.borrow().committed.clone())
    }

    /// Append entries, and complete the callback once they are on disk
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<ControlConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // every frame in one write
        let entries: Vec<Entry> = entries.into_iter().collect();
        let mut bytes = Vec::new();
        for entry in &entries {
            bytes.extend_from_slice(&encode_frame(entry)?);
        }
        // the write, at the tail, and the sync that makes it durable
        let result = self.write_frames(bytes).await;
        // the entries are visible in memory only once they are on disk
        if result.is_ok() {
            let mut inner = self.inner.borrow_mut();
            for entry in entries {
                inner.entries.insert(entry.index(), entry);
            }
        }
        // the callback is what openraft's quorum counts, so it fires after the sync and never before
        callback.io_completed(result.as_ref().map(|_| ()).map_err(|error| io::Error::new(error.kind(), error.to_string())));
        result
    }

    /// Drop every entry after a log id, rewriting the file
    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.borrow_mut();
            // none means everything goes
            let keep_to = last_log_id.as_ref().map(|log_id| log_id.index());
            inner.entries.retain(|index, _| keep_to.is_some_and(|keep| *index <= keep));
        }
        self.rewrite().await
    }

    /// Drop every entry up to and including a log id, rewriting the file
    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let dir = self.inner.borrow().dir.clone();
        {
            let mut inner = self.inner.borrow_mut();
            inner.entries.retain(|index, _| *index > log_id.index());
            // a purge never moves the boundary backwards
            let purged = match &inner.purged {
                Some(existing) if existing.index() >= log_id.index() => existing.clone(),
                _ => log_id,
            };
            inner.purged = Some(purged);
        }
        // the boundary first, so a crash between the two leaves a log that begins at or after it
        let purged = self.inner.borrow().purged.clone();
        write_json(&dir, PURGED_FILE, &purged).await?;
        self.rewrite().await
    }
}

impl ControlLog {
    /// Write frames at the tail of the log and sync them
    ///
    /// # Arguments
    ///
    /// * `bytes` - The frames
    async fn write_frames(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        // nothing to write is nothing to sync
        if bytes.is_empty() {
            return Ok(());
        }
        // the file and offset, taken out so the borrow does not span the await
        let (file, offset) = {
            let mut inner = self.inner.borrow_mut();
            let file = inner
                .file
                .take()
                .ok_or_else(|| io::Error::other("the control log is not open"))?;
            (file, inner.offset)
        };
        let len = bytes.len() as u64;
        let written = file.write_at(bytes, offset).await;
        let synced = match written {
            Ok(_) => file.fdatasync().await.map_err(io),
            Err(error) => Err(io(error)),
        };
        // the file goes back whatever happened, and the offset moves only on success
        let mut inner = self.inner.borrow_mut();
        inner.file = Some(file);
        if synced.is_ok() {
            inner.offset = offset + len;
        }
        synced
    }

    /// Rewrite the log file from the map
    async fn rewrite(&mut self) -> io::Result<()> {
        // the inner state is borrowed across the awaits of the rewrite, which is sound on one
        // thread as long as nothing else reaches the log in between - and nothing does, since
        // openraft drives the log store from one task
        let inner = self.inner.clone();
        let mut inner = inner.borrow_mut();
        inner.rewrite().await
    }
}

/// What the state machine keeps on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    /// The last applied log id
    applied: Option<LogId>,
    /// The last membership applied
    membership: StoredMembershipOf<ControlConfig>,
    /// The application state
    state: ControlState,
}

/// What a snapshot keeps on disk
#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    /// What the snapshot covers
    meta: SnapshotMetaOf<ControlConfig>,
    /// The snapshot's data, which is a `state.json`
    data: Vec<u8>,
}

/// The state machine's state, shared with its snapshot builder
struct MachineInner {
    /// The directory the files are in
    dir: PathBuf,
    /// What has been applied
    persisted: PersistedState,
    /// The last snapshot built or installed
    snapshot: Option<SnapshotOf<ControlConfig, SnapshotData>>,
    /// Who to tell after every persisted apply or install, with the index applied
    ///
    /// The control loop hangs off this: it is how a joiner learns it has been admitted and
    /// how every node learns the topology moved, without polling the state
    /// ([F39](../../../../docs/src/features/membership.md)).
    on_applied: Option<Rc<dyn Fn(u64)>>,
}

/// The control state machine
#[derive(Clone)]
pub struct ControlStateMachine {
    /// The shared state
    inner: Rc<RefCell<MachineInner>>,
}

impl ControlStateMachine {
    /// Open the state machine under a control directory, recovering whatever is there
    ///
    /// # Arguments
    ///
    /// * `dir` - The control directory
    ///
    /// # Errors
    ///
    /// Fails if the directory cannot be created or a file cannot be read.
    pub async fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        // the applied state, or nothing applied yet
        let persisted = read_json::<PersistedState>(&dir.join(STATE_FILE))
            .await?
            .unwrap_or_else(|| PersistedState {
                applied: None,
                membership: StoredMembership::default(),
                state: ControlState::default(),
            });
        // the last snapshot, if one was ever built
        let snapshot = read_json::<PersistedSnapshot>(&dir.join(SNAPSHOT_FILE))
            .await?
            .map(|snapshot| Snapshot {
                meta: snapshot.meta,
                snapshot: Cursor::new(snapshot.data),
            });
        Ok(ControlStateMachine {
            inner: Rc::new(RefCell::new(MachineInner {
                dir: dir.to_path_buf(),
                persisted,
                snapshot,
                on_applied: None,
            })),
        })
    }

    /// The applied state, as it is now
    pub fn state(&self) -> ControlState {
        self.inner.borrow().persisted.state.clone()
    }

    /// The last applied log index, or zero before anything was
    pub fn applied_index(&self) -> u64 {
        self.inner
            .borrow()
            .persisted
            .applied
            .as_ref()
            .map_or(0, |log_id| log_id.index())
    }

    /// Hear about every persisted apply or install, with the index applied
    ///
    /// # Arguments
    ///
    /// * `hook` - Called on the state machine's own thread after each write lands
    pub fn on_applied(&self, hook: Rc<dyn Fn(u64)>) {
        self.inner.borrow_mut().on_applied = Some(hook);
    }

    /// Tell whoever asked that something was applied
    fn notify_applied(&self) {
        let (hook, index) = {
            let inner = self.inner.borrow();
            (
                inner.on_applied.clone(),
                inner.persisted.applied.as_ref().map_or(0, |log_id| log_id.index()),
            )
        };
        if let Some(hook) = hook {
            hook(index);
        }
    }

    /// Write the applied state to disk
    async fn persist(&self) -> io::Result<()> {
        let (dir, persisted) = {
            let inner = self.inner.borrow();
            (inner.dir.clone(), inner.persisted.clone())
        };
        write_json(&dir, STATE_FILE, &persisted).await
    }
}

impl RaftSnapshotBuilder<ControlConfig> for ControlStateMachine {
    type SnapshotData = SnapshotData;

    /// Build a snapshot of the applied state, and keep it as the current one
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<ControlConfig, SnapshotData>, io::Error> {
        let (dir, meta, data) = {
            let inner = self.inner.borrow();
            let meta = SnapshotMeta {
                last_log_id: inner.persisted.applied.clone(),
                last_membership: inner.persisted.membership.clone(),
            };
            (inner.dir.clone(), meta, serde_json::to_vec_pretty(&inner.persisted)?)
        };
        // on disk, so a restart still has it
        write_json(
            &dir,
            SNAPSHOT_FILE,
            &PersistedSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            },
        )
        .await?;
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(data),
        };
        self.inner.borrow_mut().snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }
}

impl RaftStateMachine<ControlConfig> for ControlStateMachine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = ControlStateMachine;

    /// What has been applied, and the membership as of then
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembershipOf<ControlConfig>), io::Error> {
        let inner = self.inner.borrow();
        Ok((inner.persisted.applied.clone(), inner.persisted.membership.clone()))
    }

    /// Apply a batch of entries, persist the result, and then answer each one
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<ControlConfig>, io::Error>> + Unpin + OptionalSend,
    {
        // apply everything in memory first, keeping each responder for after the write
        let mut responders = Vec::new();
        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            let log_id = entry.log_id();
            let response = {
                let mut inner = self.inner.borrow_mut();
                let response = match entry.payload {
                    // a blank entry changes nothing, and answers with where things stand
                    EntryPayload::Blank => ControlResponse::Applied {
                        topology_version: inner.persisted.state.topology_version,
                    },
                    // a command, applied by the state
                    EntryPayload::Normal(command) => inner.persisted.state.apply(&command),
                    // a membership change, recorded as of this entry and reflected into the
                    // members' roles, so the state and the configuration never disagree
                    EntryPayload::Membership(membership) => {
                        inner.persisted.state.observe_membership(&membership);
                        inner.persisted.membership =
                            StoredMembership::new(Some(log_id.clone()), membership);
                        ControlResponse::Applied {
                            topology_version: inner.persisted.state.topology_version,
                        }
                    }
                };
                inner.persisted.applied = Some(log_id);
                response
            };
            responders.push((responder, response));
        }
        // then on disk, so an answer is never given for a state a crash could lose
        self.persist().await?;
        // and only then the answers, and whoever is listening for applies
        for (responder, response) in responders {
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        self.notify_applied();
        Ok(())
    }

    /// The builder, which is this state machine
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Replace the applied state with a snapshot's
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlConfig>,
        snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        // the state the snapshot carries, which is a persisted state whole
        let persisted: PersistedState = serde_json::from_slice(&data)?;
        {
            let mut inner = self.inner.borrow_mut();
            inner.persisted = persisted;
            // the snapshot's meta is authoritative about where it ends, and its membership is
            // reflected into the roles as an entry's would be
            inner.persisted.applied = meta.last_log_id.clone();
            inner.persisted.membership = meta.last_membership.clone();
            inner.persisted.state.observe_membership(meta.last_membership.membership());
            inner.snapshot = Some(Snapshot {
                meta: meta.clone(),
                snapshot: Cursor::new(data.clone()),
            });
        }
        self.persist().await?;
        let dir = self.inner.borrow().dir.clone();
        write_json(
            &dir,
            SNAPSHOT_FILE,
            &PersistedSnapshot {
                meta: meta.clone(),
                data,
            },
        )
        .await?;
        self.notify_applied();
        Ok(())
    }

    /// The last snapshot built or installed
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<ControlConfig, SnapshotData>>, io::Error> {
        Ok(self.inner.borrow().snapshot.clone())
    }
}

/// Rewrite a stopped survivor's membership after a permanent majority loss
///
/// Appends a membership entry naming the survivors as the whole configuration and a
/// [`ControlCommand::ForceRecovered`] after it, both at a term past every term the log and the
/// vote have seen and led by this node; grants this node's vote at that term; marks both
/// committed, which a configuration of one survivor makes true by itself; and applies both to
/// the state machine, so the node starts with the lost members removed and the recovery
/// recorded. Every entry the log holds past what the machine applied is applied first, in
/// order: the recovery commits the whole log, so a joiner replaying it applies those entries
/// too, and a survivor that skipped them would carry a state no replay of its own log
/// reproduces. Offline only: run on a stopped directory under its lock
/// ([F49](../../../../docs/src/features/backup-and-recovery.md)). Idempotent by inspection: a
/// log whose last entry is already the same recovery is left alone.
///
/// # Arguments
///
/// * `log` - The survivor's control log
/// * `machine` - Its state machine
/// * `me` - The survivor
/// * `membership` - The membership to write: the survivors, with their records
/// * `command` - The recovery to record
///
/// # Errors
///
/// Fails if a write does not land.
pub async fn force_recover(
    log: &mut ControlLog,
    machine: &ControlStateMachine,
    me: super::types::NodeIdOf,
    membership: openraft::Membership<super::types::NodeIdOf, super::types::MemberRecord>,
    command: super::types::ControlCommand,
) -> io::Result<u64> {
    use openraft::storage::RaftLogStorageExt as _;
    use openraft::vote::{RaftLeaderId as _, RaftVote as _};
    // where the log and the vote stand, so the recovery is past both
    let state = log.get_log_state().await?;
    let vote = log.read_vote().await?;
    let last = state.last_log_id.clone();
    let last_index = last.as_ref().map_or_else(|| machine.applied_index(), |log_id| log_id.index());
    let last_term = last.as_ref().map_or(0, |log_id| log_id.leader_id.term);
    let vote_term = vote.as_ref().map_or(0, |vote| vote.leader_id().term);
    let term = last_term.max(vote_term) + 1;
    // the same recovery already written is not written twice: the same survivors, the same
    // lost members, run on the same node, whatever its identity and time
    if let Some(last) = &last {
        if let Some(entry) = log.inner.borrow().entries.get(&last.index()) {
            if let (
                EntryPayload::Normal(super::types::ControlCommand::ForceRecovered { survivors, lost, at, .. }),
                super::types::ControlCommand::ForceRecovered { survivors: wanted, lost: lost_wanted, at: at_wanted, .. },
            ) = (&entry.payload, &command)
            {
                if survivors == wanted && lost == lost_wanted && at == at_wanted {
                    return Ok(last.index());
                }
            }
        }
    }
    let leader = openraft::impls::leader_id_adv::LeaderId::new(term, me);
    let vote_at = Vote::from_leader_id(leader.clone(), true);
    let first = LogId::new(leader.clone(), last_index + 1);
    let second = LogId::new(leader.clone(), last_index + 2);
    let entries = vec![
        Entry::new_membership(first.clone(), membership.clone()),
        Entry::new_normal(second.clone(), command.clone()),
    ];
    // the log first: the entries, the vote at the new term, and the commit
    log.blocking_append(entries).await.map_err(|error| io::Error::other(format!("{error}")))?;
    log.save_vote(&vote_at).await?;
    log.save_committed(Some(second.clone())).await?;
    // then the applied state, so the start finds the lost members removed. what the log held
    // unapplied is applied first: it is committed now, and every replica of this log applies
    // it in order, so the survivor has to as well or its state diverges from every joiner's
    let unapplied: Vec<Entry> = {
        let inner = log.inner.borrow();
        inner
            .entries
            .range(machine.applied_index() + 1..=last_index)
            .map(|(_, entry)| entry.clone())
            .collect()
    };
    {
        let mut inner = machine.inner.borrow_mut();
        for entry in unapplied {
            match &entry.payload {
                EntryPayload::Membership(named) => {
                    inner.persisted.state.observe_membership(named);
                    inner.persisted.membership = StoredMembership::new(Some(entry.log_id()), named.clone());
                }
                EntryPayload::Normal(command) => {
                    inner.persisted.state.apply(command);
                }
                EntryPayload::Blank => {}
            }
            inner.persisted.applied = Some(entry.log_id());
        }
        inner.persisted.state.observe_membership(&membership);
        inner.persisted.membership = StoredMembership::new(Some(first), membership);
        inner.persisted.state.apply(&command);
        inner.persisted.applied = Some(second.clone());
    }
    machine.persist().await?;
    Ok(second.index())
}

/// Open both halves of the store under a control directory
///
/// # Arguments
///
/// * `dir` - The control directory
///
/// # Errors
///
/// Fails if either half cannot be opened.
pub async fn open(dir: &Path) -> io::Result<(ControlLog, ControlStateMachine)> {
    let log = ControlLog::open(dir).await?;
    let machine = ControlStateMachine::open(dir).await?;
    Ok((log, machine))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::control::runtime::GlommioRuntime;
    use openraft::testing::log::StoreBuilder;
    use openraft::type_config::TypeConfigExt as _;
    use openraft::{AsyncRuntime as _, StorageError};
    use std::io::Write as _;

    /// Builds a fresh store in its own temp dir for each test of the suite
    struct Builder;

    impl StoreBuilder<ControlConfig, ControlLog, ControlStateMachine, tempfile::TempDir> for Builder {
        /// A fresh store, and the directory that has to outlive it
        async fn build(
            &self,
        ) -> Result<(tempfile::TempDir, ControlLog, ControlStateMachine), StorageError<ControlConfig>> {
            let dir = tempfile::tempdir().expect("failed to build a temp dir");
            let (log, machine) = open(&dir.path().join(CONTROL_DIR))
                .await
                .map_err(|error| StorageError::read(ControlConfig::err_from_error(&error)))?;
            Ok((dir, log, machine))
        }
    }

    /// The whole openraft storage conformance suite passes on the control store
    ///
    /// The test [M1](../../../../../docs/src/distributed/milestones.md) names for the storage
    /// seam: every read after every write, membership found in the log and in the state machine,
    /// truncation, purging, snapshots built and installed.
    #[test]
    fn control_store_passes_the_openraft_storage_suite() {
        let mut runtime = GlommioRuntime::new(1);
        runtime.block_on(async {
            openraft::testing::log::Suite::test_all(Builder)
                .await
                .expect("the storage suite failed");
        });
    }

    /// A crash between the write of an append and its sync leaves no partial entry visible
    ///
    /// The log is appended to directly, with a frame cut off partway, which is what the file
    /// looks like after a write the kernel took and never finished writing. Reopening finds the
    /// entries before it whole, the torn one gone, the vote intact, and the next append landing
    /// after the last whole frame rather than after the garbage.
    #[test]
    fn control_store_recovers_from_a_torn_append() {
        use openraft::storage::RaftLogStorageExt as _;
        use openraft::vote::RaftLeaderIdExt as _;

        let mut runtime = GlommioRuntime::new(1);
        runtime.block_on(async {
            let dir = tempfile::tempdir().expect("failed to build a temp dir");
            let control = dir.path().join(CONTROL_DIR);
            // an entry of each shape, and a vote
            let leader = <ControlConfig as openraft::RaftTypeConfig>::LeaderId::new_committed(
                1,
                crate::shared::identity::NodeId::from(7),
            );
            let entries: Vec<Entry> = (1..=3)
                .map(|index| Entry::new_blank(LogId::new(leader.clone(), index)))
                .collect();
            let vote = Vote::new(1, crate::shared::identity::NodeId::from(7));
            {
                let (mut log, _machine) = open(&control).await.expect("failed to open");
                log.blocking_append(entries.clone()).await.expect("failed to append");
                log.save_vote(&vote).await.expect("failed to save a vote");
            }
            // the crash: a frame whose body was cut off, then a frame whose checksum is wrong
            let whole = std::fs::metadata(control.join(LOG_FILE)).expect("a log").len();
            {
                let torn = encode_frame(&Entry::new_blank(LogId::new(leader.clone(), 4)))
                    .expect("a frame");
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(control.join(LOG_FILE))
                    .expect("failed to open the log");
                file.write_all(&torn[..torn.len() - 5]).expect("failed to tear the log");
            }
            // reopening sees the three whole entries, and the vote
            {
                let (mut log, _machine) = open(&control).await.expect("failed to reopen");
                assert_eq!(log.len(), 3, "a torn entry was recovered as whole");
                let state = log.get_log_state().await.expect("a log state");
                assert_eq!(state.last_log_id, Some(LogId::new(leader.clone(), 3)));
                assert_eq!(log.read_vote().await.expect("a vote"), Some(vote.clone()));
                // and the file was cut back to the last whole frame
                let after = std::fs::metadata(control.join(LOG_FILE)).expect("a log").len();
                assert_eq!(after, whole, "the torn tail was left on disk");
                // so the next append lands after it, and reads back
                log.blocking_append([Entry::new_blank(LogId::new(leader.clone(), 4))])
                    .await
                    .expect("failed to append after recovery");
            }
            {
                let (log, _machine) = open(&control).await.expect("failed to reopen again");
                assert_eq!(log.len(), 4);
            }
            // a checksum that does not match is torn too, even with the length intact
            {
                let mut bad = encode_frame(&Entry::new_blank(LogId::new(leader.clone(), 5)))
                    .expect("a frame");
                let last = bad.len() - 1;
                bad[last] ^= 0xff;
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(control.join(LOG_FILE))
                    .expect("failed to open the log");
                file.write_all(&bad).expect("failed to corrupt the log");
            }
            let (log, _machine) = open(&control).await.expect("failed to reopen a corrupt log");
            assert_eq!(log.len(), 4, "a corrupt entry was recovered as whole");
        });
    }

    /// A recovery rewrites a survivor's membership to itself alone and a sole voter leads
    ///
    /// The spike [F49](../../../../../docs/src/features/backup-and-recovery.md) asked for: a
    /// control store whose committed membership names three voters is recovered offline to one,
    /// and a `Raft` opened over it with `enable_leader_restore: Some(false)` - the setting the
    /// control plane runs under, which refuses to lead from a lease it cannot prove - elects
    /// itself from the rewritten membership without reaching anybody. The lost members are
    /// removed and tombstoned in the applied state, the recovery is recorded, and running the
    /// recovery again writes nothing. An entry the log held past what the machine had applied -
    /// what a leader appended and lost its quorum before committing - is applied by the
    /// recovery, since it commits the whole log and every joiner replays it.
    #[test]
    fn a_recovery_rewrites_membership_and_a_sole_voter_leads() {
        use crate::server::control::network::PeerNetwork;
        use crate::server::control::types::{ControlCommand, MemberPhase, MemberRecord};
        use crate::server::meta::Identity;
        use crate::server::peer::Local;
        use crate::shared::identity::{ClusterId, NodeId};
        use openraft::storage::RaftLogStorageExt as _;
        use openraft::vote::RaftLeaderIdExt as _;
        use openraft_rt::WatchReceiver as _;
        use std::collections::{BTreeMap, BTreeSet};

        let mut runtime = GlommioRuntime::new(1);
        runtime.block_on(async {
            let dir = tempfile::tempdir().expect("failed to build a temp dir");
            let control = dir.path().join(CONTROL_DIR);
            let (a, b, c) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
            let record = |node: NodeId| MemberRecord {
                node,
                client: format!("127.0.0.1:1{}", node.0.as_u128() % 1000),
                data: "127.0.0.1:2".to_string(),
                control: format!("127.0.0.1:3{}", node.0.as_u128() % 1000),
                shards: 1,
                incarnation: 1,
                ..MemberRecord::default()
            };
            // a cluster of three voters, bootstrapped and admitted, as the log would hold it
            let cluster = ClusterId::mint();
            let leader = <ControlConfig as openraft::RaftTypeConfig>::LeaderId::new_committed(1, a);
            let nodes: BTreeMap<NodeId, MemberRecord> = [a, b, c].into_iter().map(|node| (node, record(node))).collect();
            let three = openraft::Membership::new(vec![BTreeSet::from([a, b, c])], nodes).expect("a membership of three");
            let policy = crate::server::conf::Cluster::default().policy();
            let entries: Vec<Entry> = vec![
                Entry::new_membership(LogId::new(leader.clone(), 0), three.clone()),
                Entry::new_normal(
                    LogId::new(leader.clone(), 1),
                    ControlCommand::Bootstrap { cluster, policy, member: record(a) },
                ),
                Entry::new_normal(LogId::new(leader.clone(), 2), ControlCommand::Admit(record(b))),
                Entry::new_normal(LogId::new(leader.clone(), 3), ControlCommand::Admit(record(c))),
            ];
            {
                let (mut log, machine) = open(&control).await.expect("failed to open");
                log.blocking_append(entries.clone()).await.expect("failed to append");
                log.save_vote(&Vote::new(1, a)).await.expect("failed to save a vote");
                log.save_committed(Some(LogId::new(leader.clone(), 3))).await.expect("failed to save the commit");
                // applied, the way a running node would have
                {
                    let mut inner = machine.inner.borrow_mut();
                    for entry in &entries {
                        match &entry.payload {
                            EntryPayload::Membership(membership) => {
                                inner.persisted.state.observe_membership(membership);
                                inner.persisted.membership = StoredMembership::new(Some(entry.log_id()), membership.clone());
                            }
                            EntryPayload::Normal(command) => {
                                inner.persisted.state.apply(command);
                            }
                            EntryPayload::Blank => {}
                        }
                        inner.persisted.applied = Some(entry.log_id());
                    }
                }
                machine.persist().await.expect("failed to persist");
                assert_eq!(machine.state().members.len(), 3);
                // and one more appended, never committed and never applied: the leader lost
                // its quorum with it in flight
                let version = machine.state().topology_version;
                let pending = ControlCommand::SetControlVoters { op: uuid::Uuid::new_v4(), principal: "test".to_string(), expected_version: version, count: 1 };
                log.blocking_append(vec![Entry::new_normal(LogId::new(leader.clone(), 4), pending)])
                    .await
                    .expect("failed to append the pending entry");
                assert_eq!(machine.state().policy.as_ref().expect("a policy").control_voters, 3, "the pending entry is not applied");
            }
            // the recovery: a alone, b and c lost
            let command = ControlCommand::ForceRecovered {
                op: uuid::Uuid::new_v4(),
                survivors: vec![a],
                lost: vec![b, c],
                at: a,
                last_committed: 3,
                recovered_ms: 1,
            };
            let alone = openraft::Membership::new(vec![BTreeSet::from([a])], BTreeMap::from([(a, record(a))])).expect("a membership of one");
            let recovered_at = {
                let (mut log, machine) = open(&control).await.expect("failed to reopen");
                let at = force_recover(&mut log, &machine, a, alone.clone(), command.clone()).await.expect("the recovery failed");
                assert_eq!(at, 6, "the recovery lands past the five entries");
                // the applied state: the pending entry applied, b and c removed and
                // tombstoned, the recovery recorded
                let state = machine.state();
                assert_eq!(state.policy.as_ref().expect("a policy").control_voters, 1, "the entry the log held unapplied is applied by the recovery");
                assert_eq!(state.members[&b].phase, MemberPhase::Removing);
                assert_eq!(state.members[&c].phase, MemberPhase::Removing);
                assert!(state.tombstones.contains_key(&b) && state.tombstones.contains_key(&c));
                assert_eq!(state.open_plans().len(), 2, "a removal plan per lost member");
                assert_eq!(state.members[&a].phase, MemberPhase::Member);
                assert_eq!(state.recoveries.len(), 1);
                assert_eq!(state.recoveries[0].lost, vec![b, c]);
                assert_eq!(state.recoveries[0].last_committed, 3);
                // the log: a membership of one at term 2, then the command, both committed
                let log_state = log.get_log_state().await.expect("a log state");
                assert_eq!(log_state.last_log_id.as_ref().map(|id| (id.leader_id.term, id.index)), Some((2, 6)));
                assert_eq!(log.read_committed().await.expect("a commit").map(|id| id.index), Some(6));
                assert_eq!(log.read_vote().await.expect("a vote").map(|vote| vote.leader_id().term), Some(2));
                // and again is nothing: the same recovery is not written twice
                let again = force_recover(&mut log, &machine, a, alone.clone(), command.clone()).await.expect("the second run failed");
                assert_eq!(again, 6);
                assert_eq!(log.len(), 7);
                at
            };
            // a group opened over the recovered store elects itself, reaching nobody
            let (log, machine) = open(&control).await.expect("failed to reopen for the group");
            let identity = Identity {
                node: a,
                cluster: Some(cluster),
                incarnation: 2,
                slots: 1,
                physical: 1,
                rehome: None,
                layout: crate::server::meta::CLUSTER_LAYOUT,
                topology_at_claim: 0,
                fresh: false,
                mode: crate::server::meta::MarkerMode::Cluster,
            };
            let local = Rc::new(RefCell::new(Local::new(&identity, 1, 0, 1 << 20, None)));
            let network = PeerNetwork::new(local, BTreeMap::new(), None, crate::server::conf::cluster::Transport::default());
            let config = openraft::Config {
                cluster_name: "recovered".to_string(),
                enable_leader_restore: Some(false),
                election_timeout_min: 150,
                election_timeout_max: 300,
                heartbeat_interval: 50,
                ..openraft::Config::default()
            }
            .validate()
            .expect("a config");
            let raft = openraft::Raft::<ControlConfig, ControlStateMachine>::new(a, std::sync::Arc::new(config), network, log, machine.clone())
                .await
                .expect("the group starts");
            raft.wait(Some(std::time::Duration::from_secs(10)))
                .current_leader(a, "the sole voter leads")
                .await
                .expect("the survivor never led");
            // and commits on its own
            let written = raft
                .client_write(ControlCommand::SetControlVoters { op: uuid::Uuid::new_v4(), principal: "test".to_string(), expected_version: machine.state().topology_version, count: 1 })
                .await
                .expect("a write through the sole voter");
            assert!(written.log_id.index > recovered_at);
            assert_eq!(raft.metrics().borrow_watched().membership_config.membership().voter_ids().collect::<Vec<_>>(), vec![a]);
            raft.shutdown().await.expect("shutdown");
        });
    }
}
