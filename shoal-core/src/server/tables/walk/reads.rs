//! A scan page's archive reads, several in flight and in runs in disk order
//! ([F125](../../../../../docs/src/features/page-reads-in-runs.md))
//!
//! A page's merge hands its partitions out in key order, and a page that read each archived one
//! as its merge reached it waited out a device's latency for every partition. A scan's page
//! instead plans the archived partitions it could still hand out before it reads any of them, a
//! wave at a time: their whole records sorted by where they lie and cut into runs across gaps of
//! a few KiB, each run one read, and a partition written as fragments read alone. The wave is
//! drawn from the page's budget before any of it is read, and a reader of the page's own, off its
//! merge, issues the reads in the order the merge needs them, each paced by its device before it
//! is issued and at most sixteen in flight, and sends each read's records to the merge once read,
//! which takes them in key order
//! ([W5](../../../../../docs/src/future-work/scans/tablet-reader.md#3-read-archives-in-the-order-they-lie-on-disk)).
//!
//! # Invariants
//!
//! **A record read in a run is verified as a read of it alone is**, and a run that cannot be read
//! whole is read again a record at a time, by the merge as it reaches each, so a record's outcome
//! never depends on the run it was read in.
//!
//! **A wave is drawn from the page's budget before any of its reads is issued**, and each read is
//! paced by its device before it is.
//!
//! **Only a partition no resident copy shadows is planned, from the page's view of its archive.**
//! A resident partition's archive is read as its encode step leaves the map, never as the plan's
//! view had it.
//!
//! **A wave is planned only once every partition of the one before it was taken**, so one reader
//! at a time answers a page, and the answers of a wave name partitions still ahead of its merge.

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use glommio::io::ReadResult;
use std::cell::Cell;
use std::collections::VecDeque;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{event, Level};

use super::{chain_bytes, ArchivedWalk, PageDraws, PendingWalk, WalkRows};
use crate::server::budget::Shed;
use crate::server::faults;
use crate::server::scans::PagePace;
use crate::server::tables::storage::fs::map::{run_ranges, ArchiveEntry};
use crate::server::tables::storage::fs::{ArchiveMap, ChainEntry};
use crate::server::tables::PartitionBytes;
use crate::server::ServerError;

/// The widest gap of other bytes a page's run reads through rather than start a new read: X17's,
/// over merges written in key order, never a snapshot cut's 128 KiB, which over a tablet's sparse
/// records reads several times its bytes to spare a few of its reads
/// ([W42](../../../../../docs/src/future-work/scans/scan-speed.md))
pub const RUN_GAP: u64 = 4 << 10;

/// The most bytes one read of a page spans, other partitions' bytes between its records included
pub const RUN_SPAN: u64 = 1 << 20;

/// The most reads a page keeps in flight: X17's sixteen, faster than two and a fifth cheaper a
/// partition ([X17](../../../../../docs/src/future-work/scans/page-rebuilt.md#what-it-says-for-the-design))
pub const READS_IN_FLIGHT: usize = 16;

/// The bytes ahead of a record's payload that a verified read of it reads too: its checksum
const RECORD_LEAD: u64 = 8;

/// Where the read of an archived partition a page looked ahead to stands
#[derive(Debug)]
pub(super) enum ReadState {
    /// In no wave: a driver's page reads it alone as its merge reaches it, a scan's plans a wave
    /// from it
    Unplanned,
    /// In a wave, its read not answered yet
    Planned,
    /// Read in a run: its payload, or why it was refused, as a read of it alone would answer
    Record(Result<ReadResult, ServerError>),
    /// Read alone as a whole partition: its bytes, or none if the map no longer holds it, and
    /// whether it was looked up again
    Partition(Result<(Option<PartitionBytes>, bool), ServerError>),
    /// Its run could not be read whole, so its merge reads it alone
    Alone,
}

impl ReadState {
    /// Whether the page's reader read this partition, so its merge reads nothing of it again
    #[must_use]
    pub(super) fn is_read(&self) -> bool {
        // a run's record, or a partition read alone by the reader
        matches!(self, ReadState::Record(_) | ReadState::Partition(_))
    }
}

/// An archived partition of a page's range, looked ahead to
#[derive(Debug)]
pub(super) struct Ahead {
    /// Its key
    pub(super) key: u64,
    /// Where the page's view says its records are
    pub(super) chain: ChainEntry,
    /// Where its read stands
    pub(super) state: ReadState,
}

/// One read a wave issues
#[derive(Debug)]
enum Issue {
    /// A run of whole records, as a range of the wave's records in disk order
    Run(Range<usize>),
    /// A partition written as fragments, read alone as a whole partition
    Chain {
        /// Its number among the partitions the page looked ahead to
        seq: u64,
        /// Its key
        key: u64,
        /// Where its records are
        chain: ChainEntry,
    },
}

impl Issue {
    /// The bytes this read takes from its device: a run's span, the bytes between its records
    /// included, or a chain's records
    ///
    /// # Arguments
    ///
    /// * `entries` - The wave's whole records in disk order
    fn bytes(&self, entries: &[ArchiveEntry]) -> u64 {
        // a run from its first record's checksum to its last record's end
        match self {
            Issue::Run(run) => span_of(&entries[run.clone()]),
            Issue::Chain { chain, .. } => chain_bytes(chain),
        }
    }
}

/// The bytes one read of a run spans, from its first record's checksum to its furthest end
///
/// # Arguments
///
/// * `run` - The run's records
fn span_of(run: &[ArchiveEntry]) -> u64 {
    // the records of a run are in offset order, but the span is taken whatever their order
    let start = run.iter().map(|entry| entry.offset.saturating_sub(RECORD_LEAD)).min();
    let end = run.iter().map(|entry| entry.offset + entry.size as u64).max();
    match (start, end) {
        (Some(start), Some(end)) => end - start,
        _ => 0,
    }
}

/// What one of a wave's reads answered, sent to the page's merge once read
#[derive(Debug)]
enum Answer {
    /// A run's records, each payload or refusal in the run's order
    Run {
        /// Where the run starts among the wave's records in disk order
        first: usize,
        /// Each record's payload, or why it was refused
        records: Vec<Result<ReadResult, ServerError>>,
    },
    /// A run that could not be read whole, whose records the merge reads alone
    Failed {
        /// Where the run starts among the wave's records in disk order
        first: usize,
        /// How many records it holds
        len: usize,
    },
    /// A partition read alone
    Partition {
        /// Its number among the partitions the page looked ahead to
        seq: u64,
        /// Its bytes, or none if the map no longer holds it, and whether it was looked up again
        read: Result<(Option<PartitionBytes>, bool), ServerError>,
    },
}

/// What a page's readers counted, read by its merge once the page ends
#[derive(Debug, Default)]
struct ReaderFigures {
    /// Archive reads issued: a run each, a chain's records a read each
    issued: Cell<u64>,
    /// How long the readers waited for their device
    paced: Cell<Duration>,
}

/// A page's archive reads: the partitions it looked ahead to, and the wave being read
#[derive(Default)]
pub(super) struct PageReads {
    /// The archived partitions looked ahead to, in key order, the merge's next at the front
    ahead: VecDeque<Ahead>,
    /// The number of the partition at the front, every partition numbered as it is looked ahead to
    front: u64,
    /// The wave's whole records in disk order, by their partitions' numbers
    slots: Vec<u64>,
    /// Where the wave's reads are answered
    answers: Option<UnboundedReceiver<Answer>>,
    /// The wave's reader, cancelled with the page
    reader: Option<glommio::Task<()>>,
    /// What the page's readers counted
    figures: Rc<ReaderFigures>,
}

impl std::fmt::Debug for PageReads {
    /// How far the page looked ahead and where its reader stands, since its records hold buffers
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PageReads")
            .field("ahead", &self.ahead.len())
            .field("front", &self.front)
            .field("reading", &self.reader.is_some())
            .finish()
    }
}

impl PageReads {
    /// The key of the archived partition at the front, looked ahead to if none is
    ///
    /// # Arguments
    ///
    /// * `archived` - The page's view of its archive
    ///
    /// # Errors
    ///
    /// Fails if the archive map's pages cannot be read.
    pub(super) async fn head(&mut self, archived: &mut ArchivedWalk) -> Result<Option<u64>, ServerError> {
        // the merge's next archived partition, read from the view only once it is needed
        if self.ahead.is_empty() {
            if let Some((key, chain)) = archived.next().await? {
                self.ahead.push_back(Ahead {
                    key,
                    chain,
                    state: ReadState::Unplanned,
                });
            }
        }
        Ok(self.ahead.front().map(|ahead| ahead.key))
    }

    /// Take the archived partition at the front
    pub(super) fn pop(&mut self) -> Option<Ahead> {
        // the partition behind it is the front's number now
        let ahead = self.ahead.pop_front()?;
        self.front += 1;
        Some(ahead)
    }

    /// Whether the archived partition at the front is in no wave yet
    #[must_use]
    pub(super) fn front_unplanned(&self) -> bool {
        // a partition a run failed to read is read alone, never planned again
        matches!(
            self.ahead.front(),
            Some(Ahead {
                state: ReadState::Unplanned,
                ..
            })
        )
    }

    /// Wait until the read of the partition at the front is answered, taking every answer that
    /// comes before it
    ///
    /// # Errors
    ///
    /// Fails if the page's reader ended with the front's read unanswered.
    pub(super) async fn wait_front(&mut self) -> Result<(), ServerError> {
        // the answers come in the order the reads finish, which is not the merge's
        while matches!(
            self.ahead.front(),
            Some(Ahead {
                state: ReadState::Planned,
                ..
            })
        ) {
            let answer = match self.answers.as_mut() {
                Some(answers) => answers.next().await,
                None => None,
            };
            let Some(answer) = answer else {
                return Err(ServerError::GlommioGeneric(
                    "a scan page's reader ended before its reads were answered".into(),
                ));
            };
            self.take(answer);
        }
        Ok(())
    }

    /// Put a read's answer on the partitions that wait for it
    ///
    /// # Arguments
    ///
    /// * `answer` - What the read answered
    fn take(&mut self, answer: Answer) {
        match answer {
            // each record of a run on its partition, verified as a read of it alone would be
            Answer::Run { first, records } => {
                for (at, record) in records.into_iter().enumerate() {
                    if let Some(ahead) = self.slot(first + at) {
                        ahead.state = ReadState::Record(record);
                    }
                }
            }
            // a run that failed whole leaves each of its records to be read alone
            Answer::Failed { first, len } => {
                for at in first..first + len {
                    if let Some(ahead) = self.slot(at) {
                        ahead.state = ReadState::Alone;
                    }
                }
            }
            // a partition read alone
            Answer::Partition { seq, read } => {
                if let Some(ahead) = self.numbered(seq) {
                    ahead.state = ReadState::Partition(read);
                }
            }
        }
    }

    /// The partition of one of the wave's records, by its place among them in disk order
    ///
    /// # Arguments
    ///
    /// * `at` - The record's place
    fn slot(&mut self, at: usize) -> Option<&mut Ahead> {
        // a place the wave does not have names nothing
        let seq = *self.slots.get(at)?;
        self.numbered(seq)
    }

    /// The partition of a number, if it is still ahead of the merge
    ///
    /// # Arguments
    ///
    /// * `seq` - The partition's number
    fn numbered(&mut self, seq: u64) -> Option<&mut Ahead> {
        // a number behind the front was taken already
        let at = usize::try_from(seq.checked_sub(self.front)?).ok()?;
        self.ahead.get_mut(at)
    }

    /// The archive reads the page's readers issued
    #[must_use]
    pub(super) fn issued(&self) -> u64 {
        self.figures.issued.get()
    }

    /// How long the page's readers waited for their device
    #[must_use]
    pub(super) fn paced(&self) -> Duration {
        self.figures.paced.get()
    }
}

impl PendingWalk {
    /// Plan a wave of a scan page's archive reads from the partition at the front of what it
    /// looked ahead to, draw it from the page's budget, and set its reader going
    ///
    /// The wave holds the archived partitions no resident copy shadows that the page could still
    /// hand out, counted with the resident partitions between them as the merge will count them,
    /// until the page's bounds of partitions or bytes are met. Their whole records are sorted by
    /// where they lie and cut into runs, a partition written as fragments is read alone, and the
    /// reads are issued in the order the merge needs them.
    ///
    /// Returns what the budget shed, if it had no room for the wave.
    ///
    /// # Arguments
    ///
    /// * `page` - The page so far
    /// * `draws` - A scan page's draws on its node's scan budget
    /// * `pace` - A scan page's pace through its device
    ///
    /// # Errors
    ///
    /// Fails if the archive map's pages cannot be read.
    pub(super) async fn plan_reads(
        &mut self,
        page: &WalkRows,
        draws: Option<&mut PageDraws>,
        pace: Option<&PagePace>,
    ) -> Result<Result<(), Shed>, ServerError> {
        // nothing is archived without a map, so there is nothing to read ahead of the merge
        let Some(map) = self.archived.inner.as_ref().map(|(map, _)| map.clone()) else {
            if let Some(front) = self.reads.ahead.front_mut() {
                front.state = ReadState::Alone;
            }
            return Ok(Ok(()));
        };
        let limit = self.bounds.limit.max(1);
        // what the page holds already, counted as its merge counts it
        let mut count = page.partitions.len();
        let mut bytes = page.bytes;
        let mut resident = self.resident.iter().peekable();
        // the wave's whole records with their partitions' numbers, and its reads by when needed
        let mut records: Vec<(ArchiveEntry, u64)> = Vec::new();
        let mut issue: Vec<(u64, Issue)> = Vec::new();
        let mut wave_bytes = 0u64;
        let mut at = 0usize;
        loop {
            // the next partition looked ahead to, looked further for once every one is
            if at == self.reads.ahead.len() {
                match self.archived.next().await? {
                    Some((key, chain)) => self.reads.ahead.push_back(Ahead {
                        key,
                        chain,
                        state: ReadState::Unplanned,
                    }),
                    None => break,
                }
            }
            let key = self.reads.ahead[at].key;
            // the resident partitions before it are handed out first, and one at its key shadows it
            let mut shadowed = false;
            while let Some((resident_key, captured)) = resident.peek() {
                if *resident_key > key {
                    break;
                }
                if captured.is_certain() {
                    count += 1;
                    bytes = bytes.saturating_add(captured.bound_bytes());
                }
                shadowed |= *resident_key == key;
                resident.next();
            }
            // nothing past the page's bounds is planned
            if count >= limit || bytes >= self.bounds.max_bytes {
                break;
            }
            let seq = self.reads.front + at as u64;
            let ahead = &mut self.reads.ahead[at];
            if !shadowed && matches!(ahead.state, ReadState::Unplanned) {
                // a whole record goes into a run, and a chain is read alone
                let partition_bytes = chain_bytes(&ahead.chain);
                if ahead.chain.fragments.is_empty() {
                    records.push((ahead.chain.base, seq));
                } else {
                    wave_bytes += partition_bytes;
                    issue.push((
                        seq,
                        Issue::Chain {
                            seq,
                            key,
                            chain: ahead.chain.clone(),
                        },
                    ));
                }
                ahead.state = ReadState::Planned;
                count += 1;
                bytes = bytes.saturating_add(partition_bytes);
            }
            at += 1;
        }
        // the whole records in disk order, cut into runs across a few KiB at most
        records.sort_unstable_by_key(|(entry, _)| (entry.archive, entry.offset));
        let (entries, slots): (Vec<ArchiveEntry>, Vec<u64>) = records.into_iter().unzip();
        for run in run_ranges(&entries, RUN_GAP, RUN_SPAN) {
            // a run is needed from its first partition in key order, and its read spans its
            // records' checksums and the bytes between them
            let need = slots[run.clone()].iter().copied().min().unwrap_or(u64::MAX);
            wave_bytes += span_of(&entries[run.clone()]);
            issue.push((need, Issue::Run(run)));
        }
        // issued in the order the merge needs them, so its next partition is never queued behind
        // ones it reaches later
        issue.sort_unstable_by_key(|(need, _)| *need);
        // the whole wave drawn before any of it is read, so no buffer is read past the budget (F118)
        if let Some(draws) = draws {
            if let Err(shed) = draws.cover(wave_bytes).await {
                return Ok(Err(shed));
            }
        }
        // its reader set going off the merge, the wave before it done with
        let (answers, answered) = futures::channel::mpsc::unbounded();
        let reader = read_wave(
            map,
            entries.into(),
            issue.into_iter().map(|(_, issue)| issue).collect(),
            pace.map(PagePace::beside),
            answers,
            self.reads.figures.clone(),
        );
        self.reads.slots = slots;
        self.reads.answers = Some(answered);
        self.reads.reader = Some(glommio::spawn_local(reader));
        Ok(Ok(()))
    }
}

/// Read a wave's reads, each paced by its device before it is issued and at most
/// [`READS_IN_FLIGHT`] at once, and send each one's answer to the page's merge once read
///
/// # Arguments
///
/// * `map` - The archive map the reads go through
/// * `entries` - The wave's whole records in disk order
/// * `issue` - The reads, in the order the merge needs them
/// * `pace` - The page's pace through its device, none for a page its node does not pace
/// * `answers` - Where each read's answer goes
/// * `figures` - What the page's readers count
async fn read_wave(
    map: Arc<ArchiveMap>,
    entries: Rc<[ArchiveEntry]>,
    issue: Vec<Issue>,
    pace: Option<PagePace>,
    answers: UnboundedSender<Answer>,
    figures: Rc<ReaderFigures>,
) {
    // each read paced in the order it is issued, so the page's credit carries from one to the next
    let paced = futures::stream::unfold((issue.into_iter(), pace), {
        let (entries, figures) = (entries.clone(), figures.clone());
        move |(mut issue, mut pace)| {
            let (entries, figures) = (entries.clone(), figures.clone());
            async move {
                let next = issue.next()?;
                if let Some(pace) = pace.as_mut() {
                    let before = pace.waited();
                    pace.take(next.bytes(&entries)).await;
                    figures.paced.set(figures.paced.get() + pace.waited().saturating_sub(before));
                }
                Some((next, (issue, pace)))
            }
        }
    });
    // at most READS_IN_FLIGHT read at once, each answered as soon as it is read
    paced
        .map(|next| read_one(map.clone(), entries.clone(), next, figures.clone()))
        .buffer_unordered(READS_IN_FLIGHT)
        .for_each(|answer| {
            // a page that ended takes no more answers, and cancels its reader as it drops
            let _ = answers.unbounded_send(answer);
            futures::future::ready(())
        })
        .await;
}

/// Issue one of a wave's reads and say what it answered
///
/// # Arguments
///
/// * `map` - The archive map the read goes through
/// * `entries` - The wave's whole records in disk order
/// * `issue` - The read
/// * `figures` - What the page's readers count
async fn read_one(
    map: Arc<ArchiveMap>,
    entries: Rc<[ArchiveEntry]>,
    issue: Issue,
    figures: Rc<ReaderFigures>,
) -> Answer {
    // a test may hold a page's reads once they are drawn and paced
    faults::wait_page_reads(map.table_name()).await;
    match issue {
        Issue::Run(run) => {
            figures.issued.set(figures.issued.get() + 1);
            let records = &entries[run.clone()];
            // one read of the run, every record in it verified apart, its archive let go whether
            // or not the read worked
            let read = async {
                let Some(first) = records.first() else {
                    return Ok(Vec::new());
                };
                let archive = map.get_archive(&first.archive).await?;
                let read = map.read_run_each(&archive, records).await;
                let released = archive.release().await;
                let read = read?;
                released?;
                Ok::<_, ServerError>(read)
            }
            .await;
            match read {
                Ok(records) => Answer::Run {
                    first: run.start,
                    records,
                },
                // an archive gone under the page, or a read that failed whole: each record is
                // read alone as the merge reaches it, followed to where it moved as a read of it
                // alone would be
                Err(error) => {
                    event!(Level::DEBUG, msg = "a scan page's run could not be read whole; its records are read alone", table = map.table_name(), records = run.len(), error = ?error);
                    Answer::Failed {
                        first: run.start,
                        len: run.len(),
                    }
                }
            }
        }
        // a chain's records a read each, folded into one partition, followed if it moved
        Issue::Chain { seq, key, chain } => {
            figures
                .issued
                .set(figures.issued.get() + 1 + chain.fragments.len() as u64);
            Answer::Partition {
                seq,
                read: map.read_current_from(key, chain).await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run_ranges, ArchiveEntry, READS_IN_FLIGHT, RUN_GAP, RUN_SPAN};
    use crate::server::budget::Budget;
    use crate::server::errors::ShoalError;
    use crate::server::faults;
    use crate::server::scans::{PagePace, TablePace};
    use crate::server::steps::Steps;
    use crate::server::tables::storage::fs::conf::{
        FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
    };
    use crate::server::tables::storage::fs::map::write_record;
    use crate::server::tables::storage::fs::ArchiveMap;
    use crate::server::tables::walk::{aligned, ArchivedWalk, PageDraws, PendingWalk, WalkBounds, WalkRows};
    use crate::server::ServerError;
    use crate::shared::identity::TableId;
    use futures::AsyncWriteExt;
    use glommio::LocalExecutor;
    use rkyv::util::AlignedVec;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use uuid::Uuid;

    /// The table these rows read, a name of their own since the hold on a page's reads is keyed
    /// by table
    const TABLE: &str = "PageReadsTest";

    /// Held by each row for its whole length: two of them hold this table's reads, and the count
    /// of reads waiting is process wide
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Take a row's turn, whichever row before it panicked, with no read of the table held
    fn turn() -> MutexGuard<'static, ()> {
        let turn = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        faults::hold_page_reads(TableId::of(TABLE).0, false);
        turn
    }

    /// A directory on a filesystem that reads directly, under cargo's target dir rather than a
    /// tmpfs, where glommio reads through the page cache
    fn test_dir() -> TempDir {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/shoal-test-tmp");
        std::fs::create_dir_all(&base).expect("a test directory");
        TempDir::new_in(&base).expect("a test directory")
    }

    /// An archive map of this module's table, its directories in place
    ///
    /// # Arguments
    ///
    /// * `temp_dir` - The directory to keep the table's storage in
    // shared as a table's storage shares its map, on one executor
    #[allow(clippy::arc_with_non_send_sync)]
    async fn archive_map(temp_dir: &TempDir) -> (FileSystemTableConf, Arc<ArchiveMap>) {
        let conf = FileSystemTableConf::builder()
            .latency_sensitive(FileSystemLatencyWriterConf::default().path(temp_dir.path()))
            .throughput_sensitive(FileSystemThroughputWriterConf::default().path(temp_dir.path()));
        conf.setup_paths(TABLE).await.unwrap();
        let map = ArchiveMap::new("shard-0", TABLE, &conf).await.unwrap();
        (conf, Arc::new(map))
    }

    /// The payload of a partition at a version
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `version` - Its version
    /// * `size` - Its length
    fn payload_of(key: u64, version: u64, size: usize) -> Vec<u8> {
        (0..size).map(|at| (key.wrapping_add(version * 7).wrapping_add(at as u64) % 251) as u8).collect()
    }

    /// Write records into the map's active archive one after another, each keyed one set in the
    /// map over whatever it held; a record of no key is another partition's bytes the walks never
    /// name
    ///
    /// Returns each record's entry, in the order written.
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    /// * `records` - Each record's key and payload length
    /// * `version` - The version every payload is written at
    async fn write(map: &ArchiveMap, records: &[(Option<u64>, usize)], version: u64) -> Vec<ArchiveEntry> {
        let mut writer = map.get_active_writer().await.unwrap();
        let archive = *map.active.borrow();
        let mut entries = Vec::with_capacity(records.len());
        for (key, size) in records {
            let named = key.unwrap_or(u64::MAX);
            let offset = write_record(&mut writer, &payload_of(named, version, *size)).await.unwrap();
            let entry = ArchiveEntry {
                key: named,
                archive,
                offset,
                size: *size,
            };
            if let Some(key) = key {
                let old = map.chain_of(*key).await.unwrap();
                map.set_partition(*key, entry, old.as_ref());
            }
            entries.push(entry);
        }
        writer.sync().await.unwrap();
        writer.close().await.unwrap();
        entries
    }

    /// Flip one byte in the middle of a record's payload on disk
    ///
    /// # Arguments
    ///
    /// * `conf` - The table's settings, for its archive's path
    /// * `entry` - The record
    fn flip_a_byte(conf: &FileSystemTableConf, entry: &ArchiveEntry) {
        use std::io::{Read, Seek, SeekFrom, Write};
        let path = conf.get_archive_path(TABLE).join(entry.archive.to_string());
        let mut file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let at = entry.offset + entry.size as u64 / 2;
        file.seek(SeekFrom::Start(at)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x40;
        file.seek(SeekFrom::Start(at)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
    }

    /// The test table's partition from what was read: its archived bytes as they are
    fn fold(
        _key: u64,
        archived: Option<&[u8]>,
        _resident: Option<&[u8]>,
    ) -> Result<Option<(u32, AlignedVec)>, ServerError> {
        Ok(archived.map(|bytes| (1, aligned(bytes))))
    }

    /// A page of the table's archived partitions in a range, nothing resident
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    /// * `low` - The first key of the range
    /// * `high` - The last key of the range
    /// * `scan` - Whether it is a scan's page, which reads in runs, or a driver's
    fn page_of(map: &Arc<ArchiveMap>, low: u64, high: u64, scan: bool) -> PendingWalk {
        let bounds = WalkBounds {
            tablet: 0,
            start: low,
            high,
            limit: 1000,
            max_bytes: 1 << 20,
            plan: scan,
        };
        let archived = ArchivedWalk::new(map.clone(), map.scan_range(low, high));
        PendingWalk::new(bounds, high, VecDeque::new(), archived, fold)
    }

    /// The keys a page handed out, each checked against its payload at a version
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    /// * `version` - The version each partition was written at, by key
    fn keys_of(page: &WalkRows, version: impl Fn(u64) -> u64) -> Vec<u64> {
        page.partitions
            .iter()
            .map(|partition| {
                let expected = payload_of(partition.key, version(partition.key), partition.bytes.len());
                assert_eq!(&partition.bytes[..], &expected[..], "partition {}", partition.key);
                partition.key
            })
            .collect()
    }

    /// Wait until a number of page reads are held, or fail the row after ten seconds
    ///
    /// # Arguments
    ///
    /// * `held` - How many
    async fn held_reads(held: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while faults::page_reads_waiting() < held {
            assert!(Instant::now() < deadline, "{} reads held of {held}", faults::page_reads_waiting());
            glommio::timer::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Records a few KiB apart are read in one run and records far apart alone, and every record
    /// is handed out in key order
    /// ([W5](../../../../../docs/src/future-work/scans/tablet-reader.md#acceptance-tests), M23)
    #[test]
    fn a_page_reads_its_archives_in_runs() {
        let _turn = turn();
        // the rule alone: a gap of exactly the run gap is read through and one byte more parts the
        // records, and no run spans more than the span or two archives
        let archive = Uuid::new_v4();
        let at = |offset: u64, size: usize| ArchiveEntry {
            key: offset,
            archive,
            offset,
            size,
        };
        assert_eq!(run_ranges(&[at(8, 100), at(116 + RUN_GAP, 100)], RUN_GAP, RUN_SPAN), vec![0..2]);
        assert_eq!(run_ranges(&[at(8, 100), at(117 + RUN_GAP, 100)], RUN_GAP, RUN_SPAN), vec![0..1, 1..2]);
        let wide = [at(8, 600 << 10), at((600 << 10) + 16, 600 << 10)];
        assert_eq!(run_ranges(&wide, RUN_GAP, RUN_SPAN), vec![0..1, 1..2]);
        let other = ArchiveEntry {
            archive: Uuid::new_v4(),
            ..at(116, 100)
        };
        assert_eq!(run_ranges(&[at(8, 100), other], RUN_GAP, RUN_SPAN), vec![0..1, 1..2]);
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let (_conf, map) = archive_map(&temp_dir).await;
            // four records side by side in no key order, 64 KiB of a partition the walk never
            // names, and two records past it
            let records = [
                (Some(50), 4096),
                (Some(30), 4096),
                (Some(90), 4096),
                (Some(10), 4096),
                (None, 64 << 10),
                (Some(20), 4096),
                (Some(80), 4096),
            ];
            write(&map, &records, 1).await;
            // a scan's page reads the four in one read and the two in another, and hands every
            // partition out in key order
            let scan = page_of(&map, 0, 1000, true)
                .finish(Steps::yielding(), None, None)
                .await
                .expect("a scan's page");
            assert_eq!(keys_of(&scan, |_| 1), vec![10, 20, 30, 50, 80, 90]);
            assert_eq!(scan.reads, 2, "the page did not read its records in two runs");
            assert_eq!(scan.archived, 6);
            // a driver's page reads the same partitions, a read each
            let driver = page_of(&map, 0, 1000, false)
                .finish(Steps::unyielding(), None, None)
                .await
                .expect("a driver's page");
            assert_eq!(keys_of(&driver, |_| 1), vec![10, 20, 30, 50, 80, 90]);
            assert_eq!(driver.reads, 6);
            map.close_all().await.unwrap();
        });
    }

    /// A scan's page issues several reads before the first is answered, each drawn from its
    /// budget and paced by its device first
    /// ([W42](../../../../../docs/src/future-work/scans/scan-speed.md#acceptance-tests), M23)
    #[test]
    fn a_scan_page_keeps_its_reads_in_flight() {
        let _turn = turn();
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let (_conf, map) = archive_map(&temp_dir).await;
            // six records, each 64 KiB of other bytes from the next, so each is a run of its own
            let mut records = Vec::new();
            for key in 1..=6u64 {
                records.push((Some(key * 10), 4096));
                records.push((None, 64 << 10));
            }
            write(&map, &records, 1).await;
            // a budget and a device of the page's own, and its reads held once drawn and paced
            let budget = Budget::new("a test's scan budget", 64 << 20);
            let device = Arc::new(TablePace::new(0, 1, 1 << 30, 1 << 30));
            faults::hold_page_reads(TableId::of(TABLE).0, true);
            let before = faults::page_reads_waiting();
            let reading = glommio::spawn_local({
                let (map, budget, device) = (map.clone(), budget.clone(), device.clone());
                async move {
                    let draws = PageDraws::new(budget, 0, 0);
                    page_of(&map, 0, 1000, true)
                        .finish(Steps::yielding(), Some(draws), Some(PagePace::new(device)))
                        .await
                }
            });
            // every one of the six is in flight before any is answered
            const { assert!(READS_IN_FLIGHT >= 6) };
            held_reads(before + 6).await;
            // the wave was drawn whole before any of it was read, a record and its checksum a run
            let spans = 6 * (4096 + 8);
            assert_eq!(budget.used(), spans, "the page's reads were not drawn before they were issued");
            // and each read was paced by its device before it was issued
            assert!(device.idle_bytes() >= spans, "{} paced of {spans}", device.idle_bytes());
            faults::hold_page_reads(TableId::of(TABLE).0, false);
            let page = reading.await.expect("the page");
            assert_eq!(keys_of(&page, |_| 1), vec![10, 20, 30, 40, 50, 60]);
            assert_eq!(page.reads, 6);
            // the partitions it handed out were drawn beside the wave, and every draw is given
            // back once the page's bytes drop
            assert_eq!(budget.used(), spans + 6 * 4096);
            drop(page);
            assert_eq!(budget.used(), 0);
            map.close_all().await.unwrap();
        });
    }

    /// A run with one corrupt record refuses that record as a read of it alone would, counted
    /// once, and hands on the others
    /// ([W42](../../../../../docs/src/future-work/scans/scan-speed.md#acceptance-tests), M23)
    #[test]
    fn a_record_in_a_run_is_verified_as_a_lone_read_is() {
        let _turn = turn();
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let (conf, map) = archive_map(&temp_dir).await;
            // three records side by side, the one between the others corrupt and last in key order
            let written = write(&map, &[(Some(10), 4096), (Some(30), 1024), (Some(20), 4096)], 1).await;
            flip_a_byte(&conf, &written[1]);
            // read as one run, each record verified apart: the corrupt one refused as a read of it
            // alone refuses it and counted once, the one after it on disk handed on
            let failures = map.integrity.checksum_failures.get();
            let archive = map.get_archive(&written[0].archive).await.unwrap();
            let records = map.read_run_each(&archive, &written).await.expect("the run was read");
            archive.release().await.unwrap();
            assert_eq!(map.integrity.checksum_failures.get(), failures + 1);
            let lone = map.read_record(&written[1]).await.expect_err("a flipped byte read as valid");
            let refused = records[1].as_ref().expect_err("a flipped byte read as valid in a run");
            assert!(
                matches!(refused, ServerError::Shoal(ShoalError::CorruptArchive { partition_id: 30, .. })),
                "{refused:?}"
            );
            assert_eq!(format!("{refused:?}"), format!("{lone:?}"));
            for (entry, record) in [(&written[0], &records[0]), (&written[2], &records[2])] {
                let payload = record.as_ref().expect("a sound record beside a corrupt one");
                assert_eq!(&payload[..], &payload_of(entry.key, 1, entry.size)[..]);
            }
            // a scan's page over the three fails naming it, counted once, as a driver's page
            // reading each alone fails
            let failures = map.integrity.checksum_failures.get();
            let scan = page_of(&map, 0, 1000, true)
                .finish(Steps::yielding(), None, None)
                .await
                .expect_err("a scan's page handed out a corrupt record");
            assert_eq!(map.integrity.checksum_failures.get(), failures + 1);
            let driver = page_of(&map, 0, 1000, false)
                .finish(Steps::unyielding(), None, None)
                .await
                .expect_err("a driver's page handed out a corrupt record");
            assert_eq!(format!("{scan:?}"), format!("{driver:?}"));
            // and a page whose range stops short of it reads the other two in one run across its
            // bytes, which nothing verifies, and hands both out
            let short = page_of(&map, 0, 25, true)
                .finish(Steps::yielding(), None, None)
                .await
                .expect("a page beside a corrupt record");
            assert_eq!(keys_of(&short, |_| 1), vec![10, 20]);
            assert_eq!(short.reads, 1);
            map.close_all().await.unwrap();
        });
    }

    /// Records an archive pass moves, or a merge rewrites, between a page's plan and its read are
    /// found, each at or after the page's boundary
    /// ([W5](../../../../../docs/src/future-work/scans/tablet-reader.md#acceptance-tests), M23)
    #[test]
    fn a_compaction_under_a_page_is_read_where_it_moved() {
        let _turn = turn();
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let (conf, map) = archive_map(&temp_dir).await;
            let table = TableId::of(TABLE).0;
            let records = [(Some(10), 4096), (Some(20), 4096), (Some(30), 4096)];
            let written = write(&map, &records, 1).await;
            // a scan's page planned, its run held once drawn and paced
            faults::hold_page_reads(table, true);
            let before = faults::page_reads_waiting();
            let reading = glommio::spawn_local({
                let map = map.clone();
                async move { page_of(&map, 0, 1000, true).finish(Steps::yielding(), None, None).await }
            });
            held_reads(before + 1).await;
            // an archive pass under it: the records copied into a new active archive, the map
            // repointed over them, and the old archive taken out and unlinked
            *map.active.borrow_mut() = Uuid::new_v4();
            write(&map, &records, 1).await;
            map.remove_archive(&written[0].archive).await;
            std::fs::remove_file(conf.get_archive_path(TABLE).join(written[0].archive.to_string())).unwrap();
            faults::hold_page_reads(table, false);
            // the run found its archive gone, and each record was read where it moved
            let page = reading.await.expect("a page whose records a pass moved");
            assert_eq!(keys_of(&page, |_| 1), vec![10, 20, 30]);
            assert_eq!(page.reprobed, 3);
            // a merge under the next page rewrites a partition into a new archive and repoints it,
            // its old record left where it was: the page reads the record its view names, the
            // partition as it stood at its boundary
            faults::hold_page_reads(table, true);
            let before = faults::page_reads_waiting();
            let reading = glommio::spawn_local({
                let map = map.clone();
                async move { page_of(&map, 0, 1000, true).finish(Steps::yielding(), None, None).await }
            });
            held_reads(before + 1).await;
            *map.active.borrow_mut() = Uuid::new_v4();
            write(&map, &[(Some(20), 4096)], 2).await;
            faults::hold_page_reads(table, false);
            let page = reading.await.expect("a page beside a merge");
            assert_eq!(keys_of(&page, |_| 1), vec![10, 20, 30]);
            assert_eq!(page.reprobed, 0);
            // and a page after the merge reads the new version
            let after = page_of(&map, 0, 1000, true)
                .finish(Steps::yielding(), None, None)
                .await
                .expect("a page after a merge");
            assert_eq!(keys_of(&after, |key| if key == 20 { 2 } else { 1 }), vec![10, 20, 30]);
            map.close_all().await.unwrap();
        });
    }
}
