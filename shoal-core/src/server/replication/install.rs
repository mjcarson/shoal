//! Assembling a received snapshot on the shard that hosts its group, and the crash points
//!
//! A snapshot's bytes arrive on the bulk lane in chunks, on whichever shard the kernel handed
//! the connection to, and are relayed to the shard that hosts the group as
//! [`ServerMsg::SnapshotBytes`](crate::server::messages::ServerMsg). That shard keeps at most
//! one partial per group ([`Partial`]) in `wal/Shard-N/install/<group>.part`, and a task per
//! partial writes chunks as they come. The decisions - which chunk is next, which is a repeat,
//! which is out of order - are the [`Assembler`]'s, which does no I/O and is tested alone
//! ([F43](../../../../docs/src/features/node-recovery.md)).
//!
//! One TCP stream delivers in order, so a chunk is either exactly the next byte, wholly below
//! it (a repeat, after a resume), or past it (something was lost); a chunk past the prefix is
//! dropped and counted, and the resume offset - the contiguous prefix - is what recovers it.
//! The checksum is folded as the bytes are written, so a complete partial is verified without
//! being read back.
//!
//! The crash points are the fixture's: a process-global armed by `ShoalPool::crash_at`, checked
//! at each step of an install by [`crash_point::hit`], which exits the process there. Off
//! unless armed, behind no feature, and costing one relaxed load at each point.

use std::collections::VecDeque;

use super::snapshot::{FileHasher, SnapshotManifest};
use crate::server::wal::Vote;
use crate::shared::identity::NodeId;

/// What the assembler says about a chunk
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offer {
    /// The next bytes of the prefix: write them
    Write,
    /// Bytes already held: ignore them
    Duplicate,
    /// Bytes past the prefix, or a chunk that overlaps it: drop them
    Dropped,
}

/// The pure half of a partial: where the prefix ends, what it hashes to, what was seen
#[derive(Debug, Clone)]
pub struct Assembler {
    /// How many bytes the whole stream carries
    pub total: u64,
    /// The first byte not yet held: the contiguous prefix's length, and the resume offset
    pub next: u64,
    /// The checksum over the prefix
    hasher: FileHasher,
    /// Chunks accepted
    pub chunks: u64,
    /// Chunks that were repeats
    pub duplicates: u64,
    /// Chunks that were dropped
    pub dropped: u64,
}

impl Assembler {
    /// An assembler for a stream of some length, holding nothing yet
    ///
    /// # Arguments
    ///
    /// * `total` - How many bytes the stream carries
    #[must_use]
    pub fn new(total: u64) -> Self {
        Assembler {
            total,
            next: 0,
            hasher: FileHasher::default(),
            chunks: 0,
            duplicates: 0,
            dropped: 0,
        }
    }

    /// Judge a chunk by where it lands
    ///
    /// # Arguments
    ///
    /// * `offset` - Where the chunk starts
    /// * `len` - How long it is
    #[must_use]
    pub fn offer(&mut self, offset: u64, len: u64) -> Offer {
        // exactly the next bytes, and not past the end
        if offset == self.next && offset + len <= self.total {
            return Offer::Write;
        }
        // wholly below the prefix: seen before
        if offset + len <= self.next {
            self.duplicates += 1;
            return Offer::Duplicate;
        }
        // past the prefix, or overlapping it, or past the end
        self.dropped += 1;
        Offer::Dropped
    }

    /// Note that the next bytes were written
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes, which start at `next`
    pub fn advance(&mut self, bytes: &[u8]) {
        self.hasher.write(bytes);
        self.next += bytes.len() as u64;
        self.chunks += 1;
    }

    /// Whether every byte is held
    #[must_use]
    pub fn complete(&self) -> bool {
        self.next == self.total
    }

    /// The checksum over the prefix held
    #[must_use]
    pub fn checksum(&self) -> u64 {
        self.hasher.finish()
    }
}

/// One snapshot being assembled for one group
#[derive(Debug)]
pub struct Partial {
    /// The peer sending it
    pub from: NodeId,
    /// The stream, so a chunk of another stream is not written into this one
    pub stream: [u8; 16],
    /// The sender's vote, which the install is handed to openraft under
    ///
    /// The sender's and never this replica's own: openraft judges it as it judges an append,
    /// and a replica that has been electing itself while cut off holds an uncommitted vote of
    /// its own that no install may be run under.
    pub vote: Vote,
    /// What is coming
    pub manifest: SnapshotManifest,
    /// Where the prefix ends and what it hashes to
    pub assembler: Assembler,
    /// Chunks waiting for the writer task, in arrival order
    pub queue: VecDeque<(u64, Vec<u8>)>,
    /// How many bytes the queue holds
    pub queued_bytes: usize,
    /// Whether a writer task is draining the queue
    pub writing: bool,
    /// Whether the writer opened the file, which a new stream truncates
    pub opened: bool,
    /// Whether the file is synced through the prefix
    pub synced: bool,
    /// Why writing failed, if it did
    pub failed: Option<String>,
    /// Whether the partial was resumed from a held prefix
    pub resumed: bool,
    /// Whether the lane feeding it ended since the last chunk, so its end answers a resume at once
    pub lane_lost: bool,
}

impl Partial {
    /// A partial for a stream that is about to start
    ///
    /// # Arguments
    ///
    /// * `from` - The peer sending it
    /// * `stream` - The stream
    /// * `vote` - The sender's vote
    /// * `manifest` - What is coming
    #[must_use]
    pub fn new(from: NodeId, stream: [u8; 16], vote: Vote, manifest: SnapshotManifest) -> Self {
        let total = manifest.total;
        Partial {
            from,
            stream,
            vote,
            manifest,
            assembler: Assembler::new(total),
            queue: VecDeque::new(),
            queued_bytes: 0,
            writing: false,
            opened: false,
            synced: false,
            failed: None,
            resumed: false,
            lane_lost: false,
        }
    }
}

/// Where an install may be made to die, for the crash matrix
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CrashPoint {
    /// Nowhere: the process runs
    None = 0,
    /// The stream is verified and nothing is on disk but the partial
    BeforePending = 1,
    /// The pending marker is durable and openraft has not been told
    PendingWritten = 2,
    /// The compactor wrote the first record into the archive
    MidInstall = 3,
    /// The archive map is repointed and synced, and the loop has not heard
    MapSaved = 4,
    /// The partitions are evicted and the state moved, before the checkpoint is written
    BeforeCheckpoint = 5,
    /// The checkpoint is durable and the marker is still there
    AfterCheckpoint = 6,
    /// The marker and the file are gone
    AfterCleanup = 7,
}

impl CrashPoint {
    /// Every point, in install order
    pub const ALL: [CrashPoint; 7] = [
        CrashPoint::BeforePending,
        CrashPoint::PendingWritten,
        CrashPoint::MidInstall,
        CrashPoint::MapSaved,
        CrashPoint::BeforeCheckpoint,
        CrashPoint::AfterCheckpoint,
        CrashPoint::AfterCleanup,
    ];

    /// The name the fixture arms a point by
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            CrashPoint::None => "none",
            CrashPoint::BeforePending => "before_pending",
            CrashPoint::PendingWritten => "pending_written",
            CrashPoint::MidInstall => "mid_install",
            CrashPoint::MapSaved => "map_saved",
            CrashPoint::BeforeCheckpoint => "before_checkpoint",
            CrashPoint::AfterCheckpoint => "after_checkpoint",
            CrashPoint::AfterCleanup => "after_cleanup",
        }
    }

    /// The point a name arms, if it is one
    ///
    /// # Arguments
    ///
    /// * `name` - The name
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        CrashPoint::ALL.iter().copied().find(|point| point.name() == name)
    }
}

/// The crash point armed for this process, and where an install checks it
pub mod crash_point {
    use super::CrashPoint;
    use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

    /// The armed point, as its discriminant; zero for none
    static ARMED: AtomicU8 = AtomicU8::new(0);

    /// How long an install pauses after its first record, in milliseconds; zero for no pause
    ///
    /// The fixture's way of holding a group `installing` long enough to read through it.
    static HOLD_MS: AtomicU64 = AtomicU64::new(0);

    /// Make every install on this process pause after its first record
    ///
    /// # Arguments
    ///
    /// * `ms` - How long, or zero for no pause
    pub fn hold(ms: u64) {
        HOLD_MS.store(ms, Ordering::Relaxed);
    }

    /// The pause an install takes after its first record, if one is armed
    pub async fn held() {
        let ms = HOLD_MS.load(Ordering::Relaxed);
        if ms > 0 {
            glommio::timer::sleep(std::time::Duration::from_millis(ms)).await;
        }
    }

    /// Arm a point, or disarm every point with `None`
    ///
    /// # Arguments
    ///
    /// * `point` - The point
    pub fn arm(point: CrashPoint) {
        ARMED.store(point as u8, Ordering::Relaxed);
    }

    /// The armed point
    #[must_use]
    pub fn armed() -> CrashPoint {
        let raw = ARMED.load(Ordering::Relaxed);
        CrashPoint::ALL.iter().copied().find(|point| *point as u8 == raw).unwrap_or(CrashPoint::None)
    }

    /// Die here if this point is armed
    ///
    /// Exits with 137, the way a kill does, so the fixture cannot tell a crash here from one
    /// it injected; nothing is flushed on the way out, which is the point.
    ///
    /// # Arguments
    ///
    /// * `point` - The point reached
    pub fn hit(point: CrashPoint) {
        if ARMED.load(Ordering::Relaxed) == point as u8 && point != CrashPoint::None {
            tracing::error!(msg = "dying at an armed crash point", point = point.name());
            std::process::exit(137);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Assembler, CrashPoint, Offer};
    use crate::server::replication::snapshot::FileHasher;

    /// Repeated and reordered chunks are counted and never written twice, the resume offset
    /// is the contiguous prefix, and the checksum over the prefix matches the file's
    #[test]
    fn snapshot_duplicates_and_resume_are_safe() {
        let bytes: Vec<u8> = (0..200_000u32).map(|at| (at % 251) as u8).collect();
        let mut whole = FileHasher::default();
        whole.write(&bytes);
        let expected = whole.finish();
        let chunk = 30_000u64;
        let mut assembler = Assembler::new(bytes.len() as u64);
        let offsets: Vec<u64> = (0..)
            .map(|at| at * chunk)
            .take_while(|offset| *offset < bytes.len() as u64)
            .collect();
        let slice = |offset: u64| &bytes[offset as usize..(offset + chunk).min(bytes.len() as u64) as usize];
        // the first two chunks, in order
        for offset in &offsets[..2] {
            assert_eq!(assembler.offer(*offset, slice(*offset).len() as u64), Offer::Write);
            assembler.advance(slice(*offset));
        }
        assert_eq!(assembler.next, 2 * chunk);
        // the first again: a repeat, held once
        assert_eq!(assembler.offer(0, chunk), Offer::Duplicate);
        assert_eq!(assembler.duplicates, 1);
        assert_eq!(assembler.next, 2 * chunk);
        // the fourth before the third: dropped, and the prefix is still the resume offset
        assert_eq!(assembler.offer(3 * chunk, chunk), Offer::Dropped);
        assert_eq!(assembler.dropped, 1);
        assert_eq!(assembler.next, 2 * chunk);
        // a chunk overlapping the prefix is dropped too, never partly written
        assert_eq!(assembler.offer(chunk + 1, chunk), Offer::Dropped);
        // the rest in order, as a resume from the prefix would send them
        for offset in &offsets[2..] {
            assert_eq!(assembler.offer(*offset, slice(*offset).len() as u64), Offer::Write);
            assembler.advance(slice(*offset));
        }
        assert!(assembler.complete());
        assert_eq!(assembler.chunks, offsets.len() as u64);
        // a chunk past the end is dropped
        assert_eq!(assembler.offer(bytes.len() as u64, 1), Offer::Dropped);
        assert_eq!(assembler.checksum(), expected, "the prefix's checksum is the file's");
        // the crash points round trip their names, in order
        for (at, point) in CrashPoint::ALL.iter().enumerate() {
            assert_eq!(CrashPoint::from_name(point.name()), Some(*point));
            assert_eq!(*point as u8, at as u8 + 1);
        }
        assert_eq!(CrashPoint::from_name("nowhere"), None);
        assert_eq!(super::crash_point::armed(), CrashPoint::None);
    }
}
