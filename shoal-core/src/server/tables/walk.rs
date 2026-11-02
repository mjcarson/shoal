//! A walk of one tablet's rows, a page at a time
//!
//! A driver on the shard that leads a tablet's group asks for the tablet's rows in pages: a range
//! of the tablet's keys, a cursor, and bounds on how much one page holds
//! ([F87](../../../../docs/src/features/tablet-walk.md)). A page is taken in two halves. The table
//! captures its resident partitions in the range on the shard's loop and takes a view of its
//! archive map in the same step, so both describe one moment, the page's boundary; a task off
//! the loop then merges the two in key order, reading what is archived, and hands the page out.
//! A driver's page reads each archived partition as its merge reaches it; a scan's reads them
//! ahead of its merge, several in flight and in runs in disk order (`reads`, M23).
//!
//! # Invariants
//!
//! **A page is the copy's state at its boundary, or newer.** The resident capture and the
//! archive map's view are taken with nothing awaited between them. A partition that is not
//! resident has its current state in its archive, since every partition written since its last
//! compaction is resident and is evicted only once that generation is flushed. An archived
//! record is never rewritten in place, only repointed by the compactor, so a record the view
//! names that is gone has moved: it is looked up again in the map as it is now, which is a
//! state at or after the boundary, and a partition the map no longer holds was deleted after it
//! and is left out.
//!
//! **A resident copy wins, and an overlay is merged.** A resident partition is the newer copy of
//! its key whatever its archive holds, a tombstone included. A sorted partition written before
//! it was read holds only its new rows (`check_disk`), and is merged over its archived rows
//! here as a read would merge it, never taken as the whole partition.
//!
//! **Keys come out in ascending order, each once, and a page holds at least one partition.** The
//! cursor is the last key a page handed out, so the next page starts past it.

use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::util::AlignedVec;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::Archive;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

mod reads;

pub use reads::{READS_IN_FLIGHT, RUN_GAP, RUN_SPAN};
use reads::{PageReads, ReadState};

use crate::server::budget::{Budget, Permit, Shed, DRAW_WAIT};
use crate::server::errors::ShoalError;
use crate::server::scans::{PagePace, TablePace};
use crate::server::step_clock::CaptureClock;
use crate::server::steps::{Steps, STEP};
use crate::server::tables::partitions::{ArchivedMaybeRow, SortedPartition, UnsortedPartition};
use crate::server::tables::storage::fs::index::Scan;
use crate::server::tables::storage::fs::{ArchiveMap, ChainEntry};
use crate::server::tables::PartitionBytes;
use crate::server::ServerError;
use crate::shared::objects::ObjectQueries;
use crate::shared::protocol::stats::{CapturePart, CaptureParts};
use crate::shared::stripes::StripeRow;
use crate::shared::traits::{RkyvSupport, ShoalSortedTable, ShoalUnsortedTable};

/// How long a scan's page waits for its device before it ends short, once it holds a partition:
/// a page its device's share holds back hands out what it has well inside its answer's deadline,
/// its next page going on from there ([F128](../../../../docs/src/features/device-time-pace.md))
pub const PACED_BOUND: Duration = Duration::from_secs(1);

/// What one page of a walk asks a table for
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkBounds {
    /// The tablet whose rows are walked
    pub tablet: u16,
    /// The first key the page may hand out
    pub start: u64,
    /// The last key of the range the walk covers
    pub high: u64,
    /// The most partitions the page holds
    pub limit: usize,
    /// The bytes past which the page holds no more partitions
    pub max_bytes: u64,
    /// Whether the page plans its resident partitions on the loop rather than capturing them,
    /// each encoded later in a step of the loop's second inbox: a scan's page, since M23
    /// ([W5](../../../../docs/src/future-work/scans/tablet-reader.md#2-plan-on-the-loop-serializing-nothing))
    pub plan: bool,
    /// An encode step's ask of one planned partition, a piece of it at a time; none for a page
    /// ([W5](../../../../docs/src/future-work/scans/tablet-reader.md#4-encode-resident-partitions-in-steps-on-the-loop), M23)
    pub piece: Option<PieceAsk>,
}

/// What an encode step asks a planned partition for: its rows past the last piece's, a step's
/// bytes of them at most, and the rows its shard's step time holds at most
#[derive(Debug, Clone)]
pub struct PieceAsk {
    /// The archived sort key of the last piece's last row, none for the partition's first piece
    pub after: Option<AlignedVec>,
    /// The most bytes of the partition the step encodes
    pub bytes: u64,
    /// The most rows of it the step encodes, by what a row of its table has shown it costs the
    /// shard's loop ([item 347](../../../../docs/src/appendix/resolved/encode-steps-by-rows.md))
    pub rows: usize,
    /// When the step's copy of its rows has held its loop its share of the step, past which a
    /// piece takes no more, if the step is timed
    /// ([item 350](../../../../docs/src/appendix/resolved/encode-copy-deadline.md))
    pub until: Option<std::time::Instant>,
}

impl PartialEq for PieceAsk {
    /// The same cursor, the same bounds and the same deadline, a cursor compared by its archived bytes
    fn eq(&self, other: &Self) -> bool {
        // an archived key is its bytes
        self.bytes == other.bytes
            && self.rows == other.rows
            && self.until == other.until
            && self.after.as_deref() == other.after.as_deref()
    }
}

impl Eq for PieceAsk {}

/// One resident partition, as the page captured it on the loop
#[derive(Debug)]
pub enum Captured {
    /// A whole partition with live rows, which shadows whatever its archive holds
    Whole {
        /// Its live rows
        rows: u32,
        /// Its archived bytes
        bytes: AlignedVec,
        /// Whether its bytes hold tombstones a fold has to take out
        strip: bool,
    },
    /// The new rows of a sorted partition written before it was read, merged over its archive
    Overlay {
        /// Its archived bytes, tombstones included, since they shadow archived rows
        bytes: AlignedVec,
        /// Whether any of its own rows is live, which makes it certain to be handed out
        live: bool,
        /// How many rows it holds, tombstones included, as its table counts them
        /// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
        rows: u32,
    },
    /// A partition with no live row, which hides its archived copy
    Shadow,
    /// A resident partition a planned page names and has not copied: encoded once the page reaches
    /// it, in a step of the loop's second inbox over a fresh lookup of its key (M23)
    Planned {
        /// Whether it was new rows over an archive not read yet when planned
        overlay: bool,
        /// Whether any of its own rows was live when planned
        live: bool,
        /// Its size as the table counts it, which the page's bounds are planned by
        estimate: u64,
    },
    /// A piece of a resident sorted partition an encode step took: its rows past the last piece's,
    /// a step's bytes and rows of them at most, as a partition of them alone (M23)
    Piece {
        /// Its rows, tombstones included, since an overlay's shadow archived rows
        bytes: AlignedVec,
        /// Whether its rows are new rows over an archive not read yet
        overlay: bool,
        /// The archived sort key of its last row, none once it reached the partition's last row
        last: Option<AlignedVec>,
        /// How many rows it holds, tombstones included, which its shard learns a row's cost from
        /// ([item 347](../../../../docs/src/appendix/resolved/encode-steps-by-rows.md))
        rows: u32,
    },
    /// A resident partition held as its archive's validated bytes, shared by an encode step rather
    /// than copied on the loop, and folded off it as an archived record is (M23)
    Shared {
        /// Its bytes, the table's own, which nothing changes
        bytes: PartitionBytes,
    },
}

impl Captured {
    /// Whether this capture is certain to be handed out, and so counts against the page's bounds
    #[must_use]
    pub fn is_certain(&self) -> bool {
        // a whole partition always has a live row; an overlay is certain once one of its own is
        match self {
            Captured::Whole { .. } => true,
            Captured::Overlay { live, .. } => *live,
            Captured::Shadow => false,
            // a planned overlay is certain as its overlay would be, a whole one always
            Captured::Planned { overlay, live, .. } => !*overlay || *live,
            // a piece's and a shared partition's are an encode step's, which no page's bounds count
            Captured::Piece { overlay, .. } => !*overlay,
            Captured::Shared { .. } => true,
        }
    }

    /// The bytes this capture counts against its page's bounds: what it holds, or what a planned
    /// partition is estimated to hold
    #[must_use]
    pub fn bound_bytes(&self) -> u64 {
        // a planned partition by the table's own count of it
        match self {
            Captured::Planned { estimate, .. } => *estimate,
            other => other.bytes(),
        }
    }

    /// The bytes this capture holds
    #[must_use]
    pub fn bytes(&self) -> u64 {
        // what was copied off the loop
        match self {
            Captured::Whole { bytes, .. }
            | Captured::Overlay { bytes, .. }
            | Captured::Piece { bytes, .. } => bytes.len() as u64,
            // nothing of a shadow, a planned partition or a shared one was copied off the loop
            Captured::Shadow | Captured::Planned { .. } | Captured::Shared { .. } => 0,
        }
    }

    /// How many rows an encode step encoded to take this capture, where it says: a piece's, or a
    /// whole partition's live rows; none for what it copied no rows of or does not count
    /// ([item 347](../../../../docs/src/appendix/resolved/encode-steps-by-rows.md))
    #[must_use]
    pub fn encoded_rows(&self) -> Option<u32> {
        // a piece counts every row it took, a whole partition its live ones
        match self {
            Captured::Piece { rows, .. } | Captured::Whole { rows, .. } => Some(*rows),
            Captured::Overlay { .. } | Captured::Shadow | Captured::Planned { .. } | Captured::Shared { .. } => None,
        }
    }

    /// How many rows this capture copied off the loop, as a node's figures count a step's: a
    /// piece's and an overlay's every row, a whole partition's live ones, and none of a shadow, a
    /// planned partition or one shared as its archive's bytes
    /// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
    #[must_use]
    pub fn copied_rows(&self) -> u64 {
        // what was copied, as each kind counts its rows
        match self {
            Captured::Piece { rows, .. } | Captured::Whole { rows, .. } | Captured::Overlay { rows, .. } => {
                u64::from(*rows)
            }
            Captured::Shadow | Captured::Planned { .. } | Captured::Shared { .. } => 0,
        }
    }
}

/// Make the partition a walk hands out from an archived record, a resident copy, or both
///
/// Returns the live rows and the archived bytes of the partition, or nothing if no row of it is
/// live. A table names one of these for its partitions:
///
/// * `key` - The partition key, for an error
/// * `archived` - The record its archive holds, if it has one
/// * `resident` - The resident overlay, or a whole copy whose tombstones are to be taken out
pub type WalkFold =
    fn(u64, Option<&[u8]>, Option<&[u8]>) -> Result<Option<(u32, AlignedVec)>, ServerError>;

/// One part of a planned partition its encode steps took in pieces, in its order: the rows a step
/// found within the part's bounds (M23)
#[derive(Debug)]
pub struct WalkPart {
    /// The rows the step took, a partition of them alone or the whole partition, of which only
    /// those within the part's bounds are its; none once the partition was resident no longer,
    /// its archive holding the rest
    pub rows: Option<AlignedVec>,
    /// Whether its rows are new rows over the partition's archive, which lies under them
    pub overlay: bool,
    /// The archived sort key the part starts after, none for the partition's first part
    pub after: Option<AlignedVec>,
    /// The archived sort key the part ends at, none for the partition's last part
    pub last: Option<AlignedVec>,
}

/// Fold the parts a planned partition's encode steps took over its archive, a step at a time, its
/// live rows handed out in sort order as pieces of at most a step's bytes each (M23,
/// [F127](../../../../docs/src/features/folds-in-steps.md))
///
/// Returns the live rows and every piece, or nothing if no row of it is live. A table names one of
/// these beside its fold:
///
/// * `key` - The partition key, for an error
/// * `parts` - Its parts, in their order
/// * `archived` - Its archive as the map held it after the last part's step, where an overlay's
///   rows lie over it or the rest of the partition lies in it
/// * `steps` - The page's steps, which the fold yields at between its pieces
pub type WalkPieces = for<'a> fn(u64, Vec<WalkPart>, Option<PartitionBytes>, &'a mut Steps) -> WalkFolding<'a>;

/// A fold of a planned partition's parts under way: its live rows and every piece once it is done,
/// or none if no row lived (M23)
pub type WalkFolding<'a> =
    futures::future::LocalBoxFuture<'a, Result<Option<(u32, Vec<AlignedVec>)>, ServerError>>;

/// One partition a page handed out
#[derive(Debug)]
pub struct WalkedPartition {
    /// Its key
    pub key: u64,
    /// How many live rows it holds, every piece's
    pub rows: u32,
    /// Its archived bytes, as a snapshot's record holds them, with no tombstone: the whole
    /// partition, or the first piece of one handed out in pieces
    pub bytes: AlignedVec,
    /// The rest of a partition a scan's page folded a step at a time, each piece a partition of a
    /// stretch of its live rows in sort order, at most a step's bytes of them; empty for every other
    /// partition (M23, [F127](../../../../docs/src/features/folds-in-steps.md))
    pub more: Vec<AlignedVec>,
}

impl WalkedPartition {
    /// Every piece of the partition's bytes, in sort order: its bytes, then any more
    pub fn pieces(&self) -> impl Iterator<Item = &AlignedVec> {
        // the first piece, which a partition not in pieces is whole in
        std::iter::once(&self.bytes).chain(self.more.iter())
    }
}

/// What a page's reads came to
#[derive(Debug, Default)]
pub struct WalkRows {
    /// The partitions, in ascending key order
    pub partitions: Vec<WalkedPartition>,
    /// Their bytes
    pub bytes: u64,
    /// Where the next page starts, or none once the range is walked
    pub next: Option<u64>,
    /// How many partitions came from memory
    pub resident: u32,
    /// How many came from an archive, merged with an overlay or not
    pub archived: u32,
    /// How many archived records had moved and were looked up again
    pub reprobed: u32,
    /// How many archive reads the page issued, a chain's records a read each, which a node's
    /// figures count ([M23](../../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load))
    pub reads: u64,
    /// How many of its resident partitions it planned and had encoded in steps of the loop (M23)
    pub planned: u32,
    /// How many encode steps it asked of its loop, a planned partition's pieces a step each (M23)
    pub encode_steps: u32,
    /// The most bytes one of its encode steps copied off the loop (M23)
    pub largest_step: u64,
    /// What a scan page drew from its node's scan budget past what it was admitted with, given
    /// back once its bytes are dropped (F118)
    pub drawn: Vec<Permit>,
    /// How long a scan page's reads waited for their device's idle time or its trickle (F119)
    pub paced: std::time::Duration,
}

/// What a scan page has drawn from its node's scan budget, drawn again as its reads pass it
/// ([F118](../../../../docs/src/features/scan-budget.md))
///
/// A page is admitted with a draw of its byte bound and its capture's excess; every archived
/// record it reads past what it drew is drawn before the record is read, so no buffer of a scan's
/// page is ever allocated past its budget.
#[derive(Debug)]
pub struct PageDraws {
    /// The node's scan budget
    budget: Arc<Budget>,
    /// The bytes drawn for the page so far, admission's included
    drawn: u64,
    /// The bytes the page holds so far, captured and read
    held: u64,
    /// The permits drawn past admission's, handed back with the page
    permits: Vec<Permit>,
}

impl PageDraws {
    /// A page's draws, as its admission and its capture left them
    ///
    /// # Arguments
    ///
    /// * `budget` - The node's scan budget
    /// * `drawn` - The bytes drawn for the page already
    /// * `held` - The bytes its capture holds
    #[must_use]
    pub fn new(budget: Arc<Budget>, drawn: u64, held: u64) -> Self {
        PageDraws {
            budget,
            drawn,
            held,
            permits: Vec::new(),
        }
    }

    /// Draw what the page needs to hold `more` bytes besides what it holds, waiting a while for it
    ///
    /// # Arguments
    ///
    /// * `more` - The bytes about to be read
    ///
    /// # Errors
    ///
    /// What the budget shed, once its wait ran out or at once for a page past the whole budget.
    async fn cover(&mut self, more: u64) -> Result<(), crate::server::budget::Shed> {
        let wanted = self.held.saturating_add(more);
        if wanted > self.drawn {
            let permit = self.budget.draw(wanted - self.drawn, DRAW_WAIT).await?;
            self.drawn += permit.bytes();
            self.permits.push(permit);
        }
        self.held = wanted;
        Ok(())
    }

    /// Note that the page holds `bytes` it drew for no longer, so what it holds next fits in their
    /// room first; the permits stay with the page, which gives them back with its bytes (M23)
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes let go
    fn release(&mut self, bytes: u64) {
        // what it drew it keeps, and only its count of what it holds moves
        self.held = self.held.saturating_sub(bytes);
    }
}

/// The archived half of a page: the map's view of the range, and the map to read through
pub struct ArchivedWalk {
    /// The map, and its index as it stood when the page began; none for an engine that stores nothing
    inner: Option<(Arc<ArchiveMap>, Scan)>,
}

impl std::fmt::Debug for ArchivedWalk {
    /// Whether the walk reads an archive, since a scan holds nothing worth printing
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivedWalk")
            .field("archived", &self.inner.is_some())
            .finish()
    }
}

/// The bytes an archived partition's records hold, read before they are
///
/// # Arguments
///
/// * `chain` - Where the view says its records are
fn chain_bytes(chain: &ChainEntry) -> u64 {
    chain.base.size as u64 + chain.fragments.iter().map(|fragment| fragment.size as u64).sum::<u64>()
}

impl ArchivedWalk {
    /// The archived half of a page of an engine that stores nothing
    #[must_use]
    pub fn empty() -> Self {
        ArchivedWalk { inner: None }
    }

    /// The archived half of a page, from a map and its view of the page's range
    ///
    /// # Arguments
    ///
    /// * `map` - The archive map
    /// * `scan` - Its index over the page's range, as it stood when the page began
    #[must_use]
    pub fn new(map: Arc<ArchiveMap>, scan: Scan) -> Self {
        ArchivedWalk {
            inner: Some((map, scan)),
        }
    }

    /// The archive map's delta entries the view copied and the runs it took a cursor over, none for
    /// an engine that stores nothing, which a capture counts its view by
    /// ([item 353](../../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts))
    #[must_use]
    pub fn taken(&self) -> (usize, usize) {
        // a view of no map took nothing
        match &self.inner {
            Some((_, scan)) => (scan.delta_entries(), scan.runs()),
            None => (0, 0),
        }
    }

    /// How long the view's scan took, by the wall, to copy the delta's entries and to make its
    /// cursors over the runs, in nanoseconds, none for an engine that stores nothing (item 353)
    #[must_use]
    pub fn took_ns(&self) -> (u64, u64) {
        // a view of no map took no time of its own
        match &self.inner {
            Some((_, scan)) => scan.took_ns(),
            None => (0, 0),
        }
    }

    /// The next archived partition of the range and where its records are
    async fn next(&mut self) -> Result<Option<(u64, ChainEntry)>, ServerError> {
        // an engine that stores nothing has nothing archived
        match &mut self.inner {
            Some((_, scan)) => scan.next().await,
            None => Ok(None),
        }
    }

    /// Read an archived partition, looking it up again if its archive moved
    ///
    /// Returns the partition's bytes, or nothing if the map no longer holds it, and whether it
    /// was looked up again.
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `chain` - Where the view says its records are
    async fn read(
        &self,
        key: u64,
        chain: &ChainEntry,
    ) -> Result<(Option<PartitionBytes>, bool), ServerError> {
        // nothing is archived without a map
        let Some((map, _)) = &self.inner else {
            return Ok((None, false));
        };
        // first where the view says, then where the map says now: the one rule every read of a
        // partition by its key keeps, a partition deleted since the page began left out
        // ([Resolved #343](../../../../docs/src/appendix/resolved/moved-partition-read-again.md))
        map.read_current_from(key, chain.clone()).await
    }
}

/// One page of a walk, captured on the loop and read off it
#[derive(Debug)]
pub struct PendingWalk {
    /// What the page was asked for
    bounds: WalkBounds,
    /// The last key the page reads, which is the range's end unless the resident capture filled it
    end: u64,
    /// The resident partitions of `[start, end]`, in key order
    resident: VecDeque<(u64, Captured)>,
    /// The archived half of `[start, end]`
    archived: ArchivedWalk,
    /// The archived partitions looked ahead to, and a scan page's reads of them (M23)
    reads: PageReads,
    /// How the table makes a partition from what was captured and read
    fold: WalkFold,
    /// How the table makes a partition from the pieces its encode steps took (M23)
    pieces: WalkPieces,
    /// A planned page's way to its shard's loop, which encodes its planned partitions (M23)
    encoder: Option<Encoder>,
    /// A scan page's device, whose scan reads in flight each read the page makes alone holds a
    /// place among ([F128](../../../../docs/src/features/device-time-pace.md))
    device: Option<Arc<TablePace>>,
    /// The resident keys of its tablet its capture looked at, which a node's figures count the
    /// capture's step by ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
    looked_up: u64,
    /// Its capture taken apart, each part by the wall, its rest its step's to say
    /// ([item 353](../../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts))
    parts: CaptureParts,
}

/// A planned partition a page asks its shard's loop to encode, in a step of its second inbox
/// ([W5](../../../../docs/src/future-work/scans/tablet-reader.md#4-encode-resident-partitions-in-steps-on-the-loop))
#[derive(Debug)]
pub struct EncodeAsk {
    /// Its table
    pub table: crate::shared::identity::TableId,
    /// Its tablet
    pub tablet: u16,
    /// Its key
    pub key: u64,
    /// The archived sort key of the last piece's last row, none for the partition's first step
    pub after: Option<AlignedVec>,
    /// Where the step's capture goes: the partition as a fresh lookup found it, or none if it is
    /// resident no longer
    pub reply: futures::channel::oneshot::Sender<Option<Captured>>,
    /// Its page's class, which the encoder names: a scan's step waits for its loop's second
    /// inbox, and a driver's is taken in order with the messages beside it (M23)
    pub class: crate::shared::protocol::admin::WalkClass,
}

/// What a planned partition's encode steps came to (M23)
#[derive(Debug)]
enum Encoded {
    /// The partition as one capture took it, needing no fold, and the chain its archive is read
    /// from where the merge reads it
    Captured(Option<Captured>, Option<ChainEntry>),
    /// The partition folded a step at a time: its live rows and its pieces, or none if no row lived
    Made(Option<(u32, Vec<AlignedVec>)>),
}

/// A planned page's way to its shard's loop: a send of one ask, false once the loop is gone
pub struct Encoder(pub Box<dyn Fn(EncodeAsk) -> futures::future::LocalBoxFuture<'static, bool>>);

impl std::fmt::Debug for Encoder {
    /// An encoder is a closure, said by name alone
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Encoder")
    }
}

impl PendingWalk {
    /// Give a planned page its way to its shard's loop, which encodes its planned partitions in
    /// steps of its second inbox
    ///
    /// # Arguments
    ///
    /// * `encoder` - How it asks the loop
    pub fn set_encoder(&mut self, encoder: Encoder) {
        self.encoder = Some(encoder);
    }

    /// The resident capture of a key, if the page captured it, for an encode step's capture of
    /// that key alone
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    pub fn take_resident(&mut self, key: u64) -> Option<Captured> {
        // a range of one key captures that key or nothing
        match self.resident.front() {
            Some((front, _)) if *front == key => self.resident.pop_front().map(|(_, captured)| captured),
            _ => None,
        }
    }

    /// A step of a planned partition's encoding by its shard's loop, as a fresh lookup finds it, or
    /// none if it is resident no longer
    ///
    /// # Arguments
    ///
    /// * `key` - The planned partition's key
    /// * `after` - The archived sort key the last piece ended at, none for its first step
    ///
    /// # Errors
    ///
    /// Fails if the page has no way to its loop, or the loop is gone before it answers.
    async fn encode_planned(&self, key: u64, after: Option<AlignedVec>) -> Result<Option<Captured>, ServerError> {
        // asked of the loop, which takes it as a step of its second inbox; the encoder names the
        // table, which the page does not know
        let encoder = self
            .encoder
            .as_ref()
            .ok_or_else(|| ServerError::GlommioGeneric("a planned page has no way to its loop".into()))?;
        let (reply, answered) = futures::channel::oneshot::channel();
        let ask = EncodeAsk {
            table: crate::shared::identity::TableId(0),
            tablet: self.bounds.tablet,
            key,
            after,
            reply,
            class: crate::shared::protocol::admin::WalkClass::Scan,
        };
        if !(encoder.0)(ask).await {
            return Err(ServerError::GlommioGeneric("the loop is gone before a planned partition's step".into()));
        }
        answered
            .await
            .map_err(|_| ServerError::GlommioGeneric("a planned partition's step was dropped".into()))
    }

    /// A partition's chain as the archive map holds it now, for a planned partition whose archive
    /// is read after its step
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    ///
    /// # Errors
    ///
    /// Fails if the map's pages cannot be read.
    async fn chain_now(&self, key: u64) -> Result<Option<ChainEntry>, ServerError> {
        // nothing is archived without a map
        match &self.archived.inner {
            Some((map, _)) => map.chain_of(key).await,
            None => Ok(None),
        }
    }

    /// Encode a planned partition in steps of its shard's loop, and fold what they took in steps
    /// of its scan queue
    ///
    /// The first step's answer is the partition as a capture would have taken it, unless it is too
    /// large for one step: then each step takes its rows past the last piece's, at most a step's
    /// bytes of them, over a fresh lookup of its key, until a step reaches its last row or finds it
    /// whole, shared, shadowed or resident no longer. An overlay, a partition in pieces and a whole
    /// copy holding tombstones are folded a step at a time over its archive as the map holds it
    /// after the last step, at least as new as every piece's step (W5's first and third facts), and
    /// handed out as pieces of at most a step's bytes. Every step's bytes are drawn from the page's
    /// budget before the step is asked, and what the parts and the archive held is let go once the
    /// fold is done
    /// ([W5](../../../../docs/src/future-work/scans/tablet-reader.md#4-encode-resident-partitions-in-steps-on-the-loop), M23).
    ///
    /// Returns what the steps came to, or what the budget shed.
    ///
    /// # Arguments
    ///
    /// * `key` - The planned partition's key
    /// * `page` - The page, whose counts this keeps
    /// * `steps` - The page's steps, which the fold yields at
    /// * `draws` - A scan page's draws on its node's scan budget
    /// * `pace` - A scan page's pace through its device
    ///
    /// # Errors
    ///
    /// Fails if a step or the archive's read fails, or the parts do not fold.
    async fn encode_in_steps(
        &self,
        key: u64,
        page: &mut WalkRows,
        steps: &mut Steps,
        mut draws: Option<&mut PageDraws>,
        pace: Option<&mut PagePace>,
    ) -> Result<Result<Encoded, Shed>, ServerError> {
        let step = STEP as u64;
        let mut parts: Vec<WalkPart> = Vec::new();
        let mut after: Option<AlignedVec> = None;
        loop {
            // a step's bytes drawn before the loop copies them (F118)
            if let Some(draws) = draws.as_deref_mut() {
                if let Err(shed) = draws.cover(step).await {
                    return Ok(Err(shed));
                }
            }
            page.encode_steps += 1;
            let first = after.is_none();
            let encoded = self.encode_planned(key, after.clone()).await?;
            // a step taken is the page moving, however long its partition takes (F128)
            if let Some(pace) = pace.as_deref() {
                pace.progressed();
            }
            // what the answer holds, a shared partition's copy off the loop included, drawn past
            // the step's bytes or given back short of them
            let copied = encoded.as_ref().map_or(0, Captured::bytes);
            let held = match &encoded {
                Some(Captured::Shared { bytes }) => bytes.len() as u64,
                _ => copied,
            };
            page.largest_step = page.largest_step.max(copied);
            if let Some(draws) = draws.as_deref_mut() {
                if held > step {
                    if let Err(shed) = draws.cover(held - step).await {
                        return Ok(Err(shed));
                    }
                } else {
                    draws.release(step - held);
                }
            }
            // a first step whose answer needs no fold is the partition's capture: whole, shared, a
            // shadow, or resident no longer and read from its archive as the map holds it now
            if first {
                match encoded {
                    Some(Captured::Whole { strip: false, .. }) | Some(Captured::Shared { .. }) | Some(Captured::Shadow) => {
                        return Ok(Ok(Encoded::Captured(encoded, None)));
                    }
                    None => return Ok(Ok(Encoded::Captured(None, self.chain_now(key).await?))),
                    _ => {}
                }
            }
            match encoded {
                // a piece, and the next step from its last row
                Some(Captured::Piece { bytes, overlay, last, .. }) => {
                    let next = last.clone();
                    parts.push(WalkPart {
                        rows: Some(bytes),
                        overlay,
                        after: after.take(),
                        last,
                    });
                    match next {
                        Some(next) => after = Some(next),
                        None => break,
                    }
                }
                // whole or overlaid: its rows past the last piece are the rest, a whole copy's
                // tombstones folded out with them
                Some(Captured::Whole { bytes, .. }) => {
                    parts.push(WalkPart {
                        rows: Some(bytes),
                        overlay: false,
                        after: after.take(),
                        last: None,
                    });
                    break;
                }
                Some(Captured::Overlay { bytes, .. }) => {
                    parts.push(WalkPart {
                        rows: Some(bytes),
                        overlay: true,
                        after: after.take(),
                        last: None,
                    });
                    break;
                }
                // held as its archive's bytes now: copied off the loop, its rows past the last
                // piece the rest
                Some(Captured::Shared { bytes }) => {
                    parts.push(WalkPart {
                        rows: Some(aligned(&bytes)),
                        overlay: false,
                        after: after.take(),
                        last: None,
                    });
                    break;
                }
                // no live row now, so nothing past the last piece
                Some(Captured::Shadow) => break,
                // resident no longer: the rest is in its archive as the map holds it now
                None => {
                    parts.push(WalkPart {
                        rows: None,
                        overlay: true,
                        after: after.take(),
                        last: None,
                    });
                    break;
                }
                Some(Captured::Planned { .. }) => {
                    return Err(ServerError::GlommioGeneric("a planned partition's step planned it again".into()));
                }
            }
        }
        // the archive an overlay's rows lie over, or an evicted partition's rest lies in, read as
        // the map holds it after the last step, drawn and paced as a read alone is
        let mut archived = None;
        if parts.iter().any(|part| part.overlay) {
            if let Some(chain) = self.chain_now(key).await? {
                let bytes = chain_bytes(&chain);
                if let Some(draws) = draws.as_deref_mut() {
                    if let Err(shed) = draws.cover(bytes).await {
                        return Ok(Err(shed));
                    }
                }
                if let Some(pace) = pace {
                    pace.take(bytes).await;
                }
                archived = self.read(key, &chain, page).await?;
            }
        }
        // the pieces the fold makes, drawn beside the parts and the archive before they are made
        let held = parts
            .iter()
            .filter_map(|part| part.rows.as_ref())
            .map(|rows| rows.len() as u64)
            .sum::<u64>()
            + archived.as_ref().map_or(0, |read| read.len() as u64);
        if let Some(draws) = draws.as_deref_mut() {
            if let Err(shed) = draws.cover(held).await {
                return Ok(Err(shed));
            }
        }
        // folded a step of the scan queue at a time, the parts and the archive let go inside it
        let made = (self.pieces)(key, parts, archived, steps).await?;
        if let Some(draws) = draws {
            draws.release(held);
        }
        Ok(Ok(Encoded::Made(made)))
    }

    /// Capture a page from a tablet's resident partitions and its archive's view
    ///
    /// The resident partitions of the page's range are captured in key order, until the page's
    /// bounds are met by partitions certain to be handed out; the archive is then read only up to
    /// the last of them, since nothing past it could be on the page. The archived half has to be
    /// taken by the caller in the same step, with nothing awaited, over `[bounds.start, end]` of
    /// what this returns.
    ///
    /// Returns the captures, the last key the page reads, and how many of the tablet's resident
    /// keys it looked at, which a node's figures count its step by
    /// ([F130](../../../../docs/src/features/loop-steps-by-kind.md)). Its keys and its plan are
    /// each ended on `clock` as they end, so the caller times its view after them
    /// ([item 353](../../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts)).
    ///
    /// # Arguments
    ///
    /// * `bounds` - What the page was asked for
    /// * `keys` - The keys of the tablet's resident partitions, in no order
    /// * `clock` - The capture's clock of its parts
    /// * `capture` - How the table captures one of its resident partitions
    #[allow(clippy::type_complexity)]
    pub fn capture<C>(
        bounds: &WalkBounds,
        keys: impl Iterator<Item = u64>,
        clock: &mut CaptureClock,
        mut capture: C,
    ) -> Result<(VecDeque<(u64, Captured)>, u64, u64), ServerError>
    where
        C: FnMut(u64) -> Result<Option<Captured>, ServerError>,
    {
        // the resident keys of the range, in order, every key of the tablet looked at counted
        let mut looked_up = 0u64;
        let mut keys: Vec<u64> = keys
            .inspect(|_| looked_up += 1)
            .filter(|key| (bounds.start..=bounds.high).contains(key))
            .collect();
        // a planned page selects the smallest keys a page bounded by count could hold rather than
        // sorting every resident key of its tablet, and reads no further than the last of them
        let take = bounds.limit.max(1);
        let selected = bounds.plan && keys.len() > take;
        if selected {
            keys.select_nth_unstable(take - 1);
            keys.truncate(take);
        }
        keys.sort_unstable();
        let last = keys.last().copied();
        // its keys looked at, kept, selected and sorted: the first of its parts (item 353)
        clock.ended(CapturePart::Keys);
        let mut captured = VecDeque::new();
        let mut certain = 0usize;
        let mut bytes = 0u64;
        for key in keys {
            // a key the table no longer holds is not captured
            let Some(entry) = capture(key)? else {
                continue;
            };
            // a partition certain to be handed out counts against the bounds, a planned one by
            // the table's own count of it
            let counts = entry.is_certain();
            if counts {
                certain += 1;
                bytes = bytes.saturating_add(entry.bound_bytes());
            }
            captured.push_back((key, entry));
            // once the bounds are met nothing past this key can be on the page
            if counts && (certain >= take || bytes >= bounds.max_bytes) {
                // its plan ended at the key that met them (item 353)
                clock.ended(CapturePart::Plan);
                return Ok((captured, key, looked_up));
            }
        }
        // its keys planned or captured: the second of its parts (item 353)
        clock.ended(CapturePart::Plan);
        // a page that selected its keys ends at the last of them, the next page going on from it
        let end = match last {
            Some(last) if selected => last,
            _ => bounds.high,
        };
        Ok((captured, end, looked_up))
    }

    /// The page, with how many of its tablet's resident keys its capture looked at
    ///
    /// # Arguments
    ///
    /// * `looked_up` - The resident keys its capture looked at
    #[must_use]
    pub fn with_looked_up(mut self, looked_up: u64) -> Self {
        self.looked_up = looked_up;
        self
    }

    /// How many of its tablet's resident keys the page's capture looked at (F130)
    #[must_use]
    pub fn looked_up(&self) -> u64 {
        self.looked_up
    }

    /// The page, with its capture taken apart
    /// ([item 353](../../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts))
    ///
    /// # Arguments
    ///
    /// * `parts` - Its capture's parts, as its clock of them read them
    #[must_use]
    pub fn with_parts(mut self, parts: CaptureParts) -> Self {
        self.parts = parts;
        self
    }

    /// The page's capture taken apart, each part by the wall: none for a page of a table that does
    /// not time its capture, or an encode step's (item 353)
    #[must_use]
    pub fn parts(&self) -> CaptureParts {
        self.parts
    }

    /// The bytes the page's capture holds, its resident partitions copied off the loop
    #[must_use]
    pub fn held_bytes(&self) -> u64 {
        self.resident.iter().map(|(_, captured)| captured.bytes()).sum()
    }

    /// The rows the page's capture copied off the loop: none for a planned page's (F130)
    #[must_use]
    pub fn held_rows(&self) -> u64 {
        self.resident.iter().map(|(_, captured)| captured.copied_rows()).sum()
    }

    /// The pace of the device the page's archives are on, if its node paces its scans
    /// ([F119](../../../../docs/src/features/scan-pacing.md))
    #[must_use]
    pub fn pace(&self) -> Option<Arc<TablePace>> {
        self.archived.inner.as_ref().and_then(|(map, _)| map.pace().cloned())
    }

    /// A page from what the table captured and the archive's view of the same range
    ///
    /// # Arguments
    ///
    /// * `bounds` - What the page was asked for
    /// * `end` - The last key the page reads
    /// * `resident` - The resident partitions of `[start, end]`, in key order
    /// * `archived` - The archived half of `[start, end]`
    /// * `fold` - How the table makes a partition
    /// * `pieces` - How the table makes a partition from its encode steps' pieces
    #[must_use]
    pub fn new(
        bounds: WalkBounds,
        end: u64,
        resident: VecDeque<(u64, Captured)>,
        archived: ArchivedWalk,
        fold: WalkFold,
        pieces: WalkPieces,
    ) -> Self {
        PendingWalk {
            bounds,
            end,
            resident,
            archived,
            reads: PageReads::default(),
            fold,
            pieces,
            encoder: None,
            device: None,
            looked_up: 0,
            parts: CaptureParts::default(),
        }
    }

    /// Read the page: merge the resident partitions with the archived ones in key order
    ///
    /// Every partition handled is counted into `steps` by the bytes it took, captured and read,
    /// so a scan's page yields every 64 KiB of work and a driver's, whose steps never yield, runs
    /// as it did ([F117](../../../../docs/src/features/scan-class.md)).
    ///
    /// A driver's page reads each archived partition as its merge reaches it. A scan's plans its
    /// archived reads ahead of its merge, a wave at a time, in runs in disk order and several in
    /// flight, and its merge takes each partition's read in key order once it is answered
    /// ([F125](../../../../docs/src/features/page-reads-in-runs.md)).
    ///
    /// A scan page's archived reads are drawn from its node's scan budget before they are read
    /// (F118), a wave's all at once before any is issued, and the bytes each partition read in a
    /// wave hands out drawn again as it is folded, since the wave's buffers are held beside them:
    /// past what the page drew, a draw waits a while for room, and a page that found none ends
    /// short at the last partition it handed out, or is shed if it handed out none. Each read then
    /// waits for its device's idle time under its ceiling, or its trickle (F119).
    ///
    /// # Arguments
    ///
    /// * `steps` - How the page's work is stepped
    /// * `draws` - A scan page's draws on its node's scan budget, none for a driver's page
    /// * `pace` - A scan page's pace through its device, none for a driver's page or a node that
    ///   paces nothing
    ///
    /// # Errors
    ///
    /// Fails if a record cannot be read or does not validate, naming it, or if a scan page found
    /// no room in its budget before its first partition.
    #[instrument(name = "PendingWalk::finish", skip_all, err(Debug))]
    pub async fn finish(
        mut self,
        mut steps: Steps,
        mut draws: Option<PageDraws>,
        mut pace: Option<PagePace>,
    ) -> Result<WalkRows, ServerError> {
        let mut page = WalkRows::default();
        let limit = self.bounds.limit.max(1);
        // a scan page's reads alone hold their places among its device's scan reads (F128)
        self.device = pace.as_ref().map(|pace| pace.device().clone());
        loop {
            // the smaller of the two heads; a key both hold is the resident one's
            let resident_key = self.resident.front().map(|(key, _)| *key);
            let archived_key = self.reads.head(&mut self.archived).await?;
            let key = match (resident_key, archived_key) {
                (Some(resident), Some(archived)) => resident.min(archived),
                (Some(resident), None) => resident,
                (None, Some(archived)) => archived,
                // both are spent: the range up to the page's end is walked
                (None, None) => {
                    let next = (self.end < self.bounds.high).then_some(self.end);
                    return Ok(self.ended(page, next, draws, pace.as_ref()));
                }
            };
            // the resident capture of this key, and its archived partition, whichever exist
            let captured = if resident_key == Some(key) {
                self.resident.pop_front().map(|(_, captured)| captured)
            } else {
                None
            };
            let ahead = if archived_key == Some(key) {
                // a scan page plans its archived reads ahead of its merge, a wave from the first
                // partition no resident copy shadows and none planned, and waits for this one's
                if captured.is_none() && self.bounds.plan {
                    if self.reads.front_unplanned() {
                        // a page its device held back past its bound plans no wave it would wait
                        // for, and ends short at what it holds (F128)
                        if let Some(next) = self.paced_out(&page, pace.as_ref()) {
                            return Ok(self.ended(page, Some(next), draws, pace.as_ref()));
                        }
                        if let Err(shed) = self.plan_reads(&page, draws.as_mut(), pace.as_ref()).await? {
                            return self.short(page, &shed, draws, pace.as_ref());
                        }
                    }
                    self.reads.wait_front().await?;
                }
                self.reads.pop()
            } else {
                None
            };
            let (chain, state) = match ahead {
                Some(ahead) => (Some(ahead.chain), ahead.state),
                None => (None, ReadState::Unplanned),
            };
            // a planned partition is encoded now, in a step of the loop's second inbox over a fresh
            // lookup of its key; an archive it needs is read as the map holds it after that step,
            // never as the plan's view had it, so an overlay is folded only over a state from its
            // own time or later (W5's third fact, M23)
            let mut made = None;
            // a page its device held back past its bound encodes no planned partition, whose steps
            // may have to read an archive, and ends short at what it holds (F128)
            if matches!(captured, Some(Captured::Planned { .. })) {
                if let Some(next) = self.paced_out(&page, pace.as_ref()) {
                    return Ok(self.ended(page, Some(next), draws, pace.as_ref()));
                }
            }
            let (captured, chain, state) = match captured {
                Some(Captured::Planned { .. }) => {
                    page.planned += 1;
                    match self
                        .encode_in_steps(key, &mut page, &mut steps, draws.as_mut(), pace.as_mut())
                        .await?
                    {
                        Ok(Encoded::Captured(encoded, fresh)) => (encoded, fresh, ReadState::Unplanned),
                        // folded a step at a time already, its archive read and its work stepped
                        Ok(Encoded::Made(pieces)) => {
                            page.resident += 1;
                            made = Some(pieces);
                            (None, None, ReadState::Unplanned)
                        }
                        Err(shed) => return self.short(page, &shed, draws, pace.as_ref()),
                    }
                }
                other => (other, chain, state),
            };
            // a scan page draws a record's bytes before it is read, a whole or shared copy's archive
            // unread; a partition its reader read was drawn with its wave, and the bytes it hands out
            // are drawn now, since the wave's buffers are held beside them
            let reads = !matches!(
                captured,
                Some(Captured::Whole { .. }) | Some(Captured::Shadow) | Some(Captured::Shared { .. })
            );
            // nor one it would read alone (F128)
            if reads && chain.is_some() && !state.is_read() {
                if let Some(next) = self.paced_out(&page, pace.as_ref()) {
                    return Ok(self.ended(page, Some(next), draws, pace.as_ref()));
                }
            }
            let shed = match (draws.as_mut(), chain.as_ref(), reads) {
                (Some(draws), Some(chain), true) => draws.cover(chain_bytes(chain)).await.err(),
                _ => None,
            };
            if let Some(shed) = shed {
                return self.short(page, &shed, draws, pace.as_ref());
            }
            // and one the merge reads alone waits for its device's idle time under its ceiling,
            // or its trickle, as the reader's reads did before they were issued (F119)
            if let (Some(pace), Some(chain), true, false) = (pace.as_mut(), chain.as_ref(), reads, state.is_read()) {
                pace.take(chain_bytes(chain)).await;
            }
            // the partition this key comes to, if any of it is live, and the bytes it took: a
            // partition folded in steps is its pieces, the rest a partition of one
            let mut handled = 0usize;
            let walked = match (made, captured) {
                (Some(made), _) => made,
                (None, captured) => match captured {
                // a whole resident copy is the state, and its archive is not read
                Some(Captured::Whole { rows, bytes, strip }) => {
                    page.resident += 1;
                    handled = bytes.len();
                    if strip {
                        (self.fold)(key, None, Some(&bytes))?
                    } else {
                        Some((rows, bytes))
                    }
                }
                // a deleted partition hides its archived copy
                Some(Captured::Shadow) => None,
                // a resident partition its step shared as its archive's bytes, folded off the loop as
                // an archived record is, and its archive not read (M23)
                Some(Captured::Shared { bytes }) => {
                    page.resident += 1;
                    handled = bytes.len();
                    (self.fold)(key, Some(&bytes), None)?
                }
                // an overlay's new rows over whatever its archive holds
                Some(Captured::Overlay { bytes, .. }) => {
                    let read = match &chain {
                        Some(chain) => self.read(key, chain, &mut page).await?,
                        None => None,
                    };
                    page.resident += 1;
                    handled = bytes.len() + read.as_ref().map_or(0, |read| read.len());
                    (self.fold)(key, read.as_deref(), Some(&bytes))?
                }
                // a partition only its archive holds, as the page's reader read it or read now
                None => match &chain {
                    Some(chain) => match self.archived_partition(key, chain, state, &mut page).await? {
                        Some(read) => {
                            page.archived += 1;
                            handled = read.len();
                            (self.fold)(key, Some(&read), None)?
                        }
                        None => None,
                    },
                    None => None,
                },
                // a step always captures, so a planned partition is never planned again, and its
                // pieces are folded before the merge reaches it
                Some(Captured::Planned { .. }) | Some(Captured::Piece { .. }) => {
                    return Err(ServerError::GlommioGeneric("a planned partition's step planned it again".into()));
                }
                }
                .map(|(rows, bytes)| (rows, vec![bytes])),
            };
            // the work counted into a step, which a scan's page yields at (F117)
            steps.take(handled).await;
            // hand it out, every piece of it, and stop once the page's bounds are met
            if let Some((rows, mut pieces)) = walked {
                let size: u64 = pieces.iter().map(|piece| piece.len() as u64).sum();
                page.bytes = page.bytes.saturating_add(size);
                let bytes = if pieces.is_empty() { AlignedVec::new() } else { pieces.remove(0) };
                page.partitions.push(WalkedPartition {
                    key,
                    rows,
                    bytes,
                    more: pieces,
                });
                if let Some(pace) = pace.as_ref() {
                    pace.progressed();
                }
                if page.partitions.len() >= limit || page.bytes >= self.bounds.max_bytes {
                    // nothing left to read means the range is done, not the page
                    let more = !self.resident.is_empty()
                        || self.reads.head(&mut self.archived).await?.is_some()
                        || self.end < self.bounds.high;
                    return Ok(self.ended(page, more.then_some(key), draws, pace.as_ref()));
                }
            }
        }
    }

    /// Where a scan's page that its device has held back past its bound ends, once it holds a
    /// partition and no read a wave paid for is still ahead of its merge: at the last partition it
    /// handed out (F128)
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    /// * `pace` - A scan page's pace through its device, none for a page nothing paces
    fn paced_out(&self, page: &WalkRows, pace: Option<&PagePace>) -> Option<u64> {
        // the merge's waits and its readers' together, and a wave's reads handed out first
        let pace = pace?;
        let last = page.partitions.last()?.key;
        let waited = pace.waited() + self.reads.paced() >= PACED_BOUND;
        (waited && !self.reads.holds_reads()).then_some(last)
    }

    /// The page as it ends: where its next page starts, what it drew, and what its reads came to
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    /// * `next` - Where its next page starts, or none once the range is walked
    /// * `draws` - A scan page's draws on its node's scan budget, which move into the page
    /// * `pace` - A scan page's pace through its device
    fn ended(
        &self,
        mut page: WalkRows,
        next: Option<u64>,
        draws: Option<PageDraws>,
        pace: Option<&PagePace>,
    ) -> WalkRows {
        // the permits drop with the page's bytes (F118), and its readers' reads and waits are its own
        page.next = next;
        page.drawn = draws.map(|draws| draws.permits).unwrap_or_default();
        page.paced = pace.map_or_else(Default::default, PagePace::waited) + self.reads.paced();
        page.reads += self.reads.issued();
        page
    }

    /// The page as a draw on its budget left it: short at the last partition it handed out, or
    /// refused if it handed out none or a draw could never fit the whole budget (F118)
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    /// * `shed` - What the budget shed
    /// * `draws` - The page's draws on its node's scan budget
    /// * `pace` - The page's pace through its device
    ///
    /// # Errors
    ///
    /// The budget's refusal, for a page that could hand out nothing.
    fn short(
        &self,
        page: WalkRows,
        shed: &Shed,
        draws: Option<PageDraws>,
        pace: Option<&PagePace>,
    ) -> Result<WalkRows, ServerError> {
        // a page holding partitions ends short where it stands, its next page going on from them
        if page.partitions.is_empty() || shed.never_fits() {
            return Err(ServerError::Shoal(ShoalError::ScanBudget {
                needed: shed.needed,
                limit: shed.limit,
            }));
        }
        let next = page.partitions.last().map(|partition| partition.key);
        Ok(self.ended(page, next, draws, pace))
    }

    /// An archived partition's bytes for this page: as the page's reader read them, or read now
    ///
    /// A record a run read and refused is refused here with the error a read of it alone
    /// answers, as the merge reaches it; a partition the reader did not read is read now.
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `chain` - Where the page's view says its records are
    /// * `state` - Where its read stands
    /// * `page` - The page, whose counts this keeps
    async fn archived_partition(
        &self,
        key: u64,
        chain: &ChainEntry,
        state: ReadState,
        page: &mut WalkRows,
    ) -> Result<Option<PartitionBytes>, ServerError> {
        match state {
            // a record a run read, verified as a read of it alone is
            ReadState::Record(read) => Ok(Some(PartitionBytes::Record(read?))),
            // a partition the reader read alone, followed if it had moved
            ReadState::Partition(read) => {
                let (read, reprobed) = read?;
                if reprobed {
                    page.reprobed += 1;
                }
                Ok(read)
            }
            // a driver's page's partition, or one a run could not read whole, read now
            ReadState::Unplanned | ReadState::Planned | ReadState::Alone => {
                self.read(key, chain, page).await
            }
        }
    }

    /// Read an archived partition for this page, counting a record that had moved
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `chain` - Where the page's view says its records are
    /// * `page` - The page, whose count of moved records this keeps
    async fn read(
        &self,
        key: u64,
        chain: &ChainEntry,
        page: &mut WalkRows,
    ) -> Result<Option<PartitionBytes>, ServerError> {
        // a scan page's read holds its place among its device's scan reads in flight (F128)
        let _place = match &self.device {
            Some(device) => Some(device.read(chain_bytes(chain)).await),
            None => None,
        };
        // read where the view says, or where the map says now, a chain's records a read each
        let (read, reprobed) = self.archived.read(key, chain).await?;
        page.reads += 1 + chain.fragments.len() as u64;
        if reprobed {
            page.reprobed += 1;
        }
        Ok(read)
    }
}

#[cfg(test)]
mod draws_tests {
    use super::PageDraws;
    use crate::server::budget::Budget;
    use glommio::LocalExecutorBuilder;

    /// A scan page's reads draw only what passes what it holds and drew, keep the permits, and
    /// are shed past the budget (F118)
    #[test]
    fn a_pages_reads_draw_past_what_it_was_admitted_with() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let budget = Budget::new("a test's scan budget", 4 << 20);
                // admitted with a megabyte drawn, its capture holding half of it
                let admitted = budget.try_draw(1 << 20).expect("fits");
                let mut draws = PageDraws::new(budget.clone(), 1 << 20, 512 << 10);
                // a read inside what it drew draws nothing more
                draws.cover(256 << 10).await.expect("inside what was drawn");
                assert!(draws.permits.is_empty());
                assert_eq!(budget.used(), 1 << 20);
                // one past it draws the excess alone
                draws.cover(1 << 20).await.expect("the budget has room");
                assert_eq!(draws.permits.len(), 1);
                assert_eq!(budget.used(), (1 << 20) + (768 << 10));
                // and one past the whole budget is shed, never drawing past it
                let shed = draws.cover(8 << 20).await.expect_err("drew past the budget");
                assert!(shed.never_fits());
                assert!(budget.used() <= budget.limit());
                drop(draws);
                drop(admitted);
                assert_eq!(budget.used(), 0);
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }
}

/// A walked partition of an unsorted table as its one row
///
/// # Arguments
///
/// * `bytes` - The walked partition's archived bytes
///
/// # Errors
///
/// Fails if the bytes do not validate as a partition of the table, or hold no live row.
pub fn walked_unsorted<R: ShoalUnsortedTable>(bytes: &[u8]) -> Result<R, ServerError>
where
    for<'a> <UnsortedPartition<R> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    // aligned for the archive, then validated before anything is read
    let bytes = aligned(bytes);
    let partition = <UnsortedPartition<R> as RkyvSupport>::access(&bytes)?;
    match &partition.row {
        ArchivedMaybeRow::Row(row) => Ok(<R as RkyvSupport>::deserialize(row)?),
        // a walk never hands out a deleted row
        ArchivedMaybeRow::Tombstone => Err(ServerError::GlommioGeneric(
            "a walked partition holds no live row".to_string(),
        )),
    }
}

/// A walked partition of a sorted table as its live rows, in sort order
///
/// # Arguments
///
/// * `bytes` - The walked partition's archived bytes
///
/// # Errors
///
/// Fails if the bytes do not validate as a partition of the table.
pub fn walked_sorted<R: ShoalSortedTable>(bytes: &[u8]) -> Result<Vec<R>, ServerError>
where
    <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
    for<'a> <SortedPartition<R> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    // aligned for the archive, then validated before anything is read
    let bytes = aligned(bytes);
    let partition = <SortedPartition<R> as RkyvSupport>::access(&bytes)?;
    // every live row, in the order the partition keeps them
    partition
        .live_row_values()
        .map(|row| <R as RkyvSupport>::deserialize(row).map_err(ServerError::from))
        .collect()
}

/// A walked partition of a sorted table as its live rows, in sort order, every piece of it read
///
/// A scan's page hands a partition it folded a step at a time out in pieces, each a partition of a
/// stretch of its rows ([F127](../../../../docs/src/features/folds-in-steps.md)), so a reader of a
/// sorted table's walked partitions reads every piece.
///
/// # Arguments
///
/// * `partition` - The walked partition
///
/// # Errors
///
/// Fails if a piece does not validate as a partition of the table.
pub fn walked_rows<R: ShoalSortedTable>(partition: &WalkedPartition) -> Result<Vec<R>, ServerError>
where
    <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
    for<'a> <SortedPartition<R> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    // each piece's live rows, the pieces in sort order
    let mut rows = Vec::new();
    for piece in partition.pieces() {
        rows.extend(walked_sorted::<R>(piece)?);
    }
    Ok(rows)
}

/// A walked partition of a bucket's `StripeMeta` table, as the object layer reasons about it
///
/// A walked partition is a snapshot's record, an unsorted partition holding one live row, so its
/// row is read by the same conversion a `StripeMeta` answer is
/// ([F87](../../../../docs/src/features/tablet-walk.md)).
///
/// # Arguments
///
/// * `bytes` - The walked partition's archived bytes
///
/// # Errors
///
/// Fails if the bytes do not validate as a partition of the table, or hold no live row.
pub fn walked_stripe<M: ObjectQueries>(bytes: &[u8]) -> Result<StripeRow, ServerError>
where
    for<'a> <UnsortedPartition<M::StripeMeta> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // aligned for the archive, then validated before anything is read
    let bytes = aligned(bytes);
    let partition = <UnsortedPartition<M::StripeMeta> as RkyvSupport>::access(&bytes)?;
    match &partition.row {
        ArchivedMaybeRow::Row(row) => Ok(M::stripe_row(row)),
        // a walk never hands out a deleted row
        ArchivedMaybeRow::Tombstone => Err(ServerError::GlommioGeneric(
            "a walked stripe partition holds no live row".to_string(),
        )),
    }
}

/// A walked partition of a bucket's `ObjectMeta` table, as the object layer reasons about it: what
/// reclamation reads every entry of a tablet by (M20)
///
/// # Arguments
///
/// * `bytes` - The walked partition's archived bytes
///
/// # Errors
///
/// Fails if the bytes do not validate as a partition of the table, or hold no live row.
pub fn walked_object<M: ObjectQueries>(bytes: &[u8]) -> Result<crate::shared::objects::ObjectRow, ServerError>
where
    for<'a> <UnsortedPartition<M::ObjectMeta> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // aligned for the archive, then validated before anything is read
    let bytes = aligned(bytes);
    let partition = <UnsortedPartition<M::ObjectMeta> as RkyvSupport>::access(&bytes)?;
    match &partition.row {
        ArchivedMaybeRow::Row(row) => Ok(M::object_row(row)),
        // a walk never hands out a deleted row
        ArchivedMaybeRow::Tombstone => Err(ServerError::GlommioGeneric(
            "a walked object partition holds no live row".to_string(),
        )),
    }
}

/// Copy a read's bytes into a buffer a partition can be accessed from
///
/// # Arguments
///
/// * `bytes` - The bytes
#[must_use]
pub fn aligned(bytes: &[u8]) -> AlignedVec {
    // rkyv lays a u128 at a sixteen byte boundary, which a plain vector does not promise
    let mut aligned = AlignedVec::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    aligned
}
