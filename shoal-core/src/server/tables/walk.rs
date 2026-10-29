//! A walk of one tablet's rows, a page at a time
//!
//! A driver on the shard that leads a tablet's group asks for the tablet's rows in pages: a range
//! of the tablet's keys, a cursor, and bounds on how much one page holds
//! ([F87](../../../../docs/src/features/tablet-walk.md)). A page is taken in two halves. The table
//! captures its resident partitions in the range on the shard's loop and takes a view of its
//! archive map in the same step, so both describe one moment, the page's boundary; a task off
//! the loop then merges the two in key order, reading what is archived, and hands the page out.
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
use tracing::{event, instrument, Level};

use crate::server::budget::{Budget, Permit, DRAW_WAIT};
use crate::server::errors::ShoalError;
use crate::server::scans::{PagePace, TablePace};
use crate::server::steps::Steps;
use crate::server::tables::partitions::{ArchivedMaybeRow, SortedPartition, UnsortedPartition};
use crate::server::tables::storage::fs::index::Scan;
use crate::server::tables::storage::fs::{ArchiveMap, ChainEntry};
use crate::server::tables::PartitionBytes;
use crate::server::ServerError;
use crate::shared::objects::ObjectQueries;
use crate::shared::stripes::StripeRow;
use crate::shared::traits::{RkyvSupport, ShoalSortedTable, ShoalUnsortedTable};

/// How many times a record whose archive moved is looked up again before a page gives up
const REPROBES: usize = 3;

/// What one page of a walk asks a table for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

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
    },
    /// A partition with no live row, which hides its archived copy
    Shadow,
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
        }
    }

    /// The bytes this capture holds
    #[must_use]
    pub fn bytes(&self) -> u64 {
        // what was copied off the loop
        match self {
            Captured::Whole { bytes, .. } | Captured::Overlay { bytes, .. } => bytes.len() as u64,
            Captured::Shadow => 0,
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

/// One partition a page handed out
#[derive(Debug)]
pub struct WalkedPartition {
    /// Its key
    pub key: u64,
    /// How many live rows it holds
    pub rows: u32,
    /// Its archived bytes, as a snapshot's record holds them, with no tombstone
    pub bytes: AlignedVec,
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
        // first where the view says, then where the map says now
        let mut chain = chain.clone();
        let mut reprobed = false;
        for _ in 0..=REPROBES {
            match map.read_chain(&chain).await {
                Ok(bytes) => return Ok((Some(bytes), reprobed)),
                // an archive pass removed the archive after repointing what it held, so the
                // map says where the partition is now, or that it is gone
                Err(ServerError::Shoal(ShoalError::ArchiveMissing { archive, .. })) => {
                    event!(
                        Level::DEBUG,
                        msg = "a walked record's archive moved; looking it up again",
                        key,
                        %archive
                    );
                    reprobed = true;
                    match map.chain_of(key).await? {
                        Some(moved) => chain = moved,
                        // deleted since the page began, so it is left out
                        None => return Ok((None, true)),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(ServerError::GlommioGeneric(format!(
            "partition {key:016x} moved archives {REPROBES} times under one page of a walk"
        )))
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
    /// How the table makes a partition from what was captured and read
    fold: WalkFold,
}

impl PendingWalk {
    /// Capture a page from a tablet's resident partitions and its archive's view
    ///
    /// The resident partitions of the page's range are captured in key order, until the page's
    /// bounds are met by partitions certain to be handed out; the archive is then read only up to
    /// the last of them, since nothing past it could be on the page. The archived half has to be
    /// taken by the caller in the same step, with nothing awaited, over `[bounds.start, end]` of
    /// what this returns.
    ///
    /// Returns the captures and the last key the page reads.
    ///
    /// # Arguments
    ///
    /// * `bounds` - What the page was asked for
    /// * `keys` - The keys of the tablet's resident partitions, in no order
    /// * `capture` - How the table captures one of its resident partitions
    pub fn capture<C>(
        bounds: &WalkBounds,
        keys: impl Iterator<Item = u64>,
        mut capture: C,
    ) -> Result<(VecDeque<(u64, Captured)>, u64), ServerError>
    where
        C: FnMut(u64) -> Result<Option<Captured>, ServerError>,
    {
        // the resident keys of the range, in order
        let mut keys: Vec<u64> = keys
            .filter(|key| (bounds.start..=bounds.high).contains(key))
            .collect();
        keys.sort_unstable();
        let mut captured = VecDeque::new();
        let mut certain = 0usize;
        let mut bytes = 0u64;
        for key in keys {
            // a key the table no longer holds is not captured
            let Some(entry) = capture(key)? else {
                continue;
            };
            // a partition certain to be handed out counts against the bounds
            let counts = entry.is_certain();
            if counts {
                certain += 1;
                bytes = bytes.saturating_add(entry.bytes());
            }
            captured.push_back((key, entry));
            // once the bounds are met nothing past this key can be on the page
            if counts && (certain >= bounds.limit.max(1) || bytes >= bounds.max_bytes) {
                return Ok((captured, key));
            }
        }
        Ok((captured, bounds.high))
    }

    /// The bytes the page's capture holds, its resident partitions copied off the loop
    #[must_use]
    pub fn held_bytes(&self) -> u64 {
        self.resident.iter().map(|(_, captured)| captured.bytes()).sum()
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
    #[must_use]
    pub fn new(
        bounds: WalkBounds,
        end: u64,
        resident: VecDeque<(u64, Captured)>,
        archived: ArchivedWalk,
        fold: WalkFold,
    ) -> Self {
        PendingWalk {
            bounds,
            end,
            resident,
            archived,
            fold,
        }
    }

    /// Read the page: merge the resident partitions with the archived ones in key order
    ///
    /// Every partition handled is counted into `steps` by the bytes it took, captured and read,
    /// so a scan's page yields every 64 KiB of work and a driver's, whose steps never yield, runs
    /// as it did ([F117](../../../../docs/src/features/scan-class.md)).
    ///
    /// A scan page's archived reads are drawn from its node's scan budget before they are read
    /// (F118): past what the page drew, a record waits a while for room, and a page that found
    /// none ends short at the last partition it handed out, or is shed if it handed out none.
    /// Each then waits for its device's idle time under its ceiling, or its trickle (F119).
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
        // the archive's next partition, read ahead
        let mut archived = self.archived.next().await?;
        loop {
            // the smaller of the two heads; a key both hold is the resident one's
            let resident_key = self.resident.front().map(|(key, _)| *key);
            let archived_key = archived.as_ref().map(|(key, _)| *key);
            let key = match (resident_key, archived_key) {
                (Some(resident), Some(archived)) => resident.min(archived),
                (Some(resident), None) => resident,
                (None, Some(archived)) => archived,
                // both are spent: the range up to the page's end is walked
                (None, None) => {
                    page.next = (self.end < self.bounds.high).then_some(self.end);
                    page.drawn = draws.map(|draws| draws.permits).unwrap_or_default();
                    page.paced = pace.as_ref().map_or_else(Default::default, PagePace::waited);
                    return Ok(page);
                }
            };
            // the resident capture of this key, and its archived chain, whichever exist
            let captured = if resident_key == Some(key) {
                self.resident.pop_front().map(|(_, captured)| captured)
            } else {
                None
            };
            let chain = if archived_key == Some(key) {
                let chain = archived.take().map(|(_, chain)| chain);
                archived = self.archived.next().await?;
                chain
            } else {
                None
            };
            // a scan page draws a record's bytes before it is read, a whole copy's archive unread
            let reads = !matches!(captured, Some(Captured::Whole { .. }) | Some(Captured::Shadow));
            if let (Some(draws), Some(chain), true) = (draws.as_mut(), chain.as_ref(), reads) {
                if let Err(shed) = draws.cover(chain_bytes(chain)).await {
                    // a page holding partitions ends short where it stands, its next page going
                    // on from them; one holding none, or a record past the whole budget, is refused
                    if page.partitions.is_empty() || shed.never_fits() {
                        return Err(ServerError::Shoal(ShoalError::ScanBudget {
                            needed: shed.needed,
                            limit: shed.limit,
                        }));
                    }
                    page.next = page.partitions.last().map(|partition| partition.key);
                    page.drawn = draws.permits.drain(..).collect();
                    page.paced = pace.as_ref().map_or_else(Default::default, PagePace::waited);
                    return Ok(page);
                }
            }
            // and waits for its device's idle time under its ceiling, or its trickle (F119)
            if let (Some(pace), Some(chain), true) = (pace.as_mut(), chain.as_ref(), reads) {
                pace.take(chain_bytes(chain)).await;
            }
            // the partition this key comes to, if any of it is live, and the bytes it took
            let mut handled = 0usize;
            let walked = match captured {
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
                // a partition only its archive holds
                None => match &chain {
                    Some(chain) => match self.read(key, chain, &mut page).await? {
                        Some(read) => {
                            page.archived += 1;
                            handled = read.len();
                            (self.fold)(key, Some(&read), None)?
                        }
                        None => None,
                    },
                    None => None,
                },
            };
            // the work counted into a step, which a scan's page yields at (F117)
            steps.take(handled).await;
            // hand it out, and stop once the page's bounds are met
            if let Some((rows, bytes)) = walked {
                page.bytes = page.bytes.saturating_add(bytes.len() as u64);
                page.partitions.push(WalkedPartition { key, rows, bytes });
                if page.partitions.len() >= limit || page.bytes >= self.bounds.max_bytes {
                    // nothing left to read means the range is done, not the page
                    let more = !self.resident.is_empty()
                        || archived.is_some()
                        || self.end < self.bounds.high;
                    page.next = more.then_some(key);
                    page.drawn = draws.map(|draws| draws.permits).unwrap_or_default();
                    page.paced = pace.as_ref().map_or_else(Default::default, PagePace::waited);
                    return Ok(page);
                }
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
        // read where the view says, or where the map says now
        let (read, reprobed) = self.archived.read(key, chain).await?;
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
