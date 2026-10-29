//! A write in place, and an append
//!
//! An object held in its entry is written there while it stays at or under its pool's inline
//! threshold, and is moved to stripes once, never back, when a write grows it past
//! ([S3](../../../../../docs/src/object-storage/objects.md#small-objects-stay-inline)). A striped
//! object's write is S7's three steps on each stripe it touches
//! ([S7](../../../../../docs/src/object-storage/write-path.md#the-preferred-direction-step-by-step)):
//!
//! 1. **read**: the entry strongly, for the truncate epoch, then the stripe's row at `Quorum`
//!    through its group's leader; a row fenced past the epoch read moves the epoch past a dead
//!    truncate's fence, and one stamped past it means the entry is read again;
//! 2. **stage**: the new values of every unit the write changes - a unit it changes in part read
//!    first and laid over, the units a floor hides written as zeros, and any pending bytes carried
//!    - staged on every holder under the write's label, a tag a try;
//! 3. **commit**: once `k + f` holders staged durably, one conditional commit of the stripe's row
//!    on the sequence read, its identity the write's, the stripe's and the try's; the holders that
//!    did not stage marked missed. Then the holders are told to apply.
//!
//! A write below the pool's small-write threshold rides inside its commit instead: every holder
//! confirms the chunk its bytes are laid over, the commit carries them as the row's pending bytes,
//! merged over any there, and the writer then has every holder fold them and clears them from the
//! row once `k + f` hold their label ([X1](../../../../../docs/src/object-storage/stripe-model.md#a-small-write-in-its-commit)).
//!
//! A write past the object's end commits its stripes first and its size after, on the epoch it
//! read. An append claims its region first - the size moved on the entry's version, so two
//! appenders never claim one region - and then writes it ([F86](../../../../../docs/src/features/replicated-pools.md)).
//!
//! # Invariants
//!
//! **A stage is discarded only once nothing can commit it**: after its commit was refused, or
//! answered as another try's, or never proposed. Never after a timeout, when a commit in flight may
//! still land ([P16](../../../../../docs/src/object-storage/contract.md)).
//!
//! **A holder is told to apply only the label this task committed.** An answer that a reused
//! identity committed is another coordinator's label; the holders learn that one from the row.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::join_all;

use super::place::{Placement, StripeSet};
use super::read::StripeRead;
use super::{not_found, Meta, MetaAnswer, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::server::objects::executor::answer_of;
use crate::server::objects::put::{self as pipeline, PutPlan, PIECE_BYTES};
use crate::server::objects::{Holder, ObjectStore};
use crate::shared::checksum;
use crate::shared::objects::{
    ChangedUnits, Label, ObjectEntry, PendingBytes, PendingUnit, StripeAnswer, SMALL_WRITE_THRESHOLD,
};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::{ObjectAnswer, ObjectOpKind, ObjectOutcome};
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece, UnitBytes};
use crate::shared::queries::ConditionRefusal;
use crate::shared::stripes::{
    commit_bundle, hidden_from, is_hole, stripes_of, write_tag, ChunkId, StripeRow, Touch,
    WriteCommit,
};
use crate::shared::traits::QuerySupport;

/// How many times a commit whose outcome is unknown is sent again, byte for byte, before the
/// write is answered as unknown
const RESENDS: usize = 3;

/// How many rounds in a row a write may lose its stripes to other writes before it paces its next
/// round (item 312)
const LOSSES_BEFORE_PACING: u32 = 8;

/// The longest a write waits before a round once it paces them: each lost round past
/// [`LOSSES_BEFORE_PACING`] doubles the bound a wait is drawn under, from a millisecond up to this
/// (item 312)
const MOST_PACING: Duration = Duration::from_millis(50);

/// How long a write waits before its next round, having lost this many in a row
///
/// None for the first [`LOSSES_BEFORE_PACING`], then a draw under a bound that doubles with each
/// further loss from a millisecond up to [`MOST_PACING`], so the writes that keep meeting each other
/// on a stripe spread out rather than meet again (item 312).
///
/// # Arguments
///
/// * `lost` - The rounds this write lost in a row
fn pacing_after(lost: u32) -> Duration {
    // the first losses are taken again at once
    let Some(past) = lost.checked_sub(LOSSES_BEFORE_PACING).filter(|past| *past > 0) else {
        return Duration::ZERO;
    };
    // a bound that doubles with each loss past them, never past the most; sixteen doublings of a
    // millisecond are past any most, and a shift past them would lose its bits
    let most = u64::try_from(MOST_PACING.as_micros()).unwrap_or(u64::MAX);
    let bound = match past - 1 {
        doublings if doublings < 16 => (1_000u64 << doublings).min(most),
        _ => most,
    };
    // a draw under it, so the writes that met do not meet again at the same moment
    Duration::from_micros(rand::random::<u64>() % (bound + 1))
}

/// What one stripe's write came to
pub(super) enum StripeStep {
    /// Its commit applied, this try's or an earlier try's of the same write
    Committed,
    /// Its commit was refused, or its row moved: read the row again and try again
    Retry(RetryWhy),
    /// A holder holds a label its row does not name and not an older one: read the row again,
    /// and refuse the write by name if it reads the same, since the row is behind its chunks (F93)
    Ahead,
    /// The row is fenced past the epoch read: a truncate is committing, or died having fenced
    Advance,
    /// The row is stamped past the epoch read: the entry has to be read again
    Reread,
    /// It cannot be written now, and this is the answer
    Refused(ObjectAnswer),
    /// The machinery failed under it
    Failed(ErrorCode, String),
}

/// Why one stripe's write is tried again, which a write refused `Busy` counts by (item 312)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetryWhy {
    /// Its commit was refused: another write committed first on the sequence it read
    Refused,
    /// Its stages or confirmations fell short, and its row had moved since it was read
    RowMoved,
    /// Its placement group moved since its state was read
    Placement,
    /// It had no row, and one was made for it before anything was staged
    RowMade,
}

/// How a write's rounds ended, stripe by stripe, which a write refused `Busy` names (item 312)
#[derive(Debug, Default)]
struct RoundTally {
    /// Commits refused, another write first on the sequence read
    refused: u32,
    /// Stages or confirmations short with the row moved since it was read
    row_moved: u32,
    /// Placement groups moved since their state was read
    placement: u32,
    /// Rows made for stripes that had none
    row_made: u32,
    /// Holders found past the row read
    ahead: u32,
    /// Rows fenced past the epoch read
    advance: u32,
    /// Rows stamped past the epoch read
    reread: u32,
}

impl RoundTally {
    /// Count one retry by why
    ///
    /// # Arguments
    ///
    /// * `why` - Why the stripe is tried again
    fn retried(&mut self, why: RetryWhy) {
        // one count a reason
        match why {
            RetryWhy::Refused => self.refused += 1,
            RetryWhy::RowMoved => self.row_moved += 1,
            RetryWhy::Placement => self.placement += 1,
            RetryWhy::RowMade => self.row_made += 1,
        }
    }
}

impl std::fmt::Display for RoundTally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} commits refused, {} short with the row moved, {} placements moved, {} rows made, {} holders ahead, {} fenced, {} stamped",
            self.refused, self.row_moved, self.placement, self.row_made, self.ahead, self.advance, self.reread
        )
    }
}

/// One stripe's part of a write
pub(super) struct StripePart<'a> {
    /// The stripe
    pub stripe: u64,
    /// Where in the stripe the part starts
    pub at: u64,
    /// Its bytes
    pub data: &'a [u8],
}

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Write the bytes in place, at the operation's offset or, for an append, at the object's end
    pub(super) async fn write(&self) -> Outcome {
        let op = &self.op;
        // the bytes, held to the checksum the head named before anything is staged
        let data = self.data.clone().unwrap_or_default();
        let crc = checksum::crc64(&data);
        if crc != op.checksum {
            return Ok((self.mismatch(data.len() as u64, crc), None));
        }
        let key = self.key();
        let nonce: u64 = rand::random();
        for _ in 0..MAX_TRIES {
            // the entry, strongly, for its size and its truncate epoch
            let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
            let Some(entry) = row
                .as_ref()
                .and_then(|row| row.entry(&op.path))
                .filter(|entry| entry.state.live)
                .cloned()
            else {
                return Ok((not_found(op), None));
            };
            let offset = if op.kind == ObjectOpKind::Append {
                entry.size
            } else {
                op.offset
            };
            let end = offset + data.len() as u64;
            // an object held in its entry is written there, or moved to stripes once
            if let Some(inline) = &entry.inline {
                let mut bytes = inline.clone();
                if bytes.len() < end as usize {
                    bytes.resize(end as usize, 0);
                }
                bytes[offset as usize..end as usize].copy_from_slice(&data);
                let outcome = if bytes.len() as u64 <= self.pool.inline_threshold() {
                    self.rewrite_inline(key, row.as_ref(), &entry, bytes).await?
                } else {
                    self.move_to_stripes(key, row.as_ref(), &entry, bytes).await?
                };
                match outcome {
                    Some(done) => return Ok((done, None)),
                    // the row moved: read it again
                    None => continue,
                }
            }
            // an append claims its region first, so no other appender writes over it
            let entry = if op.kind == ObjectOpKind::Append {
                match self.claim(key, row.as_ref(), &entry, end).await? {
                    Some(claimed) => claimed,
                    None => continue,
                }
            } else {
                entry
            };
            return self.write_region(entry, offset, &data, nonce).await;
        }
        Err(super::contended(&op.path))
    }

    /// Rewrite an inline object's bytes in its entry, conditional on the row read
    ///
    /// Returns the answer, or none when the row moved.
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `row` - The row read
    /// * `entry` - The entry read
    /// * `bytes` - The object's bytes after the write
    pub(super) async fn rewrite_inline(
        &self,
        key: u64,
        row: Option<&crate::shared::objects::ObjectRow>,
        entry: &ObjectEntry,
        bytes: Vec<u8>,
    ) -> Result<Option<ObjectAnswer>, (ErrorCode, String)> {
        let mut next = entry.clone();
        next.size = bytes.len() as u64;
        next.put_length = next.size;
        next.content_version += 1;
        next.modified_ms = super::now_ms();
        next.inline = Some(bytes);
        match self.meta.ask(self.with_entry(key, row, next.clone()), None).await {
            MetaAnswer::Written => Ok(Some(self.done(&next, 0))),
            MetaAnswer::Refused(_) => Ok(None),
            MetaAnswer::Failed(code, why) => Err((code, why)),
            MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                Err((ErrorCode::Internal, "a write answered as a read".to_string()))
            }
        }
    }

    /// Move an inline object to stripes, once: its bytes after the write written into place as
    /// its put's chunks, then its entry committed without them
    ///
    /// Returns the answer, or none when the row moved.
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `row` - The row read
    /// * `entry` - The entry read
    /// * `bytes` - The object's bytes after the write
    pub(super) async fn move_to_stripes(
        &self,
        key: u64,
        row: Option<&crate::shared::objects::ObjectRow>,
        entry: &ObjectEntry,
        bytes: Vec<u8>,
    ) -> Result<Option<ObjectAnswer>, (ErrorCode, String)> {
        let store = match self.store() {
            Ok(store) => store.clone(),
            Err(refused) => return Ok(Some(refused)),
        };
        let placement = match Placement::new(self.map.clone(), &self.pool_name, &self.pool, self.consumer, true) {
            Ok(placement) => placement,
            Err((outcome, why)) => return Ok(Some(self.refused(outcome, why))),
        };
        // the chunks of the object as it will be, at its put's label, written as a put's are,
        // where each stripe's placement group's state names them
        let geometry = entry.geometry;
        let length = bytes.len() as u64;
        let stripes = length.div_ceil(u64::from(geometry.stripe_bytes));
        let sets = match self.place_stripes(&placement, entry.object, stripes).await {
            Ok(sets) => sets,
            Err(why) => return Ok(Some(self.refused(ObjectOutcome::NotReady, why))),
        };
        let plan = PutPlan {
            op: self.op.id,
            stripe_bytes: u64::from(geometry.stripe_bytes),
            unit_bytes: geometry.unit_bytes,
            data: geometry.data,
            parity: geometry.parity,
            length,
            stripes: sets.iter().map(|set| set.targets.clone()).collect(),
            needed: placement.needed(),
            straggler_wait: self.timings.straggler_wait.duration(),
            deadline: self.meta.wait,
            inflight: self.timings.inflight_stripes,
            owner: self.key(),
        };
        let (feed, stream) = kanal::bounded_async(self.timings.put_window_frames.max(1));
        let outcome = pipeline::start(&store, plan, stream);
        for frame in bytes.chunks(PIECE_BYTES as usize) {
            if feed.send(frame.to_vec()).await.is_err() {
                break;
            }
        }
        drop(feed);
        let done = match outcome.recv().await {
            Ok(Ok(done)) => done,
            Ok(Err(failure)) => return Ok(Some(self.refused(ObjectOutcome::NotReady, failure.to_string()))),
            Err(_) => return Err((ErrorCode::Unavailable, "the write's pipeline stopped".to_string())),
        };
        // the stripes a position missed under their groups' state now say so, before the entry
        // names them, as a put's do
        if let Some(refused) = self.check_put(&placement, entry.object, &sets, &done, length).await? {
            return Ok(Some(refused));
        }
        // the entry, its bytes now its stripes', which reach as far as its length (M20)
        let mut next = entry.clone();
        next.size = length;
        next.put_length = length;
        next.extent = next.extent.max(length);
        next.content_version += 1;
        next.modified_ms = super::now_ms();
        next.inline = None;
        match self.meta.ask(self.with_entry(key, row, next.clone()), None).await {
            MetaAnswer::Written => Ok(Some(self.done(&next, 0))),
            MetaAnswer::Refused(_) => Ok(None),
            MetaAnswer::Failed(code, why) => Err((code, why)),
            MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                Err((ErrorCode::Internal, "a write answered as a read".to_string()))
            }
        }
    }

    /// An append's claim of its region: the size moved past it, on the entry's version
    ///
    /// Returns the entry after the claim, or none when the row moved.
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `row` - The row read
    /// * `entry` - The entry read
    /// * `end` - Where the append ends
    async fn claim(
        &self,
        key: u64,
        row: Option<&crate::shared::objects::ObjectRow>,
        entry: &ObjectEntry,
        end: u64,
    ) -> Result<Option<ObjectEntry>, (ErrorCode, String)> {
        let mut next = entry.clone();
        next.size = end;
        // the region it claims is one its stripes may reach (M20)
        next.extent = next.extent.max(end);
        next.content_version += 1;
        next.modified_ms = super::now_ms();
        match self.meta.ask(self.with_entry(key, row, next.clone()), None).await {
            MetaAnswer::Written => Ok(Some(next)),
            MetaAnswer::Refused(_) => Ok(None),
            MetaAnswer::Failed(code, why) => Err((code, why)),
            MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                Err((ErrorCode::Internal, "a write answered as a read".to_string()))
            }
        }
    }

    /// Write a region of a striped object, stripe by stripe, then grow its size if it ended past
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry read, an append's after its claim
    /// * `offset` - Where the region starts
    /// * `data` - Its bytes
    /// * `nonce` - This task's own, which every tag it makes is drawn from
    async fn write_region(&self, mut entry: ObjectEntry, offset: u64, data: &[u8], nonce: u64) -> Outcome {
        let op = &self.op;
        let placement = match Placement::new(self.map.clone(), &self.pool_name, &self.pool, self.consumer, true) {
            Ok(placement) => placement,
            Err((outcome, why)) => return Ok((self.refused(outcome, why), None)),
        };
        let stripe_bytes = u64::from(entry.geometry.stripe_bytes);
        let mut left: Vec<u64> = stripes_of(entry.geometry.stripe_bytes, offset, data.len() as u64).collect();
        let mut tries: BTreeMap<u64, u32> = BTreeMap::new();
        // the row each stripe was found behind a holder at, which read again unchanged is behind
        // its chunks rather than moving under this write (F93)
        let mut ahead_of: BTreeMap<u64, Option<StripeRow>> = BTreeMap::new();
        let read_epoch = entry.epoch;
        let mut rounds: u32 = 0;
        // how the rounds ended, which a refusal names, and how many in a row committed nothing
        let mut tally = RoundTally::default();
        let mut lost: u32 = 0;
        // a write tries its stripes again for as long as one of its own queries may take, and
        // never for a count of rounds: sixty-four were 51 ms on an SSD, and eight writers on one
        // stripe on disks spent them (item 312)
        let started = std::time::Instant::now();
        while !left.is_empty() {
            rounds += 1;
            if rounds > 1 && started.elapsed() > self.meta.wait {
                return Ok((
                    self.refused(
                        ObjectOutcome::Busy,
                        format!(
                            "the stripes of {} moved under this write for {:?}, {} rounds: {tally}; try it again",
                            op.path,
                            started.elapsed(),
                            rounds - 1
                        ),
                    ),
                    None,
                ));
            }
            // a write that keeps losing waits a little before it reads its rows again
            let pacing = pacing_after(lost);
            if !pacing.is_zero() {
                glommio::timer::sleep(pacing).await;
            }
            // the rows of every stripe still to write, at Quorum, through their leaders
            let rows = self.meta.stripe_rows(self.consumer, entry.object, &left).await?;
            let mut next_left = Vec::new();
            // whether any stripe committed this round, which ends a run of losses
            let mut committed = false;
            for stripe in left {
                // this stripe's part of the region
                let start = stripe * stripe_bytes;
                let from = offset.max(start);
                let to = (offset + data.len() as u64).min(start + stripe_bytes);
                let part = StripePart {
                    stripe,
                    at: from - start,
                    data: &data[(from - offset) as usize..(to - offset) as usize],
                };
                let attempt = *tries.get(&stripe).unwrap_or(&0);
                // where the stripe's placement group's state as this shard last read it names it
                let view = self.meta.placement_for(&placement, placement.pg_of(entry.object, stripe)).await;
                let set = match placement.stripe_set(entry.object, stripe, &view) {
                    Ok(set) => set,
                    Err(why) => return Ok((self.refused(ObjectOutcome::NotReady, why), None)),
                };
                let step = self
                    .write_stripe(&placement, &set, &entry, rows.get(&stripe), &part, attempt, nonce)
                    .await;
                match step {
                    StripeStep::Committed => committed = true,
                    StripeStep::Retry(why) => {
                        tally.retried(why);
                        tries.insert(stripe, attempt + 1);
                        next_left.push(stripe);
                    }
                    StripeStep::Ahead => {
                        tally.ahead += 1;
                        let read = rows.get(&stripe).cloned();
                        if ahead_of.get(&stripe) == Some(&read) {
                            let sequence = read.as_ref().map_or(0, |row| row.sequence);
                            return Ok((self.behind(stripe, sequence), None));
                        }
                        ahead_of.insert(stripe, read);
                        tries.insert(stripe, attempt + 1);
                        next_left.push(stripe);
                    }
                    StripeStep::Advance => {
                        tally.advance += 1;
                        self.advance(&entry).await?;
                        entry = self.reread_entry().await?;
                        next_left.push(stripe);
                    }
                    StripeStep::Reread => {
                        tally.reread += 1;
                        entry = self.reread_entry().await?;
                        next_left.push(stripe);
                    }
                    StripeStep::Refused(answer) => return Ok((answer, None)),
                    StripeStep::Failed(code, why) => return Err((code, why)),
                }
            }
            // a round that committed a stripe ends a run of losses, one that committed none adds to it
            lost = if committed { 0 } else { lost + 1 };
            left = next_left;
            // a truncate that committed after an append's claim cut it: what it wrote is gone
            if op.kind == ObjectOpKind::Append && entry.epoch != read_epoch {
                return Ok((self.done(&entry, 0), None));
            }
        }
        // a write past the end grows the size after its stripes, if no truncate came between
        let end = offset + data.len() as u64;
        let outcome = if op.kind == ObjectOpKind::WriteAt && end > entry.size {
            self.extend(end, read_epoch).await?
        } else {
            (self.done(&entry, 0), None)
        };
        // the answer says where the bytes went, which an append is told only here
        let (mut answer, range) = outcome;
        if answer.outcome == ObjectOutcome::Done {
            answer.offset = offset;
            answer.length = data.len() as u64;
        }
        Ok((answer, range))
    }

    /// Grow the object's size to cover a write that ended past it, on the epoch the write read
    ///
    /// # Arguments
    ///
    /// * `end` - Where the write ended
    /// * `read_epoch` - The truncate epoch the write read
    async fn extend(&self, end: u64, read_epoch: u64) -> Outcome {
        let key = self.key();
        for _ in 0..MAX_TRIES {
            let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
            let Some(entry) = row
                .as_ref()
                .and_then(|row| row.entry(&self.op.path))
                .filter(|entry| entry.state.live)
                .cloned()
            else {
                return Ok((not_found(&self.op), None));
            };
            // a truncate between the write and its size hid what it wrote: whether the write
            // happened is not known
            if entry.epoch != read_epoch {
                return Err((
                    ErrorCode::OutcomeUnknown,
                    format!("a truncate of {} committed before this write's size", self.op.path),
                ));
            }
            if entry.size >= end {
                return Ok((self.done(&entry, 0), None));
            }
            let mut next = entry.clone();
            next.size = end;
            // the stripes it wrote reach this far, whatever a truncate later cuts (M20)
            next.extent = next.extent.max(end);
            next.content_version += 1;
            next.modified_ms = super::now_ms();
            match self.meta.ask(self.with_entry(key, row.as_ref(), next.clone()), None).await {
                MetaAnswer::Written => return Ok((self.done(&next, 0), None)),
                MetaAnswer::Refused(_) => continue,
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((ErrorCode::Internal, "a write answered as a read".to_string()))
                }
            }
        }
        Err(super::contended(&self.op.path))
    }

    /// Move the entry's epoch past a fence a truncate left, changing nothing else
    ///
    /// A truncate still committing is refused by it and fences again; one that died is passed
    /// ([X1](../../../../../docs/src/object-storage/stripe-model.md#truncate-q18)).
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry read
    async fn advance(&self, entry: &ObjectEntry) -> Result<(), (ErrorCode, String)> {
        let key = self.key();
        let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
        let Some(current) = row.as_ref().and_then(|row| row.entry(&self.op.path)) else {
            return Ok(());
        };
        // only from the epoch read; a commit since moved it already
        if current.epoch != entry.epoch {
            return Ok(());
        }
        let mut next = current.clone();
        next.epoch += 1;
        match self.meta.ask(self.with_entry(key, row.as_ref(), next), None).await {
            MetaAnswer::Failed(code, why) => Err((code, why)),
            _ => Ok(()),
        }
    }

    /// Write one stripe's part: read, stage on every holder, commit, apply
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `set` - Where the stripe's chunks are, under the placement group's state read
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `part` - The stripe's part of the write
    /// * `attempt` - Which try of this stripe this is
    /// * `nonce` - This task's own
    #[allow(clippy::too_many_arguments)]
    async fn write_stripe(
        &self,
        placement: &Placement,
        set: &StripeSet,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        part: &StripePart<'_>,
        attempt: u32,
        nonce: u64,
    ) -> StripeStep {
        // an erasure coded stripe's write reads, encodes and confirms as its layout needs (F90)
        if entry.geometry.is_erasure() {
            return self.write_erasure(placement, set, entry, row, part, attempt, nonce).await;
        }
        let store = match self.store() {
            Ok(store) => store.clone(),
            Err(refused) => return StripeStep::Refused(refused),
        };
        let holders = &set.targets;
        let stripe_bytes = u64::from(entry.geometry.stripe_bytes);
        let unit = u64::from(entry.geometry.unit_bytes);
        let start = part.stripe * stripe_bytes;
        // the row read, or a stripe its put wrote as it would be if it had one
        let put_length = entry.put_length.saturating_sub(start).min(stripe_bytes);
        let base = row
            .cloned()
            .unwrap_or_else(|| StripeRow::put(self.consumer, entry.object, part.stripe, set.width, put_length, self.key()));
        // a stripe an operator declared lost takes no write: its bytes are gone (F93)
        if base.lost {
            return StripeStep::Refused(self.refused(
                ObjectOutcome::Lost,
                format!(
                    "stripe {} of {} was declared lost by an operator's ResolveStripe",
                    part.stripe, self.op.path
                ),
            ));
        }
        // a fence past the epoch read: a truncate is committing, or died having fenced
        if base.fence > entry.epoch {
            return StripeStep::Advance;
        }
        // a stamp past it: a truncate committed since the entry was read
        if base.stamp > entry.epoch {
            return StripeStep::Reread;
        }
        let hole = is_hole(entry, row, part.stripe);
        let hidden = hidden_from(entry, row, part.stripe).saturating_sub(start);
        let label = Label {
            sequence: base.sequence + 1,
            tag: write_tag(&self.op.id, part.stripe, attempt, nonce),
        };
        // how much of the chunk holds bytes after it
        let held = if hole { 0 } else { base.length.min(hidden) };
        let length = held.max(part.at + part.data.len() as u64);
        // every unit the write changes, with its new value
        let mut units: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let first = part.at / unit;
        let last = (part.at + part.data.len() as u64 - 1) / unit;
        for index in first..=last {
            let unit_start = index * unit;
            let from = part.at.max(unit_start);
            let to = (part.at + part.data.len() as u64).min(unit_start + unit);
            // a unit the write changes in part starts from what it holds now
            let mut value = if from == unit_start && to == unit_start + unit {
                vec![0u8; unit as usize]
            } else {
                match self.old_unit(placement, entry, row, part.stripe, index, held, unit).await {
                    Ok(value) => value,
                    Err(StripeRead::Ahead) => return StripeStep::Ahead,
                    Err(StripeRead::Failed(why)) => {
                        return StripeStep::Refused(self.refused(ObjectOutcome::NotReady, why))
                    }
                }
            };
            value[(from - unit_start) as usize..(to - unit_start) as usize]
                .copy_from_slice(&part.data[(from - part.at) as usize..(to - part.at) as usize]);
            units.insert(index as u32, value);
        }
        // the units a floor hides are written as zeros, so none of what it cut returns (P13)
        if !hole && hidden < base.length {
            let first_hidden = hidden / unit;
            let last_hidden = (base.length.min(stripe_bytes) - 1) / unit;
            for index in first_hidden..=last_hidden {
                if units.contains_key(&(index as u32)) {
                    continue;
                }
                // the unit the cut falls inside keeps what is before it
                let mut value = if index * unit < hidden {
                    match self.old_unit(placement, entry, row, part.stripe, index, held, unit).await {
                        Ok(value) => value,
                        Err(StripeRead::Ahead) => return StripeStep::Ahead,
                        Err(StripeRead::Failed(why)) => {
                            return StripeStep::Refused(self.refused(ObjectOutcome::NotReady, why))
                        }
                    }
                } else {
                    vec![0u8; unit as usize]
                };
                let keep = hidden.saturating_sub(index * unit).min(unit) as usize;
                value[keep..].fill(0);
                units.insert(index as u32, value);
            }
        }
        // pending bytes are carried, so the commit takes them out of the row with its own
        if let Some(pending) = &base.pending {
            for pending_unit in &pending.units {
                units.entry(pending_unit.index).or_insert_with(|| pending_unit.bytes.clone());
            }
        }
        // a write that takes the chunk past its file's class is staged whole, every unit it holds
        // carried, since a chunk is never extended in place; its fold could not be (F96)
        let outgrows = !hole && entry.geometry.outgrows(base.length.min(stripe_bytes), length);
        // a small write rides inside its commit, where the pool takes the path
        let threshold = placement.small_write_threshold();
        if !hole && !outgrows && !base.tombstone && (part.data.len() as u64) < threshold {
            // every unit it changes, merged over what is pending; a carried unit merges to itself
            let pending_units: Vec<PendingUnit> = units
                .iter()
                .map(|(index, bytes)| PendingUnit {
                    index: *index,
                    bytes: bytes.clone(),
                })
                .collect();
            let after = PendingBytes::after(&base, label, &pending_units);
            if after.bytes() <= SMALL_WRITE_THRESHOLD {
                return self
                    .small_write(&store, placement, set, entry, row.is_some(), &base, label, after, length, part.stripe, attempt)
                    .await;
            }
        }
        // the units the chunk holds that the write does not carry, read at the row's label in one
        // read, when it outgrows its class and so is staged whole (F96)
        if outgrows {
            let missing: Vec<u64> = (0..held.div_ceil(unit))
                .filter(|index| !units.contains_key(&(*index as u32)))
                .collect();
            if !missing.is_empty() {
                let old = match self.read_stripe(placement, entry, row, part.stripe, 0, held).await {
                    // a pool of copies answers one part, which stays where it came
                    Ok(old) => old.into_contiguous(),
                    Err(StripeRead::Ahead) => return StripeStep::Ahead,
                    Err(StripeRead::Failed(why)) => {
                        return StripeStep::Refused(self.refused(ObjectOutcome::NotReady, why))
                    }
                };
                for index in missing {
                    // each unit as it is held, the last one's tail zeros
                    let from = usize::try_from(index * unit).unwrap_or(usize::MAX).min(old.len());
                    let to = usize::try_from((index + 1) * unit).unwrap_or(usize::MAX).min(old.len());
                    let mut value = vec![0u8; unit as usize];
                    value[..to - from].copy_from_slice(&old[from..to]);
                    units.insert(index as u32, value);
                }
            }
        }
        // stage on every holder: the whole chunk where it is all new or outgrows its class, else
        // the units it changes
        let unit_count = length.div_ceil(unit);
        let whole = hole || outgrows || (0..unit_count).all(|index| units.contains_key(&(index as u32)));
        // a whole chunk's pieces, or the units it changes, with their checksums, once for every
        // holder
        let pieces = match whole {
            true => Some(WholePieces::cut(entry.geometry.unit_bytes, length, &units).await),
            false => None,
        };
        let changed = match whole {
            true => Vec::new(),
            false => checked_units(units.iter().map(|(index, bytes)| (*index, bytes))).await,
        };
        // every holder of every position, an arrival beside its old slice while the group moves,
        // each whole chunk's header naming whose it is (M20)
        let owner = self.key();
        let mut answers = Vec::with_capacity(holders.len());
        for (chunk, holder) in holders {
            let answer = if let Some(pieces) = &pieces {
                stage_whole(&store, *holder, *chunk, label, stripe_bytes, entry.geometry.unit_bytes, length, pieces, nonce, length, owner)
                    .await
            } else {
                let request = ChunkRequest::StageUnits {
                    chunk: *chunk,
                    makes: label,
                    expects: base.current_labels(usize::from(chunk.position)),
                    length,
                    unit_bytes: entry.geometry.unit_bytes,
                    units: changed.clone(),
                };
                store.send(*holder, request).await
            };
            answers.push(answer);
        }
        let replies = gather(answers, self.meta.wait).await;
        let staged: Vec<bool> = replies.iter().map(|reply| matches!(reply, Some(ChunkReply::Done))).collect();
        // a position staged it only if every holder of it did: a moving one's arrival too
        let held_positions = set.positions(&staged);
        let count = held_positions.iter().filter(|held| **held).count();
        // fewer than k + f staged: nothing of it is proposed, so its stages are discarded now
        if count < placement.needed() {
            discard(&store, holders, &staged, label);
            // a holder past the row this write read holds a write that committed since, or one
            // the row does not name: the loop reads the row again after this answer, and only a
            // row unchanged past it is behind its chunks (F93, item 256, item 262)
            if self.holders_ahead(&store, holders, &base).await {
                return StripeStep::Ahead;
            }
            // holders that cannot make what the row named may be past it: the row moved under
            // this write, which tries again against it as it is now (item 239)
            if self.row_moved(entry.object, part.stripe, row).await {
                return StripeStep::Retry(RetryWhy::RowMoved);
            }
            let why: Vec<String> = replies
                .iter()
                .enumerate()
                .filter(|(_, reply)| !matches!(reply, Some(ChunkReply::Done)))
                .map(|(position, reply)| format!("position {position}: {reply:?}"))
                .collect();
            return StripeStep::Refused(self.refused(
                ObjectOutcome::NotReady,
                format!(
                    "stripe {} of {} was staged by {count} holders and a write needs {}: {}",
                    part.stripe,
                    self.op.path,
                    placement.needed(),
                    why.join("; ")
                ),
            ));
        }
        // a test can hold a stager here, as a pause across a leader change would, or end it
        if let Some(hold) = crate::server::objects::faults::stager_hold() {
            crate::server::objects::faults::hold_this_stager(hold).await;
        }
        crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Answered);
        // what it changed, for the missed record: whole chunks, or a staged write that carried
        // pending bytes, change the whole chunk; any other the run of units it staged (F88)
        let every = ChangedUnits::every(set.width);
        let changed_units = if whole || base.pending.is_some() {
            ChangedUnits::whole(every)
        } else {
            ChangedUnits::covering(every, changed.iter().map(|unit| unit.index))
        };
        // the commit: the next row, on the sequence read, under this try's identity
        let next = base.commit(&WriteCommit {
            label,
            positions: held_positions
                .iter()
                .map(|held| if *held { Touch::Held } else { Touch::Missed })
                .collect(),
            stamp: entry.epoch,
            length,
            pending: None,
            changed: changed_units,
        })
        // claiming the epoch of the placement group's state it was staged under (F88)
        .claiming(set.epoch);
        let committed = self
            .propose(next, row.map(|row| row.sequence), commit_bundle(&self.op.id, part.stripe, attempt))
            .await;
        match committed {
            Proposed::Committed => {
                crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Committed);
                // the holders that staged are told; one that never hears asks the row
                apply(&store, holders, &staged, label);
                StripeStep::Committed
            }
            Proposed::CommittedElsewhere => {
                discard(&store, holders, &staged, label);
                StripeStep::Committed
            }
            Proposed::Refused => {
                discard(&store, holders, &staged, label);
                StripeStep::Retry(RetryWhy::Refused)
            }
            // the group moved since its state was read: read it again, and try under a new try
            Proposed::Placement => {
                self.meta.refresh_placement(set.key).await;
                discard(&store, holders, &staged, label);
                StripeStep::Retry(RetryWhy::Placement)
            }
            Proposed::Failed(code, why) => StripeStep::Failed(code, why),
        }
    }

    /// The value one unit of a stripe holds now, from a current holder, or zeros past what it holds
    ///
    /// # Arguments
    ///
    /// * `placement` - Where the stripe is
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe
    /// * `index` - The unit
    /// * `held` - How much of the chunk holds bytes
    /// * `unit` - The bytes of a unit
    #[allow(clippy::too_many_arguments)]
    async fn old_unit(
        &self,
        placement: &Placement,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        stripe: u64,
        index: u64,
        held: u64,
        unit: u64,
    ) -> Result<Vec<u8>, StripeRead> {
        let unit_start = index * unit;
        // nothing held there reads as zeros, without asking anyone
        if unit_start >= held {
            return Ok(vec![0u8; unit as usize]);
        }
        let len = unit.min(held - unit_start);
        let mut value = self
            .read_stripe(placement, entry, row, stripe, unit_start, len)
            .await?
            .into_contiguous();
        value.resize(unit as usize, 0);
        Ok(value.into_vec())
    }

    /// A small write in its commit: every holder confirms its base, one commit carries the bytes
    ///
    /// # Arguments
    ///
    /// * `store` - The node's object store
    /// * `placement` - The pool's placement
    /// * `set` - Where the stripe's chunks are, under the placement group's state read
    /// * `entry` - The object's entry
    /// * `exists` - Whether the stripe's row was read
    /// * `base` - The row read
    /// * `label` - The write's label
    /// * `pending` - The pending bytes the row holds after it
    /// * `length` - How much of the chunk holds bytes after it
    /// * `stripe` - The stripe
    /// * `attempt` - Which try this is
    #[allow(clippy::too_many_arguments)]
    async fn small_write(
        &self,
        store: &ObjectStore,
        placement: &Placement,
        set: &StripeSet,
        entry: &ObjectEntry,
        exists: bool,
        base: &StripeRow,
        label: Label,
        pending: PendingBytes,
        length: u64,
        stripe: u64,
        attempt: u32,
    ) -> StripeStep {
        // every position's holder confirms, in this write's round, the chunk the bytes go over
        // (Q16), and while the group moves, every moving position's arrival too
        let holders = &set.targets;
        let mut asks = Vec::with_capacity(holders.len());
        for (chunk, holder) in holders {
            let request = ChunkRequest::Confirm {
                chunk: *chunk,
                labels: base.current_labels(usize::from(chunk.position)),
            };
            asks.push(store.send(*holder, request).await);
        }
        let replies = gather(asks, self.meta.wait).await;
        let answered: Vec<bool> = replies
            .iter()
            .zip(holders)
            .map(|(reply, (chunk, _))| {
                matches!(reply, Some(ChunkReply::Held { label: Some(held) }) if base.makes(usize::from(chunk.position), *held))
            })
            .collect();
        // a position confirmed only if every holder of it did
        let confirmed = set.positions(&answered);
        let count = confirmed.iter().filter(|held| **held).count();
        if count < placement.needed() {
            // a holder past the base it read: the loop reads the row again after this answer
            // (F93, item 262)
            if self.holders_ahead(store, holders, base).await {
                return StripeStep::Ahead;
            }
            // the row moved under this write (item 239)
            if self.row_moved(entry.object, stripe, exists.then_some(base)).await {
                return StripeStep::Retry(RetryWhy::RowMoved);
            }
            return StripeStep::Refused(self.refused(
                ObjectOutcome::NotReady,
                format!(
                    "stripe {stripe} of {} was confirmed by {count} holders and a write needs {}",
                    self.op.path,
                    placement.needed()
                ),
            ));
        }
        // a test can hold a stager here, once its holders confirmed, or end it
        if let Some(hold) = crate::server::objects::faults::stager_hold() {
            crate::server::objects::faults::hold_this_stager(hold).await;
        }
        crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Answered);
        // what it changed, for the missed record: the units its pending bytes cover (F88)
        let changed_units = ChangedUnits::covering(
            ChangedUnits::every(set.width),
            pending.units.iter().map(|unit| unit.index),
        );
        // the commit carries the bytes as the row's pending bytes, merged over any there
        let next = base.commit(&WriteCommit {
            label,
            positions: confirmed
                .iter()
                .map(|held| if *held { Touch::Held } else { Touch::Missed })
                .collect(),
            stamp: entry.epoch,
            length,
            pending: Some(pending),
            changed: changed_units,
        })
        // claiming the epoch of the placement group's state it was confirmed under (F88)
        .claiming(set.epoch);
        let committed = self
            .propose(next.clone(), exists.then_some(base.sequence), commit_bundle(&self.op.id, stripe, attempt))
            .await;
        match committed {
            Proposed::Committed => {
                crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Committed);
                // every holder folds them, and the row is cleared of them once k + f hold them
                spawn_fold_and_clear(
                    self.meta.clone(),
                    store.clone(),
                    set.clone(),
                    next,
                    placement.needed(),
                    entry.geometry.unit_bytes,
                    self.timings.clear_wait.duration(),
                );
                StripeStep::Committed
            }
            Proposed::CommittedElsewhere => StripeStep::Committed,
            Proposed::Refused => StripeStep::Retry(RetryWhy::Refused),
            Proposed::Placement => {
                self.meta.refresh_placement(set.key).await;
                StripeStep::Retry(RetryWhy::Placement)
            }
            Proposed::Failed(code, why) => StripeStep::Failed(code, why),
        }
    }

    /// Whether a holder of a stripe holds a label its row does not name and not an older one
    ///
    /// Asked once a write fell short, before its row is read again: a label is applied only after
    /// its commit, so such a holder holds a write that committed since the row was read, or one
    /// the row is behind. Only a strong read after this answer tells them apart, which the write's
    /// loop makes when the stripe answers `StripeStep::Ahead`; a read before it lets a commit land
    /// between the two and refuses a healthy stripe
    /// ([F93](../../../../../docs/src/features/operations-and-the-real-cluster.md), item 256,
    /// [Resolved #262](../../../../../docs/src/appendix/resolved/behind-asked-of-holders-first.md)).
    ///
    /// # Arguments
    ///
    /// * `store` - The node's object store
    /// * `holders` - Each position's chunk and holder
    /// * `base` - The row the write read, or the put's as it would be
    pub(super) async fn holders_ahead(
        &self,
        store: &ObjectStore,
        holders: &[(crate::shared::stripes::ChunkId, crate::server::objects::Holder)],
        base: &StripeRow,
    ) -> bool {
        for (chunk, holder) in holders {
            let position = usize::from(chunk.position);
            let reply = store
                .ask_holder(
                    *holder,
                    ChunkRequest::Confirm {
                        chunk: *chunk,
                        labels: base.current_labels(position),
                    },
                )
                .await;
            if let ChunkReply::Held { label: Some(held) } = reply {
                if held.ahead_of(base.named(position)) {
                    return true;
                }
            }
        }
        false
    }

    /// The answer to a write of a stripe whose row is behind its chunks, by name
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `sequence` - The sequence its row is at, which a `ResolveStripe` is written against
    pub(super) fn behind(&self, stripe: u64, sequence: u64) -> ObjectAnswer {
        self.refused(
            ObjectOutcome::Behind,
            format!(
                "a holder of stripe {stripe} of {} applied a write its row does not name: the row, at \
                 sequence {sequence}, is behind its chunks, which a ResolveStripe settles",
                self.op.path
            ),
        )
    }

    /// Whether a stripe's row moved since a write read it, read again at `Quorum`
    ///
    /// A write whose holders could not take it asks this before it answers `NotReady`: holders
    /// that hold a label past the row it read mean another write committed and was applied
    /// meanwhile, which is a write to try again, never a pool that is not ready (item 239).
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `stripe` - The stripe
    /// * `read` - The row the write read, or none when the stripe had no row
    pub(super) async fn row_moved(&self, object: u128, stripe: u64, read: Option<&StripeRow>) -> bool {
        // a test can fail this read, as a leader change or a deadline under load does
        if crate::server::objects::faults::row_rereads_fail() {
            return false;
        }
        // a row that cannot be read now is not known to have moved
        let Ok(rows) = self.meta.stripe_rows(self.consumer, object, &[stripe]).await else {
            return false;
        };
        rows.get(&stripe).map(|row| row.sequence) != read.map(|row| row.sequence)
    }

    /// Propose a stripe's next row under a write's identity, resending it as it was after an
    /// unknown outcome
    ///
    /// # Arguments
    ///
    /// * `next` - The row after the write
    /// * `read` - The sequence read, or none when the stripe had no row
    /// * `bundle` - The commit's identity
    pub(super) async fn propose(&self, next: StripeRow, read: Option<u64>, bundle: uuid::Uuid) -> Proposed {
        // a test can refuse it before it is proposed, as another writer's beat it (item 312)
        if crate::server::objects::faults::stripe_commit_refused(&self.op.path) {
            return Proposed::Refused;
        }
        for _ in 0..RESENDS {
            match self.meta.commit_stripe(next.clone(), read, Some(bundle)).await {
                StripeAnswer::Written => return Proposed::Committed,
                StripeAnswer::Refused(ConditionRefusal::Placement) => return Proposed::Placement,
                StripeAnswer::Refused(_) => return Proposed::Refused,
                StripeAnswer::Failed(ErrorCode::IdentityReused, said) => {
                    return if said == "applied" {
                        Proposed::CommittedElsewhere
                    } else {
                        Proposed::Refused
                    }
                }
                // not known: the same payload again is answered as it was
                StripeAnswer::Failed(ErrorCode::OutcomeUnknown | ErrorCode::Timeout, _) => continue,
                StripeAnswer::Failed(code, why) => return Proposed::Failed(code, why),
                StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                    return Proposed::Failed(ErrorCode::Internal, "a stripe's commit answered as a read".to_string())
                }
            }
        }
        Proposed::Failed(
            ErrorCode::OutcomeUnknown,
            format!("a commit of {} was not answered after {RESENDS} tries", self.op.path),
        )
    }
}

/// What proposing a stripe's commit came to
pub(super) enum Proposed {
    /// This task's commit applied
    Committed,
    /// A try of this write with another payload committed, another coordinator's
    CommittedElsewhere,
    /// The row moved, or this try's identity was refused before
    Refused,
    /// The placement group moved past the epoch the commit claimed (F88)
    Placement,
    /// The machinery failed, or the outcome stayed unknown
    Failed(ErrorCode, String),
}

/// Units as a stage or a fold sends them, each with the checksum it crosses with
///
/// # Arguments
///
/// * `units` - Each unit's index and bytes
pub(super) async fn checked_units<'a>(units: impl Iterator<Item = (u32, &'a Vec<u8>)>) -> Vec<UnitBytes> {
    let mut checked = Vec::new();
    for (index, bytes) in units {
        checked.push(UnitBytes {
            index,
            crc: crate::shared::checksum::crc64(bytes),
            bytes: bytes::Bytes::copy_from_slice(bytes),
        });
        glommio::yield_if_needed().await;
    }
    checked
}

/// A whole chunk cut into the pieces a stage sends, each with its units' checksums
#[derive(Debug, Clone)]
pub(super) struct WholePieces {
    /// Each piece: where it starts in the chunk, its bytes, shared by every holder's stage of it
    /// (F106), and each unit's CRC-64/NVME
    pieces: Vec<(u64, bytes::Bytes, Vec<u64>)>,
}

impl WholePieces {
    /// Cut a chunk's units into pieces, computing each unit's checksum once for every holder
    ///
    /// # Arguments
    ///
    /// * `unit_bytes` - A unit's size
    /// * `length` - How much of the chunk holds bytes
    /// * `units` - Its units that hold bytes; any missing are zeros
    async fn cut(unit_bytes: u32, length: u64, units: &BTreeMap<u32, Vec<u8>>) -> Self {
        let unit = u64::from(unit_bytes);
        let count = length.div_ceil(unit);
        // the chunk's bytes, every unit that holds any
        let mut bytes = vec![0u8; (count * unit) as usize];
        for (index, value) in units {
            let at = u64::from(*index) * unit;
            if at < count * unit {
                bytes[at as usize..(at + unit) as usize].copy_from_slice(value);
            }
        }
        // whole units a piece, at most a frame's worth
        let per_piece = ((PIECE_BYTES / unit).max(1) * unit) as usize;
        let mut pieces = Vec::with_capacity(bytes.len().div_ceil(per_piece.max(1)));
        for (index, piece) in bytes.chunks(per_piece).enumerate() {
            // each unit's checksum crosses with its bytes, computed here once
            let mut crcs = Vec::with_capacity(piece.len() / unit as usize);
            for value in piece.chunks(unit as usize) {
                crcs.push(crate::shared::checksum::crc64(value));
                glommio::yield_if_needed().await;
            }
            pieces.push(((index * per_piece) as u64, bytes::Bytes::copy_from_slice(piece), crcs));
        }
        WholePieces { pieces }
    }

    /// Cut a chunk's bytes into pieces, its last unit padded with zeros, computing each unit's
    /// checksum once for every holder (F90)
    ///
    /// # Arguments
    ///
    /// * `unit_bytes` - A unit's size
    /// * `bytes` - The chunk's bytes, as many as hold data
    pub(super) async fn from_bytes(unit_bytes: u32, bytes: &[u8]) -> Self {
        let unit = u64::from(unit_bytes.max(1));
        let mut units: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        for (index, value) in bytes.chunks(unit as usize).enumerate() {
            let mut value = value.to_vec();
            value.resize(unit as usize, 0);
            units.insert(index as u32, value);
            glommio::yield_if_needed().await;
        }
        Self::cut(unit_bytes, bytes.len() as u64, &units).await
    }
}

/// Stage a whole chunk on a holder, in pieces, and hand back where its answer arrives
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holder` - The holder
/// * `chunk` - The chunk
/// * `label` - The label it is staged under
/// * `chunk_bytes` - The chunk's size
/// * `unit_bytes` - A unit's size
/// * `length` - How much of it holds bytes
/// * `whole` - Its pieces
/// * `nonce` - The task's own, which names the stream
/// * `stripe_length` - How much of the chunk's stripe holds bytes, which its header records (F90)
/// * `owner` - The key the bucket records the object under, which its header records (M20)
#[allow(clippy::too_many_arguments)]
pub(super) async fn stage_whole(
    store: &ObjectStore,
    holder: Holder,
    chunk: ChunkId,
    label: Label,
    chunk_bytes: u64,
    unit_bytes: u32,
    length: u64,
    whole: &WholePieces,
    nonce: u64,
    stripe_length: u64,
    owner: u64,
) -> Result<kanal::AsyncReceiver<Result<crate::server::objects::executor::Answer, crate::server::objects::store::StoreError>>, ChunkReply> {
    let stream = nonce ^ crate::shared::rendezvous::mix64(chunk.stripe ^ (u64::from(chunk.position) << 56));
    let mut last = None;
    let total = whole.pieces.len();
    for (index, (offset, bytes, crcs)) in whole.pieces.iter().enumerate() {
        let request = ChunkRequest::Stage(Piece {
            stream,
            chunk,
            label,
            chunk_bytes,
            unit_bytes,
            length,
            offset: *offset,
            last: index + 1 == total,
            stripe_length,
            owner,
            crcs: crcs.clone(),
            bytes: bytes.clone(),
        });
        last = Some(store.send(holder, request).await);
    }
    last.unwrap_or_else(|| {
        Err(ChunkReply::Refused {
            code: crate::shared::protocol::peer::chunk::StoreCode::Invalid,
            message: "a whole chunk of nothing".to_string(),
        })
    })
}

/// Wait for every holder's answer, until a deadline: none for a holder that did not answer
///
/// # Arguments
///
/// * `answers` - Where each holder's answer arrives, or its refusal already
/// * `deadline` - How long to wait
pub(super) async fn gather(
    answers: Vec<Result<kanal::AsyncReceiver<Result<crate::server::objects::executor::Answer, crate::server::objects::store::StoreError>>, ChunkReply>>,
    deadline: Duration,
) -> Vec<Option<ChunkReply>> {
    let waits = answers.into_iter().map(|answer| async move {
        match answer {
            Ok(receiver) => {
                let waited = answer_of(receiver);
                futures::pin_mut!(waited);
                let sleep = glommio::timer::sleep(deadline);
                futures::pin_mut!(sleep);
                match futures::future::select(waited, sleep).await {
                    futures::future::Either::Left((reply, _)) => Some(reply),
                    futures::future::Either::Right(_) => None,
                }
            }
            Err(refused) => Some(refused),
        }
    });
    join_all(waits).await
}

/// Tell every holder that staged a committed write to apply it, without waiting
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holders` - The stripe's chunks and holders
/// * `held` - Which positions staged it
/// * `label` - The write's label
pub(super) fn apply(store: &ObjectStore, holders: &[(ChunkId, Holder)], held: &[bool], label: Label) {
    tell(store, holders, held, |chunk| ChunkRequest::Apply { chunk, label });
}

/// Tell every holder that staged a write nothing can commit to discard it, without waiting
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holders` - The stripe's chunks and holders
/// * `held` - Which positions staged it
/// * `label` - The write's label
pub(super) fn discard(store: &ObjectStore, holders: &[(ChunkId, Holder)], held: &[bool], label: Label) {
    tell(store, holders, held, |chunk| ChunkRequest::Discard { chunk, label });
}

/// Send each holder that staged a request, on a task of its own
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holders` - The stripe's chunks and holders
/// * `held` - Which positions staged
/// * `request` - The request each is sent
fn tell(
    store: &ObjectStore,
    holders: &[(ChunkId, Holder)],
    held: &[bool],
    request: impl Fn(ChunkId) -> ChunkRequest,
) {
    for (position, (chunk, holder)) in holders.iter().enumerate() {
        if !held.get(position).copied().unwrap_or(false) {
            continue;
        }
        let store = store.clone();
        let holder = *holder;
        let request = request(*chunk);
        glommio::spawn_local(async move {
            let _ = store.ask_holder(holder, request).await;
        })
        .detach();
    }
}

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Fold and clear the pending bytes a writer left in a row, once this shard has met them
    /// there for `clear_wait`: a writer that died after its commit never clears them itself
    ///
    /// # Arguments
    ///
    /// * `placement` - Where the stripe is
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row
    pub(super) async fn meet_pending(&self, placement: &Placement, entry: &ObjectEntry, row: &StripeRow) {
        let Some(pending) = &row.pending else {
            return;
        };
        let Ok(store) = self.store() else {
            return;
        };
        // by this shard's own clock since it first met them, never by a time the row carries
        let wait = self.timings.clear_wait.duration();
        if !self.meta.client.pending_overdue(row.key(), pending.label, wait) {
            return;
        }
        let view = self.meta.placement_for(placement, placement.pg_of(entry.object, row.key().2)).await;
        let Ok(set) = placement.stripe_set(entry.object, row.key().2, &view) else {
            return;
        };
        spawn_fold_and_clear(
            self.meta.clone(),
            store.clone(),
            set,
            row.clone(),
            placement.needed(),
            entry.geometry.unit_bytes,
            wait,
        );
    }
}

/// Fold a small write's committed bytes into every holder, then clear them from the row once
/// `k + f` hold their label, on a task of its own
///
/// The writer's own, straight after its commit; a reader or a writer that meets pending bytes
/// nobody cleared drives it too. A clear is conditional on the row's sequence, so two never both
/// land, and every holder that did not say it holds the label is marked missed (P17).
///
/// # Arguments
///
/// * `meta` - How to commit the clear
/// * `store` - The node's object store
/// * `set` - Where the stripe's chunks are, under the placement group's state read
/// * `row` - The row holding the pending bytes
/// * `needed` - `k + f`
/// * `unit_bytes` - A unit's size
/// * `wait` - How long every holder is waited for
pub(super) fn spawn_fold_and_clear<D: ShoalDatabase>(
    meta: Meta<D>,
    store: ObjectStore,
    set: StripeSet,
    row: StripeRow,
    needed: usize,
    unit_bytes: u32,
    wait: Duration,
) where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    let key = row.key();
    // one clear of a stripe at once on this shard
    if !meta.client.begin_clear(key) {
        return;
    }
    glommio::spawn_local(async move {
        if let Some(pending) = row.pending.as_ref() {
            // the bytes with their checksums, once for every holder
            let folded = checked_units(pending.units.iter().map(|unit| (unit.index, &unit.bytes))).await;
            // every holder is given the bytes to fold over the chunk it holds, a moving
            // position's arrival beside its old slice
            let mut asks = Vec::with_capacity(set.targets.len());
            for (chunk, holder) in &set.targets {
                let request = ChunkRequest::Fold {
                    chunk: *chunk,
                    makes: pending.label,
                    sources: pending.sources(usize::from(chunk.position)),
                    length: row.length,
                    unit_bytes,
                    units: folded.clone(),
                };
                asks.push(store.send(*holder, request).await);
            }
            let replies = gather(asks, wait).await;
            let folded: Vec<bool> = replies.iter().map(|reply| matches!(reply, Some(ChunkReply::Done))).collect();
            // a position holds the label only if every holder of it does
            let holding = set.positions(&folded);
            // once k + f hold it durably, the row is cleared on the sequence it is at, claiming
            // the epoch the holders were named under; a group that moved since refuses it (F88)
            if holding.iter().filter(|held| **held).count() >= needed {
                let next = row.clear(&holding).claiming(set.epoch);
                if let StripeAnswer::Refused(ConditionRefusal::Placement) =
                    meta.commit_stripe(next, Some(row.sequence), None).await
                {
                    meta.refresh_placement(set.key).await;
                }
            }
        }
        meta.client.end_clear(&key);
    })
    .detach();
}

#[cfg(test)]
mod tests {
    use super::{pacing_after, LOSSES_BEFORE_PACING, MOST_PACING};
    use std::time::Duration;

    /// A write that keeps losing its stripes takes its first rounds again at once, then waits a
    /// draw under a bound that doubles with each loss, never past the most (item 312)
    #[test]
    fn a_losing_write_paces_its_rounds_under_a_doubling_bound() {
        // the first losses are taken again at once
        for lost in 0..=LOSSES_BEFORE_PACING {
            assert_eq!(pacing_after(lost), Duration::ZERO, "after {lost} losses");
        }
        // then under a millisecond, two, four, and never past the most however many
        for _ in 0..64 {
            assert!(pacing_after(LOSSES_BEFORE_PACING + 1) <= Duration::from_millis(1));
            assert!(pacing_after(LOSSES_BEFORE_PACING + 2) <= Duration::from_millis(2));
            assert!(pacing_after(LOSSES_BEFORE_PACING + 3) <= Duration::from_millis(4));
            assert!(pacing_after(LOSSES_BEFORE_PACING + 60) <= MOST_PACING);
            assert!(pacing_after(u32::MAX) <= MOST_PACING);
        }
        // and the draws spread under the bound rather than land on it
        let draws: std::collections::BTreeSet<Duration> =
            (0..64).map(|_| pacing_after(LOSSES_BEFORE_PACING + 20)).collect();
        assert!(draws.len() > 8, "64 draws under the most came to {draws:?}");
    }
}
