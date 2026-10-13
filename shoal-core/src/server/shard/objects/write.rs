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

use super::place::Placement;
use super::read::StripeRead;
use super::{not_found, Meta, MetaAnswer, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::server::objects::executor::answer_of;
use crate::server::objects::put::{self as pipeline, PutPlan, PIECE_BYTES};
use crate::server::objects::{Holder, ObjectStore};
use crate::shared::checksum;
use crate::shared::objects::{
    Label, ObjectEntry, PendingBytes, PendingUnit, StripeAnswer, SMALL_WRITE_THRESHOLD,
};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::{ObjectAnswer, ObjectOpKind, ObjectOutcome};
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece, UnitBytes};
use crate::shared::stripes::{
    commit_bundle, hidden_from, is_hole, stripes_of, write_tag, ChunkId, StripeRow, Touch,
    WriteCommit,
};
use crate::shared::traits::QuerySupport;

/// How many times a commit whose outcome is unknown is sent again, byte for byte, before the
/// write is answered as unknown
const RESENDS: usize = 3;

/// What one stripe's write came to
pub(super) enum StripeStep {
    /// Its commit applied, this try's or an earlier try's of the same write
    Committed,
    /// Its commit was refused, or its row moved: read the row again and try again
    Retry,
    /// The row is fenced past the epoch read: a truncate is committing, or died having fenced
    Advance,
    /// The row is stamped past the epoch read: the entry has to be read again
    Reread,
    /// It cannot be written now, and this is the answer
    Refused(ObjectAnswer),
    /// The machinery failed under it
    Failed(ErrorCode, String),
}

/// One stripe's part of a write
struct StripePart<'a> {
    /// The stripe
    stripe: u64,
    /// Where in the stripe the part starts
    at: u64,
    /// Its bytes
    data: &'a [u8],
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
        // the chunks of the object as it will be, at its put's label, written as a put's are
        let geometry = entry.geometry;
        let length = bytes.len() as u64;
        let stripes = length.div_ceil(u64::from(geometry.stripe_bytes));
        let mut placed = Vec::new();
        for stripe in 0..stripes {
            match placement.stripe(entry.object, stripe) {
                Ok(holders) => placed.push(holders),
                Err(why) => return Ok(Some(self.refused(ObjectOutcome::NotReady, why))),
            }
        }
        let plan = PutPlan {
            op: self.op.id,
            stripe_bytes: u64::from(geometry.stripe_bytes),
            unit_bytes: geometry.unit_bytes,
            length,
            stripes: placed,
            needed: placement.needed(),
            straggler_wait: self.timings.straggler_wait.duration(),
            deadline: self.meta.wait,
            inflight: self.timings.inflight_stripes,
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
        // the stripes some holders missed say so, before the entry names them
        for (stripe, positions) in &done.missed {
            let start = stripe * u64::from(geometry.stripe_bytes);
            let held = length.saturating_sub(start).min(u64::from(geometry.stripe_bytes));
            let mut stripe_row = StripeRow::put(self.consumer, entry.object, *stripe, placement.width(), held);
            for position in positions {
                if let Some(label) = stripe_row.labels.get_mut(*position) {
                    *label = label.marked();
                }
            }
            if let StripeAnswer::Failed(code, why) = self.meta.commit_stripe(stripe_row, None, None).await {
                return Err((code, why));
            }
        }
        // the entry, its bytes now its stripes'
        let mut next = entry.clone();
        next.size = length;
        next.put_length = length;
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
        let read_epoch = entry.epoch;
        let mut rounds = 0;
        while !left.is_empty() {
            rounds += 1;
            if rounds > MAX_TRIES * 4 {
                return Ok((
                    self.refused(
                        ObjectOutcome::Busy,
                        format!("the stripes of {} moved under this write too often; try it again", op.path),
                    ),
                    None,
                ));
            }
            // the rows of every stripe still to write, at Quorum, through their leaders
            let rows = self.meta.stripe_rows(self.consumer, entry.object, &left).await?;
            let mut next_left = Vec::new();
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
                let step = self
                    .write_stripe(&placement, &entry, rows.get(&stripe), &part, attempt, nonce)
                    .await;
                match step {
                    StripeStep::Committed => {}
                    StripeStep::Retry => {
                        tries.insert(stripe, attempt + 1);
                        next_left.push(stripe);
                    }
                    StripeStep::Advance => {
                        self.advance(&entry).await?;
                        entry = self.reread_entry().await?;
                        next_left.push(stripe);
                    }
                    StripeStep::Reread => {
                        entry = self.reread_entry().await?;
                        next_left.push(stripe);
                    }
                    StripeStep::Refused(answer) => return Ok((answer, None)),
                    StripeStep::Failed(code, why) => return Err((code, why)),
                }
            }
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
    /// * `placement` - Where the stripe is
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `part` - The stripe's part of the write
    /// * `attempt` - Which try of this stripe this is
    /// * `nonce` - This task's own
    async fn write_stripe(
        &self,
        placement: &Placement,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        part: &StripePart<'_>,
        attempt: u32,
        nonce: u64,
    ) -> StripeStep {
        let store = match self.store() {
            Ok(store) => store.clone(),
            Err(refused) => return StripeStep::Refused(refused),
        };
        let holders = match placement.stripe(entry.object, part.stripe) {
            Ok(holders) => holders,
            Err(why) => return StripeStep::Refused(self.refused(ObjectOutcome::NotReady, why)),
        };
        let stripe_bytes = u64::from(entry.geometry.stripe_bytes);
        let unit = u64::from(entry.geometry.unit_bytes);
        let start = part.stripe * stripe_bytes;
        // the row read, or a stripe its put wrote as it would be if it had one
        let put_length = entry.put_length.saturating_sub(start).min(stripe_bytes);
        let base = row
            .cloned()
            .unwrap_or_else(|| StripeRow::put(self.consumer, entry.object, part.stripe, holders.len(), put_length));
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
                    Err(StripeRead::Newer) => return StripeStep::Retry,
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
                        Err(StripeRead::Newer) => return StripeStep::Retry,
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
        // a small write rides inside its commit, where the pool takes the path
        let threshold = placement.small_write_threshold();
        if !hole && !base.tombstone && (part.data.len() as u64) < threshold {
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
                    .small_write(&store, placement, &holders, entry, row.is_some(), &base, label, after, length, part.stripe, attempt)
                    .await;
            }
        }
        // stage on every holder: the whole chunk where it is all new, else the units it changes
        let unit_count = length.div_ceil(unit);
        let whole = hole || (0..unit_count).all(|index| units.contains_key(&(index as u32)));
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
        let mut answers = Vec::with_capacity(holders.len());
        for (position, (chunk, holder)) in holders.iter().enumerate() {
            let answer = if let Some(pieces) = &pieces {
                stage_whole(&store, *holder, *chunk, label, stripe_bytes, entry.geometry.unit_bytes, length, pieces, nonce)
                    .await
            } else {
                let request = ChunkRequest::StageUnits {
                    chunk: *chunk,
                    makes: label,
                    expects: base.current_labels(position),
                    length,
                    unit_bytes: entry.geometry.unit_bytes,
                    units: changed.clone(),
                };
                store.send(*holder, request).await
            };
            answers.push(answer);
        }
        let replies = gather(answers, self.meta.wait).await;
        let held_positions: Vec<bool> = replies.iter().map(|reply| matches!(reply, Some(ChunkReply::Done))).collect();
        let count = held_positions.iter().filter(|held| **held).count();
        // fewer than k + f staged: nothing of it is proposed, so its stages are discarded now
        if count < placement.needed() {
            discard(&store, &holders, &held_positions, label);
            // holders that cannot make what the row named may be past it: the row moved under
            // this write, which tries again against it as it is now (item 239)
            if self.row_moved(entry.object, part.stripe, row).await {
                return StripeStep::Retry;
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
            glommio::timer::sleep(hold).await;
        }
        crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Answered);
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
        });
        let committed = self
            .propose(next, row.map(|row| row.sequence), commit_bundle(&self.op.id, part.stripe, attempt))
            .await;
        match committed {
            Proposed::Committed => {
                crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Committed);
                // the holders that staged are told; one that never hears asks the row
                apply(&store, &holders, &held_positions, label);
                StripeStep::Committed
            }
            Proposed::CommittedElsewhere => {
                discard(&store, &holders, &held_positions, label);
                StripeStep::Committed
            }
            Proposed::Refused => {
                discard(&store, &holders, &held_positions, label);
                StripeStep::Retry
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
        let mut value = self.read_stripe(placement, entry, row, stripe, unit_start, len).await?;
        value.resize(unit as usize, 0);
        Ok(value)
    }

    /// A small write in its commit: every holder confirms its base, one commit carries the bytes
    ///
    /// # Arguments
    ///
    /// * `store` - The node's object store
    /// * `placement` - Where the stripe is
    /// * `holders` - Its chunks and holders, by position
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
        holders: &[(ChunkId, Holder)],
        entry: &ObjectEntry,
        exists: bool,
        base: &StripeRow,
        label: Label,
        pending: PendingBytes,
        length: u64,
        stripe: u64,
        attempt: u32,
    ) -> StripeStep {
        // every position's holder confirms, in this write's round, the chunk the bytes go over (Q16)
        let mut asks = Vec::with_capacity(holders.len());
        for (position, (chunk, holder)) in holders.iter().enumerate() {
            let request = ChunkRequest::Confirm {
                chunk: *chunk,
                labels: base.current_labels(position),
            };
            asks.push(store.send(*holder, request).await);
        }
        let replies = gather(asks, self.meta.wait).await;
        let confirmed: Vec<bool> = replies
            .iter()
            .enumerate()
            .map(|(position, reply)| {
                matches!(reply, Some(ChunkReply::Held { label: Some(held) }) if base.makes(position, *held))
            })
            .collect();
        let count = confirmed.iter().filter(|held| **held).count();
        if count < placement.needed() {
            // holders past the base it read: the row moved under this write (item 239)
            if self.row_moved(entry.object, stripe, exists.then_some(base)).await {
                return StripeStep::Retry;
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
            glommio::timer::sleep(hold).await;
        }
        crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Answered);
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
        });
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
                    holders.to_vec(),
                    next,
                    placement.needed(),
                    entry.geometry.unit_bytes,
                    self.timings.clear_wait.duration(),
                );
                StripeStep::Committed
            }
            Proposed::CommittedElsewhere => StripeStep::Committed,
            Proposed::Refused => StripeStep::Retry,
            Proposed::Failed(code, why) => StripeStep::Failed(code, why),
        }
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
    async fn row_moved(&self, object: u128, stripe: u64, read: Option<&StripeRow>) -> bool {
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
    async fn propose(&self, next: StripeRow, read: Option<u64>, bundle: uuid::Uuid) -> Proposed {
        for _ in 0..RESENDS {
            match self.meta.commit_stripe(next.clone(), read, Some(bundle)).await {
                StripeAnswer::Written => return Proposed::Committed,
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
enum Proposed {
    /// This task's commit applied
    Committed,
    /// A try of this write with another payload committed, another coordinator's
    CommittedElsewhere,
    /// The row moved, or this try's identity was refused before
    Refused,
    /// The machinery failed, or the outcome stayed unknown
    Failed(ErrorCode, String),
}

/// Units as a stage or a fold sends them, each with the checksum it crosses with
///
/// # Arguments
///
/// * `units` - Each unit's index and bytes
async fn checked_units<'a>(units: impl Iterator<Item = (u32, &'a Vec<u8>)>) -> Vec<UnitBytes> {
    let mut checked = Vec::new();
    for (index, bytes) in units {
        checked.push(UnitBytes {
            index,
            crc: crate::shared::checksum::crc64(bytes),
            bytes: bytes.clone(),
        });
        glommio::yield_if_needed().await;
    }
    checked
}

/// A whole chunk cut into the pieces a stage sends, each with its units' checksums
struct WholePieces {
    /// Each piece: where it starts in the chunk, its bytes, and each unit's CRC-64/NVME
    pieces: Vec<(u64, Vec<u8>, Vec<u64>)>,
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
            pieces.push(((index * per_piece) as u64, piece.to_vec(), crcs));
        }
        WholePieces { pieces }
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
#[allow(clippy::too_many_arguments)]
async fn stage_whole(
    store: &ObjectStore,
    holder: Holder,
    chunk: ChunkId,
    label: Label,
    chunk_bytes: u64,
    unit_bytes: u32,
    length: u64,
    whole: &WholePieces,
    nonce: u64,
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
async fn gather(
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
fn apply(store: &ObjectStore, holders: &[(ChunkId, Holder)], held: &[bool], label: Label) {
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
fn discard(store: &ObjectStore, holders: &[(ChunkId, Holder)], held: &[bool], label: Label) {
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
    pub(super) fn meet_pending(&self, placement: &Placement, entry: &ObjectEntry, row: &StripeRow) {
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
        let Ok(holders) = placement.stripe(entry.object, row.key().2) else {
            return;
        };
        spawn_fold_and_clear(
            self.meta.clone(),
            store.clone(),
            holders,
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
/// * `holders` - The stripe's chunks and holders
/// * `row` - The row holding the pending bytes
/// * `needed` - `k + f`
/// * `unit_bytes` - A unit's size
/// * `wait` - How long every holder is waited for
pub(super) fn spawn_fold_and_clear<D: ShoalDatabase>(
    meta: Meta<D>,
    store: ObjectStore,
    holders: Vec<(ChunkId, Holder)>,
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
            // every holder is given the bytes to fold over the chunk it holds
            let mut asks = Vec::with_capacity(holders.len());
            for (position, (chunk, holder)) in holders.iter().enumerate() {
                let request = ChunkRequest::Fold {
                    chunk: *chunk,
                    makes: pending.label,
                    sources: pending.sources(position),
                    length: row.length,
                    unit_bytes,
                    units: folded.clone(),
                };
                asks.push(store.send(*holder, request).await);
            }
            let replies = gather(asks, wait).await;
            let holding: Vec<bool> = replies.iter().map(|reply| matches!(reply, Some(ChunkReply::Done))).collect();
            // once k + f hold it durably, the row is cleared on the sequence it is at
            if holding.iter().filter(|held| **held).count() >= needed {
                let next = row.clear(&holding);
                let _ = meta.commit_stripe(next, Some(row.sequence), None).await;
            }
        }
        meta.client.end_clear(&key);
    })
    .detach();
}
