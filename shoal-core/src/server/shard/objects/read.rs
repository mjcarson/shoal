//! A read of a range of an object, or a stat of it
//!
//! An inline object is answered from its entry. A striped one is S9's read
//! ([S9](../../../../../docs/src/object-storage/read-path.md)):
//!
//! 1. the entry, at the level the read names;
//! 2. one get of every stripe the range falls in, at `Quorum`, after the entry: a row read at
//!    `One` could be older than the entry, which no state of the object ever was; a row stamped
//!    past the entry's epoch means a truncate committed between them, and the entry is read again;
//! 3. each stripe's bytes from a holder the row calls current, a holder on this node first, at
//!    the label the row names - or at a label its pending bytes fold from, with them laid over. On
//!    a pool of copies on disks, the holder with the fewest reads queued once a remote holder's
//!    carries the node's `disk_read_margin`, four unless the node sets it null (F97, F101);
//! 4. a hole, and bytes a floor hides, read as zeros without asking any holder.
//!
//! The range is answered as the parts it was read in: each stripe's bytes, or each data chunk's
//! part of an erasure coded one, where the holder's answer left them, and a hole as a run of zeros.
//! Its checksum is folded from theirs, a step at a time, and the connection's writer frames it from
//! them, so a striped read copies none of its bytes on its table shard
//! ([F109](../../../../../docs/src/features/reads-from-stripe-buffers.md)). A stripe answers
//! exactly the bytes asked of it, or the read fails: one of another length would move every byte
//! after it.
//!
//! A holder that answers with a label newer than the row's means a write committed since the row
//! was read: the reader reads its row again, forward, and asks again, and never takes a chunk
//! ahead of its row ([P10](../../../../../docs/src/object-storage/contract.md)). One older, or one
//! that holds nothing, is stale to this reader, who asks another.
//!
//! A read answers at most `storage.object_timings.max_read` bytes; a client asks for the rest from where
//! it stopped ([S12](../../../../../docs/src/object-storage/wire-and-client.md)).

use std::collections::BTreeMap;

use tracing::{event, Level};

use super::place::Placement;
use super::{not_found, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::server::objects::RangeParts;
use crate::shared::checksum;
use crate::shared::objects::ObjectEntry;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::ObjectOutcome;
use crate::shared::protocol::peer::chunk::{ChunkBytes, ChunkReply, ChunkRequest, StoreCode};
use crate::shared::protocol::read::ReadLevel;
use crate::shared::stripes::{shown, stripes_of, StripeRow};
use crate::shared::traits::QuerySupport;

/// Why one stripe's range could not be read
pub(super) enum StripeRead {
    /// A holder holds a label the row does not name and not an older one: the row has to be
    /// read again, and is behind its chunks if it reads the same (F93)
    Ahead,
    /// No holder the row calls current answered with the bytes
    Failed(String),
}

/// Why a range of a striped object could not be read
pub(super) enum RangeFailure {
    /// The machinery failed under it, to be answered as an error
    Code(ErrorCode, String),
    /// A stripe's row is behind its chunks, refused by name and never retried (F93)
    Behind(String),
    /// A stripe an operator declared lost, answered its loss by name (F93)
    Lost(String),
}

impl From<(ErrorCode, String)> for RangeFailure {
    /// An error the metadata answered
    ///
    /// # Arguments
    ///
    /// * `failure` - The code and why
    fn from(failure: (ErrorCode, String)) -> Self {
        RangeFailure::Code(failure.0, failure.1)
    }
}

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Read a range of the object, or stat it
    ///
    /// # Arguments
    ///
    /// * `bytes` - Whether to answer with the range's bytes, which a stat does not
    pub(super) async fn read(&self, bytes: bool) -> Outcome {
        let op = &self.op;
        // the path's entry, at the level the operation named or a strong read
        let key = self.key();
        let level = op.level.unwrap_or(DEFAULT_LEVEL);
        let row = self.meta.row(key, Some(level)).await?;
        let Some(entry) = row
            .as_ref()
            .and_then(|row| row.entry(&op.path))
            .filter(|entry| entry.state.live)
            .cloned()
        else {
            return Ok((not_found(op), None));
        };
        let mut answer = self.done(&entry, 0);
        answer.kind = op.kind;
        // a stat says what the object is, and nothing of its bytes; a striped object's checksum
        // is not kept anywhere a stat can read, so it says none
        if !bytes {
            answer.attributes = Some(entry.attributes.clone());
            answer.checksum = entry.inline.as_deref().map_or(0, checksum::crc64);
            return Ok((answer, None));
        }
        // the range asked for, cut to the object and to what one read answers
        let start = op.offset.min(entry.size);
        let end = op
            .offset
            .saturating_add(op.length)
            .min(entry.size)
            .min(start.saturating_add(self.timings.max_read as u64));
        // a test can hold a striped read here, between its entry and its stripes, until it is let
        // go or the hold runs out
        if entry.inline.is_none() {
            if let Some(hold) = crate::server::objects::faults::read_hold() {
                let until = std::time::Instant::now() + hold;
                while std::time::Instant::now() < until && crate::server::objects::faults::read_hold().is_some() {
                    glommio::timer::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        }
        let range = match &entry.inline {
            Some(inline) => RangeParts::from(inline[start as usize..end as usize].to_vec()),
            None => match self.read_range(entry.clone(), start, end).await {
                Ok(range) => range,
                // a row behind its chunks is refused by name, never served mixed and never
                // answered as something to try again (F93, item 256)
                Err(RangeFailure::Behind(why)) => {
                    return Ok((self.refused(ObjectOutcome::Behind, why), None));
                }
                Err(RangeFailure::Lost(why)) => {
                    return Ok((self.refused(ObjectOutcome::Lost, why), None));
                }
                Err(RangeFailure::Code(code, why)) => {
                    // a reader that outlived its object's grace is told so by name, never given
                    // another object's bytes (M20)
                    if let Some(gone) = self.gone(key, &entry).await {
                        return Ok((gone, None));
                    }
                    return Err((code, why));
                }
            },
        };
        // its checksum folded from its parts', a step at a time on this shard (F109)
        answer.offset = start;
        answer.length = range.len();
        answer.checksum = range.crc64().await;
        Ok((answer, Some(range)))
    }

    /// The answer to a read whose object is no longer current at its path, read strongly: it was
    /// replaced or deleted while it was read, and its bytes may be reclaimed (M20)
    ///
    /// None while the object is still current, or the entry cannot be read, so the read's own
    /// failure stands.
    ///
    /// # Arguments
    ///
    /// * `key` - The path's row's key
    /// * `entry` - The entry the read began from
    async fn gone(&self, key: u64, entry: &ObjectEntry) -> Option<crate::shared::protocol::object::ObjectAnswer> {
        let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await.ok()?;
        let current = row
            .as_ref()
            .and_then(|row| row.entry(&self.op.path))
            .is_some_and(|now| now.state.live && now.object == entry.object);
        (!current).then(|| {
            self.refused(
                crate::shared::protocol::object::ObjectOutcome::Gone,
                format!(
                    "{}'s object {:032x} was replaced or deleted while it was read, and its bytes are reclaimed",
                    self.op.path, entry.object
                ),
            )
        })
    }

    /// Read a range of a striped object, every stripe of it from a current holder or as zeros,
    /// as the parts it was read in (F109)
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry as read
    /// * `start` - Where the range starts
    /// * `end` - Where it ends
    pub(super) async fn read_range(
        &self,
        mut entry: ObjectEntry,
        start: u64,
        end: u64,
    ) -> Result<RangeParts, RangeFailure> {
        // nothing to read is read
        if start >= end {
            return Ok(RangeParts::new());
        }
        let placement = Placement::new(self.map.clone(), &self.pool_name, &self.pool, self.consumer, false)
            .map_err(|(_, why)| RangeFailure::Code(ErrorCode::Unavailable, why))?;
        let stripes: Vec<u64> = stripes_of(entry.geometry.stripe_bytes, start, end - start).collect();
        // the rows a holder was found ahead of, which read again unchanged are behind their
        // chunks rather than moving under this read
        let mut ahead_of: Option<BTreeMap<u64, StripeRow>> = None;
        for _ in 0..MAX_TRIES {
            // every stripe's row, at Quorum, after the entry
            let rows = self.meta.stripe_rows(self.consumer, entry.object, &stripes).await?;
            // a row stamped past the entry's epoch: a truncate committed between them
            if rows.values().any(|row| row.stamp > entry.epoch) {
                entry = self.reread_entry().await?;
                continue;
            }
            // a stripe an operator declared lost reads as nothing but its loss (F93)
            if let Some((stripe, _)) = rows.iter().find(|(_, row)| row.lost) {
                return Err(RangeFailure::Lost(format!(
                    "stripe {stripe} of {} was declared lost by an operator's ResolveStripe",
                    self.op.path
                )));
            }
            match self.read_stripes(&placement, &entry, &rows, &stripes, start, end).await {
                Ok(bytes) => return Ok(bytes),
                // a holder ahead of its row: read the rows again. A label is applied only after
                // its commit, so the rows a second strong read gives back unchanged are behind
                // their chunks, which only a `force_recover` that lost a write leaves: refused by
                // name, the chunks kept for an operator's word (F93, item 256)
                Err(StripeRead::Ahead) => {
                    if ahead_of.as_ref() == Some(&rows) {
                        return Err(RangeFailure::Behind(format!(
                            "a holder of {} applied a write its stripes' rows do not name: the rows \
                             are behind their chunks, which a ResolveStripe settles; the rows are at \
                             {:?}",
                            self.op.path,
                            stripes
                                .iter()
                                .map(|stripe| (*stripe, rows.get(stripe).map_or(0, |row| row.sequence)))
                                .collect::<Vec<_>>()
                        )));
                    }
                    ahead_of = Some(rows);
                }
                Err(StripeRead::Failed(why)) => {
                    return Err(RangeFailure::Code(ErrorCode::Unavailable, why))
                }
            }
        }
        Err(RangeFailure::Code(
            ErrorCode::Shedding,
            format!(
                "the stripes of {} moved under this read {MAX_TRIES} times; try it again",
                self.op.path
            ),
        ))
    }

    /// Read the range from each of its stripes, given their rows, each stripe's parts kept as its
    /// holders' answers left them and its zeros as a run (F109)
    ///
    /// # Arguments
    ///
    /// * `placement` - Where the stripes are
    /// * `entry` - The object's entry
    /// * `rows` - The stripes' rows, by stripe
    /// * `stripes` - The stripes
    /// * `start` - Where the range starts
    /// * `end` - Where it ends
    async fn read_stripes(
        &self,
        placement: &Placement,
        entry: &ObjectEntry,
        rows: &BTreeMap<u64, StripeRow>,
        stripes: &[u64],
        start: u64,
        end: u64,
    ) -> Result<RangeParts, StripeRead> {
        let mut out = RangeParts::new();
        for stripe in stripes {
            let row = rows.get(stripe);
            let Some(shown) = shown(entry, row, *stripe) else {
                break;
            };
            // the part of the range in this stripe, and the part of it a chunk holds
            let from = start.max(shown.start);
            let to = end.min(shown.end);
            let read_to = to.min(shown.read_to).max(from);
            if read_to > from {
                let bytes = self
                    .read_stripe(placement, entry, row, *stripe, from - shown.start, read_to - from)
                    .await?;
                // a stripe answers exactly what was asked of it: bytes of another length would move
                // every byte after them, under a checksum taken of them as they came
                if bytes.len() != read_to - from {
                    return Err(StripeRead::Failed(format!(
                        "stripe {stripe} of {} answered {} bytes where {} were asked",
                        self.op.path,
                        bytes.len(),
                        read_to - from
                    )));
                }
                out.append(bytes);
            }
            // pending bytes a writer left in the row are folded and cleared by whoever meets them
            if let Some(row) = row {
                self.meet_pending(placement, entry, row).await;
            }
            // a hole, and what a floor hides, are zeros
            out.push_zeros(to - read_to);
        }
        Ok(out)
    }

    /// Read part of one stripe's chunk from a holder the row calls current
    ///
    /// The holders are where the stripe's placement group's state as this shard last read it
    /// names them; when none of them answers with the bytes, the state is read again from the
    /// group, linearizably, and a group that moved since is asked where it is now (F88). The part
    /// is the holder's answer where it left it, or an erasure coded stripe's data chunks' parts
    /// (F109).
    ///
    /// # Arguments
    ///
    /// * `placement` - Where the stripe is
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe
    /// * `offset` - Where in the chunk the part starts
    /// * `len` - How long it is
    pub(super) async fn read_stripe(
        &self,
        placement: &Placement,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        stripe: u64,
        offset: u64,
        len: u64,
    ) -> Result<RangeParts, StripeRead> {
        // an erasure coded stripe's range is read from the data chunks it falls in, decoded where
        // they cannot answer (F90)
        if entry.geometry.is_erasure() {
            return self.read_erasure(placement, entry, row, stripe, offset, len).await;
        }
        let key = placement.pg_of(entry.object, stripe);
        let known = self.meta.placement_for(placement, key).await;
        let set = placement
            .stripe_set(entry.object, stripe, &known)
            .map_err(StripeRead::Failed)?;
        let read = match self
            .read_holders(set.current(), entry, row, stripe, offset, len, known.epoch)
            .await
        {
            Err(StripeRead::Failed(why)) => {
                // the group's state as it is now, which a move may have changed under this shard
                let Ok(mut read) = self.meta.read_placements(&[key]).await else {
                    return Err(StripeRead::Failed(why));
                };
                let now = read.remove(&key).unwrap_or_default();
                if now == known {
                    return Err(StripeRead::Failed(why));
                }
                let set = placement
                    .stripe_set(entry.object, stripe, &now)
                    .map_err(StripeRead::Failed)?;
                self.read_holders(set.current(), entry, row, stripe, offset, len, now.epoch).await
            }
            other => other,
        };
        // one holder's answer is the stripe's one part
        read.map(RangeParts::from)
    }

    /// Read part of one stripe's chunk from whichever of some holders the row calls current
    ///
    /// # Arguments
    ///
    /// * `holders` - Each position's chunk and holder
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe
    /// * `offset` - Where in the chunk the part starts
    /// * `len` - How long it is
    /// * `epoch` - The placement group's epoch the holders were placed by
    #[allow(clippy::too_many_arguments)]
    async fn read_holders(
        &self,
        holders: &[(crate::shared::stripes::ChunkId, crate::server::objects::Holder)],
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        stripe: u64,
        offset: u64,
        len: u64,
        epoch: u64,
    ) -> Result<ChunkBytes, StripeRead> {
        let Some(store) = &self.store else {
            return Err(StripeRead::Failed("this node runs no object executor to read stripes with".to_string()));
        };
        // a stripe its put wrote and nothing changed has no row: every position at the put's label
        let start = stripe * u64::from(entry.geometry.stripe_bytes);
        let row = row.cloned().unwrap_or_else(|| {
            let length = entry
                .put_length
                .saturating_sub(start)
                .min(u64::from(entry.geometry.stripe_bytes));
            StripeRow::put(self.consumer, entry.object, stripe, holders.len(), length, self.key())
        });
        // the positions the row calls current
        let mut order: Vec<usize> = (0..holders.len()).filter(|pos| row.current(*pos)).collect();
        match self.timings.disk_read_margin.filter(|_| self.pool.is_rotational()) {
            Some(margin) => {
                // a disk answers in the time of its queue, so the holder with the fewest reads
                // queued once a remote holder's carries the margin, a copy on this node on a tie and
                // the others in turn: a disk whose node leads more of the pool's groups no longer
                // serves every read they lead (F97), and a read leaves its local disk only for a
                // queue shorter by more than the link it crosses costs (F101)
                order_by_queue(
                    &mut order,
                    margin,
                    store.next_turn(),
                    holders.len(),
                    |pos| store.queue_of(&holders[pos].1),
                    |pos| store.is_local(&holders[pos].1),
                );
            }
            None => {
                // a holder on this node first, on SSDs or where the node's margin is null: over the
                // network a local copy is worth more than an idle remote one (F97, F101)
                order.sort_by_key(|pos| !store.is_local(&holders[*pos].1));
            }
        }
        let mut why = Vec::new();
        for pos in order {
            let (chunk, holder) = holders[pos];
            let labels = row.current_labels(pos);
            let reply = store
                .read_from(
                    holder,
                    ChunkRequest::Read {
                        chunk,
                        offset,
                        len,
                        labels,
                    },
                )
                .await;
            match reply {
                // the label the row names
                ChunkReply::Bytes { label, bytes } if label == row.named(pos) => return Ok(bytes),
                // a label the row's pending bytes fold from, with them laid over
                ChunkReply::Bytes { label, mut bytes } if row.makes(pos, label) => {
                    lay_over(&row, offset, &mut bytes, u64::from(entry.geometry.unit_bytes));
                    return Ok(bytes);
                }
                // a holder ahead of the row: the row is read again, never the chunk taken
                ChunkReply::Held { label: Some(held) } if held.ahead_of(row.named(pos)) => {
                    return Err(StripeRead::Ahead)
                }
                // a unit that fails its checksum is reported, not only refused: its position is
                // marked missed, behind the read, for recovery to rebuild from a copy that
                // verifies, and another holder is asked (F89)
                ChunkReply::Refused {
                    code: StoreCode::Corrupt,
                    message,
                } => {
                    self.report_corrupt(entry, &row, stripe, pos, epoch);
                    why.push(format!("{holder:?}: corrupt: {message}"));
                }
                // stale to this reader, or nothing at all: another holder
                other => why.push(format!("{holder:?}: {other:?}")),
            }
        }
        Err(StripeRead::Failed(format!(
            "no current holder of stripe {stripe} of {} answered, its row at {} naming {:?}: {}",
            self.op.path,
            row.sequence,
            (0..holders.len()).map(|pos| row.current_labels(pos)).collect::<Vec<_>>(),
            why.join("; ")
        )))
    }

    /// Mark a position whose holder answered a unit failing its checksum, on a task of its own so
    /// the read goes on to another holder at once (F89)
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, or its put's as it would be
    /// * `stripe` - The stripe
    /// * `position` - The position whose holder failed
    /// * `epoch` - The placement group's epoch the holder was placed by
    pub(super) fn report_corrupt(&self, entry: &ObjectEntry, row: &StripeRow, stripe: u64, position: usize, epoch: u64) {
        self.meta
            .client
            .count_scrub(&self.pool_name, |figures| figures.corrupt += 1);
        let meta = self.meta.clone();
        let (consumer, object, width, length) = (self.consumer, entry.object, row.labels.len(), row.length);
        let owner = self.key();
        glommio::spawn_local(async move {
            match meta
                .mark_missed(consumer, object, stripe, width, length, owner, &[position], epoch)
                .await
            {
                Ok(_) => event!(Level::WARN, msg = "a read found a stripe chunk corrupt; its position is marked for a rebuild", object = %format!("{object:032x}"), stripe, position),
                Err((code, why)) => event!(Level::WARN, msg = "a read found a stripe chunk corrupt and could not mark it; a scrub will", object = %format!("{object:032x}"), stripe, position, %code, why),
            }
        })
        .detach();
    }

    /// The object's entry read again, strongly, after a row showed a truncate it had not seen
    pub(super) async fn reread_entry(&self) -> Result<ObjectEntry, (ErrorCode, String)> {
        self.meta
            .row(self.key(), Some(ReadLevel::Quorum))
            .await?
            .and_then(|row| row.entry(&self.op.path).cloned())
            .filter(|entry| entry.state.live)
            .ok_or_else(|| (ErrorCode::Unavailable, format!("{} went while it was read", self.op.path)))
    }
}

/// Order the holders a read of a pool of copies on disks asks: by the reads queued on each one's
/// disk once a remote holder's carries the margin, a copy on this node first on a tie, and remote
/// holders that tie in turn (F97, F101)
///
/// A remote read crosses a link that a local one does not, so a read leaves its local disk only for
/// a queue shorter by more than the margin. A margin of zero is F97's choice by queue alone.
///
/// # Arguments
///
/// * `order` - The positions the row calls current, put in the order to ask them in
/// * `margin` - How many more reads a local copy's queue holds before a remote holder goes first
/// * `turn` - This read's turn, which rotates the remote holders that tie
/// * `width` - The positions of the stripe
/// * `queued` - The reads queued on a position's holder
/// * `local` - Whether a position's holder is on this node
fn order_by_queue(
    order: &mut [usize],
    margin: u32,
    turn: usize,
    width: usize,
    queued: impl Fn(usize) -> u64,
    local: impl Fn(usize) -> bool,
) {
    let width = width.max(1);
    order.sort_by_key(|pos| {
        // a remote holder's queue carries the margin, a local copy's does not
        let here = local(*pos);
        let carried = if here { 0 } else { u64::from(margin) };
        (queued(*pos).saturating_add(carried), !here, (pos + width - turn % width) % width)
    });
}

/// Lay a row's pending bytes over a range of a chunk read at a label they fold from
///
/// # Arguments
///
/// * `row` - The stripe's row
/// * `offset` - Where in the chunk the range starts
/// * `bytes` - The range
/// * `unit_bytes` - The bytes of a unit
pub(super) fn lay_over(row: &StripeRow, offset: u64, bytes: &mut [u8], unit_bytes: u64) {
    let Some(pending) = &row.pending else {
        return;
    };
    let end = offset + bytes.len() as u64;
    // each pending unit, where it falls in the range
    for unit in &pending.units {
        let unit_start = u64::from(unit.index) * unit_bytes;
        let unit_end = unit_start + unit.bytes.len() as u64;
        let from = unit_start.max(offset);
        let to = unit_end.min(end);
        if from >= to {
            continue;
        }
        bytes[(from - offset) as usize..(to - offset) as usize]
            .copy_from_slice(&unit.bytes[(from - unit_start) as usize..(to - unit_start) as usize]);
    }
}

#[cfg(test)]
mod tests {
    use super::order_by_queue;

    /// A read of a pool of copies on disks leaves its local copy only for a remote holder whose
    /// queue is shorter by more than the margin, a tie goes to the local copy, and remote holders
    /// that tie are taken in turn; a margin of zero is F97's choice by queue alone (F101)
    #[test]
    fn a_read_leaves_its_local_disk_only_past_the_margin() {
        // position zero is this node's copy, one and two other nodes'
        let order = |queued: [u64; 3], margin: u32, turn: usize| {
            let mut order = vec![0, 1, 2];
            order_by_queue(&mut order, margin, turn, 3, |pos| queued[pos], |pos| pos == 0);
            order
        };
        // idle disks: the local copy first, then the others in turn
        assert_eq!(order([0, 0, 0], 4, 0), vec![0, 1, 2]);
        assert_eq!(order([0, 0, 0], 4, 2), vec![0, 2, 1]);
        // a local queue as deep as the margin stays local
        assert_eq!(order([4, 0, 0], 4, 0), vec![0, 1, 2]);
        // one past it goes first to the shorter remote queue, then to the local copy on a tie
        assert_eq!(order([5, 0, 1], 4, 0), vec![1, 0, 2]);
        // a margin of zero leaves the local copy for any shorter queue, as F97 did
        assert_eq!(order([1, 0, 0], 0, 0), vec![1, 2, 0]);
    }
}
