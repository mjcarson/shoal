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
//!    the label the row names - or at a label its pending bytes fold from, with them laid over;
//! 4. a hole, and bytes a floor hides, read as zeros without asking any holder.
//!
//! A holder that answers with a label newer than the row's means a write committed since the row
//! was read: the reader reads its row again, forward, and asks again, and never takes a chunk
//! ahead of its row ([P10](../../../../../docs/src/object-storage/contract.md)). One older, or one
//! that holds nothing, is stale to this reader, who asks another.
//!
//! A read answers at most `storage.object_timings.max_read` bytes; a client asks for the rest from where
//! it stopped ([S12](../../../../../docs/src/object-storage/wire-and-client.md)).

use std::collections::BTreeMap;

use super::place::Placement;
use super::{not_found, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::shared::checksum;
use crate::shared::objects::{Label, ObjectEntry};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest};
use crate::shared::protocol::read::ReadLevel;
use crate::shared::stripes::{shown, stripes_of, StripeRow};
use crate::shared::traits::QuerySupport;

/// Why one stripe's range could not be read
pub(super) enum StripeRead {
    /// A holder holds a label newer than the row read: the row has to be read again
    Newer,
    /// No holder the row calls current answered with the bytes
    Failed(String),
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
        let range = match &entry.inline {
            Some(inline) => inline[start as usize..end as usize].to_vec(),
            None => match self.read_range(entry, start, end).await {
                Ok(range) => range,
                Err(failure) => return Err(failure),
            },
        };
        answer.offset = start;
        answer.length = range.len() as u64;
        answer.checksum = checksum::crc64(&range);
        Ok((answer, Some(range)))
    }

    /// Read a range of a striped object, every stripe of it from a current holder or as zeros
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry as read
    /// * `start` - Where the range starts
    /// * `end` - Where it ends
    async fn read_range(&self, mut entry: ObjectEntry, start: u64, end: u64) -> Result<Vec<u8>, (ErrorCode, String)> {
        // nothing to read is read
        if start >= end {
            return Ok(Vec::new());
        }
        let placement = Placement::new(self.map.clone(), &self.pool_name, &self.pool, self.consumer, false)
            .map_err(|(_, why)| (ErrorCode::Unavailable, why))?;
        let stripes: Vec<u64> = stripes_of(entry.geometry.stripe_bytes, start, end - start).collect();
        for _ in 0..MAX_TRIES {
            // every stripe's row, at Quorum, after the entry
            let rows = self.meta.stripe_rows(self.consumer, entry.object, &stripes).await?;
            // a row stamped past the entry's epoch: a truncate committed between them
            if rows.values().any(|row| row.stamp > entry.epoch) {
                entry = self.reread_entry().await?;
                continue;
            }
            match self.read_stripes(&placement, &entry, &rows, &stripes, start, end).await {
                Ok(bytes) => return Ok(bytes),
                // a holder ahead of its row: read the rows again
                Err(StripeRead::Newer) => continue,
                Err(StripeRead::Failed(why)) => return Err((ErrorCode::Unavailable, why)),
            }
        }
        Err((
            ErrorCode::Shedding,
            format!(
                "the stripes of {} moved under this read {MAX_TRIES} times; try it again",
                self.op.path
            ),
        ))
    }

    /// Read the range from each of its stripes, given their rows
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
    ) -> Result<Vec<u8>, StripeRead> {
        let mut out = Vec::with_capacity((end - start) as usize);
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
                out.extend_from_slice(&bytes);
            }
            // pending bytes a writer left in the row are folded and cleared by whoever meets them
            if let Some(row) = row {
                self.meet_pending(placement, entry, row);
            }
            // a hole, and what a floor hides, are zeros
            out.resize(out.len() + (to - read_to) as usize, 0);
        }
        Ok(out)
    }

    /// Read part of one stripe's chunk from a holder the row calls current
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
    ) -> Result<Vec<u8>, StripeRead> {
        let Some(store) = &self.store else {
            return Err(StripeRead::Failed("this node runs no object executor to read stripes with".to_string()));
        };
        let holders = placement
            .stripe(entry.object, stripe)
            .map_err(StripeRead::Failed)?;
        // a stripe its put wrote and nothing changed has no row: every position at the put's label
        let start = stripe * u64::from(entry.geometry.stripe_bytes);
        let row = row.cloned().unwrap_or_else(|| {
            let length = entry
                .put_length
                .saturating_sub(start)
                .min(u64::from(entry.geometry.stripe_bytes));
            StripeRow::put(self.consumer, entry.object, stripe, holders.len(), length)
        });
        // the positions the row calls current, a holder on this node first
        let mut order: Vec<usize> = (0..holders.len()).filter(|pos| row.current(*pos)).collect();
        order.sort_by_key(|pos| !store.is_local(&holders[*pos].1));
        let mut why = Vec::new();
        for pos in order {
            let (chunk, holder) = holders[pos];
            let labels = row.current_labels(pos);
            let reply = store
                .ask_holder(
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
                ChunkReply::Held { label: Some(held) } if newer(held, row.named(pos)) => {
                    return Err(StripeRead::Newer)
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

/// Whether a holder's label is past the one a row names
///
/// # Arguments
///
/// * `held` - The holder's label
/// * `named` - The row's
#[must_use]
pub(super) fn newer(held: Label, named: Label) -> bool {
    held.sequence > named.sequence
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
