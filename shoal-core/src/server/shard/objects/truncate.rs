//! A truncate: an object cut, or grown with zeros, to a length
//!
//! Size lives in the object's entry and stripes in other tablets, so nothing is atomic across the
//! two, and the order of commits is what keeps cut bytes from returning
//! ([S3](../../../../../docs/src/object-storage/objects.md#size-holes-and-truncate)):
//!
//! - a truncate moves the epoch and leaves a floor at the shorter of the two lengths, which hides
//!   every stripe stamped below it from where it cut;
//! - a cut inside a stripe first fences that stripe's row at the epoch it is about to commit,
//!   which moves the row's sequence, so a writer that read the epoch before it is refused;
//! - then it commits the entry only at the epoch and the size it read, and reads and fences again
//!   if either moved.
//!
//! A truncate that dies after fencing leaves its fence; the next writer moves the epoch past it.
//! An inline object is cut or grown in its entry, and moves to stripes if it grows past its pool's
//! threshold.

use super::{not_found, MetaAnswer, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::shared::objects::{Floor, StripeAnswer, MAX_FLOORS};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::ObjectOutcome;
use crate::shared::stripes::StripeRow;
use crate::shared::traits::QuerySupport;

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Cut or grow the object to the length the operation names
    pub(super) async fn truncate(&self) -> Outcome {
        let op = &self.op;
        let len = op.length;
        let key = self.key();
        for _ in 0..MAX_TRIES {
            // the entry, strongly, for its size and its epoch
            let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
            let Some(entry) = row
                .as_ref()
                .and_then(|row| row.entry(&op.path))
                .filter(|entry| entry.state.live)
                .cloned()
            else {
                return Ok((not_found(op), None));
            };
            // an inline object is cut or grown where it is held
            if let Some(inline) = &entry.inline {
                let mut bytes = inline.clone();
                bytes.resize(len as usize, 0);
                let outcome = if len <= self.pool.inline_threshold() {
                    self.rewrite_inline(key, row.as_ref(), &entry, bytes).await?
                } else {
                    self.move_to_stripes(key, row.as_ref(), &entry, bytes).await?
                };
                match outcome {
                    Some(done) => return Ok((done, None)),
                    None => continue,
                }
            }
            // a floor a truncate leaves goes once reclamation has reclaimed what it hides (M20)
            if entry.floors.len() >= MAX_FLOORS {
                return Ok((
                    self.refused(
                        ObjectOutcome::TooManyFloors,
                        format!(
                            "{} keeps {} floors of truncates whose stripes are not yet reclaimed, the most an entry keeps",
                            op.path,
                            entry.floors.len()
                        ),
                    ),
                    None,
                ));
            }
            let epoch = entry.epoch + 1;
            let stripe_bytes = u64::from(entry.geometry.stripe_bytes);
            // a cut inside a stripe fences that stripe first, at the epoch about to be committed
            if len < entry.size && !len.is_multiple_of(stripe_bytes) {
                let stripe = len / stripe_bytes;
                let rows = self.meta.stripe_rows(self.consumer, entry.object, &[stripe]).await?;
                let read = rows.get(&stripe);
                if read.is_none_or(|read| read.fence < epoch) {
                    let width = usize::from(self.pool.redundancy.width());
                    let start = stripe * stripe_bytes;
                    let put_length = entry.put_length.saturating_sub(start).min(stripe_bytes);
                    let base = read
                        .cloned()
                        .unwrap_or_else(|| StripeRow::put(self.consumer, entry.object, stripe, width, put_length));
                    match self
                        .meta
                        .commit_stripe(base.fence(epoch), read.map(|read| read.sequence), None)
                        .await
                    {
                        StripeAnswer::Written => {}
                        // the row moved under the fence: read everything again
                        StripeAnswer::Refused(_) => continue,
                        StripeAnswer::Failed(code, why) => return Err((code, why)),
                        StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                            return Err((ErrorCode::Internal, "a fence answered as a read".to_string()))
                        }
                    }
                }
            }
            // the entry, at the epoch and the size read: the commit is conditional on its version
            let mut next = entry.clone();
            next.epoch = epoch;
            next.floors.push(Floor {
                length: len.min(entry.size),
                epoch,
            });
            next.size = len;
            next.put_length = entry.put_length.min(len);
            next.content_version += 1;
            next.modified_ms = super::now_ms();
            match self.meta.ask(self.with_entry(key, row.as_ref(), next.clone()), None).await {
                MetaAnswer::Written => return Ok((self.done(&next, 0), None)),
                // the epoch or the size moved: read, and fence, again
                MetaAnswer::Refused(_) => continue,
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((ErrorCode::Internal, "a truncate answered as a read".to_string()))
                }
            }
        }
        Err(super::contended(&op.path))
    }
}
