//! An operator's word on a stripe whose row is behind its chunks
//! ([F93](../../../../../docs/src/features/operations-and-the-real-cluster.md))
//!
//! After a `force_recover` that lost a write its holders applied, a stripe's row names a label its
//! holders are past, and every read and write of it is refused `Behind` by name (item 256). Nothing
//! the cluster knows says what to trust: the holders agreeing on a newer label could be moved to,
//! but the object's entry, its size after an append among it, may be behind too, and nothing on a
//! holder says what it was. So the cluster asks an operator, and `ResolveStripe` is the answer:
//!
//! - **`accept`** commits the label `k + f` holders agree on, which makes the stripe readable
//!   again at what its holders applied; a position whose holder does not hold it is marked missed,
//!   for recovery to rebuild from those that do;
//! - **`lost`** declares the stripe lost, durably on its row: every read and write of it is
//!   answered `Lost` by name, and its bytes are reclaimed with its object.
//!
//! # Invariants
//!
//! **A resolution is versioned by the stripe's row**, not the topology: its expected version is
//! the row's sequence the operator read in the refusal, and its commit is conditional on it under
//! the operation's own id. A repeat is answered as the first was, by that identity; a row that
//! moved since is refused as a stale version, as any other mutation's is.
//!
//! **An accept never invents a label.** It moves the row only to a label `k + f` holders hold, the
//! acknowledgement rule's own count, and never to one fewer hold.

use std::collections::BTreeMap;

use tracing::{event, Level};

use super::place::Placement;
use super::write::Proposed;
use super::{ObjectTask, DEFAULT_LEVEL};
use crate::server::database::ShoalDatabase;
use crate::shared::objects::{Geometry, Label};
use crate::shared::protocol::admin::{AdminError, StripeResolution};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest};
use crate::shared::objects::ChangedUnits;
use crate::shared::stripes::{StripeRow, Touch, WriteCommit, ANY_EPOCH};
use crate::shared::traits::QuerySupport;

/// What a resolution did, as its admin answer reports it
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Resolved {
    /// The stripe
    pub stripe: u64,
    /// What the operator said
    pub resolution: StripeResolution,
    /// The row's sequence after the resolution
    pub sequence: u64,
    /// The label an accept moved the row to
    pub label: Option<Label>,
    /// The positions an accept marked missed, which recovery rebuilds
    pub missed: Vec<usize>,
    /// Whether this was a repeat of a resolution already committed under the operation's id
    #[serde(skip)]
    pub repeated: bool,
}

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Settle one stripe of the operation's object on an operator's word
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `resolution` - Accept what `k + f` holders hold, or declare the stripe lost
    /// * `expected` - The row's sequence the operator read, zero for a stripe with no row
    ///
    /// # Errors
    ///
    /// Why the stripe cannot be settled so, by name.
    pub(crate) async fn resolve(
        &self,
        stripe: u64,
        resolution: StripeResolution,
        expected: u64,
    ) -> Result<Resolved, AdminError> {
        let refuse = |why: String| AdminError::new(ErrorCode::InvalidRequest, why);
        let failed = |(code, why): (ErrorCode, String)| AdminError::new(code, why);
        // the path's entry, strongly
        let key = self.key();
        let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await.map_err(failed)?;
        let Some(entry) = row
            .as_ref()
            .and_then(|row| row.entry(&self.op.path))
            .filter(|entry| entry.state.live)
            .cloned()
        else {
            return Err(refuse(format!("no object is at {}", self.op.path)));
        };
        // an inline object has no stripe to settle, and a stripe past its extent none either
        if entry.inline.is_some() {
            return Err(refuse(format!("{} is held inline and has no stripes", self.op.path)));
        }
        let stripe_bytes = u64::from(entry.geometry.stripe_bytes.max(1));
        if !has_stripe(stripe, stripe_bytes, entry.extent.max(entry.size)) {
            return Err(refuse(format!(
                "{} has no stripe {stripe}: its stripes reach {} bytes",
                self.op.path,
                entry.extent.max(entry.size)
            )));
        }
        // where its chunks are, under its placement group's state as it is now
        let placement = Placement::new(self.map.clone(), &self.pool_name, &self.pool, self.consumer, false)
            .map_err(|(_, why)| refuse(why))?;
        let key_pg = placement.pg_of(entry.object, stripe);
        let view = self
            .meta
            .read_placements(&[key_pg])
            .await
            .map_err(failed)?
            .remove(&key_pg)
            .unwrap_or_default();
        let set = placement.stripe_set(entry.object, stripe, &view).map_err(refuse)?;
        // the stripe's row, strongly, or its put's as it would be
        let rows = self
            .meta
            .stripe_rows(self.consumer, entry.object, &[stripe])
            .await
            .map_err(failed)?;
        let read = rows.get(&stripe).cloned();
        let start = stripe * stripe_bytes;
        let base = read.clone().unwrap_or_else(|| {
            let length = entry.put_length.saturating_sub(start).min(stripe_bytes);
            StripeRow::put(self.consumer, entry.object, stripe, set.width, length, key)
        });
        if base.tombstone {
            return Err(refuse(format!("stripe {stripe} of {} was reclaimed", self.op.path)));
        }
        // a row read at another sequence than the one asked about: a repeat of a resolution
        // committed under this operation's id, answered as the first was, or a request written
        // against a row that moved since. The tablet group answers a reused identity before it
        // judges the condition, and the condition cannot hold, so the probe applies nothing
        if let Some(current) = read.as_ref().filter(|row| row.sequence != expected) {
            let refused_stale = || {
                AdminError::new(
                    ErrorCode::StaleVersion,
                    format!(
                        "the request was written against stripe {stripe} of {}'s row at sequence \
                         {expected}, and it is at {}: read it again",
                        self.op.path, current.sequence
                    ),
                )
            };
            return match self.propose(current.noop(), Some(expected), self.op.id).await {
                Proposed::Committed | Proposed::CommittedElsewhere => Ok(Resolved {
                    stripe,
                    resolution,
                    sequence: current.sequence,
                    label: (resolution == StripeResolution::Accept).then(|| current.named(0)),
                    missed: Vec::new(),
                    repeated: true,
                }),
                Proposed::Refused => Err(refused_stale()),
                Proposed::Placement => Err(refuse(format!(
                    "stripe {stripe} of {}'s placement group moved while it was resolved: ask again",
                    self.op.path
                ))),
                Proposed::Failed(code, why) => Err(AdminError::new(code, why)),
            };
        }
        let next = match resolution {
            // the stripe's bytes given up: the row says so, and nothing reads or writes it again
            StripeResolution::Lost => {
                let mut next = base.noop();
                next.lost = true;
                next.placement_epoch = ANY_EPOCH;
                next
            }
            // the label k + f holders agree on, past the row
            StripeResolution::Accept => {
                let store = self.store().map_err(|answer| refuse(answer.message))?;
                // what each position's holder applied
                let mut held: Vec<Option<Label>> = vec![None; set.width];
                for (chunk, holder) in set.current() {
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
                    if let ChunkReply::Held { label: Some(label) } = reply {
                        held[position] = Some(label);
                    }
                }
                // the label k + f positions agree on, and every other position missed
                let needed = placement.needed();
                let Some((label, positions)) = accepted(&held, &base, &entry.geometry, needed) else {
                    return Err(refuse(format!(
                        "no {needed} holders of stripe {stripe} of {} agree on a label past its row, \
                         which names {:?}; they hold {held:?}: declare it lost instead",
                        self.op.path,
                        (0..set.width).map(|position| base.named(position)).collect::<Vec<_>>()
                    )));
                };
                let sequence = label.sequence;
                let mut next = base
                    .commit(&WriteCommit {
                        label,
                        positions,
                        stamp: base.stamp,
                        length: base.length,
                        pending: None,
                        changed: ChangedUnits::whole(ChangedUnits::every(set.width)),
                    })
                    .claiming(set.epoch);
                // the row's sequence never falls behind a label it names
                next.sequence = next.sequence.max(sequence + 1);
                next
            }
        };
        // conditional on the sequence the operator read, under the operation's own id, so a
        // retry of this very proposal is answered by that identity
        let missed: Vec<usize> = (0..set.width).filter(|position| !next.current(*position)).collect();
        let label = (resolution == StripeResolution::Accept).then(|| next.named(0));
        let sequence = next.sequence;
        let condition = match &read {
            Some(_) => Some(expected),
            None if expected == 0 => None,
            None => {
                return Err(AdminError::new(
                    ErrorCode::StaleVersion,
                    format!(
                        "stripe {stripe} of {} has no row, and the request was written against its \
                         row at sequence {expected}",
                        self.op.path
                    ),
                ))
            }
        };
        match self.propose(next, condition, self.op.id).await {
            Proposed::Committed | Proposed::CommittedElsewhere => {
                event!(Level::WARN, msg = "an operator resolved a stripe whose row was behind its chunks", path = %self.op.path, stripe, ?resolution, sequence);
                Ok(Resolved {
                    stripe,
                    resolution,
                    sequence,
                    label,
                    missed: if resolution == StripeResolution::Accept { missed } else { Vec::new() },
                    repeated: false,
                })
            }
            Proposed::Refused => Err(AdminError::new(
                ErrorCode::StaleVersion,
                format!(
                    "the request was written against stripe {stripe} of {}'s row at sequence \
                     {expected}, and it is at {}: read it again",
                    self.op.path,
                    read.as_ref().map_or(0, |row| row.sequence)
                ),
            )),
            Proposed::Placement => Err(refuse(format!(
                "stripe {stripe} of {}'s placement group moved while it was resolved: ask again",
                self.op.path
            ))),
            Proposed::Failed(code, why) => Err(AdminError::new(code, why)),
        }
    }
}

/// Whether an object whose stripes reach a number of bytes has a stripe
///
/// The stripe is an operator's number, so its start is never wrapped: a number whose start a
/// multiply cannot hold is no stripe, where it once wrapped onto another
/// ([Resolved #265](../../../../../docs/src/appendix/resolved/accept-counts-empty-chunks.md)).
///
/// # Arguments
///
/// * `stripe` - The stripe, as an operator named it
/// * `stripe_bytes` - How many bytes a stripe holds
/// * `reach` - How far the object's stripes reach
fn has_stripe(stripe: u64, stripe_bytes: u64, reach: u64) -> bool {
    // its start, if a multiply holds it, short of how far the stripes reach
    stripe
        .checked_mul(stripe_bytes)
        .is_some_and(|start| start < reach)
}

/// The label `k + f` positions of a stripe agree on past its row, and what each position is to it
///
/// A position whose holder answered no label counts for every label when its chunk holds no
/// bytes at the row's length: a data chunk past a short stripe's end is never stored and is
/// current by definition, as a put and a write count it
/// ([F90](../../../../../docs/src/features/erasure-coded-pools.md),
/// [Resolved #265](../../../../../docs/src/appendix/resolved/accept-counts-empty-chunks.md)). One
/// that holds bytes and answered nothing is missed.
///
/// # Arguments
///
/// * `held` - What each position's holder applied, none where it answered no label
/// * `base` - The stripe's row
/// * `geometry` - The object's layout
/// * `needed` - How many positions a label needs, `k + f`
fn accepted(held: &[Option<Label>], base: &StripeRow, geometry: &Geometry, needed: usize) -> Option<(Label, Vec<Touch>)> {
    // how many positions hold each label past the row
    let mut agreeing: BTreeMap<(u64, u64), usize> = BTreeMap::new();
    for (position, label) in held.iter().enumerate() {
        if let Some(label) = label {
            if label.ahead_of(base.named(position)) {
                *agreeing.entry((label.sequence, label.tag)).or_default() += 1;
            }
        }
    }
    // the positions whose chunk holds nothing, which no holder stored and every label has
    let empty: Vec<bool> = held
        .iter()
        .enumerate()
        .map(|(position, label)| label.is_none() && geometry.chunk_length(base.length, position) == 0)
        .collect();
    let padding = empty.iter().filter(|empty| **empty).count();
    // the most agreed on, the newest among equals
    let (&(sequence, tag), _) = agreeing
        .iter()
        .filter(|(_, count)| **count + padding >= needed)
        .max_by_key(|((sequence, _), count)| (**count, *sequence))?;
    let label = Label { sequence, tag };
    // every position holding it, and every empty one, is current; every other is missed and
    // rebuilt from them
    let positions = held
        .iter()
        .zip(&empty)
        .map(|(have, empty)| match have {
            Some(have) if *have == label => Touch::Held,
            None if *empty => Touch::Held,
            _ => Touch::Missed,
        })
        .collect();
    Some((label, positions))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4+2 geometry of 4 MiB stripes, 1 MiB chunks
    fn four_two() -> Geometry {
        Geometry {
            stripe_bytes: 4 << 20,
            unit_bytes: 64 << 10,
            data: 4,
            parity: 2,
        }
    }

    /// A short erasure coded stripe's never stored data chunks count as held for an accept and are
    /// never marked missed, and a stored chunk that answered nothing still is (item 265)
    #[test]
    fn an_accept_counts_a_short_stripes_empty_chunks() {
        let ahead = Label { sequence: 1, tag: 9 };
        // one chunk of bytes: data positions one to three were never stored
        let base = StripeRow::put(1, 7, 0, 6, 1 << 20, 0);
        let held = [Some(ahead), None, None, None, Some(ahead), Some(ahead)];
        let (label, positions) = accepted(&held, &base, &four_two(), 5).expect("accepted");
        assert_eq!(label, ahead);
        assert_eq!(positions, vec![Touch::Held; 6]);
        // two chunks of bytes, data position one's holder silent: it is missed, the empty two held
        let base = StripeRow::put(1, 7, 0, 6, 2 << 20, 0);
        let held = [Some(ahead), None, None, None, Some(ahead), Some(ahead)];
        let (_, positions) = accepted(&held, &base, &four_two(), 5).expect("accepted");
        assert_eq!(
            positions,
            vec![Touch::Held, Touch::Missed, Touch::Held, Touch::Held, Touch::Held, Touch::Held]
        );
        // a whole stripe with three agreeing is still refused, and so is a label no one holds
        let base = StripeRow::put(1, 7, 0, 6, 4 << 20, 0);
        assert!(accepted(&held, &base, &four_two(), 5).is_none());
        assert!(accepted(&[None; 6], &StripeRow::put(1, 7, 0, 6, 1 << 20, 0), &four_two(), 5).is_none());
        // copies hold the whole stripe each, so a silent one is never padded
        let copies = Geometry {
            stripe_bytes: 1 << 20,
            unit_bytes: 64 << 10,
            data: 1,
            parity: 2,
        };
        let base = StripeRow::put(1, 7, 0, 3, 100, 0);
        assert!(accepted(&[Some(ahead), Some(ahead), None], &base, &copies, 3).is_none());
    }

    /// A stripe an operator names past what a multiply can hold is no stripe, never a wrapped one
    #[test]
    fn a_stripe_past_the_multiply_is_no_stripe() {
        let stripe_bytes = 4u64 << 20;
        assert!(has_stripe(0, stripe_bytes, 1));
        assert!(!has_stripe(1, stripe_bytes, stripe_bytes));
        // 2^42 + 1 stripes of 4 MiB wrap to one stripe's start
        assert!(!has_stripe((1 << 42) + 1, stripe_bytes, 8 << 20));
        assert!(!has_stripe(u64::MAX, stripe_bytes, u64::MAX));
    }
}
