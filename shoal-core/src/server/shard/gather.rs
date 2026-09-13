//! The shares of the queries a shard split, and the rule for when one is answered
//!
//! A query naming partitions on several shards is answered in pieces, and the client is owed
//! exactly one response for it. The shard that split it keeps a [`Gather`] until every slot it
//! sent a piece to is covered or one has failed, merges the pieces, puts their rows back into
//! the order the query named its partitions in, applies the query's limit to their union, and
//! replies once. A gather that is still owed shares when its deadline passes is answered once
//! with `Timeout` and forgotten, so a share that never comes cannot hold a client forever
//! ([F41](../../../../docs/src/features/read-consistency.md),
//! [Resolved #33](../../../../docs/src/appendix/resolved/gather-expiry.md)).
//!
//! # Invariants
//!
//! **Coverage is the slot, never the rows.** A share with no rows covers its slot exactly as one
//! with a thousand does, so an empty partition and a missing share are told apart by whether the
//! slot was filled, and no reply is ever built from fewer covered slots than the query has.
//!
//! **A key is removed on completion or on expiry, never both.** Whichever happens first takes
//! the gather out of the map; anything that arrives afterwards finds no key and is late.
//!
//! **A share is judged by attempt and slot, not by arrival.** A share for an attempt the gather
//! has moved past is late; one for a slot already covered is a duplicate. Both are counted and
//! dropped, and neither changes the answer.
//!
//! This module holds no shard: everything in it is pure over the map, which is what lets the
//! completion, expiry and identity rules be tested without a server.

use std::collections::HashMap;

use tracing::Span;
use uuid::Uuid;

use crate::server::shard::ShardContact;
use crate::server::stage_profile::{StageStamps, Stamp};
use crate::shared::traits::ShoalResponseSupport;

/// What a gather needs of a share: that two of them can be folded into one
///
/// The schema's response type does this through [`ShoalResponseSupport`]; the trait exists so
/// the map can be tested with a share that is nothing but a merge.
pub(super) trait MergeShare {
    /// Fold another share of the same query into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The other share
    fn merge_share(&mut self, other: Self);
}

impl<R: ShoalResponseSupport> MergeShare for R {
    /// A response merges as the schema's derive says it does
    ///
    /// # Arguments
    ///
    /// * `other` - The other share
    fn merge_share(&mut self, other: Self) {
        self.merge(other);
    }
}

/// Whether one slot of a gather has been answered, and how
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlotState {
    /// The share has not arrived
    Outstanding,
    /// The share arrived, with rows or without
    Covered,
    /// The share arrived as a failure, which fails the whole answer
    Failed,
}

/// One shard's piece of a split query, and whether it has arrived
#[derive(Debug, Clone)]
pub(super) struct Slot {
    /// The shard the piece was sent to
    pub(super) contact: ShardContact,
    /// Whether it has answered
    pub(super) state: SlotState,
}

/// The shares of one query that was split across several shards
///
/// Generic over the schema's table name and response type rather than over the database, so
/// the map is testable with a stand-in for both.
pub(super) struct Gather<T, R> {
    /// The id of the client waiting on this query
    pub(super) client: Uuid,
    /// The span context for this query
    pub(super) span: Span,
    /// When this query reached each stage on the shard that split it
    ///
    /// The shares each carry their own stamps and each become their own record, flagged as
    /// shares. This is the one the client actually waited on, so it is the one whose stages
    /// describe the latency the client saw.
    pub(super) stamps: StageStamps,
    /// The table the query named, so an expiry can be answered in its variant
    pub(super) table: T,
    /// Whether the query was the last of its stream
    pub(super) end: bool,
    /// Which attempt at the bundle these shares belong to
    pub(super) attempt: u64,
    /// When the client stops waiting, after which the gather is answered `Timeout`
    pub(super) deadline: Stamp,
    /// The most rows this query asked for, if it set a limit
    pub(super) limit: Option<usize>,
    /// The partitions this query named, in the order it named them
    ///
    /// Shares arrive in whatever order the shards answer in, so this is what the merged
    /// rows are put back into before the limit is applied to them.
    pub(super) partition_order: Vec<u64>,
    /// One slot per shard the query was split to, in the order the pieces were sent
    pub(super) slots: Vec<Slot>,
    /// The shares merged so far
    pub(super) merged: Option<R>,
}

impl<T, R> Gather<T, R> {
    /// How many slots have not been answered
    #[must_use]
    pub(super) fn outstanding(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.state == SlotState::Outstanding)
            .count()
    }

    /// How many slots have been answered, with rows, without, or with a failure
    #[must_use]
    pub(super) fn arrived(&self) -> usize {
        self.slots.len() - self.outstanding()
    }

    /// Whether every slot is covered, or one has failed, so the query can be answered
    #[must_use]
    fn is_complete(&self) -> bool {
        // one failure is enough to answer, since the merge has already made the answer one
        if self.slots.iter().any(|slot| slot.state == SlotState::Failed) {
            return true;
        }
        self.slots.iter().all(|slot| slot.state == SlotState::Covered)
    }
}

/// What arrived at a gather, judged by the key, the attempt and the slot it named
pub(super) enum Arrival<T, R> {
    /// The share was merged and more are still owed
    Merged,
    /// The share was merged and it was the last one owed: the gather is out of the map
    Complete(Gather<T, R>),
    /// No gather holds this key, or the gather has moved past this attempt
    Late,
    /// The slot this share names was already covered
    Duplicate,
}

impl<T, R> std::fmt::Debug for Arrival<T, R> {
    /// Name the arrival, since a gather's contents are not printable
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Arrival::Merged => write!(f, "Merged"),
            Arrival::Complete(_) => write!(f, "Complete"),
            Arrival::Late => write!(f, "Late"),
            Arrival::Duplicate => write!(f, "Duplicate"),
        }
    }
}

/// The gathers a shard holds, keyed by the query they answer
///
/// Keyed by (bundle id, index), the pair that uniquely identifies one query within one bundle -
/// the same key the tables use for their own partial results.
pub(super) struct Gathers<T, R> {
    /// Every gather that is still owed a share
    map: HashMap<(Uuid, usize), Gather<T, R>>,
}

impl<T, R: MergeShare> Default for Gathers<T, R> {
    /// An empty map
    fn default() -> Self {
        Gathers { map: HashMap::new() }
    }
}

impl<T, R: MergeShare> Gathers<T, R> {
    /// How many gathers are resident
    #[must_use]
    pub(super) fn len(&self) -> usize {
        self.map.len()
    }

    /// Remember what a split query is owed before any piece of it is sent
    ///
    /// # Arguments
    ///
    /// * `key` - The bundle and index the query answers under
    /// * `gather` - What is owed
    pub(super) fn insert(&mut self, key: (Uuid, usize), gather: Gather<T, R>) {
        self.map.insert(key, gather);
    }

    /// Take one share into the gather it names, saying what the share turned out to be
    ///
    /// # Arguments
    ///
    /// * `key` - The bundle and index the share answers under
    /// * `attempt` - The attempt the share was sent under
    /// * `slot` - The slot the share fills
    /// * `response` - The share
    /// * `failed` - Whether the share is a failure rather than rows
    pub(super) fn arrive(
        &mut self,
        key: (Uuid, usize),
        attempt: u64,
        slot: u16,
        response: R,
        failed: bool,
    ) -> Arrival<T, R> {
        // a share for a query nobody is waiting on any more is late, whatever it carries
        let Some(gather) = self.map.get_mut(&key) else {
            return Arrival::Late;
        };
        // so is a share for an attempt the gather has moved past
        if gather.attempt != attempt {
            return Arrival::Late;
        }
        // a slot the gather does not have, or one already filled, is a duplicate
        let Some(target) = gather.slots.get_mut(usize::from(slot)) else {
            return Arrival::Duplicate;
        };
        if target.state != SlotState::Outstanding {
            return Arrival::Duplicate;
        }
        // the share fills its slot, with rows or without: coverage is the slot
        target.state = if failed { SlotState::Failed } else { SlotState::Covered };
        // and is merged into what has been collected so far
        match &mut gather.merged {
            Some(merged) => merged.merge_share(response),
            None => gather.merged = Some(response),
        }
        // wait for the rest if any are still owed and nothing has failed
        if !gather.is_complete() {
            return Arrival::Merged;
        }
        // every slot has reported, so this query is ours to answer now, and the key goes with it
        match self.map.remove(&key) {
            Some(gather) => Arrival::Complete(gather),
            None => Arrival::Late,
        }
    }

    /// Take every gather whose deadline has passed
    ///
    /// Each one is answered once by the caller and never seen again here: a share that arrives
    /// for it afterwards is late.
    ///
    /// # Arguments
    ///
    /// * `now` - The current stamp
    pub(super) fn expire(&mut self, now: Stamp) -> Vec<((Uuid, usize), Gather<T, R>)> {
        // find what has waited too long, then take it out
        let expired: Vec<(Uuid, usize)> = self
            .map
            .iter()
            .filter(|(_, gather)| now.since(gather.deadline) > 0)
            .map(|(key, _)| *key)
            .collect();
        expired
            .into_iter()
            .filter_map(|key| self.map.remove(&key).map(|gather| (key, gather)))
            .collect()
    }

    /// Withdraw a gather that was recorded and then never sent
    ///
    /// # Arguments
    ///
    /// * `key` - The bundle and index it was recorded under
    pub(super) fn forget_query(&mut self, key: (Uuid, usize)) {
        self.map.remove(&key);
    }

    /// Drop every gather a client was waiting on, now that the client is gone
    ///
    /// # Arguments
    ///
    /// * `client` - The client that went away
    pub(super) fn forget_client(&mut self, client: Uuid) -> usize {
        let before = self.map.len();
        self.map.retain(|_, gather| gather.client != client);
        before - self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A share for the tests: an exists answer that merges with an or, or a failure that wins
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Share {
        /// Whether the share found the row
        Exists(bool),
        /// The share failed
        Error,
    }

    impl MergeShare for Share {
        /// A failure takes over; two answers or together
        ///
        /// # Arguments
        ///
        /// * `other` - The other share
        fn merge_share(&mut self, other: Self) {
            *self = match (*self, other) {
                (Share::Error, _) | (_, Share::Error) => Share::Error,
                (Share::Exists(ours), Share::Exists(theirs)) => Share::Exists(ours || theirs),
            };
        }
    }

    /// A share saying a row exists or not
    ///
    /// # Arguments
    ///
    /// * `found` - Whether the share found the row
    fn share(found: bool) -> Share {
        Share::Exists(found)
    }

    /// A share that failed
    fn failure() -> Share {
        Share::Error
    }

    /// A gather over this many local slots, at this attempt, with this deadline
    ///
    /// # Arguments
    ///
    /// * `slots` - How many shards the query was split to
    /// * `attempt` - The attempt
    /// * `deadline` - When it expires
    fn gather(slots: usize, attempt: u64, deadline: Stamp) -> Gather<(), Share> {
        Gather {
            client: Uuid::from_u128(7),
            span: Span::none(),
            stamps: StageStamps::new(Stamp::now()),
            table: (),
            end: true,
            attempt,
            deadline,
            limit: None,
            partition_order: Vec::new(),
            slots: (0..slots)
                .map(|index| Slot {
                    contact: ShardContact::Local(index),
                    state: SlotState::Outstanding,
                })
                .collect(),
            merged: None,
        }
    }

    /// Expiry removes, a share after expiry is late, a stale attempt is late, a covered slot
    /// again is a duplicate, and completion fires exactly once (F41)
    #[test]
    fn gather_timeout_completes_once_and_discards_late_replies() {
        let key = (Uuid::from_u128(1), 3);
        let mut gathers: Gathers<(), Share> = Gathers::default();
        // a gather whose deadline has already passed expires on the first sweep, once
        let now = Stamp::now();
        gathers.insert(key, gather(2, 1, now.minus_nanos(1)));
        let expired = gathers.expire(now);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, key);
        assert_eq!(expired[0].1.arrived(), 0);
        assert_eq!(gathers.len(), 0);
        assert!(gathers.expire(now).is_empty());
        // a share that arrives after the expiry is late
        assert!(matches!(gathers.arrive(key, 1, 0, share(true), false), Arrival::Late));
        // a live gather: a share for an older attempt is late and covers nothing
        gathers.insert(key, gather(2, 5, now.plus_nanos(1_000_000_000)));
        assert!(matches!(gathers.arrive(key, 4, 0, share(true), false), Arrival::Late));
        // the first share merges and leaves one outstanding
        assert!(matches!(gathers.arrive(key, 5, 0, share(true), false), Arrival::Merged));
        // the same slot again is a duplicate, and does not complete the gather
        assert!(matches!(gathers.arrive(key, 5, 0, share(true), false), Arrival::Duplicate));
        assert_eq!(gathers.len(), 1);
        // a slot the gather does not have is a duplicate too, not a crash
        assert!(matches!(gathers.arrive(key, 5, 9, share(true), false), Arrival::Duplicate));
        // the last slot completes it exactly once, and the answer is the merge of both
        let Arrival::Complete(done) = gathers.arrive(key, 5, 1, share(false), false) else {
            panic!("the last share did not complete the gather");
        };
        assert_eq!(done.arrived(), 2);
        assert_eq!(done.merged, Some(Share::Exists(true)));
        assert_eq!(gathers.len(), 0);
        // and anything after that is late
        assert!(matches!(gathers.arrive(key, 5, 1, share(false), false), Arrival::Late));
        // an unexpired gather is left alone by a sweep
        gathers.insert(key, gather(1, 6, now.plus_nanos(1_000_000_000)));
        assert!(gathers.expire(now).is_empty());
        assert_eq!(gathers.len(), 1);
    }

    /// A failed share completes the gather at once with the failure as its answer; an empty
    /// share covers its slot exactly as a full one does (F41)
    #[test]
    fn a_failed_share_completes_at_once_and_an_empty_share_still_covers() {
        let key = (Uuid::from_u128(2), 0);
        let far = Stamp::now().plus_nanos(1_000_000_000);
        let mut gathers: Gathers<(), Share> = Gathers::default();
        // three slots; the second fails before the third arrives
        gathers.insert(key, gather(3, 1, far));
        assert!(matches!(gathers.arrive(key, 1, 0, share(true), false), Arrival::Merged));
        let Arrival::Complete(done) = gathers.arrive(key, 1, 1, failure(), true) else {
            panic!("a failed share did not complete the gather");
        };
        assert_eq!(done.arrived(), 2);
        assert_eq!(done.outstanding(), 1);
        assert_eq!(done.merged, Some(Share::Error));
        // the third share is late: the answer has gone out
        assert!(matches!(gathers.arrive(key, 1, 2, share(true), false), Arrival::Late));
        // an empty share is coverage: two empties complete a gather of two
        gathers.insert(key, gather(2, 2, far));
        assert!(matches!(gathers.arrive(key, 2, 0, share(false), false), Arrival::Merged));
        assert!(matches!(gathers.arrive(key, 2, 1, share(false), false), Arrival::Complete(_)));
    }

    /// A client that goes away takes its gathers with it and nobody else's
    #[test]
    fn a_gone_client_drops_only_its_own_gathers() {
        let far = Stamp::now().plus_nanos(1_000_000_000);
        let mut gathers: Gathers<(), Share> = Gathers::default();
        gathers.insert((Uuid::from_u128(1), 0), gather(2, 1, far));
        gathers.insert((Uuid::from_u128(2), 0), gather(2, 1, far));
        let mut other = gather(2, 1, far);
        other.client = Uuid::from_u128(8);
        gathers.insert((Uuid::from_u128(3), 0), other);
        assert_eq!(gathers.forget_client(Uuid::from_u128(7)), 2);
        assert_eq!(gathers.len(), 1);
        assert_eq!(gathers.forget_client(Uuid::from_u128(7)), 0);
    }
}
