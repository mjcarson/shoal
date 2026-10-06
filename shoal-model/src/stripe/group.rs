//! The tablet groups, held to their own contract and not modelled again
//!
//! A tablet group is an atomic object that applies commands in one committed order, answers a
//! read barrier with its latest state, and lets a lagging replica answer with any committed
//! prefix. That is what P1-P6 promise, the tablet model checks them, and S16 says not to model
//! Raft twice ([S16](../../../docs/src/object-storage/testing.md#alternatives-rejected)). A
//! leader change appears here as a stager losing its view: a proposal or its answer lost.
//!
//! Two kinds of group matter to one object: the one holding its `ObjectMeta` entry, and the one
//! holding each stripe's `StripeMeta` row beside its placement group's generation and positions.
//! They are different tablets, so nothing is atomic across them
//! ([S3](../../../docs/src/object-storage/objects.md#size-holes-and-truncate)).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::OpId;
use crate::stripe::content::Unit;
use crate::stripe::ids::{Epoch, Generation, Label, Pos, Seq, SliceId, Tag};
use crate::stripe::layout::Layout;
use crate::stripe::policy::{ConditionRule, EpochRule, GenerationRule, MissedRule, ReclaimRule};
use crate::stripe::policy::{StampRule, StripePolicy, TruncateRule};

/// A floor a truncate leaves: bytes past `len` stamped below `epoch` are a hole
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Floor {
    /// Where the cut was
    pub len: u32,
    /// The epoch the truncate moved the object to
    pub epoch: Epoch,
}

/// The object's `ObjectMeta` entry, as far as a write's safety needs it
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntryState {
    /// The object's length, in data units
    pub size: u32,
    /// The truncate epoch
    pub epoch: Epoch,
    /// The floors left by truncates not yet reclaimed
    pub floors: Vec<Floor>,
    /// How much of the object the put that created it wrote, cut by every truncate since
    ///
    /// A stripe with no row past it is a hole and nothing is read for it
    /// ([S9](../../../docs/src/object-storage/read-path.md#holes-and-ends)).
    pub created: u32,
}

/// What the entry's group is asked to commit
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryCommand {
    /// Cut or grow the object to a length, moving the epoch and leaving a floor
    Truncate {
        /// The truncate's identity
        op: OpId,
        /// The new length
        len: u32,
        /// The epoch it read, which it is refused at any other; none to commit at any epoch
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect: Option<Epoch>,
        /// The size it read, which decided where its floor falls and so which stripe it fenced;
        /// refused at any other, since an extension moves the size and not the epoch
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size: Option<u32>,
    },
    /// Move the epoch past a fence a truncate left and did not commit, changing nothing else
    Advance {
        /// The writer's identity
        op: OpId,
        /// The epoch it moves from
        from: Epoch,
    },
    /// Grow the size after an extending write committed its stripe, if no truncate came between
    Extend {
        /// The write's identity
        op: OpId,
        /// The length it grows to
        to: u32,
        /// The epoch the write read
        read_epoch: Epoch,
    },
    /// Remove a floor whose stripes reclamation has deleted
    DropFloor {
        /// The reclaimer's identity
        op: OpId,
        /// The floor's epoch
        epoch: Epoch,
    },
}

impl EntryCommand {
    /// The operation that asked for it
    pub fn op(&self) -> OpId {
        match self {
            EntryCommand::Truncate { op, .. }
            | EntryCommand::Advance { op, .. }
            | EntryCommand::Extend { op, .. }
            | EntryCommand::DropFloor { op, .. } => *op,
        }
    }
}

/// What a group answered a proposal
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Applied, and the state after it is at this index of the group's history
    Committed {
        /// The index of the state the command produced
        index: u32,
    },
    /// A write already committed under the same identity, answered as it was the first time
    Repeated {
        /// The index of the state its first commit produced
        index: u32,
    },
    /// Refused: the row moved, or the generation, or the epoch
    Refused,
}

/// The entry's tablet group
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryGroup {
    /// Every committed state, the first the object as its put created it
    pub history: Vec<EntryState>,
    /// The command that produced each state after the first
    pub commands: Vec<EntryCommand>,
    /// The answer each operation was given, so a retry is answered the same
    pub answered: BTreeMap<OpId, Decision>,
}

impl EntryGroup {
    /// The entry of an object of a given length, just put
    ///
    /// # Arguments
    ///
    /// * `size` - Its length, in data units
    pub fn new(size: u32) -> Self {
        Self {
            history: vec![EntryState {
                size,
                epoch: Epoch(0),
                floors: Vec::new(),
                created: size,
            }],
            commands: Vec::new(),
            answered: BTreeMap::new(),
        }
    }

    /// The latest committed state and its index
    pub fn latest(&self) -> (u32, &EntryState) {
        let index = self.history.len() - 1;
        (index as u32, &self.history[index])
    }

    /// The state a replica `lag` commits behind answers with
    ///
    /// # Arguments
    ///
    /// * `lag` - How far behind
    pub fn lagging(&self, lag: u8) -> (u32, &EntryState) {
        let index = (self.history.len() - 1).saturating_sub(usize::from(lag));
        (index as u32, &self.history[index])
    }

    /// Apply a command in committed order, judging its condition here
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command
    pub fn propose(&mut self, cmd: &EntryCommand) -> Decision {
        // a retried operation that committed is answered what its first try was
        if let Some(decision) = self.answered.get(&cmd.op()) {
            return *decision;
        }
        let (_, latest) = self.latest();
        let mut next = latest.clone();
        let decision = match cmd {
            EntryCommand::Truncate {
                expect: Some(epoch),
                ..
            } if *epoch != latest.epoch => None,
            EntryCommand::Truncate {
                size: Some(size), ..
            } if *size != latest.size => None,
            EntryCommand::Advance { from, .. } => {
                if *from != latest.epoch {
                    None
                } else {
                    next.epoch = Epoch(latest.epoch.0 + 1);
                    Some(next)
                }
            }
            EntryCommand::Truncate { len, .. } => {
                // the epoch moves, and the cut leaves a floor at the shorter of the two lengths
                next.epoch = Epoch(latest.epoch.0 + 1);
                next.floors.push(Floor {
                    len: (*len).min(latest.size),
                    epoch: next.epoch,
                });
                next.size = *len;
                next.created = latest.created.min(*len);
                Some(next)
            }
            EntryCommand::Extend { to, read_epoch, .. } => {
                // an extension stands only if no truncate came between its write's read and now
                if *read_epoch != latest.epoch {
                    None
                } else {
                    next.size = latest.size.max(*to);
                    Some(next)
                }
            }
            EntryCommand::DropFloor { epoch, .. } => {
                next.floors.retain(|floor| floor.epoch != *epoch);
                Some(next)
            }
        };
        match decision {
            Some(state) => {
                self.history.push(state);
                self.commands.push(cmd.clone());
                let decision = Decision::Committed {
                    index: (self.history.len() - 1) as u32,
                };
                self.answered.insert(cmd.op(), decision);
                decision
            }
            None => Decision::Refused,
        }
    }
}

/// A stripe's row and its placement group, as one tablet group holds them
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowState {
    /// Whether the stripe has a row; a stripe written only by its put has none
    pub exists: bool,
    /// Whether the row is a tombstone left by reclamation
    pub tombstone: bool,
    /// How many writes have committed
    pub seq: Seq,
    /// The label each position's chunk must carry
    pub labels: Vec<Label>,
    /// The positions whose chunk is stale: the missed record, derived at apply
    pub missed: Vec<bool>,
    /// The truncate epoch the last write's writer read
    pub stamp: Epoch,
    /// The placement group's generation
    pub generation: Generation,
    /// The slice at each position, the placement group's state beside its generation
    pub positions: Vec<SliceId>,
    /// The least truncate epoch a write's writer must have read to commit: a truncate cutting
    /// inside this stripe fences it before it commits
    #[serde(default, skip_serializing_if = "is_epoch_zero")]
    pub fence: Epoch,
}

/// Whether an epoch is zero, so a row with no fence serializes as it did before fences
fn is_epoch_zero(epoch: &Epoch) -> bool {
    epoch.0 == 0
}

impl RowState {
    /// The state of a stripe its put wrote and nothing has changed
    ///
    /// # Arguments
    ///
    /// * `layout` - The pool's layout
    /// * `positions` - The slices the placement group was first placed on
    pub fn put(layout: Layout, positions: Vec<SliceId>) -> Self {
        Self {
            exists: false,
            tombstone: false,
            seq: Seq(0),
            labels: vec![Label::PUT; layout.width()],
            missed: vec![false; layout.width()],
            stamp: Epoch(0),
            generation: Generation(0),
            positions,
            fence: Epoch(0),
        }
    }

    /// Whether the row calls a position's chunk current
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn current(&self, pos: Pos) -> bool {
        !self.tombstone && !self.missed[usize::from(pos.0)]
    }

    /// The label at a position
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn label(&self, pos: Pos) -> Label {
        self.labels[usize::from(pos.0)]
    }

    /// The slice at a position
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn slice(&self, pos: Pos) -> SliceId {
        self.positions[usize::from(pos.0)]
    }
}

/// Where a counted chunk's currency came from, for P11
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Evidence {
    /// A synced stage of this write, answered by the holder
    Staged,
    /// The holder answered, in this write's round, that it holds the label
    Confirmed,
    /// The row's word alone
    RowWord,
}

/// What a stripe's group is asked to commit
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StripeCommand {
    /// A write's commit: move the sequence and set the labels of the chunks it touched
    Write {
        /// The write's identity
        op: OpId,
        /// Which try of it
        attempt: u8,
        /// Whether the row existed when it was read
        base_exists: bool,
        /// The sequence it was staged against
        base: Seq,
        /// The generation it was staged under
        generation: Generation,
        /// The truncate epoch its writer read
        read_epoch: Epoch,
        /// Its tag
        tag: Tag,
        /// The positions it touched
        touched: Vec<Pos>,
        /// The touched positions that answered their stage
        staged: Vec<Pos>,
        /// The positions it counted toward `k + f`, and on what evidence
        counted: Vec<(Pos, Evidence)>,
        /// The data units it writes, zeros included, for the checker's ground truth only
        units: Vec<(u8, Unit)>,
    },
    /// The leader's no-op, prompted by a timer: moves the sequence and changes no label
    Noop {
        /// The sequence it moves past
        base: Seq,
    },
    /// A rebuilt or moved chunk made current under the label it already had
    Rebuild {
        /// The driver's identity
        op: OpId,
        /// The position
        pos: Pos,
        /// The label it holds
        label: Label,
        /// The sequence it read
        base: Seq,
        /// The generation it read
        generation: Generation,
        /// The slice now holding it
        slice: SliceId,
        /// Whether this is a move: the slice takes the position, under a new generation
        switch: bool,
        /// Whether the slice now holds the chunk: a move of a position that held nothing current
        /// switches it and leaves it missed
        current: bool,
    },
    /// A truncate cutting inside this stripe fences it with the epoch it is about to commit
    Fence {
        /// The truncate's identity
        op: OpId,
        /// The epoch
        epoch: Epoch,
    },
    /// Reclamation of a stripe a floor hides entirely
    Reclaim {
        /// The reclaimer's identity
        op: OpId,
        /// Whether the row existed when it was read
        base_exists: bool,
        /// The sequence it read
        base: Seq,
        /// The floor's epoch the stripe sits under
        below: Epoch,
    },
}

impl StripeCommand {
    /// The operation that asked for it, if one did
    pub fn op(&self) -> Option<OpId> {
        match self {
            StripeCommand::Write { op, .. }
            | StripeCommand::Rebuild { op, .. }
            | StripeCommand::Fence { op, .. }
            | StripeCommand::Reclaim { op, .. } => Some(*op),
            StripeCommand::Noop { .. } => None,
        }
    }
}

/// One stripe's tablet group
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeGroup {
    /// Every committed state, the first the stripe as its put left it
    pub history: Vec<RowState>,
    /// The command that produced each state after the first
    pub commands: Vec<StripeCommand>,
    /// The index each write committed at, by identity, so a retry is the same write
    pub written: BTreeMap<OpId, u32>,
}

impl StripeGroup {
    /// The group of a stripe its put wrote
    ///
    /// # Arguments
    ///
    /// * `layout` - The pool's layout
    /// * `positions` - The slices it was placed on
    pub fn new(layout: Layout, positions: Vec<SliceId>) -> Self {
        Self {
            history: vec![RowState::put(layout, positions)],
            commands: Vec::new(),
            written: BTreeMap::new(),
        }
    }

    /// The latest committed state and its index
    pub fn latest(&self) -> (u32, &RowState) {
        let index = self.history.len() - 1;
        (index as u32, &self.history[index])
    }

    /// The state a replica `lag` commits behind answers with
    ///
    /// # Arguments
    ///
    /// * `lag` - How far behind
    pub fn lagging(&self, lag: u8) -> (u32, &RowState) {
        let index = (self.history.len() - 1).saturating_sub(usize::from(lag));
        (index as u32, &self.history[index])
    }

    /// Apply a command in committed order, judging its condition here
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command
    /// * `policy` - The policy in force
    pub fn propose(&mut self, cmd: &StripeCommand, policy: &StripePolicy) -> Decision {
        // a write already committed under its identity is answered as it was the first time
        if let StripeCommand::Write { op, .. } = cmd {
            if let Some(index) = self.written.get(op) {
                return Decision::Repeated { index: *index };
            }
        }
        let (_, row) = self.latest();
        let next = match cmd {
            StripeCommand::Write {
                base_exists,
                base,
                generation,
                read_epoch,
                tag,
                touched,
                staged,
                ..
            } => {
                // POLICY P8: the contract refuses a commit whose row moved; the unsafe setting lands it
                let moved = row.exists != *base_exists || row.seq != *base;
                if moved && policy.condition == ConditionRule::Sequence {
                    return Decision::Refused;
                }
                // POLICY P8: the contract refuses a commit staged under another generation
                if row.generation != *generation && policy.generation == GenerationRule::Checked {
                    return Decision::Refused;
                }
                // Q18: a stamp that would move backwards is refused once that is the rule
                if row.stamp > *read_epoch && policy.stamp == StampRule::NeverBackwards {
                    return Decision::Refused;
                }
                // Q18: a writer that read the epoch before a truncate fenced this stripe is refused
                if row.fence > *read_epoch && policy.truncate == TruncateRule::Fenced {
                    return Decision::Refused;
                }
                let mut next = row.clone();
                next.exists = true;
                next.tombstone = false;
                next.seq = row.seq.next();
                for pos in touched {
                    let index = usize::from(pos.0);
                    next.labels[index] = Label {
                        seq: next.seq,
                        tag: *tag,
                    };
                    // POLICY P11, P17: the contract records who missed it; the unsafe setting does not
                    next.missed[index] =
                        policy.missed == MissedRule::Recorded && !staged.contains(pos);
                }
                // POLICY P13: the contract stamps the epoch the writer read
                if policy.epoch == EpochRule::Stamped {
                    next.stamp = *read_epoch;
                }
                next
            }
            StripeCommand::Noop { base } => {
                // a no-op for a sequence already past has nothing to do
                if row.seq != *base {
                    return Decision::Refused;
                }
                // a no-op on a stripe with no row makes one, so a staged first write is excluded
                let mut next = row.clone();
                next.exists = true;
                next.seq = row.seq.next();
                next
            }
            StripeCommand::Rebuild {
                pos,
                label,
                base,
                generation,
                slice,
                switch,
                current,
                ..
            } => {
                // a rebuild never overwrites a newer write, and never lands under another map
                if row.seq != *base || row.generation != *generation || row.label(*pos) != *label {
                    return Decision::Refused;
                }
                let mut next = row.clone();
                let index = usize::from(pos.0);
                // P17: only a chunk the slice holds is made current; a switch alone changes where
                next.missed[index] = !*current;
                // a move switches the generation and records the new position in one commit
                if *switch {
                    next.positions[index] = *slice;
                    next.generation = Generation(row.generation.0 + 1);
                }
                next
            }
            StripeCommand::Fence { epoch, .. } => {
                // a fence moves the sequence, so every write staged before it is refused
                let mut next = row.clone();
                next.exists = true;
                next.seq = row.seq.next();
                next.fence = row.fence.max(*epoch);
                next
            }
            StripeCommand::Reclaim {
                base_exists,
                base,
                below,
                ..
            } => {
                // only a stripe still under the floor, as read, is reclaimed
                if row.exists != *base_exists || row.seq != *base || row.stamp >= *below {
                    return Decision::Refused;
                }
                let mut next = row.clone();
                match policy.reclaim {
                    // S3 as written: the row is deleted, which is the state of a stripe with none
                    ReclaimRule::Deleted => {
                        next.exists = false;
                        next.tombstone = false;
                        next.seq = Seq(0);
                        next.labels = vec![Label::PUT; row.labels.len()];
                        next.missed = vec![true; row.missed.len()];
                        next.stamp = Epoch(0);
                    }
                    // a tombstone moves the sequence past every write staged before it
                    ReclaimRule::Tombstoned => {
                        next.exists = true;
                        next.tombstone = true;
                        next.seq = row.seq.next();
                        next.missed = vec![true; row.missed.len()];
                        // stamped with the floor it was reclaimed under, so a reader can tell
                        next.stamp = *below;
                    }
                }
                next
            }
        };
        self.history.push(next);
        self.commands.push(cmd.clone());
        let index = (self.history.len() - 1) as u32;
        if let StripeCommand::Write { op, .. } = cmd {
            self.written.insert(*op, index);
        }
        Decision::Committed { index }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stripe::policy::StripePolicy;

    /// A stripe group of a replicated stripe
    fn group() -> StripeGroup {
        StripeGroup::new(
            Layout::Replicated3,
            vec![SliceId(0), SliceId(1), SliceId(2)],
        )
    }

    /// A write's commit against a base
    fn write(op: u32, base: u32, read_epoch: u32) -> StripeCommand {
        StripeCommand::Write {
            op: OpId(op),
            attempt: 0,
            base_exists: base > 0,
            base: Seq(base),
            generation: Generation(0),
            read_epoch: Epoch(read_epoch),
            tag: Tag(op),
            touched: vec![Pos(0), Pos(1), Pos(2)],
            staged: vec![Pos(0), Pos(1)],
            counted: Vec::new(),
            units: vec![(0, Unit::Write(OpId(op)))],
        }
    }

    /// A commit against a row that moved is refused, and a write that committed is answered again
    #[test]
    fn a_moved_row_refuses_and_a_retry_is_answered_as_it_was() {
        let policy = StripePolicy::safe();
        let mut group = group();
        assert_eq!(
            group.propose(&write(1, 0, 0), &policy),
            Decision::Committed { index: 1 }
        );
        assert_eq!(group.propose(&write(2, 0, 0), &policy), Decision::Refused);
        assert_eq!(
            group.propose(&write(1, 1, 0), &policy),
            Decision::Repeated { index: 1 }
        );
        // the touched position that did not stage is recorded as missed
        let (_, row) = group.latest();
        assert_eq!(row.missed, vec![false, false, true]);
    }

    /// A fence refuses a writer that read the epoch before it, and a stamp never moves back
    #[test]
    fn a_fence_and_the_stamp_refuse_a_stale_writer() {
        let policy = StripePolicy::safe();
        let mut group = group();
        group.propose(&write(1, 0, 2), &policy);
        // a writer that read epoch 1 after the row was stamped 2
        assert_eq!(group.propose(&write(2, 1, 1), &policy), Decision::Refused);
        group.propose(
            &StripeCommand::Fence {
                op: OpId(3),
                epoch: Epoch(3),
            },
            &policy,
        );
        assert_eq!(group.propose(&write(4, 2, 2), &policy), Decision::Refused);
        assert!(matches!(
            group.propose(&write(5, 2, 3), &policy),
            Decision::Committed { .. }
        ));
    }

    /// A tombstone moves the sequence past every staged write; a deleted row sets it back to none
    #[test]
    fn a_tombstone_moves_the_sequence_and_a_deleted_row_sets_it_back() {
        let reclaim = StripeCommand::Reclaim {
            op: OpId(9),
            base_exists: true,
            base: Seq(1),
            below: Epoch(1),
        };
        let mut tombstoned = group();
        tombstoned.propose(&write(1, 0, 0), &StripePolicy::safe());
        tombstoned.propose(&reclaim, &StripePolicy::safe());
        let (_, row) = tombstoned.latest();
        assert!(row.tombstone && row.seq == Seq(2) && row.stamp == Epoch(1));
        let deleting = StripePolicy {
            reclaim: ReclaimRule::Deleted,
            ..StripePolicy::safe()
        };
        let mut deleted = group();
        deleted.propose(&write(1, 0, 0), &deleting);
        deleted.propose(&reclaim, &deleting);
        let (_, row) = deleted.latest();
        assert!(!row.exists && row.seq == Seq(0));
    }

    /// A truncate at another epoch or size is refused, and one that grows floors at the old size
    #[test]
    fn a_truncate_commits_at_its_epoch_and_floors_at_the_shorter_length() {
        let mut entry = EntryGroup::new(8);
        let late = EntryCommand::Truncate {
            op: OpId(1),
            len: 2,
            expect: Some(Epoch(1)),
            size: None,
        };
        assert_eq!(entry.propose(&late), Decision::Refused);
        // one that read another size would put its floor somewhere it did not fence
        let moved = EntryCommand::Truncate {
            op: OpId(4),
            len: 2,
            expect: Some(Epoch(0)),
            size: Some(4),
        };
        assert_eq!(entry.propose(&moved), Decision::Refused);
        let cut = EntryCommand::Truncate {
            op: OpId(2),
            len: 2,
            expect: Some(Epoch(0)),
            size: Some(8),
        };
        assert_eq!(entry.propose(&cut), Decision::Committed { index: 1 });
        let grow = EntryCommand::Truncate {
            op: OpId(3),
            len: 6,
            expect: None,
            size: None,
        };
        entry.propose(&grow);
        let (_, state) = entry.latest();
        assert_eq!(state.size, 6);
        assert_eq!(state.floors.last().map(|floor| floor.len), Some(2));
        assert_eq!(state.epoch, Epoch(2));
    }
}
