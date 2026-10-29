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
use crate::stripe::ids::{Epoch, Generation, Label, Pos, Seq, SliceId, StripeIx, Tag};
use crate::stripe::layout::Layout;
use crate::stripe::policy::{ConditionRule, EpochRule, GenerationRule, MissedRule, ReclaimRule};
use crate::stripe::policy::{FloorPrune, PendingMerge, StampRule, StripePolicy, TruncateRule};

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
    /// A put's entry commit: what it wrote is the object's, up to its end, if no truncate came
    /// between its read and now (M16)
    Put {
        /// The put's identity
        op: OpId,
        /// The length it wrote to
        to: u32,
        /// The epoch it read
        read_epoch: Epoch,
    },
}

impl EntryCommand {
    /// The operation that asked for it
    pub fn op(&self) -> OpId {
        match self {
            EntryCommand::Truncate { op, .. }
            | EntryCommand::Advance { op, .. }
            | EntryCommand::Extend { op, .. }
            | EntryCommand::DropFloor { op, .. }
            | EntryCommand::Put { op, .. } => *op,
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
    /// * `policy` - The policy in force
    pub fn propose(&mut self, cmd: &EntryCommand, policy: &StripePolicy) -> Decision {
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
                let floor = Floor {
                    len: (*len).min(latest.size),
                    epoch: next.epoch,
                };
                // POLICY P13: M20 prunes every floor the new one subsumes, which hides all it hid;
                // the unsafe setting prunes every older floor, whatever its cut
                if policy.reclamation.released() {
                    match policy.reclamation.prune {
                        FloorPrune::Subsumed => next.floors.retain(|older| older.len < floor.len),
                        FloorPrune::Older => next.floors.clear(),
                    }
                }
                next.floors.push(floor);
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
            EntryCommand::Put { to, read_epoch, .. } => {
                // a put's bytes stand only if no truncate came between its read and now
                if *read_epoch != latest.epoch {
                    None
                } else {
                    next.size = latest.size.max(*to);
                    next.created = latest.created.max(*to);
                    Some(next)
                }
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

/// The bytes of small writes that rode in their commits and that the row still holds
///
/// X8's field of pending bytes ([Q27](../../../docs/src/object-storage/contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)):
/// a write below the threshold commits its new values into the row rather than staging them,
/// every position's label moves to the write's, and a holder whose chunk is at the base makes
/// that label by laying these units over it. A later small write merges its units in, so a holder
/// still at the base, or at the label of an earlier write merged here, makes the latest label
/// from them too. They leave the row only by a commit: a clear once `k + f` holders hold the label,
/// a staged write that carried them, or a reclamation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PendingBytes {
    /// The label they make: the last small write's
    pub label: Label,
    /// The label each position's chunk carried when the first of them committed
    pub base: Vec<Label>,
    /// The labels of the earlier small writes merged here, which a holder may have made already
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain: Vec<Label>,
    /// The data units, by index within the stripe, and their new values
    pub units: Vec<(u8, Unit)>,
}

impl PendingBytes {
    /// The labels a holder's chunk can be at for these bytes to make the label they make
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn sources(&self, pos: Pos) -> Vec<Label> {
        // the base, then every earlier write merged since it
        let mut sources = vec![self.base[usize::from(pos.0)]];
        sources.extend(self.chain.iter().copied());
        sources
    }

    /// Whether a chunk at a label makes these bytes' label once they are laid over it
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    /// * `held` - The label the chunk is at
    pub fn folds_from(&self, pos: Pos, held: Label) -> bool {
        self.sources(pos).contains(&held)
    }

    /// The bytes a small write leaves in a row: its units merged over what is pending there
    ///
    /// The stager computes them to tell holders what to fold, and the group to commit them; both
    /// from the same row, since the commit is refused at any other.
    ///
    /// # Arguments
    ///
    /// * `row` - The row the write commits over
    /// * `label` - The write's label
    /// * `written` - The units it writes, zeros it adds included
    /// * `policy` - The policy in force
    pub fn after(
        row: &RowState,
        label: Label,
        written: &[(u8, Unit)],
        policy: &StripePolicy,
    ) -> Self {
        let Some(pending) = &row.pending_bytes else {
            // the first small write since the chunks were whole: the row's labels are the base
            let mut units = written.to_vec();
            units.sort_unstable();
            return Self {
                label,
                base: row.labels.clone(),
                chain: Vec::new(),
                units,
            };
        };
        // POLICY P9: the contract merges its units over what is pending; X8's spike replaced them
        let mut units: Vec<(u8, Unit)> = match policy.small_writes.merge {
            PendingMerge::Merged => pending
                .units
                .iter()
                .filter(|(unit, _)| !written.iter().any(|(w, _)| w == unit))
                .copied()
                .chain(written.iter().copied())
                .collect(),
            PendingMerge::Replaced => written.to_vec(),
        };
        units.sort_unstable();
        let mut chain = pending.chain.clone();
        chain.push(pending.label);
        Self {
            label,
            base: pending.base.clone(),
            chain,
            units,
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
    /// The bytes of small writes that rode in their commits and not every holder has folded
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_bytes: Option<PendingBytes>,
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
            pending_bytes: None,
        }
    }

    /// Whether a chunk at a label is the position's current state, the pending bytes laid over it
    /// included: the label the row names, or a label they fold from into it
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    /// * `held` - The label the chunk is at
    pub fn makes(&self, pos: Pos, held: Label) -> bool {
        held == self.label(pos)
            || self
                .pending_bytes
                .as_ref()
                .is_some_and(|pending| {
                    pending.label == self.label(pos) && pending.folds_from(pos, held)
                })
    }

    /// The labels a holder may hold for a position to be current: the one the row names, then
    /// the ones pending bytes fold from into it
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn current_labels(&self, pos: Pos) -> Vec<Label> {
        let mut labels = vec![self.label(pos)];
        if let Some(pending) = &self.pending_bytes {
            if pending.label == self.label(pos) {
                labels.extend(pending.sources(pos));
            }
        }
        labels
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
        /// Whether its bytes ride in the commit, into the row's pending bytes, with nothing staged
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        bytes_in_commit: bool,
        /// The placement group's epoch it staged under, which the group refuses it at any other
        /// once its state is the group's (M16)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        epoch: Option<u32>,
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
        /// The placement group's epoch it wrote under (M16)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        epoch: Option<u32>,
    },
    /// A truncate cutting inside this stripe fences it with the epoch it is about to commit
    Fence {
        /// The truncate's identity
        op: OpId,
        /// The epoch
        epoch: Epoch,
    },
    /// The leader's driver clears a row's pending bytes, once enough holders hold their label
    ClearPendingBytes {
        /// The driver's identity
        op: OpId,
        /// The sequence it read
        base: Seq,
        /// The generation it read
        generation: Generation,
        /// The label the pending bytes make
        label: Label,
        /// The positions whose holders said, in its round, that they hold that label durably
        holding: Vec<Pos>,
        /// The placement group's epoch it asked its holders under (M16)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        epoch: Option<u32>,
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
            | StripeCommand::ClearPendingBytes { op, .. }
            | StripeCommand::Reclaim { op, .. } => Some(*op),
            StripeCommand::Noop { .. } => None,
        }
    }

    /// The placement group's epoch the command claims, if it gates on one
    ///
    /// A no-op, a fence and a reclamation change no label a holder holds, so they claim none and
    /// land at any epoch.
    pub fn claim(&self) -> Option<u32> {
        match self {
            StripeCommand::Write { epoch, .. }
            | StripeCommand::Rebuild { epoch, .. }
            | StripeCommand::ClearPendingBytes { epoch, .. } => *epoch,
            StripeCommand::Noop { .. }
            | StripeCommand::Fence { .. }
            | StripeCommand::Reclaim { .. } => None,
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
                counted,
                units,
                bytes_in_commit,
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
                let label = Label {
                    seq: next.seq,
                    tag: *tag,
                };
                // Q27: a small write's bytes go into the row, over what is pending there; a
                // staged write carried any pending bytes in its stages, so they leave with it
                next.pending_bytes = bytes_in_commit
                    .then(|| PendingBytes::after(row, label, units, policy));
                for pos in touched {
                    let index = usize::from(pos.0);
                    next.labels[index] = label;
                    // a small write stages nothing: a position is current if its holder confirmed
                    // the chunk its bytes are laid over, as an untouched chunk counts (Q16)
                    let held = if *bytes_in_commit {
                        counted.iter().any(|(counted, _)| counted == pos)
                    } else {
                        staged.contains(pos)
                    };
                    // POLICY P11, P17: the contract records who missed it; the unsafe setting does not
                    next.missed[index] = policy.missed == MissedRule::Recorded && !held;
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
                // M16: a mark of a stripe with no row makes one, as a put's mark creates its row
                if !*current && policy.placement.grouped() {
                    next.exists = true;
                }
                // a move switches the generation and records the new position in one commit
                if *switch {
                    next.positions[index] = *slice;
                    next.generation = Generation(row.generation.0 + 1);
                }
                next
            }
            StripeCommand::ClearPendingBytes {
                base,
                generation,
                label,
                holding,
                ..
            } => {
                // only the pending bytes it asked about, on the row and map it read, are cleared
                let named = row
                    .pending_bytes
                    .as_ref()
                    .is_some_and(|pending| pending.label == *label);
                if row.seq != *base || row.generation != *generation || !named {
                    return Decision::Refused;
                }
                // a clear moves the sequence, as every command that changes what a holder may
                // serve does, so a stager judges it on the row it read
                let mut next = row.clone();
                next.seq = row.seq.next();
                next.pending_bytes = None;
                // POLICY P17: the contract marks every position whose holder did not say it holds
                // the label missed, and the ones that did current; the unsafe setting leaves them
                if policy.small_writes.clear_missed == MissedRule::Recorded {
                    for (pos, missed) in next.missed.iter_mut().enumerate() {
                        *missed = !holding.contains(&Pos(pos as u8));
                    }
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
                        next.pending_bytes = None;
                    }
                    // a tombstone moves the sequence past every write staged before it
                    ReclaimRule::Tombstoned => {
                        next.exists = true;
                        next.tombstone = true;
                        next.seq = row.seq.next();
                        next.missed = vec![true; row.missed.len()];
                        // stamped with the floor it was reclaimed under, so a reader can tell
                        next.stamp = *below;
                        // whatever was pending is under the floor with the rest
                        next.pending_bytes = None;
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

/// How many writes one stripe's entry in the missed record keeps before it is the whole chunk
///
/// Two, so a model run reaches the cap; the product's is sixteen ranges.
pub const MAX_RANGES: usize = 2;

/// A move in flight: the position whose slice changes, and the slice it changes to
///
/// A move changes one position, which is what one device added, removed or failed changes in a
/// placement group's set: rendezvous moves only what changed ([X2](../../../docs/src/object-storage/placement-simulation.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Moving {
    /// The move's identity, which its switch and its abort name
    pub op: OpId,
    /// The position whose slice changes
    pub pos: Pos,
    /// The slice the position arrives on
    pub to: SliceId,
}

/// A placement group's state in its tablet group (M16)
///
/// Every stripe of the object is in the one placement group the model holds. The epoch moves with
/// every command that changes where a stripe's chunks are staged or read, and every commit that
/// makes a label current names the epoch it staged under: the group refuses it at any other
/// ([S5](../../../docs/src/object-storage/placement.md#generations)).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgState {
    /// Moved by every begin, switch and abort of a move
    pub epoch: u32,
    /// The generation the group's chunks sit under
    pub generation: Generation,
    /// The slice at each position
    pub positions: Vec<SliceId>,
    /// The move in flight, while the group is in both generations
    pub moving: Option<Moving>,
    /// The slices the group left still holding what it placed there, each until it is released
    /// (M20): a move's old slice once it switched, its arrival once it was given up
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub left: Vec<SliceId>,
}

impl PgState {
    /// The state the object's put left: the first generation, every position its put's slice
    ///
    /// # Arguments
    ///
    /// * `positions` - The slices the group was first placed on
    pub fn new(positions: Vec<SliceId>) -> Self {
        Self {
            epoch: 0,
            generation: Generation(0),
            positions,
            moving: None,
            left: Vec::new(),
        }
    }

    /// Whether a slice holds one of the group's positions, or is the arrival of its move in flight
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    pub fn places_on(&self, slice: SliceId) -> bool {
        self.positions.contains(&slice) || self.moving.is_some_and(|moving| moving.to == slice)
    }

    /// The slice a position is read from: its slice in the generation the group is at
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn slice(&self, pos: Pos) -> SliceId {
        self.positions[usize::from(pos.0)]
    }

    /// The slice a moving position arrives on, while a move is in flight
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn arrival(&self, pos: Pos) -> Option<SliceId> {
        self.moving
            .filter(|moving| moving.pos == pos)
            .map(|moving| moving.to)
    }

    /// Every slice a stage, a confirmation or a fold of a position goes to: its own, then its
    /// arrival while the group is in both generations
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn slices(&self, pos: Pos) -> Vec<SliceId> {
        let mut slices = vec![self.slice(pos)];
        slices.extend(self.arrival(pos));
        slices
    }
}

/// What a placement group's tablet group is asked to commit about the group itself (M16)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgCommand {
    /// A move begins: the group is in both generations until it switches or aborts
    MoveBegin {
        /// The move's identity
        op: OpId,
        /// The epoch it read, which it is refused at any other
        expect: u32,
        /// The position whose slice changes
        pos: Pos,
        /// The slice it arrives on
        to: SliceId,
    },
    /// The move's copier is done: the arrival takes the position, under the next generation
    MoveSwitch {
        /// The move's identity
        op: OpId,
    },
    /// The move is given up: the group is back in its one generation
    MoveAbort {
        /// The move's identity
        op: OpId,
    },
    /// A backfill walked every stripe a position missed and found each in the record, so the
    /// record says again that it holds every stripe the position is stale on
    BackfillDone {
        /// The backfill's identity
        op: OpId,
        /// The position
        pos: Pos,
        /// The drops the record had counted when the backfill began
        drops: u32,
    },
    /// A slice the group left was released of what it held, or found holding a position again,
    /// so the group no longer names it as one to release (M20)
    ///
    /// It moves no epoch: nothing a writer stages by or a reader reads from changes.
    ReleaseDone {
        /// The releaser's identity
        op: OpId,
        /// The slice
        slice: SliceId,
    },
}

/// What one stripe's entry in the missed record says a position missed
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Missed {
    /// More than the record keeps, or not known: the chunk is rebuilt whole
    Whole,
    /// Every write the position missed since it was last current, each label with the units it
    /// changed within the chunk
    Ranges {
        /// The label the position held when it was last current: a holder below it may have
        /// dropped a committed write it never applied, which no range here names
        since: Label,
        /// The writes it missed, in order
        writes: Vec<(Label, Vec<u8>)>,
    },
}

/// A map written as a list of pairs, so its keys survive a JSON file whatever their type
mod pairs {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Write the map as its pairs, in key order
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    /// * `serializer` - Where it goes
    pub fn serialize<S, K, V>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: Serialize,
        V: Serialize,
    {
        serializer.collect_seq(map.iter())
    }

    /// Read the map back from its pairs
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where it comes from
    pub fn deserialize<'de, D, K, V>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        D: Deserializer<'de>,
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
    {
        let pairs: Vec<(K, V)> = Vec::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}

/// One position's part of the missed record
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PositionRecord {
    /// Each stripe the position is stale on, and what it missed
    #[serde(with = "pairs")]
    pub stripes: BTreeMap<StripeIx, Missed>,
    /// Whether a stripe was ever dropped for the bound, so a stripe the position is stale on may
    /// be missing from it until a backfill says otherwise
    pub overflowed: bool,
    /// How many stripes were dropped for the bound, ever
    pub drops: u32,
}

/// The missed record: derived at apply, by every replica, from the committed rows alone (S10)
///
/// A row's marks say which positions are stale; the record is the index a rebuild finds them by
/// without a walk, and the ranges it rebuilds a stale chunk by when they are fewer than the chunk
/// ([Q17](../../../docs/src/object-storage/contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09)).
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MissedRecord {
    /// Each position's part
    pub positions: Vec<PositionRecord>,
}

/// What a committed stripe command changed of a chunk, for the positions it leaves stale
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Changed {
    /// No chunk's bytes: a no-op, a fence, a rebuild that made a chunk current
    Nothing,
    /// Some units of the chunk, under a label
    Units(Label, Vec<u8>),
    /// The whole chunk
    Whole(Label),
}

impl MissedRecord {
    /// An empty record for a layout's positions
    ///
    /// # Arguments
    ///
    /// * `layout` - The pool's layout
    pub fn new(layout: Layout) -> Self {
        Self {
            positions: vec![PositionRecord::default(); layout.width()],
        }
    }

    /// What one stripe's entry says a position missed
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `pos` - The position
    pub fn entry(&self, stripe: StripeIx, pos: Pos) -> Option<&Missed> {
        self.positions[usize::from(pos.0)].stripes.get(&stripe)
    }

    /// Derive the record from a committed row: a position the row leaves stale is recorded with
    /// what the command changed, and one it leaves current is dropped
    ///
    /// The row's marks are the whole truth; what a command changed is all it adds. A position
    /// already stale and recorded gains the command's units; one with no entry gains one, unless
    /// the bound is reached, which drops it and says so. Once the record has dropped a stripe, a
    /// stripe it has no entry for may have been dropped, so its first entry is the whole chunk.
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `before` - The row the command committed over, for the label a newly stale position held
    /// * `after` - The row the command produced
    /// * `changed` - What the command changed, at each position it touched
    /// * `bound` - How many stripes one position's part may hold
    pub fn derive(
        &mut self,
        stripe: StripeIx,
        before: &RowState,
        after: &RowState,
        changed: &[(Pos, Changed)],
        bound: usize,
    ) {
        for (pos, part) in self.positions.iter_mut().enumerate() {
            let pos = Pos(pos as u8);
            // a reclaimed or current position is stale on nothing
            if after.tombstone || !after.missed[usize::from(pos.0)] {
                part.stripes.remove(&stripe);
                continue;
            }
            let change = changed
                .iter()
                .find(|(at, _)| *at == pos)
                .map(|(_, change)| change.clone())
                .unwrap_or(Changed::Nothing);
            let (label, units) = match change {
                // nothing changed here: the entry, or its absence, stands
                Changed::Nothing => continue,
                Changed::Units(label, units) => (label, Some(units)),
                Changed::Whole(label) => (label, None),
            };
            match part.stripes.get_mut(&stripe) {
                Some(Missed::Whole) => {}
                Some(Missed::Ranges { writes, .. }) => match units {
                    Some(units) if writes.len() < MAX_RANGES => writes.push((label, units)),
                    _ => {
                        part.stripes.insert(stripe, Missed::Whole);
                    }
                },
                None => {
                    // the bound: a stripe past it is dropped, and the record says it overflowed
                    if part.stripes.len() >= bound {
                        part.overflowed = true;
                        part.drops += 1;
                        continue;
                    }
                    // the entry begins at the label the row named here before this command: a
                    // holder at it or past it lacks only what the entry records from now on. A
                    // position stale already, whose earlier entry the bound dropped, named a write
                    // its holder missed, so the holder is below it and is given the whole chunk
                    let entry = match units {
                        Some(units) => Missed::Ranges {
                            since: before.label(pos),
                            writes: vec![(label, units)],
                        },
                        None => Missed::Whole,
                    };
                    part.stripes.insert(stripe, entry);
                }
            }
        }
    }

    /// The units a holder at a label has to be given to make the row's state, if the record
    /// knows them: every write after its label the stripe's entry recorded
    ///
    /// None is the whole chunk: no entry, a whole entry, or a holder at no label.
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `pos` - The position
    /// * `held` - The label the holder's chunk is at, if it has one
    pub fn units_after(&self, stripe: StripeIx, pos: Pos, held: Option<Label>) -> Option<Vec<u8>> {
        self.entry(stripe, pos)?.units_after(held?)
    }
}

impl Missed {
    /// The units a holder at a label has to be given to make the row's state: every write after
    /// its label this entry recorded; none for the whole chunk
    ///
    /// # Arguments
    ///
    /// * `held` - The label the holder's chunk is at
    pub fn units_after(&self, held: Label) -> Option<Vec<u8>> {
        let Missed::Ranges { since, writes } = self else {
            return None;
        };
        // a holder below the label the position was last current at fell back past a write this
        // entry never saw: only the whole chunk makes it current
        if held.seq < since.seq {
            return None;
        }
        let mut units: Vec<u8> = writes
            .iter()
            .filter(|(label, _)| label.seq > held.seq)
            .flat_map(|(_, units)| units.iter().copied())
            .collect();
        units.sort_unstable();
        units.dedup();
        // a holder past every write recorded holds the row's state, or something the record
        // cannot explain: either way the chunk is written whole
        (!units.is_empty()).then_some(units)
    }
}

/// A placement group's tablet group, as far as the group's own state goes (M16)
///
/// In the product the group's rows and its placement state are one tablet group's; here the rows
/// keep their own histories, and every stripe command's claim is judged against this state in the
/// same step it commits, so the two are ordered as one group orders them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgGroup {
    /// Every committed state, the first the group as the put placed it
    pub history: Vec<PgState>,
    /// The command that produced each state after the first
    pub commands: Vec<PgCommand>,
    /// The missed record, derived from every stripe's committed rows
    pub record: MissedRecord,
    /// Every step the record was derived by, in committed order: what the product's derivation
    /// is replayed in
    pub derived: Vec<RecordStep>,
    /// Whether a switch and an abort name the slice the group leaves, for M20's release
    pub releases: bool,
    /// The step each state in the history was committed at, the put's at zero, for the checker
    pub steps: Vec<u64>,
}

/// One step of the missed record's derivation, in committed order
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordStep {
    /// A stripe commit, by its stripe and the index of the state it produced
    Row(StripeIx, u32),
    /// A backfill that said a position's record holds every stale stripe again
    Backfill {
        /// The position
        pos: Pos,
        /// The drops it read when it began
        drops: u32,
    },
}

impl PgGroup {
    /// The group of an object's put
    ///
    /// # Arguments
    ///
    /// * `layout` - The pool's layout
    /// * `positions` - The slices it was placed on
    /// * `releases` - Whether the slices the group leaves are named for M20's release
    pub fn new(layout: Layout, positions: Vec<SliceId>, releases: bool) -> Self {
        Self {
            history: vec![PgState::new(positions)],
            commands: Vec::new(),
            record: MissedRecord::new(layout),
            derived: Vec::new(),
            releases,
            steps: vec![0],
        }
    }

    /// The latest committed state
    pub fn latest(&self) -> &PgState {
        self.history.last().expect("the put's state")
    }

    /// The state a replica `lag` commits behind answers with
    ///
    /// # Arguments
    ///
    /// * `lag` - How far behind
    pub fn lagging(&self, lag: u8) -> &PgState {
        let index = (self.history.len() - 1).saturating_sub(usize::from(lag));
        &self.history[index]
    }

    /// Apply a command in committed order, judging its condition here
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command
    /// * `policy` - The policy in force
    pub fn propose(&mut self, cmd: &PgCommand) -> Decision {
        let latest = self.latest().clone();
        let mut next = latest.clone();
        let committed = match cmd {
            PgCommand::MoveBegin {
                op,
                expect,
                pos,
                to,
            } => {
                // one move at a time, from the state its driver read, to a slice not in the set
                let free = !latest.positions.contains(to);
                if latest.moving.is_some() || latest.epoch != *expect || !free {
                    false
                } else {
                    next.moving = Some(Moving {
                        op: *op,
                        pos: *pos,
                        to: *to,
                    });
                    next.epoch += 1;
                    true
                }
            }
            PgCommand::MoveSwitch { op } => match latest.moving {
                // the arrival takes its position, and the generation and positions move together
                Some(moving) if moving.op == *op => {
                    let old = latest.slice(moving.pos);
                    next.positions[usize::from(moving.pos.0)] = moving.to;
                    next.generation = Generation(latest.generation.0 + 1);
                    next.moving = None;
                    next.epoch += 1;
                    // M20: the old slice is left holding the position's chunks, and an arrival the
                    // group left before holds a position again
                    if self.releases {
                        next.left.retain(|slice| *slice != moving.to);
                        if !next.left.contains(&old) {
                            next.left.push(old);
                        }
                    }
                    true
                }
                _ => false,
            },
            PgCommand::MoveAbort { op } => match latest.moving {
                Some(moving) if moving.op == *op => {
                    next.moving = None;
                    next.epoch += 1;
                    // M20: the arrival is left holding whatever the copier brought it
                    if self.releases && !next.left.contains(&moving.to) {
                        next.left.push(moving.to);
                    }
                    true
                }
                _ => false,
            },
            PgCommand::ReleaseDone { slice, .. } => {
                // only a slice the group still names, and only once
                if !latest.left.contains(slice) {
                    false
                } else {
                    next.left.retain(|left| left != slice);
                    true
                }
            }
            PgCommand::BackfillDone { pos, drops, .. } => {
                self.derived.push(RecordStep::Backfill {
                    pos: *pos,
                    drops: *drops,
                });
                let part = &mut self.record.positions[usize::from(pos.0)];
                // the record is whole again only if nothing was dropped while the backfill walked
                if part.drops == *drops {
                    part.overflowed = false;
                }
                // the group's own state is unchanged; the command is recorded all the same
                self.commands.push(cmd.clone());
                return if part.overflowed {
                    Decision::Refused
                } else {
                    Decision::Committed {
                        index: (self.history.len() - 1) as u32,
                    }
                };
            }
        };
        if !committed {
            return Decision::Refused;
        }
        self.history.push(next);
        self.commands.push(cmd.clone());
        Decision::Committed {
            index: (self.history.len() - 1) as u32,
        }
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
            bytes_in_commit: false,
            epoch: None,
        }
    }

    /// A small write's commit of one unit, its positions counted on their holders' confirmations
    fn small_write(op: u32, base: u32, unit: u8, confirmed: &[u8]) -> StripeCommand {
        StripeCommand::Write {
            op: OpId(op),
            attempt: 0,
            base_exists: base > 0,
            base: Seq(base),
            generation: Generation(0),
            read_epoch: Epoch(0),
            tag: Tag(op),
            touched: vec![Pos(0), Pos(1), Pos(2)],
            staged: Vec::new(),
            counted: confirmed
                .iter()
                .map(|pos| (Pos(*pos), Evidence::Confirmed))
                .collect(),
            units: vec![(unit, Unit::Write(OpId(op)))],
            bytes_in_commit: true,
            epoch: None,
        }
    }

    /// A small write's bytes go into the row over its labels, and a second merges over the first
    #[test]
    fn a_small_write_commits_its_bytes_and_a_second_merges_over_them() {
        let policy = StripePolicy::safe().in_commit();
        let mut group = group();
        group.propose(&small_write(1, 0, 1, &[0, 1]), &policy);
        let (_, row) = group.latest();
        let first = row.label(Pos(0));
        let pending = row.pending_bytes.clone().expect("pending");
        assert_eq!(pending.base, vec![Label::PUT; 3]);
        assert_eq!(pending.units, vec![(1, Unit::Write(OpId(1)))]);
        // the position whose holder did not confirm its base is missed, the others current
        assert_eq!(row.missed, vec![false, false, true]);
        assert!(row.makes(Pos(0), Label::PUT) && row.makes(Pos(0), first));
        // a second over the same unit merges: same base, the first's label in the chain
        group.propose(&small_write(2, 1, 1, &[0, 1]), &policy);
        let (_, row) = group.latest();
        let pending = row.pending_bytes.clone().expect("pending");
        assert_eq!(pending.base, vec![Label::PUT; 3]);
        assert_eq!(pending.chain, vec![first]);
        assert_eq!(pending.units, vec![(1, Unit::Write(OpId(2)))]);
        assert!(row.makes(Pos(1), first) && row.makes(Pos(1), Label::PUT));
        // and a staged write, which carried them in its stages, leaves with them
        group.propose(&write(3, 2, 0), &policy);
        assert!(group.latest().1.pending_bytes.is_none());
    }

    /// A clear needs the bytes it asked about on the row it read, moves the sequence, and marks
    /// every position whose holder did not say it holds them missed
    #[test]
    fn a_clear_moves_the_sequence_and_marks_the_rest_missed() {
        let policy = StripePolicy::safe().in_commit();
        let mut group = group();
        group.propose(&small_write(1, 0, 1, &[0, 1, 2]), &policy);
        let label = group.latest().1.label(Pos(0));
        let clear = |base: u32, label: Label| StripeCommand::ClearPendingBytes {
            op: OpId(9),
            base: Seq(base),
            generation: Generation(0),
            label,
            holding: vec![Pos(0), Pos(2)],
            epoch: None,
        };
        assert_eq!(group.propose(&clear(0, label), &policy), Decision::Refused);
        assert_eq!(group.propose(&clear(1, Label::PUT), &policy), Decision::Refused);
        assert!(matches!(
            group.propose(&clear(1, label), &policy),
            Decision::Committed { .. }
        ));
        let (_, row) = group.latest();
        assert!(row.pending_bytes.is_none() && row.seq == Seq(2));
        assert_eq!(row.missed, vec![false, true, false]);
        // as one might write it, the position that does not hold the label stays current
        let leaving = StripePolicy {
            small_writes: crate::stripe::policy::SmallWriteRules {
                clear_missed: MissedRule::NotRecorded,
                ..policy.small_writes
            },
            ..policy
        };
        let mut left = group_of_one_small_write(&leaving);
        left.propose(&clear(1, label), &leaving);
        assert_eq!(left.latest().1.missed, vec![false, false, false]);
    }

    /// A group holding one small write that every holder confirmed
    fn group_of_one_small_write(policy: &StripePolicy) -> StripeGroup {
        let mut group = group();
        group.propose(&small_write(1, 0, 1, &[0, 1, 2]), policy);
        group
    }

    /// A reclamation takes the pending bytes with the rest of the stripe
    #[test]
    fn a_tombstone_takes_the_pending_bytes() {
        let policy = StripePolicy::safe().in_commit();
        let mut group = group_of_one_small_write(&policy);
        group.propose(
            &StripeCommand::Reclaim {
                op: OpId(9),
                base_exists: true,
                base: Seq(1),
                below: Epoch(1),
            },
            &policy,
        );
        let (_, row) = group.latest();
        assert!(row.tombstone && row.pending_bytes.is_none());
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
        assert_eq!(entry.propose(&late, &StripePolicy::safe()), Decision::Refused);
        // one that read another size would put its floor somewhere it did not fence
        let moved = EntryCommand::Truncate {
            op: OpId(4),
            len: 2,
            expect: Some(Epoch(0)),
            size: Some(4),
        };
        assert_eq!(entry.propose(&moved, &StripePolicy::safe()), Decision::Refused);
        let cut = EntryCommand::Truncate {
            op: OpId(2),
            len: 2,
            expect: Some(Epoch(0)),
            size: Some(8),
        };
        assert_eq!(entry.propose(&cut, &StripePolicy::safe()), Decision::Committed { index: 1 });
        let grow = EntryCommand::Truncate {
            op: OpId(3),
            len: 6,
            expect: None,
            size: None,
        };
        entry.propose(&grow, &StripePolicy::safe());
        let (_, state) = entry.latest();
        assert_eq!(state.size, 6);
        assert_eq!(state.floors.last().map(|floor| floor.len), Some(2));
        assert_eq!(state.epoch, Epoch(2));
    }
}
