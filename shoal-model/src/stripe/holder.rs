//! Disks, nodes and slices: where stripe chunks are staged, applied and lost
//!
//! A slice holds one chunk of every stripe placed on it, the writes staged against them, and,
//! when the policy keeps one, each chunk's previous state. What is synced survives a crash; what
//! is not is lost with it. An apply in place is two steps and a third that drops the staged copy,
//! so a crash can tear it, and a restart writes a torn apply again from the staged copy
//! ([S6](../../../docs/src/object-storage/device-store.md#staging-two-cases)).
//!
//! A slice never decides anything. It applies a staged write when it learns the row names its
//! label, and discards one when it learns a committed fact that excludes it; the
//! [`crate::stripe::check`] module judges every discard and every byte against the row.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::NodeId;
use crate::stripe::content::{fold_all, rows_of, Change, Content, ParityUnit, Unit};
use crate::stripe::event::{ChunkReply, Endpoint, Payload, StageRefusal};
use crate::stripe::group::RowState;
use crate::stripe::ids::{DeviceId, DiskId, Label, Pos, Seq, SliceId, StripeIx};
use crate::stripe::policy::{
    ApplyTiming, BeneathRule, DiscardView, LabelRule, ParityRecord, PreviousState, SpaceRule,
    StagedCopy, StripePolicy,
};

/// A physical disk
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disk {
    /// Its identity as a thing that fails
    pub id: DiskId,
    /// Whether it answers; a failed disk errors on every call and never comes back
    pub healthy: bool,
    /// Whether something else has filled it
    pub full: bool,
    /// The device and slice markers written on it when it was claimed, if it has been
    pub marker: Option<(DeviceId, SliceId)>,
}

/// A node: one bay, one disk in it, and the slice it serves from that disk
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// The node
    pub id: NodeId,
    /// Whether it is running
    pub up: bool,
    /// The disk in its bay
    pub disk: DiskId,
    /// The slice it serves, once it has claimed its disk
    pub slice: Option<SliceId>,
    /// Whether it has reported its disk failed, so the pool map marks it
    pub reported: bool,
}

/// A stripe chunk as a holder has it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// The position its header names
    pub pos: Pos,
    /// The label in its header
    pub label: Label,
    /// Its units
    pub content: Content,
}

/// What a staged record holds
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Record {
    /// A whole chunk, in a file beside the old one
    Whole(Content),
    /// New values for some units of a data chunk
    Units(Vec<(u8, Unit)>),
    /// New values for some units of a parity chunk
    Parity(Vec<(u8, ParityUnit)>),
    /// A change to fold into parity: a patch, which the contract forbids
    Patch(Vec<Change>),
}

impl Record {
    /// The chunk this record makes of a chunk
    ///
    /// # Arguments
    ///
    /// * `chunk` - What the holder has now
    pub fn apply(&self, chunk: Option<&Content>) -> Option<Content> {
        match (self, chunk) {
            (Record::Whole(content), _) => Some(content.clone()),
            (Record::Units(units), Some(Content::Data(old))) => {
                let mut new = old.clone();
                for (unit, value) in units {
                    new[usize::from(*unit)] = *value;
                }
                Some(Content::Data(new))
            }
            (Record::Parity(units), Some(Content::Parity(old))) => {
                let mut new = old.clone();
                for (unit, value) in units {
                    new[usize::from(*unit)] = value.clone();
                }
                Some(Content::Parity(new))
            }
            (Record::Patch(changes), Some(content @ Content::Parity(_))) => {
                Some(fold_all(content, changes))
            }
            // a change to part of a chunk the holder does not have makes nothing
            _ => None,
        }
    }

    /// The units an apply in place writes, which a crash during it tears
    pub fn units_written(&self) -> Vec<usize> {
        match self {
            Record::Whole(_) => Vec::new(),
            Record::Units(units) => units.iter().map(|(unit, _)| usize::from(*unit)).collect(),
            Record::Parity(units) => units.iter().map(|(unit, _)| usize::from(*unit)).collect(),
            Record::Patch(changes) => rows_of(changes),
        }
    }

    /// Whether this is a whole chunk, applied by a rename that cannot tear
    pub fn is_whole(&self) -> bool {
        matches!(self, Record::Whole(_))
    }
}

/// A write staged on a slice
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Staged {
    /// The position the chunk is at
    pub pos: Pos,
    /// The write's label
    pub label: Label,
    /// Whether the row existed when the write read it
    pub base_exists: bool,
    /// The sequence the write was staged against
    pub base: Seq,
    /// The label the chunk had to carry, for a change to part of it
    pub expects: Option<Label>,
    /// The bytes
    pub record: Record,
    /// Whether it holds space taken at the stage
    pub reserved: bool,
}

/// Where an apply in place has got to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The units and header are being written in place; a crash now tears them
    Writing,
    /// They are synced; the staged copy has not been dropped
    Synced,
}

/// An apply under way
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applying {
    /// The position the chunk is at
    pub pos: Pos,
    /// The label being applied
    pub label: Label,
    /// The record, kept here so an apply whose staged copy was dropped can finish
    pub record: Record,
    /// How far it has got
    pub phase: Phase,
}

/// A stage that has not been synced, and who to answer once it is
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsynced {
    /// The stripe
    pub stripe: StripeIx,
    /// The record
    pub staged: Staged,
    /// Who sent it
    pub reply_to: Endpoint,
}

/// Something a slice did that the checker judges
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HolderFact {
    /// A staged record was discarded
    DiscardedStage {
        /// The stripe
        stripe: StripeIx,
        /// What was discarded
        staged: Staged,
    },
    /// A chunk was discarded
    DiscardedChunk {
        /// The stripe
        stripe: StripeIx,
        /// Its label
        label: Label,
    },
    /// A staged record was written again after a restart
    Replayed {
        /// The stripe
        stripe: StripeIx,
        /// Its label
        label: Label,
        /// The chunk before
        before: Content,
        /// The chunk after
        after: Option<Content>,
    },
    /// An apply of a committed write found no space for itself
    ApplyFailedForSpace {
        /// The stripe
        stripe: StripeIx,
        /// Its label
        label: Label,
    },
    /// An apply finished
    Applied {
        /// The stripe
        stripe: StripeIx,
        /// Its label
        label: Label,
    },
}

/// A slice and everything on it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slice {
    /// The slice
    pub id: SliceId,
    /// The device it is on
    pub device: DeviceId,
    /// The disk its bytes are on
    pub disk: DiskId,
    /// The node that serves it
    pub node: NodeId,
    /// Whether its device is gone: replaced, or claimed under another identity
    pub gone: bool,
    /// The chunks, synced
    pub chunks: BTreeMap<StripeIx, Chunk>,
    /// Each chunk's previous state, when the policy keeps one
    pub previous: BTreeMap<StripeIx, Chunk>,
    /// Staged writes, synced
    pub staged: BTreeMap<(StripeIx, Label), Staged>,
    /// Stages received and not yet synced, lost by a crash
    pub unsynced: Vec<Unsynced>,
    /// Labels this slice has learned are committed and not yet applied, lost by a crash
    pub committed: BTreeSet<(StripeIx, Label)>,
    /// Applies under way, lost by a crash, which tears the ones still writing
    pub applying: BTreeMap<StripeIx, Applying>,
}

/// Whether two labels are the same, as the policy compares them
///
/// # Arguments
///
/// * `a` - One label
/// * `b` - The other
/// * `policy` - The policy in force
pub fn same(a: Label, b: Label, policy: &StripePolicy) -> bool {
    // POLICY P9, P10: the contract compares the tag; the unsafe setting the sequence alone
    match policy.label {
        LabelRule::SequenceAndTag => a == b,
        LabelRule::SequenceOnly => a.seq == b.seq,
    }
}

impl Slice {
    /// A slice on a disk, holding nothing
    ///
    /// # Arguments
    ///
    /// * `id` - The slice
    /// * `device` - Its device
    /// * `disk` - Its disk
    /// * `node` - The node serving it
    pub fn new(id: SliceId, device: DeviceId, disk: DiskId, node: NodeId) -> Self {
        Self {
            id,
            device,
            disk,
            node,
            gone: false,
            chunks: BTreeMap::new(),
            previous: BTreeMap::new(),
            staged: BTreeMap::new(),
            unsynced: Vec::new(),
            committed: BTreeSet::new(),
            applying: BTreeMap::new(),
        }
    }

    /// The staged writes a label stands on at a position, newest first, down to the chunk
    ///
    /// A change to part of a chunk is staged as the units it changed, over the label it expects,
    /// so the label it makes is its record over that one: the chunk's, or another record staged
    /// beneath it. Empty when the label is the chunk's own; `None` when nothing here reaches it.
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `pos` - The position
    /// * `label` - The label
    fn chain(&self, stripe: StripeIx, pos: Pos, label: Label) -> Option<Vec<&Staged>> {
        let chunk = self.chunks.get(&stripe).filter(|chunk| chunk.pos == pos);
        let mut records: Vec<&Staged> = Vec::new();
        let mut next = label;
        loop {
            // the chunk is where every chain ends that does not end in a whole record
            if chunk.is_some_and(|chunk| chunk.label == next) {
                return Some(records);
            }
            let staged = self
                .staged
                .get(&(stripe, next))
                .filter(|staged| staged.pos == pos)?;
            records.push(staged);
            match staged.expects {
                // a whole chunk stands on nothing
                None => return Some(records),
                Some(expects) => next = expects,
            }
            // labels only fall going down, so a chain longer than the records is no chain
            if records.len() > self.staged.len() {
                return None;
            }
        }
    }

    /// The chunk as it will be once every write learned committed is applied
    ///
    /// Each committed write is laid over the label it expects, never over another: a change to
    /// part of a chunk whose base this holder cannot make makes nothing.
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `policy` - The policy in force
    fn effective(&self, stripe: StripeIx, policy: &StripePolicy) -> Option<Chunk> {
        let mut chunk = self.chunks.get(&stripe).cloned();
        // the apply under way, then each committed write in label order
        if let Some(applying) = self.applying.get(&stripe) {
            if applying.phase == Phase::Writing {
                chunk = applying
                    .record
                    .apply(chunk.as_ref().map(|chunk| &chunk.content))
                    .map(|content| Chunk {
                        pos: applying.pos,
                        label: applying.label,
                        content,
                    });
            }
        }
        for (s, label) in &self.committed {
            // a committed write the chunk has already passed changes nothing
            let passed = chunk
                .as_ref()
                .is_some_and(|chunk| chunk.label.seq >= label.seq);
            if *s != stripe || passed {
                continue;
            }
            let Some(staged) = self.staged.get(&(stripe, *label)) else {
                continue;
            };
            // a change to part of the chunk goes only over the label it was staged against
            let base = match (staged.expects, &chunk) {
                (None, _) => true,
                (Some(expects), Some(chunk)) => same(chunk.label, expects, policy),
                (Some(_), None) => false,
            };
            if !base {
                continue;
            }
            chunk = staged
                .record
                .apply(chunk.as_ref().map(|chunk| &chunk.content))
                .map(|content| Chunk {
                    pos: staged.pos,
                    label: *label,
                    content,
                });
        }
        chunk
    }

    /// Take a stage, answering at once only if it is refused or already synced
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `staged` - The record as the stage describes it, before the holder makes its bytes
    /// * `payload` - The bytes the stage carried
    /// * `full` - Whether the disk is full
    /// * `reply_to` - Who to answer
    /// * `policy` - The policy in force
    pub fn stage(
        &mut self,
        stripe: StripeIx,
        mut staged: Staged,
        payload: Payload,
        full: bool,
        reply_to: Endpoint,
        policy: &StripePolicy,
    ) -> Option<Option<StageRefusal>> {
        // a slice holds one position of a stripe: placement never puts two on one slice
        let elsewhere = self
            .chunks
            .get(&stripe)
            .is_some_and(|chunk| chunk.pos != staged.pos)
            || self
                .staged
                .iter()
                .any(|((s, _), other)| *s == stripe && other.pos != staged.pos)
            || self
                .unsynced
                .iter()
                .any(|pending| pending.stripe == stripe && pending.staged.pos != staged.pos);
        if elsewhere {
            return Some(Some(StageRefusal::Label));
        }
        // staging the same write twice is staging it once
        // staging the same write twice is staging it once, while this holder can make it; a record
        // whose base is gone is no copy of the write, and a new stage of its label replaces it
        if self.staged.contains_key(&(stripe, staged.label)) {
            if self.holds(stripe, staged.pos, staged.label) {
                return Some(None);
            }
            self.staged.remove(&(stripe, staged.label));
        }
        if self
            .unsynced
            .iter()
            .any(|pending| pending.stripe == stripe && pending.staged.label == staged.label)
        {
            return None;
        }
        // a change to part of a chunk needs the chunk at the label the write read, verified
        let effective = self.effective(stripe, policy);
        if let Some(expects) = staged.expects {
            match &effective {
                Some(chunk) if same(chunk.label, expects, policy) && !chunk.content.is_torn() => {}
                _ => return Some(Some(StageRefusal::Label)),
            }
        }
        // POLICY P7, P11: the contract takes the space at the stage; the unsafe setting at the apply
        if policy.space == SpaceRule::AtStage {
            if full {
                return Some(Some(StageRefusal::Full));
            }
            staged.reserved = true;
        }
        // the holder makes the record's bytes: new values, or for the unsafe setting a patch
        staged.record = match payload {
            Payload::Whole(content) => Record::Whole(content),
            Payload::Units(units) => Record::Units(units),
            Payload::Delta(changes) => match policy.parity_record {
                ParityRecord::NewValues => {
                    let old = effective.map(|chunk| chunk.content);
                    match old.as_ref().map(|content| fold_all(content, &changes)) {
                        Some(Content::Parity(units)) => Record::Parity(
                            rows_of(&changes)
                                .into_iter()
                                .map(|row| (row as u8, units[row].clone()))
                                .collect(),
                        ),
                        _ => return Some(Some(StageRefusal::Label)),
                    }
                }
                ParityRecord::Patch => Record::Patch(changes),
            },
        };
        self.unsynced.push(Unsynced {
            stripe,
            staged,
            reply_to,
        });
        None
    }

    /// Sync what was staged, and say who to answer
    ///
    /// # Arguments
    ///
    /// * `policy` - The policy in force
    pub fn sync(&mut self, policy: &StripePolicy) -> Vec<(Endpoint, StripeIx, Pos, Label)> {
        let mut answers = Vec::new();
        for pending in std::mem::take(&mut self.unsynced) {
            let key = (pending.stripe, pending.staged.label);
            answers.push((
                pending.reply_to,
                pending.stripe,
                pending.staged.pos,
                pending.staged.label,
            ));
            // POLICY P9: the contract waits for the commit; the unsafe setting applies at once
            if policy.apply_timing == ApplyTiming::BeforeCommit {
                self.committed.insert(key);
            }
            self.staged.insert(key, pending.staged);
        }
        answers
    }

    /// Learn that the row names a label: the write it belongs to is committed
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `label` - The label the row names
    /// * `policy` - The policy in force
    pub fn learn_named(&mut self, stripe: StripeIx, label: Label, policy: &StripePolicy) {
        let staged: Vec<Label> = self
            .staged
            .keys()
            .chain(
                self.unsynced
                    .iter()
                    .map(|pending| (pending.stripe, pending.staged.label))
                    .collect::<Vec<_>>()
                    .iter(),
            )
            .filter(|(s, l)| *s == stripe && same(*l, label, policy))
            .map(|(_, l)| *l)
            .collect();
        for label in staged {
            self.committed.insert((stripe, label));
        }
    }

    /// Learn a state of a stripe's row: apply what it names, discard what it excludes
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `row` - The state, from whichever replica answered
    /// * `policy` - The policy in force
    pub fn learn_row(
        &mut self,
        stripe: StripeIx,
        row: &RowState,
        policy: &StripePolicy,
    ) -> Vec<HolderFact> {
        let mut facts = Vec::new();
        let records: Vec<Staged> = self
            .staged
            .iter()
            .filter(|((s, _), _)| *s == stripe)
            .map(|(_, staged)| staged.clone())
            .collect();
        // POLICY P16, P17: the contract keeps every committed write a label the row names stands
        // on, since that label is its record over them; S10 as written kept only what is named
        let beneath: BTreeSet<Label> = if row.exists && policy.beneath == BeneathRule::Kept {
            records
                .iter()
                .filter(|staged| same(row.label(staged.pos), staged.label, policy))
                .filter_map(|staged| self.chain(stripe, staged.pos, staged.label))
                .flat_map(|chain| chain.into_iter().map(|record| record.label))
                .collect()
        } else {
            BTreeSet::new()
        };
        for staged in records {
            let named = same(row.label(staged.pos), staged.label, policy) && row.exists;
            // a write a named label stands on committed too: its stager read it from a row
            if named || beneath.contains(&staged.label) {
                self.committed.insert((stripe, staged.label));
                continue;
            }
            // POLICY P16: the contract discards only on a fact that excludes the write for good:
            // the row moved past its base and names another label. The unsafe setting discards
            // on any view that does not name it, a lagging replica's absence included
            let excluded = match policy.discard_view {
                DiscardView::CommittedFact => {
                    // a row past the base excludes the write for good; a view with no row, or one
                    // at or below the base, may be a replica that has not caught up, and says nothing
                    let moved = row.exists && row.seq > staged.base;
                    moved && !same(row.label(staged.pos), staged.label, policy)
                }
                DiscardView::AnyView => true,
            };
            // POLICY P7, P9: the contract's staged copy outlives its apply even once a fact
            // excludes it, since a crash before the apply is synced tears the chunk and only the
            // copy writes it again; the apply's last step drops it
            let in_apply = self
                .applying
                .get(&stripe)
                .is_some_and(|applying| applying.label == staged.label);
            if in_apply && policy.staged_copy == StagedCopy::OutlivesApply {
                continue;
            }
            if excluded {
                self.staged.remove(&(stripe, staged.label));
                self.committed.remove(&(stripe, staged.label));
                facts.push(HolderFact::DiscardedStage { stripe, staged });
            }
        }
        facts
    }

    /// Drop a staged write because a stager said to (an unsafe setting only)
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `label` - The write's label
    /// * `policy` - The policy in force
    pub fn drop_label(
        &mut self,
        stripe: StripeIx,
        label: Label,
        policy: &StripePolicy,
    ) -> Vec<HolderFact> {
        let labels: Vec<Label> = self
            .staged
            .keys()
            .filter(|(s, l)| *s == stripe && same(*l, label, policy))
            .map(|(_, l)| *l)
            .collect();
        let mut facts = Vec::new();
        for label in labels {
            if let Some(staged) = self.staged.remove(&(stripe, label)) {
                self.committed.remove(&(stripe, label));
                facts.push(HolderFact::DiscardedStage { stripe, staged });
            }
        }
        facts
    }

    /// Discard a reclaimed stripe's chunk, if it is from before the reclamation
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `through` - The sequence the reclamation committed past
    pub fn discard_stripe(&mut self, stripe: StripeIx, through: Seq) -> Vec<HolderFact> {
        let mut facts = Vec::new();
        self.previous.remove(&stripe);
        // a chunk a later write made is not the reclaimed stripe's
        if self
            .chunks
            .get(&stripe)
            .is_some_and(|chunk| chunk.label.seq <= through)
        {
            let chunk = self.chunks.remove(&stripe).expect("checked");
            facts.push(HolderFact::DiscardedChunk {
                stripe,
                label: chunk.label,
            });
        }
        facts
    }

    /// Take one step of applying a write learned committed
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `full` - Whether the disk is full
    /// * `policy` - The policy in force
    pub fn apply_step(
        &mut self,
        stripe: StripeIx,
        full: bool,
        policy: &StripePolicy,
    ) -> Option<Vec<HolderFact>> {
        // an apply under way takes its next step
        if let Some(applying) = self.applying.get_mut(&stripe) {
            match applying.phase {
                Phase::Writing => {
                    // the units and header are synced in place
                    let (pos, label) = (applying.pos, applying.label);
                    let old = self.chunks.get(&stripe).cloned();
                    let content = applying
                        .record
                        .apply(old.as_ref().map(|chunk| &chunk.content))?;
                    applying.phase = Phase::Synced;
                    if policy.previous == PreviousState::Kept {
                        if let Some(old) = old {
                            self.previous.insert(stripe, old);
                        }
                    }
                    self.chunks.insert(
                        stripe,
                        Chunk {
                            pos,
                            label,
                            content,
                        },
                    );
                    return Some(Vec::new());
                }
                Phase::Synced => {
                    // and only then is the staged copy dropped
                    let label = applying.label;
                    self.applying.remove(&stripe);
                    self.staged.remove(&(stripe, label));
                    self.committed.remove(&(stripe, label));
                    return Some(vec![HolderFact::Applied { stripe, label }]);
                }
            }
        }
        // otherwise the lowest committed write whose base the chunk is at, and never one older
        // than what the chunk holds: a lagging view can name a label a newer write has passed
        let current = self.chunks.get(&stripe).cloned();
        let next = self
            .committed
            .iter()
            .filter(|(s, _)| *s == stripe)
            .filter_map(|(_, label)| self.staged.get(&(stripe, *label)))
            .filter(|staged| {
                current
                    .as_ref()
                    .is_none_or(|chunk| chunk.label.seq < staged.label.seq)
            })
            .find(|staged| match staged.expects {
                None => true,
                Some(expects) => current
                    .as_ref()
                    .is_some_and(|chunk| same(chunk.label, expects, policy)),
            })
            .cloned()?;
        // POLICY P7: the contract took the space at the stage; the unsafe setting needs it now
        if !next.reserved && full && policy.space == SpaceRule::AtApply {
            return Some(vec![HolderFact::ApplyFailedForSpace {
                stripe,
                label: next.label,
            }]);
        }
        // a whole chunk is renamed over the old one, which cannot tear
        if next.record.is_whole() {
            let content = next.record.apply(None)?;
            if policy.previous == PreviousState::Kept {
                if let Some(old) = current {
                    self.previous.insert(stripe, old);
                }
            }
            self.chunks.insert(
                stripe,
                Chunk {
                    pos: next.pos,
                    label: next.label,
                    content,
                },
            );
            self.staged.remove(&(stripe, next.label));
            self.committed.remove(&(stripe, next.label));
            return Some(vec![HolderFact::Applied {
                stripe,
                label: next.label,
            }]);
        }
        // POLICY P7, P9: the contract keeps the staged copy until the apply is synced
        if policy.staged_copy == StagedCopy::DroppedAtApplyStart {
            self.staged.remove(&(stripe, next.label));
        }
        self.applying.insert(
            stripe,
            Applying {
                pos: next.pos,
                label: next.label,
                record: next.record,
                phase: Phase::Writing,
            },
        );
        Some(Vec::new())
    }

    /// Answer a read of a chunk at a label
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `pos` - The position the reader asks for; a chunk's identity binds it to its place
    /// * `label` - The label the reader's row names
    /// * `policy` - The policy in force
    pub fn read(
        &self,
        stripe: StripeIx,
        pos: Pos,
        label: Label,
        policy: &StripePolicy,
    ) -> ChunkReply {
        // a chunk of another position verifies nowhere but its own place
        let chunk = self.chunks.get(&stripe).filter(|chunk| chunk.pos == pos);
        // the chunk itself, verified
        if let Some(chunk) = chunk {
            if same(chunk.label, label, policy) && !chunk.content.is_torn() {
                return ChunkReply::Bytes(chunk.content.clone());
            }
        }
        // a staged write the reader's row names, overlaid on what it stands on: the chunk, and
        // any committed write between the two this holder has not applied yet
        let staged = self
            .staged
            .iter()
            .find(|((s, l), staged)| *s == stripe && staged.pos == pos && same(*l, label, policy))
            .map(|((_, l), _)| *l);
        if let Some(records) = staged.and_then(|named| self.chain(stripe, pos, named)) {
            let whole = records
                .last()
                .is_some_and(|record| record.expects.is_none());
            let base = if whole {
                None
            } else {
                chunk.filter(|chunk| !chunk.content.is_torn())
            };
            if whole || base.is_some() {
                let content = records.iter().rev().try_fold(
                    base.map(|chunk| chunk.content.clone()),
                    |content, record| record.record.apply(content.as_ref()).map(Some),
                );
                if let Some(Some(content)) = content {
                    return ChunkReply::Bytes(content);
                }
            }
        }
        // the previous state, when it is kept, for a reader whose row has not moved
        if let Some(previous) = self.previous.get(&stripe) {
            if previous.pos == pos && same(previous.label, label, policy) {
                return ChunkReply::Bytes(previous.content.clone());
            }
        }
        match chunk {
            Some(chunk) if !chunk.content.is_torn() => ChunkReply::Other {
                label: chunk.label,
                content: chunk.content.clone(),
            },
            _ => ChunkReply::Nothing,
        }
    }

    /// Whether this slice durably holds a label for a stripe's position
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `pos` - The position
    /// * `label` - The label
    pub fn holds(&self, stripe: StripeIx, pos: Pos, label: Label) -> bool {
        let chunk = self
            .chunks
            .get(&stripe)
            .is_some_and(|chunk| chunk.pos == pos && chunk.label == label);
        let applying = self
            .applying
            .get(&stripe)
            .is_some_and(|applying| applying.pos == pos && applying.label == label);
        // a staged label is held only if this holder can make it: its record and every one it
        // stands on, down to the chunk
        let staged = self.staged.contains_key(&(stripe, label))
            && self
                .chain(stripe, pos, label)
                .is_some_and(|records| !records.is_empty());
        chunk || applying || staged
    }

    /// A crash: everything not synced is lost, and an apply still writing tears its units
    pub fn crash(&mut self) {
        for (stripe, applying) in std::mem::take(&mut self.applying) {
            if applying.phase != Phase::Writing {
                continue;
            }
            // the header made it and the units it was writing did not
            if let Some(chunk) = self.chunks.get_mut(&stripe) {
                chunk.label = applying.label;
                chunk.content.tear(&applying.record.units_written());
            }
        }
        self.unsynced.clear();
        self.committed.clear();
    }

    /// A restart: every staged write whose apply began is written again from its record
    pub fn recover(&mut self) -> Vec<HolderFact> {
        let mut facts = Vec::new();
        let begun: Vec<(StripeIx, Label)> = self
            .staged
            .keys()
            .filter(|(stripe, label)| {
                self.chunks
                    .get(stripe)
                    .is_some_and(|chunk| chunk.label == *label)
            })
            .copied()
            .collect();
        for (stripe, label) in begun {
            let Some(staged) = self.staged.remove(&(stripe, label)) else {
                continue;
            };
            let before = self.chunks[&stripe].content.clone();
            let after = staged.record.apply(Some(&before));
            if let Some(content) = &after {
                self.chunks.insert(
                    stripe,
                    Chunk {
                        pos: staged.pos,
                        label,
                        content: content.clone(),
                    },
                );
            }
            facts.push(HolderFact::Replayed {
                stripe,
                label,
                before,
                after,
            });
        }
        facts
    }

    /// Lose everything: the disk under this slice is gone
    pub fn wipe(&mut self) {
        self.chunks.clear();
        self.previous.clear();
        self.staged.clear();
        self.unsynced.clear();
        self.committed.clear();
        self.applying.clear();
    }

    /// Whether the slice holds a staged write for a stripe that is not yet decided
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    pub fn holds_staged(&self, stripe: StripeIx) -> bool {
        self.staged.keys().any(|(s, _)| *s == stripe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::OpId;
    use crate::stripe::ids::Tag;
    use crate::stripe::layout::Layout;

    /// A slice holding stripe 0's first data chunk as the put left it
    fn slice() -> Slice {
        let mut slice = Slice::new(SliceId(0), DeviceId(0), DiskId(0), NodeId(0));
        slice.chunks.insert(
            StripeIx(0),
            Chunk {
                pos: Pos(0),
                label: Label::PUT,
                content: Content::Data(vec![Unit::Write(OpId(0)); 2]),
            },
        );
        slice
    }

    /// A write's label at a sequence
    fn label(seq: u32, op: u32) -> Label {
        Label {
            seq: Seq(seq),
            tag: Tag::of(OpId(op), StripeIx(0), 0),
        }
    }

    /// A stage of new values for unit 1 of position 0, against a base
    fn staged(seq: u32, op: u32, expects: Label) -> Staged {
        Staged {
            pos: Pos(0),
            label: label(seq, op),
            base_exists: true,
            base: Seq(seq - 1),
            expects: Some(expects),
            record: Record::Units(Vec::new()),
            reserved: false,
        }
    }

    /// Stage, sync and learn the label committed
    fn stage_and_commit(slice: &mut Slice, staged: Staged, policy: &StripePolicy) {
        let label = staged.label;
        let payload = Payload::Units(vec![(1, Unit::Write(OpId(label.tag.0)))]);
        let answer = slice.stage(StripeIx(0), staged, payload, false, Endpoint::Entry, policy);
        assert_eq!(answer, None);
        slice.sync(policy);
        slice.learn_named(StripeIx(0), label, policy);
    }

    /// A change to part of a chunk is refused by a holder whose chunk is at another label
    #[test]
    fn a_stage_against_another_label_is_refused() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        let answer = slice.stage(
            StripeIx(0),
            staged(2, 9, label(1, 8)),
            Payload::Units(vec![(1, Unit::Write(OpId(9)))]),
            false,
            Endpoint::Entry,
            &policy,
        );
        assert_eq!(answer, Some(Some(StageRefusal::Label)));
    }

    /// Staging the same write twice is staging it once
    #[test]
    fn staging_the_same_write_twice_is_staging_it_once() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        stage_and_commit(&mut slice, staged(1, 5, Label::PUT), &policy);
        let again = slice.stage(
            StripeIx(0),
            staged(1, 5, Label::PUT),
            Payload::Units(vec![(1, Unit::Write(OpId(5)))]),
            false,
            Endpoint::Entry,
            &policy,
        );
        assert_eq!(again, Some(None));
        assert_eq!(slice.staged.len(), 1);
    }

    /// An apply and a replay of a record of new values leave the chunk as one apply does
    #[test]
    fn a_record_of_new_values_replays_to_the_same_chunk() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        stage_and_commit(&mut slice, staged(1, 5, Label::PUT), &policy);
        // written in place and synced, and the node goes before the record is dropped
        slice
            .apply_step(StripeIx(0), false, &policy)
            .expect("begins");
        slice
            .apply_step(StripeIx(0), false, &policy)
            .expect("syncs");
        let applied = slice.chunks[&StripeIx(0)].clone();
        slice.crash();
        let facts = slice.recover();
        assert!(matches!(
            facts.as_slice(),
            [HolderFact::Replayed { before, after: Some(after), .. }] if before == after
        ));
        assert_eq!(slice.chunks[&StripeIx(0)], applied);
    }

    /// A crash in the middle of an apply tears it, and the staged copy writes it again
    #[test]
    fn a_torn_apply_is_written_again_from_its_staged_copy() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        stage_and_commit(&mut slice, staged(1, 5, Label::PUT), &policy);
        slice
            .apply_step(StripeIx(0), false, &policy)
            .expect("begins");
        slice.crash();
        assert!(slice.chunks[&StripeIx(0)].content.is_torn());
        slice.recover();
        let chunk = &slice.chunks[&StripeIx(0)];
        assert!(!chunk.content.is_torn());
        assert_eq!(chunk.label, label(1, 5));
    }

    /// A holder never moves a chunk back to a label a newer write has passed
    #[test]
    fn a_holder_never_applies_a_label_its_chunk_has_passed() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        slice.chunks.get_mut(&StripeIx(0)).expect("put").label = label(3, 7);
        // an old record a lagging view still names
        slice.staged.insert(
            (StripeIx(0), label(2, 6)),
            Staged {
                expects: None,
                record: Record::Whole(Content::Data(vec![Unit::Zero; 2])),
                ..staged(2, 6, Label::PUT)
            },
        );
        slice.committed.insert((StripeIx(0), label(2, 6)));
        assert_eq!(slice.apply_step(StripeIx(0), false, &policy), None);
        assert_eq!(slice.chunks[&StripeIx(0)].label, label(3, 7));
    }

    /// A holder answers for the position its chunk is at and no other
    #[test]
    fn a_holder_answers_only_for_its_own_position() {
        let policy = StripePolicy::safe();
        let slice = slice();
        assert!(slice.holds(StripeIx(0), Pos(0), Label::PUT));
        assert!(!slice.holds(StripeIx(0), Pos(1), Label::PUT));
        assert_eq!(
            slice.read(StripeIx(0), Pos(1), Label::PUT, &policy),
            ChunkReply::Nothing
        );
    }

    /// A committed write a later change to part of the chunk stands on is kept, and made from
    #[test]
    fn a_write_a_named_label_stands_on_is_kept() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        // two changes to part of the chunk, the second staged over the first, neither applied
        stage_and_commit(&mut slice, staged(1, 5, Label::PUT), &policy);
        stage_and_commit(&mut slice, staged(2, 6, label(1, 5)), &policy);
        let mut row = RowState::put(Layout::Replicated3, vec![SliceId(0); 3]);
        row.exists = true;
        row.seq = Seq(2);
        row.labels[0] = label(2, 6);
        // the row moved past the first's base and names the second: the first is kept
        assert!(slice.learn_row(StripeIx(0), &row, &policy).is_empty());
        assert!(slice.holds(StripeIx(0), Pos(0), label(2, 6)));
        assert!(matches!(
            slice.read(StripeIx(0), Pos(0), label(2, 6), &policy),
            ChunkReply::Bytes(_)
        ));
        // as the pages wrote it, the first is dropped and the second can no longer be made
        let as_written = StripePolicy {
            beneath: BeneathRule::Discarded,
            ..policy
        };
        let facts = slice.learn_row(StripeIx(0), &row, &as_written);
        assert!(matches!(
            facts.as_slice(),
            [HolderFact::DiscardedStage { .. }]
        ));
        assert!(!slice.holds(StripeIx(0), Pos(0), label(2, 6)));
    }

    /// A staged copy whose apply is under way outlives a fact that excludes it
    #[test]
    fn a_staged_copy_being_applied_outlives_a_fact_that_excludes_it() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        stage_and_commit(&mut slice, staged(1, 5, Label::PUT), &policy);
        slice
            .apply_step(StripeIx(0), false, &policy)
            .expect("begins");
        // a later write committed over the position, and the holder hears of it mid-apply
        let mut row = RowState::put(Layout::Replicated3, vec![SliceId(0); 3]);
        row.exists = true;
        row.seq = Seq(2);
        row.labels[0] = label(2, 7);
        assert!(slice.learn_row(StripeIx(0), &row, &policy).is_empty());
        // so a crash before the apply is synced is written again from the copy
        slice.crash();
        slice.recover();
        assert!(!slice.chunks[&StripeIx(0)].content.is_torn());
    }

    /// A record whose base is gone is replaced by a new stage of its label, not taken for it
    #[test]
    fn a_record_whose_base_is_gone_is_replaced_by_a_stage_of_its_label() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        // a change to part of the chunk staged over a write this holder never had
        slice
            .staged
            .insert((StripeIx(0), label(2, 6)), staged(2, 6, label(1, 5)));
        assert!(!slice.holds(StripeIx(0), Pos(0), label(2, 6)));
        // a rebuild stages the label whole, and the holder takes it rather than answer it a repeat
        let whole = Staged {
            expects: None,
            ..staged(2, 6, Label::PUT)
        };
        let content = Content::Data(vec![Unit::Write(OpId(6)); 2]);
        let answer = slice.stage(
            StripeIx(0),
            whole,
            Payload::Whole(content),
            false,
            Endpoint::Entry,
            &policy,
        );
        assert_eq!(answer, None);
        slice.sync(&policy);
        assert!(slice.holds(StripeIx(0), Pos(0), label(2, 6)));
    }

    /// A discard waits for a row past the record's base; absence on a lagging replica is no fact
    #[test]
    fn a_record_is_discarded_only_on_a_row_past_its_base() {
        let policy = StripePolicy::safe();
        let mut slice = slice();
        let staged = staged(1, 5, Label::PUT);
        slice
            .staged
            .insert((StripeIx(0), staged.label), staged.clone());
        let mut row = RowState::put(Layout::Replicated3, vec![SliceId(0); 3]);
        // a replica that has not applied the row's creation says nothing
        assert!(slice.learn_row(StripeIx(0), &row, &policy).is_empty());
        // a row past the base that names another write excludes it for good
        row.exists = true;
        row.seq = Seq(1);
        row.labels[0] = label(1, 6);
        let facts = slice.learn_row(StripeIx(0), &row, &policy);
        assert!(matches!(
            facts.as_slice(),
            [HolderFact::DiscardedStage { .. }]
        ));
    }
}
