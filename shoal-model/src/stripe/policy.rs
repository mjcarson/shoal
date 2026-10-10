//! The safe stripe protocol, and every way of getting it wrong
//!
//! Every knob's first variant is what the contract requires. The unsafe settings are S16's table
//! ([S16](../../../docs/src/object-storage/testing.md#the-model)): one for each rule the design
//! depends on, each named for the clause it breaks, each with a schedule of
//! [S7](../../../docs/src/object-storage/write-path.md#the-schedules-that-shaped-it) that makes the
//! check fire. They are not options; they exist so that the checker can be shown to catch them.
//!
//! Three knobs were questions the design left open, run both ways under the safe policy: whether
//! an untouched chunk counts toward `k + f` on the row's word (Q16), whether a holder keeps a
//! chunk's previous state for a reader in flight, and whether the leader reserves a stripe for one
//! stager. The model settled the first: counting on the row's word broke P11 either way, so the
//! two answers that do are deviations. Moving the other two never is.
//!
//! The model also found rules the pages stated that do not hold, each now a knob whose first
//! variant is the repair and whose second is the rule as written: the stamp, the hidden units, a
//! reclaimed row, the entry a reader hides by, where a default read takes its row, the tag, the
//! truncate's order against the stripe it cuts inside, and what a holder keeps beneath a label.
//!
//! The small write that rides in its commit, which X8 measured after the rest was modelled, is a
//! fourth knob run both ways, and its rules are knobs of their own in [`SmallWriteRules`]: each
//! first variant the contract, each second a way of writing the rule that loses a write.

use serde::{Deserialize, Serialize};

use crate::invariants::Property;

/// What labels a stripe chunk (P9, P10)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelRule {
    /// The row's sequence and a tag derived from the write's identity
    SequenceAndTag,
    /// The sequence alone, so two stagers on one base make the same label
    SequenceOnly,
}

/// What a stripe's commit is conditional on (P8)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionRule {
    /// The row's sequence is the one the write was staged against
    Sequence,
    /// Nothing: a commit lands whatever the row became
    Unconditional,
}

/// What a holder does when a stager that gave up tells it to drop what it staged (P16)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagerWord {
    /// A stager that gives up says nothing to holders; only a committed fact discards
    Ignored,
    /// The stager tells its holders to drop, and they do
    Obeyed,
}

/// When a write may be acknowledged (P11)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckRule {
    /// Once `k + f` chunks are current, in distinct failure domains, and the commit applied
    KPlusF,
    /// After the first stage is answered
    AfterOneStage,
}

/// What a parity holder stages (P15)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParityRecord {
    /// The new parity: replaying it writes the same bytes again
    NewValues,
    /// The change, folded in at the apply: replaying it folds it twice
    Patch,
}

/// When a holder applies what it staged (P9)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyTiming {
    /// Once the row names the write's label for its chunk
    AfterCommit,
    /// As soon as the stage is synced
    BeforeCommit,
}

/// What a reader does with a chunk under a label its row does not name (P10)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReaderRule {
    /// Treats an older one as missing, and reads its row again for a newer one
    MovesForward,
    /// Takes a newer chunk as it is
    AcceptsNewer,
}

/// Which view of a row a holder may discard on (P16)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscardView {
    /// Only a committed fact that excludes the bytes: the sequence past their base, another label
    CommittedFact,
    /// Any view that does not name them, a lagging replica's absence included
    AnyView,
}

/// Whether a commit checks the placement group's generation (P8, P17)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationRule {
    /// The commit names the generation it was staged under and is refused at any other
    Checked,
    /// The commit lands whatever the generation became
    Ignored,
}

/// Whether a stripe's commit carries the truncate epoch its writer read (P13)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpochRule {
    /// The commit stamps the epoch, and a reader hides a stripe stamped below a floor over it
    Stamped,
    /// Nothing is stamped, so a floor hides nothing and a reader clips at the size alone
    Ignored,
}

/// Whether a commit records the holders its write touched that did not stage (P11, P17)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedRule {
    /// It does, and the row calls their chunks stale until a rebuild
    #[default]
    Recorded,
    /// It does not, so a slice that returns is taken as current
    NotRecorded,
}

/// When a holder drops a staged copy it is applying (P7, P9)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedCopy {
    /// Only after the write in place is synced, so a torn apply can be written again
    OutlivesApply,
    /// When the apply starts
    DroppedAtApplyStart,
}

/// How a restarted node knows a device (P7, P17)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceIdentity {
    /// By the ids in its marker: an empty directory is a new device with new slices
    Ids,
    /// By its path, so an empty disk mounted there is taken for the old one
    PathOnly,
}

/// When a write takes the space its apply needs (P7, P11)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpaceRule {
    /// At the stage, which is refused if the device cannot hold it
    AtStage,
    /// At the apply, after the commit
    AtApply,
}

/// Whether a rebuild asks a holder what it holds before writing (progress)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebuildRule {
    /// It asks, and a holder that already holds the label is committed current unwritten
    AsksHolder,
    /// It rebuilds whatever the row calls stale
    Blind,
}

/// When a chunk a write did not touch counts toward `k + f` (Q16, run both ways and settled)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UntouchedRule {
    /// Only when its holder answers, in the write's round, that it holds the label
    Confirmed,
    /// On the row's word, unless its holder is known to be down
    UpOnly,
    /// On the row's word, down or not
    CountedWhenDown,
}

/// Whether a holder keeps a chunk's previous state after an apply (progress, run both ways)
///
/// Kept is what X1 recommends: without it, readers of a stripe written continuously fail by
/// name or run past the bound as writers are added, and with it they do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviousState {
    /// Kept until the next apply, for a reader whose row has not moved
    Kept,
    /// Dropped with the apply
    Dropped,
}

/// Whether the leader orders a stripe's stagers (progress, run both ways)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reservation {
    /// Every stager stages at once
    None,
    /// A stager asks the leader first and stages when granted; advisory, never a lock
    Granted,
}

/// What a stripe's commit stamps, and what it is refused for (Q18)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StampRule {
    /// The epoch its writer read, refused if the row was stamped later
    NeverBackwards,
    /// The epoch its writer read, whatever the row holds (S3 as written)
    ReadEpoch,
}

/// What a write into a stripe a floor hides does with the hidden units (Q18)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HiddenRule {
    /// Writes them as zeros in the same commit
    Zeroed,
    /// Leaves them as they are (S3 as written), so the stamp it moves uncovers them
    Untouched,
}

/// What reclaiming a stripe under a floor does to its row (Q18, P16)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimRule {
    /// Leaves a tombstone a sequence past every write staged before it
    Tombstoned,
    /// Deletes it (S3 as written), which sets its sequence back to a stripe's with no row
    Deleted,
}

/// Which entry a reader hides a stripe's units by (P12, P13)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryRule {
    /// The one it read, read again if the row is stamped past it
    ForwardToStamp,
    /// The one it read (S9 as written)
    AsRead,
}

/// How a truncate orders itself against the stripe its cut falls inside (P12, P13)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncateRule {
    /// It fences that stripe's row with its epoch first, and commits only at the epoch it read;
    /// a stripe commit below a fence is refused
    Fenced,
    /// It commits to the entry alone (S3 as written)
    Unfenced,
}

/// What a write's tag is derived from (P9)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TagRule {
    /// The request identity and the try, so two tries never share a label; the group's retry
    /// table, not the tag, recognises a retry
    PerAttempt,
    /// The request identity alone (S7 as written), so a retry against the same row stages under
    /// the label its first try did, though a truncate between them changed its bytes
    PerIdentity,
}

/// Where a default read takes a stripe's row (P12)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowLevel {
    /// At the row group's leader, after the entry, so the row has every commit the entry saw
    AfterEntryAtLeader,
    /// At any replica (S9 as written), which can be behind an extension the entry already shows
    AtOne,
}

/// What a holder keeps beneath the label a row names for its chunk (P16, P17)
///
/// A change to part of a chunk is staged as the units it changed, over the label it expects, so
/// the label it makes stands on every committed write beneath it the chunk has not reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeneathRule {
    /// Every committed write the named label stands on, until the chunk reaches it
    Kept,
    /// Only what the row names (S10 as written): a write the row has moved past is discarded,
    /// though a later change to part of the chunk was staged over it
    Discarded,
}

/// Whether a small write rides inside its stripe's commit (Q27, run both ways)
///
/// X8 found a write below 64 KiB is cheaper in its commit on a device whose sync flushes its
/// cache, and staged on one whose cache writes through
/// ([Q27](../../../docs/src/object-storage/contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)),
/// so both are the contract and neither is a deviation. Only a replicated stripe takes the first:
/// an erasure coded small write is M18's.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmallWritePath {
    /// Every write is staged on its holders before its commit
    #[default]
    Staged,
    /// A write of at most [`PENDING_BOUND`] units commits its bytes into the row's pending bytes,
    /// which holders fold into their chunks afterwards
    InCommit,
}

/// How many data units a stripe row's pending bytes may hold: the model's small-write threshold
///
/// One unit of a chunk's two. The product's is 64 KiB of a chunk of a megabyte or more, and what
/// the bound buys is the same: the row stays bounded ([P18](../../../docs/src/object-storage/contract.md#the-contract)).
pub const PENDING_BOUND: usize = 1;

/// What a small write does with the pending bytes an earlier one left in the row (P9)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingMerge {
    /// Merges its units over them, so the row holds every unit written since the base
    #[default]
    Merged,
    /// Replaces them with its own (X8's spike, which waited for every fold first), so a holder
    /// still at the base folds only the last write's units
    Replaced,
}

/// When the leader's driver may clear a row's pending bytes (P11)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClearRule {
    /// Once `k + f` holders said, in its round, that they hold the bytes' label durably
    #[default]
    KPlusF,
    /// On the first holder's word
    FirstHolder,
}

/// What a reader, a rebuild or a stager does with a chunk at the base pending bytes overlay (P10)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRead {
    /// Lays the row's pending units over it, which makes the label the row names
    #[default]
    Overlaid,
    /// Takes it as the label the row names, as it is
    BaseTaken,
}

/// What a holder folds a row's pending bytes onto (P9)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FoldBase {
    /// Only the base they were committed over, or a label they themselves made since
    #[default]
    BaseOrChain,
    /// Whatever chunk it holds
    AnyChunk,
}

/// The small write in its commit: whether it is taken, and every rule it depends on
///
/// The first variant of each rule is the contract; the path is run both ways. Absent from a
/// schedule file, it is the staged path with every rule as the contract has it, which is what
/// every schedule saved before the path was modelled ran under.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmallWriteRules {
    /// Whether a small write rides in its commit
    pub path: SmallWritePath,
    /// What a later small write does with the pending bytes
    pub merge: PendingMerge,
    /// When the pending bytes may be cleared
    pub clear: ClearRule,
    /// Whether the clear marks the positions that did not hold them missed
    pub clear_missed: MissedRule,
    /// What a reader does with a chunk at the base
    pub reader: PendingRead,
    /// What a rebuild or a move does with a chunk at the base
    pub rebuild: PendingRead,
    /// What a stager writing over pending bytes does with a chunk at the base
    pub stager: PendingRead,
    /// What a holder folds pending bytes onto
    pub fold: FoldBase,
}

impl SmallWriteRules {
    /// Whether these are the rules every schedule saved before the path was modelled ran under
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Every knob together
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripePolicy {
    /// What labels a chunk
    pub label: LabelRule,
    /// What a commit is conditional on
    pub condition: ConditionRule,
    /// What a holder does with a stager's word
    pub stager_word: StagerWord,
    /// When a write is acknowledged
    pub ack: AckRule,
    /// What a parity holder stages
    pub parity_record: ParityRecord,
    /// When a holder applies
    pub apply_timing: ApplyTiming,
    /// What a reader does with a chunk under another label
    pub reader: ReaderRule,
    /// Which view a holder discards on
    pub discard_view: DiscardView,
    /// Whether a commit checks the generation
    pub generation: GenerationRule,
    /// Whether a commit carries the truncate epoch
    pub epoch: EpochRule,
    /// Whether a commit records who missed it
    pub missed: MissedRule,
    /// When a staged copy is dropped
    pub staged_copy: StagedCopy,
    /// How a device is known
    pub identity: DeviceIdentity,
    /// When space is taken
    pub space: SpaceRule,
    /// Whether a rebuild asks first
    pub rebuild: RebuildRule,
    /// When an untouched chunk counts (Q16)
    pub untouched: UntouchedRule,
    /// Whether a holder keeps a chunk's previous state
    pub previous: PreviousState,
    /// Whether the leader reserves a stripe
    pub reservation: Reservation,
    /// What a commit stamps
    pub stamp: StampRule,
    /// What a write does with units a floor hides
    pub hidden: HiddenRule,
    /// What reclaiming does to a row
    pub reclaim: ReclaimRule,
    /// Which entry a reader hides by
    pub reader_entry: EntryRule,
    /// Where a default read takes the row
    pub reader_row: RowLevel,
    /// What a write's tag is derived from
    pub tag: TagRule,
    /// How a truncate orders itself against the stripe its cut falls inside
    pub truncate: TruncateRule,
    /// What a holder keeps beneath the label a row names
    pub beneath: BeneathRule,
    /// The small write in its commit, and its rules
    #[serde(default, skip_serializing_if = "SmallWriteRules::is_default")]
    pub small_writes: SmallWriteRules,
}

/// An unsafe setting: its name, the policy, the clauses S16 says it breaks, and its S7 schedule
pub type UnsafeSetting = (&'static str, StripePolicy, &'static [Property], u8);

/// A rule the small write in its commit depends on, as one might write it: its name, the policy
/// with the path taken and that rule moved, and the clauses a schedule of it has to fire
pub type SmallWriteSetting = (&'static str, StripePolicy, &'static [Property]);

impl StripePolicy {
    /// The contract: the first variant of every knob
    pub const fn safe() -> Self {
        Self {
            label: LabelRule::SequenceAndTag,
            condition: ConditionRule::Sequence,
            stager_word: StagerWord::Ignored,
            ack: AckRule::KPlusF,
            parity_record: ParityRecord::NewValues,
            apply_timing: ApplyTiming::AfterCommit,
            reader: ReaderRule::MovesForward,
            discard_view: DiscardView::CommittedFact,
            generation: GenerationRule::Checked,
            epoch: EpochRule::Stamped,
            missed: MissedRule::Recorded,
            staged_copy: StagedCopy::OutlivesApply,
            identity: DeviceIdentity::Ids,
            space: SpaceRule::AtStage,
            rebuild: RebuildRule::AsksHolder,
            untouched: UntouchedRule::Confirmed,
            previous: PreviousState::Kept,
            reservation: Reservation::None,
            stamp: StampRule::NeverBackwards,
            hidden: HiddenRule::Zeroed,
            reclaim: ReclaimRule::Tombstoned,
            reader_entry: EntryRule::ForwardToStamp,
            reader_row: RowLevel::AfterEntryAtLeader,
            tag: TagRule::PerAttempt,
            truncate: TruncateRule::Fenced,
            beneath: BeneathRule::Kept,
            small_writes: SmallWriteRules {
                path: SmallWritePath::Staged,
                merge: PendingMerge::Merged,
                clear: ClearRule::KPlusF,
                clear_missed: MissedRule::Recorded,
                reader: PendingRead::Overlaid,
                rebuild: PendingRead::Overlaid,
                stager: PendingRead::Overlaid,
                fold: FoldBase::BaseOrChain,
            },
        }
    }

    /// This policy with small writes riding in their commits
    pub const fn in_commit(self) -> Self {
        let mut policy = self;
        policy.small_writes.path = SmallWritePath::InCommit;
        policy
    }

    /// Every rule the small write in its commit depends on, each as one might write it
    ///
    /// Each is the safe policy with the path taken and that one rule moved. None of them has a
    /// schedule of S7's: they were written for the path X8 measured, which came after S7's table,
    /// and each is saved with the first generated run that breaks it, minimized.
    pub fn small_write_settings() -> Vec<SmallWriteSetting> {
        let safe = Self::safe().in_commit();
        let rules = safe.small_writes;
        vec![
            (
                "pending_replaced_by_the_next_write",
                Self {
                    small_writes: SmallWriteRules {
                        merge: PendingMerge::Replaced,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P9, Property::P12],
            ),
            (
                "pending_cleared_before_k_plus_f_hold_it",
                Self {
                    small_writes: SmallWriteRules {
                        clear: ClearRule::FirstHolder,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P11],
            ),
            (
                "clear_leaves_the_rest_current",
                Self {
                    small_writes: SmallWriteRules {
                        clear_missed: MissedRule::NotRecorded,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P17],
            ),
            (
                "reader_ignores_pending",
                Self {
                    small_writes: SmallWriteRules {
                        reader: PendingRead::BaseTaken,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P10, Property::P12],
            ),
            (
                "rebuild_copies_the_base",
                Self {
                    small_writes: SmallWriteRules {
                        rebuild: PendingRead::BaseTaken,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P9, Property::P12],
            ),
            (
                "staged_write_over_pending_omits_it",
                Self {
                    small_writes: SmallWriteRules {
                        stager: PendingRead::BaseTaken,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P9, Property::P12, Property::P17],
            ),
            (
                "fold_onto_another_base",
                Self {
                    small_writes: SmallWriteRules {
                        fold: FoldBase::AnyChunk,
                        ..rules
                    },
                    ..safe
                },
                &[Property::P9],
            ),
        ]
    }

    /// Every unsafe setting of S16's table, one knob moved at a time
    ///
    /// The names are what a saved schedule is filed under and what `deviations` reports; the
    /// clauses are the ones S16 says each breaks, of which its schedule has to fire one.
    pub fn unsafe_settings() -> Vec<UnsafeSetting> {
        let safe = Self::safe();
        vec![
            (
                "sequence_as_label",
                Self {
                    label: LabelRule::SequenceOnly,
                    ..safe
                },
                &[Property::P9, Property::P10],
                1,
            ),
            (
                "commit_with_no_condition",
                Self {
                    condition: ConditionRule::Unconditional,
                    ..safe
                },
                &[Property::P8],
                2,
            ),
            (
                "returning_slice_taken_as_current",
                Self {
                    missed: MissedRule::NotRecorded,
                    ..safe
                },
                &[Property::P11, Property::P17],
                3,
            ),
            (
                "commit_ignores_the_truncate_epoch",
                Self {
                    epoch: EpochRule::Ignored,
                    ..safe
                },
                &[Property::P13],
                4,
            ),
            (
                "commit_ignores_the_generation",
                Self {
                    generation: GenerationRule::Ignored,
                    ..safe
                },
                &[Property::P8, Property::P17],
                5,
            ),
            (
                "holder_discards_on_a_stagers_word",
                Self {
                    stager_word: StagerWord::Obeyed,
                    ..safe
                },
                &[Property::P16],
                6,
            ),
            (
                "ack_after_one_stage",
                Self {
                    ack: AckRule::AfterOneStage,
                    ..safe
                },
                &[Property::P11],
                7,
            ),
            (
                "parity_staged_as_a_patch",
                Self {
                    parity_record: ParityRecord::Patch,
                    ..safe
                },
                &[Property::P15],
                8,
            ),
            (
                "staged_copy_dropped_when_its_apply_starts",
                Self {
                    staged_copy: StagedCopy::DroppedAtApplyStart,
                    ..safe
                },
                &[Property::P7, Property::P9],
                9,
            ),
            (
                "device_known_by_its_path",
                Self {
                    identity: DeviceIdentity::PathOnly,
                    ..safe
                },
                &[Property::P7, Property::P17],
                12,
            ),
            (
                "space_taken_at_the_apply",
                Self {
                    space: SpaceRule::AtApply,
                    ..safe
                },
                &[Property::P7, Property::P11],
                13,
            ),
            (
                "discard_on_a_lagging_replicas_view",
                Self {
                    discard_view: DiscardView::AnyView,
                    ..safe
                },
                &[Property::P16],
                14,
            ),
            (
                "apply_before_the_commit",
                Self {
                    apply_timing: ApplyTiming::BeforeCommit,
                    ..safe
                },
                &[Property::P9],
                15,
            ),
            (
                "reader_accepts_a_newer_chunk",
                Self {
                    reader: ReaderRule::AcceptsNewer,
                    ..safe
                },
                &[Property::P10],
                16,
            ),
        ]
    }

    /// The rules the pages stated that the model found unsafe, and Q16's answers that count on
    /// the row's word: the safe policy with each one, as written, put back
    pub fn documented_rules() -> Vec<(&'static str, StripePolicy)> {
        let safe = Self::safe();
        vec![
            (
                "untouched_chunk_counted_while_believed_up",
                Self {
                    untouched: UntouchedRule::UpOnly,
                    ..safe
                },
            ),
            (
                "untouched_chunk_counted_when_down",
                Self {
                    untouched: UntouchedRule::CountedWhenDown,
                    ..safe
                },
            ),
            (
                "stamp_moves_backwards",
                Self {
                    stamp: StampRule::ReadEpoch,
                    ..safe
                },
            ),
            (
                "hidden_units_left_under_a_write",
                Self {
                    hidden: HiddenRule::Untouched,
                    ..safe
                },
            ),
            (
                "reclaimed_row_deleted",
                Self {
                    reclaim: ReclaimRule::Deleted,
                    ..safe
                },
            ),
            (
                "reader_hides_by_the_entry_it_read",
                Self {
                    reader_entry: EntryRule::AsRead,
                    ..safe
                },
            ),
            (
                "default_read_takes_the_row_at_one",
                Self {
                    reader_row: RowLevel::AtOne,
                    ..safe
                },
            ),
            (
                "tag_from_the_identity_alone",
                Self {
                    tag: TagRule::PerIdentity,
                    ..safe
                },
            ),
            (
                "truncate_unfenced",
                Self {
                    truncate: TruncateRule::Unfenced,
                    ..safe
                },
            ),
            (
                "write_discarded_beneath_a_later_one",
                Self {
                    beneath: BeneathRule::Discarded,
                    ..safe
                },
            ),
        ]
    }

    /// The setting S16 holds to the progress check alone, with its S7 schedule
    pub fn progress_setting() -> (&'static str, StripePolicy, u8) {
        (
            "rebuild_without_asking_the_holder",
            Self {
                rebuild: RebuildRule::Blind,
                ..Self::safe()
            },
            11,
        )
    }

    /// The names of every knob this policy moves away from the contract; empty means safe
    ///
    /// The three knobs run both ways are not deviations, whichever way they are set.
    pub fn deviations(&self) -> Vec<&'static str> {
        // compare every knob that has a safe answer against the contract
        let safe = Self::safe();
        let mut out = Vec::new();
        let mut moved = |differs: bool, name: &'static str| {
            if differs {
                out.push(name);
            }
        };
        moved(self.label != safe.label, "sequence_as_label");
        moved(self.condition != safe.condition, "commit_with_no_condition");
        moved(
            self.missed != safe.missed,
            "returning_slice_taken_as_current",
        );
        moved(
            self.epoch != safe.epoch,
            "commit_ignores_the_truncate_epoch",
        );
        moved(
            self.generation != safe.generation,
            "commit_ignores_the_generation",
        );
        moved(
            self.stager_word != safe.stager_word,
            "holder_discards_on_a_stagers_word",
        );
        moved(self.ack != safe.ack, "ack_after_one_stage");
        moved(
            self.parity_record != safe.parity_record,
            "parity_staged_as_a_patch",
        );
        moved(
            self.staged_copy != safe.staged_copy,
            "staged_copy_dropped_when_its_apply_starts",
        );
        moved(self.identity != safe.identity, "device_known_by_its_path");
        moved(self.space != safe.space, "space_taken_at_the_apply");
        moved(
            self.discard_view != safe.discard_view,
            "discard_on_a_lagging_replicas_view",
        );
        moved(
            self.apply_timing != safe.apply_timing,
            "apply_before_the_commit",
        );
        moved(self.reader != safe.reader, "reader_accepts_a_newer_chunk");
        moved(
            self.rebuild != safe.rebuild,
            "rebuild_without_asking_the_holder",
        );
        moved(self.stamp != safe.stamp, "stamp_moves_backwards");
        moved(
            self.hidden != safe.hidden,
            "hidden_units_left_under_a_write",
        );
        moved(self.reclaim != safe.reclaim, "reclaimed_row_deleted");
        moved(
            self.reader_entry != safe.reader_entry,
            "reader_hides_by_the_entry_it_read",
        );
        moved(
            self.reader_row != safe.reader_row,
            "default_read_takes_the_row_at_one",
        );
        moved(self.tag != safe.tag, "tag_from_the_identity_alone");
        moved(self.truncate != safe.truncate, "truncate_unfenced");
        moved(
            self.beneath != safe.beneath,
            "write_discarded_beneath_a_later_one",
        );
        moved(
            self.untouched == UntouchedRule::UpOnly,
            "untouched_chunk_counted_while_believed_up",
        );
        moved(
            self.untouched == UntouchedRule::CountedWhenDown,
            "untouched_chunk_counted_when_down",
        );
        // the small write's rules; whether it is taken at all is not a deviation either way
        let (rules, contract) = (self.small_writes, safe.small_writes);
        moved(
            rules.merge != contract.merge,
            "pending_replaced_by_the_next_write",
        );
        moved(
            rules.clear != contract.clear,
            "pending_cleared_before_k_plus_f_hold_it",
        );
        moved(
            rules.clear_missed != contract.clear_missed,
            "clear_leaves_the_rest_current",
        );
        moved(rules.reader != contract.reader, "reader_ignores_pending");
        moved(rules.rebuild != contract.rebuild, "rebuild_copies_the_base");
        moved(
            rules.stager != contract.stager,
            "staged_write_over_pending_omits_it",
        );
        moved(rules.fold != contract.fold, "fold_onto_another_base");
        out
    }

    /// The safe policy with the three open questions set as given
    ///
    /// # Arguments
    ///
    /// * `untouched` - When an untouched chunk counts (Q16)
    /// * `previous` - Whether a holder keeps a chunk's previous state
    /// * `reservation` - Whether the leader reserves a stripe
    pub fn safe_with(
        untouched: UntouchedRule,
        previous: PreviousState,
        reservation: Reservation,
    ) -> Self {
        Self {
            untouched,
            previous,
            reservation,
            ..Self::safe()
        }
    }

    /// How the open questions are set, as a short label for tables
    pub fn variant(&self) -> String {
        // each open question's answer, in a fixed order
        let untouched = match self.untouched {
            UntouchedRule::UpOnly => "up-only",
            UntouchedRule::CountedWhenDown => "down-counts",
            UntouchedRule::Confirmed => "confirmed",
        };
        let previous = match self.previous {
            PreviousState::Dropped => "drop-prev",
            PreviousState::Kept => "keep-prev",
        };
        let reservation = match self.reservation {
            Reservation::None => "no-reserve",
            Reservation::Granted => "reserve",
        };
        // the small write's path is named only when it is taken, so every label from before it
        // was modelled is unchanged
        match self.small_writes.path {
            SmallWritePath::Staged => format!("{untouched}/{previous}/{reservation}"),
            SmallWritePath::InCommit => format!("{untouched}/{previous}/{reservation}/in-commit"),
        }
    }
}

impl Default for StripePolicy {
    /// The contract
    fn default() -> Self {
        Self::safe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract deviates from itself in nothing
    #[test]
    fn the_safe_stripe_policy_has_no_deviations() {
        assert!(StripePolicy::safe().deviations().is_empty());
    }

    /// Each unsafe setting moves exactly the knob it is named for, and S16 has fourteen
    #[test]
    fn every_unsafe_stripe_setting_deviates_in_exactly_its_own_name() {
        let settings = StripePolicy::unsafe_settings();
        assert_eq!(settings.len(), 14);
        for (name, policy, clauses, _) in settings {
            assert_eq!(policy.deviations(), vec![name]);
            assert!(!clauses.is_empty());
        }
        let (name, policy, _) = StripePolicy::progress_setting();
        assert_eq!(policy.deviations(), vec![name]);
    }

    /// The two progress questions are never deviations; Q16's answers on the row's word are
    #[test]
    fn the_progress_questions_are_not_deviations_and_q16_is_settled() {
        let policy = StripePolicy::safe_with(
            UntouchedRule::Confirmed,
            PreviousState::Dropped,
            Reservation::Granted,
        );
        assert!(policy.deviations().is_empty());
        assert_eq!(policy.variant(), "confirmed/drop-prev/reserve");
        let counted = StripePolicy::safe_with(
            UntouchedRule::CountedWhenDown,
            PreviousState::Kept,
            Reservation::None,
        );
        assert_eq!(
            counted.deviations(),
            vec!["untouched_chunk_counted_when_down"]
        );
    }

    /// Each rule as the pages wrote it deviates in exactly its own name
    #[test]
    fn every_documented_rule_deviates_in_exactly_its_own_name() {
        let rules = StripePolicy::documented_rules();
        assert_eq!(rules.len(), 10);
        for (name, policy) in rules {
            assert_eq!(policy.deviations(), vec![name]);
        }
    }

    /// A policy survives the trip through a schedule file
    #[test]
    fn a_stripe_policy_round_trips_through_json() {
        let small = StripePolicy::small_write_settings()
            .into_iter()
            .map(|(_, policy, _)| policy);
        for policy in StripePolicy::unsafe_settings()
            .into_iter()
            .map(|(_, policy, _, _)| policy)
            .chain(small)
        {
            let json = serde_json::to_string(&policy).unwrap();
            let back: StripePolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(back, policy);
        }
    }

    /// The small write in its commit is the contract too, and is named in the variant only when taken
    #[test]
    fn the_small_write_in_its_commit_is_not_a_deviation() {
        let policy = StripePolicy::safe().in_commit();
        assert!(policy.deviations().is_empty());
        assert_eq!(policy.variant(), "confirmed/keep-prev/no-reserve/in-commit");
        assert_eq!(
            StripePolicy::safe().variant(),
            "confirmed/keep-prev/no-reserve"
        );
    }

    /// Each rule the small write depends on, as one might write it, deviates in exactly its own name
    #[test]
    fn every_small_write_setting_deviates_in_exactly_its_own_name() {
        let settings = StripePolicy::small_write_settings();
        assert_eq!(settings.len(), 7);
        for (name, policy, clauses) in settings {
            assert_eq!(policy.deviations(), vec![name]);
            assert_eq!(policy.small_writes.path, SmallWritePath::InCommit);
            assert!(!clauses.is_empty());
        }
    }

    /// A policy written before the small write was modelled reads as the staged path, and the safe
    /// staged policy writes nothing of it, so every schedule saved before is unchanged
    #[test]
    fn a_policy_from_before_the_small_write_reads_as_staged() {
        let json = serde_json::to_string(&StripePolicy::safe()).unwrap();
        assert!(!json.contains("small_writes"));
        let back: StripePolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, StripePolicy::safe());
        let taken = serde_json::to_string(&StripePolicy::safe().in_commit()).unwrap();
        assert!(taken.contains("in_commit"));
    }
}
