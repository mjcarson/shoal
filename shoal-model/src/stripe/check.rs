//! The object contract's clauses, checked against durable facts after every event
//!
//! The checker never takes a stager's, a holder's or a reader's word for what is current. It
//! keeps its own ground truth from what the tablet groups committed - the stripe's data after
//! every committed state, the epoch every write's writer read - and judges each holder's synced
//! bytes, each discard, each acknowledgement and each read against it. Every check names the
//! `P` number it enforces, which is what `object_model_preserves_acknowledged_bytes` requires of
//! it ([S18](../../../docs/src/object-storage/contract.md#acceptance-tests)).
//!
//! Two readings are this model's own, and are written down on X1's record. P11 is judged at the
//! moment of each chunk's evidence - a synced stage's answer, a holder's confirmation, or for a
//! chunk counted on the row's word alone the acknowledgement itself - since a disk can fail the
//! instant after it answers and no protocol can prevent that. And a read is judged against the
//! sequential object at a point the entry's commits and the stripe's commits can both be cut at,
//! where a stripe commit falls after the truncates whose epochs its writer had read and before
//! the rest.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::OpId;
use crate::invariants::{Property, Violation};
use crate::stripe::content::{encode, Content, Unit};
use crate::stripe::group::{EntryCommand, Evidence, RowState, StripeCommand};
use crate::stripe::holder::HolderFact;
use crate::stripe::ids::{Epoch, Label, Pos, SliceId, StripeIx};
use crate::stripe::oracle::{OpKind, OpRecord, ReadResult, StripeOutcome};
use crate::stripe::schedule::StripeParams;
use crate::stripe::world::StripeWorld;

/// What a run exercised, so a run that exercised nothing cannot pass as evidence
///
/// Its `Debug` is written out rather than derived: the search folds it into every run's digest,
/// and the small write's counts are named only when one moved, so a run of a configuration from
/// before they existed digests as it did then.
#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripeCoverage {
    /// Nodes crashed
    pub crashes: u32,
    /// Nodes restarted
    pub restarts: u32,
    /// Applies a crash tore
    pub torn_applies: u32,
    /// Staged records written again after a restart
    pub replays: u32,
    /// Disks failed silently
    pub disk_failures: u32,
    /// Disks swapped for empty ones
    pub disk_replacements: u32,
    /// Disks filled
    pub fills: u32,
    /// Positions moved to another slice
    pub moves: u32,
    /// Stale positions rebuilt
    pub rebuilds: u32,
    /// Truncates committed
    pub truncates: u32,
    /// Sizes extended by a write
    pub extensions: u32,
    /// The leader's no-ops committed
    pub noops: u32,
    /// Commits refused by their condition
    pub refusals: u32,
    /// Operations whose outcome became unknown
    pub unknown_outcomes: u32,
    /// Strong reads that returned
    pub strong_reads: u32,
    /// Default reads that returned
    pub default_reads: u32,
    /// Group reads answered by a replica that lags
    pub lagging_answers: u32,
    /// Staged records and chunks discarded
    pub discards: u32,
    /// Stripes reclaimed
    pub reclaims: u32,
    /// Operations tried again
    pub retries: u32,
    /// Messages delivered twice
    pub duplicates: u32,
    /// Commands committed to stripe groups
    pub commits: u32,
    /// Writes acknowledged
    pub acks: u32,
    /// Small writes whose bytes committed into their rows
    #[serde(default)]
    pub small_writes: u32,
    /// Small writes that merged into bytes already pending
    #[serde(default)]
    pub merges: u32,
    /// Pending bytes a holder journalled to fold
    #[serde(default)]
    pub folds: u32,
    /// Pending bytes cleared from a row
    #[serde(default)]
    pub clears: u32,
    /// Chunks a reader, a rebuild or a stager took at a base with pending bytes laid over
    #[serde(default)]
    pub overlaid_reads: u32,
    /// Staged writes committed over pending bytes, carrying them
    #[serde(default)]
    pub staged_over_pending: u32,
    /// Stripes that ended the run with bytes still pending in their rows
    #[serde(default)]
    pub pending_at_end: u32,
}

impl std::fmt::Debug for StripeCoverage {
    /// Every count from before the small write was modelled, as a derived `Debug` writes them,
    /// then the small write's, only when one of them moved
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("StripeCoverage");
        out.field("crashes", &self.crashes)
            .field("restarts", &self.restarts)
            .field("torn_applies", &self.torn_applies)
            .field("replays", &self.replays)
            .field("disk_failures", &self.disk_failures)
            .field("disk_replacements", &self.disk_replacements)
            .field("fills", &self.fills)
            .field("moves", &self.moves)
            .field("rebuilds", &self.rebuilds)
            .field("truncates", &self.truncates)
            .field("extensions", &self.extensions)
            .field("noops", &self.noops)
            .field("refusals", &self.refusals)
            .field("unknown_outcomes", &self.unknown_outcomes)
            .field("strong_reads", &self.strong_reads)
            .field("default_reads", &self.default_reads)
            .field("lagging_answers", &self.lagging_answers)
            .field("discards", &self.discards)
            .field("reclaims", &self.reclaims)
            .field("retries", &self.retries)
            .field("duplicates", &self.duplicates)
            .field("commits", &self.commits)
            .field("acks", &self.acks);
        // the small write's counts are all zero in every run that never took it
        let small = [
            self.small_writes,
            self.merges,
            self.folds,
            self.clears,
            self.overlaid_reads,
            self.staged_over_pending,
            self.pending_at_end,
        ];
        if small.iter().any(|count| *count > 0) {
            out.field("small_writes", &self.small_writes)
                .field("merges", &self.merges)
                .field("folds", &self.folds)
                .field("clears", &self.clears)
                .field("overlaid_reads", &self.overlaid_reads)
                .field("staged_over_pending", &self.staged_over_pending)
                .field("pending_at_end", &self.pending_at_end);
        }
        out.finish()
    }
}

impl StripeCoverage {
    /// Add another run's counts to these
    ///
    /// # Arguments
    ///
    /// * `other` - The counts to add
    pub fn add(&mut self, other: &StripeCoverage) {
        self.crashes += other.crashes;
        self.restarts += other.restarts;
        self.torn_applies += other.torn_applies;
        self.replays += other.replays;
        self.disk_failures += other.disk_failures;
        self.disk_replacements += other.disk_replacements;
        self.fills += other.fills;
        self.moves += other.moves;
        self.rebuilds += other.rebuilds;
        self.truncates += other.truncates;
        self.extensions += other.extensions;
        self.noops += other.noops;
        self.refusals += other.refusals;
        self.unknown_outcomes += other.unknown_outcomes;
        self.strong_reads += other.strong_reads;
        self.default_reads += other.default_reads;
        self.lagging_answers += other.lagging_answers;
        self.discards += other.discards;
        self.reclaims += other.reclaims;
        self.retries += other.retries;
        self.duplicates += other.duplicates;
        self.commits += other.commits;
        self.acks += other.acks;
        self.small_writes += other.small_writes;
        self.merges += other.merges;
        self.folds += other.folds;
        self.clears += other.clears;
        self.overlaid_reads += other.overlaid_reads;
        self.staged_over_pending += other.staged_over_pending;
        self.pending_at_end += other.pending_at_end;
    }
}

/// The ground truth and the checks over it
#[derive(Debug, Clone, Default)]
pub struct StripeChecker {
    /// What the run exercised
    pub coverage: StripeCoverage,
    /// Disks lost in the run, which the generator holds to the pool's `f`
    pub losses: u32,
    /// For each stripe, its data units after each committed state
    spec: Vec<Vec<Vec<Unit>>>,
    /// The truncate epoch each write's writer read, by the write
    read_epochs: BTreeMap<OpId, Epoch>,
    /// P11's verdict on each write's commit, by stripe and index: the failure, if it failed
    p11: BTreeMap<(StripeIx, u32), Option<String>>,
    /// The stripe and index each write committed at
    commits: BTreeMap<OpId, (StripeIx, u32)>,
}

impl StripeChecker {
    /// The ground truth of an object its put just wrote
    ///
    /// # Arguments
    ///
    /// * `params` - The layout and the number of stripes
    pub fn new(params: &StripeParams) -> Self {
        let put = vec![Unit::Write(OpId(0)); params.layout.data_units()];
        let mut read_epochs = BTreeMap::new();
        read_epochs.insert(OpId(0), Epoch(0));
        Self {
            spec: vec![vec![put]; usize::from(params.stripes)],
            read_epochs,
            ..Self::default()
        }
    }

    /// A violation with the step left for the world to fill in
    fn violation(property: Property, detail: String) -> Violation {
        Violation {
            property,
            step: 0,
            detail: format!("{property}: {detail}"),
        }
    }

    /// The chunk a position must hold under a label, if a committed row state ever named it
    ///
    /// # Arguments
    ///
    /// * `world` - The world
    /// * `stripe` - The stripe
    /// * `pos` - The position
    /// * `label` - The label
    pub fn content_at(
        &self,
        world: &StripeWorld,
        stripe: StripeIx,
        pos: Pos,
        label: Label,
    ) -> Option<Content> {
        let history = &world.rows[usize::from(stripe.0)].history;
        let index = history.iter().position(|row| row.label(pos) == label)?;
        Some(encode(
            world.layout(),
            pos,
            &self.spec[usize::from(stripe.0)][index],
        ))
    }

    /// A command committed to a stripe's group: extend the truth, and judge the commit
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `index` - The index of the state it produced
    /// * `cmd` - The command
    /// * `world` - The world after it
    pub fn on_stripe_commit(
        &mut self,
        stripe: StripeIx,
        index: u32,
        cmd: &StripeCommand,
        world: &StripeWorld,
    ) -> Option<Violation> {
        let history = &world.rows[usize::from(stripe.0)].history;
        let before = &history[index as usize - 1];
        let spec = &mut self.spec[usize::from(stripe.0)];
        let mut data = spec[index as usize - 1].clone();
        // P11: pending bytes leave the row only once k + f holders said they hold their label
        if let StripeCommand::ClearPendingBytes { label, holding, .. } = cmd {
            spec.push(data);
            self.coverage.clears += 1;
            let floor = world.layout().ack_floor();
            if holding.len() < floor {
                return Some(Self::violation(
                    Property::P11,
                    format!(
                        "stripe {}'s pending bytes of {} left its row with {} chunks holding them where it needs {}",
                        stripe.0,
                        label,
                        holding.len(),
                        floor
                    ),
                ));
            }
            return None;
        }
        let StripeCommand::Write {
            op,
            base_exists,
            base,
            generation,
            read_epoch,
            counted,
            units,
            bytes_in_commit,
            ..
        } = cmd
        else {
            spec.push(data);
            return None;
        };
        // what the small write's path did, for the coverage counts
        if *bytes_in_commit {
            self.coverage.small_writes += 1;
            self.coverage.merges += u32::from(before.pending_bytes.is_some());
        } else if before.pending_bytes.is_some() {
            self.coverage.staged_over_pending += 1;
        }
        // the truth after a write is the truth before it with its units written
        for (unit, value) in units {
            data[usize::from(*unit)] = *value;
        }
        spec.push(data);
        self.read_epochs.entry(*op).or_insert(*read_epoch);
        self.commits.entry(*op).or_insert((stripe, index));
        // P8: a commit applied over a row it was not staged against
        if before.exists != *base_exists || before.seq != *base {
            return Some(Self::violation(
                Property::P8,
                format!(
                    "write {} committed to stripe {} at sequence {}{}, though it was staged against {}{}",
                    op.0,
                    stripe.0,
                    before.seq.0,
                    if before.exists { "" } else { " with no row" },
                    base.0,
                    if *base_exists { "" } else { " with no row" },
                ),
            ));
        }
        // P8: a commit applied under a generation it was not staged under
        if before.generation != *generation {
            return Some(Self::violation(
                Property::P8,
                format!(
                    "write {} committed to stripe {} under generation {}, though it was staged under {}",
                    op.0, stripe.0, before.generation.0, generation.0
                ),
            ));
        }
        // P11, judged now and reported if the write is acknowledged
        let verdict = self.judge_counted(world, stripe, before, *op, counted);
        self.p11.insert((stripe, index), verdict);
        None
    }

    /// Whether the chunks a write counted were current on the evidence it had
    ///
    /// # Arguments
    ///
    /// * `world` - The world
    /// * `stripe` - The stripe
    /// * `row` - The row the write read
    /// * `op` - The write
    /// * `counted` - What it counted, and on what evidence
    fn judge_counted(
        &self,
        world: &StripeWorld,
        stripe: StripeIx,
        row: &RowState,
        op: OpId,
        counted: &[(Pos, Evidence)],
    ) -> Option<String> {
        let floor = world.layout().ack_floor();
        let mut lost = Vec::new();
        for (pos, evidence) in counted {
            // a staged or confirmed chunk was current when its holder answered; what happens
            // to its disk after that is one of the f losses the write survives
            if *evidence != Evidence::RowWord {
                continue;
            }
            // a chunk counted on the row's word alone has to be current now
            let slice = row.slice(*pos);
            let current = world.slices.get(&slice).is_some_and(|s| {
                !s.gone
                    && world.disk_of(slice).is_some_and(|disk| disk.healthy)
                    && row
                        .current_labels(*pos)
                        .into_iter()
                        .any(|label| s.holds(stripe, *pos, label))
            });
            if !current {
                lost.push(pos.0);
            }
        }
        let current = counted.len() - lost.len();
        if current >= floor {
            return None;
        }
        Some(format!(
            "write {} to stripe {} was acknowledged with {} current chunks where it needs {}: it counted {}{}",
            op.0,
            stripe.0,
            current,
            floor,
            counted.len(),
            if lost.is_empty() {
                String::new()
            } else {
                format!(", on the row's word, positions {lost:?} that no longer held their chunks")
            }
        ))
    }

    /// A command committed to the entry's group
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the state it produced
    /// * `cmd` - The command
    /// * `world` - The world after it
    pub fn on_entry_commit(&mut self, index: u32, cmd: &EntryCommand, world: &StripeWorld) {
        // nothing to judge: a truncate's effect is judged where it is read
        let _ = (index, cmd, world);
    }

    /// An operation was answered: judge an acknowledgement and a read
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `outcome` - What it was told
    /// * `world` - The world
    pub fn on_complete(
        &mut self,
        op: OpId,
        outcome: &StripeOutcome,
        world: &StripeWorld,
    ) -> Option<Violation> {
        let record = world.ledger.records.get(&op)?;
        match (&record.kind, outcome) {
            (OpKind::Write { .. }, StripeOutcome::Ok) => {
                self.coverage.acks += 1;
                // P11: the acknowledgement of a write whose commit counted too few
                let key = self.commits.get(&op)?;
                if let Some(Some(detail)) = self.p11.get(key) {
                    return Some(Self::violation(Property::P11, detail.clone()));
                }
                None
            }
            (OpKind::Read { stripe, .. }, StripeOutcome::Read(result)) => {
                // P10: a read that took a chunk under a label its row does not name
                let row = &world.rows[usize::from(stripe.0)].history[result.row as usize];
                for (pos, label) in &result.used {
                    if row.label(*pos) != *label {
                        return Some(Self::violation(
                            Property::P10,
                            format!(
                                "read {} of stripe {} decoded position {} at {} beside a row that names {}",
                                op.0,
                                stripe.0,
                                pos.0,
                                label,
                                row.label(*pos)
                            ),
                        ));
                    }
                }
                self.judge_read(world, op, record, result, false)
            }
            _ => None,
        }
    }

    /// A holder did something: judge it
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `fact` - What it did
    /// * `world` - The world after it did it
    pub fn on_fact(
        &mut self,
        slice: SliceId,
        fact: &HolderFact,
        world: &StripeWorld,
    ) -> Option<Violation> {
        match fact {
            HolderFact::DiscardedStage { stripe, staged } => {
                // P16: a staged write discarded with no committed fact excluding it
                let history = &world.rows[usize::from(stripe.0)].history;
                // excluded by a row past its base that refers to it nowhere: not by the label it
                // names, and not as the chunk its pending bytes fold from
                let excluded = history.iter().any(|row| {
                    row.exists
                        && row.seq > staged.base
                        && !row.current_labels(staged.pos).contains(&staged.label)
                });
                if excluded {
                    return None;
                }
                let (_, latest) = world.rows[usize::from(stripe.0)].latest();
                Some(Self::violation(
                    Property::P16,
                    format!(
                        "slice {} discarded stripe {}'s staged {} at position {} with no committed fact excluding it: the row is at {}{}",
                        slice.0,
                        stripe.0,
                        staged.label,
                        staged.pos.0,
                        latest.seq.0,
                        if latest.exists { "" } else { " with no row" }
                    ),
                ))
            }
            HolderFact::DiscardedChunk { stripe, label } => {
                // P16: a chunk discarded with no reclamation committed past its label
                let group = &world.rows[usize::from(stripe.0)];
                let reclaimed = group.commands.iter().any(|cmd| {
                    matches!(cmd, StripeCommand::Reclaim { base, .. } if *base >= label.seq)
                });
                if reclaimed {
                    return None;
                }
                Some(Self::violation(
                    Property::P16,
                    format!(
                        "slice {} discarded stripe {}'s chunk {}, which no reclamation committed",
                        slice.0, stripe.0, label
                    ),
                ))
            }
            HolderFact::Replayed {
                stripe,
                label,
                before,
                after,
            } => {
                // P15: a record written again over a chunk that already held it changed the chunk
                if before.is_torn() || after.as_ref() == Some(before) {
                    return None;
                }
                Some(Self::violation(
                    Property::P15,
                    format!(
                        "slice {} wrote stripe {}'s staged {} again after a restart over a chunk that already held it, and changed it",
                        slice.0, stripe.0, label
                    ),
                ))
            }
            HolderFact::ApplyFailedForSpace { stripe, label } => Some(Self::violation(
                Property::P7,
                format!(
                    "slice {} could not apply stripe {}'s committed {}: its device filled after the commit",
                    slice.0, stripe.0, label
                ),
            )),
            HolderFact::Applied { .. } => None,
        }
    }

    /// The checks of what every holder has synced and what every row calls current
    ///
    /// # Arguments
    ///
    /// * `world` - The world
    pub fn after_step(&mut self, world: &StripeWorld) -> Option<Violation> {
        // P9: every synced chunk holds exactly what its label's commit made it
        for (id, slice) in &world.slices {
            if slice.gone || !world.disk_of(*id).is_some_and(|disk| disk.healthy) {
                continue;
            }
            let chunks = slice
                .chunks
                .iter()
                .map(|(stripe, chunk)| (*stripe, chunk, false))
                .chain(
                    slice
                        .previous
                        .iter()
                        .map(|(stripe, chunk)| (*stripe, chunk, true)),
                );
            for (stripe, chunk, previous) in chunks {
                if chunk.content.is_torn() {
                    // a torn apply is safe only while its staged copy can write it again
                    let redo = slice.staged.contains_key(&(stripe, chunk.label))
                        || slice.applying.contains_key(&stripe);
                    if redo || previous {
                        continue;
                    }
                    return Some(Self::violation(
                        Property::P9,
                        format!(
                            "slice {} holds stripe {} position {} torn under {}, with no staged copy to write it again",
                            id.0, stripe.0, chunk.pos.0, chunk.label
                        ),
                    ));
                }
                match self.content_at(world, stripe, chunk.pos, chunk.label) {
                    None => {
                        return Some(Self::violation(
                            Property::P9,
                            format!(
                                "slice {} holds stripe {} position {} under {}, which no committed row state names",
                                id.0, stripe.0, chunk.pos.0, chunk.label
                            ),
                        ))
                    }
                    Some(content) if content != chunk.content => {
                        return Some(Self::violation(
                            Property::P9,
                            format!(
                                "slice {} holds stripe {} position {} under {} with bytes that are not that label's",
                                id.0, stripe.0, chunk.pos.0, chunk.label
                            ),
                        ))
                    }
                    Some(_) => {}
                }
            }
        }
        // P17: every chunk a row calls current is held where the row says, unless its disk failed
        for (stripe, group) in world.rows.iter().enumerate() {
            let (_, row) = group.latest();
            if row.tombstone {
                continue;
            }
            for pos in world.layout().positions() {
                if !row.current(pos) {
                    continue;
                }
                let id = row.slice(pos);
                let Some(slice) = world.slices.get(&id) else {
                    continue;
                };
                if slice.gone || !world.disk_of(id).is_some_and(|disk| disk.healthy) {
                    continue;
                }
                // a chunk the row's pending bytes fold from into its label is current too
                let held = row
                    .current_labels(pos)
                    .into_iter()
                    .any(|label| slice.holds(StripeIx(stripe as u8), pos, label));
                if !held {
                    return Some(Self::violation(
                        Property::P17,
                        format!(
                            "the row calls stripe {} position {} current at {} on slice {}, which does not hold it",
                            stripe,
                            pos.0,
                            row.label(pos),
                            id.0
                        ),
                    ));
                }
            }
        }
        None
    }

    /// The end of the run: judge every read again against the whole history, and every refusal
    ///
    /// # Arguments
    ///
    /// * `world` - The world at the end
    pub fn judge_history(&self, world: &StripeWorld) -> Option<Violation> {
        for (op, record) in &world.ledger.records {
            match (&record.kind, &record.outcome) {
                (OpKind::Read { .. }, Some(StripeOutcome::Read(result))) => {
                    if let Some(violation) = self.judge_read(world, *op, record, result, true) {
                        return Some(violation);
                    }
                }
                // a write told it was refused changed nothing
                (OpKind::Write { stripe, .. }, Some(StripeOutcome::Refused))
                    if self.commits.contains_key(op) =>
                {
                    return Some(Self::violation(
                        Property::P9,
                        format!(
                            "write {} was told it was refused, and committed to stripe {}",
                            op.0, stripe.0
                        ),
                    ));
                }
                _ => {}
            }
        }
        None
    }

    /// The data units a read at a point of the merged order returns: hidden, clipped, or written
    ///
    /// Each unit is paired with whether a truncate hides it.
    ///
    /// # Arguments
    ///
    /// * `world` - The world
    /// * `stripe` - The stripe
    /// * `i` - The stripe's state
    /// * `j` - The entry's state
    fn expected(
        &self,
        world: &StripeWorld,
        stripe: StripeIx,
        i: u32,
        j: u32,
    ) -> Vec<(Option<Unit>, bool)> {
        let data = &self.spec[usize::from(stripe.0)][i as usize];
        let entry = &world.entry.history[j as usize];
        let start = u32::from(stripe.0) * world.layout().data_units() as u32;
        // the floor every truncate up to this state left, kept even after it was dropped
        let cuts: Vec<(u32, Epoch)> = world.entry.commands[..j as usize]
            .iter()
            .enumerate()
            .filter(|(_, cmd)| matches!(cmd, EntryCommand::Truncate { .. }))
            .filter_map(|(index, _)| {
                world.entry.history[index + 1]
                    .floors
                    .last()
                    .map(|floor| (floor.len, floor.epoch))
            })
            .collect();
        data.iter()
            .enumerate()
            .map(|(unit, value)| {
                let offset = start + unit as u32;
                if offset >= entry.size {
                    return (None, false);
                }
                let Unit::Write(writer) = value else {
                    return (Some(*value), false);
                };
                let read = self.read_epochs.get(writer).copied().unwrap_or_default();
                let hidden = cuts
                    .iter()
                    .any(|(len, epoch)| offset >= *len && read < *epoch);
                if hidden {
                    (Some(Unit::Zero), true)
                } else {
                    (Some(*value), false)
                }
            })
            .collect()
    }

    /// Whether a stripe state and an entry state are a cut of the one merged order
    ///
    /// A write whose writer read the epoch a truncate made, or a later one, comes after that
    /// truncate. A write whose writer read an earlier epoch and that wrote a unit the truncate
    /// cut comes before it: the floor hides that unit, which is the truncate cutting it. A write
    /// that read an earlier epoch and wrote nothing the truncate cut can go either side, since
    /// its bytes are the same whichever. And a write's extension of the size comes after its
    /// stripe's commit. A cut is a stripe state and an entry state that put no write on the
    /// wrong side of a truncate, and no extension before its write.
    ///
    /// # Arguments
    ///
    /// * `world` - The world
    /// * `stripe` - The stripe
    /// * `i` - The stripe's state
    /// * `j` - The entry's state
    fn consistent(&self, world: &StripeWorld, stripe: StripeIx, i: u32, j: u32) -> bool {
        let start = u32::from(stripe.0) * world.layout().data_units() as u32;
        let entry = &world.entry;
        world.rows[usize::from(stripe.0)]
            .commands
            .iter()
            .enumerate()
            .all(|(index, cmd)| {
                let StripeCommand::Write { op, units, .. } = cmd else {
                    return true;
                };
                let read = self.read_epochs.get(op).copied().unwrap_or_default();
                let included = (index as u32) < i;
                entry.commands.iter().enumerate().all(|(at, entry_cmd)| {
                    let at_included = (at as u32) < j;
                    match entry_cmd {
                        EntryCommand::Truncate { .. } => {
                            let Some(floor) = entry.history[at + 1].floors.last() else {
                                return true;
                            };
                            let after = read >= floor.epoch;
                            let before = read < floor.epoch
                                && units
                                    .iter()
                                    .any(|(unit, _)| start + u32::from(*unit) >= floor.len);
                            !(included && !at_included && after)
                                && !(!included && at_included && before)
                        }
                        // a write's extension comes after its stripe's commit
                        EntryCommand::Extend { op: extender, .. } => {
                            !(extender == op && at_included && !included)
                        }
                        EntryCommand::DropFloor { .. } | EntryCommand::Advance { .. } => true,
                    }
                })
            })
    }

    /// Judge a read against the sequential object
    ///
    /// It has to equal the object at some point the merged order can be cut at, between the
    /// states it read and the last ones a write or truncate concurrent with it could have made.
    /// A strong read's point is at or after every write and truncate acknowledged before it
    /// began.
    ///
    /// # Arguments
    ///
    /// * `world` - The world
    /// * `op` - The read
    /// * `record` - Its record
    /// * `result` - What it returned
    /// * `whole` - Whether the run is over, so commits after the read can explain it
    fn judge_read(
        &self,
        world: &StripeWorld,
        op: OpId,
        record: &OpRecord,
        result: &ReadResult,
        whole: bool,
    ) -> Option<Violation> {
        let OpKind::Read { stripe, strong } = record.kind else {
            return None;
        };
        let group = &world.rows[usize::from(stripe.0)];
        let complete = record.complete.unwrap_or(u64::MAX);
        let concurrent = |writer: OpId| {
            world
                .ledger
                .records
                .get(&writer)
                .is_some_and(|other| other.invoke < complete)
        };
        // the latest states a write or truncate concurrent with the read could explain it by
        let mut hi_i = result.row_seen;
        if whole {
            for (index, cmd) in group.commands.iter().enumerate().skip(hi_i as usize) {
                if let StripeCommand::Write { op: writer, .. } = cmd {
                    if !concurrent(*writer) {
                        break;
                    }
                }
                hi_i = index as u32 + 1;
            }
        }
        let mut hi_j = result.entry_seen;
        if whole {
            for (index, cmd) in world.entry.commands.iter().enumerate().skip(hi_j as usize) {
                if !concurrent(cmd.op()) {
                    break;
                }
                hi_j = index as u32 + 1;
            }
        }
        // a strong read sees every write and truncate acknowledged before it began
        let acked_before = |writer: OpId| {
            world.ledger.records.get(&writer).is_some_and(|other| {
                other.outcome.as_ref().is_some_and(StripeOutcome::is_ok)
                    && other.complete.is_some_and(|done| done < record.invoke)
            })
        };
        // a default read takes the entry at one replica too, so any committed entry state up to
        // the one it read may be the one its bytes are; a strong read's is at least that one
        let mut lo_i = result.row;
        let mut lo_j = if strong { result.entry } else { 0 };
        if strong {
            for (index, cmd) in group.commands.iter().enumerate() {
                if let StripeCommand::Write { op: writer, .. } = cmd {
                    if acked_before(*writer) {
                        lo_i = lo_i.max(index as u32 + 1);
                    }
                }
            }
            for (index, cmd) in world.entry.commands.iter().enumerate() {
                if matches!(
                    cmd,
                    EntryCommand::Truncate { .. } | EntryCommand::Extend { .. }
                ) && acked_before(cmd.op())
                {
                    lo_j = lo_j.max(index as u32 + 1);
                }
            }
        }
        let matches = |i: u32, j: u32| {
            self.consistent(world, stripe, i, j)
                && self
                    .expected(world, stripe, i, j)
                    .iter()
                    .map(|(unit, _)| *unit)
                    .eq(result.units.iter().copied())
        };
        for i in lo_i..=hi_i.max(lo_i) {
            for j in lo_j..=hi_j.max(lo_j) {
                if i <= hi_i && j <= hi_j && matches(i, j) {
                    return None;
                }
            }
        }
        // P13: against the states it read, a unit a truncate cut came back, or a later write was hidden
        let own = self.expected(world, stripe, result.row, result.entry);
        for (unit, ((expected, hidden), got)) in own.iter().zip(result.units.iter()).enumerate() {
            if expected == got {
                continue;
            }
            if *hidden && matches!(got, Some(Unit::Write(_))) {
                return Some(Self::violation(
                    Property::P13,
                    format!(
                        "read {} of stripe {} returned unit {}, which a truncate cut, as {:?}",
                        op.0, stripe.0, unit, got
                    ),
                ));
            }
            if matches!(expected, Some(Unit::Write(_))) && *got == Some(Unit::Zero) {
                return Some(Self::violation(
                    Property::P13,
                    format!(
                        "read {} of stripe {} hid unit {}, written by {:?} after every truncate that could cut it",
                        op.0, stripe.0, unit, expected
                    ),
                ));
            }
        }
        // a strong read that some earlier point explains missed an acknowledged write
        if strong {
            for i in result.row..=hi_i {
                for j in 0..=hi_j {
                    if matches(i, j) {
                        return Some(Self::violation(
                            Property::P12,
                            format!(
                                "strong read {} of stripe {} returned a state older than a write or truncate acknowledged before it began",
                                op.0, stripe.0
                            ),
                        ));
                    }
                }
            }
        }
        Some(Self::violation(
            Property::P12,
            format!(
                "read {} of stripe {} returned bytes that are no committed state of it",
                op.0, stripe.0
            ),
        ))
    }
}
