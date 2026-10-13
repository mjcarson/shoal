//! The stripe row's rules held to the stripe model, over its saved schedules and generated ones
//!
//! The product marks a missed position in its label where the model keeps a `missed` flag beside
//! it ([F86](../../docs/src/features/replicated-pools.md)). This replays every schedule the safe
//! policy runs, walks each stripe's committed history, and holds the product's next-row functions
//! and predicates to the model's at every step: the same sequence, labels and marks, stamp, fence,
//! tombstone and pending bytes, and the same answer to whether a chunk at a label makes the row's
//! state, which labels are current, and whether a position is.

use shoal_model::stripe::content::Unit;
use shoal_model::stripe::group::{RowState, StripeCommand};
use shoal_model::stripe::ids::{Label as ModelLabel, Pos};
use shoal_model::stripe::policy::{PreviousState, Reservation, UntouchedRule};
use shoal_model::stripe::{generate, Layout, StripeParams, StripePolicy, StripeSchedule, StripeWorld};
use shoal_proto::shared::objects::{Label, PendingBytes, PendingUnit};
use shoal_proto::shared::stripes::{StripeRow, Touch, WriteCommit};

/// How many generated schedules each configuration runs
const SEEDS: u64 = 6;

/// A model label as the product's
///
/// # Arguments
///
/// * `label` - The model's label
fn label(label: ModelLabel) -> Label {
    Label {
        sequence: u64::from(label.seq.0),
        tag: u64::from(label.tag.0),
    }
}

/// A model unit's value as the bytes the product would hold for it
///
/// # Arguments
///
/// * `unit` - The model's unit
fn unit_bytes(unit: Unit) -> Vec<u8> {
    // each value a distinct eight bytes, so two units compare as their values do
    let word = match unit {
        Unit::Zero => 0u64,
        Unit::Write(op) => u64::from(op.0) + 1,
        Unit::Garbage => u64::MAX - 1,
        Unit::Torn => u64::MAX,
    };
    word.to_le_bytes().to_vec()
}

/// A model row as the product's row, its missed flags marked into its labels
///
/// # Arguments
///
/// * `row` - The model's row state
fn encode(row: &RowState) -> StripeRow {
    // every position's label, marked where the model calls it missed
    let labels = row
        .labels
        .iter()
        .zip(&row.missed)
        .map(|(named, missed)| {
            let named = label(*named);
            if *missed {
                named.marked()
            } else {
                named
            }
        })
        .collect();
    let pending = row.pending_bytes.as_ref().map(|pending| {
        Box::new(PendingBytes {
            label: label(pending.label),
            base: pending.base.iter().copied().map(label).collect(),
            chain: pending.chain.iter().copied().map(label).collect(),
            units: pending
                .units
                .iter()
                .map(|(index, value)| PendingUnit {
                    index: u32::from(*index),
                    bytes: unit_bytes(*value),
                })
                .collect(),
        })
    });
    StripeRow {
        consumer: 1,
        object: 2,
        stripe: 0,
        sequence: u64::from(row.seq.0),
        labels,
        length: 0,
        stamp: u64::from(row.stamp.0),
        fence: u64::from(row.fence.0),
        tombstone: row.tombstone,
        pending,
    }
}

/// The product's next row for a command the model committed over a row
///
/// # Arguments
///
/// * `row` - The product's row before it
/// * `cmd` - The command
fn next(row: &StripeRow, cmd: &StripeCommand) -> StripeRow {
    match cmd {
        StripeCommand::Write {
            read_epoch,
            tag,
            touched,
            staged,
            counted,
            units,
            bytes_in_commit,
            ..
        } => {
            let written = Label {
                sequence: row.sequence + 1,
                tag: u64::from(tag.0),
            };
            // a touched position is held on its stage, or for a small write on its confirmation
            let positions = (0..row.labels.len())
                .map(|index| {
                    let pos = Pos(index as u8);
                    if !touched.contains(&pos) {
                        return Touch::Untouched;
                    }
                    let held = if *bytes_in_commit {
                        counted.iter().any(|(counted, _)| *counted == pos)
                    } else {
                        staged.contains(&pos)
                    };
                    if held {
                        Touch::Held
                    } else {
                        Touch::Missed
                    }
                })
                .collect();
            // a small write leaves its units merged over what was pending; a staged one leaves none
            let pending = bytes_in_commit.then(|| {
                let written_units: Vec<PendingUnit> = units
                    .iter()
                    .map(|(index, value)| PendingUnit {
                        index: u32::from(*index),
                        bytes: unit_bytes(*value),
                    })
                    .collect();
                PendingBytes::after(row, written, &written_units)
            });
            row.commit(&WriteCommit {
                label: written,
                positions,
                stamp: u64::from(read_epoch.0),
                length: 0,
                pending,
            })
        }
        StripeCommand::Noop { .. } => row.noop(),
        StripeCommand::Fence { epoch, .. } => row.fence(u64::from(epoch.0)),
        StripeCommand::ClearPendingBytes { holding, .. } => {
            let holding: Vec<bool> = (0..row.labels.len())
                .map(|index| holding.contains(&Pos(index as u8)))
                .collect();
            row.clear(&holding)
        }
        StripeCommand::Rebuild { pos, current, .. } => row.rebuilt(usize::from(pos.0), *current),
        StripeCommand::Reclaim { below, .. } => row.reclaimed(u64::from(below.0)),
    }
}

/// Every label a stripe's history names, which the predicates are asked about
///
/// # Arguments
///
/// * `history` - The stripe's committed states
fn every_label(history: &[RowState]) -> Vec<ModelLabel> {
    let mut labels: Vec<ModelLabel> = history
        .iter()
        .flat_map(|row| {
            let pending = row.pending_bytes.iter().flat_map(|pending| {
                std::iter::once(pending.label)
                    .chain(pending.base.iter().copied())
                    .chain(pending.chain.iter().copied())
            });
            row.labels.iter().copied().chain(pending)
        })
        .collect();
    labels.sort_unstable();
    labels.dedup();
    labels
}

/// Hold one replayed schedule's stripes to the product's rules
///
/// Returns how many commands were checked.
///
/// # Arguments
///
/// * `name` - What the schedule is called, for a failure
/// * `schedule` - The schedule
fn conform(name: &str, schedule: &StripeSchedule) -> usize {
    let world = StripeWorld::replay_world(schedule);
    let mut checked = 0;
    for (stripe, group) in world.rows.iter().enumerate() {
        let labels = every_label(&group.history);
        let mut row = encode(&group.history[0]);
        for (step, cmd) in group.commands.iter().enumerate() {
            let model = &group.history[step + 1];
            row = next(&row, cmd);
            // the same state, field by field
            assert_eq!(
                row,
                encode(model),
                "{name}: stripe {stripe} step {step} after {cmd:?}"
            );
            // and the same answers to every question a holder or a reader asks of it
            for index in 0..row.labels.len() {
                let pos = Pos(index as u8);
                assert_eq!(
                    row.current(index),
                    model.current(pos),
                    "{name}: stripe {stripe} step {step} current({index})"
                );
                let current: Vec<Label> =
                    model.current_labels(pos).into_iter().map(label).collect();
                assert_eq!(
                    row.current_labels(index),
                    current,
                    "{name}: stripe {stripe} step {step} current_labels({index})"
                );
                for held in &labels {
                    assert_eq!(
                        row.makes(index, label(*held)),
                        model.makes(pos, *held),
                        "{name}: stripe {stripe} step {step} makes({index}, {held})"
                    );
                }
            }
            checked += 1;
        }
    }
    checked
}

/// Every saved schedule the safe policy runs, and generated ones at every layout, small writes in
/// their commits included, take the same states under the product's rules as under the model's
#[test]
fn stripe_rows_follow_the_model() {
    let mut checked = 0;
    // the saved schedules the contract's own policy runs; the rest break it on purpose
    for (path, schedule) in StripeSchedule::load_all() {
        if !schedule.policy.deviations().is_empty() {
            continue;
        }
        checked += conform(&path.display().to_string(), &schedule);
    }
    // and generated ones, every layout staged and r3 with small writes in their commits
    let mut configs: Vec<(Layout, bool)> =
        Layout::ALL.into_iter().map(|layout| (layout, false)).collect();
    configs.push((Layout::Replicated3, true));
    for (layout, in_commit) in configs {
        let mut policy = StripePolicy::safe_with(
            UntouchedRule::Confirmed,
            PreviousState::Kept,
            Reservation::None,
        );
        if in_commit {
            policy = policy.in_commit();
        }
        let params = StripeParams::default_small(layout);
        for seed in 0..SEEDS {
            let schedule = generate("conformance", seed, &params, policy);
            checked += conform(&format!("{} seed {seed}", layout.short()), &schedule);
        }
    }
    // and it held over something
    assert!(checked > 100, "only {checked} commands were checked");
}
