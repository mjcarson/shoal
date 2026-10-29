//! The stripe row's rules held to the stripe model, over its saved schedules and generated ones
//!
//! The product marks a missed position in its label where the model keeps a `missed` flag beside
//! it ([F86](../../docs/src/features/replicated-pools.md)). This replays every schedule the safe
//! policy runs, walks each stripe's committed history, and holds the product's next-row functions
//! and predicates to the model's at every step: the same sequence, labels and marks, stamp, fence,
//! tombstone and pending bytes, and the same answer to whether a chunk at a label makes the row's
//! state, which labels are current, and whether a position is.
//!
//! Since M16 it also holds the product's missed record to the model's: every placement group
//! schedule's committed rows are derived by `PlacementState::derive` in the order the model's
//! group derived its own record, and the two records are the same entry for entry at every step -
//! the stripes each position is stale on, where each entry began, each write's label and the run
//! of units it changed, and whether a position overflowed and how often it dropped a stripe.

use shoal_model::stripe::content::Unit;
use shoal_model::stripe::group::{
    Missed as ModelMissed, MissedRecord, RecordStep, RowState, StripeCommand,
};
use shoal_model::stripe::ids::{Label as ModelLabel, Pos};
use shoal_model::stripe::policy::{PreviousState, Reservation, UntouchedRule};
use shoal_model::stripe::{generate, Layout, StripeParams, StripePolicy, StripeSchedule, StripeWorld};
use shoal_proto::shared::objects::{ChangedUnits, Label, PendingBytes, PendingUnit};
use shoal_proto::shared::placement_state::{Missed, PlacementCommand, PlacementState};
use shoal_proto::shared::poolmap::PlacementGroup;
use shoal_proto::shared::stripes::{StripeRow, Touch, WriteCommit, ANY_EPOCH};

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
        // the model never declares a stripe lost: that is an operator's word (F93)
        lost: false,
        pending,
        // what the model keeps apart from the row: its claim, and what it changed
        placement_epoch: 0,
        changed: ChangedUnits::NONE,
        prior: Label::PUT,
        owner: 0,
    }
}

/// A product row with what the model keeps apart from it cleared, to compare field by field
///
/// # Arguments
///
/// * `row` - The product's row
fn rows_part(row: &StripeRow) -> StripeRow {
    StripeRow {
        placement_epoch: 0,
        changed: ChangedUnits::NONE,
        prior: Label::PUT,
        ..row.clone()
    }
}

/// What a coordinator puts in a write's commit of what it changed: the model's rule, as the
/// product's coordinator applies it
///
/// # Arguments
///
/// * `layout` - The pool's layout
/// * `row` - The row the write commits over
/// * `touched` - The positions it touched
/// * `units` - The units it writes
/// * `bytes_in_commit` - Whether its bytes ride in its commit
/// * `pending` - The pending bytes it leaves, for a small write
fn changed_of(
    layout: Layout,
    row: &StripeRow,
    touched: &[Pos],
    units: &[(u8, Unit)],
    bytes_in_commit: bool,
    pending: Option<&PendingBytes>,
) -> ChangedUnits {
    let every = ChangedUnits::every(layout.width());
    // only a replicated stripe's chunk is the stripe's units, which a range names; an erasure
    // coded write changes the whole chunk at the positions it touched, and no other (F90)
    if !layout.is_replicated() {
        return ChangedUnits::whole(ChangedUnits::mask(
            touched.iter().map(|pos| usize::from(pos.0)),
        ));
    }
    if bytes_in_commit {
        let pending = pending.expect("a small write's bytes");
        return ChangedUnits::covering(every, pending.units.iter().map(|unit| unit.index));
    }
    // a staged write over pending bytes carried them whole, as does one of every unit
    if row.pending.is_some() || units.len() == layout.data_units() {
        return ChangedUnits::whole(every);
    }
    ChangedUnits::covering(every, units.iter().map(|(index, _)| u32::from(*index)))
}

/// The product's next row for a command the model committed over a row
///
/// # Arguments
///
/// * `layout` - The pool's layout
/// * `row` - The product's row before it
/// * `cmd` - The command
fn next(layout: Layout, row: &StripeRow, cmd: &StripeCommand) -> StripeRow {
    // the claim the coordinator puts on the commit: the epoch the model's command was staged
    // under, or any for one that changes no label
    let next = next_row(layout, row, cmd);
    match cmd.claim() {
        Some(epoch) => next.claiming(u64::from(epoch)),
        None if matches!(cmd, StripeCommand::Noop { .. } | StripeCommand::Fence { .. } | StripeCommand::Reclaim { .. }) => next,
        None => next.claiming(ANY_EPOCH),
    }
}

/// The product's next row for a command, before the coordinator puts its claim on it
///
/// # Arguments
///
/// * `layout` - The pool's layout
/// * `row` - The product's row before it
/// * `cmd` - The command
fn next_row(layout: Layout, row: &StripeRow, cmd: &StripeCommand) -> StripeRow {
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
            let changed = changed_of(layout, row, touched, units, *bytes_in_commit, pending.as_ref());
            row.commit(&WriteCommit {
                label: written,
                positions,
                stamp: u64::from(read_epoch.0),
                length: 0,
                pending,
                changed,
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
    let layout = world.layout();
    let mut checked = 0;
    // every stripe's product rows, by the index of the model state each was held to
    let mut products: Vec<Vec<StripeRow>> = Vec::new();
    for (stripe, group) in world.rows.iter().enumerate() {
        let labels = every_label(&group.history);
        let mut row = encode(&group.history[0]);
        let mut rows = vec![row.clone()];
        for (step, cmd) in group.commands.iter().enumerate() {
            let model = &group.history[step + 1];
            row = next(layout, &row, cmd);
            rows.push(row.clone());
            // the same state, field by field
            assert_eq!(
                rows_part(&row),
                encode(model),
                "{name}: stripe {stripe} step {step} after {cmd:?}"
            );
            // and the same claim: the epoch the model's command was staged under, or any
            assert_eq!(
                row.claims(),
                cmd.claim().map(u64::from),
                "{name}: stripe {stripe} step {step} claim of {cmd:?}"
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
        products.push(rows);
    }
    // the missed record, derived from the product's rows in the model's committed order
    if world.policy.placement.grouped() {
        let mut state = PlacementState::default();
        let key = (1, PlacementGroup::default());
        let bound = usize::from(world.params.record_bound);
        for (at, step) in world.pg.derived.iter().enumerate() {
            match step {
                RecordStep::Row(stripe, index) => {
                    let row = &products[usize::from(stripe.0)][*index as usize];
                    state.derive(key, (2, u64::from(stripe.0)), row, bound);
                }
                RecordStep::Backfill { pos, drops } => {
                    // refused or not, as the model's was
                    let _ = state.apply(&PlacementCommand::BackfillDone {
                        consumer: key.0,
                        pg: key.1,
                        position: pos.0,
                        drops: u64::from(*drops),
                    });
                }
            }
            checked += 1;
            // the model's record is the one after the last derivation; compare as each is made
            if at + 1 == world.pg.derived.len() {
                same_record(name, &world.pg.record, &state, key);
            }
        }
    }
    checked
}

/// Hold the product's record of one placement group to the model's, entry for entry
///
/// # Arguments
///
/// * `name` - What the schedule is called, for a failure
/// * `model` - The model's record
/// * `state` - The product's state
/// * `key` - The placement group
fn same_record(
    name: &str,
    model: &MissedRecord,
    state: &PlacementState,
    key: (u64, PlacementGroup),
) {
    let group = state.get(&key);
    for (pos, part) in model.positions.iter().enumerate() {
        let product = group.and_then(|group| group.record.get(pos));
        // whether it overflowed, and how often it dropped a stripe
        assert_eq!(
            part.overflowed,
            product.is_some_and(|product| product.overflowed),
            "{name}: position {pos} overflowed"
        );
        assert_eq!(
            u64::from(part.drops),
            product.map_or(0, |product| product.drops),
            "{name}: position {pos} drops"
        );
        // the same stripes, each with the same entry
        let stripes: Vec<u64> = part.stripes.keys().map(|stripe| u64::from(stripe.0)).collect();
        let theirs: Vec<u64> = product
            .map(|product| product.stripes.keys().map(|(_, stripe)| *stripe).collect())
            .unwrap_or_default();
        assert_eq!(stripes, theirs, "{name}: position {pos}'s stripes");
        for (stripe, entry) in &part.stripes {
            let product = product
                .and_then(|product| product.stripes.get(&(2, u64::from(stripe.0))))
                .expect("compared above");
            match (entry, product) {
                (ModelMissed::Whole, Missed::Whole) => {}
                (ModelMissed::Ranges { since, writes }, Missed::Ranges { since: theirs, writes: their_writes }) => {
                    assert_eq!(label(*since), *theirs, "{name}: stripe {} position {pos} since", stripe.0);
                    let ours: Vec<(Label, u32, u32)> = writes
                        .iter()
                        .map(|(written, units)| {
                            let low = u32::from(*units.iter().min().expect("a unit"));
                            let high = u32::from(*units.iter().max().expect("a unit"));
                            (label(*written), low, high - low + 1)
                        })
                        .collect();
                    assert_eq!(&ours, their_writes, "{name}: stripe {} position {pos} writes", stripe.0);
                }
                (ours, theirs) => panic!(
                    "{name}: stripe {} position {pos}: the model has {ours:?}, the product {theirs:?}",
                    stripe.0
                ),
            }
        }
    }
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

/// The placement group's saved schedules and generated ones, at every layout and r3 with small
/// writes in their commits, take the same rows and the same missed record under the product's
/// rules as under the model's (M16), in a group that holds stripes when its moves begin and in one
/// that holds nothing until its late put (item 250)
#[test]
fn the_missed_record_follows_the_model() {
    let mut checked = 0;
    for (path, schedule) in StripeSchedule::load_all() {
        if !schedule.policy.deviations().is_empty() || !schedule.policy.placement.grouped() {
            continue;
        }
        checked += conform(&path.display().to_string(), &schedule);
    }
    let mut configs: Vec<(Layout, bool)> =
        Layout::ALL.into_iter().map(|layout| (layout, false)).collect();
    configs.push((Layout::Replicated3, true));
    for (layout, in_commit) in configs {
        let mut policy = StripePolicy::safe().grouped();
        if in_commit {
            policy = policy.in_commit();
        }
        // a group's own world, and an empty group's: one stripe, which is the late put's
        for empty in [false, true] {
            let mut params = StripeParams::default_group(layout);
            if empty {
                params.stripes = 1;
            }
            let world = if empty { "empty group" } else { "group" };
            for seed in 0..SEEDS {
                let schedule = generate("conformance", seed, &params, policy);
                checked += conform(&format!("{} {world} seed {seed}", layout.short()), &schedule);
            }
        }
    }
    assert!(checked > 100, "only {checked} commands were checked");
}

/// A model slice as a product slice: a device of its own, at index zero
///
/// # Arguments
///
/// * `slice` - The model's slice
fn slice_ref(slice: shoal_model::stripe::ids::SliceId) -> shoal_proto::shared::poolmap::SliceRef {
    shoal_proto::shared::poolmap::SliceRef {
        device: shoal_proto::shared::devices::DeviceId(uuid::Uuid::from_u128(u128::from(slice.0) + 1)),
        index: 0,
    }
}

/// Hold one replayed schedule's floors and released slices to the product's rules (M20)
///
/// Every truncate the model's entry committed leaves the floors `floors_after` leaves, and every
/// move's switch and abort and every release the model's placement group committed leaves the
/// product's group naming the same slices left, at the same epoch. Returns how many commands were
/// checked.
///
/// # Arguments
///
/// * `name` - What the schedule is called, for a failure
/// * `schedule` - The schedule
fn conform_reclamation(name: &str, schedule: &StripeSchedule) -> usize {
    use shoal_model::stripe::group::{EntryCommand, PgCommand};
    use shoal_proto::shared::objects::{floors_after, Floor};
    let world = StripeWorld::replay_world(schedule);
    let mut checked = 0;
    // every truncate's floors, as the product prunes them
    for (step, cmd) in world.entry.commands.iter().enumerate() {
        if !matches!(cmd, EntryCommand::Truncate { .. }) {
            continue;
        }
        let floor = |floor: &shoal_model::stripe::group::Floor| Floor {
            length: u64::from(floor.len),
            epoch: u64::from(floor.epoch.0),
        };
        let before: Vec<Floor> = world.entry.history[step].floors.iter().map(floor).collect();
        let after: Vec<Floor> = world.entry.history[step + 1].floors.iter().map(floor).collect();
        let left = after.last().cloned().expect("a truncate leaves a floor");
        assert_eq!(floors_after(&before, left), after, "{name}: the floors after truncate {step}");
        checked += 1;
    }
    // every move's and release's slices left, as the product's group names them
    let mut state = PlacementState::default();
    let key = (1, PlacementGroup::default());
    let mut at = 0;
    for cmd in &world.pg.commands {
        let before = &world.pg.history[at];
        let product = match cmd {
            // a backfill changes no state of the group's own
            PgCommand::BackfillDone { .. } => continue,
            PgCommand::MoveBegin { op, expect, .. } => PlacementCommand::MoveBegin {
                consumer: key.0,
                pg: key.1,
                id: u64::from(op.0),
                expect: u64::from(*expect),
                generation: u64::from(before.generation.0) + 1,
                positions: (0..before.positions.len() as u8).collect(),
            },
            // the old holder of the position that moved, as the product's driver names it
            PgCommand::MoveSwitch { op } => PlacementCommand::MoveSwitch {
                consumer: key.0,
                pg: key.1,
                id: u64::from(op.0),
                left: before
                    .moving
                    .map(|moving| slice_ref(before.slice(moving.pos)))
                    .into_iter()
                    .collect(),
            },
            // the arrival the copier may have written to
            PgCommand::MoveAbort { op } => PlacementCommand::MoveAbort {
                consumer: key.0,
                pg: key.1,
                id: u64::from(op.0),
                left: before.moving.map(|moving| slice_ref(moving.to)).into_iter().collect(),
            },
            PgCommand::ReleaseDone { slice, .. } => PlacementCommand::ReleaseDone {
                consumer: key.0,
                pg: key.1,
                slice: slice_ref(*slice),
            },
        };
        let after = &world.pg.history[at + 1];
        at += 1;
        state
            .apply(&product)
            .unwrap_or_else(|refused| panic!("{name}: the product refused {product:?} the model committed: {refused:?}"));
        let view = state.view(&key);
        let mut theirs = view.left.clone();
        let mut ours: Vec<_> = after.left.iter().map(|slice| slice_ref(*slice)).collect();
        theirs.sort_unstable();
        ours.sort_unstable();
        assert_eq!(theirs, ours, "{name}: the slices left after {cmd:?}");
        assert_eq!(view.epoch, u64::from(after.epoch), "{name}: the epoch after {cmd:?}");
        checked += 1;
    }
    checked
}

/// Every saved schedule of M20's rules, and generated ones at every layout with and without a
/// placement group, take the same floors and the same slices left under the product's rules as
/// under the model's (M20)
#[test]
fn floors_and_released_slices_follow_the_model() {
    let mut checked = 0;
    for (path, schedule) in StripeSchedule::load_all() {
        if !schedule.policy.deviations().is_empty() || !schedule.policy.reclamation.released() {
            continue;
        }
        checked += conform_reclamation(&path.display().to_string(), &schedule);
    }
    for grouped in [true, false] {
        for layout in Layout::ALL {
            let mut policy = StripePolicy::safe().released();
            let params = if grouped {
                policy = policy.grouped();
                StripeParams::default_group(layout)
            } else {
                StripeParams::default_small(layout)
            };
            for seed in 0..SEEDS * 4 {
                let schedule = generate("conformance", seed, &params, policy);
                checked += conform_reclamation(&format!("{} seed {seed} grouped {grouped}", layout.short()), &schedule);
            }
        }
    }
    assert!(checked > 100, "only {checked} commands were checked");
}
