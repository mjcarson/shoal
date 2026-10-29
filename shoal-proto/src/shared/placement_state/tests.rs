//! The placement group's state and the missed record, held to the stripe model's rules

use super::*;
use crate::shared::objects::{ChangedUnits, Label};
use crate::shared::scrub::ScrubKind;
use crate::shared::stripes::{StripeRow, Touch, WriteCommit};

/// The placement group every test here is about
fn key() -> PgKey {
    (7, PlacementGroup { tablet: 3, sub: 0 })
}

/// A write's label at a sequence
///
/// # Arguments
///
/// * `sequence` - The sequence
fn label(sequence: u64) -> Label {
    Label {
        sequence,
        tag: 100 + sequence,
    }
}

/// A replicated stripe's row after a write of some units that one position missed
///
/// # Arguments
///
/// * `row` - The row it commits over
/// * `missed` - The position that missed it
/// * `first` - The first unit it changes
/// * `count` - How many units
fn write(row: &StripeRow, missed: Option<usize>, first: u32, count: u32) -> StripeRow {
    let positions = (0..row.labels.len())
        .map(|pos| {
            if Some(pos) == missed {
                Touch::Missed
            } else {
                Touch::Held
            }
        })
        .collect();
    row.commit(&WriteCommit {
        label: label(row.sequence + 1),
        positions,
        stamp: 0,
        length: 1 << 20,
        pending: None,
        changed: ChangedUnits::units(ChangedUnits::every(row.labels.len()), first, count),
    })
}

/// A write a position missed begins its entry at the label the row named before it, and a second
/// one it missed is added; the units a holder needs are every write after its own label
#[test]
fn a_missed_write_is_recorded_from_where_its_position_was_current() {
    let mut state = PlacementState::default();
    let put = StripeRow::put(7, 1, 0, 3, 1 << 20, 0);
    // position 2 misses units 4..6, then units 9..10
    let first = write(&put, Some(2), 4, 2);
    state.derive(key(), (1, 0), &first, 16);
    let second = write(&first, Some(2), 9, 2);
    state.derive(key(), (1, 0), &second, 16);
    let missed = state.get(&key()).expect("a record").missed((1, 0), 2).expect("an entry");
    assert_eq!(
        missed,
        &Missed::Ranges {
            since: Label::PUT,
            writes: vec![(label(1), 4, 2), (label(2), 9, 2)],
        }
    );
    // a holder at the put needs both, one at the first write the second alone
    assert_eq!(missed.units_after(Label::PUT), Some((4, 7)));
    assert_eq!(missed.units_after(label(1)), Some((9, 2)));
    // and one past every write recorded holds what the record cannot explain: whole
    assert_eq!(missed.units_after(label(2)), None);
    // the positions that held it have no entry
    assert!(state.get(&key()).unwrap().missed((1, 0), 0).is_none());
}

/// A holder below the label an entry began at fell back past a write the record never saw, and is
/// given the whole chunk: the stripe model's finding that made `since` part of an entry
#[test]
fn a_holder_below_where_its_entry_began_is_rebuilt_whole() {
    let mut state = PlacementState::default();
    let put = StripeRow::put(7, 1, 0, 3, 1 << 20, 0);
    // position 2 held the first write, then missed the second
    let first = write(&put, None, 0, 1);
    state.derive(key(), (1, 0), &first, 16);
    let second = write(&first, Some(2), 5, 1);
    state.derive(key(), (1, 0), &second, 16);
    let missed = state.get(&key()).unwrap().missed((1, 0), 2).unwrap().clone();
    // a holder that dropped the first write's record it never applied is back at the put
    assert_eq!(missed.units_after(Label::PUT), None);
    assert_eq!(missed.units_after(label(1)), Some((5, 1)));
}

/// A rebuild that makes a position current drops its entry, and the group is forgotten once
/// nothing of it is left to keep
#[test]
fn a_current_position_has_no_entry_and_a_group_at_its_base_is_forgotten() {
    let mut state = PlacementState::default();
    let put = StripeRow::put(7, 1, 0, 3, 1 << 20, 0);
    let first = write(&put, Some(1), 0, 1);
    state.derive(key(), (1, 0), &first, 16);
    assert!(state.get(&key()).is_some());
    let rebuilt = first.rebuilt(1, true);
    state.derive(key(), (1, 0), &rebuilt, 16);
    assert!(state.get(&key()).is_none(), "{state:?}");
}

/// A position's part holds at most its bound of stripes: past it a stripe is dropped, the part
/// says it overflowed, and a backfill clears that only if nothing was dropped while it walked
#[test]
fn the_record_is_bounded_and_a_backfill_clears_its_overflow_only_over_no_drop() {
    let mut state = PlacementState::default();
    let put = |stripe| StripeRow::put(7, 1, stripe, 3, 1 << 20, 0);
    for stripe in 0..3 {
        state.derive(key(), (1, stripe), &write(&put(stripe), Some(0), 0, 1), 2);
    }
    let part = &state.get(&key()).unwrap().record[0];
    assert_eq!(part.stripes.len(), 2);
    assert!(part.overflowed);
    assert_eq!(part.drops, 1);
    // a backfill that began before another drop is refused
    state.derive(key(), (1, 9), &write(&put(9), Some(0), 0, 1), 2);
    let done = |drops| PlacementCommand::BackfillDone {
        consumer: 7,
        pg: key().1,
        position: 0,
        drops,
    };
    assert_eq!(state.apply(&done(1)), Err(PlacementRefusal::Dropped));
    assert!(state.apply(&done(2)).is_ok());
    assert!(!state.get(&key()).unwrap().record[0].overflowed);
}

/// A whole chunk's commit, or more writes than an entry keeps, makes the entry whole
#[test]
fn a_whole_chunk_or_too_many_writes_make_an_entry_whole() {
    let mut state = PlacementState::default();
    let mut row = StripeRow::put(7, 1, 0, 3, 1 << 20, 0);
    for _ in 0..=MAX_MISSED_WRITES {
        row = write(&row, Some(0), 0, 1);
        state.derive(key(), (1, 0), &row, 16);
    }
    assert_eq!(state.get(&key()).unwrap().missed((1, 0), 0), Some(&Missed::Whole));
    // and a mark, which misses a position whole
    let mut other = PlacementState::default();
    let marked = StripeRow::put(7, 1, 0, 3, 1 << 20, 0).rebuilt(2, false);
    other.derive(key(), (1, 0), &marked, 16);
    assert_eq!(other.get(&key()).unwrap().missed((1, 0), 2), Some(&Missed::Whole));
}

/// A move begins from the epoch its driver read, switches or aborts under its own identity, and
/// moves the epoch at every step; anything else is refused and changes nothing
#[test]
fn a_move_begins_switches_and_aborts_under_its_epoch() {
    let mut state = PlacementState::default();
    let begin = |id, expect| PlacementCommand::MoveBegin {
        consumer: 7,
        pg: key().1,
        id,
        expect,
        generation: 4,
        positions: vec![0, 2, 1],
    };
    assert_eq!(state.apply(&begin(1, 3)), Err(PlacementRefusal::Epoch));
    assert!(state.apply(&begin(1, 0)).is_ok());
    assert_eq!(state.epoch(&key()), 1);
    assert_eq!(state.apply(&begin(2, 1)), Err(PlacementRefusal::Moving));
    let switch = |id| PlacementCommand::MoveSwitch {
        consumer: 7,
        pg: key().1,
        id,
        left: Vec::new(),
    };
    assert_eq!(state.apply(&switch(2)), Err(PlacementRefusal::NotMoving));
    assert!(state.apply(&switch(1)).is_ok());
    let group = state.get(&key()).unwrap();
    assert_eq!((group.epoch, group.generation, group.positions.clone()), (2, Some(4), Some(vec![0, 2, 1])));
    // a begin whose positions name one member twice is no move
    let twice = PlacementCommand::MoveBegin {
        consumer: 7,
        pg: key().1,
        id: 3,
        expect: 2,
        generation: 5,
        positions: vec![0, 0, 1],
    };
    assert_eq!(state.apply(&twice), Err(PlacementRefusal::NotPermutation));
    assert!(state.apply(&begin(3, 2)).is_ok());
    let abort = PlacementCommand::MoveAbort {
        consumer: 7,
        pg: key().1,
        id: 3,
        left: Vec::new(),
    };
    assert!(state.apply(&abort).is_ok());
    let group = state.get(&key()).unwrap();
    assert_eq!((group.epoch, group.generation, group.moving.clone()), (4, Some(4), None));
}

/// The state survives a trip through serde, which the checkpoint's sidecar and a snapshot's
/// trailer write it by
#[test]
fn the_state_round_trips_through_serde() {
    let mut state = PlacementState::default();
    let put = StripeRow::put(7, 1, 0, 3, 1 << 20, 0);
    state.derive(key(), (1, 0), &write(&put, Some(2), 3, 4), 16);
    let text = serde_json::to_string(&state).expect("serializes");
    let back: PlacementState = serde_json::from_str(&text).expect("deserializes");
    assert_eq!(back, state);
}

/// A history's state at an index is its base with every change at or below the index replayed, so a
/// cut between the checkpoint and the applied position sees exactly what its boundary's apply saw
#[test]
fn a_history_replays_to_any_boundary_above_its_base() {
    let put = StripeRow::put(7, 1, 0, 3, 1 << 20, 0);
    let missed = write(&put, Some(1), 0, 2);
    let begin = PlacementCommand::MoveBegin {
        consumer: 7,
        pg: key().1,
        id: 1,
        expect: 0,
        generation: 4,
        positions: vec![0, 1, 2],
    };
    let history = PlacementHistory {
        base: PlacementState::default(),
        changes: vec![
            (
                10,
                PlacementChange::Derive {
                    key: key(),
                    stripe: (1, 0),
                    row: RecordInput::of(&missed),
                    bound: 16,
                },
            ),
            (12, PlacementChange::Command(begin.clone())),
        ],
    };
    // nothing at or below the base's own index, the record at 10, the move at 12
    assert_eq!(history.at(9), PlacementState::default());
    let at_ten = history.at(11);
    assert_eq!(at_ten.epoch(&key()), 0);
    assert!(at_ten.get(&key()).unwrap().missed((1, 0), 1).is_some());
    let at_twelve = history.at(12);
    assert_eq!(at_twelve.epoch(&key()), 1);
    // and the same as applying them in order by hand
    let mut by_hand = PlacementState::default();
    by_hand.derive(key(), (1, 0), &missed, 16);
    by_hand.apply(&begin).expect("begins");
    assert_eq!(at_twelve, by_hand);
}

/// Every command round trips its fixed encoding, and a cut or padded one is no command
#[test]
fn a_command_round_trips_its_bytes() {
    let pg = key().1;
    let commands = [
        PlacementCommand::MoveBegin {
            consumer: 7,
            pg,
            id: 11,
            expect: 3,
            generation: 9,
            positions: vec![2, 0, 1],
        },
        PlacementCommand::MoveSwitch {
            consumer: 7,
            pg,
            id: 11,
            left: vec![crate::shared::poolmap::SliceRef {
                device: crate::shared::devices::DeviceId(uuid::Uuid::from_u128(5)),
                index: 1,
            }],
        },
        PlacementCommand::MoveAbort {
            consumer: 7,
            pg,
            id: 12,
            left: Vec::new(),
        },
        PlacementCommand::ReleaseDone {
            consumer: 7,
            pg,
            slice: crate::shared::poolmap::SliceRef {
                device: crate::shared::devices::DeviceId(uuid::Uuid::from_u128(5)),
                index: 1,
            },
        },
        PlacementCommand::BackfillDone {
            consumer: 7,
            pg,
            position: 2,
            drops: 40,
        },
        PlacementCommand::ScrubBegin {
            consumer: 7,
            pg,
            kind: ScrubKind::Deep,
            pass: 4,
            request: 2,
            asked: true,
            began_ms: 1_760_000_000_000,
        },
        PlacementCommand::ScrubStep {
            consumer: 7,
            pg,
            kind: ScrubKind::Light,
            pass: 4,
            after: Some((7, PlacementGroup { tablet: 2, sub: 9 })),
        },
        PlacementCommand::ScrubStep {
            consumer: 7,
            pg,
            kind: ScrubKind::Light,
            pass: 1,
            after: None,
        },
        PlacementCommand::ScrubEnd {
            consumer: 7,
            pg,
            kind: ScrubKind::Deep,
            pass: 4,
            finished_ms: 1_760_000_100_000,
        },
    ];
    for cmd in commands {
        let bytes = cmd.encode();
        assert_eq!(PlacementCommand::decode(&bytes), Some(cmd.clone()));
        assert_eq!(PlacementCommand::decode(&bytes[..bytes.len() - 1]), None);
        let mut padded = bytes.clone();
        padded.push(0);
        assert_eq!(PlacementCommand::decode(&padded), None);
    }
}

/// A scrub's pass begins one past the cursor, steps through its placement groups forward and
/// from where its driver read the cursor, and ends once; it never moves an epoch, and a command
/// that does not follow the cursor is refused and changes nothing (F89)
#[test]
fn a_scrub_cursor_moves_only_forward_and_from_where_it_was_read() {
    let mut state = PlacementState::default();
    let group = |sub: u16| (7, PlacementGroup { tablet: 3, sub });
    let begin = |pass: u64, request: u64| PlacementCommand::ScrubBegin {
        consumer: 7,
        pg: group(0).1,
        kind: ScrubKind::Deep,
        pass,
        request,
        asked: false,
        began_ms: 100,
    };
    let step = |pass: u64, after: Option<PgKey>, sub: u16| PlacementCommand::ScrubStep {
        consumer: 7,
        pg: group(sub).1,
        kind: ScrubKind::Deep,
        pass,
        after,
    };
    // a pass begins one past the cursor and nowhere else
    assert_eq!(state.apply(&begin(2, 0)), Err(PlacementRefusal::ScrubCursor));
    state.apply(&begin(1, 0)).expect("begins");
    assert!(state.scrub.deep.running());
    // a step from where the cursor is, forward
    state.apply(&step(1, None, 0)).expect("steps");
    state.apply(&step(1, Some(group(0)), 1)).expect("steps");
    // a step read from an older place, a step backward and a step of another pass are refused
    let before = state.clone();
    assert_eq!(state.apply(&step(1, Some(group(0)), 2)), Err(PlacementRefusal::ScrubCursor));
    assert_eq!(state.apply(&step(1, Some(group(1)), 1)), Err(PlacementRefusal::ScrubCursor));
    assert_eq!(state.apply(&step(2, Some(group(1)), 2)), Err(PlacementRefusal::ScrubCursor));
    assert_eq!(state, before);
    assert_eq!((state.scrub.deep.after, state.scrub.deep.done), (Some(group(1)), 2));
    // the light cursor never moved, and no epoch did
    assert_eq!(state.scrub.light, crate::shared::scrub::ScrubCursor::default());
    assert_eq!(state.epoch(&group(0)), 0);
    assert!(state.groups.is_empty());
    // the end, once; a step after it is refused
    let end = PlacementCommand::ScrubEnd {
        consumer: 7,
        pg: group(1).1,
        kind: ScrubKind::Deep,
        pass: 1,
        finished_ms: 900,
    };
    state.apply(&end).expect("ends");
    assert_eq!(state.apply(&end), Err(PlacementRefusal::ScrubCursor));
    assert_eq!(state.apply(&step(1, Some(group(1)), 2)), Err(PlacementRefusal::ScrubCursor));
    assert_eq!((state.scrub.deep.served, state.scrub.deep.last_ms), (Some(0), 900));
    // an operator's request begins the next pass, and a begin over a pass under way starts it again
    state.apply(&begin(2, 1)).expect("begins");
    state.apply(&step(2, None, 0)).expect("steps");
    state.apply(&begin(3, 2)).expect("begins again");
    assert_eq!((state.scrub.deep.after, state.scrub.deep.done, state.scrub.deep.request), (None, 0, 2));
    // the last finished pass is kept until another finishes
    assert_eq!((state.scrub.deep.served, state.scrub.deep.last_ms), (Some(0), 900));
    // and the cursor rides the state's serde, as the checkpoint's sidecar writes it
    let back: PlacementState = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    assert_eq!(back, state);
}
