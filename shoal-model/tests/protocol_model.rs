//! The M0 acceptance tests over the protocol model
//!
//! Three tests, each named in the acceptance tables of `docs/src/distributed/`: the contract
//! holds under the safe policy and each unsafe knob is caught (C13), a saved schedule replays
//! to the violation it records and a fresh one minimizes to a reproducible core (C11), and the
//! oracle tells the three outcome contracts apart (C11).

use shoal_model::event::{ClientOp, MutationOp, OpResult};
use shoal_model::ids::{Attempt, Key, NodeId, OpId, TabletId, Value};
use shoal_model::minimize::minimize;
use shoal_model::oracle::{check, Ledger, OracleError, Outcome};
use shoal_model::schedule::{generate, stale_report_schedule, Schedule, ScheduleParams};
use shoal_model::{Coverage, Policy, Property, World};

/// How many seeds the safe policy is run under
const SAFE_SEEDS: u64 = 32;

/// Deterministic schedules preserve every acknowledged result, and each violation is caught
///
/// Two halves. Under the safe policy, thirty-two seeded schedules - with elections, crashes,
/// pauses, duplicated and dropped messages, retries and unknown outcomes, which the coverage
/// counts prove happened - produce no violation and a history the oracle accepts. Under each
/// unsafe knob, a saved schedule replays to exactly the `P`-numbered violation the contract
/// table names for it, and the B=100/C=101/A+B=102 schedule from C7 is among them.
#[test]
fn protocol_model_preserves_acknowledged_history() {
    // the contract holds
    let params = ScheduleParams::default_small();
    let mut coverage = Coverage::default();
    for seed in 0..SAFE_SEEDS {
        let schedule = generate("safe", seed, &params, Policy::safe());
        let outcome = World::replay(&schedule);
        assert!(
            outcome.violation.is_none(),
            "seed {seed} violated the contract under the safe policy: {}",
            outcome.violation.unwrap()
        );
        assert!(
            outcome.oracle.is_ok(),
            "seed {seed} produced a history the oracle refuses: {:?}",
            outcome.oracle
        );
        coverage.add(&outcome.coverage);
    }
    // and it held under something, not under nothing
    assert!(coverage.elections > 0, "no election happened: {coverage:?}");
    assert!(coverage.truncations > 0, "no log was ever truncated: {coverage:?}");
    assert!(coverage.crashes > 0, "no node crashed: {coverage:?}");
    assert!(coverage.pauses > 0, "no node paused: {coverage:?}");
    assert!(coverage.duplicates > 0, "no message was duplicated: {coverage:?}");
    assert!(coverage.retries > 0, "no attempt was retried: {coverage:?}");
    assert!(coverage.unknown_outcomes > 0, "no outcome was unknown: {coverage:?}");
    assert!(coverage.commits > 0, "nothing was committed: {coverage:?}");
    // every way of breaking it is caught, by the property it breaks
    let saved = Schedule::load_all();
    for (name, policy, property) in Policy::unsafe_knobs() {
        let files: Vec<&Schedule> = saved
            .iter()
            .map(|(_, schedule)| schedule)
            .filter(|schedule| schedule.policy.deviations() == vec![name])
            .collect();
        assert!(!files.is_empty(), "no saved schedule exercises {name}");
        for schedule in files {
            let expected = schedule
                .expected
                .as_ref()
                .unwrap_or_else(|| panic!("{} records no violation", schedule.name));
            assert_eq!(
                expected.property, property,
                "{} violates {} rather than {property}",
                schedule.name, expected.property
            );
            assert!(
                expected.detail.starts_with(&format!("{property}:")),
                "{}'s detail does not name its property: {}",
                schedule.name,
                expected.detail
            );
            let found = World::replay(schedule).violation;
            assert_eq!(
                found.as_ref(),
                Some(expected),
                "{} replayed to a different violation",
                schedule.name
            );
        }
        assert_eq!(policy.deviations(), vec![name]);
    }
    // the schedule the design pages tell: saved at a short prefix, and built at their numbers
    let (_, stale) = saved
        .iter()
        .find(|(_, schedule)| schedule.name.starts_with("stale_report_"))
        .expect("the stale report schedule is saved");
    assert_eq!(stale.expected.as_ref().map(|v| v.property), Some(Property::P5));
    let election = Policy::unsafe_knobs()
        .into_iter()
        .find(|(name, _, _)| *name == "election_by_heartbeat_max_report")
        .map(|(_, policy, _)| policy)
        .unwrap();
    let literal = stale_report_schedule(99, election);
    assert_eq!(literal.name, "stale_report_b100_c101_ab102");
    let expected = literal.expected.as_ref().expect("the stale report loses a write");
    assert_eq!(expected.property, Property::P5);
    assert!(
        expected.detail.contains("ends at 101") && expected.detail.contains("ends at 102"),
        "the stale report detail does not tell the story: {}",
        expected.detail
    );
    // and the same schedule under the contract loses nothing: C, one entry short, is refused
    // and B, which holds 102, leads
    let safe = stale_report_schedule(99, Policy::safe());
    assert!(
        safe.expected.is_none(),
        "the contract lost the write too: {}",
        safe.expected.unwrap()
    );
    let world = World::replay_world(&safe);
    let tablet = safe.params.tablets[0];
    assert!(world.group(NodeId(2), tablet).is_leader(), "B did not take over");
    assert!(!world.group(NodeId(3), tablet).is_leader(), "C led with a short log");
    assert_eq!(world.checker.committed_index(tablet).0, 102);
}

/// A seeded failure minimizes to a core that replays identically, and every saved one does
///
/// A fresh failure is found under the duplicate-acknowledgement knob, minimized, and shown to be
/// a subsequence that still fails the same way, to be one-minimal, and to survive the trip
/// through JSON. Then every file under `schedules/` replays to the violation it records and is
/// byte-identical to its own canonical form, so a file cannot drift from what it claims.
#[test]
fn saved_protocol_schedule_reproduces_failure() {
    let (name, policy, property) = Policy::unsafe_knobs()
        .into_iter()
        .find(|(name, _, _)| *name == "duplicate_ack_counts_again")
        .expect("the duplicate knob");
    let params = ScheduleParams::default_small();
    // a fresh failure, from the first seed that finds one
    let schedule = (0..16)
        .map(|seed| generate(name, seed, &params, policy))
        .find(|schedule| schedule.expected.is_some())
        .expect("a seed below 16 fails under the duplicate knob");
    let target = schedule.expected.clone().unwrap();
    assert_eq!(target.property, property);
    // minimized, it is a subsequence that fails the same way
    let small = minimize(&schedule);
    assert!(small.events.len() <= schedule.events.len());
    assert!(is_subsequence(&small.events, &schedule.events), "minimization reordered events");
    let found = World::replay(&small).violation.expect("the minimized schedule still fails");
    assert!(found.same_failure(&target), "minimized to a different failure: {found}");
    // and one-minimal: no single event can go
    for index in 0..small.events.len() {
        let mut shorter = small.clone();
        shorter.events.remove(index);
        let still = World::replay(&shorter)
            .violation
            .is_some_and(|found| found.same_failure(&target));
        assert!(!still, "event {index} of the minimized schedule was not needed");
    }
    // the trip through JSON changes nothing
    let back = Schedule::from_json(&small.to_json()).expect("the schedule parses");
    assert_eq!(back, small);
    assert_eq!(World::replay(&back).violation, small.expected);
    // every saved schedule replays to what it records, and is canonical
    let saved = Schedule::load_all();
    assert!(!saved.is_empty(), "no schedules are saved");
    for (path, schedule) in saved {
        let expected = schedule
            .expected
            .clone()
            .unwrap_or_else(|| panic!("{} records no violation", path.display()));
        let found = World::replay(&schedule).violation;
        assert_eq!(found, Some(expected), "{} replays differently", path.display());
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            text.trim_end(),
            schedule.to_json(),
            "{} is not in canonical form; regenerate it",
            path.display()
        );
    }
}

/// Whether one list is the other with events left out
fn is_subsequence<T: PartialEq>(small: &[T], big: &[T]) -> bool {
    let mut position = 0;
    for event in small {
        match big[position..].iter().position(|candidate| candidate == event) {
            Some(offset) => position += offset + 1,
            None => return false,
        }
    }
    true
}

/// The oracle permits an ambiguous effect only where the history rules allow one
///
/// C11's ledger: a successful operation happened once with its result, a rejected one never
/// happened, an unknown one happened zero times or once, and a successful retry of an unknown
/// one settles what it did. Seven hand-written histories on one key, each exercising one rule.
#[test]
fn history_oracle_distinguishes_unknown_and_rejected() {
    let key = Key(1);
    let insert = |v: u32| ClientOp::Mutate(MutationOp::Insert { key, value: Value(v) });
    let delete = || ClientOp::Mutate(MutationOp::Delete { key });
    let read = || ClientOp::Read { key };
    let saw = |v: Option<u32>| Outcome::Ok(OpResult::Value(v.map(Value)));
    let applied = |b| Outcome::Ok(OpResult::Applied(b));
    // (a) an unknown insert, then a read that sees it: it may have happened
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, Outcome::Unknown),
        (2, 0, read(), 3, 4, saw(Some(7))),
    ]);
    assert_eq!(check(&history), Ok(()), "an unknown insert may have taken effect");
    // (b) a rejected insert, then a read that sees it: it never happened
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, Outcome::Rejected),
        (2, 0, read(), 3, 4, saw(Some(7))),
    ]);
    assert!(
        matches!(check(&history), Err(OracleError::NotLinearizable { .. })),
        "a rejected insert was allowed to take effect"
    );
    // (c) an unknown insert seen by a read, then a later insert that succeeds as if the key were
    // absent: the unknown one either happened or it did not, not both
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, Outcome::Unknown),
        (2, 0, read(), 3, 4, saw(Some(7))),
        (3, 0, insert(8), 5, 6, applied(true)),
    ]);
    assert!(
        matches!(check(&history), Err(OracleError::NotLinearizable { .. })),
        "an unknown insert was counted as both present and absent"
    );
    // (d) an unknown attempt, its retry succeeding, then another insert succeeding: the retry
    // settled that the identity took effect once, so the key was present
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, Outcome::Unknown),
        (1, 1, insert(7), 3, 4, applied(true)),
        (2, 0, insert(8), 5, 6, applied(true)),
    ]);
    assert!(
        matches!(check(&history), Err(OracleError::NotLinearizable { .. })),
        "a settled identity was allowed to take effect twice"
    );
    // (e) two successes at one identity with different results
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, applied(true)),
        (1, 1, insert(7), 3, 4, applied(false)),
    ]);
    assert!(
        matches!(check(&history), Err(OracleError::InconsistentRetry { id: OpId(1), .. })),
        "a retry was allowed a different result"
    );
    // (f) an unknown insert that never took effect, then an insert that succeeds as if absent
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, Outcome::Unknown),
        (2, 0, insert(8), 3, 4, applied(true)),
        (3, 0, read(), 5, 6, saw(Some(8))),
    ]);
    assert_eq!(check(&history), Ok(()), "an unknown insert may have taken no effect");
    // (g) delete and reinsert, with a stale One read between them seeing the old value
    let history = ledger(&[
        (1, 0, insert(7), 1, 2, applied(true)),
        (2, 0, delete(), 3, 4, applied(true)),
        (3, 0, insert(9), 5, 6, applied(true)),
        (4, 0, read(), 7, 8, saw(Some(7))),
        (5, 0, read(), 9, 10, saw(Some(9))),
    ]);
    assert_eq!(check(&history), Ok(()), "a stale committed-prefix read was refused");
}

/// A ledger on one tablet from (id, retry, op, invoke, complete, outcome)
fn ledger(rows: &[(u32, u8, ClientOp, u64, u64, Outcome)]) -> Ledger {
    let mut ledger = Ledger::default();
    for (id, retry, op, invoke, complete, outcome) in rows {
        let attempt = Attempt {
            id: OpId(*id),
            retry: *retry,
        };
        ledger.invoke(attempt, TabletId::new(1, 0), op.clone(), *invoke);
        assert!(ledger.complete(attempt, *complete, *outcome));
    }
    ledger
}

/// The stale-report schedule is written by the builder, not by hand, so a small one is checked
/// here for the shape the big one relies on
#[test]
fn the_stale_report_schedule_loses_the_write_at_any_size() {
    let policy = Policy::unsafe_knobs()
        .into_iter()
        .find(|(name, _, _)| *name == "election_by_heartbeat_max_report")
        .map(|(_, policy, _)| policy)
        .unwrap();
    for prefix in [1, 2, 4] {
        let schedule = stale_report_schedule(prefix, policy);
        let violation = schedule.expected.expect("the write is lost");
        assert_eq!(violation.property, Property::P5, "{violation}");
        assert!(
            violation.detail.contains(&format!("ends at {}", prefix + 2))
                && violation.detail.contains(&format!("ends at {}", prefix + 3)),
            "{violation}"
        );
        assert_eq!(schedule.events.iter().filter(|e| matches!(e, shoal_model::Event::Crash { node: NodeId(1) })).count(), 1);
    }
}
