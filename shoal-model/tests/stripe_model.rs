//! The stripe model's acceptance tests (X1, kept as M11's)
//!
//! `object_model_preserves_acknowledged_bytes` is S18's and `every_unsafe_policy_has_a_saved_schedule`
//! S16's, both named in the acceptance tables of `docs/src/object-storage/` for M11. The rest hold
//! the saved schedules to what they record: S7's sixteen, the rules as the pages wrote them that the
//! model found unsafe, and the two settings the progress check judges.

use std::collections::BTreeSet;

use shoal_model::invariants::Property;
use shoal_model::schedule::Schedule;
use shoal_model::stripe::minimize::minimize;
use shoal_model::stripe::policy::{PreviousState, Reservation, UntouchedRule};
use shoal_model::stripe::scenarios::{findings, policy_named, s7};
use shoal_model::stripe::{
    generate, Layout, StripeCoverage, StripeParams, StripePolicy, StripeSchedule, StripeWorld,
};

/// How many seeds of each configuration the safe policy is run under
const SAFE_SEEDS: u64 = 4;

/// Generated schedules of crashes, lost and reordered messages, leader changes, device loss and map
/// changes preserve every acknowledged byte range under the safe policy
///
/// Every layout, with the holder keeping a chunk's previous state and not, and with the leader's
/// reservation and without: no clause breaks, every reader begun once the faults stop finishes
/// within its bound, the stagers on a stripe commit within theirs, and the coverage counts prove the
/// runs did what the failure model allows rather than nothing.
#[test]
fn object_model_preserves_acknowledged_bytes() {
    let mut coverage = StripeCoverage::default();
    for layout in Layout::ALL {
        for (previous, reservation) in [
            (PreviousState::Kept, Reservation::None),
            (PreviousState::Dropped, Reservation::None),
            (PreviousState::Kept, Reservation::Granted),
        ] {
            let policy = StripePolicy::safe_with(UntouchedRule::Confirmed, previous, reservation);
            let params = StripeParams::default_small(layout);
            for seed in 0..SAFE_SEEDS {
                let schedule = generate("safe", seed, &params, policy);
                let outcome = StripeWorld::replay(&schedule);
                assert!(
                    outcome.violation.is_none(),
                    "{} {} seed {seed} broke the contract: {}",
                    layout.short(),
                    policy.variant(),
                    outcome.violation.unwrap()
                );
                assert!(
                    outcome.stalled.is_none(),
                    "{} {} seed {seed} broke a progress bound: {:?}",
                    layout.short(),
                    policy.variant(),
                    outcome.stalled
                );
                coverage.add(&outcome.coverage);
            }
        }
    }
    // and it held under something, not under nothing
    let counts = [
        ("crashes", coverage.crashes),
        ("restarts", coverage.restarts),
        ("disk failures", coverage.disk_failures),
        ("disk replacements", coverage.disk_replacements),
        ("fills", coverage.fills),
        ("moves", coverage.moves),
        ("rebuilds", coverage.rebuilds),
        ("truncates", coverage.truncates),
        ("extensions", coverage.extensions),
        ("no-ops", coverage.noops),
        ("refusals", coverage.refusals),
        ("unknown outcomes", coverage.unknown_outcomes),
        ("strong reads", coverage.strong_reads),
        ("default reads", coverage.default_reads),
        ("lagging answers", coverage.lagging_answers),
        ("discards", coverage.discards),
        ("reclaims", coverage.reclaims),
        ("retries", coverage.retries),
        ("duplicates", coverage.duplicates),
        ("acknowledged writes", coverage.acks),
    ];
    for (name, count) in counts {
        assert!(count > 0, "no {name} happened: {coverage:?}");
    }
}

/// What a commit's condition has to compare at the group: the sequence and the generation
///
/// The safe policy refuses a commit whose row moved, whose generation moved, whose stamp is past
/// the epoch its writer read, or whose fence is. Every committed state that changed the stamp or
/// the fence also moved the sequence, so a stager that read the row can judge those two itself,
/// and the group's condition is equality on the sequence and the generation alone: what F68's
/// conditional write offers.
#[test]
fn a_commit_compares_only_the_sequence_and_the_generation() {
    let (mut stamps, mut fences) = (0, 0);
    for layout in Layout::ALL {
        let params = StripeParams::default_small(layout);
        for seed in 0..SAFE_SEEDS * 2 {
            let schedule = generate("condition", seed, &params, StripePolicy::safe());
            let world = StripeWorld::replay_world(&schedule);
            // every pair of consecutive committed states of every stripe
            for group in &world.rows {
                for pair in group.history.windows(2) {
                    let (before, after) = (&pair[0], &pair[1]);
                    stamps += u32::from(before.stamp != after.stamp);
                    fences += u32::from(before.fence != after.fence);
                    if before.stamp != after.stamp || before.fence != after.fence {
                        assert_ne!(
                            before.seq,
                            after.seq,
                            "{} seed {seed}: a commit moved the stamp or the fence and not the sequence",
                            layout.short()
                        );
                    }
                }
            }
        }
    }
    // and the runs moved both, so the check was asked something
    assert!(stamps > 0 && fences > 0, "stamps {stamps}, fences {fences}");
}

/// Each unsafe setting replays to the violation recorded for it
///
/// Every one of S16's fourteen settings has a saved schedule that deviates from the contract in
/// that setting alone, records a violation of a clause S16 names for it, and replays to exactly
/// that violation. The setting S16 holds to the progress check alone records the bound it breaks.
#[test]
fn every_unsafe_policy_has_a_saved_schedule() {
    let saved = StripeSchedule::load_all();
    for (name, _, clauses, _) in StripePolicy::unsafe_settings() {
        let files: Vec<&StripeSchedule> = saved
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
            assert!(
                clauses.contains(&expected.property),
                "{} breaks {} where S16 names {clauses:?} for {name}",
                schedule.name,
                expected.property
            );
            assert!(expected
                .detail
                .starts_with(&format!("{}:", expected.property)));
            let found = StripeWorld::replay(schedule).violation;
            assert_eq!(
                found.as_ref(),
                Some(expected),
                "{} replayed otherwise",
                schedule.name
            );
        }
    }
    // the progress setting breaks the rebuild bound, and no clause
    let (name, _, s7_number) = StripePolicy::progress_setting();
    let schedule = saved
        .iter()
        .map(|(_, schedule)| schedule)
        .find(|schedule| schedule.policy.deviations() == vec![name])
        .unwrap_or_else(|| panic!("no saved schedule exercises {name}"));
    assert_eq!(schedule.s7, Some(s7_number));
    assert!(schedule.expected.is_none());
    let outcome = StripeWorld::replay(schedule);
    assert_eq!(
        outcome
            .stalled
            .as_ref()
            .map(|stalled| stalled.bound.as_str()),
        Some("rebuild")
    );
    assert_eq!(outcome.stalled, schedule.stalled);
}

/// Every schedule of S7 is saved, rebuilt the same, and rejected by the safe policy
///
/// The sixteen are built by hand as functions of the policy. Built under the setting each is saved
/// under, it equals its file byte for byte; built under the safe policy, the same story breaks no
/// clause and no bound.
#[test]
fn every_s7_schedule_is_saved_and_the_safe_policy_rejects_it() {
    let saved = StripeSchedule::load_all();
    let numbers: BTreeSet<u8> = saved
        .iter()
        .filter_map(|(_, schedule)| schedule.s7)
        .collect();
    assert_eq!(numbers, (1..=16).collect::<BTreeSet<u8>>());
    for (number, setting, build) in s7() {
        let unsafe_run = build(policy_named(setting));
        let (_, file) = saved
            .iter()
            .find(|(_, schedule)| schedule.name == unsafe_run.name)
            .unwrap_or_else(|| panic!("{} is not saved", unsafe_run.name));
        assert_eq!(&unsafe_run, file, "{} was not regenerated", unsafe_run.name);
        assert_eq!(unsafe_run.s7, number);
        let safe_run = build(StripePolicy::safe());
        assert_eq!(
            safe_run.expected, None,
            "the safe policy breaks a clause in S7's schedule {number:?}"
        );
        assert_eq!(
            safe_run.stalled, None,
            "the safe policy breaks a bound in S7's schedule {number:?}"
        );
    }
}

/// The rules as the pages wrote them that the model found unsafe each break, and the repairs hold
///
/// Q16's two answers that count a chunk on the row's word break P11; the stamp, the hidden units,
/// the deleted row, the entry a reader hides by, the row a default read takes, the tag, the
/// unfenced truncate and a write discarded beneath a later one each break the clause their saved
/// schedule records. The safe policy replays
/// the hand-built ones to nothing.
#[test]
fn the_rules_as_written_break_and_their_repairs_hold() {
    let saved = StripeSchedule::load_all();
    for (name, _) in StripePolicy::documented_rules() {
        let schedule = saved
            .iter()
            .map(|(_, schedule)| schedule)
            .find(|schedule| schedule.policy.deviations() == vec![name])
            .unwrap_or_else(|| panic!("no saved schedule breaks {name}"));
        let expected = schedule
            .expected
            .as_ref()
            .unwrap_or_else(|| panic!("{} records no violation", schedule.name));
        assert_eq!(
            StripeWorld::replay(schedule).violation.as_ref(),
            Some(expected)
        );
    }
    for (_, setting, build) in findings() {
        assert_eq!(
            build(policy_named(setting)).expected.map(|v| v.property),
            if setting.starts_with("untouched") {
                Some(Property::P11)
            } else {
                Some(Property::P13)
            }
        );
        let safe = build(StripePolicy::safe());
        assert_eq!(safe.expected, None, "{setting}'s repair breaks a clause");
    }
}

/// Every saved stripe schedule replays to what it records and is in canonical form
#[test]
fn saved_stripe_schedules_replay_and_are_canonical() {
    let saved = StripeSchedule::load_all();
    assert_eq!(saved.len(), 26);
    for (path, schedule) in &saved {
        let outcome = StripeWorld::replay(schedule);
        assert_eq!(outcome.violation, schedule.expected, "{}", path.display());
        assert_eq!(outcome.stalled, schedule.stalled, "{}", path.display());
        assert!(
            schedule.expected.is_some() || schedule.stalled.is_some(),
            "{} records nothing",
            path.display()
        );
        let text = std::fs::read_to_string(path).expect("the file");
        assert_eq!(text.trim_end(), schedule.to_json(), "{}", path.display());
    }
    // and the tablet model's loader neither sees them nor loses its own
    assert_eq!(Schedule::load_all().len(), 8);
}

/// A generated failure minimizes to a subsequence that fails the same way and survives a file
#[test]
fn a_stripe_failure_minimizes_to_a_reproducible_core() {
    let policy = policy_named("commit_with_no_condition");
    let params = StripeParams::default_small(Layout::Replicated3);
    let schedule = (0..64)
        .map(|seed| generate("no_condition", seed, &params, policy))
        .find(|schedule| schedule.expected.is_some())
        .expect("an unconditional commit is caught within 64 seeds");
    let target = schedule.expected.clone().expect("found");
    let small = minimize(&schedule);
    assert!(small.events.len() < schedule.events.len());
    let found = small.expected.clone().expect("still fails");
    assert!(found.same_failure(&target));
    // the minimized list is a subsequence of the original
    let mut original = schedule.events.iter();
    for event in &small.events {
        assert!(original.any(|candidate| candidate == event));
    }
    // and it survives a trip through a file
    let back = StripeSchedule::from_json(&small.to_json()).expect("parses");
    assert_eq!(back, small);
    assert_eq!(StripeWorld::replay(&back).violation, small.expected);
}
