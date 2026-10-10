//! Shrinking a failing stripe schedule to the events that matter
//!
//! The tablet model's method on the stripe model's types: every subsequence of a valid schedule
//! is a valid schedule ([`StripeWorld::apply`] is total), so a failing one can be shrunk by
//! leaving events out and keeping the removal whenever the same failure is still found. Delta
//! debugging first, then one event at a time. The step of the failure is expected to move; the
//! clause and the detail, or the bound, are not.

use crate::invariants::Violation;
use crate::stripe::event::StripeEvent;
use crate::stripe::progress::ProgressFailure;
use crate::stripe::schedule::StripeSchedule;
use crate::stripe::world::StripeWorld;

/// What a schedule fails with: a clause, or a progress bound
#[derive(Debug, Clone, PartialEq, Eq)]
enum Failure {
    /// A clause
    Clause(Violation),
    /// A bound
    Bound(ProgressFailure),
}

/// The failure a schedule's replay finds, a clause before a bound
fn failure_of(schedule: &StripeSchedule) -> Option<Failure> {
    let outcome = StripeWorld::replay(schedule);
    match (outcome.violation, outcome.stalled) {
        (Some(violation), _) => Some(Failure::Clause(violation)),
        (None, Some(stalled)) => Some(Failure::Bound(stalled)),
        (None, None) => None,
    }
}

/// Whether two failures are the same, ignoring the step a clause broke at
fn same(a: &Failure, b: &Failure) -> bool {
    match (a, b) {
        (Failure::Clause(a), Failure::Clause(b)) => a.same_failure(b),
        (Failure::Bound(a), Failure::Bound(b)) => a.bound == b.bound && a.detail == b.detail,
        _ => false,
    }
}

/// Shrink a failing stripe schedule
///
/// Returns the schedule with as few events as this can find that still reproduce the same
/// failure, with `expected` and `stalled` refreshed from its own replay. A schedule that does not
/// fail is returned unchanged.
///
/// # Arguments
///
/// * `schedule` - The schedule to shrink
pub fn minimize(schedule: &StripeSchedule) -> StripeSchedule {
    let Some(target) = failure_of(schedule) else {
        return schedule.clone();
    };
    let events = ddmin(schedule, &target);
    let events = greedy(schedule, &target, events);
    let mut out = StripeSchedule {
        events,
        ..schedule.clone()
    };
    let outcome = StripeWorld::replay(&out);
    out.expected = outcome.violation;
    out.stalled = outcome.stalled;
    out
}

/// Whether a list of events still fails the same way
fn still_fails(schedule: &StripeSchedule, target: &Failure, events: &[StripeEvent]) -> bool {
    let candidate = StripeSchedule {
        events: events.to_vec(),
        ..schedule.clone()
    };
    failure_of(&candidate).is_some_and(|found| same(&found, target))
}

/// Zeller's delta debugging over the event list
fn ddmin(schedule: &StripeSchedule, target: &Failure) -> Vec<StripeEvent> {
    let mut events = schedule.events.clone();
    let mut n = 2;
    while events.len() >= 2 && n <= events.len() {
        let chunk = events.len().div_ceil(n);
        let mut reduced = false;
        for start in (0..events.len()).step_by(chunk) {
            // everything but this chunk
            let complement: Vec<StripeEvent> = events
                .iter()
                .enumerate()
                .filter(|(index, _)| *index < start || *index >= start + chunk)
                .map(|(_, event)| event.clone())
                .collect();
            if complement.len() < events.len() && still_fails(schedule, target, &complement) {
                events = complement;
                n = (n - 1).max(2);
                reduced = true;
                break;
            }
        }
        if !reduced {
            if n >= events.len() {
                break;
            }
            n = (n * 2).min(events.len());
        }
    }
    events
}

/// Remove single events while the failure persists
fn greedy(
    schedule: &StripeSchedule,
    target: &Failure,
    mut events: Vec<StripeEvent>,
) -> Vec<StripeEvent> {
    loop {
        let mut removed = false;
        let mut index = 0;
        while index < events.len() {
            let mut candidate = events.clone();
            candidate.remove(index);
            if still_fails(schedule, target, &candidate) {
                events = candidate;
                removed = true;
            } else {
                index += 1;
            }
        }
        if !removed {
            return events;
        }
    }
}
