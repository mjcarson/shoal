//! Shrinking a failing schedule to the events that matter
//!
//! Every subsequence of a valid schedule is a valid schedule (see [`World::apply`]), so a
//! failing one can be shrunk by trying to leave events out and keeping the removal whenever the
//! same failure is still found. Delta debugging first, to take big bites, then one event at a
//! time until nothing more can go. The step of the violation is expected to move; the property
//! and the detail are not.

use crate::event::Event;
use crate::invariants::Violation;
use crate::schedule::Schedule;
use crate::world::World;

/// Shrink a failing schedule
///
/// Returns the schedule with as few events as this can find that still reproduce the same
/// failure, with `expected` refreshed from its own replay. A schedule that does not fail is
/// returned unchanged.
///
/// # Arguments
///
/// * `schedule` - The schedule to shrink
pub fn minimize(schedule: &Schedule) -> Schedule {
    let Some(target) = World::replay(schedule).violation else {
        return schedule.clone();
    };
    let events = ddmin(schedule, &target);
    let events = greedy(schedule, &target, events);
    let mut out = Schedule {
        events,
        ..schedule.clone()
    };
    out.expected = World::replay(&out).violation;
    out
}

/// Whether a list of events still finds the target failure
fn still_fails(schedule: &Schedule, target: &Violation, events: &[Event]) -> bool {
    let candidate = Schedule {
        events: events.to_vec(),
        ..schedule.clone()
    };
    World::replay(&candidate)
        .violation
        .is_some_and(|found| found.same_failure(target))
}

/// Zeller's delta debugging over the event list
///
/// Split into `n` chunks and try leaving each one out; on success keep the shorter list and
/// coarsen, on failure refine until a chunk is one event.
///
/// # Arguments
///
/// * `schedule` - The schedule
/// * `target` - The failure to preserve
pub fn ddmin(schedule: &Schedule, target: &Violation) -> Vec<Event> {
    let mut events = schedule.events.clone();
    let mut n = 2;
    while events.len() >= 2 && n <= events.len() {
        let chunk = events.len().div_ceil(n);
        let mut reduced = false;
        for start in (0..events.len()).step_by(chunk) {
            // everything but this chunk
            let complement: Vec<Event> = events
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
///
/// # Arguments
///
/// * `schedule` - The schedule
/// * `target` - The failure to preserve
/// * `events` - The list to shrink
pub fn greedy(schedule: &Schedule, target: &Violation, mut events: Vec<Event>) -> Vec<Event> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Policy;
    use crate::schedule::{generate, ScheduleParams};

    /// A failing schedule shrinks, and the shrunk one still fails the same way
    #[test]
    fn a_padded_failure_shrinks_to_its_core() {
        let (name, policy, _) = Policy::unsafe_knobs()
            .into_iter()
            .find(|(name, _, _)| *name == "ack_on_receipt")
            .unwrap();
        let schedule = generate(name, 0, &ScheduleParams::default_small(), policy);
        let target = schedule.expected.clone().expect("the knob fails at once");
        let small = minimize(&schedule);
        assert!(small.events.len() < schedule.events.len());
        assert!(small.expected.unwrap().same_failure(&target));
    }

    /// A schedule that does not fail is left alone
    #[test]
    fn a_passing_schedule_is_unchanged() {
        let schedule = generate("safe", 0, &ScheduleParams::default_small(), Policy::safe());
        assert_eq!(minimize(&schedule), schedule);
    }
}
