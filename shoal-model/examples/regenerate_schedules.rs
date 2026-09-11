//! Regenerates the saved schedules under `shoal-model/schedules/`
//!
//! One file per unsafe knob, generated from the first seed that fails and minimized, plus the
//! B=100/C=101/A+B=102 schedule written by hand. Run by hand after a change to the model; the
//! tests only ever load what this wrote, and `saved_protocol_schedule_reproduces_failure` fails
//! if a file no longer replays to the violation it records.
//!
//! ```text
//! cargo run -p shoal-model --example regenerate_schedules
//! ```

use shoal_model::ids::NodeId;
use shoal_model::minimize::minimize;
use shoal_model::schedule::{generate, stale_report_schedule, Schedule, ScheduleParams};
use shoal_model::{Policy, World};

/// How many seeds to try before giving up on a knob
const SEEDS: u64 = 256;

fn main() {
    let dir = Schedule::dir();
    std::fs::create_dir_all(&dir).expect("the schedules directory");
    // one generated, minimized failure per unsafe knob
    for (name, policy, property) in Policy::unsafe_knobs() {
        let mut params = ScheduleParams::default_small();
        // the async knob needs a disk that acknowledges early to have anything to count
        if name == "async_receipt_counts_as_durable" {
            params.async_nodes = vec![NodeId(3)];
        }
        let found = (0..SEEDS)
            .map(|seed| generate(name, seed, &params, policy))
            .find(|schedule| schedule.expected.is_some());
        let Some(schedule) = found else {
            panic!("no seed below {SEEDS} violated {property} under {name}");
        };
        let before = schedule.events.len();
        let small = minimize(&schedule);
        let expected = small.expected.as_ref().expect("a minimized failure still fails");
        assert_eq!(expected.property, property, "{name} violated the wrong property");
        println!(
            "{name}: seed {} failed {} at {} events, minimized to {}",
            schedule.seed, expected.property, before, small.events.len()
        );
        small.save(&dir.join(format!("{name}.json")));
    }
    // the stale-report schedule under the unsafe election, at a prefix short enough to read.
    // the literal 100/101/102 is half a megabyte of JSON, so the test builds that one from the
    // same builder rather than loading it
    let policy = Policy::unsafe_knobs()
        .into_iter()
        .find(|(name, _, _)| *name == "election_by_heartbeat_max_report")
        .map(|(_, policy, _)| policy)
        .expect("the election knob");
    let schedule = stale_report_schedule(3, policy);
    let expected = schedule.expected.as_ref().expect("the stale report loses a write");
    println!("{}: {} at {} events", schedule.name, expected, schedule.events.len());
    schedule.save(&dir.join(format!("{}.json", schedule.name)));
}
