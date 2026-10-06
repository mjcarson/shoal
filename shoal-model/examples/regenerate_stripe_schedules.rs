//! Regenerates the saved stripe schedules under `shoal-model/schedules/stripe/`
//!
//! S7's sixteen schedules and the schedules built by hand for X1's findings are written from
//! their builders, each under its setting. The rules as the pages wrote them that the search found
//! unsafe are written from the first generated seed that breaks them, minimized. Run by hand after
//! a change to the model; the tests only ever load what this wrote, build the hand-built ones
//! again, and fail if a file no longer replays to what it records.
//!
//! ```text
//! cargo run -p shoal-model --release --example regenerate_stripe_schedules
//! ```

use shoal_model::stripe::minimize::minimize;
use shoal_model::stripe::scenarios::{findings, policy_named, s7};
use shoal_model::stripe::{generate, Layout, StripeParams, StripeSchedule};

/// How many seeds to try before giving up on a rule
const SEEDS: u64 = 4000;

/// The rules as written the generated search breaks, and the layout each is saved at
const SEARCHED: [(&str, Layout); 7] = [
    ("hidden_units_left_under_a_write", Layout::TwoPlusOne),
    ("reclaimed_row_deleted", Layout::Replicated3),
    ("reader_hides_by_the_entry_it_read", Layout::Replicated3),
    ("default_read_takes_the_row_at_one", Layout::Replicated3),
    ("tag_from_the_identity_alone", Layout::Replicated3),
    ("truncate_unfenced", Layout::Replicated3),
    ("write_discarded_beneath_a_later_one", Layout::Replicated3),
];

fn main() {
    let dir = StripeSchedule::dir();
    std::fs::create_dir_all(&dir).expect("the stripe schedules directory");
    // the schedules built by hand, each under the setting it is saved under
    for (_, setting, build) in s7().into_iter().chain(findings()) {
        let schedule = build(policy_named(setting));
        let found = schedule
            .expected
            .as_ref()
            .map(|violation| violation.detail.clone())
            .or_else(|| {
                schedule
                    .stalled
                    .as_ref()
                    .map(|stalled| stalled.detail.clone())
            })
            .unwrap_or_else(|| panic!("{} finds nothing under {setting}", schedule.name));
        println!(
            "{}: {} events, {found}",
            schedule.name,
            schedule.events.len()
        );
        schedule.save(&dir.join(format!("{}.json", schedule.name)));
    }
    // the rules the search breaks, from the first seed that does, minimized
    for (setting, layout) in SEARCHED {
        let policy = policy_named(setting);
        let params = StripeParams::default_small(layout);
        let found = (0..SEEDS)
            .map(|seed| generate(setting, seed, &params, policy))
            .find(|schedule| schedule.expected.is_some());
        let Some(schedule) = found else {
            panic!("no seed below {SEEDS} broke anything under {setting}");
        };
        let before = schedule.events.len();
        let small = minimize(&schedule);
        let expected = small
            .expected
            .as_ref()
            .expect("a minimized failure still fails");
        println!(
            "{setting}: seed {} found {} at {before} events, minimized to {}",
            schedule.seed,
            expected.detail,
            small.events.len()
        );
        small.save(&dir.join(format!("{setting}.json")));
    }
}
