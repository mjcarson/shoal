//! Named sets of benchmarks, so a capture can answer one question
//!
//! # Why a filter was not enough
//!
//! Filtering has always worked the way `cargo test` filters tests: a substring of an identifier
//! selects it. That is the right primitive and it is a bad way to describe an *intention*. Three
//! things go wrong once a capture is four to five hours long and the thing being asked is "measure
//! the storage settings":
//!
//! - **A prefix is not discoverable.** Nothing enumerates the useful ones, so knowing that
//!   `macro/conf/storage/` is a coherent set is knowledge held outside the tool.
//! - **A typo selects the wrong set silently.** `macro/conf/storag` matches nothing and errors, but
//!   `macro/conf` matches every configuration arm when only half was wanted, and a capture that
//!   measured twice what was asked for looks exactly like one that measured the right thing.
//! - **A set is rarely one prefix.** "Everything that isolates one path" is every macro workload
//!   except the grid and the configuration sweeps, which is not expressible as a substring at all.
//!
//! A group is a name for a set, declared once, printed by `list --groups`, and selected with
//! `--group`. It resolves to identifiers and nothing else.
//!
//! # A group selects; it never schedules
//!
//! **Nothing here makes anything run concurrently.** A capture runs one `shoal-workload` process at
//! a time, workload-outer and run-inner, and it did before groups existed and does after. Two
//! servers running at once would share a page cache, a device queue and a set of cores, and every
//! number either of them produced would be a number about the other one as well. The
//! port-per-workload scheme in `crate::run::plan` exists so that an arm binds the same port in every
//! capture, not so that two of them can bind at once.
//!
//! # Why this file cannot see the workloads
//!
//! Same reason [`crate::workload_ids`] cannot: the runner half of this crate builds with no engine
//! at all, and `list` has to work there. So a group is expressed over identifier *shapes* - prefixes
//! and exact names - rather than over anything a workload knows about itself. That is why the
//! configuration sweep puts its section into its identifier: `conf/storage` is a prefix check here
//! because the identifier was built to make it one.

use crate::registry::{BenchId, Layer};

/// How a group decides whether a benchmark belongs to it
///
/// Deliberately small. A group is a set of benchmarks a person would ask for by name, and every one
/// of those turns out to be expressible as "these layers", "these prefixes", or "these prefixes but
/// not those" - so the vocabulary stops there rather than growing into a query language nobody
/// wanted to write.
pub struct Members {
    /// The layers a member may come from, where an empty list accepts any layer
    pub layers: &'static [Layer],
    /// The identifier prefixes a member may carry, where an empty list accepts any identifier
    pub prefixes: &'static [&'static str],
    /// The identifier prefixes that disqualify a member, applied after everything above
    ///
    /// What makes `isolating` expressible: every macro workload whose identifier does not begin
    /// with the grid's prefix or the configuration sweep's.
    pub excluding: &'static [&'static str],
    /// Identifiers that are members whatever the rules above say
    ///
    /// Only `quick` uses this. A hand-picked set is a hand-picked set, and pretending it is a
    /// pattern would make it look like it generalises.
    pub exactly: &'static [&'static str],
}

impl Members {
    /// Whether a benchmark belongs to this set
    ///
    /// # Arguments
    ///
    /// * `entry` - The benchmark to test
    pub fn holds(&self, entry: &BenchId) -> bool {
        // a named identifier is a member regardless of what the patterns say
        if self.exactly.contains(&entry.id.as_str()) {
            return true;
        }
        // an explicit list of names and nothing else means exactly those names
        if !self.exactly.is_empty() && self.layers.is_empty() && self.prefixes.is_empty() {
            return false;
        }
        // then the layer restriction, where naming none accepts every layer
        if !self.layers.is_empty() && !self.layers.contains(&entry.layer) {
            return false;
        }
        // then the prefixes, where naming none accepts every identifier
        if !self.prefixes.is_empty()
            && !self
                .prefixes
                .iter()
                .any(|prefix| entry.id.starts_with(prefix))
        {
            return false;
        }
        // and finally the exclusions, which have the last word
        !self
            .excluding
            .iter()
            .any(|prefix| entry.id.starts_with(prefix))
    }
}

/// One named set of benchmarks
pub struct Group {
    /// What this group is called, which is what `--group` takes
    pub name: &'static str,
    /// The question a capture of this group answers
    ///
    /// Written as an answer rather than as a description of contents, because the reason to pick a
    /// group over another is the question, and `list --groups` is read when choosing.
    pub summary: &'static str,
    /// Which benchmarks belong to it
    pub members: Members,
}

/// Every declared group, in the order `list --groups` prints them
///
/// Ordered cheapest first, which is also roughly the order they should be reached for: prove
/// everything runs, then narrow to the question at hand, then take the wide ones.
pub const GROUPS: &[Group] = &[
    Group {
        name: "quick",
        summary: "does everything still run - one arm from each family, not a measurement",
        members: Members {
            layers: &[],
            prefixes: &[],
            excluding: &[],
            // one arm per family, hand-picked. these are checked against the registry by a test, so
            // a workload that is renamed out from under this list fails rather than silently
            // shrinking the set
            exactly: &[
                "macro/insert_unsorted",
                "macro/get_resident",
                "macro/get_archived",
                "macro/insert_ephemeral",
                "macro/get_ephemeral",
                "macro/fanout/resident/16",
                "macro/transport/send_one/small",
                "macro/encryption/depth/plain/4096/32",
                "macro/grid/unsorted/r50/1024",
                "macro/grid/depth/32",
                "macro/skew/zipfian/unsorted",
                "macro/conf/storage/durability/r50/fsync",
            ],
        },
    },
    Group {
        name: "micro",
        summary: "did the partition internals or the wire format move",
        members: Members {
            layers: &[Layer::Micro],
            prefixes: &[],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "attribution",
        summary: "where the time inside one query goes - never a latency or a throughput",
        members: Members {
            layers: &[Layer::Hotpath, Layer::Stages],
            prefixes: &[],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "controls",
        summary: "the storage-free pairs - what persistence costs, with everything else held",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &[
                "macro/insert_ephemeral",
                "macro/get_ephemeral",
                "macro/fanout/ephemeral/",
                "macro/insert_unsorted",
                "macro/get_resident",
                "macro/get_archived",
                "macro/fanout/resident/",
                "macro/fanout/evicted/",
            ],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "fanout",
        summary: "what reading many partitions at once costs, as a curve in the key count",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &["macro/fanout/"],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "transport",
        summary: "what the client's streaming mode and the wire's encryption cost",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &["macro/transport/", "macro/encryption/"],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "isolating",
        summary: "which path moved - every workload that drives one path and only one",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &[],
            // the grid and the configuration sweeps drive mixtures, so a difference in one of them
            // is never attributable to a path. that is exactly what this group excludes.
            excluding: &["macro/grid/", "macro/skew/", "macro/conf/"],
            exactly: &[],
        },
    },
    Group {
        name: "conf/storage",
        summary: "what each filesystem writer setting is worth",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &["macro/conf/storage/"],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "conf/resources",
        summary: "how the server scales with cores and memory, and what the frame bound costs",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &["macro/conf/resources/"],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "conf",
        summary: "what every configuration setting is worth - both halves of the sweep",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &["macro/conf/"],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "grid",
        summary: "what a caller's mixture costs, across row width, read share, skew and depth",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &["macro/grid/", "macro/skew/"],
            excluding: &[],
            exactly: &[],
        },
    },
    Group {
        name: "macro",
        summary: "every workload that runs against a live server",
        members: Members {
            layers: &[Layer::Macro],
            prefixes: &[],
            excluding: &[],
            exactly: &[],
        },
    },
];

/// The group with a name, if there is one
///
/// # Arguments
///
/// * `name` - The group name to look for
pub fn by_name(name: &str) -> Option<&'static Group> {
    // a linear scan over a dozen entries, which is not worth a map
    GROUPS.iter().find(|group| group.name == name)
}

/// Whether a benchmark belongs to any of the named groups
///
/// Several groups are combined with or, the same way several filters are: asking for `conf/storage`
/// and `fanout` means both, not the empty intersection of the two.
///
/// # Arguments
///
/// * `entry` - The benchmark to test
/// * `names` - The group names to test it against, where no names accepts everything
pub fn matches_groups(entry: &BenchId, names: &[String]) -> bool {
    // no group named means every benchmark, the way no filter means every benchmark
    if names.is_empty() {
        return true;
    }
    names
        .iter()
        .filter_map(|name| by_name(name))
        .any(|group| group.members.holds(entry))
}

/// Explains a group name that names no group, and suggests what was probably meant
///
/// # Arguments
///
/// * `names` - The group names the caller asked for
pub fn unknown(names: &[String]) -> Option<String> {
    // the first name that is not a group is the one worth complaining about
    let missing = names.iter().find(|name| by_name(name).is_none())?;
    // the declared names, so the correction is one line away rather than a documentation lookup
    let declared: Vec<&str> = GROUPS.iter().map(|group| group.name).collect();
    Some(format!(
        "unknown group '{missing}'\n  groups are: {}",
        declared.join(", ")
    ))
}

/// How long a whole capture of the macro layer takes, end to end
///
/// Measured rather than derived, and it has to be: a capture spends most of its time seeding,
/// building, and starting and stopping a server per arm, and **none of that is in any artifact**.
/// The measured phases of every macro workload put together come to a couple of minutes; the
/// capture they come from takes an hour. So the sum of the wall clocks cannot be presented as a
/// duration - it is off by two orders of magnitude - and what it is good for is a *share*. This
/// constant is what turns that share back into a number somebody can plan around.
///
/// **This was 4h30m and was never timed.** The `F20-conf` capture of 2026-08-22 is the first one
/// anybody put a clock on: two hundred and nine workloads at five runs took sixty minutes of macro
/// layer, inside seventy-five minutes end to end. Every projection this constant fed before that
/// was overstated by more than four times, which is the direction that makes somebody not run a
/// benchmark they had time for.
///
/// Update it when the observed cost of a full capture changes. `docs/src/performance/benchmarking.md`
/// is where the figure comes from.
const FULL_MACRO_CAPTURE_SECS: u128 = 3_600;

/// What one workload's measured runs last, taken from the newest capture that holds it
///
/// # Arguments
///
/// * `store` - The artifact tree to read the committed captures out of
fn measured_ns(store: &crate::store::Store) -> std::collections::BTreeMap<String, u128> {
    let mut measured = std::collections::BTreeMap::new();
    // oldest first, so a workload measured in several captures ends up carrying the newest of them
    let mut labels = store.labels().unwrap_or_default();
    labels.sort_by_key(|label| {
        store
            .read_meta(label)
            .ok()
            .flatten()
            .map(|meta| meta.captured)
            .unwrap_or_default()
    });
    for label in labels {
        let path = store.run_artifact(&label, Layer::Macro);
        // a label with no macro artifact contributes nothing rather than failing the listing
        let Ok(capture) = store.read_macro(&path) else {
            continue;
        };
        for (id, workload) in &capture.workloads {
            measured.insert(id.clone(), workload.median_wall_clock_ns());
        }
    }
    measured
}

/// Renders a nanosecond count as a duration somebody would say out loud
///
/// # Arguments
///
/// * `nanos` - The duration to render
///
/// # Examples
///
/// ```
/// use shoal_bench::groups::human_duration;
///
/// assert_eq!(human_duration(90 * 1_000_000_000), "1m30s");
/// assert_eq!(human_duration(7_200 * 1_000_000_000), "2h0m");
/// ```
pub fn human_duration(nanos: u128) -> String {
    let seconds = nanos / 1_000_000_000;
    // hours and minutes once it is long enough that seconds stop mattering, minutes below that
    if seconds >= 3_600 {
        format!("{}h{}m", seconds / 3_600, (seconds % 3_600) / 60)
    } else if seconds >= 60 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

/// Runs `shoal-bench list --groups`
///
/// # Arguments
///
/// * `store` - The artifact tree to estimate against
/// * `entries` - Every benchmark the registry holds
/// * `runs` - How many runs a capture would take of each workload
/// * `format` - How to print the table
pub fn run_groups(
    store: &crate::store::Store,
    entries: &[BenchId],
    runs: u32,
    format: crate::cli::Format,
) -> anyhow::Result<i32> {
    let measured = measured_ns(store);
    // the denominator is every macro workload that has ever been captured, so a group's share is a
    // share of a whole capture rather than of the groups that happen to be declared
    let whole: u128 = entries
        .iter()
        .filter(|entry| entry.layer == Layer::Macro)
        .filter_map(|entry| measured.get(&entry.id).copied())
        .sum();
    // one row per declared group, in declaration order, which runs cheapest first
    let rows: Vec<GroupReport> = GROUPS
        .iter()
        .map(|group| {
            let picked: Vec<&BenchId> = entries
                .iter()
                .filter(|entry| group.members.holds(entry))
                .collect();
            // only the macro layer has a recorded duration; criterion never wrote one, so a group
            // of micro benchmarks reports no estimate rather than an estimate of nothing
            let timed = picked
                .iter()
                .filter(|entry| measured.contains_key(&entry.id))
                .count();
            let measured_ns: u128 = picked
                .iter()
                .filter_map(|entry| measured.get(&entry.id).copied())
                .sum();
            // a group's share of a whole capture's measured time, projected onto what a whole
            // capture actually costs
            let share = if whole == 0 {
                None
            } else {
                Some(measured_ns as f64 / whole as f64)
            };
            GroupReport {
                name: group.name,
                summary: group.summary,
                benches: picked.len(),
                timed,
                measured_ns,
                share,
                projected_secs: share
                    .filter(|_| timed > 0)
                    .map(|share| (share * FULL_MACRO_CAPTURE_SECS as f64) as u128),
            }
        })
        .collect();
    match format {
        crate::cli::Format::Json => {
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        crate::cli::Format::Text | crate::cli::Format::Markdown => {
            println!(
                "{:<16} {:>8} {:>7} {:>9}  {}",
                "GROUP", "BENCHES", "SHARE", "~CAPTURE", "WHAT IT ANSWERS"
            );
            for row in &rows {
                // a group none of whose members has ever been captured cannot be estimated, and a
                // zero would read as free rather than as never measured
                let (share, cost) = match (row.share, row.projected_secs) {
                    (Some(share), Some(secs)) if row.timed > 0 => (
                        format!("{:.0}%", share * 100.0),
                        human_duration(secs * 1_000_000_000),
                    ),
                    _ => ("-".to_string(), "-".to_string()),
                };
                println!(
                    "{:<16} {:>8} {:>7} {:>9}  {}",
                    row.name, row.benches, share, cost, row.summary
                );
            }
            eprintln!(
                "\nShare is of a whole macro capture's measured time, from the newest capture \
                 holding each workload.\n~capture projects that share onto the {} a full capture \
                 takes, since seeding and server startup dominate and no artifact records them.\nA \
                 group whose workloads have never been captured shows `-` rather than a guess; \
                 --runs {runs} is assumed throughout.",
                human_duration(FULL_MACRO_CAPTURE_SECS * 1_000_000_000)
            );
        }
    }
    Ok(0)
}

/// One row of the group listing
#[derive(serde::Serialize)]
pub struct GroupReport {
    /// What the group is called
    pub name: &'static str,
    /// The question a capture of it answers
    pub summary: &'static str,
    /// How many benchmarks it selects
    pub benches: usize,
    /// How many of those have ever been captured, and so contribute to the estimate
    pub timed: usize,
    /// What its captured members measured, in nanoseconds
    pub measured_ns: u128,
    /// Its share of a whole macro capture's measured time
    pub share: Option<f64>,
    /// That share projected onto what a whole capture costs, in seconds
    pub projected_secs: Option<u128>,
}

#[cfg(test)]
mod tests {
    use super::{GROUPS, by_name, human_duration, matches_groups, unknown};
    use crate::registry::{BenchId, Layer, PROFILED_WORKLOADS, instrumented_id};

    /// Every benchmark there is, as the registry would hold it
    ///
    /// Built from the declared lists rather than from criterion, so this runs with no engine and
    /// without a build. The micro entries are stand-ins: a group that selects on the micro layer
    /// selects on the layer and never on the shape of a criterion id.
    fn every_bench() -> Vec<BenchId> {
        let mut entries = vec![
            BenchId::new(Layer::Micro, "partition_sorted/get_key/16"),
            BenchId::new(Layer::Micro, "seek_bytes/new/one_key"),
        ];
        for id in crate::workload_ids::IDS {
            entries.push(BenchId::new(Layer::Macro, *id));
        }
        for layer in [Layer::Hotpath, Layer::Stages] {
            for id in PROFILED_WORKLOADS {
                entries.push(BenchId::new(layer, instrumented_id(layer, id)));
            }
        }
        entries
    }

    /// No two groups share a name, since `--group` resolves by name
    #[test]
    fn every_group_name_is_unique() {
        let mut names: Vec<&str> = GROUPS.iter().map(|group| group.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "a group name is declared twice");
    }

    /// Every group says what a capture of it answers, since `list --groups` prints it
    #[test]
    fn every_group_says_what_it_answers() {
        for group in GROUPS {
            assert!(
                group.summary.len() > 20,
                "{} has no useful summary",
                group.name
            );
        }
    }

    /// Every group selects something
    ///
    /// A group that matches nothing is worse than no group: `--group` would report an empty
    /// selection as a mistake by the caller, when the mistake is in this file.
    #[test]
    fn every_group_selects_something() {
        let all = every_bench();
        for group in GROUPS {
            let picked = all.iter().filter(|entry| group.members.holds(entry)).count();
            assert!(picked > 0, "the {} group selects nothing", group.name);
        }
    }

    /// Every hand-picked member of `quick` is a benchmark that exists
    ///
    /// The list names identifiers directly, so a workload renamed out from under it would leave
    /// `quick` quietly smaller rather than failing.
    #[test]
    fn the_quick_group_names_only_real_benchmarks() {
        let all = every_bench();
        let quick = by_name("quick").expect("the quick group");
        for id in quick.members.exactly {
            assert!(
                all.iter().any(|entry| entry.id == *id),
                "{id} is in the quick group and is not a benchmark"
            );
        }
    }

    /// The two halves of the configuration sweep partition it exactly
    ///
    /// Every configuration arm is in one of them and no arm is in both, so a capture of
    /// `conf/storage` and one of `conf/resources` together is a capture of `conf`.
    #[test]
    fn the_conf_halves_partition_the_sweep() {
        let all = every_bench();
        let storage = by_name("conf/storage").expect("conf/storage");
        let resources = by_name("conf/resources").expect("conf/resources");
        let whole = by_name("conf").expect("conf");
        let mut halves = 0;
        for entry in all.iter().filter(|entry| whole.members.holds(entry)) {
            let in_storage = storage.members.holds(entry);
            let in_resources = resources.members.holds(entry);
            assert!(
                in_storage ^ in_resources,
                "{} is in neither half or in both",
                entry.id
            );
            halves += 1;
        }
        assert_eq!(halves, 48, "the configuration sweep changed size");
    }

    /// The isolating group holds no mixture, since a mixture attributes nothing
    #[test]
    fn the_isolating_group_excludes_every_mixture() {
        let all = every_bench();
        let isolating = by_name("isolating").expect("the isolating group");
        for entry in all.iter().filter(|entry| isolating.members.holds(entry)) {
            assert!(
                !entry.id.starts_with("macro/grid/")
                    && !entry.id.starts_with("macro/skew/")
                    && !entry.id.starts_with("macro/conf/"),
                "{} drives a mixture and is in the isolating group",
                entry.id
            );
        }
        // and it is not empty of the things it should hold
        assert!(
            isolating
                .members
                .holds(&BenchId::new(Layer::Macro, "macro/insert_unsorted"))
        );
    }

    /// A group restricted to a layer holds nothing from another layer
    #[test]
    fn a_layer_bound_group_holds_one_layer() {
        let all = every_bench();
        let micro = by_name("micro").expect("the micro group");
        for entry in all.iter().filter(|entry| micro.members.holds(entry)) {
            assert_eq!(entry.layer, Layer::Micro, "{}", entry.id);
        }
    }

    /// Several groups are combined with or, not and
    #[test]
    fn groups_are_combined_with_or() {
        let entry = BenchId::new(Layer::Macro, "macro/fanout/resident/16");
        let names = vec!["conf/storage".to_string(), "fanout".to_string()];
        assert!(matches_groups(&entry, &names));
        // and one of them alone does not hold it
        assert!(!matches_groups(&entry, &["conf/storage".to_string()]));
    }

    /// No group named accepts everything, the way no filter does
    #[test]
    fn no_group_accepts_everything() {
        let entry = BenchId::new(Layer::Macro, "macro/insert_unsorted");
        assert!(matches_groups(&entry, &[]));
    }

    /// A duration reads the way somebody would say it, at every magnitude the listing shows
    #[test]
    fn a_duration_reads_out_loud() {
        assert_eq!(human_duration(45 * 1_000_000_000), "45s");
        assert_eq!(human_duration(90 * 1_000_000_000), "1m30s");
        assert_eq!(human_duration(7_200 * 1_000_000_000), "2h0m");
        assert_eq!(human_duration(10_245 * 1_000_000_000), "2h50m");
    }

    /// A name that is not a group is reported, with the declared names to choose from
    #[test]
    fn an_unknown_group_is_reported() {
        let message = unknown(&["conf/storag".to_string()]).expect("an unknown group is reported");
        assert!(message.contains("unknown group 'conf/storag'"), "{message}");
        assert!(message.contains("conf/storage"), "{message}");
        // and a declared name is not reported
        assert!(unknown(&["conf/storage".to_string()]).is_none());
    }
}
