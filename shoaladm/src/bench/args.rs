//! `shoaladm bench`'s command line, and how it is folded into a spec
//!
//! Every field of a [`BenchSpec`] can be set by a flag, and a flag beats the spec file. The
//! flags that are not the spec's - where the cluster is, where results go, what the bench may
//! do to the hosts - are the run's own.

use clap::{Args, Subcommand};
use color_eyre::eyre::eyre;
use shoal_loadgen::feed::Preload;
use shoal_loadgen::keys::KeyDistribution;
use shoal_loadgen::spec::{BenchSpec, EventKind, Mode, OnExhaust, ReadLevel, Reads, Workload};
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::cli::InventoryArg;

/// The bench's commands
#[derive(Subcommand, Debug)]
pub enum BenchCommand {
    /// Benchmark the project's schema against a dataset folder, on a cluster of the bench's own
    /// beside the inventory's, or on the inventory's own with --attach
    Run(Box<BenchRunArgs>),
    /// List the captures under the project's results
    List(RootArg),
    /// Print one capture's arms
    Show {
        /// The capture: a label under the results, or a path
        capture: String,
        /// Where the captures are
        #[clap(flatten)]
        root: RootArg,
    },
    /// Compare a capture against a baseline, refusing ones that differ in anything that moves
    /// their numbers unless that difference is waived by name
    Compare {
        /// The capture compared against: a label under the results, or a path
        baseline: String,
        /// The capture compared: a label under the results, or a path
        candidate: String,
        /// A fact allowed to differ, by the name the refusal gives it
        #[clap(long)]
        allow: Vec<String>,
        /// Where the captures are
        #[clap(flatten)]
        root: RootArg,
    },
}

impl BenchCommand {
    /// Whether this command connects to a cluster, and so is the schema's program's to run
    #[must_use]
    pub fn needs_schema(&self) -> bool {
        matches!(self, BenchCommand::Run(_))
    }

    /// The inventory the command names, if it does
    #[must_use]
    pub fn inventory(&self) -> Option<PathBuf> {
        match self {
            BenchCommand::Run(args) => args.inventory.inventory.clone(),
            _ => None,
        }
    }
}

/// Where captures are kept, if not under the project's target directory
#[derive(Args, Debug, Clone, Default)]
pub struct RootArg {
    /// The directory holding the captures; the project's `target/shoaladm-bench` if not given
    #[clap(long)]
    pub root: Option<PathBuf>,
}

/// A list given as one comma separated flag
///
/// A newtype rather than a `Vec`, because clap reads a `Vec` field as a repeated flag.
#[derive(Debug, Clone, PartialEq)]
pub struct List<T>(pub Vec<T>);

/// Parse a comma separated list
///
/// # Arguments
///
/// * `raw` - The list as written
fn list<T: std::str::FromStr>(raw: &str) -> Result<List<T>, String>
where
    T::Err: std::fmt::Display,
{
    // each entry on its own, so a bad one is named
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.parse::<T>().map_err(|error| format!("{entry:?}: {error}")))
        .collect::<Result<Vec<T>, String>>()
        .map(List)
}

/// Parse a list of workloads; `read:N,insert:M` is one workload, so workloads are separated by
/// `;` or spaces when a custom one is among them, and by `,` otherwise
///
/// # Arguments
///
/// * `raw` - The list as written
fn workloads(raw: &str) -> Result<List<Workload>, String> {
    // a custom workload holds commas of its own
    if raw.contains(':') {
        return raw
            .split([';', ' '])
            .filter(|entry| !entry.trim().is_empty())
            .map(str::parse)
            .collect::<Result<Vec<Workload>, String>>()
            .map(List);
    }
    list(raw)
}

/// Parse table weights, `Movie:3,Review:1`
///
/// # Arguments
///
/// * `raw` - The weights as written
pub(crate) fn weights(raw: &str) -> Result<BTreeMap<String, u32>, String> {
    // each table and its weight
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (table, weight) = entry
                .split_once(':')
                .ok_or_else(|| format!("{entry:?} is not Table:weight"))?;
            let weight = weight
                .trim()
                .parse()
                .map_err(|error| format!("{entry:?}: {error}"))?;
            Ok((table.trim().to_string(), weight))
        })
        .collect()
}

/// Parse a key distribution by name
///
/// # Arguments
///
/// * `raw` - The name
fn distribution(raw: &str) -> Result<KeyDistribution, String> {
    serde_yaml::from_str(raw).map_err(|_| format!("{raw:?} is not uniform, zipfian or latest"))
}

/// Parse a read level by name
///
/// # Arguments
///
/// * `raw` - The name
fn read_level(raw: &str) -> Result<ReadLevel, String> {
    serde_yaml::from_str(raw).map_err(|_| format!("{raw:?} is not default, one or quorum"))
}

/// Parse whether reads are warm or cold
///
/// # Arguments
///
/// * `raw` - The name
fn reads(raw: &str) -> Result<Reads, String> {
    serde_yaml::from_str(raw).map_err(|_| format!("{raw:?} is not warm or cold"))
}

/// Parse what an arm does when its inserts run out
///
/// # Arguments
///
/// * `raw` - The name
fn on_exhaust(raw: &str) -> Result<OnExhaust, String> {
    serde_yaml::from_str(raw).map_err(|_| format!("{raw:?} is not end or wrap"))
}

/// Everything `bench run` takes
#[derive(Args, Debug, Clone, Default)]
pub struct BenchRunArgs {
    /// The inventory of the cluster: copied for the bench's own cluster, or driven with --attach
    #[clap(flatten)]
    pub inventory: InventoryArg,
    /// A spec file whose fields the flags below override
    #[clap(long)]
    pub spec: Option<PathBuf>,
    /// The dataset folder: one `<Table>.csv`, `.json` or `.jsonl` a table
    #[clap(long)]
    pub dataset: Option<PathBuf>,
    /// Drive the inventory's own cluster as it is, rather than a cluster of the bench's own
    #[clap(long)]
    pub attach: bool,
    /// Drive one node started by hand at this client address, with no inventory; implies --attach
    #[clap(long)]
    pub addr: Option<String>,
    /// The workloads, each a share of reads and inserts: read100 (reads only), insert100 (inserts
    /// only), rw50 (half each), read90 (nine reads to an insert), or read:N,insert:M (separate
    /// those with ';'). Leave it out on a terminal to choose the whole run in a wizard that
    /// explains every choice
    #[clap(long, alias = "mixes", value_parser = workloads)]
    pub workloads: Option<List<Workload>>,
    /// The bundle sizes, comma separated
    #[clap(long, value_parser = list::<usize>)]
    pub bundles: Option<List<usize>>,
    /// The events, comma separated: none, kill, stop, rebalance, decommission, remove, repair, backup
    #[clap(long, value_parser = list::<EventKind>)]
    pub events: Option<List<EventKind>>,
    /// How many streams drive the cluster, spread over its members
    #[clap(long)]
    pub workers: Option<usize>,
    /// How many queries a stream keeps outstanding; four bundles if not given
    #[clap(long)]
    pub in_flight: Option<usize>,
    /// How long each arm is measured, in seconds
    #[clap(long)]
    pub duration: Option<u64>,
    /// How long each arm runs before it is measured, in seconds
    #[clap(long)]
    pub warmup: Option<u64>,
    /// How many times each arm is run; two at least for compare to judge it
    #[clap(long)]
    pub runs: Option<u32>,
    /// The seed every choice is drawn from
    #[clap(long)]
    pub seed: Option<u64>,
    /// Which read keys are asked for most: uniform, zipfian or latest
    #[clap(long, value_parser = distribution)]
    pub distribution: Option<KeyDistribution>,
    /// How many keys one read asks for, as one get
    #[clap(long)]
    pub read_keys: Option<usize>,
    /// The level reads are served at: default, one or quorum
    #[clap(long, value_parser = read_level)]
    pub read_level: Option<ReadLevel>,
    /// Whether reads start warm, or cold after every node is restarted
    #[clap(long, value_parser = reads)]
    pub reads: Option<Reads>,
    /// How much of each file is loaded before anything is measured: rows, or a percent
    #[clap(long)]
    pub preload: Option<Preload>,
    /// With --attach, the dataset's preload is already in the cluster: load nothing, check a sample
    #[clap(long)]
    pub preloaded: bool,
    /// Skip an insert whose key an earlier row had, so every insert is a new row
    #[clap(long)]
    pub dedupe: bool,
    /// How many times an arm's query that failed in a retriable way is sent again; none by
    /// default, so every failure is counted where it happened
    #[clap(long)]
    pub retries: Option<u32>,
    /// When an arm's inserts run out: end it there, or wrap and insert them again
    #[clap(long, value_parser = on_exhaust)]
    pub on_exhaust: Option<OnExhaust>,
    /// The share of a file's rows that may fail to parse, in percent
    #[clap(long)]
    pub max_parse_errors: Option<f64>,
    /// Each table's weight, `Movie:3,Review:1`; each table's pool if not given
    #[clap(long, value_parser = weights)]
    pub tables: Option<BTreeMap<String, u32>>,
    /// How far into an event arm's measured time its event starts, in percent
    #[clap(long)]
    pub event_at: Option<f64>,
    /// How far into a kill or stop arm the node is started again, in percent
    #[clap(long)]
    pub restart_at: Option<f64>,
    /// The longest an event arm waits past its time for its event, in seconds
    #[clap(long)]
    pub event_timeout: Option<u64>,
    /// The node an event acts on: an inventory name, `leader`, or `last`
    #[clap(long)]
    pub victim: Option<String>,
    /// The node a rebalance or decommission moves onto, outside the inventory's bootstrap set
    #[clap(long)]
    pub spare: Option<String>,
    /// The table a repair or backup acts on
    #[clap(long)]
    pub event_table: Option<String>,
    /// Do not read back every acknowledged insert after its arm
    #[clap(long)]
    pub no_verify_acks: bool,
    /// The capture's name; the time and commit if not given
    #[clap(long)]
    pub label: Option<String>,
    /// Where the capture goes; under the project's `target/shoaladm-bench` if not given
    #[clap(long)]
    pub out: Option<PathBuf>,
    /// Replace a capture of the same label
    #[clap(long)]
    pub overwrite: bool,
    /// Measure a project or shoal with uncommitted changes, recording that it was
    #[clap(long)]
    pub allow_dirty: bool,
    /// A unit to stop for the run and start again after, such as shoal-tmdb; repeatable
    #[clap(long)]
    pub stop_unit: Vec<String>,
    /// Run beside other shoal units on the hosts rather than refusing, recording that it did
    #[clap(long)]
    pub allow_neighbours: bool,
    /// Set every node's cpu governor for the run and restore what each had, such as performance
    #[clap(long)]
    pub governor: Option<String>,
    /// How far the bench's cluster's ports are moved from the inventory's
    #[clap(long, default_value_t = 100)]
    pub port_offset: u16,
    /// One directory on every host for the bench's cluster's storage, rather than each root with `-bench`
    #[clap(long)]
    pub bench_storage: Option<String>,
    /// Leave the bench's cluster running at the end rather than destroying it
    #[clap(long)]
    pub keep_cluster: bool,
    /// Build the node from the project even when the inventory names a built `server:`
    #[clap(long)]
    pub from_project: bool,
    /// Deploy a profiling build of the node: frame pointers and jemalloc heap profiles, collected after each arm
    #[clap(long)]
    pub profile: bool,
    /// With --attach, allow inserts into the attached cluster
    #[clap(long)]
    pub yes_write: bool,
    /// With --attach, allow a repair or backup of the attached cluster
    #[clap(long)]
    pub yes_events: bool,
    /// Print a line a second rather than drawing the full screen view
    #[clap(long)]
    pub basic: bool,
    /// Print the plan and every refusal, and touch nothing
    #[clap(long)]
    pub dry_run: bool,
}

impl BenchRunArgs {
    /// The spec this run asks for: the file's, with every flag given laid over it
    ///
    /// # Errors
    ///
    /// When the spec file cannot be read or does not parse.
    pub fn spec(&self) -> color_eyre::Result<BenchSpec> {
        // the file, or every default
        let mut spec = match &self.spec {
            Some(path) => {
                let text = std::fs::read_to_string(path)
                    .map_err(|error| eyre!("{}: {error}", path.display()))?;
                let mut spec: BenchSpec = serde_yaml::from_str(&text)
                    .map_err(|error| eyre!("{}: {error}", path.display()))?;
                // a dataset named in the file is relative to the file
                if spec.dataset.is_relative() && !spec.dataset.as_os_str().is_empty() {
                    if let Some(parent) = path.parent() {
                        spec.dataset = parent.join(&spec.dataset);
                    }
                }
                spec
            }
            None => BenchSpec::default(),
        };
        // every flag that was given
        if let Some(dataset) = &self.dataset {
            spec.dataset.clone_from(dataset);
        }
        if self.attach || self.addr.is_some() {
            spec.mode = Mode::Attach;
        }
        macro_rules! take {
            ($($field:ident),*) => {
                $(if let Some(value) = &self.$field { spec.$field = value.clone(); })*
            };
        }
        // the lists, out of their wrappers
        if let Some(List(workloads)) = &self.workloads {
            spec.workloads.clone_from(workloads);
        }
        if let Some(List(bundles)) = &self.bundles {
            spec.bundles.clone_from(bundles);
        }
        if let Some(List(events)) = &self.events {
            spec.events.clone_from(events);
        }
        take!(
            workers, duration, warmup, runs, seed, distribution, retries,
            read_keys, read_level, reads, preload, on_exhaust, max_parse_errors, tables,
            event_at, restart_at, event_timeout
        );
        if self.in_flight.is_some() {
            spec.in_flight = self.in_flight;
        }
        if self.victim.is_some() {
            spec.victim.clone_from(&self.victim);
        }
        if self.spare.is_some() {
            spec.spare.clone_from(&self.spare);
        }
        if self.event_table.is_some() {
            spec.event_table.clone_from(&self.event_table);
        }
        spec.preloaded |= self.preloaded;
        spec.dedupe |= self.dedupe;
        if self.no_verify_acks {
            spec.verify_acks = false;
        }
        Ok(spec)
    }

    /// Whether the run names its workloads, by flag or in its spec file
    ///
    /// A run that names none runs the defaults, or on a terminal opens the wizard
    /// ([F67](../../../docs/src/features/bench-run-wizard.md)). A spec file written before F67
    /// names them `mixes`, which counts too.
    ///
    /// # Errors
    ///
    /// When the spec file cannot be read or is not YAML.
    pub fn workloads_chosen(&self) -> color_eyre::Result<bool> {
        // the flag names them outright
        if self.workloads.is_some() {
            return Ok(true);
        }
        // a spec file names them if it has the key, whatever it lists
        let Some(path) = &self.spec else {
            return Ok(false);
        };
        let text = std::fs::read_to_string(path).map_err(|error| eyre!("{}: {error}", path.display()))?;
        let value: serde_yaml::Value =
            serde_yaml::from_str(&text).map_err(|error| eyre!("{}: {error}", path.display()))?;
        Ok(value.get("workloads").is_some() || value.get("mixes").is_some())
    }

    /// Every problem with what this run may do, beside the spec's own
    ///
    /// # Arguments
    ///
    /// * `spec` - The spec it asks for
    #[must_use]
    pub fn problems(&self, spec: &BenchSpec) -> Vec<String> {
        // the spec's own first
        let mut problems = spec.problems();
        let attach = spec.mode == Mode::Attach;
        // what an attached cluster allows only when asked in words
        if attach {
            // the run writes when a workload inserts, and when it loads the preload itself:
            // an attached run that is not --preloaded loads it before its first arm, whatever
            // order its workloads are in, so that is all a read needs (item 201)
            if !self.yes_write {
                // a kind a schema supplies is taken to write, since only the schema knows (F69)
                if spec.workloads.iter().any(Workload::writes) {
                    problems.push(
                        "a workload inserts into the attached cluster; pass --yes-write to allow it, knowing \
                         a row it already holds is overwritten"
                            .to_string(),
                    );
                } else if !spec.preloaded {
                    problems.push(
                        "the run loads the preload into the attached cluster before its first arm, which \
                         writes; pass --yes-write to allow it, or --preloaded if the preload is already in it"
                            .to_string(),
                    );
                }
            }
            if spec.events.iter().any(|event| *event != EventKind::None) && !self.yes_events {
                problems.push("an event acts on the attached cluster; pass --yes-events to allow it".to_string());
            }
            for (flag, given) in [
                ("--governor", self.governor.is_some()),
                ("--stop-unit", !self.stop_unit.is_empty()),
                ("--profile", self.profile),
                ("--keep-cluster", self.keep_cluster),
                ("--bench-storage", self.bench_storage.is_some()),
            ] {
                if given {
                    problems.push(format!("{flag} is for the bench's own cluster, not an attached one"));
                }
            }
        } else if self.preloaded {
            problems.push("--preloaded is for an attached cluster; the bench's own is preloaded by it".to_string());
        }
        if self.addr.is_some() && spec.events.iter().any(|event| *event != EventKind::None) {
            problems.push("--addr drives one node with no inventory, so no event can be run".to_string());
        }
        problems
    }
}

/// Refuse a run whose spec or flags have problems, naming every one
///
/// # Arguments
///
/// * `problems` - What is wrong
///
/// # Errors
///
/// When there is anything wrong.
pub fn refuse(problems: &[String]) -> color_eyre::Result<()> {
    // every problem at once, so they are fixed together
    if problems.is_empty() {
        return Ok(());
    }
    let lines: Vec<String> = problems.iter().map(|problem| format!("  - {problem}")).collect();
    Err(eyre!("the bench cannot run:\n{}", lines.join("\n")))
}

#[cfg(test)]
mod tests {
    use super::BenchCommand;
    use crate::cli::{Cli, Command};
    use clap::Parser;
    use shoal_loadgen::spec::Mode;

    /// Parse a bench command line
    ///
    /// # Arguments
    ///
    /// * `args` - The arguments after `bench`
    fn parse(args: &[&str]) -> BenchCommand {
        let line = std::iter::once("shoaladm").chain(std::iter::once("bench")).chain(args.iter().copied());
        match Cli::try_parse_from(line).expect("the line parses").command {
            Command::Bench(command) => command,
            other => panic!("not a bench: {other:?}"),
        }
    }

    /// Workloads are chosen by the flag, or by a spec file that names them under either name
    #[test]
    fn workloads_are_chosen_by_flag_or_spec() {
        let dir = tempfile::tempdir().unwrap();
        let run = |args: &[&str]| match parse(args) {
            BenchCommand::Run(args) => args.workloads_chosen().unwrap(),
            other => panic!("not a run: {other:?}"),
        };
        assert!(!run(&["run", "--dataset", "d"]));
        assert!(run(&["run", "--dataset", "d", "--workloads", "rw50"]));
        for (name, text, chosen) in [
            ("new.yml", "dataset: d\nworkloads: [rw50]\n", true),
            ("old.yml", "dataset: d\nmixes: [rw50]\n", true),
            ("none.yml", "dataset: d\nruns: 2\n", false),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, text).unwrap();
            assert_eq!(run(&["run", "--spec", &path.display().to_string()]), chosen, "{name}");
        }
    }

    /// Flags lay over the spec file, and a run is the only bench command that needs the schema
    #[test]
    fn flags_override_the_spec_and_only_run_needs_the_schema() {
        let dir = tempfile::tempdir().unwrap();
        let spec = dir.path().join("bench.yml");
        std::fs::write(&spec, "dataset: data\nbundles: [4]\nruns: 5\n").unwrap();
        let spec_arg = spec.display().to_string();
        let command = parse(&["run", "--spec", &spec_arg, "--runs", "2", "--workloads", "read100,rw50", "--attach"]);
        assert!(command.needs_schema());
        let BenchCommand::Run(args) = command else {
            panic!("not a run");
        };
        let spec = args.spec().unwrap();
        // the file's dataset is relative to the file, its bundles kept, its runs overridden
        assert_eq!(spec.dataset, dir.path().join("data"));
        assert_eq!(spec.bundles, vec![4]);
        assert_eq!(spec.runs, 2);
        assert_eq!(spec.workloads.len(), 2);
        assert_eq!(spec.mode, Mode::Attach);
        // a custom workload is separated by a semicolon
        let BenchCommand::Run(args) = parse(&["run", "--dataset", "d", "--workloads", "read:3,insert:1;read100"]) else {
            panic!("not a run");
        };
        assert_eq!(args.spec().unwrap().workloads.len(), 2);
        assert!(!parse(&["list"]).needs_schema());
        assert!(!parse(&["compare", "a", "b", "--allow", "dataset"]).needs_schema());
    }

    /// An attached cluster refuses writes, events and host changes unless asked in words
    #[test]
    fn an_attached_run_refuses_what_it_was_not_allowed() {
        let BenchCommand::Run(args) = parse(&[
            "run", "--attach", "--dataset", "d", "--workloads", "read100,insert100", "--governor", "performance",
        ]) else {
            panic!("not a run");
        };
        let spec = args.spec().unwrap();
        let problems = args.problems(&spec).join("\n");
        assert!(problems.contains("--yes-write"), "{problems}");
        assert!(problems.contains("--governor"), "{problems}");
        // with the data already there and writes allowed, nothing is left; spelled
        // with the flag's name before F67, which still parses
        let BenchCommand::Run(args) = parse(&[
            "run", "--attach", "--dataset", "d", "--mixes", "read100,insert100", "--preloaded", "--yes-write",
        ]) else {
            panic!("not a run");
        };
        let spec = args.spec().unwrap();
        assert_eq!(spec.workloads.len(), 2);
        assert!(args.problems(&spec).is_empty(), "{:?}", args.problems(&spec));
    }

    /// An attached run loads its own preload before its first arm, so a workload that reads may
    /// come first once writes are allowed; a run that only reads still needs one or the other
    #[test]
    fn an_attached_read_first_runs_once_writes_are_allowed() {
        let problems = |line: &[&str]| {
            let BenchCommand::Run(args) = parse(line) else {
                panic!("not a run");
            };
            let spec = args.spec().unwrap();
            args.problems(&spec)
        };
        // reads listed before inserts, writes allowed: the preload goes in before either
        let allowed = problems(&["run", "--attach", "--dataset", "d", "--workloads", "read100,insert100", "--yes-write"]);
        assert!(allowed.is_empty(), "{allowed:?}");
        // only reads, nothing allowed: the preload would write, so one of the two is asked for
        let refused = problems(&["run", "--attach", "--dataset", "d", "--workloads", "read100"]).join("\n");
        assert!(refused.contains("--yes-write") && refused.contains("--preloaded"), "{refused}");
        for flag in ["--yes-write", "--preloaded"] {
            let allowed = problems(&["run", "--attach", "--dataset", "d", "--workloads", "read100", flag]);
            assert!(allowed.is_empty(), "{flag}: {allowed:?}");
        }
    }
}
