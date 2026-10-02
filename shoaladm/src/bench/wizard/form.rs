//! What the run wizard holds and decides: the draft of a run, the page and row in focus, and what
//! every key does to them
//!
//! Nothing here draws or touches a file. A key is handled into an [`Outcome`] the loop acts on,
//! and the draft is judged into a [`BenchSpec`] and the [`Issue`]s standing between it and a run,
//! so every decision can be tested without a terminal
//! ([F67](../../../../docs/src/features/bench-run-wizard.md)).

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use shoal_loadgen::feed::Preload;
use shoal_loadgen::keys::KeyDistribution;
use shoal_loadgen::spec::{BenchSpec, EventKind, Mode, OnExhaust, ReadLevel, Reads, Workload};
use std::path::PathBuf;

use crate::bench::args::{weights, BenchRunArgs};

/// The named workloads, in the order the wizard lists them
pub const NAMED: [&str; 4] = ["read100", "insert100", "rw50", "read90"];

/// The bundle sizes the wizard offers to tick, in the order it lists them
pub const BUNDLES: [usize; 5] = [1, 4, 16, 64, 256];

/// Every event, in the order the wizard lists them
pub const EVENTS: [EventKind; 8] = [
    EventKind::None,
    EventKind::Kill,
    EventKind::Stop,
    EventKind::Rebalance,
    EventKind::Decommission,
    EventKind::Remove,
    EventKind::Repair,
    EventKind::Backup,
];

/// The key distributions, in the order a choice cycles them
pub const DISTRIBUTIONS: [&str; 3] = ["uniform", "zipfian", "latest"];

/// The read levels, in the order a choice cycles them
pub const READ_LEVELS: [&str; 3] = ["default", "one", "quorum"];

/// Warm or cold reads, in the order a choice cycles them
pub const READS: [&str; 2] = ["warm", "cold"];

/// What an arm does when its inserts run out, in the order a choice cycles them
pub const ON_EXHAUST: [&str; 2] = ["end", "wrap"];

/// What a named workload does, what it answers, and what it needs
///
/// The one place a workload is explained: the wizard's panel and the docs say the same.
///
/// # Arguments
///
/// * `name` - The workload's name, or anything else for a custom one
#[must_use]
pub fn workload_help(name: &str) -> &'static str {
    match name {
        "read100" => {
            "Only gets, each for a key the preload put in. It measures the read path alone: the \
             lookup, the index and the archive under it. It never writes, so it is the one \
             workload that sends no insert - but it reads only preloaded keys, so on an attached \
             cluster the run loads the preload first, which needs --yes-write, unless the \
             cluster already holds it (--preloaded)."
        }
        "insert100" => {
            "Only inserts, of the rows the preload left out of each file. It measures the write \
             path: the log, replication and the commit. It ends early once a file's rows run out \
             unless on-exhaust is wrap, and on an attached cluster it needs --yes-write and \
             overwrites any row with the same key."
        }
        "rw50" => {
            "Half gets and half inserts, drawn query by query. The reference mixture: how reads \
             and writes slow each other down. It writes, so an attached cluster needs --yes-write."
        }
        "read90" => {
            "Nine gets to every insert: a read heavy service with a trickle of writes. It writes, \
             so an attached cluster needs --yes-write."
        }
        _ => {
            "Any share by weight, written read:N,insert:M - read:3,insert:1 is three gets to \
             every insert. Its arms are named read{N}-insert{M}, so two spellings of one share \
             are one arm."
        }
    }
}

/// What an event does to the cluster while an arm runs, and what it needs
///
/// # Arguments
///
/// * `event` - The event
#[must_use]
pub fn event_help(event: EventKind) -> &'static str {
    match event {
        EventKind::None => "Nothing: the steady state every other event is read against.",
        EventKind::Kill => {
            "A node is killed with SIGKILL at --event-at (a third of the way in) and started \
             again at --restart-at. It shows what a crash costs the clients, and how long until \
             they recover. The bench's own cluster only."
        }
        EventKind::Stop => {
            "A node is stopped cleanly and started again, at the same marks as kill: a planned \
             restart. The bench's own cluster only."
        }
        EventKind::Rebalance => {
            "A spare node (--spare, outside the bootstrap set) is added and the cluster \
             rebalanced onto it while the arm runs. The bench's own cluster only."
        }
        EventKind::Decommission => {
            "A node is drained onto a spare (--spare) and leaves. The bench's own cluster only."
        }
        EventKind::Remove => {
            "A node is killed for good and the cluster removes it after its grace. The bench's \
             own cluster only."
        }
        EventKind::Repair => {
            "A table's replicas are verified against each other in the background: what a scrub \
             costs the foreground. An attached cluster needs --yes-events."
        }
        EventKind::Backup => {
            "A table is backed up in the background: what a backup costs the foreground. An \
             attached cluster needs --yes-events."
        }
    }
}

/// What a bundle size measures
pub const BUNDLE_HELP: &str = "A bundle is how many queries travel to the cluster in one frame \
    and come back as one answer. At 1 every query pays its own round trip, so it measures \
    latency; larger bundles share it, so they measure throughput. Every size runs every \
    workload and event again, so each one ticked multiplies the arms. A worker keeps \
    --in-flight queries outstanding, four bundles by default.";

/// The pages of the wizard, in the order they are walked
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Page {
    /// What each arm sends: the share of reads and inserts
    Workloads,
    /// How many queries go in one frame
    Bundles,
    /// What is done to the cluster while an arm runs
    Events,
    /// How long, how many times, and how many streams
    Timing,
    /// What is read, how, and how much is loaded first
    Reads,
    /// The arms, the time they take, and what stands in the way
    Review,
}

impl Page {
    /// Every page, in order
    pub const ALL: [Page; 6] = [
        Page::Workloads,
        Page::Bundles,
        Page::Events,
        Page::Timing,
        Page::Reads,
        Page::Review,
    ];

    /// The page's title
    #[must_use]
    pub fn title(self) -> &'static str {
        match self {
            Page::Workloads => "Workloads",
            Page::Bundles => "Bundles",
            Page::Events => "Events",
            Page::Timing => "Timing",
            Page::Reads => "Reads",
            Page::Review => "Review",
        }
    }

    /// The page after this one, if any
    #[must_use]
    pub fn next(self) -> Option<Page> {
        let index = Page::ALL.iter().position(|page| *page == self)?;
        Page::ALL.get(index + 1).copied()
    }

    /// The page before this one, if any
    #[must_use]
    pub fn prev(self) -> Option<Page> {
        let index = Page::ALL.iter().position(|page| *page == self)?;
        index.checked_sub(1).map(|index| Page::ALL[index])
    }
}

/// How a row of a page is edited
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    /// Ticked or not, with space
    Check,
    /// Free text, typed
    Text,
    /// One of a fixed set, cycled with the arrows or space
    Choice,
}

/// Every row the wizard can focus
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Row {
    /// A named workload, by its place in [`NAMED`]
    Named(usize),
    /// A custom workload, by its place in the draft's list
    Custom(usize),
    /// Whether an attached cluster may be written to
    YesWrite,
    /// A bundle size, by its place in [`BUNDLES`]
    Bundle(usize),
    /// Any other bundle sizes
    ExtraBundles,
    /// An event, by its place in [`EVENTS`]
    Event(usize),
    /// Whether an attached cluster may have an event run on it
    YesEvents,
    /// How long an arm is measured
    Duration,
    /// How long it runs first
    Warmup,
    /// How many times each arm runs
    Runs,
    /// How many streams drive the cluster
    Workers,
    /// How many queries a stream keeps outstanding
    InFlight,
    /// How much of each file is loaded first
    Preload,
    /// Whether an attached cluster already holds the preload
    Preloaded,
    /// Which keys are read most
    Distribution,
    /// How many keys one get asks for
    ReadKeys,
    /// The level reads are served at
    ReadLevel,
    /// Warm or cold reads
    WarmCold,
    /// Whether a row whose key an earlier row had is skipped
    Dedupe,
    /// What an arm does when its inserts run out
    OnExhaust,
    /// Each table's weight
    Tables,
    /// Where the spec is saved
    SavePath,
}

impl Row {
    /// How the row is edited
    #[must_use]
    pub fn kind(self) -> RowKind {
        match self {
            Row::Named(_)
            | Row::YesWrite
            | Row::Bundle(_)
            | Row::Event(_)
            | Row::YesEvents
            | Row::Preloaded
            | Row::Dedupe => RowKind::Check,
            Row::Distribution | Row::ReadLevel | Row::WarmCold | Row::OnExhaust => RowKind::Choice,
            Row::Custom(_)
            | Row::ExtraBundles
            | Row::Duration
            | Row::Warmup
            | Row::Runs
            | Row::Workers
            | Row::InFlight
            | Row::Preload
            | Row::ReadKeys
            | Row::Tables
            | Row::SavePath => RowKind::Text,
        }
    }

    /// What the row is called
    #[must_use]
    pub fn label(self) -> String {
        match self {
            Row::Named(index) => NAMED[index].to_string(),
            Row::Custom(_) => "custom".to_string(),
            Row::YesWrite => "allow writes (--yes-write)".to_string(),
            Row::Bundle(index) => format!("{}", BUNDLES[index]),
            Row::ExtraBundles => "other sizes".to_string(),
            Row::Event(index) => EVENTS[index].as_str().to_string(),
            Row::YesEvents => "allow events (--yes-events)".to_string(),
            Row::Duration => "measured, seconds".to_string(),
            Row::Warmup => "warmup, seconds".to_string(),
            Row::Runs => "runs of each arm".to_string(),
            Row::Workers => "workers".to_string(),
            Row::InFlight => "in flight per worker".to_string(),
            Row::Preload => "preload".to_string(),
            Row::Preloaded => "already preloaded".to_string(),
            Row::Distribution => "key distribution".to_string(),
            Row::ReadKeys => "keys per get".to_string(),
            Row::ReadLevel => "read level".to_string(),
            Row::WarmCold => "reads start".to_string(),
            Row::Dedupe => "skip repeated keys".to_string(),
            Row::OnExhaust => "when inserts run out".to_string(),
            Row::Tables => "table weights".to_string(),
            Row::SavePath => "save spec to".to_string(),
        }
    }

    /// What the row means, shown beside the page while it has focus
    #[must_use]
    pub fn help(self) -> &'static str {
        match self {
            Row::Named(index) => workload_help(NAMED[index]),
            Row::Custom(_) => workload_help(""),
            Row::YesWrite => {
                "An attached cluster is somebody's, and the run writes to it twice over: a workload \
                 that inserts, and the preload the run loads before its first arm unless the \
                 cluster already holds it (already preloaded, on the Reads page). Either is refused \
                 unless this is ticked, knowing a row it already holds is overwritten."
            }
            Row::Bundle(_) | Row::ExtraBundles => BUNDLE_HELP,
            Row::Event(index) => event_help(EVENTS[index]),
            Row::YesEvents => {
                "A repair or backup acts on the attached cluster, and is refused unless this is \
                 ticked. An event that takes a node down is never run on an attached cluster."
            }
            Row::Duration => "How long each arm is measured. Its figures are this window's alone.",
            Row::Warmup => {
                "How long each arm runs before it is measured, so caches and connections settle \
                 first. Counted but left out of the measured figures."
            }
            Row::Runs => {
                "How many times each arm runs, rotated so no arm always goes first. Compare needs \
                 two at least to judge a difference."
            }
            Row::Workers => "How many streams drive the cluster, spread over its members.",
            Row::InFlight => {
                "How many queries a worker keeps outstanding. Blank is four bundles; it can never \
                 be less than a bundle."
            }
            Row::Preload => {
                "How much of each table's file is loaded before anything is measured: a count of \
                 rows (1000) or a share (50%). Reads ask for preloaded keys; inserts take the rest."
            }
            Row::Preloaded => {
                "With an attached cluster, the preload is already in it: nothing is loaded and a \
                 sample is read back to check."
            }
            Row::Distribution => {
                "Which keys reads ask for most: uniform, zipfian (a few keys most of the time) or \
                 latest (the most recently written)."
            }
            Row::ReadKeys => "How many keys one get asks for at once.",
            Row::ReadLevel => {
                "The level reads are served at: the table's default, one replica, or a quorum \
                 barrier first."
            }
            Row::WarmCold => {
                "Warm reads find what the preload left in memory; cold reads restart every node \
                 first, so a read starts from storage. Cold is the bench's own cluster only."
            }
            Row::Dedupe => "Skip an insert whose key an earlier row had, so every insert is a new row.",
            Row::OnExhaust => {
                "When an arm's inserts run out: end the arm there, or wrap and insert the same rows \
                 again as overwrites."
            }
            Row::Tables => {
                "Each table's share of the queries, Table:weight separated by commas. Blank \
                 weights each table by the rows it has to offer."
            }
            Row::SavePath => {
                "Where Ctrl-S writes this run as a spec file; `--spec <file>` runs it again \
                 without the wizard."
            }
        }
    }
}

/// How bad an issue is
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// The run can start, but something should be looked at
    Warning,
    /// The run cannot start until this is fixed
    Error,
}

/// Something between a draft and a run
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    /// How bad it is
    pub severity: Severity,
    /// The page it is on
    pub page: Page,
    /// The row it is on, if one
    pub row: Option<Row>,
    /// What is wrong
    pub message: String,
}

impl Issue {
    /// An error on a row
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    /// * `row` - The row
    /// * `message` - What is wrong
    fn error(page: Page, row: Row, message: String) -> Self {
        Issue {
            severity: Severity::Error,
            page,
            row: Some(row),
            message,
        }
    }
}

/// The page a refusal of the whole run belongs on, by what it names
///
/// # Arguments
///
/// * `problem` - The refusal, as `BenchRunArgs::problems` words it
#[must_use]
pub fn page_of(problem: &str) -> Page {
    // the most specific words first: a refusal naming an event's flag is an event's
    let has = |words: &[&str]| words.iter().any(|word| problem.contains(word));
    if has(&["event", "--spare", "--victim", "--yes-events"]) {
        Page::Events
    } else if has(&["workload", "--yes-write"]) {
        Page::Workloads
    } else if has(&["--preloaded"]) {
        Page::Reads
    } else if has(&["bundle", "--in-flight"]) {
        Page::Bundles
    } else if has(&["--duration", "--runs", "--workers", "--warmup"]) {
        Page::Timing
    } else if has(&["read", "preload", "--distribution", "--tables", "cold"]) {
        Page::Reads
    } else {
        Page::Review
    }
}

/// Write a preload the way it is typed
///
/// # Arguments
///
/// * `preload` - The preload
fn preload_text(preload: Preload) -> String {
    match preload {
        Preload::Rows(rows) => rows.to_string(),
        Preload::Percent(percent) => format!("{percent}%"),
    }
}

/// The name a lowercase enum is written by in a spec
///
/// # Arguments
///
/// * `value` - The value
fn name_of<T: serde::Serialize>(value: &T) -> String {
    serde_yaml::to_string(value)
        .map(|text| text.trim().to_string())
        .unwrap_or_default()
}

/// Every choice the wizard holds about a run, as it is edited
#[derive(Debug, Clone, PartialEq)]
pub struct Draft {
    /// Whether each named workload is ticked, in the order of [`NAMED`]
    pub named: [bool; NAMED.len()],
    /// The custom workloads, as typed
    pub custom: Vec<String>,
    /// Whether each bundle size is ticked, in the order of [`BUNDLES`]
    pub bundles: [bool; BUNDLES.len()],
    /// Any other bundle sizes, comma separated
    pub extra_bundles: String,
    /// Whether each event is ticked, in the order of [`EVENTS`]
    pub events: [bool; EVENTS.len()],
    /// How long an arm is measured, as typed
    pub duration: String,
    /// How long it runs first, as typed
    pub warmup: String,
    /// How many runs, as typed
    pub runs: String,
    /// How many workers, as typed
    pub workers: String,
    /// How many queries in flight, as typed; blank for the default
    pub in_flight: String,
    /// The preload, as typed
    pub preload: String,
    /// Whether an attached cluster holds the preload
    pub preloaded: bool,
    /// The key distribution, by its place in [`DISTRIBUTIONS`]
    pub distribution: usize,
    /// How many keys a get asks for, as typed
    pub read_keys: String,
    /// The read level, by its place in [`READ_LEVELS`]
    pub read_level: usize,
    /// Warm or cold, by its place in [`READS`]
    pub reads: usize,
    /// Whether repeated keys are skipped
    pub dedupe: bool,
    /// What an arm does when its inserts run out, by its place in [`ON_EXHAUST`]
    pub on_exhaust: usize,
    /// Each table's weight, as typed
    pub tables: String,
    /// Whether an attached cluster may be written to
    pub yes_write: bool,
    /// Whether an attached cluster may have an event run on it
    pub yes_events: bool,
    /// Where the spec is saved, as typed
    pub save_path: String,
}

impl Draft {
    /// A draft of a spec and the flags it came with
    ///
    /// # Arguments
    ///
    /// * `spec` - The spec as the flags and spec file left it
    /// * `args` - The flags
    /// * `save_path` - Where the spec is saved by default
    #[must_use]
    pub fn from_spec(spec: &BenchSpec, args: &BenchRunArgs, save_path: &std::path::Path) -> Self {
        // a named workload is ticked by its name, and anything else is a custom one
        let mut named = [false; NAMED.len()];
        let mut custom = Vec::new();
        for workload in &spec.workloads {
            match NAMED.iter().position(|name| *name == workload.name) {
                Some(index) => named[index] = true,
                None => custom.push(String::from(workload.clone())),
            }
        }
        // a bundle size is ticked if it is offered, and listed otherwise
        let mut bundles = [false; BUNDLES.len()];
        let mut extra = Vec::new();
        for bundle in &spec.bundles {
            match BUNDLES.iter().position(|size| size == bundle) {
                Some(index) => bundles[index] = true,
                None => extra.push(bundle.to_string()),
            }
        }
        let events = EVENTS.map(|event| spec.events.contains(&event));
        let place = |list: &[&str], value: String| list.iter().position(|name| *name == value).unwrap_or(0);
        Draft {
            named,
            custom,
            bundles,
            extra_bundles: extra.join(","),
            events,
            duration: spec.duration.to_string(),
            warmup: spec.warmup.to_string(),
            runs: spec.runs.to_string(),
            workers: spec.workers.to_string(),
            in_flight: spec.in_flight.map(|value| value.to_string()).unwrap_or_default(),
            preload: preload_text(spec.preload),
            preloaded: spec.preloaded,
            distribution: place(&DISTRIBUTIONS, name_of(&spec.distribution)),
            read_keys: spec.read_keys.to_string(),
            read_level: place(&READ_LEVELS, name_of(&spec.read_level)),
            reads: place(&READS, name_of(&spec.reads)),
            dedupe: spec.dedupe,
            on_exhaust: place(&ON_EXHAUST, name_of(&spec.on_exhaust)),
            tables: spec
                .tables
                .iter()
                .map(|(table, weight)| format!("{table}:{weight}"))
                .collect::<Vec<_>>()
                .join(","),
            yes_write: args.yes_write,
            yes_events: args.yes_events,
            save_path: save_path.display().to_string(),
        }
    }

    /// Build the spec this draft says, over the spec it started from, with what it got wrong
    ///
    /// # Arguments
    ///
    /// * `base` - The spec it started from, for every field the wizard does not edit
    #[must_use]
    pub fn build(&self, base: &BenchSpec) -> (BenchSpec, Vec<Issue>) {
        let mut spec = base.clone();
        let mut issues = Vec::new();
        // the workloads: the named ones ticked, then each custom one that parses
        spec.workloads = NAMED
            .iter()
            .zip(self.named)
            .filter(|(_, ticked)| *ticked)
            .map(|(name, _)| name.parse().expect("a named workload parses"))
            .collect();
        for (index, raw) in self.custom.iter().enumerate() {
            match raw.parse::<Workload>() {
                Ok(workload) if !spec.workloads.contains(&workload) => spec.workloads.push(workload),
                Ok(_) => issues.push(Issue::error(Page::Workloads, Row::Custom(index), "listed twice".to_string())),
                Err(error) => issues.push(Issue::error(Page::Workloads, Row::Custom(index), error)),
            }
        }
        // the bundle sizes, ticked and typed, smallest first
        spec.bundles = BUNDLES
            .iter()
            .zip(self.bundles)
            .filter(|(_, ticked)| *ticked)
            .map(|(size, _)| *size)
            .collect();
        for raw in self.extra_bundles.split(',').map(str::trim).filter(|raw| !raw.is_empty()) {
            match raw.parse::<usize>() {
                Ok(size) => spec.bundles.push(size),
                Err(error) => issues.push(Issue::error(Page::Bundles, Row::ExtraBundles, format!("{raw:?}: {error}"))),
            }
        }
        spec.bundles.sort_unstable();
        spec.bundles.dedup();
        spec.events = EVENTS
            .iter()
            .zip(self.events)
            .filter(|(_, ticked)| *ticked)
            .map(|(event, _)| *event)
            .collect();
        // the numbers, each refused on its own row when it is not one
        macro_rules! number {
            ($field:ident, $row:expr, $page:expr) => {
                match self.$field.trim().parse() {
                    Ok(value) => spec.$field = value,
                    Err(error) => issues.push(Issue::error($page, $row, format!("not a number: {error}"))),
                }
            };
        }
        number!(duration, Row::Duration, Page::Timing);
        number!(warmup, Row::Warmup, Page::Timing);
        number!(runs, Row::Runs, Page::Timing);
        number!(workers, Row::Workers, Page::Timing);
        number!(read_keys, Row::ReadKeys, Page::Reads);
        spec.in_flight = match self.in_flight.trim() {
            "" => None,
            raw => match raw.parse() {
                Ok(value) => Some(value),
                Err(error) => {
                    issues.push(Issue::error(Page::Timing, Row::InFlight, format!("not a number: {error}")));
                    spec.in_flight
                }
            },
        };
        match self.preload.parse::<Preload>() {
            Ok(preload) => spec.preload = preload,
            Err(error) => issues.push(Issue::error(Page::Reads, Row::Preload, error)),
        }
        // the choices, by the names a spec writes them
        let choose = |list: &[&'static str], at: usize| list[at.min(list.len() - 1)];
        spec.distribution = serde_yaml::from_str::<KeyDistribution>(choose(&DISTRIBUTIONS, self.distribution))
            .unwrap_or_default();
        spec.read_level = serde_yaml::from_str::<ReadLevel>(choose(&READ_LEVELS, self.read_level)).unwrap_or_default();
        spec.reads = serde_yaml::from_str::<Reads>(choose(&READS, self.reads)).unwrap_or_default();
        spec.on_exhaust = serde_yaml::from_str::<OnExhaust>(choose(&ON_EXHAUST, self.on_exhaust)).unwrap_or_default();
        spec.preloaded = self.preloaded;
        spec.dedupe = self.dedupe;
        match weights(&self.tables) {
            Ok(tables) => spec.tables = tables,
            Err(error) => issues.push(Issue::error(Page::Reads, Row::Tables, error)),
        }
        // a warning the run would start under, but compare would not judge
        if spec.runs == 1 {
            issues.push(Issue {
                severity: Severity::Warning,
                page: Page::Timing,
                row: Some(Row::Runs),
                message: "one run cannot be compared; two at least".to_string(),
            });
        }
        (spec, issues)
    }
}

/// A question the wizard is waiting on an answer to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirm {
    /// Leave without running
    Quit,
    /// Replace the spec file already at the save path
    Overwrite,
}

/// What the loop should do after a key
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Draw again and keep going
    Continue,
    /// Start the run
    Run,
    /// Write the spec to the save path, and keep going
    Save,
    /// Leave without running
    Quit,
}

/// The whole wizard: the draft, the run's flags, where the operator is, and what it is showing
#[derive(Debug, Clone)]
pub struct Wizard {
    /// The run being built
    pub draft: Draft,
    /// The spec it started from, for every field the wizard does not edit
    pub base: BenchSpec,
    /// The run's flags, which judge what an attached cluster allows
    pub args: BenchRunArgs,
    /// The tables of the dataset, for the weights' placeholder
    pub tables: Vec<String>,
    /// The page shown
    pub page: Page,
    /// The row in focus on the page
    pub focus: usize,
    /// A question waiting on an answer
    pub confirm: Option<Confirm>,
    /// Whether the operator agreed to replace the file at the save path
    pub overwrite: bool,
    /// A line to show the operator, and whether it is bad news
    pub message: Option<(Severity, String)>,
    /// How far the review is scrolled
    pub scroll: u16,
}

impl Wizard {
    /// Start a wizard on the spec the flags left
    ///
    /// # Arguments
    ///
    /// * `base` - The spec from the flags and spec file
    /// * `args` - The flags
    /// * `tables` - The dataset's tables
    /// * `save_path` - Where the spec is saved by default
    #[must_use]
    pub fn new(base: BenchSpec, args: BenchRunArgs, tables: Vec<String>, save_path: PathBuf) -> Self {
        Wizard {
            draft: Draft::from_spec(&base, &args, &save_path),
            base,
            args,
            tables,
            page: Page::Workloads,
            focus: 0,
            confirm: None,
            overwrite: false,
            message: None,
            scroll: 0,
        }
    }

    /// Whether the run drives a cluster already deployed
    #[must_use]
    pub fn attached(&self) -> bool {
        self.base.mode == Mode::Attach
    }

    /// The run's flags with the wizard's answers laid over them
    #[must_use]
    pub fn args(&self) -> BenchRunArgs {
        let mut args = self.args.clone();
        args.yes_write = self.draft.yes_write;
        args.yes_events = self.draft.yes_events;
        args
    }

    /// Build the spec and everything standing between it and a run
    #[must_use]
    pub fn build(&self) -> (BenchSpec, Vec<Issue>) {
        // what the draft says on its own
        let (spec, mut issues) = self.draft.build(&self.base);
        // then every refusal the run would make, on the page that can fix it
        for problem in self.args().problems(&spec) {
            issues.push(Issue {
                severity: Severity::Error,
                page: page_of(&problem),
                row: None,
                message: problem,
            });
        }
        (spec, issues)
    }

    /// The rows of a page, in the order the focus moves through them
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    #[must_use]
    pub fn rows(&self, page: Page) -> Vec<Row> {
        // the flags that only an attached cluster asks for
        let attached = self.attached();
        match page {
            Page::Workloads => {
                let mut rows: Vec<Row> = (0..NAMED.len()).map(Row::Named).collect();
                rows.extend((0..self.draft.custom.len()).map(Row::Custom));
                if attached {
                    rows.push(Row::YesWrite);
                }
                rows
            }
            Page::Bundles => {
                let mut rows: Vec<Row> = (0..BUNDLES.len()).map(Row::Bundle).collect();
                rows.push(Row::ExtraBundles);
                rows
            }
            Page::Events => {
                let mut rows: Vec<Row> = (0..EVENTS.len()).map(Row::Event).collect();
                if attached {
                    rows.push(Row::YesEvents);
                }
                rows
            }
            Page::Timing => vec![Row::Duration, Row::Warmup, Row::Runs, Row::Workers, Row::InFlight],
            Page::Reads => {
                let mut rows = vec![Row::Preload];
                if attached {
                    rows.push(Row::Preloaded);
                }
                rows.extend([
                    Row::Distribution,
                    Row::ReadKeys,
                    Row::ReadLevel,
                    Row::WarmCold,
                    Row::Dedupe,
                    Row::OnExhaust,
                    Row::Tables,
                ]);
                rows
            }
            Page::Review => vec![Row::SavePath],
        }
    }

    /// The row in focus, if the page has any
    #[must_use]
    pub fn focused(&self) -> Option<Row> {
        self.rows(self.page).get(self.focus).copied()
    }

    /// Whether a check row is ticked
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    #[must_use]
    pub fn checked(&self, row: Row) -> bool {
        let draft = &self.draft;
        match row {
            Row::Named(index) => draft.named[index],
            Row::Bundle(index) => draft.bundles[index],
            Row::Event(index) => draft.events[index],
            Row::YesWrite => draft.yes_write,
            Row::YesEvents => draft.yes_events,
            Row::Preloaded => draft.preloaded,
            Row::Dedupe => draft.dedupe,
            _ => false,
        }
    }

    /// The value a text or choice row shows
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    #[must_use]
    pub fn value(&self, row: Row) -> String {
        let draft = &self.draft;
        match row {
            Row::Custom(index) => draft.custom.get(index).cloned().unwrap_or_default(),
            Row::ExtraBundles => draft.extra_bundles.clone(),
            Row::Duration => draft.duration.clone(),
            Row::Warmup => draft.warmup.clone(),
            Row::Runs => draft.runs.clone(),
            Row::Workers => draft.workers.clone(),
            Row::InFlight => draft.in_flight.clone(),
            Row::Preload => draft.preload.clone(),
            Row::ReadKeys => draft.read_keys.clone(),
            Row::Tables => draft.tables.clone(),
            Row::SavePath => draft.save_path.clone(),
            Row::Distribution => DISTRIBUTIONS[draft.distribution].to_string(),
            Row::ReadLevel => READ_LEVELS[draft.read_level].to_string(),
            Row::WarmCold => READS[draft.reads].to_string(),
            Row::OnExhaust => ON_EXHAUST[draft.on_exhaust].to_string(),
            _ => String::new(),
        }
    }

    /// What a blank text row stands for
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    #[must_use]
    pub fn placeholder(&self, row: Row) -> String {
        match row {
            Row::Custom(_) => "read:N,insert:M".to_string(),
            Row::ExtraBundles => "none; e.g. 8,32".to_string(),
            Row::InFlight => "four bundles".to_string(),
            Row::Tables => {
                let tables: Vec<String> = self.tables.iter().map(|table| format!("{table}:1")).collect();
                format!("by their rows; e.g. {}", tables.join(","))
            }
            _ => String::new(),
        }
    }

    /// The text a text row edits, if it is one
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    fn text_mut(&mut self, row: Row) -> Option<&mut String> {
        let draft = &mut self.draft;
        match row {
            Row::Custom(index) => draft.custom.get_mut(index),
            Row::ExtraBundles => Some(&mut draft.extra_bundles),
            Row::Duration => Some(&mut draft.duration),
            Row::Warmup => Some(&mut draft.warmup),
            Row::Runs => Some(&mut draft.runs),
            Row::Workers => Some(&mut draft.workers),
            Row::InFlight => Some(&mut draft.in_flight),
            Row::Preload => Some(&mut draft.preload),
            Row::ReadKeys => Some(&mut draft.read_keys),
            Row::Tables => Some(&mut draft.tables),
            Row::SavePath => Some(&mut draft.save_path),
            _ => None,
        }
    }

    /// Tick or untick a check row
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    fn flip(&mut self, row: Row) {
        let draft = &mut self.draft;
        let slot = match row {
            Row::Named(index) => &mut draft.named[index],
            Row::Bundle(index) => &mut draft.bundles[index],
            Row::Event(index) => &mut draft.events[index],
            Row::YesWrite => &mut draft.yes_write,
            Row::YesEvents => &mut draft.yes_events,
            Row::Preloaded => &mut draft.preloaded,
            Row::Dedupe => &mut draft.dedupe,
            _ => return,
        };
        *slot = !*slot;
    }

    /// Move a choice row to its next or previous value
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    /// * `forward` - Whether to move forward
    fn cycle(&mut self, row: Row, forward: bool) {
        let draft = &mut self.draft;
        let (slot, len) = match row {
            Row::Distribution => (&mut draft.distribution, DISTRIBUTIONS.len()),
            Row::ReadLevel => (&mut draft.read_level, READ_LEVELS.len()),
            Row::WarmCold => (&mut draft.reads, READS.len()),
            Row::OnExhaust => (&mut draft.on_exhaust, ON_EXHAUST.len()),
            _ => return,
        };
        *slot = if forward { (*slot + 1) % len } else { (*slot + len - 1) % len };
    }

    /// Show a page, from its top
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    pub fn go(&mut self, page: Page) {
        self.page = page;
        self.focus = 0;
        self.scroll = 0;
    }

    /// Handle one key
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // a question on screen takes the next key as its answer
        if let Some(confirm) = self.confirm.take() {
            return self.answer(confirm, key.code);
        }
        // a message is read once
        self.message = None;
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        // the keys that work everywhere
        match key.code {
            KeyCode::Char('c') if ctrl => {
                self.confirm = Some(Confirm::Quit);
                return Outcome::Continue;
            }
            KeyCode::Esc => {
                self.confirm = Some(Confirm::Quit);
                return Outcome::Continue;
            }
            KeyCode::Char('n') if ctrl => return self.turn(true),
            KeyCode::PageDown => return self.turn(true),
            KeyCode::Char('p') if ctrl => return self.turn(false),
            KeyCode::PageUp => return self.turn(false),
            KeyCode::Char('s') if ctrl => return self.save(),
            KeyCode::Char('r') if ctrl => return self.run(),
            _ => (),
        }
        // the review page scrolls and starts the run
        if self.page == Page::Review {
            match key.code {
                KeyCode::Enter => return self.run(),
                KeyCode::Up => {
                    self.scroll = self.scroll.saturating_sub(1);
                    return Outcome::Continue;
                }
                KeyCode::Down => {
                    self.scroll = self.scroll.saturating_add(1);
                    return Outcome::Continue;
                }
                _ => (),
            }
        }
        self.row_key(key)
    }

    /// Turn to the next or the previous page
    ///
    /// # Arguments
    ///
    /// * `forward` - Whether to go to the next page
    fn turn(&mut self, forward: bool) -> Outcome {
        let page = if forward { self.page.next() } else { self.page.prev() };
        if let Some(page) = page {
            self.go(page);
        }
        Outcome::Continue
    }

    /// Take the answer to a question
    ///
    /// # Arguments
    ///
    /// * `confirm` - The question
    /// * `code` - The key answering it
    fn answer(&mut self, confirm: Confirm, code: KeyCode) -> Outcome {
        let yes = matches!(code, KeyCode::Char('y' | 'Y'));
        match (confirm, yes) {
            (Confirm::Quit, true) => Outcome::Quit,
            (Confirm::Overwrite, true) => {
                self.overwrite = true;
                Outcome::Save
            }
            (_, false) => Outcome::Continue,
        }
    }

    /// Start the run, if nothing stands in the way
    fn run(&mut self) -> Outcome {
        // an error keeps the run from starting, and says where it is
        let (_, issues) = self.build();
        let errors: Vec<&Issue> = issues.iter().filter(|issue| issue.severity == Severity::Error).collect();
        if let Some(first) = errors.first() {
            self.message = Some((
                Severity::Error,
                format!(
                    "{} error{} first, starting on {}: {}",
                    errors.len(),
                    if errors.len() == 1 { "" } else { "s" },
                    first.page.title(),
                    first.message
                ),
            ));
            return Outcome::Continue;
        }
        Outcome::Run
    }

    /// Save the spec, asking first before replacing a file
    fn save(&mut self) -> Outcome {
        // a spec that does not build is not worth saving
        let (_, issues) = self.draft.build(&self.base);
        if let Some(issue) = issues.iter().find(|issue| issue.severity == Severity::Error) {
            self.message = Some((Severity::Error, format!("fix {} first: {}", issue.page.title(), issue.message)));
            return Outcome::Continue;
        }
        if self.draft.save_path.trim().is_empty() {
            self.message = Some((Severity::Error, "give a path to save the spec to".to_string()));
            return Outcome::Continue;
        }
        // a file already there is replaced only when the operator says so
        if std::path::Path::new(self.draft.save_path.trim()).exists() && !self.overwrite {
            self.confirm = Some(Confirm::Overwrite);
            return Outcome::Continue;
        }
        Outcome::Save
    }

    /// Handle a key on the row in focus
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    fn row_key(&mut self, key: KeyEvent) -> Outcome {
        let rows = self.rows(self.page);
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        // a custom workload is added from anywhere on its page
        if self.page == Page::Workloads && key.code == KeyCode::Char('+') {
            self.draft.custom.push(String::new());
            self.focus = NAMED.len() + self.draft.custom.len() - 1;
            return Outcome::Continue;
        }
        let Some(row) = rows.get(self.focus).copied() else {
            return Outcome::Continue;
        };
        match key.code {
            // moving between rows
            KeyCode::Down | KeyCode::Enter => self.focus = (self.focus + 1) % rows.len(),
            KeyCode::Up => self.focus = (self.focus + rows.len() - 1) % rows.len(),
            KeyCode::Tab => return self.turn(true),
            KeyCode::BackTab => return self.turn(false),
            // a check flips on space
            KeyCode::Char(' ') if row.kind() == RowKind::Check => self.flip(row),
            // a choice cycles on the arrows and on space
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if row.kind() == RowKind::Choice => {
                self.cycle(row, key.code != KeyCode::Left);
            }
            // a custom workload is removed with its text
            KeyCode::Delete | KeyCode::Char('d') if ctrl || key.code == KeyCode::Delete => {
                if let Row::Custom(index) = row {
                    self.draft.custom.remove(index);
                    self.focus = self.focus.min(self.rows(self.page).len().saturating_sub(1));
                }
            }
            // text is typed at its end
            KeyCode::Char('u') if ctrl => {
                if let Some(text) = self.text_mut(row) {
                    text.clear();
                }
            }
            KeyCode::Char(c) if !ctrl && row.kind() == RowKind::Text => {
                if let Some(text) = self.text_mut(row) {
                    text.push(c);
                }
            }
            KeyCode::Backspace => {
                if let Some(text) = self.text_mut(row) {
                    text.pop();
                }
            }
            _ => (),
        }
        Outcome::Continue
    }
}

/// How long a spec's arms are measured and warmed up for, in seconds, before any preload, reset
/// or verify
///
/// # Arguments
///
/// * `spec` - The spec
#[must_use]
pub fn least_seconds(spec: &BenchSpec) -> u64 {
    // every run of every arm runs its warmup and its measured time
    spec.arms().len() as u64 * (spec.warmup + spec.duration)
}

/// A count of seconds as hours, minutes and seconds
///
/// # Arguments
///
/// * `seconds` - The seconds
#[must_use]
pub fn clock(seconds: u64) -> String {
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    match (hours, minutes) {
        (0, 0) => format!("{seconds}s"),
        (0, _) => format!("{minutes}m{seconds:02}s"),
        _ => format!("{hours}h{minutes:02}m"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::Parser;

    /// The flags of a `bench run` line
    ///
    /// # Arguments
    ///
    /// * `args` - The arguments after `bench run`
    fn run_args(args: &[&str]) -> BenchRunArgs {
        let line = ["shoaladm", "bench", "run"].into_iter().chain(args.iter().copied());
        match Cli::try_parse_from(line).expect("the line parses").command {
            Command::Bench(crate::bench::BenchCommand::Run(args)) => *args,
            other => panic!("not a run: {other:?}"),
        }
    }

    /// A wizard on the spec a line leaves
    ///
    /// # Arguments
    ///
    /// * `args` - The arguments after `bench run`
    fn wizard(args: &[&str]) -> Wizard {
        let args = run_args(args);
        let spec = args.spec().expect("a spec");
        Wizard::new(spec, args, vec!["Item".to_string(), "Review".to_string()], PathBuf::from("/nowhere/bench.yml"))
    }

    /// A key with no modifiers
    ///
    /// # Arguments
    ///
    /// * `code` - The key
    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    /// Type some text into the row in focus
    ///
    /// # Arguments
    ///
    /// * `wizard` - The wizard
    /// * `text` - What to type
    fn type_text(wizard: &mut Wizard, text: &str) {
        for c in text.chars() {
            wizard.handle_key(key(KeyCode::Char(c)));
        }
    }

    /// The draft opens on the spec the flags left, and builds back to it unchanged
    #[test]
    fn the_defaults_prefill_and_build_back() {
        let wizard = wizard(&["--dataset", "d"]);
        assert_eq!(wizard.draft.named, [true; 4]);
        assert_eq!(wizard.draft.bundles, [true, false, true, true, false]);
        let (spec, issues) = wizard.build();
        assert!(issues.is_empty(), "{issues:?}");
        // the same workloads, in the wizard's order, and every other field as it was
        let mut base = wizard.base.clone();
        base.workloads = NAMED.iter().map(|name| name.parse().unwrap()).collect();
        assert_eq!(spec, base);
    }

    /// Unticking, ticking and adding a custom workload builds the spec they say
    #[test]
    fn a_custom_workload_is_added_and_builds() {
        let mut wizard = wizard(&["--dataset", "d"]);
        // untick read100, insert100 and read90, leaving rw50
        for index in [0, 1, 3] {
            wizard.focus = index;
            wizard.handle_key(key(KeyCode::Char(' ')));
        }
        // a custom one, typed where the focus lands
        wizard.handle_key(key(KeyCode::Char('+')));
        assert_eq!(wizard.focused(), Some(Row::Custom(0)));
        type_text(&mut wizard, "read:3,insert:1");
        // and a bundle of 8 beside 1
        wizard.go(Page::Bundles);
        for index in [2, 3] {
            wizard.focus = index;
            wizard.handle_key(key(KeyCode::Char(' ')));
        }
        wizard.focus = BUNDLES.len();
        type_text(&mut wizard, "8");
        let (spec, issues) = wizard.build();
        assert!(issues.is_empty(), "{issues:?}");
        let names: Vec<&str> = spec.workloads.iter().map(|workload| workload.name.as_str()).collect();
        assert_eq!(names, ["rw50", "read3-insert1"]);
        assert_eq!(spec.bundles, vec![1, 8]);
        assert_eq!(spec.arms().len(), 2 * 2 * 3);
        // a custom row that does not parse is refused on its own row
        wizard.go(Page::Workloads);
        wizard.handle_key(key(KeyCode::Char('+')));
        type_text(&mut wizard, "write:1");
        let (_, issues) = wizard.build();
        assert!(
            issues.iter().any(|issue| issue.row == Some(Row::Custom(1)) && issue.page == Page::Workloads),
            "{issues:?}"
        );
        assert_eq!(wizard.handle_key(key(KeyCode::Enter)), Outcome::Continue);
        assert_eq!(wizard.handle_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL)), Outcome::Continue);
        assert!(wizard.message.is_some());
    }

    /// An attached cluster's writes land on the workloads page until they are allowed there
    #[test]
    fn an_attached_write_is_refused_until_allowed() {
        let mut wizard = wizard(&["--dataset", "d", "--attach", "--preloaded"]);
        let (_, issues) = wizard.build();
        let on_workloads: Vec<&Issue> = issues.iter().filter(|issue| issue.page == Page::Workloads).collect();
        assert!(on_workloads.iter().any(|issue| issue.message.contains("--yes-write")), "{issues:?}");
        // the toggle is the page's last row, and ticking it clears the refusal
        let rows = wizard.rows(Page::Workloads);
        assert_eq!(rows.last(), Some(&Row::YesWrite));
        wizard.focus = rows.len() - 1;
        wizard.handle_key(key(KeyCode::Char(' ')));
        let (_, issues) = wizard.build();
        assert!(issues.iter().all(|issue| !issue.message.contains("--yes-write")), "{issues:?}");
        assert!(wizard.args().yes_write);
        // and the run starts once nothing is wrong
        wizard.go(Page::Review);
        assert_eq!(wizard.handle_key(key(KeyCode::Enter)), Outcome::Run);
    }

    /// An attached run with every default ticked, reads before inserts, starts once its writes
    /// are allowed: the run loads the preload before its first arm, whatever the order
    #[test]
    fn an_attached_run_that_reads_first_starts_once_writes_are_allowed() {
        let mut wizard = wizard(&["--dataset", "d", "--attach"]);
        let rows = wizard.rows(Page::Workloads);
        wizard.focus = rows.iter().position(|row| *row == Row::YesWrite).expect("the toggle");
        wizard.handle_key(key(KeyCode::Char(' ')));
        let (spec, issues) = wizard.build();
        assert_eq!(spec.workloads[0].name, "read100");
        assert!(issues.iter().all(|issue| issue.severity != Severity::Error), "{issues:?}");
        wizard.go(Page::Review);
        assert_eq!(wizard.handle_key(key(KeyCode::Enter)), Outcome::Run);
    }

    /// The arms are every workload, size and event a run, and the least time is theirs
    #[test]
    fn the_arms_and_their_time_are_counted() {
        let wizard = wizard(&["--dataset", "d", "--runs", "2", "--duration", "30", "--warmup", "5"]);
        let (spec, _) = wizard.build();
        // four workloads, three sizes, one event, two runs
        assert_eq!(spec.arms().len(), 4 * 3 * 2);
        assert_eq!(least_seconds(&spec), 24 * 35);
        assert_eq!(clock(24 * 35), "14m00s");
        assert_eq!(clock(59), "59s");
        assert_eq!(clock(3 * 3600 + 120), "3h02m");
    }

    /// A saved spec reads back through `--spec` as the spec the wizard built
    #[test]
    fn a_saved_spec_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench.yml");
        let mut wizard = wizard(&["--dataset", "d"]);
        wizard.draft.save_path = path.display().to_string();
        wizard.draft.named = [false, false, true, false];
        assert_eq!(wizard.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL)), Outcome::Save);
        let (spec, _) = wizard.build();
        crate::bench::wizard::save(&path, &spec).unwrap();
        let path_arg = path.display().to_string();
        let again = run_args(&["--spec", &path_arg]);
        assert!(again.workloads_chosen().unwrap());
        let mut read = again.spec().unwrap();
        read.dataset = spec.dataset.clone();
        assert_eq!(read, spec);
        // a second save asks before replacing it
        assert_eq!(wizard.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL)), Outcome::Continue);
        assert_eq!(wizard.confirm, Some(Confirm::Overwrite));
        assert_eq!(wizard.handle_key(key(KeyCode::Char('y'))), Outcome::Save);
    }

    /// Every named workload, event and bundle has words of its own, and a refusal finds its page
    #[test]
    fn everything_offered_is_explained() {
        let custom = workload_help("");
        for name in NAMED {
            assert!(workload_help(name).len() > 40 && workload_help(name) != custom, "{name}");
        }
        for event in EVENTS {
            assert!(!event_help(event).is_empty(), "{}", event.as_str());
        }
        assert!(!BUNDLE_HELP.is_empty());
        assert_eq!(page_of("an event acts on the attached cluster; pass --yes-events"), Page::Events);
        assert_eq!(page_of("a workload inserts into the attached cluster; pass --yes-write"), Page::Workloads);
        assert_eq!(page_of("--runs must be at least 1"), Page::Timing);
        assert_eq!(
            page_of("the run loads the preload into the attached cluster; pass --yes-write, or --preloaded"),
            Page::Workloads
        );
        assert_eq!(page_of("--preloaded is for an attached cluster"), Page::Reads);
        assert_eq!(page_of("no dataset folder was given (--dataset)"), Page::Review);
    }

    /// Leaving asks first, and anything but yes stays
    #[test]
    fn leaving_is_asked_about() {
        let mut wizard = wizard(&["--dataset", "d"]);
        assert_eq!(wizard.handle_key(key(KeyCode::Esc)), Outcome::Continue);
        assert_eq!(wizard.confirm, Some(Confirm::Quit));
        assert_eq!(wizard.handle_key(key(KeyCode::Char('n'))), Outcome::Continue);
        assert_eq!(wizard.handle_key(key(KeyCode::Esc)), Outcome::Continue);
        assert_eq!(wizard.handle_key(key(KeyCode::Char('y'))), Outcome::Quit);
    }
}
