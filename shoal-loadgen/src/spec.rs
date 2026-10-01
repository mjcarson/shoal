//! What a benchmark runs: the spec, its mixes, and the arms it expands to
//!
//! A spec is written in YAML (`bench.yml`) and every field has a default, so a spec can be as
//! short as the dataset it names; the command line overrides any field. It expands into
//! **arms**: one mix of operations at one bundle size, under one set of inventory overrides and
//! one cluster event, run `runs` times. An arm's [`ArmId`] is the key two captures are compared
//! on, so its spelling is never changed - only added to.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::feed::Preload;
use crate::keys::KeyDistribution;

/// A mix of reads and inserts, by weight
///
/// Written as a name - `insert100`, `read100`, `rw50`, `read90` - or as `read:N,insert:M`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Mix {
    /// What the mix is called in an arm's id
    pub name: String,
    /// The weight of reads
    pub read: u32,
    /// The weight of inserts
    pub insert: u32,
}

impl Mix {
    /// The four mixes a spec runs when it names none
    #[must_use]
    pub fn defaults() -> Vec<Mix> {
        // every one of these parses
        ["read100", "insert100", "rw50", "read90"]
            .into_iter()
            .map(|name| name.parse().expect("the default mixes parse"))
            .collect()
    }

    /// Whether an arm of this mix writes
    #[must_use]
    pub fn writes(&self) -> bool {
        self.insert > 0
    }

    /// Whether an arm of this mix reads
    #[must_use]
    pub fn reads(&self) -> bool {
        self.read > 0
    }
}

impl std::str::FromStr for Mix {
    type Err = String;

    /// Parse a named mix, or `read:N,insert:M`
    ///
    /// # Arguments
    ///
    /// * `raw` - The mix as written
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // the four named mixes
        let named = |read, insert| Mix {
            name: raw.to_string(),
            read,
            insert,
        };
        match raw {
            "insert100" => return Ok(named(0, 100)),
            "read100" => return Ok(named(100, 0)),
            "rw50" => return Ok(named(50, 50)),
            "read90" => return Ok(named(90, 10)),
            _ => (),
        }
        // otherwise weights by kind
        let (mut read, mut insert) = (0u32, 0u32);
        for entry in raw.split(',').filter(|entry| !entry.trim().is_empty()) {
            let (kind, weight) = entry
                .split_once(':')
                .ok_or_else(|| format!("{entry:?} is not kind:weight; a mix is insert100, read100, rw50, read90 or read:N,insert:M"))?;
            let weight: u32 = weight
                .trim()
                .parse()
                .map_err(|error| format!("{entry:?} has a bad weight: {error}"))?;
            match kind.trim() {
                "read" => read += weight,
                "insert" => insert += weight,
                other => return Err(format!("{other:?} is not read or insert")),
            }
        }
        if read + insert == 0 {
            return Err(format!("the mix {raw:?} has no weight"));
        }
        // a custom mix is named by its weights, so two spellings of one mix are one arm
        Ok(Mix {
            name: format!("read{read}-insert{insert}"),
            read,
            insert,
        })
    }
}

impl TryFrom<String> for Mix {
    type Error = String;

    /// Parse a mix read from a spec
    ///
    /// # Arguments
    ///
    /// * `raw` - The mix as written
    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse()
    }
}

impl From<Mix> for String {
    /// Write a mix as the name it is known by
    ///
    /// # Arguments
    ///
    /// * `mix` - The mix
    fn from(mix: Mix) -> Self {
        // a custom mix's name is its weights, which parse back to it
        if mix.name.starts_with("read") && mix.name.contains("-insert") {
            return format!("read:{},insert:{}", mix.read, mix.insert);
        }
        mix.name
    }
}

/// Something done to the cluster while an arm runs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    /// Nothing: the steady state
    #[default]
    None,
    /// A node is killed with SIGKILL and started again
    Kill,
    /// A node is stopped cleanly and started again
    Stop,
    /// A spare node is added and the cluster rebalanced onto it
    Rebalance,
    /// A node is drained onto a spare and leaves
    Decommission,
    /// A node is killed for good and removed
    Remove,
    /// A table's replicas are verified against each other in the background
    Repair,
    /// A table is backed up in the background
    Backup,
}

impl EventKind {
    /// What the event is called in an arm's id
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        // the serialized spelling
        match self {
            EventKind::None => "none",
            EventKind::Kill => "kill",
            EventKind::Stop => "stop",
            EventKind::Rebalance => "rebalance",
            EventKind::Decommission => "decommission",
            EventKind::Remove => "remove",
            EventKind::Repair => "repair",
            EventKind::Backup => "backup",
        }
    }

    /// Whether the event needs a node beyond the bootstrap set to move onto
    #[must_use]
    pub fn needs_spare(&self) -> bool {
        matches!(self, EventKind::Rebalance | EventKind::Decommission)
    }

    /// Whether the event takes a node down, which an attached cluster never allows
    #[must_use]
    pub fn disrupts(&self) -> bool {
        !matches!(self, EventKind::None | EventKind::Repair | EventKind::Backup)
    }
}

impl std::str::FromStr for EventKind {
    type Err = String;

    /// Parse an event by name
    ///
    /// # Arguments
    ///
    /// * `raw` - The event as written
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // the same spellings the spec uses
        serde_yaml::from_str(raw.trim()).map_err(|_| {
            format!("{raw:?} is not none, kill, stop, rebalance, decommission, remove, repair or backup")
        })
    }
}

/// Whether the bench drives a cluster of its own or one already deployed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// A cluster the bench bootstraps beside the inventory's, wipes between arms and destroys
    #[default]
    Owned,
    /// The inventory's own cluster, as it is
    Attach,
}

/// Whether reads find their rows in memory or have to go to storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Reads {
    /// As the preload left them
    #[default]
    Warm,
    /// After every node is restarted, so a read starts from storage
    Cold,
}

/// The level reads are served at
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReadLevel {
    /// Whatever the table or cluster defaults to
    #[default]
    Default,
    /// One replica's applied state
    One,
    /// A quorum barrier first
    Quorum,
}

/// What an arm does when its inserts run out before its time does
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnExhaust {
    /// End the arm there, recording when, so the mix is never quietly changed
    #[default]
    End,
    /// Start the insert pool over, recording where, so later inserts are overwrites
    Wrap,
}

/// A named set of inventory settings an arm runs under
///
/// The settings are inventory fields, by their path in the inventory (`resources.memory`,
/// `replication.fragment_min_bytes`), applied to the bench's own cluster before the arm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Override {
    /// What the override is called in an arm's id
    pub name: String,
    /// The inventory fields it sets, by path
    pub set: BTreeMap<String, serde_yaml::Value>,
}

/// Everything a benchmark run is asked to do
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BenchSpec {
    /// The dataset folder
    pub dataset: PathBuf,
    /// A cluster of the bench's own, or the inventory's
    pub mode: Mode,
    /// How much of each file is loaded before anything is measured
    pub preload: Preload,
    /// Whether the data is already in an attached cluster, so nothing is preloaded
    pub preloaded: bool,
    /// The mixes to run
    pub mixes: Vec<Mix>,
    /// The bundle sizes to run each mix at
    pub bundles: Vec<usize>,
    /// The sets of inventory settings to run each mix under; none runs the inventory as it is
    pub overrides: Vec<Override>,
    /// The events to run each mix under
    pub events: Vec<EventKind>,
    /// How many streams drive the cluster, spread over its members
    pub workers: usize,
    /// How many queries a worker keeps outstanding; by default four bundles
    pub in_flight: Option<usize>,
    /// How long an arm is measured, in seconds
    pub duration: u64,
    /// How long an arm runs before it is measured, in seconds
    pub warmup: u64,
    /// How many times each arm is run
    pub runs: u32,
    /// The seed every choice is drawn from
    pub seed: u64,
    /// Which read keys are asked for most
    pub distribution: KeyDistribution,
    /// How many keys one read asks for, as one get
    pub read_keys: usize,
    /// The level reads are served at
    pub read_level: ReadLevel,
    /// Whether reads start warm or from storage
    pub reads: Reads,
    /// Whether an insert skips a row whose key an earlier row had
    pub dedupe: bool,
    /// What an arm does when its inserts run out
    pub on_exhaust: OnExhaust,
    /// How many times an arm's query that failed in a retriable way is sent again; none by
    /// default, so a failure is counted where it happened
    pub retries: u32,
    /// The share of a file's rows that may fail to parse, in percent
    pub max_parse_errors: f64,
    /// The weight of each table, by name; by default the size of its pool
    pub tables: BTreeMap<String, u32>,
    /// How far into an event arm's measured time its event starts, in percent
    pub event_at: f64,
    /// How far into a kill or stop arm's measured time the node is started again, in percent
    pub restart_at: f64,
    /// The longest an event arm waits past its time for its event to finish, in seconds
    pub event_timeout: u64,
    /// The node an event acts on: a name, `leader`, or `last`
    pub victim: Option<String>,
    /// The node a rebalance or decommission moves onto
    pub spare: Option<String>,
    /// The table a repair or backup acts on; by default the first in the dataset
    pub event_table: Option<String>,
    /// Whether every acknowledged insert is read back after its arm
    pub verify_acks: bool,
}

impl Default for BenchSpec {
    /// The defaults every field falls back to
    fn default() -> Self {
        BenchSpec {
            dataset: PathBuf::new(),
            mode: Mode::Owned,
            preload: Preload::default(),
            preloaded: false,
            mixes: Mix::defaults(),
            bundles: vec![1, 16, 64],
            overrides: Vec::new(),
            events: vec![EventKind::None],
            workers: 8,
            in_flight: None,
            duration: 60,
            warmup: 10,
            runs: 3,
            seed: 7,
            distribution: KeyDistribution::Uniform,
            read_keys: 1,
            read_level: ReadLevel::Default,
            reads: Reads::Warm,
            dedupe: false,
            on_exhaust: OnExhaust::End,
            retries: 0,
            max_parse_errors: 1.0,
            tables: BTreeMap::new(),
            event_at: 33.0,
            restart_at: 67.0,
            event_timeout: 300,
            victim: None,
            spare: None,
            event_table: None,
            verify_acks: true,
        }
    }
}

/// The name an arm is compared by, never renamed: `{mix}/b{bundle}[/{override}]/{event}`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArmId(pub String);

impl std::fmt::Display for ArmId {
    /// Write the id as it is spelled
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One arm of a run, in the order it runs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmPlan {
    /// What it is compared by
    pub id: ArmId,
    /// Its mix
    pub mix: Mix,
    /// How many queries a bundle holds
    pub bundle: usize,
    /// How many queries a worker keeps outstanding
    pub in_flight: usize,
    /// The inventory settings it runs under, if any
    pub overrides: Option<Override>,
    /// What is done to the cluster while it runs
    pub event: EventKind,
    /// Which run of the arm this is, from 0
    pub run: u32,
}

impl ArmPlan {
    /// Whether this arm leaves the cluster other than it found it
    #[must_use]
    pub fn disturbs(&self) -> bool {
        self.mix.writes() || self.event != EventKind::None
    }
}

impl BenchSpec {
    /// Every problem the spec has on its own, before any dataset or cluster is looked at
    #[must_use]
    pub fn problems(&self) -> Vec<String> {
        // each check says what to change
        let mut problems = Vec::new();
        if self.dataset.as_os_str().is_empty() {
            problems.push("no dataset folder was given (--dataset)".to_string());
        }
        if self.mixes.is_empty() {
            problems.push("no mix was given".to_string());
        }
        if self.bundles.is_empty() || self.bundles.contains(&0) {
            problems.push("every bundle size must be at least 1".to_string());
        }
        if self.workers == 0 {
            problems.push("--workers must be at least 1".to_string());
        }
        if let Some(in_flight) = self.in_flight {
            if let Some(bundle) = self.bundles.iter().find(|bundle| **bundle > in_flight) {
                problems.push(format!(
                    "--in-flight {in_flight} is smaller than the bundle size {bundle}, so no bundle would fill"
                ));
            }
        }
        if self.duration == 0 {
            problems.push("--duration must be at least one second".to_string());
        }
        if self.runs == 0 {
            problems.push("--runs must be at least 1".to_string());
        }
        if self.read_keys == 0 {
            problems.push("--read-keys must be at least 1".to_string());
        }
        if self.events.is_empty() {
            problems.push("no event was given; `none` is the steady state".to_string());
        }
        if !(0.0..100.0).contains(&self.event_at) || self.event_at <= 0.0 {
            problems.push("--event-at must be between 0 and 100 percent".to_string());
        }
        if self.restart_at <= self.event_at || self.restart_at >= 100.0 {
            problems.push("--restart-at must come after --event-at and before 100 percent".to_string());
        }
        if !(0.0..=100.0).contains(&self.max_parse_errors) {
            problems.push("--max-parse-errors must be between 0 and 100 percent".to_string());
        }
        // two overrides of one name would be one arm
        let mut names: Vec<&str> = self.overrides.iter().map(|o| o.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            problems.push("two overrides share a name".to_string());
        }
        if self.overrides.iter().any(|o| o.name.is_empty() || o.name.contains('/')) {
            problems.push("an override's name must be non-empty and have no '/'".to_string());
        }
        // an attached cluster is somebody's: nothing that takes a node down, and no cold reads
        if self.mode == Mode::Attach {
            if let Some(event) = self.events.iter().find(|event| event.disrupts()) {
                problems.push(format!(
                    "the event {} takes a node down, which an attached cluster never allows",
                    event.as_str()
                ));
            }
            if self.reads == Reads::Cold {
                problems.push("cold reads restart every node, which an attached cluster never allows".to_string());
            }
            if !self.overrides.is_empty() {
                problems.push("overrides reconfigure the cluster, which an attached cluster never allows".to_string());
            }
        }
        // an event that moves data needs somewhere to move it
        if self.events.iter().any(EventKind::needs_spare) && self.spare.is_none() {
            problems.push("rebalance and decommission need --spare, a node outside the inventory's bootstrap set".to_string());
        }
        problems
    }

    /// How many queries a worker keeps outstanding at a bundle size
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle size
    #[must_use]
    pub fn in_flight_for(&self, bundle: usize) -> usize {
        // four bundles unless told otherwise, and never less than one
        self.in_flight.unwrap_or(bundle * 4).max(bundle)
    }

    /// Every arm of every run, in the order they run
    ///
    /// Steady state arms run before event arms, which disturb the cluster. Within each, a run
    /// starts one arm later than the run before it, so drift over a long run is spread across
    /// the arms rather than landing on the last one every time.
    #[must_use]
    pub fn arms(&self) -> Vec<ArmPlan> {
        // one arm a combination, steady ones first
        let overrides: Vec<Option<&Override>> = if self.overrides.is_empty() {
            vec![None]
        } else {
            self.overrides.iter().map(Some).collect()
        };
        let mut steady = Vec::new();
        let mut eventful = Vec::new();
        for event in &self.events {
            for over in &overrides {
                for mix in &self.mixes {
                    for bundle in &self.bundles {
                        let id = match over {
                            Some(over) => format!("{}/b{bundle}/{}/{}", mix.name, over.name, event.as_str()),
                            None => format!("{}/b{bundle}/{}", mix.name, event.as_str()),
                        };
                        let arm = ArmPlan {
                            id: ArmId(id),
                            mix: mix.clone(),
                            bundle: *bundle,
                            in_flight: self.in_flight_for(*bundle),
                            overrides: over.cloned(),
                            event: *event,
                            run: 0,
                        };
                        if *event == EventKind::None {
                            steady.push(arm);
                        } else {
                            eventful.push(arm);
                        }
                    }
                }
            }
        }
        // each run, rotated by its index within each group
        let mut arms = Vec::new();
        for run in 0..self.runs {
            for group in [&steady, &eventful] {
                if group.is_empty() {
                    continue;
                }
                let shift = run as usize % group.len();
                for arm in group.iter().cycle().skip(shift).take(group.len()) {
                    arms.push(ArmPlan {
                        run,
                        ..arm.clone()
                    });
                }
            }
        }
        arms
    }

    /// A digest of everything in the spec that changes what is measured
    ///
    /// The dataset's path is left out, since the dataset's own digest says what was loaded, and
    /// so is the mode, which a capture records beside it.
    #[must_use]
    pub fn digest(&self) -> String {
        use sha2::{Digest, Sha256};
        // canonical json: struct fields in declaration order, maps sorted
        let mut value = serde_json::to_value(self).expect("a spec is json");
        if let Some(object) = value.as_object_mut() {
            object.remove("dataset");
            object.remove("mode");
        }
        crate::read::hex(&Sha256::digest(value.to_string().as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::{BenchSpec, EventKind, Mix, Mode, Override};
    use std::collections::BTreeMap;

    /// The named mixes and custom weights parse, and nonsense does not
    #[test]
    fn mixes_parse_by_name_or_weight() {
        let rw50: Mix = "rw50".parse().unwrap();
        assert_eq!((rw50.read, rw50.insert), (50, 50));
        let custom: Mix = "insert:3,read:7".parse().unwrap();
        assert_eq!((custom.name.as_str(), custom.read, custom.insert), ("read7-insert3", 7, 3));
        // a custom mix writes back as weights and reads back as itself
        let written: String = custom.clone().into();
        assert_eq!(written.parse::<Mix>().unwrap(), custom);
        for bad in ["update:1", "read:x", "read:0", "both"] {
            assert!(bad.parse::<Mix>().is_err(), "{bad}");
        }
    }

    /// A spec of only a dataset takes every default, and an unknown field is refused
    #[test]
    fn a_short_spec_takes_the_defaults() {
        let spec: BenchSpec = serde_yaml::from_str("dataset: data\nmixes: [read100, rw50]\npreload: 1000\n").unwrap();
        assert_eq!(spec.mixes.len(), 2);
        assert_eq!(spec.bundles, vec![1, 16, 64]);
        assert_eq!(spec.preload, crate::feed::Preload::Rows(1000));
        assert!(spec.problems().is_empty(), "{:?}", spec.problems());
        assert!(serde_yaml::from_str::<BenchSpec>("dataset: data\nbundle: [1]\n").is_err());
        // a share round trips
        let spec: BenchSpec = serde_yaml::from_str("dataset: data\npreload: 25%\n").unwrap();
        let back: BenchSpec = serde_yaml::from_str(&serde_yaml::to_string(&spec).unwrap()).unwrap();
        assert_eq!(back, spec);
    }

    /// The matrix is every combination, steady arms first, rotated a step a run
    #[test]
    fn arms_expand_and_rotate() {
        let spec = BenchSpec {
            dataset: "data".into(),
            mixes: vec!["read100".parse().unwrap(), "insert100".parse().unwrap()],
            bundles: vec![1, 16],
            events: vec![EventKind::Kill, EventKind::None],
            runs: 2,
            spare: None,
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        assert_eq!(arms.len(), 16);
        let first: Vec<&str> = arms[..8].iter().map(|arm| arm.id.0.as_str()).collect();
        assert_eq!(
            first,
            vec![
                "read100/b1/none",
                "read100/b16/none",
                "insert100/b1/none",
                "insert100/b16/none",
                "read100/b1/kill",
                "read100/b16/kill",
                "insert100/b1/kill",
                "insert100/b16/kill",
            ]
        );
        // the second run starts each group one arm later
        assert_eq!(arms[8].id.0, "read100/b16/none");
        assert_eq!(arms[8].run, 1);
        assert_eq!(arms[12].id.0, "read100/b16/kill");
        // only an arm that writes or runs an event disturbs the cluster
        assert!(!arms[0].disturbs());
        assert!(arms[2].disturbs());
        assert!(arms[4].disturbs());
        // in flight defaults to four bundles
        assert_eq!(arms[1].in_flight, 64);
    }

    /// An override names its arms
    #[test]
    fn an_override_names_its_arms() {
        let spec = BenchSpec {
            dataset: "data".into(),
            mixes: vec!["read100".parse().unwrap()],
            bundles: vec![8],
            runs: 1,
            overrides: vec![Override {
                name: "small-memory".to_string(),
                set: BTreeMap::from([("resources.memory".to_string(), "1Gi".into())]),
            }],
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        assert_eq!(arms[0].id.0, "read100/b8/small-memory/none");
    }

    /// An attached cluster refuses anything that takes it down, and a move needs a spare
    #[test]
    fn an_attached_spec_refuses_disruption() {
        let spec = BenchSpec {
            dataset: "data".into(),
            mode: Mode::Attach,
            events: vec![EventKind::Kill, EventKind::Repair, EventKind::Rebalance],
            ..BenchSpec::default()
        };
        let problems = spec.problems().join("\n");
        assert!(problems.contains("kill takes a node down"), "{problems}");
        assert!(problems.contains("--spare"), "{problems}");
        assert!(!problems.contains("repair"), "{problems}");
    }

    /// The digest moves with what is measured and not with where the data lives
    #[test]
    fn the_digest_ignores_the_dataset_path() {
        let spec = BenchSpec {
            dataset: "one".into(),
            ..BenchSpec::default()
        };
        let moved = BenchSpec {
            dataset: "two".into(),
            ..spec.clone()
        };
        let longer = BenchSpec {
            duration: 61,
            ..spec.clone()
        };
        assert_eq!(spec.digest(), moved.digest());
        assert_ne!(spec.digest(), longer.digest());
    }
}
