//! What benchmarks exist, and which of them a filter selects
//!
//! The harness has four measurement layers that share nothing: criterion drives the micro
//! benchmarks in process, the macro layer is a whole `tmdb` run against a live server, and the
//! hotpath and stage layers are separately built instrumented runs of that same workload. This
//! module is what makes them one list that can be printed and filtered the way `cargo test`
//! filters tests.

pub mod criterion_list;

use std::fmt;
use std::str::FromStr;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::cli::{DEFAULT_RUNS, Format, ListArgs, Selection};
use crate::store::Store;

/// One of the four measurement layers
///
/// The declaration order is the order these are presented in, everywhere: it runs from the
/// cheapest and most repeatable measurement to the most expensive and least, which is also the
/// order they should be read in when judging a change.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, clap::ValueEnum,
)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Layer {
    /// Criterion benchmarks over the partition internals, CPU bound and repeatable
    Micro,
    /// The `tmdb` example end to end against a live server
    Macro,
    /// A separately built instrumented run, attributing time to scopes
    Hotpath,
    /// A separately built instrumented run, attributing one query's latency to stages
    Stages,
}

impl Layer {
    /// Every layer, in presentation order
    pub const ALL: [Layer; 4] = [Layer::Micro, Layer::Macro, Layer::Hotpath, Layer::Stages];

    /// The lowercase name of this layer, which is also the key it is stored under
    pub fn as_str(&self) -> &'static str {
        // these strings are the file name infix of every artifact, so they are not cosmetic
        match self {
            Layer::Micro => "micro",
            Layer::Macro => "macro",
            Layer::Hotpath => "hotpath",
            Layer::Stages => "stages",
        }
    }

    /// Whether this layer's numbers come from an instrumented build
    ///
    /// An instrumented build stamps extra timestamps on the query path, so its wall clock is not
    /// comparable to the uninstrumented build's. These layers attribute time; they never supply a
    /// latency or a throughput.
    pub fn is_instrumented(&self) -> bool {
        // both profiling layers are built with a feature that adds work to the measured path
        matches!(self, Layer::Hotpath | Layer::Stages)
    }
}

impl fmt::Display for Layer {
    /// Writes this layer's lowercase name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write into
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // the stored name is the displayed name, so there is only one spelling to remember
        f.write_str(self.as_str())
    }
}

impl FromStr for Layer {
    type Err = String;

    /// Parses a layer from its lowercase name
    ///
    /// # Arguments
    ///
    /// * `raw` - The name to parse
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // match the same strings `as_str` writes, and nothing else
        match raw {
            "micro" => Ok(Layer::Micro),
            "macro" => Ok(Layer::Macro),
            "hotpath" => Ok(Layer::Hotpath),
            "stages" => Ok(Layer::Stages),
            other => Err(format!(
                "unknown layer '{other}', expected one of micro, macro, hotpath, stages"
            )),
        }
    }
}

/// One unit of execution: something `list` prints and a filter can select
///
/// This is deliberately not the same thing as a unit of *comparison*. One micro benchmark is one
/// comparable metric, but one macro benchmark is roughly fifteen of them - a wall clock, a
/// throughput, and a percentile for each of two operations. Conflating the two is why the shell
/// scripts this replaces could only ever compare the micro layer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BenchId {
    /// Which layer runs this benchmark
    pub layer: Layer,
    /// The identifier this benchmark is known by
    ///
    /// For the micro layer this is criterion's `full_id` verbatim, such as
    /// `partition_sorted/get_key/4096`. That is load bearing: it is the key the artifact is
    /// written under and the key a comparison joins on, so it must stay byte identical to what
    /// criterion produces or every baseline captured before this tool stops matching.
    ///
    /// The other three layers use a `<layer>/<workload>` form, such as `macro/tmdb`. No criterion
    /// id begins with one of those three prefixes, so one flat namespace is unambiguous.
    pub id: String,
}

impl BenchId {
    /// Creates an identifier for a benchmark
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer that runs it
    /// * `id` - What it is known by
    pub fn new<S: Into<String>>(layer: Layer, id: S) -> Self {
        BenchId {
            layer,
            id: id.into(),
        }
    }
}

impl fmt::Display for BenchId {
    /// Writes this benchmark's identifier
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write into
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // the id already carries its own layer prefix for everything but the micro layer, whose
        // ids must stay exactly what criterion calls them
        f.write_str(&self.id)
    }
}

/// Which workloads the instrumented layers run
///
/// `hotpath` emits one profile when a process exits, so attributing a profile to a workload means
/// one instrumented run per workload. Running every workload under both instrumented layers would
/// make attribution cost more than the rest of a capture put together, for profiles that mostly
/// repeat each other, so a workload opts in with `Workload::profiles`.
///
/// This list is the runner's copy of that decision, for the same reason
/// [`crate::workload_ids::IDS`] exists: the runner has to know it without the engine linked. A test
/// under the `workloads` feature asserts the two agree.
pub const PROFILED_WORKLOADS: &[&str] = &["macro/insert_unsorted"];

/// The identifier an instrumented layer runs a workload under
///
/// The workload identifiers are `macro/...` because that is the layer they produce numbers for.
/// The same workload run under `hotpath` is a different benchmark - a different build, measuring
/// something else, and never comparable to the macro one - so it gets its own identifier rather
/// than sharing the workload's.
///
/// # Arguments
///
/// * `layer` - The instrumented layer running it
/// * `workload` - The workload being run
///
/// # Examples
///
/// ```
/// use shoal_bench::registry::{instrumented_id, Layer};
///
/// assert_eq!(instrumented_id(Layer::Hotpath, "macro/insert_unsorted"), "hotpath/insert_unsorted");
/// ```
pub fn instrumented_id(layer: Layer, workload: &str) -> String {
    // swap the layer prefix, keeping the workload's own name so the two are recognisably the same
    // workload seen two ways
    let name = workload.strip_prefix("macro/").unwrap_or(workload);
    format!("{}/{name}", layer.as_str())
}

/// Every benchmark this harness can run
///
/// The micro entries are discovered from criterion, because criterion owns that list and asking it
/// is the only way to be sure. The workloads are not discovered: they are compiled into this crate,
/// so [`crate::workload_ids::IDS`] is the list, and there is nothing to go and ask.
#[derive(Debug, Clone)]
pub struct Registry {
    /// Every benchmark, micro first in criterion's declaration order
    entries: Vec<BenchId>,
}

impl Registry {
    /// Builds the registry, discovering the micro benchmarks from criterion
    ///
    /// # Arguments
    ///
    /// * `store` - The artifact tree, which knows where the repository is
    /// * `refresh` - Whether to rediscover the micro benchmarks even if the cache still looks valid
    pub fn load(store: &Store, refresh: bool) -> Result<Registry> {
        // ask criterion what it has, which may mean building the bench target first
        let micro = criterion_list::discover(store, refresh)?;
        Ok(Registry::from_micro_ids(micro))
    }

    /// Builds the registry from whatever micro benchmarks have already been discovered
    ///
    /// Returns `None` when nothing has been discovered yet. Used by the commands that must not
    /// trigger a build, which is every command that does not run a benchmark.
    ///
    /// # Arguments
    ///
    /// * `store` - The artifact tree, which knows where the repository is
    pub fn cached(store: &Store) -> Option<Registry> {
        // only usable if the cache still matches the file the benchmarks are declared in
        criterion_list::cached(store).map(Registry::from_micro_ids)
    }

    /// Assembles a registry from a list of micro benchmark ids
    ///
    /// # Arguments
    ///
    /// * `micro` - The criterion ids, in declaration order
    fn from_micro_ids(micro: Vec<String>) -> Registry {
        // the micro layer first, in the order criterion declares it, so `list` reads like the
        // benchmark file does
        let mut entries: Vec<BenchId> = micro
            .into_iter()
            .map(|id| BenchId::new(Layer::Micro, id))
            .collect();
        // then every workload, which is the macro layer
        for id in crate::workload_ids::IDS {
            entries.push(BenchId::new(Layer::Macro, *id));
        }
        // then the workloads that opted into attribution, once per instrumented layer. these are
        // separate entries because they are separate builds measuring separate things - selecting
        // `hotpath/insert_unsorted` must not also select the macro run of the same workload
        for layer in [Layer::Hotpath, Layer::Stages] {
            for id in PROFILED_WORKLOADS {
                entries.push(BenchId::new(layer, instrumented_id(layer, id)));
            }
        }
        Registry { entries }
    }

    /// Every workload the macro layer would run, in the order they run
    pub fn workload_ids(&self) -> Vec<&str> {
        // the layer is what distinguishes them, not the shape of the id
        self.entries
            .iter()
            .filter(|entry| entry.layer == Layer::Macro)
            .map(|entry| entry.id.as_str())
            .collect()
    }

    /// Every workload an instrumented layer would run, as the workload's own identifier
    ///
    /// The entries carry the instrumented identifier, which is what a filter selects on; this maps
    /// back to the workload the runner has to actually invoke.
    ///
    /// # Arguments
    ///
    /// * `layer` - Which instrumented layer to read
    pub fn instrumented_workloads(&self, layer: Layer) -> Vec<&'static str> {
        // keep the declared workloads whose instrumented identifier was selected
        self.entries
            .iter()
            .filter(|entry| entry.layer == layer)
            .filter_map(|entry| {
                PROFILED_WORKLOADS
                    .iter()
                    .find(|id| instrumented_id(layer, id) == entry.id)
                    .copied()
            })
            .collect()
    }

    /// Every benchmark in the registry
    pub fn entries(&self) -> &[BenchId] {
        &self.entries
    }

    /// How many benchmarks the registry holds
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry holds no benchmarks at all
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every micro benchmark, in declaration order
    pub fn micro_ids(&self) -> Vec<&str> {
        // the layer is what distinguishes them, not the shape of the id
        self.entries
            .iter()
            .filter(|entry| entry.layer == Layer::Micro)
            .map(|entry| entry.id.as_str())
            .collect()
    }

    /// The benchmarks a selection picks out
    ///
    /// Filtering follows `cargo test`: a positional argument is a substring of a benchmark's
    /// identifier, and any of them matching selects it. `--exact` switches to equality, and
    /// `--layer` intersects with whatever the substrings picked.
    ///
    /// A selection that matches nothing is an error rather than an empty set. A capture that
    /// quietly measured no benchmarks produces the same silence as one that measured all of them,
    /// and only one of those is what anybody meant.
    ///
    /// # Arguments
    ///
    /// * `selection` - What the caller asked for
    pub fn select(&self, selection: &Selection) -> Result<Vec<BenchId>> {
        // a name that is not a group is a mistake in the request rather than a set that happens to
        // be empty, and it is worth saying so before anything is filtered
        if let Some(message) = crate::groups::unknown(&selection.groups) {
            bail!("{message}");
        }
        // keep the entries that pass the layer restriction, the groups and the substring filters
        let picked: Vec<BenchId> = self
            .entries
            .iter()
            .filter(|entry| selection.layers.is_empty() || selection.layers.contains(&entry.layer))
            .filter(|entry| crate::groups::matches_groups(entry, &selection.groups))
            .filter(|entry| matches_filters(&entry.id, &selection.filters, selection.exact))
            .cloned()
            .collect();
        // an empty selection is reported, with a guess at what was meant
        if picked.is_empty() {
            bail!("{}", self.no_match_message(selection));
        }
        Ok(picked)
    }

    /// Explains a selection that matched nothing, and suggests what was probably meant
    ///
    /// # Arguments
    ///
    /// * `selection` - What the caller asked for
    fn no_match_message(&self, selection: &Selection) -> String {
        // describe what was asked for
        let mut message = if selection.filters.is_empty() {
            "no benchmarks in".to_string()
        } else {
            format!("no benchmark matches {:?} in", selection.filters)
        };
        // and which groups it was narrowed to, since a group is the likelier reason a filter that
        // looks right selected nothing
        if !selection.groups.is_empty() {
            message.push_str(&format!(" group(s) {} of", selection.groups.join(", ")));
        }
        // and which layers it was asked for in
        if selection.layers.is_empty() {
            message.push_str(" the registry");
        } else {
            let named: Vec<&str> = selection
                .layers
                .iter()
                .map(|layer| layer.as_str())
                .collect();
            message.push_str(&format!(" layer(s) {}", named.join(", ")));
        }
        // then offer the closest ids there are, so a typo is one line away from being fixed
        if let Some(filter) = selection.filters.first() {
            let near = self.nearest(filter, 3);
            if !near.is_empty() {
                message.push_str(&format!("\n  did you mean: {}", near.join(", ")));
            }
        }
        message
    }

    /// The benchmark ids most like a filter, by how much of a prefix they share with it
    ///
    /// # Arguments
    ///
    /// * `filter` - What the caller typed
    /// * `count` - How many suggestions to return
    fn nearest(&self, filter: &str, count: usize) -> Vec<&str> {
        // score every id by how far it agrees with what was typed
        let mut scored: Vec<(usize, &str)> = self
            .entries
            .iter()
            .map(|entry| (shared_prefix(&entry.id, filter), entry.id.as_str()))
            .collect();
        // best score first, breaking ties on the id so the suggestion is stable
        scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(right.1)));
        // an id that shares nothing with what was typed is not a suggestion, it is noise
        scored
            .into_iter()
            .filter(|(score, _)| *score > 0)
            .take(count)
            .map(|(_, id)| id)
            .collect()
    }
}

/// Whether an identifier passes a set of filters
///
/// # Arguments
///
/// * `id` - The identifier to test
/// * `filters` - The filters to test it against, where no filters accepts everything
/// * `exact` - Whether a filter must equal the identifier rather than appear inside it
pub fn matches_filters(id: &str, filters: &[String], exact: bool) -> bool {
    // no filter means everything, the way `cargo test` with no argument runs every test
    if filters.is_empty() {
        return true;
    }
    // otherwise any one filter matching is enough
    filters.iter().any(|filter| {
        if exact {
            id == filter
        } else {
            id.contains(filter.as_str())
        }
    })
}

/// How many leading characters two strings share
///
/// # Arguments
///
/// * `left` - The first string
/// * `right` - The second string
fn shared_prefix(left: &str, right: &str) -> usize {
    // walk both together until they disagree, counting bytes of agreement
    left.bytes()
        .zip(right.bytes())
        .take_while(|(a, b)| a == b)
        .count()
}

/// Builds the filter argument criterion should be run with
///
/// Returns `None` when the selection is every micro benchmark there is, so that a full capture
/// invokes criterion exactly the way `scripts/bench.sh` did.
///
/// The filter is built from the *resolved* id set rather than forwarded from what the caller
/// typed. Criterion's positional argument is a regular expression, and this tool's filters are
/// substrings; forwarding one as the other would make a filter containing a metacharacter mean
/// two different things in the two halves of the same command. Building an anchored alternation
/// of exact ids also means the capture knows what it selected before it runs, which is what lets
/// a partial capture record that it was one.
///
/// # Arguments
///
/// * `selected` - The micro ids the selection resolved to
/// * `all` - Every micro id the registry holds
pub fn criterion_filter(selected: &[&str], all: &[&str]) -> Option<String> {
    // selecting everything is not a filter
    if selected.len() == all.len() {
        return None;
    }
    // an alternation of every selected id, anchored so a shorter id cannot match inside a longer
    // one - `insert/16` is a prefix of nothing here, but `get_key/16` and `get_key/1024` differ
    // only in a suffix and an unanchored pattern would confuse them
    let body = selected
        .iter()
        .map(|id| escape_regex(id))
        .collect::<Vec<String>>()
        .join("|");
    Some(format!("^(?:{body})$"))
}

/// Escapes a string so a regular expression matches it literally
///
/// This is `regex::escape` for the set of characters the `regex` crate treats as meta. It is
/// hand rolled because the `regex` crate is not otherwise needed here - this tool builds patterns
/// and never parses one - and a dependency that exists to escape eighteen characters is not worth
/// its build time.
///
/// # Arguments
///
/// * `raw` - The literal to escape
///
/// # Examples
///
/// ```
/// use shoal_bench::registry::escape_regex;
///
/// assert_eq!(escape_regex("partition_sorted/get_key/16"), "partition_sorted/get_key/16");
/// assert_eq!(escape_regex("a.b+c"), r"a\.b\+c");
/// ```
pub fn escape_regex(raw: &str) -> String {
    // exactly the set `regex::is_meta_character` reports. escaping anything outside it risks
    // producing an escape sequence the regex crate rejects rather than reads literally.
    const META: [char; 18] = [
        '\\', '.', '+', '*', '?', '(', ')', '|', '[', ']', '{', '}', '^', '$', '#', '&', '-', '~',
    ];
    // copy the literal through, prefixing a backslash to anything that would otherwise be syntax
    let mut escaped = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if META.contains(&ch) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

/// Runs `shoal-bench list`
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_list(store: &Store, args: &ListArgs) -> Result<i32> {
    // discovering the list can mean building the bench target, which is the price of not
    // hardcoding it
    let registry = Registry::load(store, args.refresh)?;
    // the group listing is about the sets rather than about their members, so it prints instead of
    // the benchmarks rather than beside them
    if args.groups {
        return crate::groups::run_groups(store, registry.entries(), DEFAULT_RUNS, args.format);
    }
    let picked = registry.select(&args.selection)?;
    // print it in whichever shape was asked for
    match args.format {
        Format::Json => {
            // machine readable, for anything driving this tool rather than reading it
            let body = serde_json::to_string_pretty(&picked)?;
            println!("{body}");
        }
        Format::Text | Format::Markdown => {
            // one benchmark per line, layer first, which is how a filter is usually being checked
            for entry in &picked {
                println!("{:<8} {}", entry.layer.as_str(), entry.id);
            }
            eprintln!(
                "\n{} of {} benchmarks selected",
                picked.len(),
                registry.len()
            );
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a registry over a handful of representative ids
    fn registry() -> Registry {
        Registry::from_micro_ids(
            [
                "partition_sorted/insert/16",
                "partition_sorted/insert/4096",
                "partition_sorted/get_key/16",
                "partition_sorted/get_key/1024",
                "partition_sorted/maybe_loaded/get_key/16",
                "seek_bytes/new/one_key",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
        )
    }

    /// Builds a selection from filters
    ///
    /// # Arguments
    ///
    /// * `filters` - The substrings to match
    fn filters(filters: &[&str]) -> Selection {
        Selection {
            filters: filters.iter().map(|f| f.to_string()).collect(),
            exact: false,
            layers: Vec::new(),
            groups: Vec::new(),
        }
    }

    /// The registry is the discovered micro benchmarks plus every workload, per layer
    #[test]
    fn the_registry_holds_every_layer() {
        let registry = registry();
        assert_eq!(registry.micro_ids().len(), 6);
        // every declared workload is in the macro layer and is addressable
        assert_eq!(registry.workload_ids(), crate::workload_ids::IDS.to_vec());
        // and every workload that opted into attribution is there once per instrumented layer
        for layer in [Layer::Hotpath, Layer::Stages] {
            for id in PROFILED_WORKLOADS {
                let instrumented = instrumented_id(layer, id);
                assert!(
                    registry.entries().iter().any(|entry| entry.id == instrumented),
                    "{instrumented} is missing from the registry"
                );
            }
        }
        assert_eq!(
            registry.len(),
            6 + crate::workload_ids::IDS.len() + PROFILED_WORKLOADS.len() * 2
        );
    }

    /// An instrumented entry names its own layer rather than sharing the workload's id
    ///
    /// Selecting `hotpath/insert_unsorted` must not also select the macro run of the same
    /// workload. They are different builds measuring different things, and a capture that ran one
    /// when the other was asked for would report an instrumented wall clock as a latency.
    #[test]
    fn an_instrumented_entry_is_addressable_on_its_own() {
        let registry = registry();
        let selection = Selection {
            filters: vec!["hotpath/insert_unsorted".to_string()],
            exact: true,
            layers: Vec::new(),
            groups: Vec::new(),
        };
        let picked = registry.select(&selection).expect("selects");
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].layer, Layer::Hotpath);
        // and it maps back to the workload the runner has to invoke
        assert_eq!(
            registry.instrumented_workloads(Layer::Hotpath),
            vec!["macro/insert_unsorted"]
        );
    }

    /// No filter selects everything, the way `cargo test` with no argument runs everything
    #[test]
    fn no_filter_selects_everything() {
        let registry = registry();
        let picked = registry.select(&Selection::default()).expect("selects");
        assert_eq!(picked.len(), registry.len());
    }

    /// A substring selects everything it appears in, across layers
    #[test]
    fn a_substring_selects_what_contains_it() {
        let registry = registry();
        let picked = registry.select(&filters(&["get_key"])).expect("selects");
        assert_eq!(picked.len(), 3);
        // including the maybe_loaded variant, which contains the substring further along
        assert!(
            picked
                .iter()
                .any(|entry| entry.id == "partition_sorted/maybe_loaded/get_key/16")
        );
    }

    /// Several filters are combined with or, not and
    #[test]
    fn filters_are_combined_with_or() {
        let registry = registry();
        let picked = registry
            .select(&filters(&["seek_bytes"]))
            .expect("selects");
        assert_eq!(picked.len(), 1);
    }

    /// An exact filter selects one benchmark and never a longer one containing it
    #[test]
    fn an_exact_filter_selects_one() {
        let registry = registry();
        let selection = Selection {
            filters: vec!["partition_sorted/get_key/16".to_string()],
            exact: true,
            layers: Vec::new(),
            groups: Vec::new(),
        };
        let picked = registry.select(&selection).expect("selects");
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].id, "partition_sorted/get_key/16");
    }

    /// A layer restriction intersects with whatever the substrings picked
    #[test]
    fn a_layer_restriction_intersects() {
        let registry = registry();
        let selection = Selection {
            filters: Vec::new(),
            exact: false,
            layers: vec![Layer::Micro],
            groups: Vec::new(),
        };
        let picked = registry.select(&selection).expect("selects");
        assert_eq!(picked.len(), 6);
        assert!(picked.iter().all(|entry| entry.layer == Layer::Micro));
    }

    /// A group restriction intersects with the layers and the substrings, rather than replacing them
    #[test]
    fn a_group_restriction_intersects() {
        let registry = registry();
        // the group alone selects every configuration arm
        let by_group = Selection {
            filters: Vec::new(),
            exact: false,
            layers: Vec::new(),
            groups: vec!["conf/storage".to_string()],
        };
        let picked = registry.select(&by_group).expect("selects");
        assert!(picked.len() > 1);
        assert!(picked.iter().all(|entry| entry.id.starts_with("macro/conf/storage/")));
        // and narrowing it with a substring intersects rather than widening it back out
        let narrowed = Selection {
            filters: vec!["durability".to_string()],
            exact: false,
            layers: Vec::new(),
            groups: vec!["conf/storage".to_string()],
        };
        let picked = registry.select(&narrowed).expect("selects");
        assert!(
            picked
                .iter()
                .all(|entry| entry.id.contains("durability")),
            "{picked:?}"
        );
        assert_eq!(picked.len(), 2);
    }

    /// Two groups are combined with or, the way two filters are
    #[test]
    fn two_groups_are_combined_with_or() {
        let registry = registry();
        let selection = Selection {
            filters: Vec::new(),
            exact: false,
            layers: Vec::new(),
            groups: vec!["conf/storage".to_string(), "fanout".to_string()],
        };
        let picked = registry.select(&selection).expect("selects");
        assert!(picked.iter().any(|entry| entry.id.starts_with("macro/conf/storage/")));
        assert!(picked.iter().any(|entry| entry.id.starts_with("macro/fanout/")));
    }

    /// A group that does not exist is an error, and names the ones that do
    ///
    /// Without this a mistyped group falls through to selecting everything, and a capture that
    /// measured the whole registry when half an hour of it was asked for looks like a success.
    #[test]
    fn an_unknown_group_is_an_error() {
        let registry = registry();
        let selection = Selection {
            filters: Vec::new(),
            exact: false,
            layers: Vec::new(),
            groups: vec!["conf/storag".to_string()],
        };
        let err = registry
            .select(&selection)
            .expect_err("an unknown group is an error");
        let message = format!("{err}");
        assert!(message.contains("unknown group"), "{message}");
        assert!(message.contains("conf/storage"), "{message}");
    }

    /// A filter that matches nothing fails, and says what it might have meant
    #[test]
    fn an_unmatched_filter_is_an_error_with_a_suggestion() {
        let registry = registry();
        let err = registry
            .select(&filters(&["partition_sorted/get_kee"]))
            .expect_err("an unmatched filter is an error");
        let message = format!("{err}");
        assert!(message.contains("no benchmark matches"), "{message}");
        assert!(message.contains("did you mean"), "{message}");
        assert!(message.contains("partition_sorted/get_key/"), "{message}");
    }

    /// A filter with nothing in common with anything gets no misleading suggestion
    #[test]
    fn a_wholly_unrelated_filter_gets_no_suggestion() {
        let registry = registry();
        let err = registry
            .select(&filters(&["zzz"]))
            .expect_err("an unmatched filter is an error");
        assert!(!format!("{err}").contains("did you mean"));
    }

    /// Selecting everything runs criterion the way a full capture always has
    #[test]
    fn a_full_selection_passes_no_filter() {
        let all = ["a/1", "a/2", "b/1"];
        assert_eq!(criterion_filter(&all, &all), None);
    }

    /// A partial selection is passed as an anchored alternation of exact ids
    #[test]
    fn a_partial_selection_becomes_an_anchored_alternation() {
        let all = ["a/1", "a/2", "b/1"];
        assert_eq!(
            criterion_filter(&["a/1", "b/1"], &all),
            Some("^(?:a/1|b/1)$".to_string())
        );
    }

    /// An id carrying regex syntax is escaped rather than interpreted
    #[test]
    fn a_metacharacter_in_an_id_is_escaped() {
        let all = ["plain", "get(all)", "x+y"];
        assert_eq!(
            criterion_filter(&["get(all)", "x+y"], &all),
            Some(r"^(?:get\(all\)|x\+y)$".to_string())
        );
    }

    /// No real benchmark id needs escaping, so a full capture's filter is the ids verbatim
    ///
    /// The 59 ids in the trailing baseline are the real set. If a benchmark is ever named with a
    /// metacharacter this stops holding, which is worth knowing about.
    #[test]
    fn the_real_ids_need_no_escaping() {
        let store = Store::new(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("shoal-bench has a parent"),
        );
        let (_, trailing) = store
            .resolve_micro(crate::store::TRAILING_BASELINE)
            .expect("the trailing baseline resolves");
        // every committed id survives escaping unchanged
        for id in trailing.benchmarks.keys() {
            assert_eq!(
                &escape_regex(id),
                id,
                "{id} contains a regex metacharacter, so the anchored alternation now depends on \
                 the escaper being right"
            );
        }
    }
}
