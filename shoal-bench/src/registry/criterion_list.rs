//! Asking criterion what benchmarks it has
//!
//! The micro registry is discovered, never written down. A hardcoded list is exactly how a
//! benchmark gets added and then silently never captured, which is the failure this crate is
//! supposed to make impossible.
//!
//! `criterion` answers `--list` by walking its own registration code and printing one
//! `<full_id>: benchmark` line per benchmark, with a blank line between groups. It measures
//! nothing while doing so, which makes it cheap enough to be the source of truth - but it does
//! have to build the bench binary first, so the answer is cached.

use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::store::Store;

/// The schema version of the cached registry
const CACHE_VERSION: u32 = 1;

/// The file that declares every micro benchmark
///
/// The cache is keyed on this file because every benchmark id comes out of it: the group names,
/// the function names and the `SIZES` constant that parameterises them are all here. A benchmark
/// cannot be added, renamed or removed without changing it.
const BENCH_SOURCE: &str = "shoal/benches/partitions.rs";

/// What the cached list was valid for
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SourceKey {
    /// The size of the declaring file in bytes
    len: u64,
    /// Its modification time, in nanoseconds since the epoch
    modified_ns: u128,
}

/// A discovered list of micro benchmarks, and what it was discovered from
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedList {
    /// The schema version this cache was written with
    version: u32,
    /// What the declaring file looked like when the list was taken
    key: SourceKey,
    /// The benchmark ids, in the order criterion declares them
    ids: Vec<String>,
}

/// Reads the key the cache is validated against
///
/// # Arguments
///
/// * `store` - The artifact tree, which knows where the repository is
fn source_key(store: &Store) -> Result<SourceKey> {
    let path = store.root().join(BENCH_SOURCE);
    // stat the declaring file, which must exist for there to be any micro benchmarks at all
    let meta = std::fs::metadata(&path)
        .with_context(|| format!("stat {} to key the benchmark list cache", path.display()))?;
    // its modification time, as a plain integer so it can be compared after a round trip
    let modified_ns = meta
        .modified()
        .ok()
        .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    Ok(SourceKey {
        len: meta.len(),
        modified_ns,
    })
}

/// Where the discovered list is cached
///
/// Under `target/` rather than beside the artifacts: it is derived from the tree, is worthless on
/// another machine, and must not survive a `cargo clean` that also throws away the bench binary
/// it describes.
///
/// # Arguments
///
/// * `store` - The artifact tree, which knows where the repository is
fn cache_path(store: &Store) -> std::path::PathBuf {
    store.root().join("target/shoal-bench/registry.json")
}

/// Every micro benchmark criterion knows about, in declaration order
///
/// # Arguments
///
/// * `store` - The artifact tree, which knows where the repository is
/// * `refresh` - Whether to rediscover even if the cache still looks valid
pub fn discover(store: &Store, refresh: bool) -> Result<Vec<String>> {
    let key = source_key(store)?;
    let cache = cache_path(store);
    // use the cache when it was taken from the same declaring file, unless told not to
    if !refresh && cache.is_file() {
        // a cache that fails to parse is a cache to replace, not an error to report - it is
        // derived data and rebuilding it costs one build
        if let Ok(cached) = crate::store::read_json::<CachedList>(&cache)
            && cached.version == CACHE_VERSION
            && cached.key == key
        {
            return Ok(cached.ids);
        }
    }
    // otherwise ask criterion, and remember what it said
    let ids = ask_criterion(store)?;
    let record = CachedList {
        version: CACHE_VERSION,
        key,
        ids: ids.clone(),
    };
    // a cache that cannot be written is not worth failing the command over - the list is already
    // in hand, and the only cost is discovering it again next time
    let _ = crate::store::write_json(&cache, &record);
    Ok(ids)
}

/// Runs criterion's `--list` and parses what it prints
///
/// # Arguments
///
/// * `store` - The artifact tree, which knows where the repository is
fn ask_criterion(store: &Store) -> Result<Vec<String>> {
    // the bench feature is what exposes the crate private internals the benchmarks reach into,
    // so listing them needs the same feature running them does
    let mut command = Command::new("cargo");
    command
        .current_dir(store.root())
        .args([
            "bench",
            "-p",
            "shoal",
            "--features",
            "bench",
            "--bench",
            "partitions",
            "--",
            "--list",
        ])
        // the list comes back on stdout; cargo's build progress goes to stderr and is left
        // attached, since discovering the list can mean waiting for a release build
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let output = command
        .output()
        .context("running `cargo bench -- --list` to discover the micro benchmarks")?;
    // a failed listing is not an empty list of benchmarks, and must not be treated as one
    if !output.status.success() {
        bail!(
            "`cargo bench -- --list` failed with {}; the micro benchmarks could not be discovered",
            output.status
        );
    }
    let text = String::from_utf8(output.stdout)
        .context("`cargo bench -- --list` printed something that is not utf-8")?;
    let ids = parse_list(&text);
    // criterion printing nothing means the bench target registered no benchmarks, which is a
    // real problem rather than a tree with no micro layer
    if ids.is_empty() {
        bail!("`cargo bench -- --list` listed no benchmarks at all");
    }
    Ok(ids)
}

/// Parses criterion's `--list` output into benchmark ids
///
/// Every benchmark is one `<full_id>: benchmark` line. Groups are separated by blank lines, and
/// criterion prints other kinds of entry with a different suffix, so the suffix is matched rather
/// than assumed.
///
/// # Arguments
///
/// * `text` - What criterion printed on stdout
pub fn parse_list(text: &str) -> Vec<String> {
    // keep the lines that name a benchmark, in the order they were printed
    text.lines()
        .filter_map(|line| line.trim_end().strip_suffix(": benchmark"))
        .map(|id| id.to_string())
        .collect()
}

/// Reads a cached list without discovering one, if the cache is present and valid
///
/// Used by commands that can work from whatever has already been discovered and should not
/// trigger a build - `status` and `render` in particular, which must stay fast enough to run in a
/// loop.
///
/// # Arguments
///
/// * `store` - The artifact tree, which knows where the repository is
pub fn cached(store: &Store) -> Option<Vec<String>> {
    // the cache is only usable if it matches the declaring file it was taken from
    let key = source_key(store).ok()?;
    let cached: CachedList = crate::store::read_json(&cache_path(store)).ok()?;
    (cached.version == CACHE_VERSION && cached.key == key).then_some(cached.ids)
}

/// Whether a path is the file the micro registry is discovered from
///
/// # Arguments
///
/// * `path` - The path to check, relative to the repository root
pub fn is_bench_source(path: &Path) -> bool {
    // compared as a relative path, which is how the source manifest names files
    path == Path::new(BENCH_SOURCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The list criterion actually prints parses into exactly the ids it named
    ///
    /// This is real output, blank group separators and all.
    #[test]
    fn the_real_list_output_parses() {
        let text = "partition_sorted/insert/16: benchmark\n\
                    partition_sorted/insert/256: benchmark\n\
                    \n\
                    partition_sorted/get_key/16: benchmark\n\
                    \n\
                    seek_bytes/new/one_key: benchmark\n";
        assert_eq!(
            parse_list(text),
            vec![
                "partition_sorted/insert/16",
                "partition_sorted/insert/256",
                "partition_sorted/get_key/16",
                "seek_bytes/new/one_key",
            ]
        );
    }

    /// An entry criterion does not call a benchmark is not taken for one
    #[test]
    fn only_benchmarks_are_taken_from_the_list() {
        let text = "partition_sorted/insert/16: benchmark\n\
                    some_group: group\n\
                    partition_sorted/insert/256: benchmark\n";
        assert_eq!(
            parse_list(text),
            vec!["partition_sorted/insert/16", "partition_sorted/insert/256"]
        );
    }

    /// Nothing printed is no benchmarks, not a panic
    #[test]
    fn an_empty_list_is_empty() {
        assert!(parse_list("").is_empty());
        assert!(parse_list("\n\n\n").is_empty());
    }
}
