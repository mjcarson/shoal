//! Resolving the server configuration one workload runs against
//!
//! Every workload starts from the same committed `shoal.yml`, which is what makes their numbers
//! comparable to each other, and then states the few things it actually depends on. A read
//! workload that has to reach disk needs a memory limit low enough to force eviction; a fanout
//! curve needs a fixed shard count so partition placement does not move underneath it.
//!
//! # Two things this module exists to prevent
//!
//! **A workload meeting another workload's data.** `StorageMeta::claim` refuses to start a
//! directory that was written by a different shard count, so a one shard workload and a twelve
//! shard workload sharing a storage root is a hard failure partway through a capture. Every
//! workload gets its own subdirectory, named from its identifier, which makes that structurally
//! impossible rather than something to remember.
//!
//! **A configuration change that nothing recorded.** The resolved settings go into the artifact,
//! including a digest over the whole of it, so a comparison across a configuration change is
//! visibly invalid instead of silently so.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use shoal::Conf;
use shoal_core::server::conf::TraceLevel;

use crate::model::macro_layer::ConfFacts;
use crate::workloads::workload::ConfOverrides;

/// Turns a workload identifier into a directory name
///
/// The identifier is the only input, so the mapping is total and stable: two workloads cannot
/// collide unless their identifiers already did, and a workload's directory does not move when
/// something unrelated about it changes.
///
/// # Arguments
///
/// * `id` - The workload identifier to derive a name from
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::harness::conf::slug;
///
/// assert_eq!(slug("macro/insert_unsorted"), "macro-insert_unsorted");
/// assert_eq!(slug("macro/fanout/resident/16"), "macro-fanout-resident-16");
/// ```
pub fn slug(id: &str) -> String {
    // only the separator needs replacing - every other character an id may carry is already a
    // legal path component, and the ids are checked for their prefix by `workload_ids`
    id.replace('/', "-")
}

/// Reads the base configuration and applies a workload's overrides to it
///
/// # Arguments
///
/// * `base` - The committed config file every workload starts from
/// * `id` - The workload being configured, which names its storage subdirectory
/// * `overrides` - What this workload changes about the base
/// * `port` - The port to bind, which must differ from any other server running at the same time
pub fn resolve(base: &Path, id: &str, overrides: &ConfOverrides, port: u16) -> Result<Conf> {
    // start from the file every workload shares, so a change to it moves all of them together
    let mut conf = Conf::from_file(
        base.to_str()
            .with_context(|| format!("config path {} is not valid utf8", base.display()))?,
    )
    .with_context(|| format!("failed to load {}", base.display()))?;
    // give this workload its own storage, which is what keeps `StorageMeta::claim` out of the way
    let subdir = slug(id);
    let latency = conf.storage.default.filesystem.latency_sensitive.path.join(&subdir);
    let throughput = conf
        .storage
        .default
        .filesystem
        .throughput_sensitive
        .path
        .join(&subdir);
    conf.storage.default.filesystem.latency_sensitive.path = latency;
    conf.storage.default.filesystem.throughput_sensitive.path = throughput;
    // bind somewhere nothing else is, since several workloads run in one capture
    conf.networking.port = port;
    // a workload's own server is not the thing being observed, and an Info level log per query
    // would be
    conf.tracing.level = TraceLevel::Warn;
    // then whatever this workload actually depends on
    if let Some(shards) = overrides.shards {
        conf.resources.cores = Some(shards);
    }
    if let Some(memory) = &overrides.memory {
        // through the builder rather than the field, because the field is a byte count and the
        // override is written the way `shoal.yml` writes it
        conf.resources = conf
            .resources
            .memory(memory.as_str())
            .map_err(|error| anyhow::anyhow!("{memory} is not a memory size: {error:?}"))?;
    }
    Ok(conf)
}

/// Summarizes a configuration into the facts the artifact records
///
/// # Arguments
///
/// * `conf` - The configuration a server was actually started with
pub fn facts(conf: &Conf) -> ConfFacts {
    // the durability barrier lives on the latency sensitive writer, which is what a write waits on
    let durability = match conf.storage.default.filesystem.latency_sensitive.durability {
        shoal_core::server::tables::storage::fs::conf::Durability::Fsync => "fsync",
        shoal_core::server::tables::storage::fs::conf::Durability::Async => "async",
    };
    ConfFacts {
        // an unset core count means every online core, which is not a number this can name
        shards: conf.resources.cores.unwrap_or(0) as u64,
        memory: binary_size(conf.resources.memory as u64),
        durability: durability.to_string(),
        digest: digest(conf),
    }
}

/// Renders a byte count the way `shoal.yml` writes one
///
/// The resolved configuration holds a byte count, and a byte count is what ends up in a rendered
/// table where nobody can tell 4294967296 from 4194304000 at a glance. This puts it back into the
/// units it was written in.
///
/// # Arguments
///
/// * `bytes` - The byte count to render
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::harness::conf::binary_size;
///
/// assert_eq!(binary_size(4 << 30), "4Gi");
/// assert_eq!(binary_size(100 << 20), "100Mi");
/// // anything that is not a whole number of units stays exact rather than being rounded
/// assert_eq!(binary_size(1), "1");
/// ```
pub fn binary_size(bytes: u64) -> String {
    // largest unit first, so 4 GiB reads as 4Gi rather than 4096Mi
    for (unit, suffix) in [(1u64 << 30, "Gi"), (1 << 20, "Mi"), (1 << 10, "Ki")] {
        // only use a unit that divides exactly, since a rounded size in the artifact would make
        // two different configurations look like one
        if bytes >= unit && bytes % unit == 0 {
            return format!("{}{suffix}", bytes / unit);
        }
    }
    bytes.to_string()
}

/// A content hash over a whole resolved configuration
///
/// The three named fields in [`ConfFacts`] are the ones worth reading; this is what catches a
/// change to anything else. It deliberately covers the storage paths too, so a capture taken
/// against a different filesystem does not look like one taken against the same.
///
/// # Arguments
///
/// * `conf` - The configuration to digest
fn digest(conf: &Conf) -> String {
    use sha2::{Digest, Sha256};

    // serialize the whole configuration rather than picking fields, so a setting added to `Conf`
    // is covered without this function having to learn about it
    let rendered = serde_json::to_vec(conf).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&rendered);
    // twelve hex characters is plenty to notice a change and short enough to read in a table
    hex::encode(hasher.finalize())[..12].to_string()
}

/// Every storage directory a configuration writes to
///
/// # Arguments
///
/// * `conf` - The configuration to read the paths out of
pub fn storage_dirs(conf: &Conf) -> Vec<PathBuf> {
    // usually one directory named twice, so dedupe rather than wiping the same path twice
    let mut dirs = vec![
        conf.storage.default.filesystem.latency_sensitive.path.clone(),
        conf.storage
            .default
            .filesystem
            .throughput_sensitive
            .path
            .clone(),
    ];
    dirs.sort();
    dirs.dedup();
    dirs
}

#[cfg(test)]
mod tests {
    use super::{binary_size, slug};

    /// A byte count comes back in the units it was written in
    #[test]
    fn a_size_renders_in_its_own_units() {
        assert_eq!(binary_size(4 << 30), "4Gi");
        assert_eq!(binary_size(100 << 20), "100Mi");
        assert_eq!(binary_size(4 << 10), "4Ki");
    }

    /// A size that is not a whole number of units stays exact
    ///
    /// Rounding here would make two configurations that differ look identical in the artifact,
    /// which is the one thing recording the configuration exists to prevent.
    #[test]
    fn an_inexact_size_is_not_rounded() {
        assert_eq!(binary_size(1), "1");
        assert_eq!(binary_size((1 << 30) + 1), "1073741825");
    }

    /// A slug replaces every separator, however many a parameterized id carries
    #[test]
    fn a_slug_replaces_every_separator() {
        assert_eq!(slug("macro/insert_unsorted"), "macro-insert_unsorted");
        assert_eq!(slug("macro/fanout/evicted/256"), "macro-fanout-evicted-256");
    }

    /// Two different ids cannot slug to the same directory
    ///
    /// This is what the storage separation rests on. If two workloads shared a directory, the
    /// first one to run with a different shard count would fail `StorageMeta::claim` partway
    /// through a capture.
    #[test]
    fn distinct_ids_slug_distinctly() {
        let ids = crate::workload_ids::IDS;
        let mut slugs: Vec<String> = ids.iter().map(|id| slug(id)).collect();
        slugs.sort();
        let before = slugs.len();
        slugs.dedup();
        assert_eq!(before, slugs.len(), "two workloads share a storage subdir");
    }
}
