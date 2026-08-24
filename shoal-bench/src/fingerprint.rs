//! What a capture was taken on, gathered once and recorded
//!
//! There is exactly one function that gathers these facts, [`current`], and both halves of the
//! feature call it: `run` calls it to write a capture's provenance, and `status`, `compare` and
//! `render` call it to judge one. If capture time and judge time gathered facts differently, every
//! verdict would be comparing two things that were never measured the same way.
//!
//! All of the I/O sits behind [`Facts`]. `RealFacts` shells out to git and reads `/proc` and
//! `/sys`; the tests use a fake, so nothing spawns a process to check that a verdict is right.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::model::meta::{CodeFacts, EnvFacts};
use crate::registry::Layer;
use crate::store::Store;

/// Which sources each layer is considered to measure
#[derive(Debug, Clone, Deserialize)]
pub struct SourceManifest {
    /// The schema version this manifest was written with
    pub version: u32,
    /// The paths each layer covers, which may be files or directories
    pub layers: BTreeMap<Layer, Vec<String>>,
}

/// Everything a verdict is made from
#[derive(Debug, Clone)]
pub struct Fingerprint {
    /// What the code looks like
    pub code: CodeFacts,
    /// What the machine and toolchain are
    pub env: EnvFacts,
}

/// The facts a fingerprint is built from
///
/// Behind a trait so that the verdict logic can be tested over hand built states rather than over
/// whatever the machine running the tests happens to be.
pub trait Facts {
    /// The commit `HEAD` points at
    fn git_head(&self) -> Result<String>;

    /// Every path with uncommitted changes, relative to the repository root
    fn git_dirty(&self) -> Result<Vec<PathBuf>>;

    /// How many commits `to` is ahead of `from`
    ///
    /// `None` when `from` is not an ancestor of `to`, which is what a rebase or a branch switch
    /// looks like from here - the distance is not defined and pretending it is would be worse
    /// than saying so.
    fn commits_between(&self, from: &str, to: &str) -> Result<Option<u32>>;

    /// The contents of a file, relative to the repository root
    fn read_file(&self, relative: &Path) -> Result<Vec<u8>>;

    /// Every Rust source beneath a path, relative to the repository root, sorted
    ///
    /// A path naming a file yields just that file. A path that does not exist yields nothing,
    /// since the manifest is allowed to name a layer's sources loosely.
    fn rust_sources(&self, relative: &Path) -> Result<Vec<PathBuf>>;

    /// The host this is running on
    fn hostname(&self) -> String;

    /// The CPU model
    fn cpu_model(&self) -> String;

    /// How many logical CPUs are online
    fn cpu_online(&self) -> usize;

    /// The scaling governor cpu0 is set to
    fn governor(&self) -> String;

    /// The kernel release
    fn kernel(&self) -> String;

    /// The full `rustc -vV` output
    fn rustc_version(&self) -> String;

    /// An environment variable, if it is set
    fn env_var(&self, name: &str) -> Option<String>;
}

/// The real facts, read off the machine this is running on
#[derive(Debug, Clone)]
pub struct RealFacts {
    /// The repository root every relative path is resolved against
    root: PathBuf,
}

impl RealFacts {
    /// Reads facts about a repository
    ///
    /// # Arguments
    ///
    /// * `root` - The repository root
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        RealFacts { root: root.into() }
    }

    /// Runs a command in the repository and returns its trimmed stdout
    ///
    /// # Arguments
    ///
    /// * `program` - What to run
    /// * `args` - What to run it with
    fn capture(&self, program: &str, args: &[&str]) -> Result<String> {
        // run it where the repository is, so git answers about this tree
        let output = Command::new(program)
            .current_dir(&self.root)
            .args(args)
            .output()
            .with_context(|| format!("running {program} {}", args.join(" ")))?;
        // a command that failed has no answer to give
        if !output.status.success() {
            anyhow::bail!(
                "{program} {} failed with {}",
                args.join(" "),
                output.status
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Reads a file outside the repository, falling back to a placeholder
    ///
    /// Used for the `/proc` and `/sys` files that describe the machine. A machine that does not
    /// expose one of them is described as unknown rather than failing the command: an
    /// unrecordable governor is a worse report, not a broken tool.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to read
    fn read_system(path: &str) -> String {
        // trim the trailing newline these files all carry
        std::fs::read_to_string(path)
            .map(|body| body.trim().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    }
}

impl Facts for RealFacts {
    /// The commit `HEAD` points at
    fn git_head(&self) -> Result<String> {
        // the full hash, since an abbreviated one can become ambiguous as history grows
        self.capture("git", &["rev-parse", "HEAD"])
    }

    /// Every path with uncommitted changes, relative to the repository root
    fn git_dirty(&self) -> Result<Vec<PathBuf>> {
        // porcelain output is stable across git versions, which the human format is not
        let out = self.capture("git", &["status", "--porcelain"])?;
        let mut paths = Vec::new();
        for line in out.lines() {
            // each line is two status characters, a space, then the path
            let Some(rest) = line.get(3..) else {
                continue;
            };
            // a rename is written `old -> new`, and it is the new path that exists now
            let path = rest.rsplit(" -> ").next().unwrap_or(rest);
            // git quotes paths containing unusual characters, so the quotes come back off
            let path = path.trim_matches('"');
            paths.push(PathBuf::from(path));
        }
        Ok(paths)
    }

    /// How many commits `to` is ahead of `from`
    ///
    /// # Arguments
    ///
    /// * `from` - The earlier commit
    /// * `to` - The later commit
    fn commits_between(&self, from: &str, to: &str) -> Result<Option<u32>> {
        // a commit this tree has never heard of has no distance from anything in it
        if self
            .capture("git", &["cat-file", "-e", &format!("{from}^{{commit}}")])
            .is_err()
        {
            return Ok(None);
        }
        // nor does one that is not an ancestor, which is what a rebase looks like from here
        if self
            .capture("git", &["merge-base", "--is-ancestor", from, to])
            .is_err()
        {
            return Ok(None);
        }
        // otherwise count what has landed since
        let out = self.capture("git", &["rev-list", "--count", &format!("{from}..{to}")])?;
        Ok(out.parse::<u32>().ok())
    }

    /// The contents of a file, relative to the repository root
    fn read_file(&self, relative: &Path) -> Result<Vec<u8>> {
        let path = self.root.join(relative);
        std::fs::read(&path).with_context(|| format!("reading {}", path.display()))
    }

    /// Every Rust source beneath a path, relative to the repository root, sorted
    fn rust_sources(&self, relative: &Path) -> Result<Vec<PathBuf>> {
        let absolute = self.root.join(relative);
        // a path the manifest names that is not there contributes nothing
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        // a file names only itself
        if absolute.is_file() {
            return Ok(vec![relative.to_path_buf()]);
        }
        // a directory names every Rust source beneath it
        let mut found = Vec::new();
        for entry in walkdir::WalkDir::new(&absolute).sort_by_file_name() {
            let entry = entry.with_context(|| format!("walking {}", absolute.display()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            // recorded relative to the repository, so the digest does not depend on where the
            // checkout lives
            if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                found.push(rel.to_path_buf());
            }
        }
        // sorted, since a digest over an unordered list is not a digest of anything
        found.sort();
        Ok(found)
    }

    /// The host this is running on
    fn hostname(&self) -> String {
        Self::read_system("/proc/sys/kernel/hostname")
    }

    /// The CPU model
    fn cpu_model(&self) -> String {
        // the first `model name` line, which every core repeats
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|body| {
                body.lines()
                    .find(|line| line.starts_with("model name"))
                    .and_then(|line| line.split_once(':'))
                    .map(|(_, value)| value.trim().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// How many logical CPUs are online
    fn cpu_online(&self) -> usize {
        // one `processor` line per online logical cpu
        std::fs::read_to_string("/proc/cpuinfo")
            .map(|body| {
                body.lines()
                    .filter(|line| line.starts_with("processor"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// The scaling governor cpu0 is set to
    fn governor(&self) -> String {
        // the benchmarks assume `performance`; anything else is a different measurement
        Self::read_system("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    }

    /// The kernel release
    fn kernel(&self) -> String {
        Self::read_system("/proc/sys/kernel/osrelease")
    }

    /// The full `rustc -vV` output
    fn rustc_version(&self) -> String {
        // the whole block, since the commit hash in it distinguishes two nightlies of one version
        self.capture("rustc", &["-vV"])
            .unwrap_or_else(|_| "unknown".to_string())
    }

    /// An environment variable, if it is set
    fn env_var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Hashes a byte string
///
/// # Arguments
///
/// * `bytes` - What to hash
fn sha256(bytes: &[u8]) -> String {
    // hex, so a hash can be read out of a committed json file and compared by eye
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Reads the manifest naming which sources each layer measures
///
/// # Arguments
///
/// * `store` - The artifact tree, which knows where the manifest is
pub fn read_manifest(store: &Store) -> Result<(SourceManifest, String)> {
    let path = store.sources_manifest();
    let body = std::fs::read(&path)
        .with_context(|| format!("reading the source manifest {}", path.display()))?;
    let manifest: SourceManifest = serde_json::from_slice(&body)
        .with_context(|| format!("parsing the source manifest {}", path.display()))?;
    // the manifest's own hash is recorded alongside the digests it produced, so that a change to
    // what is being measured is as visible as a change to the measured code
    Ok((manifest, sha256(&body)))
}

/// Gathers everything a verdict is made from
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `facts` - Where to read the facts from
/// * `allow_dirty` - Whether the caller has accepted capturing on a dirty tree
pub fn current(store: &Store, facts: &dyn Facts, allow_dirty: bool) -> Result<Fingerprint> {
    // what the tree is at
    let head = facts.git_head().unwrap_or_default();
    let dirty_paths = facts.git_dirty().unwrap_or_default();
    let dirty: BTreeSet<&Path> = dirty_paths.iter().map(PathBuf::as_path).collect();
    // and which sources each layer is considered to cover
    let (manifest, manifest_sha) = read_manifest(store)?;
    let mut sources: BTreeMap<Layer, BTreeMap<String, String>> = BTreeMap::new();
    let mut layer_digest: BTreeMap<Layer, String> = BTreeMap::new();
    let mut dirty_layers: Vec<Layer> = Vec::new();
    for (layer, paths) in &manifest.layers {
        // expand the manifest's paths into the files they name
        let mut files: Vec<PathBuf> = Vec::new();
        for path in paths {
            files.extend(facts.rust_sources(Path::new(path))?);
        }
        // one path can be named twice - `shoal-core/src/server.rs` is also reached by walking a
        // sibling directory in another layer - so collapse duplicates before hashing
        files.sort();
        files.dedup();
        // hash each file, and note whether any of them is uncommitted
        let mut hashes = BTreeMap::new();
        for file in &files {
            if dirty.contains(file.as_path()) && !dirty_layers.contains(layer) {
                dirty_layers.push(*layer);
            }
            // a file that cannot be read is recorded as absent rather than skipped, so that
            // deleting a measured file changes the digest
            let hash = match facts.read_file(file) {
                Ok(body) => sha256(&body),
                Err(_) => "absent".to_string(),
            };
            hashes.insert(file.display().to_string(), hash);
        }
        // the layer's digest is one hash over its sorted (path, hash) pairs, so that a file being
        // added or removed moves it just as much as a file changing does
        let joined: String = hashes
            .iter()
            .map(|(path, hash)| format!("{path}\0{hash}\n"))
            .collect();
        layer_digest.insert(*layer, sha256(joined.as_bytes()));
        sources.insert(*layer, hashes);
    }
    // the build configuration, hashed rather than parsed - the readable rustflags string below is
    // best effort and the hash is what a comparison actually rests on
    let cargo_config = facts
        .read_file(Path::new(".cargo/config.toml"))
        .unwrap_or_default();
    let shoal_yml = facts.read_file(Path::new("shoal.yml")).unwrap_or_default();
    let rustflags = scan_rustflags(&String::from_utf8_lossy(&cargo_config));
    let rustc = facts.rustc_version();
    let env = EnvFacts {
        hostname: facts.hostname(),
        cpu_model: facts.cpu_model(),
        cpu_online: facts.cpu_online(),
        governor: facts.governor(),
        kernel: facts.kernel(),
        rustc_commit_hash: scan_rustc_commit(&rustc),
        rustc: rustc.lines().next().unwrap_or("unknown").to_string(),
        rustflags: rustflags.clone(),
        // recorded apart from the config file because it overrides it: a capture taken with it
        // set is a different build even though the config did not change
        rustflags_env: facts.env_var("RUSTFLAGS"),
        cargo_config_sha: sha256(&cargo_config),
        shoal_yml_sha: sha256(&shoal_yml),
        digest: String::new(),
    };
    // the comparability digest covers only what two captures must agree on to be comparable
    let mut env = env;
    env.digest = env_digest(&env);
    Ok(Fingerprint {
        code: CodeFacts {
            head_short: head.chars().take(7).collect(),
            head,
            dirty: !dirty_paths.is_empty(),
            dirty_paths: dirty_paths.len(),
            allow_dirty,
            sources_manifest_sha: manifest_sha,
            sources,
            layer_digest,
            dirty_layers,
        },
        env,
    })
}

/// Hashes the environment fields two captures must agree on to be comparable
///
/// Deliberately not every field. The kernel release and the online CPU count are recorded because
/// they are worth reading, but a kernel point release does not make two captures incomparable and
/// treating it as if it did would make the flag meaningless through overuse.
///
/// # Arguments
///
/// * `env` - The environment to hash
pub fn env_digest(env: &EnvFacts) -> String {
    // the host, the chip, the governor, the compiler and the flags it was given
    let joined = format!(
        "{}|{}|{}|{}|{}|{}",
        env.hostname,
        env.cpu_model,
        env.governor,
        env.rustc,
        env.rustflags,
        env.rustflags_env.as_deref().unwrap_or("")
    );
    sha256(joined.as_bytes())
}

/// Pulls the rustflags line out of a cargo config, for display
///
/// Best effort and deliberately not a TOML parse: the comparable signal is the hash of the whole
/// file, and this is only here so a report can say `-Ctarget-cpu=native` instead of a hash.
///
/// # Arguments
///
/// * `config` - The contents of `.cargo/config.toml`
fn scan_rustflags(config: &str) -> String {
    // find the rustflags assignment, ignoring anything commented out
    for line in config.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            continue;
        }
        let Some(rest) = trimmed.strip_prefix("rustflags") else {
            continue;
        };
        let Some((_, value)) = rest.split_once('=') else {
            continue;
        };
        // the value is an array of quoted flags; take what is between the quotes, in order
        let flags: Vec<&str> = value
            .split('"')
            .skip(1)
            .step_by(2)
            .collect();
        if !flags.is_empty() {
            return flags.join(" ");
        }
    }
    String::new()
}

/// Pulls the commit hash out of `rustc -vV` output
///
/// # Arguments
///
/// * `version` - The full `rustc -vV` output
fn scan_rustc_commit(version: &str) -> String {
    // one line of the block names the commit the compiler was built from
    version
        .lines()
        .find_map(|line| line.strip_prefix("commit-hash: "))
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every path the manifest names still exists in the tree
    ///
    /// A path that does not exist is skipped silently, which cannot make a stale capture look
    /// fresh but can make an affected one look **unaffected** — so a rename quietly stops a layer
    /// being watched at all. That is [item 78](../../docs/src/appendix/known-issues.md): F15 moved
    /// the client into `shoal-client` and the wire format into `shoal-proto`, and six of the
    /// seventeen paths here named files that had stopped existing, including the protocol module
    /// `wire.rs` measures. Nothing noticed for eight captures. This is the check that would have.
    #[test]
    fn every_source_the_manifest_names_exists() {
        // the manifest lives beside the artifacts, and both sit under the repo root
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the crate is a workspace member, so it has a parent")
            .to_path_buf();
        // read the manifest the same way a verdict does
        let store = Store::new(&root);
        let (manifest, _) = read_manifest(&store).expect("the committed manifest parses");
        // collect every path that does not resolve, so one failure names all of them
        let mut missing = Vec::new();
        for (layer, paths) in &manifest.layers {
            for path in paths {
                // a path may name a file or a directory, and either has to be there
                if !root.join(path).exists() {
                    missing.push(format!("{layer:?}: {path}"));
                }
            }
        }
        assert!(
            missing.is_empty(),
            "docs/perf/sources.json names paths that do not exist, so those layers are not being \
             watched: {missing:#?}"
        );
    }

    /// The flags are read out of the array, in order, ignoring comments
    #[test]
    fn rustflags_are_scanned_out_of_a_cargo_config() {
        let config = "# rustflags = [\"-Cwrong\"]\n[build]\nrustflags = [\"-Ctarget-cpu=native\"]\n";
        assert_eq!(scan_rustflags(config), "-Ctarget-cpu=native");
    }

    /// Several flags come back in the order they were written
    #[test]
    fn several_rustflags_keep_their_order() {
        let config = "[build]\nrustflags = [\"-Ctarget-cpu=native\", \"-Copt-level=3\"]\n";
        assert_eq!(scan_rustflags(config), "-Ctarget-cpu=native -Copt-level=3");
    }

    /// A config with no flags reports none rather than guessing
    #[test]
    fn a_config_without_rustflags_reports_none() {
        assert_eq!(scan_rustflags("[build]\ntarget = \"x\"\n"), "");
        assert_eq!(scan_rustflags(""), "");
    }

    /// The commit hash is pulled out of the version block
    #[test]
    fn the_rustc_commit_is_scanned() {
        let version = "rustc 1.99.0-nightly (1a98b1e13 2026-08-07)\n\
                       binary: rustc\n\
                       commit-hash: 1a98b1e135b254f209c67d447b6d8bcd56a859e0\n";
        assert_eq!(
            scan_rustc_commit(version),
            "1a98b1e135b254f209c67d447b6d8bcd56a859e0"
        );
        assert_eq!(scan_rustc_commit("rustc 1.0.0"), "");
    }

    /// The comparability digest moves with the fields that make a capture incomparable
    #[test]
    fn the_env_digest_covers_what_makes_captures_incomparable() {
        let base = EnvFacts {
            hostname: "jove".to_string(),
            cpu_model: "AMD Ryzen 9 9950X 16-Core Processor".to_string(),
            cpu_online: 32,
            governor: "performance".to_string(),
            kernel: "7.0.0-29-generic".to_string(),
            rustc: "rustc 1.99.0-nightly".to_string(),
            rustc_commit_hash: "abc".to_string(),
            rustflags: "-Ctarget-cpu=native".to_string(),
            rustflags_env: None,
            cargo_config_sha: "sha".to_string(),
            shoal_yml_sha: "sha".to_string(),
            digest: String::new(),
        };
        let before = env_digest(&base);
        // a different governor is a different measurement
        let mut governor = base.clone();
        governor.governor = "powersave".to_string();
        assert_ne!(env_digest(&governor), before);
        // so is a different machine, compiler, or set of flags
        for mutate in [
            (|e: &mut EnvFacts| e.hostname = "other".to_string()) as fn(&mut EnvFacts),
            |e: &mut EnvFacts| e.cpu_model = "other".to_string(),
            |e: &mut EnvFacts| e.rustc = "other".to_string(),
            |e: &mut EnvFacts| e.rustflags = "other".to_string(),
            |e: &mut EnvFacts| e.rustflags_env = Some("-Cwrong".to_string()),
        ] {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert_ne!(env_digest(&changed), before);
        }
        // a kernel point release is not, and neither is a core going offline
        let mut kernel = base.clone();
        kernel.kernel = "7.0.0-30-generic".to_string();
        kernel.cpu_online = 31;
        assert_eq!(env_digest(&kernel), before);
    }
}
