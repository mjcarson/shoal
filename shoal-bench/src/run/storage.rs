//! Finding the storage directory, and refusing to wipe the wrong one
//!
//! The storage directory is emptied before every macro run. That is not optional: inserting over
//! a populated store changes partition faulting, archive map size and when compaction fires, none
//! of which are held constant otherwise, so a run over a dirty store measures something the next
//! run will not.
//!
//! It does mean this tool issues a recursive delete against a path it read out of a config file.
//! `scripts/bench.sh` guarded that with a check that the path was neither empty nor `/`, which
//! would have cheerfully emptied a home directory. The guard here is that the directory has to
//! look like a Shoal store, which it does by carrying the marker the server writes into it.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// The marker file the server writes at the root of a storage directory
///
/// Kept in step with `shoal-core/src/server/meta.rs`. A directory without it is not a store this
/// harness created, and is not something to delete.
pub const STORAGE_MARKER: &str = "shoal-meta.json";

/// Just enough of `shoal.yml` to find the storage path
///
/// Deliberately a partial deserialization: this tool has no business knowing the rest of the
/// server's configuration, and a config that grows a field should not stop a benchmark running.
#[derive(Debug, Deserialize)]
struct Conf {
    /// Where each kind of storage lives
    storage: ConfStorage,
}

/// The storage section of `shoal.yml`
#[derive(Debug, Deserialize)]
struct ConfStorage {
    /// The default storage configuration
    default: ConfStorageKind,
}

/// One storage backend's configuration
#[derive(Debug, Deserialize)]
struct ConfStorageKind {
    /// The filesystem backend, which is the only one this harness benchmarks
    filesystem: ConfFilesystem,
}

/// The filesystem backend's configuration
#[derive(Debug, Deserialize)]
struct ConfFilesystem {
    /// Where latency sensitive data goes, which is the intent log
    latency_sensitive: ConfPath,
    /// Where throughput sensitive data goes, which is the archives
    throughput_sensitive: ConfPath,
}

/// A configured path
#[derive(Debug, Deserialize)]
struct ConfPath {
    /// The directory itself
    path: PathBuf,
}

/// Every directory a run writes into, deduplicated
///
/// Both halves of the filesystem backend are read rather than only the first, because
/// `scripts/bench.sh` scraped the first `path:` out of the file with a regex and would have left
/// the second populated had they ever differed.
///
/// # Arguments
///
/// * `conf` - The path to `shoal.yml`
pub fn storage_dirs(conf: &Path) -> Result<Vec<PathBuf>> {
    let body = std::fs::read_to_string(conf)
        .with_context(|| format!("reading the benchmark config {}", conf.display()))?;
    let parsed: Conf = serde_yaml::from_str(&body)
        .with_context(|| format!("parsing the benchmark config {}", conf.display()))?;
    // both halves, in a stable order, with a repeat collapsed
    let mut dirs = vec![
        parsed.storage.default.filesystem.latency_sensitive.path,
        parsed.storage.default.filesystem.throughput_sensitive.path,
    ];
    dirs.sort();
    dirs.dedup();
    Ok(dirs)
}

/// Whether a directory may be emptied
///
/// # Arguments
///
/// * `dir` - The directory in question
/// * `force` - Whether the caller has accepted wiping something that does not look like a store
pub fn check_wipeable(dir: &Path, force: bool) -> Result<()> {
    // a path that is not absolute is relative to wherever this happens to be running
    if !dir.is_absolute() {
        bail!(
            "refusing to wipe '{}': the storage path must be absolute",
            dir.display()
        );
    }
    // the root, and anything shallow enough to be a mount point rather than a store
    if dir.components().count() < 3 {
        bail!(
            "refusing to wipe '{}': it is too close to the root to be a storage directory",
            dir.display()
        );
    }
    // a symlink would be followed into wherever it points
    let meta = std::fs::symlink_metadata(dir);
    if let Ok(meta) = &meta
        && meta.file_type().is_symlink()
    {
        bail!(
            "refusing to wipe '{}': it is a symlink, so wiping it would empty its target",
            dir.display()
        );
    }
    // a directory that does not exist yet will be created by the server, and there is nothing to
    // wipe
    if meta.is_err() {
        return Ok(());
    }
    // an empty directory is safe to empty whatever it is
    let mut entries = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .peekable();
    if entries.peek().is_none() {
        return Ok(());
    }
    // and a populated one has to look like a store this harness put there
    if !holds_a_store(dir) && !force {
        bail!(
            "refusing to wipe '{}': it is not empty, and neither it nor any directory directly \
             inside it has a {STORAGE_MARKER}, so it does not look like a Shoal storage \
             directory. Check the `storage` section of your config, or pass --force-wipe if it \
             really is one.",
            dir.display()
        );
    }
    Ok(())
}

/// Whether a directory is a Shoal store, or is the parent of one
///
/// Both cases are real, and the second one is new. Every workload gets its own subdirectory under
/// the configured storage root, so that a workload running one shard and a workload running twelve
/// cannot meet in the same directory and fail `StorageMeta::claim` partway through a capture. That
/// means the configured root itself never carries the marker any more - the directories inside it
/// do - and a guard that only looked at the root would refuse to wipe the very tree this harness
/// created.
///
/// Only one level down is checked, deliberately. Walking the whole tree would find a marker
/// arbitrarily deep and make the guard progressively easier to satisfy, which is the opposite of
/// what a guard on a recursive delete should do.
///
/// # Arguments
///
/// * `dir` - The directory in question
fn holds_a_store(dir: &Path) -> bool {
    // the old layout: the server was rooted directly here
    if dir.join(STORAGE_MARKER).is_file() {
        return true;
    }
    // the workload layout: one store per workload, one level down
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries
        .filter_map(|entry| entry.ok())
        .any(|entry| entry.path().join(STORAGE_MARKER).is_file())
}

/// Empties a directory, leaving the directory itself in place
///
/// # Arguments
///
/// * `dir` - The directory to empty
/// * `force` - Whether the caller has accepted wiping something that does not look like a store
pub fn wipe(dir: &Path, force: bool) -> Result<()> {
    // never delete anything without passing the guard first
    check_wipeable(dir, force)?;
    // a directory that is not there has nothing in it
    if !dir.is_dir() {
        return Ok(());
    }
    // remove the contents rather than the directory, since it may be a mount point and the server
    // expects to find it
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("reading an entry of {}", dir.display()))?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("removing {}", path.display()))?;
        } else {
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real benchmark config parses, and names the directory the docs say it does
    #[test]
    fn the_committed_config_parses() {
        let conf = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("shoal-bench has a parent")
            .join("shoal.yml");
        let dirs = storage_dirs(&conf).expect("the committed config parses");
        assert_eq!(dirs, vec![PathBuf::from("/opt/shoal")]);
    }

    /// Both halves of the filesystem backend are read, not just the first
    #[test]
    fn both_storage_halves_are_read() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let conf = dir.path().join("shoal.yml");
        std::fs::write(
            &conf,
            "storage:\n  default:\n    filesystem:\n      latency_sensitive:\n        \
             path: \"/opt/fast\"\n      throughput_sensitive:\n        path: \"/opt/slow\"\n",
        )
        .expect("writing a config");
        assert_eq!(
            storage_dirs(&conf).expect("it parses"),
            vec![PathBuf::from("/opt/fast"), PathBuf::from("/opt/slow")]
        );
    }

    /// A relative path is refused, since it means something different from every directory
    #[test]
    fn a_relative_path_is_refused() {
        assert!(check_wipeable(Path::new("data"), false).is_err());
        assert!(check_wipeable(Path::new("./data"), false).is_err());
    }

    /// The root and anything near it is refused
    #[test]
    fn a_shallow_path_is_refused() {
        assert!(check_wipeable(Path::new("/"), false).is_err());
        assert!(check_wipeable(Path::new("/opt"), false).is_err());
    }

    /// A directory that does not exist yet has nothing to wipe and is allowed
    #[test]
    fn a_missing_directory_is_allowed() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let missing = dir.path().join("a/b/c");
        assert!(check_wipeable(&missing, false).is_ok());
    }

    /// An empty directory is safe to empty whatever it is
    #[test]
    fn an_empty_directory_is_allowed() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let target = dir.path().join("store");
        std::fs::create_dir_all(&target).expect("creating it");
        assert!(check_wipeable(&target, false).is_ok());
    }

    /// A populated directory with no marker is refused, and the force flag overrides it
    #[test]
    fn a_populated_directory_without_a_marker_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let target = dir.path().join("not-a-store");
        std::fs::create_dir_all(&target).expect("creating it");
        std::fs::write(target.join("important.txt"), b"data").expect("writing a file");
        // refused by default
        let err = check_wipeable(&target, false).expect_err("it should be refused");
        assert!(format!("{err}").contains(STORAGE_MARKER));
        // and allowed when the caller says it really is a store
        assert!(check_wipeable(&target, true).is_ok());
    }

    /// A populated directory carrying the marker is a store, and is wiped down to nothing
    #[test]
    fn a_store_is_emptied_without_being_removed() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let target = dir.path().join("store");
        std::fs::create_dir_all(target.join("Movie")).expect("creating a table directory");
        std::fs::write(target.join(STORAGE_MARKER), b"{}").expect("writing the marker");
        std::fs::write(target.join("Movie/0.log"), b"data").expect("writing a log");
        wipe(&target, false).expect("a store is wipeable");
        // emptied, but still there for the server to claim again
        assert!(target.is_dir());
        assert_eq!(
            std::fs::read_dir(&target).expect("reading it").count(),
            0
        );
    }

    /// A directory whose children are stores is a store root, and is wipeable
    ///
    /// The layout every capture now produces: one store per workload under the configured root,
    /// so that two workloads running different shard counts cannot meet in one directory. The
    /// root itself never carries the marker, and a guard that only looked there would refuse to
    /// wipe the tree this harness had just created.
    #[test]
    fn a_directory_of_stores_is_wipeable() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let root = dir.path().join("shoal");
        let workload = root.join("macro-insert_unsorted");
        std::fs::create_dir_all(&workload).expect("creating a workload store");
        std::fs::write(workload.join(STORAGE_MARKER), b"{}").expect("writing the marker");
        assert!(check_wipeable(&root, false).is_ok());
    }

    /// The marker is only looked for one level down, so a deep one does not unlock the guard
    ///
    /// A guard on a recursive delete must not get easier to satisfy the deeper the tree goes.
    #[test]
    fn a_deeply_buried_marker_does_not_unlock_the_guard() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let root = dir.path().join("home");
        let buried = root.join("projects/something/store");
        std::fs::create_dir_all(&buried).expect("creating a buried store");
        std::fs::write(buried.join(STORAGE_MARKER), b"{}").expect("writing the marker");
        assert!(check_wipeable(&root, false).is_err());
    }

    /// A symlink is refused rather than followed into whatever it points at
    #[test]
    #[cfg(unix)]
    fn a_symlink_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let real = dir.path().join("real");
        std::fs::create_dir_all(&real).expect("creating it");
        std::fs::write(real.join("important.txt"), b"data").expect("writing a file");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).expect("creating a symlink");
        let err = check_wipeable(&link, false).expect_err("a symlink should be refused");
        assert!(format!("{err}").contains("symlink"));
        // and the force flag does not turn it into a good idea
        assert!(check_wipeable(&link, true).is_err());
    }
}
