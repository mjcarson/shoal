//! The identity of a Shoal storage directory
//!
//! A partition is owned by the shard its tablet is assigned to, and that assignment comes
//! from the shard count. A shard also stores its data under its own name, so a directory
//! written by one shard count and read back under another looks for every partition in
//! the wrong place and finds nothing. Nothing about the data on disk says what count wrote
//! it, so this records it.
//!
//! There is deliberately no migration. This turns a silent loss into a refusal to start;
//! moving data between shard counts needs tablet migration, which does not exist yet.
//!
//! This is also where a node's identity belongs once Shoal is distributed — a cluster id,
//! a node id, and the epoch of the topology its data was written under all answer the same
//! kind of question this file already answers.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{event, instrument, Level};

use super::errors::ShoalError;
use super::ServerError;

/// The version of this metadata file's own format
const META_FORMAT: u32 = 1;

/// The name of the metadata file within a storage directory
const META_FILE: &str = "shoal-meta.json";

/// The identity of a Shoal storage directory
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StorageMeta {
    /// The version of this metadata file's own format
    pub format: u32,
    /// The number of shards the data in this directory was written by
    pub shards: usize,
}

impl StorageMeta {
    /// Build the metadata for a directory written by this many shards
    ///
    /// # Arguments
    ///
    /// * `shards` - The number of shards writing to this directory
    #[must_use]
    pub fn new(shards: usize) -> Self {
        StorageMeta {
            format: META_FORMAT,
            shards,
        }
    }

    /// Get the path to the metadata file within a storage directory
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    fn path(root: &Path) -> PathBuf {
        root.join(META_FILE)
    }

    /// Check this storage directory was written by the shard count we are starting with
    ///
    /// A directory with no metadata is new to us and is claimed by writing it. This is
    /// called once, before any shard is spawned, so it uses blocking IO deliberately:
    /// there is no glommio executor yet, and doing it here rather than per shard is what
    /// keeps every shard from racing to write the same file.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `shards` - The number of shards about to be started
    ///
    /// # Errors
    ///
    /// This will fail if the directory was marked in a format we cannot read, if it was
    /// written by a different number of shards, or if the metadata cannot be read or
    /// written.
    #[instrument(name = "StorageMeta::claim", skip_all, err(Debug))]
    pub fn claim(root: &Path, shards: usize) -> Result<(), ServerError> {
        // get the path this directorys metadata lives at
        let path = Self::path(root);
        // read whatever metadata this directory already carries
        match std::fs::read(&path) {
            // this directory has been written before, so it has a shard count to honour
            Ok(raw) => {
                // parse the metadata we found
                let found: StorageMeta = serde_json::from_slice(&raw)?;
                // a format we cannot read makes every field inside it a guess, including
                // the shard count, so settle the format before anything is read from it
                if found.format != META_FORMAT {
                    return Err(ServerError::Shoal(ShoalError::StorageFormatMismatch {
                        found: found.format,
                        expected: META_FORMAT,
                    }));
                }
                // a different shard count would look for every partition in the wrong place
                if found.shards != shards {
                    return Err(ServerError::Shoal(ShoalError::ShardCountMismatch {
                        found: found.shards,
                        expected: shards,
                    }));
                }
                Ok(())
            }
            // this directory has never been written to, so claim it for this shard count
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // make sure the directory we are claiming exists
                std::fs::create_dir_all(root)?;
                // build the metadata describing who is about to write here
                let meta = StorageMeta::new(shards);
                // write it before any shard has had the chance to store anything
                std::fs::write(&path, serde_json::to_vec_pretty(&meta)?)?;
                // say what we claimed, since it is what a later start is held to
                event!(
                    Level::INFO,
                    msg = "Claimed a new storage directory",
                    path = path.display().to_string(),
                    shards,
                );
                Ok(())
            }
            // anything else is a directory we cannot read
            Err(error) => Err(ServerError::IO(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory nothing has written to is claimed rather than refused
    #[test]
    fn an_unclaimed_directory_is_claimed() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claiming it has to succeed
        StorageMeta::claim(dir.path(), 4).expect("failed to claim a new directory");
        // and has to leave behind what it claimed
        let raw = std::fs::read(StorageMeta::path(dir.path())).expect("no metadata written");
        let found: StorageMeta = serde_json::from_slice(&raw).expect("bad metadata written");
        assert_eq!(found, StorageMeta::new(4));
    }

    /// A directory reopened by the shard count that wrote it starts
    #[test]
    fn the_same_shard_count_is_allowed_back() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claim it for some number of shards
        StorageMeta::claim(dir.path(), 4).expect("failed to claim a new directory");
        // reopening it with that same count is the ordinary restart
        StorageMeta::claim(dir.path(), 4).expect("failed to reopen with the same shard count");
    }

    /// A marker written by a format we do not understand is refused
    ///
    /// The shard count in a marker is only meaningful if we agree about what the fields
    /// around it mean, so the format has to be settled before the count is read.
    #[test]
    fn an_unknown_format_is_refused() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // make sure the directory we are staging a marker in exists
        std::fs::create_dir_all(dir.path()).expect("failed to build our storage directory");
        // stage a marker written by a format later than the one we know
        let future = StorageMeta {
            format: META_FORMAT + 1,
            shards: 4,
        };
        std::fs::write(
            StorageMeta::path(dir.path()),
            serde_json::to_vec_pretty(&future).expect("failed to build our marker"),
        )
        .expect("failed to stage our marker");
        // reading it under a format we do not understand has to refuse
        let error =
            StorageMeta::claim(dir.path(), 4).expect_err("an unknown marker format started");
        // and has to say which format it could not read rather than guess at the fields
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::StorageFormatMismatch {
                found: 2,
                expected: 1
            })
        ));
    }

    /// A directory reopened by a different shard count is refused
    #[test]
    fn a_different_shard_count_is_refused() {
        // get a directory nothing has written to
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // claim it for some number of shards
        StorageMeta::claim(dir.path(), 4).expect("failed to claim a new directory");
        // reopening it with another count would look for data in the wrong place
        let error = StorageMeta::claim(dir.path(), 5).expect_err("a shard count change started");
        // and has to say so rather than start and lose the data
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::ShardCountMismatch {
                found: 4,
                expected: 5
            })
        ));
    }
}
