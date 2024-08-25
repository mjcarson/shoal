//! The different storage backends supported by shoal

use rkyv::{Archive, Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

use crate::{server::Conf, shared::traits::ShoalTable};

mod fs;

pub use fs::FileSystem;

use super::partitions::Partition;

/// The different types of entries in a shoal intent log
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum Intents<T: ShoalTable> {
    Insert(T),
    Delete {
        partition_key: u64,
        sort_key: T::Sort,
    },
}

pub trait ShoalStorage<T: ShoalTable> {
    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    async fn new(shard_name: &str, conf: &Conf) -> Self;

    /// Write this new row to our storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to insert
    async fn insert(&mut self, insert: &Intents<T>);

    /// Delete a row from storage
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key to the partition we are deleting data from
    /// * `sort_key` - The sort key to use to delete data from with in a partition
    async fn delete(&mut self, partition_key: u64, sort_key: T::Sort);

    /// Flush all currently pending writes to storage
    async fn flush(&mut self);

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    async fn read_intents(path: &PathBuf, partitions: &mut HashMap<u64, Partition<T>>);
}
