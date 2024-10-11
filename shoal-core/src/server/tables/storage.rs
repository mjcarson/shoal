//! The different storage backends supported by shoal

use rkyv::{Archive, Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

use crate::{
    server::{Conf, ServerError},
    shared::{queries::Update, traits::ShoalTable},
};

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
    Update(Update<T>),
}

pub trait ShoalStorage<T: ShoalTable>: Sized {
    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[allow(async_fn_in_trait)]
    async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError>;

    /// Write this new row to storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to insert
    #[allow(async_fn_in_trait)]
    async fn insert(&mut self, insert: &Intents<T>) -> Result<(), ServerError>;

    /// Delete a row from storage
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key to the partition we are deleting data from
    /// * `sort_key` - The sort key to use to delete data from with in a partition
    #[allow(async_fn_in_trait)]
    async fn delete(&mut self, partition_key: u64, sort_key: T::Sort) -> Result<(), ServerError>;

    /// Write a row update to storage
    ///
    /// # Arguments
    ///
    /// * `update` - The update that was applied to our row
    #[allow(async_fn_in_trait)]
    async fn update(&mut self, update: Update<T>) -> Result<(), ServerError>;

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError>;

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    #[allow(async_fn_in_trait)]
    async fn read_intents(
        path: &PathBuf,
        partitions: &mut HashMap<u64, Partition<T>>,
    ) -> Result<(), ServerError>;
}
