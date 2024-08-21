//! The different storage backends supported by shoal

use rkyv::{
    ser::serializers::{
        AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec, Archive, Deserialize, Serialize,
};
use std::{collections::BTreeMap, path::PathBuf};

use crate::{
    server::Conf,
    shared::traits::{Archivable, ShoalTable},
};

mod fs;

pub use fs::FileSystem;

use super::partitions::Partition;

/// The different types of entries in a shoal intent log
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum Intents<T: std::fmt::Debug + Archive> {
    Insert(T),
}

pub trait ShoalStorage<D: ShoalTable + Archive> {
    /// The type we are storing
    //type Data: std::fmt::Debug + Archive;

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
    async fn insert(&mut self, insert: &Intents<D>);

    /// Flush all currently pending writes to storage
    async fn flush(&mut self);

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    async fn read_intents(path: &PathBuf, partitions: &mut BTreeMap<D::Sort, Partition<D>>);
}
