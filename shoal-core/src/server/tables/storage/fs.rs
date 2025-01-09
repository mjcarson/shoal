//! The file system storage module for shoal

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::Archive;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::instrument;

use super::{Intents, ShoalStorage};
use crate::server::messages::QueryMetadata;
use crate::shared::responses::{Response, ResponseAction};
use crate::{
    server::{Conf, ServerError},
    shared::{queries::Update, traits::ShoalTable},
    storage::ArchivedIntents,
    tables::partitions::Partition,
};

/// Store shoal data in an existing filesytem for persistence
pub struct FileSystem<T: ShoalTable> {
    /// The name of the shard we are storing data for
    shard_name: String,
    /// The intent log to write too
    intent_log: DmaStreamWriter,
    /// The still pending writes
    pending: VecDeque<(u64, QueryMetadata, ResponseAction<T>)>,
}

impl<T: ShoalTable> FileSystem<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Write an intent to storage
    ///
    /// # Arguments
    ///
    /// * `intent` - The intent to write to disk
    #[instrument(name = "FileSystem::write_intent", skip_all, fields(shard = &self.shard_name), err(Debug))]
    async fn write_intent(&mut self, intent: &Intents<T>) -> Result<usize, ServerError> {
        // archive this intent log entry
        let archived = rkyv::to_bytes::<_>(intent)?;
        // get the size of the data to write
        let size = archived.len();
        // write our size
        self.intent_log.write_all(&size.to_le_bytes()).await?;
        // write our data
        self.intent_log.write_all(archived.as_slice()).await?;
        Ok(size)
    }
}

impl<T: ShoalTable> ShoalStorage<T> for FileSystem<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Sort as Archive>::Archived: rkyv::Deserialize<T::Sort, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Update as Archive>::Archived:
        rkyv::Deserialize<T::Update, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[instrument(name = "ShoalStorage::new", skip(conf), err(Debug))]
    async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError> {
        // build the path to this shards intent log
        let path = conf
            .storage
            .fs
            .path
            .join(format!("Intents/{shard_name}-active"));
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await?;
        // wrap our file in a stream writer
        let intent_log = DmaStreamWriterBuilder::new(file)
            //.with_buffer_size(500)
            .build();
        // build our file system storage module
        let fs = FileSystem {
            shard_name: shard_name.to_owned(),
            intent_log,
            pending: VecDeque::with_capacity(1000),
        };
        Ok(fs)
    }

    /// Write this new row to our storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to write
    #[instrument(name = "ShoalStorage::insert", skip_all, err(Debug))]
    async fn insert(&mut self, insert: &Intents<T>) -> Result<u64, ServerError> {
        // write this insert intent log
        self.write_intent(insert).await?;
        // get the current position of the stream writer
        let current = self.intent_log.current_pos();
        Ok(current)
    }

    /// Delete a row from storage
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key to the partition we are deleting data from
    /// * `sort_key` - The sort key to use to delete data from with in a partition
    #[instrument(name = "ShoalStorage::delete", skip(self), err(Debug))]
    async fn delete(&mut self, partition_key: u64, sort_key: T::Sort) -> Result<u64, ServerError> {
        // wrap this delete command in a delete intent
        let intent = Intents::<T>::Delete {
            partition_key,
            sort_key,
        };
        // write this delete intent log
        self.write_intent(&intent).await?;
        // get the current position of the stream writer
        let current = self.intent_log.current_pos();
        Ok(current)
    }

    /// Write a row update to storage
    ///
    /// # Arguments
    ///
    /// * `update` - The update that was applied to our row
    #[instrument(name = "ShoalStorage::update", skip_all, err(Debug))]
    async fn update(&mut self, update: Update<T>) -> Result<u64, ServerError> {
        // build our intent log update entry
        let intent = Intents::Update(update);
        // write this delete intent log
        self.write_intent(&intent).await?;
        // get the current position of the stream writer
        let current = self.intent_log.current_pos();
        Ok(current)
    }

    /// Add a pending response action thats data is still being flushed
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for this query
    /// * `pos` - The position at which this entry will have been flushed to disk
    /// * `response` - The pending response action
    fn add_pending(&mut self, meta: QueryMetadata, pos: u64, response: ResponseAction<T>) {
        // add this pending action to our pending queue
        self.pending.push_back((pos, meta, response));
    }

    /// Get all flushed response actions
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed actions to return
    fn get_flushed(&mut self, flushed: &mut Vec<(SocketAddr, Response<T>)>) {
        // get the current position of flushed data
        let flushed_pos = self.intent_log.current_flushed_pos();
        // keep popping response actions until we find one that isn't yet flushed
        // or we have no more response actions to check
        while !self.pending.is_empty() {
            // check if the first item has been flushed
            let is_flushed = match self.pending.front() {
                Some((pending_pos, _, _)) => flushed_pos >= *pending_pos,
                None => break,
            };
            // if this action has been flushed to disk then pop it
            if is_flushed {
                // pop this flushed action
                if let Some((_, meta, data)) = self.pending.pop_front() {
                    // build the response for this query
                    let response = Response {
                        id: meta.id,
                        index: meta.index,
                        data,
                        end: meta.end,
                    };
                    // add this action to our flushed vec
                    flushed.push((meta.addr, response));
                }
            } else {
                // we don't have any flushed data yet
                break;
            }
        }
    }

    /// Flush all currently pending writes to storage
    #[instrument(name = "ShoalStorage::flush", skip_all, err(Debug))]
    async fn flush(&mut self) -> Result<(), ServerError> {
        self.intent_log.sync().await?;
        Ok(())
    }

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    #[instrument(name = "ShoalStorage::read_intents", skip(partitions), err(Debug))]
    async fn read_intents(
        path: &PathBuf,
        partitions: &mut HashMap<u64, Partition<T>>,
    ) -> Result<(), ServerError> {
        // open the target intent log
        let file = DmaFile::open(path).await?;
        // get the size of this file
        let file_size = file.file_size().await?;
        // don't read a file with 0 bytes
        if file_size == 0 {
            // close this file handle
            file.close().await.unwrap();
            // bail out since this shards intent log is empty
            return Ok(());
        }
        // track the amount of data we have currently read
        let mut pos = 0;
        while pos < file_size {
            // try to read the size of the next entry in this intent log
            let read = file.read_at(pos, 8).await?;
            // check if we read any data
            let size = usize::from_le_bytes(read[..8].try_into()?);
            // increment our pos 8
            pos += 8;
            // if size is 0 then skip to the next read
            if size == 0 {
                continue;
            }
            let read = file.read_at(pos, size).await?;
            // try to deserialize this row from our intent log
            let intent = unsafe { rkyv::access_unchecked::<ArchivedIntents<T>>(&read[..]) };
            // add this intent to our btreemap
            match intent {
                ArchivedIntents::Insert(archived) => {
                    // deserialize this row
                    let row = T::deserialize(archived)?;
                    // get the partition key for this row
                    let key = row.get_partition_key();
                    // get this rows partition
                    let entry = partitions.entry(key).or_default();
                    // insert this row
                    entry.insert(row);
                }
                ArchivedIntents::Delete {
                    partition_key,
                    sort_key,
                } => {
                    // convert our partition key to its native endianess
                    let partition_key = partition_key.to_native();
                    // get the partition to delete a row from
                    if let Some(partition) = partitions.get_mut(&partition_key) {
                        // deserialize this rows sort key
                        let sort_key = rkyv::deserialize::<T::Sort, rkyv::rancor::Error>(sort_key)?;
                        // remove the sort key from this partition
                        partition.remove(&sort_key);
                    }
                }
                ArchivedIntents::Update(archived) => {
                    // deserialize this row's update
                    let update = rkyv::deserialize::<Update<T>, rkyv::rancor::Error>(archived)?;
                    // try to get the partition containing our target row
                    if let Some(partition) = partitions.get_mut(&update.partition_key) {
                        // find our target row
                        if !partition.update(&update) {
                            panic!("Missing row update?");
                        }
                    }
                }
            }
            pos += size as u64;
        }
        // close this file handle
        file.close().await.unwrap();
        Ok(())
    }
}
