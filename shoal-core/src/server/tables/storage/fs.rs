//! The file system storage module for shoal

use std::{collections::HashMap, marker::PhantomData, path::PathBuf};

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};

use super::{Intents, ShoalStorage};
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
    /// The data we are storing
    store_data: PhantomData<T>,
}

impl<T: ShoalTable> FileSystem<T> {
    /// Write an intent to storage
    ///
    /// # Arguments
    ///
    /// * `intent` - The intent to write to disk
    async fn write_intent(&mut self, intent: &Intents<T>) -> Result<(), ServerError> {
        println!("Write -> {intent:#?}");
        // archive this intent log entry
        let archived = rkyv::to_bytes::<_, 1024>(intent)?;
        // get the size of the data to write
        let size = archived.len().to_le_bytes();
        // write our size
        self.intent_log.write_all(&size).await?;
        // write our data
        self.intent_log.write_all(archived.as_slice()).await?;
        // flush our data
        self.flush().await;
        Ok(())
    }
}

impl<T: ShoalTable> ShoalStorage<T> for FileSystem<T> {
    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    async fn new(shard_name: &str, conf: &Conf) -> Result<Self, ServerError> {
        // build the path to this shards intent log
        let path = conf
            .storage
            .fs
            .path
            .join(format!("Intents/{shard_name}-active"));
        // open this file
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await?;
        // wrap our file in a stream writer
        let intent_log = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(500)
            .build();
        // build our file system storage module
        let fs = FileSystem {
            shard_name: shard_name.to_owned(),
            intent_log,
            store_data: PhantomData,
        };
        Ok(fs)
    }

    /// Write this new row to our storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to write
    async fn insert(&mut self, insert: &Intents<T>) -> Result<(), ServerError> {
        // write this insert intent log
        self.write_intent(insert).await
    }

    /// Delete a row from storage
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key to the partition we are deleting data from
    /// * `sort_key` - The sort key to use to delete data from with in a partition
    async fn delete(&mut self, partition_key: u64, sort_key: T::Sort) -> Result<(), ServerError> {
        // wrap this delete command in a delete intent
        let intent = Intents::<T>::Delete {
            partition_key,
            sort_key,
        };
        // write this delete intent log
        self.write_intent(&intent).await
    }

    /// Write a row update to storage
    ///
    /// # Arguments
    ///
    /// * `update` - The update that was applied to our row
    async fn update(&mut self, update: Update<T>) -> Result<(), ServerError> {
        // build our intent log update entry
        let intent = Intents::Update(update);
        // write this delete intent log
        self.write_intent(&intent).await
    }

    /// Flush all currently pending writes to storage
    async fn flush(&mut self) -> Result<(), ServerError> {
        self.intent_log.flush().await?;
        Ok(())
    }

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
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
            // bail out since this shards intent log is empty
            return Ok(());
        }
        // track the amount of data we have currently read
        let mut pos = 0;
        while pos < file_size {
            println!("{path:?}: {} -> POS -> {pos}", file_size);
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
            // try to read the next entry
            let read = file.read_at(pos, size).await?;
            // try to deserialize this row from our intent log
            let intent = unsafe { rkyv::archived_root::<Intents<T>>(&read[..]) };
            // add this intent to our btreemap
            match intent {
                ArchivedIntents::Insert(archived) => {
                    // deserialize this row
                    let row = T::deserialize(archived);
                    println!("INSERT -> {row:?}");
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
                    // get the partition to delete a row from
                    if let Some(partition) = partitions.get_mut(partition_key) {
                        // deserialize this row
                        let sort_key = T::deserialize_sort(sort_key);
                        println!("DELETE -> {partition_key}:{sort_key:?}");
                        // remove the sort key from this partition
                        partition.remove(&sort_key);
                    }
                }
                ArchivedIntents::Update(archived) => {
                    // deserialize this row's update
                    let update = T::deserialize_update(archived);
                    println!("Update -> {update:?}");
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
        Ok(())
    }
}
