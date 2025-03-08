//! The file system storage module for shoal

use byte_unit::Byte;
use conf::FileSystemTableConf;
use futures::stream::FuturesUnordered;
use futures::{AsyncWriteExt, StreamExt};
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use glommio::{Task, TaskQueueHandle};
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::Archive;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::instrument;

mod compactor;
pub mod conf;
mod map;
mod reader;

use compactor::FileSystemCompactor;
use map::ArchiveMap;
use reader::IntentLogReader;

use super::{CompactionJob, Intents, ShoalStorage};
use crate::server::conf::TableSettings;
use crate::server::messages::QueryMetadata;
use crate::shared::responses::{Response, ResponseAction};
use crate::{
    server::{Conf, ServerError},
    shared::{queries::Update, traits::ShoalTable},
    storage::ArchivedIntents,
    tables::partitions::Partition,
};

/// The max size for our data intent log
const INTENT_LIMIT: Byte = Byte::MEBIBYTE.multiply(50).unwrap();

/// Store shoal data in an existing filesytem for persistence
pub struct FileSystem<T: ShoalTable> {
    /// The name of the shard we are storing data for
    shard_name: String,
    /// The path to our current intent log
    intent_path: PathBuf,
    /// The intent log to write too
    intent_log: DmaStreamWriter,
    /// The current intent log generation
    generation: u64,
    /// The still pending writes
    pending: VecDeque<(u64, QueryMetadata, ResponseAction<T>)>,
    /// The medium priority task queue
    medium_priority: TaskQueueHandle,
    /// The config for this table
    pub table_conf: FileSystemTableConf,
    /// The channel to send intent log compactions on
    pub intent_tx: AsyncSender<CompactionJob>,
    /// The different tasks spawned by this shards file system storage engine
    pub tasks: FuturesUnordered<Task<Result<(), ServerError>>>,
    /// The shard local shared map of archive/partition data
    map: Arc<ArchiveMap>,
}

impl<T: ShoalTable + 'static> FileSystem<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Sort as Archive>::Archived: rkyv::Deserialize<T::Sort, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Update as Archive>::Archived:
        rkyv::Deserialize<T::Update, Strategy<Pool, rkyv::rancor::Error>>,
    <<T as ShoalTable>::Sort as Archive>::Archived: Ord,
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

    /// Spawn a compactor on this shard
    async fn spawn_intent_compactor(
        &mut self,
        compact_rx: AsyncReceiver<CompactionJob>,
    ) -> Result<(), ServerError> {
        // build a compactor
        let compactor =
            FileSystemCompactor::<T>::with_capacity(&self.table_conf, compact_rx, &self.map, 1000)
                .await?;
        // spawn this compactor
        let compactor_handle = glommio::spawn_local_into(
            async move { compactor.start().await },
            self.medium_priority,
        )?;
        // add this compactor to our task list
        self.tasks.push(compactor_handle);
        Ok(())
    }

    /// Get a new stream writer for this shard
    ///
    /// # Arguments
    ///
    /// * `name` - This shards name
    /// * `table_conf` - The config for this table's storage engine
    async fn new_writer(
        intent_path: &PathBuf,
        table_conf: &FileSystemTableConf,
    ) -> Result<DmaStreamWriter, ServerError> {
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&intent_path)
            .await?;
        // wrap our file in a stream writer
        let writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(table_conf.latency_sensitive.buffer_size)
            .with_write_behind(table_conf.latency_sensitive.write_behind)
            .build();
        Ok(writer)
    }

    /// Check if we need to compact the current intent log
    async fn is_compactable(&mut self) -> Result<u64, ServerError> {
        // check if this intent log is over 50MiB
        if self.intent_log.current_pos() > INTENT_LIMIT {
            // flush this intent log
            self.flush().await?;
            // get the current flushed position
            let flushed_pos = self.intent_log.current_flushed_pos();
            // close our intent log
            self.intent_log.close().await?;
            // get our base intent path
            let mut new_path = self.table_conf.get_intent_path(T::name());
            // build the file name to rename our current intent log too
            let name = format!("{}-inactive-{}", self.shard_name, self.generation);
            // build the path to this shards new intent log
            new_path.push(name);
            // rename our old intent log
            glommio::io::rename(&self.intent_path, &new_path).await?;
            // create an intent log compaction job
            self.intent_tx
                .send(CompactionJob::IntentLog(new_path))
                .await?;
            // get a new writer
            let new_writer = Self::new_writer(&self.intent_path, &self.table_conf).await?;
            // set our new writer
            self.intent_log = new_writer;
            // increment our writer generation
            self.generation += 1;
            // increment the attempt
            self.intent_tx.send(CompactionJob::Archives).await?;
            Ok(flushed_pos)
        } else {
            // get the current position of flushed data
            let flushed_pos = self.intent_log.current_flushed_pos();
            Ok(flushed_pos)
        }
    }
}

impl<T: ShoalTable + 'static> ShoalStorage<T> for FileSystem<T>
where
    <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Sort as Archive>::Archived: rkyv::Deserialize<T::Sort, Strategy<Pool, rkyv::rancor::Error>>,
    <T::Update as Archive>::Archived:
        rkyv::Deserialize<T::Update, Strategy<Pool, rkyv::rancor::Error>>,
    <<T as ShoalTable>::Sort as Archive>::Archived: Ord,
{
    /// The settings for this storage engine
    type Settings = FileSystemTableConf;

    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    #[instrument(name = "ShoalStorage::<FileSystem>::new", skip(conf), err(Debug))]
    async fn new(
        shard_name: &str,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError> {
        // get this tables config
        let table_conf = Self::get_settings(conf)?;
        // setup our paths
        table_conf.setup_paths(T::name()).await?;
        // build the path to this shards intent log
        let mut intent_path = table_conf.get_intent_path(T::name());
        // add our shard name
        intent_path.push(format!("{shard_name}-active"));
        // build the writer for this shards intent log
        let intent_log = Self::new_writer(&intent_path, &table_conf).await?;
        // build the channel to our compactor
        let (intent_tx, intent_rx) = kanal::unbounded_async();
        // get this shards shared archive map
        let map = Arc::new(ArchiveMap::new(shard_name, T::name(), &table_conf).await?);
        // build our file system storage module
        let mut fs = FileSystem {
            shard_name: shard_name.to_owned(),
            intent_path,
            intent_log,
            generation: 0,
            pending: VecDeque::with_capacity(1000),
            medium_priority,
            table_conf,
            intent_tx,
            tasks: FuturesUnordered::default(),
            map,
        };
        // spawn our intent compactor
        fs.spawn_intent_compactor(intent_rx).await?;
        Ok(fs)
    }

    /// Get a tables config or use default settings
    ///
    /// # Arguments
    ///
    /// * `config` - The shoal config to get settings from
    fn get_settings(conf: &Conf) -> Result<Self::Settings, ServerError> {
        match conf.storage.tables.get(T::name()) {
            // make sure these are the right type of settings
            Some(conf_enum) => match conf_enum {
                TableSettings::FS(table_conf) => Ok(table_conf.clone()),
            },
            None => Ok(conf.storage.default.filesystem.clone()),
        }
    }

    /// Write this new row to our storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to write
    #[instrument(name = "ShoalStorage::<FileSystem>::insert", skip_all, err(Debug))]
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
    #[instrument(name = "ShoalStorage::<FileSystem>::delete", skip(self), err(Debug))]
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
    #[instrument(name = "ShoalStorage::<FileSystem>::update", skip_all, err(Debug))]
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
    async fn get_flushed(
        &mut self,
        flushed: &mut Vec<(SocketAddr, Response<T>)>,
    ) -> Result<(), ServerError> {
        // check if our current intent log large enough to be compacted
        // this will also get the current flushed position
        let flushed_pos = self.is_compactable().await?;
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
        Ok(())
    }

    /// Flush all currently pending writes to storage
    #[instrument(name = "ShoalStorage::<FileSystem>::flush", skip_all, err(Debug))]
    async fn flush(&mut self) -> Result<(), ServerError> {
        self.intent_log.sync().await?;
        Ok(())
    }

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    #[instrument(
        name = "ShoalStorage::<FileSystem>::read_intents",
        skip(conf, partitions),
        err(Debug)
    )]
    async fn read_intents(
        shard_name: &str,
        conf: &Conf,
        partitions: &mut HashMap<u64, Partition<T>>,
    ) -> Result<(), ServerError> {
        // get this tables settings
        let table_conf = Self::get_settings(conf)?;
        // get our intent log path for this table
        let mut intent_path = table_conf.get_intent_path(T::name());
        // add our shard name
        intent_path.push(shard_name);
        // create an intent log reader
        let mut reader = IntentLogReader::new(&intent_path).await?;
        // iterate over the entries in this intent log
        while let Some(read) = reader.next_buff().await? {
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
                    let entry = partitions.entry(key).or_insert_with(|| Partition::new(key));
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
        }
        Ok(())
    }

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(&mut self) -> Result<(), ServerError> {
        // flush any remaining intent log writes to disk
        self.flush().await?;
        // signal our intent log compactor to shutdown
        self.intent_tx.send(CompactionJob::Shutdown).await?;
        // wait for all of our tasks to complete
        while let Some(task) = self.tasks.next().await {
            // check if this task has failed
            task?;
        }
        Ok(())
    }
}
