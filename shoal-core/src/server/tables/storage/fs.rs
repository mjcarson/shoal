//! The file system storage module for shoal

use conf::FileSystemTableConf;
use futures::stream::FuturesUnordered;
use futures::{AsyncWriteExt, StreamExt};
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use glommio::{Task, TaskQueueHandle};
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::Archive;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

mod compactor;
pub mod conf;
mod loader;
mod map;
mod reader;

use compactor::FileSystemCompactor;
pub use map::ArchiveMap;
use reader::IntentLogReader;

use super::{CompactionJob, IntentReadSupport, StorageSupport};
use crate::server::conf::TableSettings;
use crate::server::messages::ShardMsg;
use crate::server::{Conf, ServerError};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport, ShoalDatabase, TableNameSupport};
use crate::storage::{ArchiveMapKinds, FilteredFullArchiveMap, FullArchiveMap, LoaderMsg, Loaders};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport};
use loader::FsLoader;

/// Store shoal data in an existing filesytem for persistence
pub struct FileSystem {
    /// The name of the shard we are storing data for
    shard_name: String,
    /// The path to our current intent log
    intent_path: PathBuf,
    /// The intent log to write too
    intent_log: DmaStreamWriter,
    /// The current intent log generation
    pub generation: u64,
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

impl FileSystem {
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

    /// Spawn a compactor on this shard
    async fn spawn_intent_compactor<
        T: IntentReadSupport<R> + 'static,
        R: PartitionKeySupport + 'static,
        S: ShoalDatabase,
    >(
        &mut self,
        table_name: S::TableNames,
        compact_rx: AsyncReceiver<CompactionJob>,
        shard_local_tx: &AsyncSender<ShardMsg<S>>,
    ) -> Result<(), ServerError>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <T as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
        for<'a> <T::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // build a compactor
        let compactor = FileSystemCompactor::<T, R, S>::with_capacity(
            table_name,
            &self.table_conf,
            compact_rx,
            shard_local_tx,
            &self.map,
            1000,
        )
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
}

impl StorageSupport for FileSystem {
    /// The settings for this storage engine
    type Settings = FileSystemTableConf;

    /// The archive map this storage engine uses
    type ArchiveMap = ArchiveMap;

    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    /// * `medium_priority` - The medium priority task queue
    #[allow(async_fn_in_trait)]
    async fn new<
        P: IntentReadSupport<R> + 'static,
        R: PartitionKeySupport + 'static,
        N: TableNameSupport,
        S: ShoalDatabase,
    >(
        shard_name: &str,
        table_name: N,
        shard_table_name: S::TableNames,
        shard_archive_map: &FullArchiveMap<N>,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
        shard_local_tx: &AsyncSender<ShardMsg<S>>,
    ) -> Result<Self, ServerError>
    where
        <P as Archive>::Archived: rkyv::Deserialize<P, Strategy<Pool, rkyv::rancor::Error>>,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
        for<'a> <P as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
        for<'a> <P::Intent as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // get this tables config
        let table_conf = Self::get_settings::<R>(conf)?;
        // setup our paths
        table_conf.setup_paths(R::name()).await?;
        // build the path to this shards intent log
        let mut intent_path = table_conf.get_intent_path(R::name());
        // add our shard name
        intent_path.push(format!("{shard_name}-active"));
        // build the writer for this shards intent log
        let intent_log = Self::new_writer(&intent_path, &table_conf).await?;
        // build the channel to our compactor
        let (intent_tx, intent_rx) = kanal::unbounded_async();
        // get this shards shared archive map
        let map = Arc::new(ArchiveMap::new(shard_name, R::name(), &table_conf).await?);
        // wrap a clone of our archive map in the filesystem kind
        let wrapped = ArchiveMapKinds::FileSystem(map.clone());
        // add our wrapped map to our shards full map
        shard_archive_map.insert(table_name, wrapped);
        // build our file system storage module
        let mut fs = FileSystem {
            shard_name: shard_name.to_owned(),
            intent_path,
            intent_log,
            generation: 0,
            medium_priority,
            table_conf,
            intent_tx,
            tasks: FuturesUnordered::default(),
            map,
        };
        // spawn our intent compactor
        fs.spawn_intent_compactor::<P, R, S>(shard_table_name, intent_rx, shard_local_tx)
            .await?;
        Ok(fs)
    }

    /// Get a tables config or use default settings
    ///
    /// # Arguments
    ///
    /// * `conf` - The shoal config to get settings from
    fn get_settings<R: PartitionKeySupport>(conf: &Conf) -> Result<Self::Settings, ServerError> {
        match conf.storage.tables.get(R::name()) {
            // make sure these are the right type of settings
            Some(conf_enum) => match conf_enum {
                TableSettings::FS(table_conf) => Ok(table_conf.clone()),
            },
            None => Ok(conf.storage.default.filesystem.clone()),
        }
    }

    /// Commit an operation to this storages intent log
    ///
    /// # Arguments
    ///
    /// * `data` - The data to commit
    #[allow(async_fn_in_trait)]
    async fn commit<I: RkyvSupport>(&mut self, data: &I) -> Result<u64, ServerError> {
        // serialize our data
        let archived = RkyvSupport::serialize(data);
        // get the size of the data to write
        let size = archived.len();
        // write our size
        self.intent_log.write_all(&size.to_le_bytes()).await?;
        // write our data
        self.intent_log.write_all(archived.as_slice()).await?;
        // get the current position of the stream writer
        let current = self.intent_log.current_pos();
        Ok(current)
    }

    /// Set our intent log to be compact if its needed
    ///
    /// Returns the current flushed position of the writer and the current generation
    ///
    /// # Arguments
    ///
    /// * `force` - Whether to force a compaction of the intent logs
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<R: PartitionKeySupport>(
        &mut self,
        force: bool,
    ) -> Result<(u64, u64), ServerError> {
        // get the latency sensistive max intent log size
        let max_size = self.table_conf.latency_sensitive.intent_log_size;
        // check if this intent log is over 50MiB or if compaction is being forced
        if force || self.intent_log.current_pos() > max_size {
            // flush this intent log
            self.flush().await?;
            // get the current flushed position
            let flushed_pos = self.intent_log.current_flushed_pos();
            // close our intent log
            self.intent_log.close().await?;
            // get our base intent path
            let mut new_path = self.table_conf.get_intent_path(R::name());
            // build the file name to rename our current intent log too
            let name = format!("{}-inactive-{}", self.shard_name, self.generation);
            // build the path to this shards new intent log
            new_path.push(name);
            // rename our old intent log
            glommio::io::rename(&self.intent_path, &new_path).await?;
            // create an intent log compaction job
            self.intent_tx
                .send(CompactionJob::IntentLog {
                    path: new_path,
                    generation: self.generation,
                })
                .await?;
            // get a new writer
            let new_writer = Self::new_writer(&self.intent_path, &self.table_conf).await?;
            // set our new writer
            self.intent_log = new_writer;
            // increment our writer generation
            self.generation += 1;
            // create an archive compaction job
            self.intent_tx.send(CompactionJob::Archives).await?;
            Ok((flushed_pos, self.generation))
        } else {
            // get the current position of flushed data
            let flushed_pos = self.intent_log.current_flushed_pos();
            Ok((flushed_pos, self.generation))
        }
    }

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError> {
        self.intent_log.sync().await?;
        Ok(())
    }

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The name of the shard to read intents for
    /// * `conf` - A Shoal config
    /// * `path` - The path to the intent log to read in
    #[allow(async_fn_in_trait)]
    async fn read_intents<P: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        shard_name: &str,
        conf: &Conf,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<P>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<(), ServerError> {
        // get this tables settings
        let table_conf = Self::get_settings::<R>(conf)?;
        // get our intent log path for this table
        let mut intent_path = table_conf.get_intent_path(R::name());
        // add our shard name
        // TODO load all of this shards intent files
        intent_path.push(format!("{shard_name}-active"));
        // create an intent log reader
        let mut reader = IntentLogReader::new(&intent_path).await?;
        // iterate over the entries in this intent log
        while let Some(read) = reader.next_buff().await? {
            // load this partitions data
            if <P as IntentReadSupport<R>>::load(&read, generation, partitions, memory_usage)
                .is_err()
            {
                panic!("Skipping intent data that was not fully committed");
            }
        }
        // close our reader
        reader.close().await?;
        Ok(())
    }

    /// Get the type of loader this storage kind requires
    fn loader_kind() -> Loaders {
        // return the filesystem loader
        Loaders::FileSystem
    }

    /// Spawn a loader for this storage type if not yet spawned
    async fn spawn_loader<D: ShoalDatabase>(
        &self,
        table_map: &FullArchiveMap<D::TableNames>,
        loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        shard_local_tx: &AsyncSender<ShardMsg<D>>,
    ) -> Result<(), ServerError> {
        // filter down to just our filesystem archive maps
        let filtered = FilteredFullArchiveMap::<D::TableNames, ArchiveMap>::from(table_map);
        // build a new filesystem loader
        let loader =
            FsLoader::new(&self.medium_priority, filtered, &loader_rx, shard_local_tx).await;
        // spawn our loader onto our medium priority task queue
        let task =
            glommio::spawn_local_into(async move { loader.start().await }, self.medium_priority)
                .unwrap();
        // add this task to our task queue
        self.tasks.push(task);
        Ok(())
    }

    /// Load a partition from disk if it exists
    ///
    /// Returns true if a partition exists and will be loaded from disk and
    /// false if it does not and wont.
    async fn load_partition<N: TableNameSupport>(
        &self,
        table_name: N,
        partition_id: u64,
        loader_tx: &AsyncSender<LoaderMsg<N>>,
    ) -> Result<bool, ServerError> {
        // check if this partition is in our archive map
        match self.map.find_partition(partition_id) {
            // we don't actually care about the entry yet but if the partition
            // doesn't yet exist then it hasn't been made yet. We don't want
            // to use the entry info yet to avoid ToCToU issues.
            Some(_) => {
                // send our partition load request
                loader_tx
                    .send(LoaderMsg::Request {
                        table_name,
                        partition_id,
                    })
                    .await?;
                // return true to let our caller know we are loading this
                // partition from disk
                Ok(true)
            }
            None => Ok(false),
        }
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
        // close any glommio files
        self.intent_log.close().await?;
        // close our archive map
        self.map.close_all().await?;
        Ok(())
    }
}
