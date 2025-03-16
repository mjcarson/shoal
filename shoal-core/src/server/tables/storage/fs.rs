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
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

mod compactor;
pub mod conf;
mod map;
mod reader;

use compactor::FileSystemCompactor;
use map::ArchiveMap;
use reader::IntentLogReader;

use super::{CompactionJob, IntentReadSupport, StorageSupport};
use crate::server::conf::TableSettings;
use crate::server::{Conf, ServerError};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport};

/// The max size for our data intent log
const INTENT_LIMIT: Byte = Byte::MEBIBYTE.multiply(50).unwrap();

/// Store shoal data in an existing filesytem for persistence
pub struct FileSystem {
    /// The name of the shard we are storing data for
    shard_name: String,
    /// The path to our current intent log
    intent_path: PathBuf,
    /// The intent log to write too
    intent_log: DmaStreamWriter,
    /// The current intent log generation
    generation: u64,
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
        T: IntentReadSupport + 'static,
        R: PartitionKeySupport + 'static,
    >(
        &mut self,
        compact_rx: AsyncReceiver<CompactionJob>,
    ) -> Result<(), ServerError>
    where
        <T as Archive>::Archived: rkyv::Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        // build a compactor
        let compactor = FileSystemCompactor::<T, R>::with_capacity(
            &self.table_conf,
            compact_rx,
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

    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    /// * `medium_priority` - The medium priority task queue
    #[allow(async_fn_in_trait)]
    async fn new<P: IntentReadSupport + 'static, R: PartitionKeySupport + 'static>(
        shard_name: &str,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError>
    where
        <P as Archive>::Archived: rkyv::Deserialize<P, Strategy<Pool, rkyv::rancor::Error>>,
        <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
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
        fs.spawn_intent_compactor::<P, R>(intent_rx).await?;
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
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<R: PartitionKeySupport>(&mut self) -> Result<u64, ServerError> {
        // check if this intent log is over 50MiB
        if self.intent_log.current_pos() > INTENT_LIMIT {
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
    async fn read_intents<P: IntentReadSupport, R: PartitionKeySupport>(
        shard_name: &str,
        conf: &Conf,
        partitions: &mut HashMap<u64, P>,
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
            <P as IntentReadSupport>::load(&read, partitions)?;
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
