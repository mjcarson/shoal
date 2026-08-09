//! The file system storage module for shoal

use conf::FileSystemTableConf;
use futures::stream::FuturesUnordered;
use futures::{AsyncWriteExt, StreamExt};
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions, ReadResult};
use glommio::{Task, TaskQueueHandle};
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::Archive;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{event, instrument, Level};

mod compactor;
pub mod conf;
mod loader;
pub(crate) mod map;
pub(crate) mod reader;
mod stream;
#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;

use compactor::FileSystemCompactor;
pub use map::ArchiveMap;
use reader::IntentLogReader;
use stream::StreamWriter;

use super::{
    CompactionJob, FlushProgress, IntentReadSupport, RecoveryStats, StorageSupport,
};
use crate::server::conf::TableSettings;
use crate::server::messages::ServerMsg;
use crate::server::{Conf, ServerError};
use crate::shared::traits::{PartitionKeySupport, RkyvSupport, ShoalDatabase, TableNameSupport};
use crate::storage::{ArchiveMapKinds, FilteredFullArchiveMap, FullArchiveMap, LoaderMsg, Loaders};
use crate::tables::partitions::{MaybeLoaded, PartitionSupport, ValidatedArchive};
use loader::FsLoader;

/// Find inactive intent log files for this shard, sorted by generation ascending
///
/// # Arguments
///
/// * `intent_dir` - The intent log directory to scan
/// * `shard_name` - The name of the shard to find inactive logs for
pub fn find_inactive_intent_logs(
    intent_dir: &PathBuf,
    shard_name: &str,
) -> Vec<(u64, PathBuf)> {
    let prefix = format!("{shard_name}-inactive-");
    let mut inactive_logs: Vec<(u64, PathBuf)> = Vec::new();
    // use std::fs::read_dir since this only runs during startup recovery
    if let Ok(entries) = std::fs::read_dir(intent_dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if let Some(gen_str) = name.strip_prefix(&prefix) {
                if let Ok(gen) = gen_str.parse::<u64>() {
                    inactive_logs.push((gen, entry.path()));
                }
            }
        }
    }
    // sort by generation ascending so we replay in order
    inactive_logs.sort_by_key(|(gen, _)| *gen);
    inactive_logs
}

/// Store shoal data in an existing filesytem for persistence
pub struct FileSystem<D: ShoalDatabase> {
    /// The name of the shard we are storing data for
    shard_name: String,
    /// The path to our current intent log
    intent_path: PathBuf,
    ///// The intent log to write too
    //intent_log: DmaStreamWriter,
    /// The intent log to write to
    intent_log2: StreamWriter<D>,
    /// The current intent log generation
    ///
    /// This starts at 1 rather than 0 so that a generation of 0 can be used to mean
    /// "no generation has been compacted yet" by anything gating on durability.
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

#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<D: ShoalDatabase> FileSystem<D> {
    ///// Get a new stream writer for this shard
    /////
    ///// # Arguments
    /////
    ///// * `name` - This shards name
    ///// * `table_conf` - The config for this table's storage engine
    //async fn new_writer(
    //    intent_path: &PathBuf,
    //    table_conf: &FileSystemTableConf,
    //) -> Result<DmaStreamWriter, ServerError> {
    //    // open this file
    //    // don't open with append or new writes will overwrite old ones
    //    let file = OpenOptions::new()
    //        .create(true)
    //        .read(true)
    //        .write(true)
    //        .dma_open(&intent_path)
    //        .await?;
    //    // wrap our file in a stream writer
    //    let writer = DmaStreamWriterBuilder::new(file)
    //        .with_buffer_size(table_conf.latency_sensitive.buffer_size)
    //        .with_write_behind(table_conf.latency_sensitive.write_behind)
    //        .build();
    //    Ok(writer)
    //}

    /// Spawn a compactor on this shard
    async fn spawn_intent_compactor<
        T: IntentReadSupport<R> + 'static,
        R: PartitionKeySupport + 'static,
    >(
        &mut self,
        table_name: D::TableNames,
        compact_rx: AsyncReceiver<CompactionJob>,
        shard_local_tx: &AsyncSender<ServerMsg<D>>,
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
        let compactor = FileSystemCompactor::<T, R, D>::with_capacity(
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

    /// Read a single intent log file, collecting its entries and what they need loaded
    ///
    /// This deliberately does not replay anything. Loading a partition an update
    /// names overwrites whatever is already in the partition map, so every log has
    /// to be read, and every partition loaded, before any log is replayed.
    ///
    /// # Arguments
    ///
    /// * `intent_path` - The path to the intent log file to read
    /// * `to_load` - The set of partition keys these intents need loaded
    /// * `stats` - The counts of what this recovery has discarded
    #[instrument(name = "FileSystem::read_intent_log", skip_all, err(Debug))]
    async fn read_intent_log<P: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        &self,
        intent_path: &PathBuf,
        to_load: &mut HashSet<u64>,
        stats: &mut RecoveryStats,
    ) -> Result<Vec<ReadResult>, ServerError> {
        // instance a vec to store this logs intent reads
        let mut reads = Vec::with_capacity(1000);
        // create an intent log reader
        let mut reader = IntentLogReader::new(intent_path).await?;
        // iterate over entries and collect the partitions we need to load
        while let Some(read) = reader.next_buff().await? {
            // name any partitions needed to properly handle updates
            <P as IntentReadSupport<R>>::scan_keys(&read, to_load)?;
            // add this read to our read list
            reads.push(read);
        }
        // a reader that stopped on a bad tail dropped everything after it
        if reader.truncated {
            // count this log as one we could not read to its end
            stats.truncated_logs += 1;
        }
        // close our reader
        reader.close().await?;
        Ok(reads)
    }

    /// Load every partition our intents need, skipping any we already hold
    ///
    /// # Arguments
    ///
    /// * `to_load` - The partition keys to load
    /// * `partitions` - The map of partitions to load into
    /// * `memory_usage` - The memory usage for this shard
    #[instrument(name = "FileSystem::load_scanned", skip_all, err(Debug))]
    async fn load_scanned<P: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        &self,
        to_load: HashSet<u64>,
        partitions: &mut HashMap<u64, MaybeLoaded<P>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<(), ServerError>
    where
        for<'a> <P as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // load each partition our intents named
        for partition_key in to_load {
            // never load over a partition we already hold, since ours is the newer copy
            if partitions.contains_key(&partition_key) {
                continue;
            }
            // get this partitions data
            if let Some(partition_read) = self.load_partition_direct(partition_key).await? {
                // validate this archive once, here, instead of on every query that reads it
                let archive = ValidatedArchive::new(partition_read)?;
                // update the memory usage for this partition, only once it is known to be good
                *memory_usage.borrow_mut() += archive.len();
                // wrap this partition as being accessible
                let wrapped = MaybeLoaded::Accessible(archive);
                // load this partition
                partitions.insert(partition_key, wrapped);
            }
        }
        Ok(())
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<D: ShoalDatabase> StorageSupport for FileSystem<D> {
    /// The settings for this storage engine
    type Settings = FileSystemTableConf;

    /// The archive map this storage engine uses
    type ArchiveMap = ArchiveMap;

    /// The database type this storage engine is associated with
    type Database = D;

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
    >(
        shard_name: &str,
        table_name: N,
        shard_table_name: D::TableNames,
        shard_archive_map: &FullArchiveMap<N>,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
        shard_local_tx: &AsyncSender<ServerMsg<D>>,
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
        let intent_log2 =
            StreamWriter::builder(&intent_path, shard_local_tx.clone())
                .buffer_size(table_conf.latency_sensitive.buffer_size)
                .write_behind(table_conf.latency_sensitive.write_behind)
                .durability(table_conf.latency_sensitive.durability)
                .build()
                .await?;
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
            intent_log2,
            // generations start at 1 so 0 can mean "nothing has been compacted yet"
            generation: 1,
            medium_priority,
            table_conf,
            intent_tx,
            tasks: FuturesUnordered::default(),
            map,
        };
        // spawn our intent compactor
        fs.spawn_intent_compactor::<P, R>(shard_table_name, intent_rx, shard_local_tx)
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
        // compute a checksum over our serialized data
        let mut hasher = GxHasher::default();
        hasher.write(archived.as_slice());
        let checksum = hasher.finish();
        // get our archived data as a slice
        let archived_slice = archived.as_slice();
        // get the total size of data were about to write
        // 16 bytes for size + checksum and then the size of our data
        let total_size = 16 + archived_slice.len();
        // get a buffer to write this commit data too
        let mut buff = self.intent_log2.prep(total_size).await;
        // write our size
        buff.write_all(&size.to_le_bytes())?;
        // write our checksum
        buff.write_all(&checksum.to_le_bytes())?;
        // write our data
        buff.write_all(archived.as_slice())?;
        // tell our writer that we have consumed some data
        self.intent_log2.consume(total_size).await;
        // return the offset one past this record, which is the position it will be
        // durable at once our writers watermark reaches it
        Ok(self.intent_log2.get_unflushed_pos())
    }

    /// Check if this intent log has grown past the size it rotates at
    ///
    /// Rotation is driven by bytes *accepted* rather than bytes durable, so this only
    /// reads the writers own position counters — no borrow of the shared flush state
    /// and no await. That is what makes it cheap enough for the shard to ask on every
    /// message before deciding whether to sweep its tables.
    fn compaction_due(&self) -> bool {
        // get the latency sensistive max intent log size
        let max_size = self.table_conf.latency_sensitive.intent_log_size;
        // our log is due to rotate once it has accepted more than that
        self.intent_log2.get_unflushed_pos() > max_size
    }

    /// Set our intent log to be compact if its needed
    ///
    /// Returns how far this tables intent log has been made durable
    ///
    /// # Arguments
    ///
    /// * `force` - Whether to force a compaction of the intent logs
    #[allow(async_fn_in_trait)]
    async fn compact_if_needed<R: PartitionKeySupport>(
        &mut self,
        force: bool,
    ) -> Result<FlushProgress, ServerError> {
        // surface any error our background write tasks hit
        self.intent_log2.check_error()?;
        // check if this intent log is too big or if compaction is being forced
        if force || self.compaction_due() {
            // get our base intent path
            let mut new_path = self.table_conf.get_intent_path(R::name());
            // build the file name to rename our current intent log too
            let name = format!("{}-inactive-{}", self.shard_name, self.generation);
            // build the path to this shards new intent log
            new_path.push(name);
            // refresh this writer to a write to a new file
            let flushed_pos = self.intent_log2.refresh(&new_path).await?;
            // create an intent log compaction job
            self.intent_tx
                .send(CompactionJob::IntentLog {
                    path: new_path,
                    generation: self.generation,
                })
                .await?;
            // increment our writer generation
            self.generation += 1;
            // create an archive compaction job
            self.intent_tx.send(CompactionJob::Archives).await?;
            Ok(FlushProgress {
                durable_pos: flushed_pos,
                generation: self.generation,
                rotated: true,
            })
        } else {
            // get the current position of durable data
            let flushed_pos = self.intent_log2.get_flushed_pos();
            Ok(FlushProgress {
                durable_pos: flushed_pos,
                generation: self.generation,
                rotated: false,
            })
        }
    }

    /// Flush all currently pending writes to storage
    #[allow(async_fn_in_trait)]
    async fn flush(&mut self) -> Result<(), ServerError> {
        // surface any error our background write tasks hit
        self.intent_log2.check_error()?;
        // sync our intent log to disk
        self.intent_log2.sync().await?;
        Ok(())
    }

    /// Read an intent log from storage
    ///
    /// Replays any inactive intent logs (from prior compaction cycles that
    /// did not complete) in generation order, then replays the active log.
    ///
    /// # Arguments
    ///
    /// * `conf` - A Shoal config
    /// * `generation` - The generation to replay these intents as
    /// * `partitions` - The map of partitions to load intents into
    /// * `memory_usage` - The memory usage for this shard
    #[allow(async_fn_in_trait)]
    #[instrument(
        name = "FileSystem::read_intents",
        skip(self, conf, partitions, memory_usage)
    )]
    async fn read_intents<P: IntentReadSupport<R> + PartitionSupport, R: PartitionKeySupport>(
        &self,
        conf: &Conf,
        generation: u64,
        partitions: &mut HashMap<u64, MaybeLoaded<P>>,
        memory_usage: &mut Arc<RefCell<usize>>,
    ) -> Result<RecoveryStats, ServerError>
    where
        for<'a> <P as Archive>::Archived: CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        // start with nothing discarded
        let mut stats = RecoveryStats::default();
        // get this tables settings
        let table_conf = Self::get_settings::<R>(conf)?;
        // get our intent log directory for this table
        let intent_dir = table_conf.get_intent_path(R::name());
        // find any inactive intent logs left by interrupted compactions
        let inactive_logs = find_inactive_intent_logs(&intent_dir, &self.shard_name);
        // our active log is always replayed last, after every generation before it
        let active_path = intent_dir.join(format!("{}-active", self.shard_name));
        // build the full ordered list of logs to recover
        let mut paths: Vec<&PathBuf> = inactive_logs.iter().map(|(_, path)| path).collect();
        paths.push(&active_path);
        // read every log up front, since replaying one and then loading a partition
        // for the next would drop that log straight over what we just replayed
        let mut to_load = HashSet::with_capacity(1000);
        let mut logs = Vec::with_capacity(paths.len());
        for path in paths {
            // log that we are recovering an intent log
            event!(Level::INFO, msg = "Recovering", path = path.to_str());
            // read this logs entries and note what they need loaded
            let reads = self
                .read_intent_log::<P, R>(path, &mut to_load, &mut stats)
                .await?;
            logs.push(reads);
        }
        // load every partition our intents named exactly once, before any replay
        self.load_scanned::<P, R>(to_load, partitions, memory_usage)
            .await?;
        // now replay every log in generation order over those partitions
        for reads in logs {
            for read in reads {
                // apply this intent, skipping it if it was never fully committed
                if let Err(err) = <P as IntentReadSupport<R>>::replay(
                    &read,
                    generation,
                    partitions,
                    memory_usage,
                    &mut stats,
                ) {
                    tracing::warn!("Skipping intent entry that was not fully committed: {err:#?}");
                    // count this entry as one we could not replay at all
                    stats.unreplayable_entries += 1;
                    continue;
                }
            }
        }
        // only now that every log has been replayed is it safe to drop any of them,
        // since a crash part way through this has to be able to start over
        for (gen, inactive_path) in &inactive_logs {
            // delete this inactive log now that its intents have been replayed
            glommio::io::remove(inactive_path).await?;
            // log that we have finished recovering an intent log
            event!(Level::INFO, msg = "Replayed and removed", gen);
        }
        Ok(stats)
    }

    /// Get the type of loader this storage kind requires
    fn loader_kind() -> Loaders {
        // return the filesystem loader
        Loaders::FileSystem
    }

    /// Spawn a loader for this storage type if not yet spawned
    async fn spawn_loader(
        &self,
        table_map: &FullArchiveMap<D::TableNames>,
        loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        shard_local_tx: &AsyncSender<ServerMsg<D>>,
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

    /// Load a partition from disk if it exists directly
    ///
    /// This doesn't use the loader channel and instead returns the Partition data.
    async fn load_partition_direct(
        &self,
        partition_id: u64,
    ) -> Result<Option<ReadResult>, ServerError> {
        // check if this partition is in our archive map
        match self.map.find_partition(partition_id) {
            // this partition exists
            Some(entry) => {
                // get this archives dma file
                let handle = self.map.get_archive(&entry.archive).await?;
                // read this partitions data from disk
                let read = loader::read_partition_helper(handle, entry).await?;
                Ok(Some(read))
            }
            None => Ok(None),
        }
    }

    /// Shutdown this storage engine
    #[allow(async_fn_in_trait)]
    async fn shutdown(mut self) -> Result<(), ServerError> {
        // durably flush any remaining intent log writes to disk
        //
        // this has to be the blocking sync, not `flush`, since `flush` only hands
        // another buffer to the kernel and returns without waiting
        self.intent_log2.sync_blocking().await?;
        // signal our intent log compactor to shutdown
        self.intent_tx.send(CompactionJob::Shutdown).await?;
        // wait for all of our tasks to complete
        while let Some(task) = self.tasks.next().await {
            // check if this task has failed
            task?;
        }
        // close any glommio files
        self.intent_log2.close().await?;
        // close our archive map
        self.map.close_all().await?;
        Ok(())
    }
}
