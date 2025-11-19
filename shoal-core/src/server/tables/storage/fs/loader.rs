//! Loads partitions from archives on a filesystem

use futures::select;
use futures::stream::{FuturesUnordered, StreamExt};
use glommio::io::{DmaFile, ReadResult};
use glommio::{GlommioError, Task, TaskQueueHandle};
use kanal::{AsyncReceiver, AsyncSender};
use tracing::instrument;

use crate::server::messages::{LoadedPartition, LoadedPartitionKinds, ShardMsg};
use crate::server::ServerError;
use crate::shared::traits::ShoalDatabase;
use crate::storage::fs::map::ArchiveEntry;
use crate::storage::fs::ArchiveMap;
use crate::storage::{FilteredFullArchiveMap, LoaderMsg};

/// Help read a partition from disk
#[instrument(name = "loader::read_partition_helper", skip_all, err(Debug))]
async fn read_partition_helper(
    archive: DmaFile,
    entry: ArchiveEntry,
) -> Result<ReadResult, GlommioError<()>> {
    // read our partition from disk
    let read_result = archive.read_at(entry.offset, entry.size).await;
    // close our archive regardless of whether the read failed or not
    archive.close().await?;
    read_result
}

/// Read a partition from disk
#[instrument(name = "loader::read_partition", skip_all, err(Debug))]
async fn read_partition<D: ShoalDatabase>(
    table: D::TableNames,
    partition_id: u64,
    archive: DmaFile,
    entry: ArchiveEntry,
    shard_local_tx: AsyncSender<ShardMsg<D>>,
) -> Result<AsyncSender<ShardMsg<D>>, ServerError> {
    // try to load our parititon from disk
    let data = read_partition_helper(archive, entry).await?;
    // build the partition load result
    let loaded = LoadedPartition { partition_id, data };
    // wrap our loaded partition so we can keep track of the table this is for
    let wrapped = LoadedPartitionKinds { table, loaded };
    // wrap our read result in a shard message
    let msg = ShardMsg::Partition(wrapped);
    // send this partition over our shard local channel
    shard_local_tx.send(msg).await.unwrap();
    // return our channel sender to be reused
    Ok(shard_local_tx)
}

pub struct FsLoader<D: ShoalDatabase> {
    /// A task queue to schedule on
    medium_priority: TaskQueueHandle,
    /// A map of the archives for this shard
    table_map: FilteredFullArchiveMap<D::TableNames, ArchiveMap>,
    /// A channel to read Load Requests from
    loader_rx: AsyncReceiver<LoaderMsg<D::TableNames>>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ShardMsg<D>>,
    /// A set of sender channels to reuse
    senders: Vec<AsyncSender<ShardMsg<D>>>,
    /// A set of loader tasks
    tasks: FuturesUnordered<Task<Result<AsyncSender<ShardMsg<D>>, ServerError>>>,
}

impl<D: ShoalDatabase> FsLoader<D> {
    /// Create a new Filesystem Loader
    pub async fn new(
        medium_priority: &TaskQueueHandle,
        table_map: FilteredFullArchiveMap<D::TableNames, ArchiveMap>,
        loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        shard_local_tx: &AsyncSender<ShardMsg<D>>,
    ) -> Self {
        // build a filesystem loader
        FsLoader {
            medium_priority: medium_priority.clone(),
            table_map,
            loader_rx: loader_rx.clone(),
            shard_local_tx: shard_local_tx.clone(),
            senders: Vec::with_capacity(100),
            tasks: FuturesUnordered::default(),
        }
    }

    #[instrument(name = "Fsloader::spawn_task", skip_all, err(Debug))]
    async fn spawn_task(
        &mut self,
        table_name: D::TableNames,
        partition_id: u64,
    ) -> Result<(), ServerError> {
        // get the archive for this table and partition
        let (entry, archive) = self.table_map.get_archive(table_name, partition_id).await?;
        // reuse an existing sender or clone a new one
        let shard_local_tx = self
            .senders
            .pop()
            .unwrap_or_else(|| self.shard_local_tx.clone());
        // spawn a task to load this partition
        let task = glommio::spawn_local_into(
            async move {
                // try to load this partition from disk
                read_partition(table_name, partition_id, archive, entry, shard_local_tx).await
            },
            self.medium_priority,
        )?;
        // add this task to our task set
        self.tasks.push(task);
        Ok(())
    }

    /// Start loading partitions from disk
    pub async fn start(mut self) -> Result<(), ServerError> {
        // keep handling loader messeges until we get a shutdown command
        loop {
            println!("LOADER TASKS -> {}", self.tasks.len());
            // wait for a message on our mesh
            let msg = self.loader_rx.recv().await.unwrap();
            // handle this message
            match msg {
                LoaderMsg::Request {
                    table_name,
                    partition_id,
                } => {
                    // try to spawn this task
                    if let Err(_) = self.spawn_task(table_name, partition_id).await {
                        // add this back onto our loader channel
                        todo!("Add back onto loader channel");
                    }
                }
                // shutdown this loader
                LoaderMsg::Shutdown => {
                    // wait for all of our current tasks to finish then exit
                    while let Some(task_result) = self.tasks.next().await {
                        // log any errors
                        if let Err(error) = task_result {
                            // TODO: handle this error
                            panic!("Error: {error:#?}");
                        }
                    }
                    // exit this loader
                    break;
                }
            }
            // loop over our tasks until either none are ready yet or its empty
            loop {
                // check if a task is ready or not yet without blocking
                select! {
                    // get our task if our task list has one
                    task_opt = self.tasks.next() => {
                        match task_opt {
                            // a task finished check if it failed
                            Some(task_result) => {
                                match task_result {
                                    // save this channel sender for reuse
                                    Ok(sender) => self.senders.push(sender),
                                    // TODO: do something with this error
                                    Err(error) => panic!("{error:#?}"),
                                }
                            },
                            // no jobs in our task set so continue on
                            None => break,
                        }
                    }
                    // no futures are ready so continue on
                    default => {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
