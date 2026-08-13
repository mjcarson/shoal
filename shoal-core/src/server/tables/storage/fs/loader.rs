//! Loads partitions from archives on a filesystem

use std::sync::Arc;
use std::time::Duration;

use futures::select;
use futures::stream::{FuturesUnordered, StreamExt};
use glommio::io::{DmaFile, ReadResult};
use glommio::{GlommioError, Task, TaskQueueHandle};
use kanal::{AsyncReceiver, AsyncSender};
use tracing::{event, instrument, Level};

use crate::server::messages::{LoadedPartition, LoadedPartitionKinds, ServerMsg};
use crate::server::{ServerError, ShoalError};
use crate::shared::traits::ShoalDatabase;
use crate::storage::fs::map::ArchiveEntry;
use crate::storage::fs::ArchiveMap;
use crate::storage::{FilteredFullArchiveMap, LoaderMsg};

/// How many times a partition read is attempted before it is given up on
///
/// Only a [`LoadFailure::Retryable`] failure is ever attempted more than once, and the queries
/// waiting on the partition are stalled for the whole of it, so this is deliberately small. It
/// exists to ride out a momentary shortage of file descriptors, not to wait out a broken disk.
const MAX_LOAD_ATTEMPTS: u8 = 3;

/// How long to wait between attempts at a partition read
const LOAD_RETRY_BACKOFF: Duration = Duration::from_millis(2);

/// What a failed partition read means for the queries waiting on it
///
/// The classes differ in whether reading again could ever produce a different answer. That is
/// the only question the loader can settle on its own, and it is the one that decides whether a
/// retry is worth the delay it costs every other query behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoadFailure {
    /// This partition is in no archive, so there is nothing to read
    ///
    /// This is the time-of-check-to-time-of-use race that [`super::FileSystem::load_partition`]
    /// deliberately allows: the compactor pruned the entry between that check and this read.
    /// It is not an error - the partition really is gone, and a query replayed against it
    /// answers correctly by finding nothing.
    Absent,
    /// The read failed for a reason that might not hold next time
    ///
    /// A shortage of file descriptors is the realistic one, since every in flight read holds a
    /// duplicated handle and nothing bounds how many of them there are.
    Retryable,
    /// The read failed for a reason that will hold no matter how often it is tried
    Fatal,
}

/// Decide what a partition read failure means for the queries waiting on it
///
/// Kept free of the database generic so it can be tested without standing up a schema.
///
/// # Arguments
///
/// * `error` - The error a partition read failed with
pub(super) fn classify(error: &ServerError) -> LoadFailure {
    // decide by whether reading again could ever answer differently
    match error {
        // the compactor pruned this partition out from under the read, so there is nothing
        // left to read and never will be
        ServerError::Shoal(ShoalError::PartitionNotFound { .. }) => LoadFailure::Absent,
        // an archive that could not be opened may open next time, since the realistic cause
        // is a shortage of file descriptors that other reads will give back
        ServerError::IO(_) | ServerError::GlommioIO { .. } => LoadFailure::Retryable,
        // everything else is structural - a table missing from the archive map was built
        // missing and stays missing, and a task queue that will not accept work is gone for
        // good - so retrying only spins
        _ => LoadFailure::Fatal,
    }
}

/// Help read a partition from disk
#[instrument(name = "loader::read_partition_helper", skip_all, err(Debug))]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub async fn read_partition_helper(
    archive: DmaFile,
    entry: ArchiveEntry,
) -> Result<ReadResult, GlommioError<()>> {
    // read our partition from disk
    let read_result = archive.read_at(entry.offset, entry.size).await;
    // close our archive regardless of whether the read failed or not
    archive.close().await?;
    read_result
}

/// Read a partition from disk, retrying a failure that might not hold
///
/// # Arguments
///
/// * `table_map` - The archive map for the table this partition belongs to
/// * `partition_id` - The partition to read
async fn read_with_retries(
    table_map: &Arc<ArchiveMap>,
    partition_id: u64,
) -> Result<ReadResult, ServerError> {
    // remember the last failure so it can be reported if every attempt fails
    let mut last = None;
    // try this read until it succeeds or we run out of attempts
    for attempt in 0..MAX_LOAD_ATTEMPTS {
        // wait before every attempt after the first, so a shortage this read is competing
        // for has a moment to clear
        if attempt > 0 {
            glommio::timer::sleep(LOAD_RETRY_BACKOFF).await;
        }
        // find where this partitions data lives, which the compactor can move or prune
        // between attempts, so it is looked up again each time
        match read_partition_once(table_map, partition_id).await {
            // this read succeeded so hand back its data
            Ok(read) => return Ok(read),
            Err(error) => {
                // a failure that will answer the same way next time is not worth retrying
                if classify(&error) != LoadFailure::Retryable {
                    return Err(error);
                }
                // log that we are going to try this read again
                event!(
                    Level::WARN,
                    msg = "Retrying a partition read",
                    partition_id,
                    attempt,
                    error = ?error,
                );
                // keep this failure in case it turns out to be our last
                last = Some(error);
            }
        }
    }
    // every attempt failed, so report the last failure we saw
    Err(last.expect("a read is attempted at least once"))
}

/// Read a partition from disk once
///
/// # Arguments
///
/// * `table_map` - The archive map for the table this partition belongs to
/// * `partition_id` - The partition to read
async fn read_partition_once(
    table_map: &Arc<ArchiveMap>,
    partition_id: u64,
) -> Result<ReadResult, ServerError> {
    // find which archive holds this partitions data
    let Some(entry) = table_map.find_partition(partition_id) else {
        // this partition is in no archive, so there is nothing to read
        return Err(ServerError::Shoal(ShoalError::PartitionNotFound {
            partition_id,
        }));
    };
    // get a handle to the archive holding it
    let archive = table_map.get_archive(&entry.archive).await?;
    // read this partition out of that archive
    let read = read_partition_helper(archive, entry).await?;
    Ok(read)
}

/// Read a partition from disk and tell our shard how it went
///
/// This always sends exactly one message to the shard - the partition on success, or a failure
/// on any error. That is what a query parked on this partition is waiting for, and nothing else
/// will ever send it, which is why this cannot be allowed to return without sending one.
///
/// The channel it borrowed is handed back so the next read can reuse it.
///
/// # Arguments
///
/// * `table` - The table this partition belongs to
/// * `partition_id` - The partition to read
/// * `table_map` - The archive map for that table
/// * `shard_local_tx` - The channel to send this reads outcome on
#[instrument(name = "loader::read_partition", skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn read_partition<D: ShoalDatabase>(
    table: D::TableNames,
    partition_id: u64,
    table_map: Arc<ArchiveMap>,
    shard_local_tx: AsyncSender<ServerMsg<D>>,
) -> AsyncSender<ServerMsg<D>> {
    // try to read this partition from disk, and build the message that says how it went
    let msg = match read_with_retries(&table_map, partition_id).await {
        // wrap our loaded partition so we can keep track of the table this is for
        Ok(data) => ServerMsg::Partition(LoadedPartitionKinds {
            table,
            loaded: LoadedPartition { partition_id, data },
        }),
        // this read failed, so tell our shard to release the queries waiting on it
        Err(error) => {
            // say how loudly this failure deserves to be said
            //
            // a pruned partition is expected on any workload that deletes, so reporting it at
            // error level would bury the failures that mean something
            match classify(&error) {
                LoadFailure::Absent => event!(
                    Level::DEBUG,
                    msg = "Partition was pruned before it could be read",
                    partition_id,
                ),
                _ => event!(
                    Level::ERROR,
                    msg = "Giving up on a partition read",
                    partition_id,
                    error = ?error,
                ),
            }
            ServerMsg::PartitionLoadFailed {
                table,
                partition_id,
            }
        }
    };
    // tell our shard how this read went, which can only fail if our shard is already gone
    if let Err(error) = shard_local_tx.send(msg).await {
        // log that the queries parked on this partition can no longer be released
        event!(
            Level::ERROR,
            msg = "Could not report a partition read",
            partition_id,
            error = ?error,
        );
    }
    // hand our channel back for the next read to reuse
    shard_local_tx
}

pub struct FsLoader<D: ShoalDatabase> {
    /// A task queue to schedule on
    medium_priority: TaskQueueHandle,
    /// A map of the archives for this shard
    table_map: FilteredFullArchiveMap<D::TableNames, ArchiveMap>,
    /// A channel to read Load Requests from
    loader_rx: AsyncReceiver<LoaderMsg<D::TableNames>>,
    /// The channel to send shard local messages on
    shard_local_tx: AsyncSender<ServerMsg<D>>,
    /// A set of sender channels to reuse
    senders: Vec<AsyncSender<ServerMsg<D>>>,
    /// A set of loader tasks
    ///
    /// A read task cannot fail: it reports every outcome to the shard itself and hands its
    /// channel back, so there is no error here for the loader to have to decide about.
    tasks: FuturesUnordered<Task<AsyncSender<ServerMsg<D>>>>,
}

impl<D: ShoalDatabase> FsLoader<D> {
    /// Create a new Filesystem Loader
    pub async fn new(
        medium_priority: &TaskQueueHandle,
        table_map: FilteredFullArchiveMap<D::TableNames, ArchiveMap>,
        loader_rx: &AsyncReceiver<LoaderMsg<D::TableNames>>,
        shard_local_tx: &AsyncSender<ServerMsg<D>>,
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

    /// Spawn a task to read one partition from disk
    ///
    /// The read itself happens in the spawned task rather than here, so a slow or failing
    /// archive delays only its own partition instead of every request queued behind it.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The table the partition to read belongs to
    /// * `partition_id` - The partition to read
    #[instrument(name = "Fsloader::spawn_task", skip_all, err(Debug))]
    async fn spawn_task(
        &mut self,
        table_name: D::TableNames,
        partition_id: u64,
    ) -> Result<(), ServerError> {
        // get the archive map for this table, cloned so the task can own it and this
        // borrow can be dropped before anything is awaited
        let table_map = self.table_map.get_table_map(table_name)?;
        // reuse an existing sender or clone a new one
        let shard_local_tx = self
            .senders
            .pop()
            .unwrap_or_else(|| self.shard_local_tx.clone());
        // spawn a task to load this partition
        //
        // a spawn that fails takes the sender we just borrowed down with the future, which
        // costs the reuse pool one entry. That is left alone because the only thing that
        // fails this call is a task queue that is gone, and a loader with no queue to run
        // tasks on has no further use for the pool
        let task = glommio::spawn_local_into(
            async move {
                // try to load this partition from disk
                read_partition(table_name, partition_id, table_map, shard_local_tx).await
            },
            self.medium_priority,
        )?;
        // add this task to our task set
        self.tasks.push(task);
        Ok(())
    }

    /// Tell our shard that a partition could not be read
    ///
    /// A load completing is the only thing that drains a tables blocked map, so this is what
    /// stops a failed read leaving its queries parked forever.
    ///
    /// # Arguments
    ///
    /// * `table` - The table the partition that could not be read belongs to
    /// * `partition_id` - The partition that could not be read
    async fn report_failure(&self, table: D::TableNames, partition_id: u64) {
        // build the failure message for this partition
        let msg = ServerMsg::PartitionLoadFailed {
            table,
            partition_id,
        };
        // tell our shard, which can only fail if our shard is already gone
        if let Err(error) = self.shard_local_tx.send(msg).await {
            // log that we could not report this failure
            event!(
                Level::ERROR,
                msg = "Could not report a failed partition read",
                partition_id,
                error = ?error,
            );
        }
    }

    /// Start loading partitions from disk
    pub async fn start(mut self) -> Result<(), ServerError> {
        // keep handling loader messeges until we get a shutdown command
        loop {
            // wait for a message on our mesh
            let msg = self.loader_rx.recv().await?;
            // handle this message
            match msg {
                LoaderMsg::Request {
                    table_name,
                    partition_id,
                } => {
                    // try to spawn this task
                    if let Err(error) = self.spawn_task(table_name, partition_id).await {
                        // this read never started, so nothing else is going to tell our shard
                        // about it and the queries parked on it would wait forever
                        event!(
                            Level::ERROR,
                            msg = "Could not start a partition read",
                            partition_id,
                            error = ?error,
                        );
                        // release the queries waiting on this partition
                        self.report_failure(table_name, partition_id).await;
                    }
                }
                // shutdown this loader
                LoaderMsg::Shutdown => {
                    // wait for all of our current tasks to finish then exit
                    while let Some(sender) = self.tasks.next().await {
                        // save this channel sender for reuse
                        self.senders.push(sender);
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
                            // a task finished, so save its channel sender for reuse
                            Some(sender) => self.senders.push(sender),
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
