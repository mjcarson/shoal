//! Orchestrates the different compactors for a shard in Shoal

use std::marker::PhantomData;

use kanal::AsyncReceiver;

use crate::shared::traits::ShoalDatabase;

use super::ServerError;

pub enum CompactionMsg {
    /// An intent log to archive
    IntentCompaction,
    /// Shutdown this compaction controller
    Shutdown,
}

/// Orchestrates the different compactors for a shard in Shoal
pub struct CompactionController<S: ShoalDatabase> {
    /// The channel to listen for compaction jobs on
    compact_rx: AsyncReceiver<CompactionMsg>,
    /// The db we are compacting things for
    phantom: PhantomData<S>,
}

impl<S: ShoalDatabase> CompactionController<S> {
    /// Create a new compaction controller
    pub fn new(compact_rx: &AsyncReceiver<CompactionMsg>) -> Self {
        CompactionController {
            compact_rx: compact_rx.clone(),
            phantom: PhantomData,
        }
    }

    /// Start listening for compaction jobs
    pub async fn start(self) -> Result<(), ServerError> {
        // keep handling compaction jobs
        loop {
            // get the next msg in our queue
            let msg = self.compact_rx.recv().await?;
            // handle this message
            match msg {
                CompactionMsg::IntentCompaction => (),
                CompactionMsg::Shutdown => break,
            };
            // log that we got a job for now
            println!("Got compaction job");
        }
        Ok(())
    }
}
