//! Handles communication between shoal shards and nodes

use kanal::{AsyncReceiver, AsyncSender};

use crate::server::messages::ServerMsg;
use crate::server::shard::ShardContact;
use crate::server::ServerError;
use crate::shared::traits::ShoalDatabase;

/// Handles communication between shoal shards
pub(super) struct Comms<S: ShoalDatabase> {
    /// A vec of channels to each shard
    shards: Vec<(AsyncSender<ServerMsg<S>>, AsyncReceiver<ServerMsg<S>>)>,
}

/// # Safety
///
/// This is done to allow kanal mesh to be setup. This is only used internally
/// to Shoal and every variant is Send other then the partition variant which
/// is never sent across threads and that must be upheld by shoal developers
/// by ensuring that loaders only have a channel to their current shard. Loaders
/// should only load data for their shard and no others.
unsafe impl<D: ShoalDatabase> Send for Comms<D> where D::TableNames: Send {}

impl<S: ShoalDatabase> Comms<S> {
    /// Create a new comms object for this node
    ///
    /// # Arguments
    ///
    /// * `shard_count` - The number of shards on this node
    pub fn with_capacity(shard_count: usize) -> Self {
        // create all of the channels for the shards on this nodes
        let shards = (0..shard_count).map(|_| kanal::unbounded_async()).collect();
        Comms { shards }
    }

    /// Send a message to a shard on this node
    ///
    /// * `shard` - The shard to send this message too
    pub async fn send(
        &mut self,
        contact: &ShardContact,
        msg: ServerMsg<S>,
    ) -> Result<(), ServerError> {
        // if this shard is on our node then send it directly over that shards channel
        match contact {
            ShardContact::Local(shard) => {
                // try to get this servers sender
                match self.shards.get(*shard) {
                    // send this message to the correct shard
                    Some((sender, _)) => sender.send(msg).await?,
                    // this is not a known shard
                    None => panic!("Who is {contact:#?}"),
                }
            }
        }
        Ok(())
    }

    /// Get a shards channels
    ///
    /// # Arguments
    ///
    /// * `shard` - The index of the shard to get our receiver for
    pub fn get_shards_channels(
        &self,
        shard: usize,
    ) -> (AsyncSender<ServerMsg<S>>, AsyncReceiver<ServerMsg<S>>) {
        // get this shards channel
        self.shards.get(shard).unwrap().clone()
    }

    /// Broadcast a message to all shards
    pub async fn broadcast(&self, msg: &ServerMsg<S>) -> Result<(), ServerError> {
        // broadcast our message to every shard that we have a channel for
        for (tx, _) in &self.shards {
            tx.send(msg.clone()).await?;
        }
        Ok(())
    }
}

impl<S: ShoalDatabase> Clone for Comms<S> {
    fn clone(&self) -> Self {
        Comms {
            shards: self.shards.clone(),
        }
    }
}
