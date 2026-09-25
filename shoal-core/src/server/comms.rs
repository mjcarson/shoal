//! Handles communication between shoal shards and nodes

use kanal::{AsyncReceiver, AsyncSender};

use crate::server::database::ShoalDatabase;
use crate::server::errors::ShoalError;
use crate::server::messages::ServerMsg;
use crate::server::shard::ShardContact;
use crate::server::ServerError;

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
    /// The mesh reaches this node's shards and nothing else. A remote contact is refused rather
    /// than reached: it goes through the shard's peer links, and a caller that handed one here
    /// has a routing bug ([F38](../../../docs/src/features/inter-node-transport.md)).
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
                    // this is not a known shard, which fails this message rather than the shard
                    None => {
                        return Err(ServerError::Shoal(ShoalError::UnknownShard {
                            shard: *shard,
                        }))
                    }
                }
            }
            // a remote shard is not on the mesh
            ShardContact::Remote { node, shard } => {
                return Err(ServerError::Shoal(ShoalError::NotLocal {
                    node: *node,
                    shard: *shard,
                }));
            }
        }
        Ok(())
    }

    /// How many messages wait on one shard's queue right now
    ///
    /// What the admission bound is judged against
    /// ([Resolved #15](../../../docs/src/appendix/resolved/shard-mesh-admission.md)); zero for
    /// a shard this mesh does not know.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard
    #[must_use]
    pub fn queued(&self, shard: usize) -> usize {
        self.shards.get(shard).map_or(0, |(sender, _)| sender.len())
    }

    /// Get a shards channels
    ///
    /// # Arguments
    ///
    /// * `shard` - The index of the shard to get our channels for
    pub fn get_shards_channels(
        &self,
        shard: usize,
    ) -> Result<(AsyncSender<ServerMsg<S>>, AsyncReceiver<ServerMsg<S>>), ServerError> {
        // get this shards channel
        match self.shards.get(shard) {
            Some((tx, rx)) => Ok((tx.clone(), rx.clone())),
            None => Err(ServerError::Shoal(ShoalError::UnknownShard { shard })),
        }
    }

    /// Broadcast a message to all shards
    ///
    /// A message that is only ever sent to one shard is refused before any shard is sent
    /// anything, rather than panicking part way through the broadcast.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to send every shard a copy of
    pub async fn broadcast(&self, msg: &ServerMsg<S>) -> Result<(), ServerError> {
        // broadcast our message to every shard that we have a channel for
        for (tx, _) in &self.shards {
            // copy this message for this shard, if it is a message every shard may be sent
            let copy = msg
                .try_clone()
                .map_err(|why| ServerError::Shoal(ShoalError::NotBroadcast { why }))?;
            tx.send(copy).await?;
        }
        Ok(())
    }
}

impl<S: ShoalDatabase> Clone for Comms<S> {
    /// Clone the channels to every shard on this node
    fn clone(&self) -> Self {
        Comms {
            shards: self.shards.clone(),
        }
    }
}
