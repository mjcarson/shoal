//! Deciding a stage its stager left: applied, discarded, or excluded by a no-op first
//!
//! A stager that died between its stages and its commit leaves bytes staged that no committed
//! fact excludes, since the row never moved ([S7](../../../../../docs/src/object-storage/write-path.md#the-preferred-direction-step-by-step)).
//! A holder's node asks the stage's row about it once its stager has had its time, and at once for
//! every stage a start found:
//!
//! - the row names its label at its position: it committed, and is applied;
//! - the row's pending bytes fold from it: a committed record a named label stands on, kept;
//! - the row is at or past its sequence and names it nowhere: excluded, and discarded;
//! - otherwise nothing has decided it: a no-op moves the row's sequence past it - or makes the row,
//!   at its object's labels, where there was none - and the row is read again.
//!
//! Every proposal goes to the row's group's leader, so this is the leader's no-op; what prompts
//! it is the holder's timer ([P16](../../../../../docs/src/object-storage/contract.md)).
//!
//! # Invariants
//!
//! **A stage is discarded only on a committed fact read from its row.** A no-op that is refused
//! means the row moved, and is read again; an error decides nothing, and the stage stays for the
//! next poll.

use crate::server::database::ShoalDatabase;
use crate::server::objects::{Holder, ObjectStore};
use crate::shared::devices::SliceId;
use crate::shared::objects::{Label, StripeAnswer};
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest};
use crate::shared::stripes::{ChunkId, StripeRow};
use crate::shared::traits::QuerySupport;

use super::{Meta, MAX_TRIES};

/// What deciding one stage came to
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decided {
    /// Its row named it: applied
    Applied,
    /// Its row's pending bytes fold from it: kept
    Kept,
    /// A committed fact excludes it: discarded
    Discarded,
    /// Nothing decided it this time, and why; the next poll asks again
    Undecided(String),
}

/// One stage on a slice of this node, to be settled by its row
pub(crate) struct DecideTask<D: ShoalDatabase> {
    /// How the row is read and the no-op committed, through the bucket the chunk's consumer is
    pub meta: Meta<D>,
    /// The node's object store
    pub store: ObjectStore,
    /// The slice the stage is on
    pub slice: SliceId,
    /// The stage's chunk
    pub chunk: ChunkId,
    /// The label it makes
    pub label: Label,
    /// How many positions the chunk's stripe has
    pub width: usize,
}

impl<D: ShoalDatabase> DecideTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Settle the stage by its row
    pub async fn run(self) -> Decided {
        let chunk = self.chunk;
        let position = usize::from(chunk.position);
        let holder = Holder {
            node: self.store.node(),
            slice: self.slice,
        };
        for _ in 0..MAX_TRIES {
            // the row, at Quorum, through its group's leader
            let rows = match self
                .meta
                .stripe_rows(chunk.consumer, chunk.object, &[chunk.stripe])
                .await
            {
                Ok(rows) => rows,
                Err((code, why)) => return Decided::Undecided(format!("{code}: {why}")),
            };
            let row = rows.get(&chunk.stripe);
            if let Some(row) = row {
                // committed: applied
                if position < row.labels.len() && row.named(position) == self.label {
                    return self.tell(holder, ChunkRequest::Apply { chunk, label: self.label }, Decided::Applied).await;
                }
                // a committed record pending bytes fold from: a named label stands on it
                if row
                    .pending
                    .as_ref()
                    .is_some_and(|pending| position < pending.base.len() && pending.folds_from(position, self.label))
                {
                    return Decided::Kept;
                }
                // a row at or past its sequence that names it nowhere excludes it
                if row.sequence >= self.label.sequence {
                    return self.tell(holder, ChunkRequest::Discard { chunk, label: self.label }, Decided::Discarded).await;
                }
            }
            // nothing decided it: a no-op moves the sequence past it, or makes the row at its
            // object's labels, as long as the chunk its put wrote holds
            let base = match row {
                Some(row) => row.clone(),
                None => {
                    let length = match self.store.chunk_header(self.slice, chunk).await {
                        Ok(Some((label, length))) if label == Label::PUT => length,
                        Ok(_) => 0,
                        Err(error) => return Decided::Undecided(error.to_string()),
                    };
                    StripeRow::put(chunk.consumer, chunk.object, chunk.stripe, self.width, length)
                }
            };
            match self
                .meta
                .commit_stripe(base.noop(), row.map(|row| row.sequence), None)
                .await
            {
                // applied, or the row moved under it: either way the row is read again
                StripeAnswer::Written | StripeAnswer::Refused(_) => continue,
                StripeAnswer::Failed(code, why) => return Decided::Undecided(format!("{code}: {why}")),
                StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                    return Decided::Undecided("a no-op answered as a read".to_string())
                }
            }
        }
        Decided::Undecided(format!("the row of {chunk:?} moved under every try"))
    }

    /// Tell the holder what its row decided, and say so
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - What it is told
    /// * `decided` - What the row decided
    async fn tell(&self, holder: Holder, request: ChunkRequest, decided: Decided) -> Decided {
        match self.store.ask_holder(holder, request).await {
            ChunkReply::Done => decided,
            other => Decided::Undecided(format!("{other:?}")),
        }
    }
}

/// Ask the rows of every stage on this node's slices that its stager has had its time with, for
/// as long as the shard runs
///
/// Run on one shard of a node. The first poll is as soon as the store is up, so every stage a
/// start found is decided without waiting a stager's time; a stage whose decide failed is asked
/// about again at the next poll ([F86](../../../../../docs/src/features/replicated-pools.md)).
///
/// # Arguments
///
/// * `client` - The shard's metadata client
/// * `shard_tx` - The shard's own channel
/// * `map` - The shard's map, whose bindings name the bucket each consumer is
/// * `store` - The node's object store, once started
/// * `decide_after` - How long a stage is left to its stager
/// * `wait` - How long one metadata query may take
pub(crate) async fn poll<D: ShoalDatabase>(
    client: super::MetaClient,
    shard_tx: kanal::AsyncSender<crate::server::messages::ServerMsg<D>>,
    map: crate::server::map::MapCell,
    store: std::sync::Arc<std::sync::OnceLock<ObjectStore>>,
    decide_after: std::time::Duration,
    wait: std::time::Duration,
) where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    // a stager's time halved, so a stage is decided within one and a half of it
    let every = (decide_after / 2).max(std::time::Duration::from_millis(100));
    loop {
        // the store starts once the shards answer
        let Some(store) = store.get().cloned() else {
            glommio::timer::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        };
        for (slice, chunk, label) in store.undecided(decide_after).await {
            // one decide of a stage at once
            if !client.deciding.borrow_mut().insert((slice, chunk)) {
                continue;
            }
            // the bucket the chunk's consumer is bound as, and its pool's width
            let installed = map.get();
            let bound = installed
                .objects
                .bindings
                .iter()
                .find(|(_, binding)| binding.consumer == chunk.consumer);
            let found = bound.and_then(|(name, binding)| {
                let ops = <D::ClientType as QuerySupport>::object_queries(crate::shared::identity::BucketId::of(name))?;
                let pool = installed.objects.pools.get(&binding.pool)?;
                Some((ops, usize::from(pool.redundancy.width())))
            });
            let Some((ops, width)) = found else {
                client.deciding.borrow_mut().remove(&(slice, chunk));
                continue;
            };
            let task = DecideTask::<D> {
                meta: Meta {
                    ops,
                    client: client.clone(),
                    shard_tx: shard_tx.clone(),
                    span: tracing::info_span!("Object::decide", ?chunk, ?label),
                    wait,
                },
                store: store.clone(),
                slice,
                chunk,
                label,
                width,
            };
            let deciding = client.deciding.clone();
            glommio::spawn_local(async move {
                let decided = task.run().await;
                tracing::event!(tracing::Level::DEBUG, msg = "a stage was decided by its row", ?chunk, ?label, ?decided);
                deciding.borrow_mut().remove(&(slice, chunk));
            })
            .detach();
        }
        glommio::timer::sleep(every).await;
    }
}
