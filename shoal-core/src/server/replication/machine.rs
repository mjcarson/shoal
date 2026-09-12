//! The state machine of a tablet group: committed entries handed to the shard loop
//!
//! openraft applies committed entries through a state machine it owns on a worker task of its
//! own. The tables an entry mutates are owned by the shard loop, and only that loop may touch
//! them, so the machine here holds no table: `apply` collects a batch of entries and their
//! responders, posts them to the loop as [`ServerMsg::Apply`], and waits until the loop says the
//! batch is through ([F40](../../../../docs/src/features/replication.md)). The loop applies each
//! command to the table it names in committed order, derives the result there, answers the
//! responder, and moves the machine's record of what is applied - which lives in a cell shared
//! with the loop for exactly that reason.
//!
//! # Invariants
//!
//! **Never await `apply` on the loop's own task.** `Raft::new` re-applies the checkpoint up to
//! the committed log id on the caller's task before the core exists, and `apply` posts to the
//! loop and waits; awaited on the loop, that is a deadlock on every restart. A group is started
//! from a spawned task, and the loop only ever receives.
//!
//! **The checkpoint is what a snapshot names, and it moves only when the compactor says so.**
//! `applied_state` at open answers the checkpoint - the last log id whose effect the table's
//! archives hold - so openraft re-applies everything after it; a snapshot is metadata at the
//! checkpoint and carries no rows, since the archives are the rows. Installing one is M7's.

use std::cell::RefCell;
use std::io::{self, Cursor};
use std::num::NonZeroUsize;
use std::rc::Rc;

use futures::{Stream, StreamExt as _};
use futures_channel::oneshot;
use kanal::AsyncSender;
use lru::LruCache;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use serde::{Deserialize, Serialize};

use super::types::{CommandResult, DataConfig};
use crate::server::database::ShoalDatabase;
use crate::server::messages::ServerMsg;
use crate::server::wal::WalLogId;
use crate::shared::identity::GroupId;
use crate::shared::protocol::peer::RequestId;

/// How many request identities a group remembers the result of
///
/// A retry of a remembered write is answered as it was the first time. Bounded so the table
/// stays small however long a group runs; the durable low-water mark that makes the bound a
/// promise is M6's.
pub const REMEMBERED_REQUESTS: usize = 4096;

/// A snapshot's data: the checkpoint, as JSON, since the rows live in the archives
pub type SnapshotData = Cursor<Vec<u8>>;

/// What a snapshot's bytes say
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotBody {
    /// The checkpoint the snapshot stands at
    checkpoint: Option<WalLogId>,
}

/// What the loop and the machine share about one group
pub struct MachineState {
    /// The last log id the loop applied
    pub applied: Option<WalLogId>,
    /// The membership as of the last applied entry
    pub membership: StoredMembershipOf<DataConfig>,
    /// The last log id whose effect the table's archives hold
    pub checkpoint: Option<WalLogId>,
    /// The membership as of the checkpoint
    pub checkpoint_membership: StoredMembershipOf<DataConfig>,
    /// The log id the last snapshot was built at, if one was
    pub snapshot_at: Option<WalLogId>,
    /// The result each remembered request produced, with the digest of its payload
    pub dedup: LruCache<RequestId, (u64, CommandResult)>,
    /// Whether the checkpoint has been written to disk since it last moved
    pub checkpoint_durable: bool,
}

impl MachineState {
    /// A state that starts at a checkpoint
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - The checkpoint, or none for a group with nothing compacted
    /// * `membership` - The membership as of it
    #[must_use]
    pub fn at(checkpoint: Option<WalLogId>, membership: StoredMembershipOf<DataConfig>) -> Self {
        MachineState {
            applied: checkpoint.clone(),
            membership: membership.clone(),
            // a checkpoint read back at open counts as snapshotted: nothing moved since
            snapshot_at: checkpoint.clone(),
            checkpoint,
            checkpoint_membership: membership,
            dedup: LruCache::new(NonZeroUsize::new(REMEMBERED_REQUESTS).expect("a positive bound")),
            checkpoint_durable: true,
        }
    }

    /// The index the loop has applied, or zero
    #[must_use]
    pub fn applied_index(&self) -> u64 {
        self.applied.as_ref().map_or(0, |log_id| log_id.index)
    }

    /// The checkpoint's index, or zero
    #[must_use]
    pub fn checkpoint_index(&self) -> u64 {
        self.checkpoint.as_ref().map_or(0, |log_id| log_id.index)
    }
}

/// A tablet group's state machine, which hands every committed batch to the shard loop
pub struct GroupMachine<D: ShoalDatabase> {
    /// The group
    group: GroupId,
    /// What the loop and this machine share
    state: Rc<RefCell<MachineState>>,
    /// The loop's channel
    tx: AsyncSender<ServerMsg<D>>,
}

impl<D: ShoalDatabase> Clone for GroupMachine<D> {
    /// Another handle on the same state
    fn clone(&self) -> Self {
        GroupMachine {
            group: self.group,
            state: self.state.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<D: ShoalDatabase> GroupMachine<D> {
    /// A machine for a group, starting at a checkpoint
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `state` - The state the loop shares
    /// * `tx` - The loop's channel
    #[must_use]
    pub fn new(group: GroupId, state: Rc<RefCell<MachineState>>, tx: AsyncSender<ServerMsg<D>>) -> Self {
        GroupMachine { group, state, tx }
    }

    /// The state the loop shares
    #[must_use]
    pub fn state(&self) -> Rc<RefCell<MachineState>> {
        self.state.clone()
    }

    /// The snapshot at the checkpoint
    fn snapshot(&self) -> SnapshotOf<DataConfig, SnapshotData> {
        let state = self.state.borrow();
        let body = SnapshotBody {
            checkpoint: state.checkpoint.clone(),
        };
        Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.checkpoint.clone(),
                last_membership: state.checkpoint_membership.clone(),
            },
            snapshot: Cursor::new(serde_json::to_vec(&body).unwrap_or_default()),
        }
    }
}

impl<D: ShoalDatabase> RaftSnapshotBuilder<DataConfig> for GroupMachine<D> {
    type SnapshotData = SnapshotData;

    /// Build the metadata-only snapshot at the checkpoint
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<DataConfig, SnapshotData>, io::Error> {
        {
            let mut state = self.state.borrow_mut();
            state.snapshot_at = state.checkpoint.clone();
        }
        Ok(self.snapshot())
    }
}

impl<D: ShoalDatabase> RaftStateMachine<DataConfig> for GroupMachine<D> {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = GroupMachine<D>;

    /// What the loop has applied, and the membership as of then
    async fn applied_state(&mut self) -> Result<(Option<WalLogId>, StoredMembershipOf<DataConfig>), io::Error> {
        let state = self.state.borrow();
        Ok((state.applied.clone(), state.membership.clone()))
    }

    /// Hand the batch to the loop and wait until it is through
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<DataConfig>, io::Error>> + Unpin + OptionalSend,
    {
        // the whole batch, so the loop applies it between two of its other messages
        let mut batch = Vec::new();
        while let Some(next) = entries.next().await {
            batch.push(next?);
        }
        if batch.is_empty() {
            return Ok(());
        }
        let (done, through) = oneshot::channel();
        self.tx
            .send(ServerMsg::Apply {
                group: self.group,
                entries: batch,
                done,
            })
            .await
            .map_err(|_| io::Error::other("the shard loop is gone"))?;
        through
            .await
            .map_err(|_| io::Error::other("the shard loop dropped an apply batch"))
    }

    /// A builder, when a durable checkpoint has moved past the last snapshot or one is forced
    ///
    /// Refusing here is what a checkpoint that is not on disk yet does: a refusal is a snapshot
    /// deferred, where a builder that failed would be a storage error the group stops on.
    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        let state = self.state.borrow();
        let moved = state.checkpoint.is_some() && state.checkpoint != state.snapshot_at;
        (force || (moved && state.checkpoint_durable)).then(|| self.clone())
    }

    /// The builder, which is this machine
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Installing a snapshot is catch-up past the purge point, which M7 delivers
    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMetaOf<DataConfig>,
        _snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "installing a tablet group snapshot is M7's; a replica behind the purge point cannot catch up yet",
        ))
    }

    /// The snapshot at the checkpoint, once there is a checkpoint
    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<DataConfig, SnapshotData>>, io::Error> {
        let built = self.state.borrow().snapshot_at.is_some();
        Ok(built.then(|| self.snapshot()))
    }
}

/// Hold the membership a group starts with when nothing is checkpointed
#[must_use]
pub fn no_membership() -> StoredMembershipOf<DataConfig> {
    StoredMembership::default()
}
