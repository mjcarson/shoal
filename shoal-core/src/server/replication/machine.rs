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
use std::collections::VecDeque;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::rc::Rc;

use futures::{Stream, StreamExt as _};
use futures_channel::oneshot;
use kanal::AsyncSender;
use lru::LruCache;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{OptionalSend, Snapshot, SnapshotMeta, StoredMembership};

use super::digest::{DigestAnswer, KEPT_REPORTS};
use crate::server::control::repair::Quarantine;
use super::snapshot::SnapshotManifest;
use super::types::{DataConfig, Remembered};
use crate::server::database::ShoalDatabase;
use crate::server::messages::ServerMsg;
use crate::server::wal::WalLogId;
use crate::shared::identity::GroupId;
use crate::shared::protocol::peer::RequestId;
use uuid::Uuid;

/// How many request identities a group remembers the result of
///
/// A retry of a remembered write is answered as it was the first time. Bounded so the table
/// stays small however long a group runs. The table is persisted beside the checkpoint and
/// seeded from there at open ([F42](../../../../docs/src/features/primary-failover.md)), so
/// the bound is a count and not a restart; the low-water mark below which an identity is
/// forgotten is recorded with the checkpoint for M9a's expiry check to read.
pub const REMEMBERED_REQUESTS: usize = 4096;

/// A snapshot's data: a lazy handle at this group's checkpoint, or a file received from a peer
///
/// The rows are never in memory. This group's own snapshot is the promise of a file the loop
/// cuts when a transfer asks for it ([`ServerMsg::BuildSnapshot`]); one received from a leader
/// is a verified file on disk with the manifest that describes it, which the loop installs
/// ([F43](../../../../docs/src/features/node-recovery.md)).
#[derive(Debug, Clone)]
pub enum SnapshotData {
    /// This group's own, at its checkpoint: the file is cut when a transfer asks
    Own {
        /// The checkpoint
        checkpoint: Option<WalLogId>,
    },
    /// A file received from a leader, verified, waiting to be installed
    Received {
        /// The file
        path: PathBuf,
        /// What it is
        manifest: SnapshotManifest,
    },
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
    /// What each remembered request produced, with its payload's digest and its applied index
    pub dedup: LruCache<RequestId, Remembered>,
    /// Whether the checkpoint has been written to disk since it last moved
    pub checkpoint_durable: bool,
    /// Whether a snapshot is being installed, during which the group's tablets serve no `One`
    /// read ([F43](../../../../docs/src/features/node-recovery.md))
    pub installing: bool,
    /// A received snapshot verified at open and not yet installed, which openraft installs
    /// when it builds the group and finds it past the checkpoint
    pub pending_install: Option<(PathBuf, SnapshotManifest)>,
    /// The last few scrubs this replica applied, by operation, and what each came to
    ///
    /// `Pending` from the apply until the cut's task posts, then the report; the leader polls
    /// these over the lane ([F44](../../../../docs/src/features/repair.md)).
    pub digests: VecDeque<(Uuid, DigestAnswer)>,
    /// Why this replica's copy is quarantined, if it is
    ///
    /// While set the group's tablets serve no read through this shard; writes still propose,
    /// since the log is checksummed and independent of the archives
    /// ([F44](../../../../docs/src/features/repair.md)).
    pub quarantined: Option<Quarantine>,
    /// Until when the checkpoint stays where it is, while a repair stream is accepted
    ///
    /// The stream's boundary has to stay past the checkpoint until the group is restarted
    /// from it, so `advance_checkpoints` leaves this group alone until then, or until the
    /// transfer's deadline if it never lands ([F44](../../../../docs/src/features/repair.md)).
    pub hold_checkpoint_until: Option<std::time::Instant>,
    /// Whether the pending install is a repair's, whose merged tail has to be merged again
    /// once it lands ([F44](../../../../docs/src/features/repair.md))
    pub repair_pending: bool,
}

impl MachineState {
    /// A state that starts at a checkpoint
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - The checkpoint, or none for a group with nothing compacted
    /// * `membership` - The membership as of it
    /// * `seed` - The remembered requests as of the checkpoint, oldest first
    #[must_use]
    pub fn at(
        checkpoint: Option<WalLogId>,
        membership: StoredMembershipOf<DataConfig>,
        seed: Vec<(RequestId, Remembered)>,
    ) -> Self {
        // the seed is put oldest first, so the newest is what the bound keeps longest
        let mut dedup = LruCache::new(NonZeroUsize::new(REMEMBERED_REQUESTS).expect("a positive bound"));
        for (request, remembered) in seed {
            dedup.put(request, remembered);
        }
        MachineState {
            applied: checkpoint.clone(),
            membership: membership.clone(),
            // a checkpoint read back at open counts as snapshotted: nothing moved since
            snapshot_at: checkpoint.clone(),
            checkpoint,
            checkpoint_membership: membership,
            dedup,
            checkpoint_durable: true,
            installing: false,
            pending_install: None,
            digests: VecDeque::new(),
            quarantined: None,
            hold_checkpoint_until: None,
            repair_pending: false,
        }
    }

    /// Whether the checkpoint is held where it is for a repair stream
    #[must_use]
    pub fn checkpoint_held(&self) -> bool {
        self.hold_checkpoint_until.is_some_and(|until| std::time::Instant::now() < until)
    }

    /// Note that a scrub was applied and its digest is on its way
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    pub fn note_scrub(&mut self, op: Uuid) {
        // the newest goes last, and only so many are kept
        self.digests.retain(|(known, _)| *known != op);
        self.digests.push_back((op, DigestAnswer::Pending));
        while self.digests.len() > KEPT_REPORTS {
            self.digests.pop_front();
        }
    }

    /// Record what a scrub's cut came to
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `answer` - The report, or that the cut failed
    pub fn record_digest(&mut self, op: Uuid, answer: DigestAnswer) {
        // replace the pending entry, or add one for a report that outlived it
        match self.digests.iter_mut().find(|(known, _)| *known == op) {
            Some((_, slot)) => *slot = answer,
            None => {
                self.digests.push_back((op, answer));
                while self.digests.len() > KEPT_REPORTS {
                    self.digests.pop_front();
                }
            }
        }
    }

    /// What this replica has for a scrub
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    #[must_use]
    pub fn digest_of(&self, op: Uuid) -> DigestAnswer {
        // a scrub this replica never applied, or forgot, is unknown
        self.digests
            .iter()
            .find(|(known, _)| *known == op)
            .map_or(DigestAnswer::Unknown, |(_, answer)| *answer)
    }

    /// The remembered requests applied at or below an index, oldest first
    ///
    /// What the retry sidecar records for a checkpoint at that index: everything above it is
    /// re-derived from the log the checkpoint leaves behind.
    ///
    /// # Arguments
    ///
    /// * `through` - The index the checkpoint stands at
    #[must_use]
    pub fn remembered_through(&self, through: u64) -> Vec<(RequestId, Remembered)> {
        // the cache iterates newest first, and the sidecar is written oldest first
        self.dedup
            .iter()
            .rev()
            .filter(|(_, remembered)| remembered.applied <= through)
            .map(|(request, remembered)| (*request, *remembered))
            .collect()
    }

    /// The lowest applied index still remembered, or zero when nothing is
    ///
    /// The low-water mark: an identity applied below it has been forgotten, and a retry of it
    /// would be applied as new. Recorded with the checkpoint for M9a's expiry check.
    #[must_use]
    pub fn retry_floor(&self) -> u64 {
        self.dedup.iter().map(|(_, remembered)| remembered.applied).min().unwrap_or(0)
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

    /// The snapshot at the checkpoint, or the received one waiting to be installed past it
    fn snapshot(&self) -> SnapshotOf<DataConfig, SnapshotData> {
        let state = self.state.borrow();
        // a received file past the checkpoint is what openraft installs at open
        if let Some((path, manifest)) = &state.pending_install {
            if manifest.boundary.index > state.checkpoint_index() {
                return Snapshot {
                    meta: SnapshotMeta {
                        last_log_id: Some(manifest.boundary.clone()),
                        last_membership: manifest.membership.clone(),
                    },
                    snapshot: SnapshotData::Received {
                        path: path.clone(),
                        manifest: manifest.clone(),
                    },
                };
            }
        }
        Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.checkpoint.clone(),
                last_membership: state.checkpoint_membership.clone(),
            },
            snapshot: SnapshotData::Own {
                checkpoint: state.checkpoint.clone(),
            },
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

    /// Install a received snapshot through the loop, or take this group's own as already there
    ///
    /// Posts the install to the loop and waits until it is durable - never on the loop's own
    /// task, which is what openraft's worker and its startup restore guarantee. This group's
    /// own snapshot is metadata at a checkpoint the archives already hold, so installing it
    /// is nothing to do ([F43](../../../../docs/src/features/node-recovery.md)).
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<DataConfig>,
        snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        match snapshot {
            SnapshotData::Own { .. } => Ok(()),
            SnapshotData::Received { path, manifest } => {
                let (done, through) = oneshot::channel();
                self.tx
                    .send(ServerMsg::InstallSnapshot {
                        group: self.group,
                        path,
                        manifest,
                        meta: meta.clone(),
                        done,
                    })
                    .await
                    .map_err(|_| io::Error::other("the shard loop is gone"))?;
                through
                    .await
                    .map_err(|_| io::Error::other("the shard loop dropped a snapshot install"))?
                    .map_err(io::Error::other)
            }
        }
    }

    /// The snapshot at the checkpoint, once there is one, or a received file waiting past it
    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<DataConfig, SnapshotData>>, io::Error> {
        let (built, pending) = {
            let state = self.state.borrow();
            (state.snapshot_at.is_some(), state.pending_install.is_some())
        };
        Ok((built || pending).then(|| self.snapshot()))
    }
}

/// Hold the membership a group starts with when nothing is checkpointed
#[must_use]
pub fn no_membership() -> StoredMembershipOf<DataConfig> {
    StoredMembership::default()
}
