//! The WAL under openraft's storage conformance suite, and the two store-level M4 rows

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{self, Cursor};
use std::rc::Rc;

use futures::{Stream, StreamExt as _};
use openraft::entry::RaftEntry as _;
use openraft::vote::RaftLeaderId as _;
use openraft::storage::{EntryResponder, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::testing::log::StoreBuilder;
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::type_config::TypeConfigExt as _;
use openraft::{AsyncRuntime as _, EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StorageError, StoredMembership};

use super::frame::{Entry, LeaderId, WalLogId};
use super::{Checkpoint, GroupCheckpoint, GroupStore, MemoryWal, ShardWal};
use crate::server::control::runtime::GlommioRuntime;
use crate::server::replication::{ApplyOutcome, CommandResult, DataConfig, ResultKind};
use crate::shared::identity::{GroupId, ShardAddr, TableId};
use crate::shared::protocol::peer::{Command, RequestId};

/// A state machine that keeps everything in memory, for the suite
///
/// The suite exercises the log store and the state machine together; the real machine hands
/// entries to a shard loop that does not exist here, so this stands in for it.
#[derive(Clone, Default)]
struct MemMachine {
    /// What has been applied
    inner: Rc<RefCell<MemMachineInner>>,
}

/// The test machine's state
#[derive(Default)]
struct MemMachineInner {
    /// The last applied log id
    applied: Option<WalLogId>,
    /// The last membership applied
    membership: StoredMembershipOf<DataConfig>,
    /// The last snapshot built or installed
    snapshot: Option<SnapshotOf<DataConfig, Cursor<Vec<u8>>>>,
}

impl RaftSnapshotBuilder<DataConfig> for MemMachine {
    type SnapshotData = Cursor<Vec<u8>>;

    /// A snapshot of where the machine stands
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<DataConfig, Cursor<Vec<u8>>>, io::Error> {
        let mut inner = self.inner.borrow_mut();
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: inner.applied.clone(),
                last_membership: inner.membership.clone(),
            },
            snapshot: Cursor::new(Vec::new()),
        };
        inner.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }
}

impl RaftStateMachine<DataConfig> for MemMachine {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = MemMachine;

    /// What has been applied, and the membership as of then
    async fn applied_state(&mut self) -> Result<(Option<WalLogId>, StoredMembershipOf<DataConfig>), io::Error> {
        let inner = self.inner.borrow();
        Ok((inner.applied.clone(), inner.membership.clone()))
    }

    /// Apply every entry, answering each
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<DataConfig>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            let log_id = entry.log_id();
            {
                let mut inner = self.inner.borrow_mut();
                if let EntryPayload::Membership(membership) = &entry.payload {
                    inner.membership = StoredMembership::new(Some(log_id.clone()), membership.clone());
                }
                inner.applied = Some(log_id);
            }
            if let Some(responder) = responder {
                responder.send(ApplyOutcome::Applied(CommandResult {
                    kind: ResultKind::Insert,
                    ok: true,
                }));
            }
        }
        Ok(())
    }

    /// The builder, which is this machine
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Take a snapshot's position as the applied state
    async fn install_snapshot(&mut self, meta: &SnapshotMetaOf<DataConfig>, snapshot: Cursor<Vec<u8>>) -> Result<(), io::Error> {
        let mut inner = self.inner.borrow_mut();
        inner.applied = meta.last_log_id.clone();
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot,
        });
        Ok(())
    }

    /// The last snapshot
    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<DataConfig, Cursor<Vec<u8>>>>, io::Error> {
        Ok(self.inner.borrow().snapshot.clone())
    }
}

/// Builds a fresh group store over a fresh WAL, in its own temp dir, for each test of the suite
struct SharedBuilder;

impl StoreBuilder<DataConfig, GroupStore, MemMachine, tempfile::TempDir> for SharedBuilder {
    /// A fresh store, and the directory that has to outlive it
    async fn build(&self) -> Result<(tempfile::TempDir, GroupStore, MemMachine), StorageError<DataConfig>> {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let wal = ShardWal::open(&dir.path().join("wal"), 1 << 20, 1 << 20)
            .await
            .map_err(|error| StorageError::read(DataConfig::err_from_error(&error)))?;
        Ok((dir, wal.store(GroupId(7)), MemMachine::default()))
    }
}

/// Builds a fresh volatile group store for each test of the suite
struct VolatileBuilder;

impl StoreBuilder<DataConfig, GroupStore, MemMachine, ()> for VolatileBuilder {
    /// A fresh store over memory
    async fn build(&self) -> Result<((), GroupStore, MemMachine), StorageError<DataConfig>> {
        Ok(((), MemoryWal::new().store(GroupId(7)), MemMachine::default()))
    }
}

/// The whole openraft storage conformance suite passes on the shared WAL and on the volatile log
///
/// The test [M4](../../../../docs/src/distributed/milestones.md) names for the data-plane
/// storage seam: every read after every write, membership found in the log and in the state
/// machine, truncation, purging, snapshots built and installed - over one group of a shared
/// file, and over the memory log an ephemeral table's group uses.
#[test]
fn data_store_passes_the_openraft_storage_suite() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        openraft::testing::log::Suite::test_all(SharedBuilder)
            .await
            .expect("the storage suite failed on the shared wal");
        openraft::testing::log::Suite::test_all(VolatileBuilder)
            .await
            .expect("the storage suite failed on the volatile log");
    });
}

/// A log id under a fixed leader
fn log_id(term: u64, index: u64) -> WalLogId {
    LogId::new(LeaderId::new(term, ShardAddr::from(1)), index)
}

/// A normal entry carrying a command of some size
fn normal(index: u64, size: usize) -> Entry {
    Entry::new_normal(
        log_id(1, index),
        Command {
            table: TableId::of("Row"),
            tablet: 0,
            request: RequestId {
                bundle: [0u8; 16],
                index,
            },
            payload: vec![index as u8; size],
        },
    )
}

/// Append entries to a store and wait for them to be durable
async fn append_durably(store: &mut GroupStore, entries: Vec<Entry>) {
    use openraft::storage::RaftLogStorageExt as _;
    store.blocking_append(entries).await.expect("failed to append");
}

/// Several forced rotations never lose a completion, and every entry reads back from its segment
///
/// Three groups append across four forced rotations. Every flush completion fires exactly once,
/// each entry's frame is located in the generation it was appended in, the sealed segments name
/// the last entry of every group in them, and reading back an entry whose frame is in a sealed
/// segment - and evicted from the cache - returns the bytes that were written
/// ([C5](../../../../docs/src/distributed/replication.md), rotation row).
#[test]
fn rotation_preserves_pending_replication_requirements() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        // a cache too small for everything, so reads have to reach the file
        let wal = ShardWal::open(&dir.path().join("wal"), 1 << 30, 2048)
            .await
            .expect("failed to open");
        let groups = [GroupId(1), GroupId(2), GroupId(3)];
        let mut stores: Vec<GroupStore> = groups.iter().map(|group| wal.store(*group)).collect();
        // count every completion as it fires
        let completed = Rc::new(RefCell::new(0usize));
        let mut expected = 0usize;
        let mut generations = Vec::new();
        for round in 1..=4u64 {
            for (at, store) in stores.iter_mut().enumerate() {
                let index = round * 10 + at as u64;
                let (tx, rx) = DataConfig::oneshot::<Result<(), io::Error>>();
                let callback = openraft::storage::IOFlushed::<DataConfig>::signal(tx);
                store
                    .append(vec![normal(index, 500)], callback)
                    .await
                    .expect("failed to append");
                expected += 1;
                let completed = completed.clone();
                glommio::spawn_local(async move {
                    rx.await.expect("the completion was dropped").expect("the flush failed");
                    *completed.borrow_mut() += 1;
                })
                .detach();
                generations.push((groups[at], index, wal.active_generation()));
            }
            wal.flush().await.expect("failed to flush");
            wal.rotate();
            wal.flush().await.expect("failed to flush after rotating");
        }
        // every completion fired once
        assert_eq!(*completed.borrow(), expected, "a completion was lost or doubled");
        // every entry's frame is in the generation it was appended in
        for (group, index, generation) in &generations {
            assert_eq!(wal.generation_of(*group, *index), Some(*generation), "{group}:{index}");
        }
        // four sealed segments, each naming the last entry of every group in it
        let segments = wal.segments();
        let sealed: Vec<_> = segments.iter().filter(|segment| segment.sealed).collect();
        assert_eq!(sealed.len(), 4, "{segments:?}");
        for (round, segment) in sealed.iter().enumerate() {
            for (at, group) in groups.iter().enumerate() {
                let last = segment.last.get(group).expect("every group wrote into every segment");
                assert_eq!(last.index, (round as u64 + 1) * 10 + at as u64);
            }
        }
        // and every entry reads back whole, most of them from a sealed file
        for (at, store) in stores.iter_mut().enumerate() {
            use openraft::storage::RaftLogReader as _;
            let entries = store.try_get_log_entries(..).await.expect("failed to read");
            assert_eq!(entries.len(), 4);
            for (round, entry) in entries.iter().enumerate() {
                let index = (round as u64 + 1) * 10 + at as u64;
                assert_eq!(entry.log_id(), log_id(1, index));
                match &entry.payload {
                    EntryPayload::Normal(command) => assert_eq!(command.payload, vec![index as u8; 500]),
                    other => panic!("not a normal entry: {other:?}"),
                }
            }
        }
        wal.close().await.expect("failed to close");
    });
}

/// Two interleaved groups recover independently, whatever order their completions fired in
///
/// Two groups append turn about; one group's flush completions are held back while the other's
/// fire, more is appended, the store is dropped without releasing them, and the directory is
/// reopened: each group's log is a contiguous prefix ending at its last durable entry, no frame
/// of one group appears in the other's index, and the votes come back
/// ([C5](../../../../docs/src/distributed/replication.md), stream row;
/// [P2](../../../../docs/src/distributed/protocol.md)).
#[test]
fn table_streams_recover_independently_without_holes() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let path = dir.path().join("wal");
        let (a, b) = (GroupId(11), GroupId(22));
        {
            let wal = ShardWal::open(&path, 1 << 30, 1 << 20).await.expect("failed to open");
            let mut store_a = wal.store(a);
            let mut store_b = wal.store(b);
            // group a's completions are held: its bytes still land, its acks do not
            wal.stall(a);
            let mut held = Vec::new();
            for index in 1..=6u64 {
                let (tx, rx) = DataConfig::oneshot::<Result<(), io::Error>>();
                store_a
                    .append(vec![normal(index, 64)], openraft::storage::IOFlushed::<DataConfig>::signal(tx))
                    .await
                    .expect("failed to append to a");
                held.push(rx);
                append_durably(&mut store_b, vec![normal(index, 64)]).await;
            }
            wal.flush().await.expect("failed to flush");
            // b's completions fired; a's are held
            assert_eq!(wal.held(), 6);
            for rx in &mut held {
                assert!(rx.try_recv().expect("the sender is alive").is_none(), "a held completion fired");
            }
            // a vote per group, durable before it is answered
            store_a
                .save_vote(&openraft::vote::RaftVote::from_leader_id(LeaderId::new(3, ShardAddr::from(1)), false))
                .await
                .expect("failed to save a's vote");
            store_b
                .save_vote(&openraft::vote::RaftVote::from_leader_id(LeaderId::new(5, ShardAddr::from(2)), true))
                .await
                .expect("failed to save b's vote");
            // b truncates its tail and appends again at the same index, which supersedes
            store_b.truncate_after(Some(log_id(1, 4))).await.expect("failed to truncate");
            append_durably(&mut store_b, vec![Entry::new_normal(log_id(2, 5), normal(5, 8).payload_command())]).await;
            wal.close().await.expect("failed to close");
            // dropped with a's completions still held
        }
        let wal = ShardWal::open(&path, 1 << 30, 1 << 20).await.expect("failed to reopen");
        // each group's log is its own contiguous prefix
        assert_eq!(wal.indexes_of(a), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(wal.indexes_of(b), vec![1, 2, 3, 4, 5]);
        let mut store_a = wal.store(a);
        let mut store_b = wal.store(b);
        {
            use openraft::storage::RaftLogReader as _;
            // a's entries are a's, b's are b's, and b's re-appended fifth is the newer one
            let entries_a = store_a.try_get_log_entries(..).await.expect("failed to read a");
            assert_eq!(entries_a.len(), 6);
            assert!(entries_a.iter().all(|entry| entry.log_id().leader_id.term == 1));
            let entries_b = store_b.try_get_log_entries(..).await.expect("failed to read b");
            assert_eq!(entries_b.len(), 5);
            assert_eq!(entries_b[4].log_id(), log_id(2, 5));
            match &entries_b[4].payload {
                EntryPayload::Normal(command) => assert_eq!(command.payload.len(), 8),
                other => panic!("not a normal entry: {other:?}"),
            }
            // and the votes came back
            assert_eq!(store_a.read_vote().await.unwrap().map(|vote| vote.leader_id.term), Some(3));
            assert_eq!(store_b.read_vote().await.unwrap().map(|vote| vote.leader_id.term), Some(5));
        }
        let state_a = store_a.get_log_state().await.expect("a's state");
        assert_eq!(state_a.last_log_id, Some(log_id(1, 6)));
        let state_b = store_b.get_log_state().await.expect("b's state");
        assert_eq!(state_b.last_log_id, Some(log_id(2, 5)));
        wal.close().await.expect("failed to close");
    });
}

/// The checkpoint file round trips a membership keyed by shard addresses
///
/// openraft's membership carries a node map keyed by the node id, which is a shard address and
/// so no JSON key; the file spells it out as lists and rebuilds it. A checkpoint that could not
/// be written killed the shard that tried
/// ([F40](../../../../docs/src/features/replication.md)).
#[test]
fn checkpoint_file_round_trips_membership() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let membership = StoredMembership::new(
            Some(log_id(1, 0)),
            openraft::Membership::new(
                vec![members(&[1, 2, 3])],
                members(&[1, 2, 3, 4]).into_iter().map(|addr| (addr, addr)).collect::<std::collections::BTreeMap<_, _>>(),
            )
            .expect("a valid membership"),
        );
        let mut file = Checkpoint::default();
        file.groups.insert(GroupId(7).to_string(), GroupCheckpoint::new(Some(log_id(2, 9)), &membership));
        file.write(dir.path()).await.expect("failed to write the checkpoint");
        let read = Checkpoint::read(dir.path()).await.expect("failed to read the checkpoint");
        assert_eq!(read, file);
        let point = read.get(GroupId(7)).expect("the group's checkpoint");
        assert_eq!(point.applied, Some(log_id(2, 9)));
        assert_eq!(point.membership(), membership);
    });
}

/// A membership set of shard addresses, for a membership entry
fn members(ids: &[u64]) -> BTreeSet<ShardAddr> {
    ids.iter().map(|id| ShardAddr::from(*id)).collect()
}

/// Reach a normal entry's command, for a test that re-appends it
trait PayloadCommand {
    /// The command, panicking on any other payload
    fn payload_command(self) -> Command;
}

impl PayloadCommand for Entry {
    fn payload_command(self) -> Command {
        match self.payload {
            EntryPayload::Normal(command) => command,
            other => panic!("not a normal entry: {other:?}"),
        }
    }
}
