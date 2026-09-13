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
use super::{Checkpoint, GroupCheckpoint, GroupRetries, GroupStore, MemoryWal, Retries, ShardWal, CHECKPOINT_FILE, RETRIES_FILE, RETRIES_MAGIC};
use crate::server::control::runtime::GlommioRuntime;
use crate::server::replication::{ApplyOutcome, CommandResult, DataConfig, MachineState, Remembered, ResultKind};
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

/// The retry table is written beside the checkpoint and seeds a group past the purge point
///
/// A group's retry table used to be rebuilt from the log alone, so an identity applied below
/// the checkpoint was forgotten at restart once the segment holding it was purged, and a retry
/// of it was applied as new. The sidecar carries the entries as of the checkpoint; a checkpoint
/// from before it seeds nothing, a sidecar written for another checkpoint seeds nothing, and an
/// entry above the checkpoint is left for the replay to re-derive
/// ([F42](../../../../docs/src/features/primary-failover.md)). The end to end half - restart
/// after a compaction and retry - is `lost_response_retry_returns_original_result`.
#[test]
fn retry_table_survives_the_purge_point() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let membership = StoredMembership::new(
            Some(log_id(1, 0)),
            openraft::Membership::new(
                vec![members(&[1, 2, 3])],
                members(&[1, 2, 3]).into_iter().map(|addr| (addr, addr)).collect::<std::collections::BTreeMap<_, _>>(),
            )
            .expect("a valid membership"),
        );
        let request = |index: u64| RequestId {
            bundle: [7; 16],
            index,
        };
        let remembered = |applied: u64, ok: bool| Remembered {
            digest: 0xfeed + applied,
            result: CommandResult {
                kind: ResultKind::Delete,
                ok,
            },
            applied,
        };
        // a machine that applied three requests, the checkpoint at the second
        let mut state = MachineState::at(None, membership.clone(), Vec::new());
        state.dedup.put(request(1), remembered(3, true));
        state.dedup.put(request(2), remembered(5, false));
        state.dedup.put(request(3), remembered(9, true));
        assert_eq!(state.retry_floor(), 3);
        let through = state.remembered_through(5);
        assert_eq!(through, vec![(request(1), remembered(3, true)), (request(2), remembered(5, false))]);
        // the sidecar and the checkpoint that names it round trip
        let mut retries = Retries::default();
        retries.groups.insert(
            GroupId(7).to_string(),
            GroupRetries {
                retries_at: 5,
                entries: through,
            },
        );
        retries.write(dir.path()).await.expect("failed to write the sidecar");
        let mut file = Checkpoint::default();
        file.groups.insert(
            GroupId(7).to_string(),
            GroupCheckpoint::new(Some(log_id(2, 5)), &membership).retries(5, state.retry_floor()),
        );
        file.write(dir.path()).await.expect("failed to write the checkpoint");
        let read_retries = Retries::read(dir.path()).await.expect("failed to read the sidecar");
        assert_eq!(read_retries, retries);
        let read = Checkpoint::read(dir.path()).await.expect("failed to read the checkpoint");
        assert_eq!(read, file);
        let point = read.get(GroupId(7)).expect("the group's checkpoint");
        assert_eq!((point.retries_at, point.retry_floor), (5, 3));
        // the seed is the sidecar's entries, and a machine seeded from it remembers them
        let seed = read_retries.seed_for(GroupId(7), point);
        assert_eq!(seed.len(), 2);
        let mut seeded = MachineState::at(Some(log_id(2, 5)), membership.clone(), seed);
        assert_eq!(seeded.dedup.get(&request(2)).copied(), Some(remembered(5, false)));
        assert_eq!(seeded.dedup.get(&request(3)), None);
        assert_eq!(seeded.retry_floor(), 3);
        // a checkpoint from before the sidecar existed - the same file without the two
        // fields - loads with zeros and seeds nothing
        let mut value = serde_json::to_value(&file).expect("a checkpoint serializes");
        let entry = value["groups"][GroupId(7).to_string()]
            .as_object_mut()
            .expect("a group's checkpoint is an object");
        entry.remove("retries_at");
        entry.remove("retry_floor");
        let old: Checkpoint = serde_json::from_value(value)
            .unwrap_or_else(|error| panic!("an M4 checkpoint no longer loads: {error}"));
        let old_point = old.get(GroupId(7)).expect("the old checkpoint");
        assert_eq!((old_point.retries_at, old_point.retry_floor), (0, 0));
        assert!(read_retries.seed_for(GroupId(7), old_point).is_empty());
        // a sidecar written for another checkpoint seeds nothing either
        let other = GroupCheckpoint::new(Some(log_id(2, 9)), &membership).retries(9, 3);
        assert!(read_retries.seed_for(GroupId(7), &other).is_empty());
        // and a missing sidecar is an empty one
        let empty = tempfile::tempdir().expect("failed to build a temp dir");
        assert_eq!(Retries::read(empty.path()).await.expect("failed to read"), Retries::default());
    });
}

/// A frame at or below a group's checkpoint is never handed to a compactor again, across a reopen
///
/// One group appends across two forced rotations. Asked for the frames of the first segment
/// with the checkpoint at its second entry, the store names only the third; with the
/// checkpoint at the last entry of the second segment it names nothing there; and the same
/// holds once the directory is reopened, when every sealed segment looks unhanded and the
/// index has been rebuilt from the files
/// ([Resolved #104](../../../../docs/src/appendix/resolved/segments-recompacted-after-restart.md)).
#[test]
fn frames_at_or_below_the_checkpoint_are_not_handed_again() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let path = dir.path().join("wal");
        let group = GroupId(4);
        let wal = ShardWal::open(&path, 1 << 30, 1 << 20).await.expect("failed to open");
        let mut store = wal.store(group);
        // three entries in the first segment, three in the second
        append_durably(&mut store, (1..=3).map(|index| normal(index, 64)).collect()).await;
        wal.rotate();
        wal.flush().await.expect("failed to flush after rotating");
        append_durably(&mut store, (4..=6).map(|index| normal(index, 64)).collect()).await;
        wal.rotate();
        wal.flush().await.expect("failed to flush after rotating");
        // the judgement, on the live index and again on one rebuilt from the files
        let judge = |wal: &ShardWal| {
            let indexes = |generation: u64, since: u64| -> Vec<u64> {
                wal.frames_in(generation, &[(group, since)]).iter().map(|frame| frame.index).collect()
            };
            assert_eq!(indexes(1, 0), vec![1, 2, 3], "nothing checkpointed: every frame is handed");
            assert_eq!(indexes(1, 2), vec![3], "the checkpoint at two leaves the third");
            assert_eq!(indexes(1, 3), Vec::<u64>::new(), "a segment below the checkpoint hands nothing");
            assert_eq!(indexes(2, 3), vec![4, 5, 6], "the next segment is whole above it");
            assert_eq!(indexes(2, 6), Vec::<u64>::new(), "and nothing once the checkpoint passed it");
            // the sealed segments know their size, which the retention budget reads
            let sealed: Vec<_> = wal.segments().into_iter().filter(|segment| segment.sealed).collect();
            assert_eq!(sealed.len(), 2);
            assert!(sealed.iter().all(|segment| segment.bytes > 0), "{sealed:?}");
        };
        judge(&wal);
        wal.close().await.expect("failed to close");
        let reopened = ShardWal::open(&path, 1 << 30, 1 << 20).await.expect("failed to reopen");
        judge(&reopened);
        reopened.close().await.expect("failed to close");
    });
}

/// A group's vote, committed and purged markers survive the deletion of the segment they were written in
///
/// A group appends, votes, records a commit and purges, all into the first segment; the store
/// rotates twice and deletes the first two segments, as the sweep does once a segment is
/// compacted and purged past; reopened, the group's purge point, its vote and its committed
/// position are what they were, because every rotation carries them into the new segment
/// ([F43](../../../../docs/src/features/node-recovery.md)).
#[test]
fn markers_survive_the_deletion_of_their_segment() {
    use openraft::storage::RaftLogStorage as _;
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let path = dir.path().join("wal");
        let group = GroupId(9);
        let wal = ShardWal::open(&path, 1 << 30, 1 << 20).await.expect("failed to open");
        let mut store = wal.store(group);
        append_durably(&mut store, (1..=6).map(|index| normal(index, 32)).collect()).await;
        let vote = openraft::Vote::new(3, ShardAddr::from(2));
        store.save_vote(&vote).await.expect("failed to vote");
        store.save_committed(Some(log_id(1, 6))).await.expect("failed to record the commit");
        store.purge(log_id(1, 4)).await.expect("failed to purge");
        wal.flush().await.expect("failed to flush");
        // two rotations, and the segments the markers were first written in deleted
        wal.rotate();
        wal.flush().await.expect("failed to flush after rotating");
        wal.rotate();
        wal.flush().await.expect("failed to flush after rotating");
        for generation in [1, 2] {
            wal.delete_segment(generation).await.expect("failed to delete a sealed segment");
        }
        assert_eq!(wal.segments().len(), 1, "{:?}", wal.segments());
        wal.close().await.expect("failed to close");
        // reopened, the markers are what they were
        let reopened = ShardWal::open(&path, 1 << 30, 1 << 20).await.expect("failed to reopen");
        let mut store = reopened.store(group);
        assert_eq!(store.purged_index(), Some(4), "the purge point was forgotten");
        assert_eq!(reopened.vote_of(group), Some(vote), "the vote was forgotten");
        assert_eq!(store.read_committed().await.expect("failed to read"), Some(log_id(1, 6)));
        let state = store.get_log_state().await.expect("failed to read the log state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
        assert_eq!(state.last_log_id, Some(log_id(1, 4)), "the entries above the purge point were in a deleted segment");
        reopened.close().await.expect("failed to close");
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

/// The checkpoint file and the retry sidecar carry checksums, and a corrupt one fails the open by name
///
/// A checkpoint is where a group starts from, so one that cannot be trusted is not a state to
/// guess at ([F44](../../../../docs/src/features/repair.md)): a flipped byte in either file is
/// refused at `read`, and a file from before there were checksums is read as it was.
#[test]
fn checkpoint_and_retries_are_checksummed() {
    let mut runtime = GlommioRuntime::new(1);
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("failed to build a temp dir");
        let membership = StoredMembership::new(
            Some(log_id(1, 0)),
            openraft::Membership::new(
                vec![members(&[1, 2, 3])],
                members(&[1, 2, 3]).into_iter().map(|addr| (addr, addr)).collect::<std::collections::BTreeMap<_, _>>(),
            )
            .expect("a valid membership"),
        );
        // a checkpoint written by this build carries a checksum and reads back
        let mut file = Checkpoint::default();
        file.groups.insert(GroupId(7).to_string(), GroupCheckpoint::new(Some(log_id(2, 9)), &membership));
        file.write(dir.path()).await.expect("failed to write the checkpoint");
        let text = std::fs::read_to_string(dir.path().join(CHECKPOINT_FILE)).expect("the file");
        assert!(text.contains("\"checksum\""), "{text}");
        assert_eq!(Checkpoint::read(dir.path()).await.expect("failed to read"), file);
        // a flipped digit in the index it names is refused by name
        let torn = text.replacen("\"index\": 9", "\"index\": 8", 1);
        assert_ne!(torn, text);
        std::fs::write(dir.path().join(CHECKPOINT_FILE), torn).expect("failed to rewrite");
        let error = Checkpoint::read(dir.path()).await.expect_err("a torn checkpoint was read");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("does not hash to its checksum"), "{error}");
        // a file from before checksums has none and is read as it was
        let legacy = serde_json::to_vec_pretty(&file).expect("json");
        std::fs::write(dir.path().join(CHECKPOINT_FILE), legacy).expect("failed to rewrite");
        assert_eq!(Checkpoint::read(dir.path()).await.expect("failed to read"), file);
        // the sidecar the same way
        let mut retries = Retries::default();
        retries.groups.insert(
            GroupId(7).to_string(),
            GroupRetries {
                retries_at: 9,
                entries: vec![(
                    RequestId {
                        bundle: [3; 16],
                        index: 1,
                    },
                    Remembered {
                        digest: 0xfeed,
                        result: CommandResult {
                            kind: ResultKind::Delete,
                            ok: true,
                        },
                        applied: 5,
                    },
                )],
            },
        );
        retries.write(dir.path()).await.expect("failed to write the sidecar");
        let bytes = std::fs::read(dir.path().join(RETRIES_FILE)).expect("the file");
        assert!(bytes.starts_with(RETRIES_MAGIC));
        assert_eq!(Retries::read(dir.path()).await.expect("failed to read"), retries);
        // a flipped byte in its payload is refused
        let mut torn = bytes.clone();
        let last = torn.len() - 1;
        torn[last] ^= 0x01;
        std::fs::write(dir.path().join(RETRIES_FILE), torn).expect("failed to rewrite");
        let error = Retries::read(dir.path()).await.expect_err("a torn sidecar was read");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("does not hash to its checksum"), "{error}");
        // and a sidecar from before checksums is the bare postcard, read as it was
        std::fs::write(dir.path().join(RETRIES_FILE), &bytes[16..]).expect("failed to rewrite");
        assert_eq!(Retries::read(dir.path()).await.expect("failed to read"), retries);
    });
}
