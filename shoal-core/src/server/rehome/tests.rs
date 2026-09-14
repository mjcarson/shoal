//! Tests of the rehome's steps: each one's idempotence when it is begun again
//!
//! The crash matrix in the fixture kills a node at each point and starts it again; what these
//! hold is the property the matrix relies on, step by step, against files built by hand.

use glommio::LocalExecutor;
use openraft::entry::RaftEntry as _;
use openraft::storage::RaftLogStorageExt as _;
use openraft::vote::RaftLeaderId as _;
use openraft::LogId;
use uuid::Uuid;

use super::manifest::{Manifest, StepKind};
use super::{archives_step, log_step, shard_name, Slots};
use crate::server::hosting::Hosting;
use crate::server::meta::{ClusterIntent, StorageMeta};
use crate::server::tables::storage::fs::map::{write_record, ArchiveEntry, ArchiveMap};
use crate::server::wal::frame::{Entry, LeaderId};
use crate::server::wal::{Checkpoint, GroupCheckpoint, ShardWal, WAL_DIR};
use crate::server::Conf;
use crate::shared::identity::{GroupId, ShardAddr, TableId};
use crate::shared::protocol::peer::{Command, RequestId};

/// A config whose storage is under a directory
///
/// # Arguments
///
/// * `root` - The directory
/// * `cluster` - Whether the config carries a `cluster:` block
fn conf_at(root: &std::path::Path, cluster: bool) -> Conf {
    let mut conf = Conf::default();
    conf.storage.default.filesystem.latency_sensitive.path = root.to_path_buf();
    conf.storage.default.filesystem.throughput_sensitive.path = root.to_path_buf();
    if cluster {
        conf.cluster = Some(crate::server::conf::Cluster::default());
    }
    conf
}

/// A partition key on a tablet, so a test can pick which executor the hosting puts it on
///
/// # Arguments
///
/// * `tablet` - The tablet
/// * `salt` - Something to tell keys of one tablet apart
fn key_on(tablet: u64, salt: u64) -> u64 {
    (tablet << (u64::BITS - crate::server::ring::TABLET_BITS)) | salt
}

/// Write records into an executor's map of a table and save it
///
/// # Arguments
///
/// * `conf` - The config
/// * `executor` - The executor
/// * `keys` - The keys, each written as a record of its own bytes
async fn write_records(conf: &Conf, executor: u16, keys: &[u64]) {
    let settings = super::table_settings(conf, "T");
    settings.setup_paths("T").await.expect("paths");
    let map = ArchiveMap::new(&shard_name(executor), "T", &settings).await.expect("a map");
    let active = *map.active.borrow();
    let mut writer = map.get_active_writer().await.expect("a writer");
    for key in keys {
        let payload = key.to_le_bytes().repeat(4);
        let offset = write_record(&mut writer, &payload).await.expect("a record");
        map.set_partition(
            *key,
            ArchiveEntry {
                key: *key,
                archive: active,
                offset,
                size: payload.len(),
            },
        );
    }
    use futures::AsyncWriteExt as _;
    writer.sync().await.expect("a sync");
    writer.close().await.expect("a close");
    // saved the way a compactor leaves a map: the file, and an empty intent log beside it
    let mut intents = map.compact_map().await.expect("a save");
    intents.close().await.expect("a close");
    map.close_all().await.expect("a close");
}

/// A redone archives step removes the partial archive the crash left and copies everything again
///
/// The manifest names the archive a crashed step was writing into; the destination's map does
/// not, since the save is the step's last act. The redo deletes it, copies every record into a
/// fresh archive, and the destination reads every key with the source's bytes.
#[test]
fn a_redone_archives_step_removes_its_partial_archive() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let conf = conf_at(dir.path(), false);
    LocalExecutor::default().run(async {
        // two executors down to one: executor one's tablets all move to zero
        let before = Hosting::identity(2);
        let after = before.plan(1, false).expect("a plan");
        let mut manifest = Manifest::plan(&before, &after, &["T".to_string()], false);
        // records on executor one, on tablets it owns under the identity hosting
        let keys: Vec<u64> = (0..6u64).map(|at| key_on(2 * at + 1, at)).collect();
        write_records(&conf, 1, &keys).await;
        // and one already on executor zero, which has to survive the copy
        write_records(&conf, 0, &[key_on(2, 9)]).await;
        // the archives step, with a partial archive the crash left on disk and on the manifest
        let at = manifest
            .steps
            .iter()
            .position(|step| matches!(step.kind, StepKind::Archives { source: 1, dest: 0, .. }))
            .expect("an archives step");
        let partial = Uuid::new_v4();
        let settings = super::table_settings(&conf, "T");
        let partial_path = settings.get_archive_path("T").join(partial.to_string());
        std::fs::write(&partial_path, b"half a record").expect("a partial");
        manifest.steps[at].archive = Some(partial);
        manifest.write(dir.path()).expect("a manifest");
        // redone: the partial goes, the copy is made into a fresh archive
        let slots = Slots::default();
        let (records, bytes) = archives_step(&mut manifest, at, &conf, dir.path(), &slots, 1, 0, "T")
            .await
            .expect("the step");
        assert_eq!(records, 6);
        assert!(bytes > 0);
        assert!(!partial_path.exists(), "the partial archive was not removed");
        let recorded = manifest.steps[at].archive.expect("the archive is on the manifest");
        assert_ne!(recorded, partial);
        // the destination names every moved key and its own, in the recorded archive
        let dst = ArchiveMap::new("Shard-0", "T", &settings).await.expect("a map");
        assert!(dst.all_archives.borrow().contains(&recorded));
        for key in &keys {
            let entry = dst.find_partition(*key).unwrap_or_else(|| panic!("key {key:x} did not move"));
            assert_eq!(entry.archive, recorded);
            let payload = dst.read_record(&entry).await.expect("a read");
            assert_eq!(&payload[..], key.to_le_bytes().repeat(4).as_slice());
        }
        assert!(dst.find_partition(key_on(2, 9)).is_some(), "the destination's own record was lost");
        dst.close_all().await.expect("a close");
        // begun a third time, the finished copy is recognized and skipped
        let (records, _) = archives_step(&mut manifest, at, &conf, dir.path(), &slots, 1, 0, "T")
            .await
            .expect("the step");
        assert_eq!(records, 0, "a finished archives step was copied again");
        // the source is untouched until the reclaim
        let src = ArchiveMap::new("Shard-1", "T", &settings).await.expect("a map");
        assert_eq!(src.to_archive.borrow().len(), 6);
        src.close_all().await.expect("a close");
    });
}

/// A log id under a fixed leader
///
/// # Arguments
///
/// * `index` - The index
fn log_id(index: u64) -> crate::server::wal::frame::WalLogId {
    LogId::new(LeaderId::new(1, ShardAddr::from(1)), index)
}

/// A normal entry carrying a command
///
/// # Arguments
///
/// * `index` - The index
fn normal(index: u64) -> Entry {
    Entry::new_normal(
        log_id(index),
        Command {
            table: TableId::of("T"),
            tablet: 0,
            request: RequestId {
                bundle: [0u8; 16],
                index,
            },
            payload: vec![index as u8; 32],
        },
    )
}

/// A redone log step skips a group the destination already holds to the source's last index
///
/// The first run moves the group's entries, vote and checkpoint; the second finds the
/// destination at the same last index and appends nothing, so the log is not duplicated and
/// the group of another slot is never touched.
#[test]
fn a_redone_log_step_skips_a_group_already_moved() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let conf = conf_at(dir.path(), true);
    LocalExecutor::default().run(async {
        // two slots on two executors down to one: slot one's groups move to executor zero
        let before = Hosting::identity(2);
        let after = before.plan(1, true).expect("a plan");
        let manifest = Manifest::plan(&before, &after, &["T".to_string()], true);
        let moving = GroupId(0x11);
        let staying = GroupId(0x22);
        let mut slots = Slots::default();
        slots.groups.insert(moving, 1);
        slots.groups.insert(staying, 0);
        // the source's WAL: five entries, a vote and a checkpoint for the moving group
        let src_dir = dir.path().join(WAL_DIR).join("Shard-1");
        let wal = ShardWal::open(&src_dir, 1 << 24, 1 << 20).await.expect("a wal");
        let mut store = wal.store(moving);
        store.blocking_append((1..=5).map(normal).collect::<Vec<_>>()).await.expect("an append");
        use openraft::storage::RaftLogStorage as _;
        let vote = openraft::Vote::new(3, ShardAddr::from(1));
        store.save_vote(&vote).await.expect("a vote");
        store.save_committed(Some(log_id(4))).await.expect("a commit");
        store.purge(log_id(2)).await.expect("a purge");
        wal.flush().await.expect("a flush");
        wal.close().await.expect("a close");
        let mut checkpoint = Checkpoint::default();
        checkpoint.groups.insert(moving.to_string(), GroupCheckpoint::new(Some(log_id(2)), &Default::default()));
        checkpoint.write(&src_dir).await.expect("a checkpoint");
        // the first move
        let (groups, dropped) = log_step(&manifest, &conf, dir.path(), &slots, 1, 0).await.expect("the step");
        assert_eq!((groups, dropped), (1, 0));
        let dst_dir = dir.path().join(WAL_DIR).join("Shard-0");
        let view = super::group_log_view(&dst_dir, moving).await.expect("a view");
        assert_eq!(view.last, Some(5));
        assert_eq!(view.purged, Some(2));
        assert!(view.voted);
        assert!(view.checkpointed);
        let dst = ShardWal::open(&dst_dir, 1 << 24, 1 << 20).await.expect("a wal");
        assert_eq!(dst.indexes_of(moving), vec![3, 4, 5]);
        assert_eq!(dst.vote_of(moving), Some(vote.clone()));
        assert!(dst.groups().iter().all(|group| *group != staying), "a group of another slot moved");
        dst.close().await.expect("a close");
        // the second: the destination is at the source's last index and nothing is appended
        let (groups, _) = log_step(&manifest, &conf, dir.path(), &slots, 1, 0).await.expect("the step");
        assert_eq!(groups, 1, "a redone step still counts the group it checked");
        let dst = ShardWal::open(&dst_dir, 1 << 24, 1 << 20).await.expect("a wal");
        assert_eq!(dst.indexes_of(moving), vec![3, 4, 5], "a redone log step duplicated entries");
        assert_eq!(dst.vote_of(moving), Some(vote));
        dst.close().await.expect("a close");
        // the source still has its log until the reclaim
        let src = ShardWal::open(&src_dir, 1 << 24, 1 << 20).await.expect("a wal");
        assert_eq!(src.indexes_of(moving), vec![3, 4, 5]);
        src.close().await.expect("a close");
    });
}

/// A manifest planned towards one count refuses a claim under another and resumes under its own
#[test]
fn a_manifest_for_another_target_is_refused() {
    let dir = tempfile::tempdir().expect("a temp dir");
    // a cluster directory at four slots, laid out on four executors
    let first = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Bootstrap).expect("a claim");
    assert_eq!(first.slots, 4);
    // a rehome to two planned and on disk
    let before = Hosting::identity(4);
    let after = before.plan(2, true).expect("a plan");
    Manifest::plan(&before, &after, &["T".to_string()], true).write(dir.path()).expect("a manifest");
    // three is neither where the files are nor where they are going
    let error = StorageMeta::claim(dir.path(), 3, None, ClusterIntent::Bootstrap).expect_err("a third count started");
    assert!(
        matches!(
            error,
            crate::server::ServerError::Shoal(crate::server::errors::ShoalError::RehomeInProgress { from: 4, to: 2, configured: 3 })
        ),
        "{error:?}"
    );
    assert!(format!("{error}").contains("start with 2 cores"), "{error}");
    // and so is four, even though the files are still there: the plan has to finish first
    let error = StorageMeta::claim(dir.path(), 4, None, ClusterIntent::Bootstrap).expect_err("the origin count started");
    assert!(matches!(
        error,
        crate::server::ServerError::Shoal(crate::server::errors::ShoalError::RehomeInProgress { to: 2, configured: 4, .. })
    ));
    // two resumes it
    let resumed = StorageMeta::claim(dir.path(), 2, None, ClusterIntent::Bootstrap).expect("the planned count was refused");
    assert_eq!(resumed.rehome, Some(crate::server::meta::PendingRehome { from: 4, to: 2 }));
    assert_eq!(resumed.slots, 4);
}
