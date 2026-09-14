//! The startup executor that moves a node's files between executor counts
//!
//! A node's files are laid out per executor: `Shard-N` names a table's intent logs and archive
//! map, and on a cluster node the WAL, checkpoint and sidecars, of executor `N`. A node
//! restarted with fewer cores has executors that no longer exist and files they left behind;
//! one restarted with more has executors with nothing to do. Before
//! [F47](../../../docs/src/features/local-rehome.md) the marker refused both. This is what
//! replaced the refusal: a plan over the files, run once before any shard starts, that moves
//! what the vanished executors held onto the live ones - or, growing, deals some of what the
//! live ones hold onto the new ones - atomically and resumably.
//!
//! Atomically means a crash anywhere leaves the directory either as it was or as it will be,
//! never half way, as far as any reader can tell: nothing runs while this does, every step's
//! effect is durable before the manifest marks it done, and every step is idempotent when it is
//! begun again. Resumably means the manifest ([`manifest::Manifest`]) is on disk before the
//! first file moves and is what a restart continues from.
//!
//! The steps, in the order the manifest plans them:
//!
//! - **Fold** (standalone): a source's intent logs of a table are compacted into its archives,
//!   so its data is archives and a map and every later step is opaque bytes.
//! - **Archives**: the source's records whose tablet (standalone) or slot (cluster) now belongs
//!   to a destination are read, verified, and written as fresh records into one new archive on
//!   the destination, whose map is then saved. The archive's id is on the manifest before the
//!   first record, so a redo removes the partial one first.
//! - **Log** (cluster): the tablet groups of the source's WAL that now host on a destination
//!   are appended to the destination's WAL - entries above the purge point, the vote, the
//!   committed index, the purge point - and their checkpoint, retry sidecar, quarantine and
//!   retired markers moved with them. A partial install is dropped: the leader feeds again.
//! - **Reclaim**: a vanished source's files are deleted; a live donor's moved entries are
//!   removed from its map and its moved groups forgotten in its WAL. A donor's archives are
//!   not rewritten (O58).
//! - **Finalize**: the new hosting is written, the marker's `physical` moves, the manifest
//!   goes.
//!
//! What it moves is decided by the hosting ([`super::hosting::Hosting`]): per tablet on a
//! standalone node, per slot on a cluster node, where a slot's groups are found through the
//! map the control plane holds. Ephemeral tables move nothing. It runs on one core, blocking
//! the start (O59), and the report it leaves is the pool's to hand out and the fixture's to
//! print.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use futures::AsyncWriteExt as _;
use glommio::{LocalExecutorBuilder, Placement};
use openraft::storage::{RaftLogReader as _, RaftLogStorage as _, RaftLogStorageExt as _};
use tracing::{event, instrument, Level};

use super::conf::TableSettings;
use super::database::ShoalDatabase;
use super::hosting::Hosting;
use super::map::TabletMap;
use super::meta::{Identity, StorageMeta};
use super::ring::Ring;
use super::tables::storage::fs::conf::FileSystemTableConf;
use super::tables::storage::fs::map::{write_record, ArchiveEntry, ArchiveMap, SerializedMap};
use super::wal::{Checkpoint, Retries, ShardWal, WAL_DIR};
use super::{Conf, ServerError};
use crate::shared::identity::{GroupId, NodeId, TableId};

pub mod manifest;

pub use manifest::{Manifest, RehomeReport, Step, StepKind};

/// Where a rehome may be made to die, for the crash matrix
///
/// Every point is after a step's durable write and before the manifest marks it done, so
/// every point is a redo of exactly one step ([F47](../../../docs/src/features/local-rehome.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CrashPoint {
    /// Nowhere: the process runs
    None = 0,
    /// The manifest is written and no step has run
    Planned = 1,
    /// The first fold is durable and not marked
    AfterFold = 2,
    /// The first archives copy is saved and not marked
    AfterArchives = 3,
    /// The first log move is durable and not marked
    AfterLog = 4,
    /// The first reclaim is done and not marked
    AfterReclaim = 5,
    /// Every step but the finalize is marked, and the finalize has not begun
    BeforeFinalize = 6,
    /// The hosting and the marker are written, and the manifest is still there
    AfterFinalize = 7,
}

impl CrashPoint {
    /// Every point, in rehome order
    pub const ALL: [CrashPoint; 7] = [
        CrashPoint::Planned,
        CrashPoint::AfterFold,
        CrashPoint::AfterArchives,
        CrashPoint::AfterLog,
        CrashPoint::AfterReclaim,
        CrashPoint::BeforeFinalize,
        CrashPoint::AfterFinalize,
    ];

    /// The name the fixture arms a point by
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            CrashPoint::None => "none",
            CrashPoint::Planned => "planned",
            CrashPoint::AfterFold => "after_fold",
            CrashPoint::AfterArchives => "after_archives",
            CrashPoint::AfterLog => "after_log",
            CrashPoint::AfterReclaim => "after_reclaim",
            CrashPoint::BeforeFinalize => "before_finalize",
            CrashPoint::AfterFinalize => "after_finalize",
        }
    }

    /// The point a name arms, if it is one
    ///
    /// # Arguments
    ///
    /// * `name` - The name
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        CrashPoint::ALL.iter().copied().find(|point| point.name() == name)
    }
}

/// The crash point armed for this process, and where the rehome checks it
///
/// The install's ([`super::replication::crash_point`]) shape exactly, over the rehome's own
/// points; the two are armed apart, since a test arms one before the pool starts and the other
/// through the pool.
pub mod crash_point {
    use super::CrashPoint;
    use std::sync::atomic::{AtomicU8, Ordering};

    /// The armed point, as its discriminant; zero for none
    static ARMED: AtomicU8 = AtomicU8::new(0);

    /// Arm a point, or disarm every point with `None`
    ///
    /// # Arguments
    ///
    /// * `point` - The point
    pub fn arm(point: CrashPoint) {
        ARMED.store(point as u8, Ordering::Relaxed);
    }

    /// Arm a point by name, refusing a name that is not one
    ///
    /// # Arguments
    ///
    /// * `name` - The point's name, or `none` to disarm
    ///
    /// # Errors
    ///
    /// Refuses a name that is not a point.
    pub fn arm_named(name: &str) -> Result<(), String> {
        let point = if name == "none" {
            CrashPoint::None
        } else {
            CrashPoint::from_name(name).ok_or_else(|| format!("{name} is not a rehome crash point"))?
        };
        arm(point);
        Ok(())
    }

    /// The armed point
    #[must_use]
    pub fn armed() -> CrashPoint {
        let raw = ARMED.load(Ordering::Relaxed);
        CrashPoint::ALL.iter().copied().find(|point| *point as u8 == raw).unwrap_or(CrashPoint::None)
    }

    /// Die here if this point is armed
    ///
    /// Exits with 137, the way a kill does, so the fixture cannot tell a crash here from one
    /// it injected; nothing is flushed on the way out, which is the point.
    ///
    /// # Arguments
    ///
    /// * `point` - The point reached
    pub fn hit(point: CrashPoint) {
        if ARMED.load(Ordering::Relaxed) == point as u8 && point != CrashPoint::None {
            tracing::error!(msg = "dying at an armed rehome crash point", point = point.name());
            std::process::exit(137);
        }
    }
}

/// The name of an executor's files
///
/// # Arguments
///
/// * `executor` - The executor
#[must_use]
pub fn shard_name(executor: u16) -> String {
    // the name every per executor file has carried since the first shard
    format!("Shard-{executor}")
}

/// A table's storage settings by name: its own, or the default
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `table` - The table
#[must_use]
pub fn table_settings(conf: &Conf, table: &str) -> FileSystemTableConf {
    // the table's own settings if the configuration names it, else the default
    match conf.storage.tables.get(table) {
        Some(TableSettings::FS(settings)) => settings.clone(),
        None => conf.storage.default.filesystem.clone(),
    }
}

/// Which slot every group and tablet of this node is on, read from the map
///
/// A cluster node deals slots, and a slot's files are found by the groups and tablets the map
/// puts on it. A group or tablet the map does not name - a copy retired before the restart, a
/// set moved away - belongs to no slot, and a vanishing source's are carried to its first
/// destination so nothing is abandoned.
#[derive(Debug, Default)]
struct Slots {
    /// The slot hosting each group this node is a member of
    groups: HashMap<GroupId, u16>,
    /// The slot hosting each tablet this node holds a copy of
    tablets: HashMap<u16, u16>,
}

impl Slots {
    /// Read the slots off the map
    ///
    /// # Arguments
    ///
    /// * `map` - The map the control plane holds
    /// * `me` - This node
    fn from_map(map: &TabletMap, me: NodeId) -> Self {
        let mut slots = Slots::default();
        // every group this node is a member of names the slot hosting it
        for spec in map.replica_groups(me) {
            slots.groups.insert(spec.id, spec.mine);
            // and every tablet the group serves is on that slot too
            for tablet in &spec.tablets {
                slots.tablets.insert(*tablet, spec.mine);
            }
        }
        slots
    }
}

/// The rehome: the executor that runs the manifest's steps
pub struct Rehome;

impl Rehome {
    /// Run the rehome the claim found pending, if it found one, blocking until it is done
    ///
    /// On a dedicated executor pinned to one of the shards' cpus, before any shard has started
    /// and while the pool holds the directory's lock, so nothing else touches the files. A
    /// claim with no rehome pending returns at once.
    ///
    /// # Arguments
    ///
    /// * `conf` - The Shoal config
    /// * `identity` - Who this node is, with the rehome the claim found
    /// * `map` - The map the control plane holds, on a cluster node
    /// * `cpu` - The cpu to run on
    ///
    /// # Errors
    ///
    /// Fails if a step fails; the manifest is left for the next start to resume.
    #[instrument(name = "Rehome::run", skip_all, err(Debug))]
    pub fn run<S: ShoalDatabase>(
        conf: &Conf,
        identity: &Identity,
        map: Option<Arc<TabletMap>>,
        cpu: usize,
    ) -> Result<Option<RehomeReport>, ServerError> {
        // nothing pending is nothing to do
        let Some(pending) = identity.rehome else {
            return Ok(None);
        };
        event!(
            Level::INFO,
            msg = "rehoming the storage directory before any shard starts",
            from = pending.from,
            to = pending.to,
            slots = identity.slots,
        );
        // the tables whose files move: the persistent ones, by name
        let tables: Vec<String> = S::persistent_tables().iter().map(|table| (*table).to_string()).collect();
        let root = conf.storage.default.filesystem.latency_sensitive.path.clone();
        let cluster = conf.cluster.is_some();
        let node = identity.node;
        let conf = conf.clone();
        // one executor, one core, until it is done
        let executor = LocalExecutorBuilder::new(Placement::Fixed(cpu))
            .name("shoal-rehome")
            .make()?;
        let report = executor.run(async move {
            run_steps::<S>(&conf, &root, pending.from, pending.to, cluster, node, map.as_deref(), &tables).await
        })?;
        event!(
            Level::INFO,
            msg = "rehomed the storage directory",
            from = report.from,
            to = report.to,
            tablets_moved = report.tablets_moved,
            slots_moved = report.slots_moved,
            groups = report.groups,
            records = report.records,
            bytes = report.bytes,
            folded = report.folded,
            installs_dropped = report.installs_dropped,
            steps_redone = report.steps_redone,
            millis = report.millis,
        );
        Ok(Some(report))
    }
}

/// Plan or resume the manifest and run every step it has left
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `root` - The root of the storage directory
/// * `from` - The executor count the files are on
/// * `to` - The executor count to move them to
/// * `cluster` - Whether this is a cluster node
/// * `node` - This node
/// * `map` - The map, on a cluster node
/// * `tables` - The persistent tables
#[allow(clippy::too_many_arguments)]
#[instrument(name = "rehome::run_steps", skip_all, fields(from, to, cluster), err(Debug))]
async fn run_steps<S: ShoalDatabase>(
    conf: &Conf,
    root: &Path,
    from: usize,
    to: usize,
    cluster: bool,
    node: NodeId,
    map: Option<&TabletMap>,
    tables: &[String],
) -> Result<RehomeReport, ServerError> {
    // the manifest on disk, or a fresh plan written before the first file moves
    let (mut manifest, resumed) = match Manifest::read(root)? {
        Some(manifest) => (manifest, true),
        None => {
            let before = Hosting::read_or_identity(root, from)?;
            let after = before.plan(to, cluster)?;
            let manifest = Manifest::plan(&before, &after, tables, cluster);
            manifest.write(root)?;
            (manifest, false)
        }
    };
    crash_point::hit(CrashPoint::Planned);
    // a resumed manifest's first undone step is one the crashed run may have begun
    if resumed && manifest.next_step().is_some() {
        manifest.report.steps_redone += 1;
        event!(
            Level::INFO,
            msg = "resuming a rehome",
            step = manifest.next_step().map(|at| format!("{:?}", manifest.steps[at].kind)),
            done = manifest.steps.iter().filter(|step| step.done).count(),
            steps = manifest.steps.len(),
        );
    }
    // the slots the map puts this node's groups and tablets on, on a cluster node
    let slots = match (cluster, map) {
        (true, Some(map)) => Slots::from_map(map, node),
        _ => Slots::default(),
    };
    // the time this start spends is added to the report at every mark
    let mut tick = Instant::now();
    while let Some(at) = manifest.next_step() {
        let step = manifest.steps[at].clone();
        match &step.kind {
            StepKind::Fold { source, table } => {
                // a source's intent logs of a table into its archives
                let Some(named) = S::table_of_id(TableId::of(table)) else {
                    event!(Level::WARN, msg = "the manifest names a table this schema does not have", table);
                    manifest.steps[at].done = true;
                    continue;
                };
                let folded = S::fold_intents(&shard_name(*source), named, conf).await?;
                manifest.report.folded += folded;
                crash_point::hit(CrashPoint::AfterFold);
            }
            StepKind::Archives { source, dest, table } => {
                let (records, bytes) = archives_step(&mut manifest, at, conf, root, &slots, *source, *dest, table).await?;
                manifest.report.records += records;
                manifest.report.bytes += bytes;
                crash_point::hit(CrashPoint::AfterArchives);
            }
            StepKind::Log { source, dest } => {
                let (groups, dropped) = log_step(&manifest, conf, root, &slots, *source, *dest).await?;
                manifest.report.groups += groups;
                manifest.report.installs_dropped += dropped;
                crash_point::hit(CrashPoint::AfterLog);
            }
            StepKind::Reclaim { source } => {
                reclaim_step(&manifest, conf, root, &slots, *source, tables).await?;
                crash_point::hit(CrashPoint::AfterReclaim);
            }
            StepKind::Finalize => {
                crash_point::hit(CrashPoint::BeforeFinalize);
                // the hosting the files are under now, then the marker, then the manifest goes
                manifest.after.write(root)?;
                StorageMeta::finish_rehome(root, manifest.to)?;
                crash_point::hit(CrashPoint::AfterFinalize);
                manifest.report.millis += u64::try_from(tick.elapsed().as_millis()).unwrap_or(u64::MAX);
                Manifest::remove(root)?;
                return Ok(manifest.report);
            }
        }
        // the step's effect is durable: say so, whole and atomically
        manifest.steps[at].done = true;
        manifest.report.millis += u64::try_from(tick.elapsed().as_millis()).unwrap_or(u64::MAX);
        tick = Instant::now();
        manifest.write(root)?;
        event!(Level::DEBUG, msg = "a rehome step is done", step = ?step.kind, at, of = manifest.steps.len());
    }
    // a manifest with every step done and no finalize cannot be planned; finish it anyway
    manifest.after.write(root)?;
    StorageMeta::finish_rehome(root, manifest.to)?;
    Manifest::remove(root)?;
    Ok(manifest.report)
}

/// Where a source's item goes: a destination of this source, or none to stay
///
/// # Arguments
///
/// * `manifest` - The plan
/// * `source` - The source the item is on
/// * `owner` - The executor the hosting after puts the item on, if it names one
fn target_of(manifest: &Manifest, source: u16, owner: Option<u16>) -> Option<u16> {
    // the destinations the plan has steps for from this source
    let dests = manifest.dests_of(source);
    match owner {
        // the item stays where it is
        Some(owner) if owner == source => None,
        // the item moves to a destination the plan has a step for
        Some(owner) if dests.contains(&owner) => Some(owner),
        // an item the plan did not foresee on a vanishing source goes to its first destination
        // rather than being abandoned; on a live donor it stays
        _ => {
            if manifest.vanishes(source) {
                dests.first().copied()
            } else {
                None
            }
        }
    }
}

/// Where a source's archived record goes, by its key
///
/// # Arguments
///
/// * `manifest` - The plan
/// * `slots` - The slots the map puts this node's tablets on
/// * `source` - The source
/// * `key` - The record's partition key
fn record_target(manifest: &Manifest, slots: &Slots, source: u16, key: u64) -> Option<u16> {
    // the tablet the key names
    let tablet = Ring::tablet_of(key);
    // the executor the hosting after puts it on: through its slot on a cluster node, directly
    // on a standalone one
    //
    // truncation cannot happen: a tablet id is twelve bits
    #[allow(clippy::cast_possible_truncation)]
    let owner = if manifest.cluster {
        slots
            .tablets
            .get(&(tablet as u16))
            .map(|slot| manifest.after.host_of_slot(*slot))
            .and_then(|host| u16::try_from(host).ok())
    } else {
        u16::try_from(manifest.after.owner_of_tablet(tablet)).ok()
    };
    target_of(manifest, source, owner)
}

/// Where a source's tablet group goes
///
/// # Arguments
///
/// * `manifest` - The plan
/// * `slots` - The slots the map puts this node's groups on
/// * `source` - The source
/// * `group` - The group
fn group_target(manifest: &Manifest, slots: &Slots, source: u16, group: GroupId) -> Option<u16> {
    // the executor the hosting after puts the group's slot on, if the map names the group
    let owner = slots
        .groups
        .get(&group)
        .map(|slot| manifest.after.host_of_slot(*slot))
        .and_then(|host| u16::try_from(host).ok());
    target_of(manifest, source, owner)
}

/// Remove a file if it is there
///
/// # Arguments
///
/// * `path` - The file
fn remove_if_exists(path: &Path) -> Result<bool, ServerError> {
    // a file that is already gone is the outcome wanted
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ServerError::IO(error)),
    }
}

/// Remove a directory and everything in it if it is there
///
/// # Arguments
///
/// * `path` - The directory
fn remove_dir_if_exists(path: &Path) -> Result<(), ServerError> {
    // a directory that is already gone is the outcome wanted
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ServerError::IO(error)),
    }
}

/// Make a directory's entries durable
///
/// # Arguments
///
/// * `dir` - The directory
fn sync_dir(dir: &Path) -> Result<(), ServerError> {
    // a removal is only durable once the directory entry is
    if dir.exists() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Copy the archived records of one table that move from a source to a destination
///
/// Returns how many records and bytes were copied.
///
/// # Arguments
///
/// * `manifest` - The plan, whose step records the archive before the first record
/// * `at` - The step
/// * `conf` - The Shoal config
/// * `root` - The root of the storage directory
/// * `slots` - The slots the map puts this node's tablets on
/// * `source` - The executor the records are read from
/// * `dest` - The executor they are written to
/// * `table` - The table
#[allow(clippy::too_many_arguments)]
#[instrument(name = "rehome::archives_step", skip_all, fields(source, dest, table), err(Debug))]
async fn archives_step(
    manifest: &mut Manifest,
    at: usize,
    conf: &Conf,
    root: &Path,
    slots: &Slots,
    source: u16,
    dest: u16,
    table: &str,
) -> Result<(u64, u64), ServerError> {
    let settings = table_settings(conf, table);
    settings.setup_paths(table).await?;
    // both maps as they lie on disk
    let src = ArchiveMap::new(&shard_name(source), table, &settings).await?;
    let dst = ArchiveMap::new(&shard_name(dest), table, &settings).await?;
    // the records that move, in archive order so the reads are sequential
    let mut moving: Vec<ArchiveEntry> = src
        .to_archive
        .borrow()
        .values()
        .filter(|entry| record_target(manifest, slots, source, entry.key) == Some(dest))
        .copied()
        .collect();
    moving.sort_by_key(|entry| (entry.archive, entry.offset));
    let total_bytes: u64 = moving.iter().map(|entry| u64::try_from(entry.size).unwrap_or(u64::MAX)).sum();
    // a redo: an archive the destination's map names is a copy that finished before the
    // crash, whose records count as moved since the crashed run never wrote its count down;
    // one it does not name is the partial the crash left, and goes before the copy is redone
    if let Some(archive) = manifest.steps[at].archive {
        if dst.all_archives.borrow().contains(&archive) {
            event!(Level::INFO, msg = "an archives step had finished before the crash; skipping it", %archive, records = moving.len());
            src.close_all().await?;
            dst.close_all().await?;
            return Ok((u64::try_from(moving.len()).unwrap_or(u64::MAX), total_bytes));
        }
        let partial = settings.get_archive_path(table).join(archive.to_string());
        if remove_if_exists(&partial)? {
            event!(Level::INFO, msg = "removed the partial archive a crashed archives step left", %archive);
        }
    }
    // nothing to copy leaves the destination untouched
    if moving.is_empty() {
        src.close_all().await?;
        dst.close_all().await?;
        return Ok((0, 0));
    }
    // fold the destination's intent log into its map first, so the save below is the whole
    // of what the destination holds and nothing older replays over it
    let mut intent_writer = dst.compact_map().await?;
    intent_writer.close().await?;
    // the archive every record lands in, on the manifest before the first one
    let active = *dst.active.borrow();
    manifest.steps[at].archive = Some(active);
    manifest.write(root)?;
    let mut writer = dst.get_active_writer().await?;
    let mut records = 0u64;
    let mut bytes = 0u64;
    for entry in &moving {
        // read the record verified, write it as a fresh record, and point the map at it
        let payload = src.read_record(entry).await?;
        let offset = write_record(&mut writer, &payload[..]).await?;
        dst.set_partition(
            entry.key,
            ArchiveEntry {
                key: entry.key,
                archive: active,
                offset,
                size: entry.size,
            },
        );
        records += 1;
        bytes += u64::try_from(entry.size).unwrap_or(u64::MAX);
    }
    // the records durable before the map that names them
    writer.sync().await?;
    writer.close().await?;
    // a staged map a crash left would refuse the save
    remove_if_exists(&dst.temp_map_path)?;
    SerializedMap::save(&dst).await?;
    src.close_all().await?;
    dst.close_all().await?;
    event!(Level::INFO, msg = "copied a source's archived records to a destination", source, dest, table, records, bytes);
    Ok((records, bytes))
}

/// The groups named by the markers in one directory under a shard's WAL directory
///
/// # Arguments
///
/// * `dir` - The marker directory
fn marker_groups(dir: &Path) -> Vec<GroupId> {
    // a marker directory that was never made holds no markers
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    // every file named by a group's hex identity
    entries
        .flatten()
        .filter_map(|entry| u64::from_str_radix(&entry.file_name().to_string_lossy(), 16).ok().map(GroupId))
        .collect()
}

/// Move a marker file from one WAL directory to another, if it is there
///
/// # Arguments
///
/// * `src_dir` - The source's WAL directory
/// * `dst_dir` - The destination's WAL directory
/// * `sub` - The marker directory under each
/// * `group` - The group
#[instrument(name = "rehome::move_marker", skip_all, fields(sub, %group), err(Debug))]
async fn move_marker(src_dir: &Path, dst_dir: &Path, sub: &str, group: GroupId) -> Result<(), ServerError> {
    // the marker as the source wrote it, if it wrote one
    let from = src_dir.join(sub).join(format!("{group}"));
    let Ok(bytes) = std::fs::read(&from) else {
        return Ok(());
    };
    // written into place the way the shard writes it, and left at the source for the reclaim
    let to_dir = dst_dir.join(sub);
    std::fs::create_dir_all(&to_dir)?;
    super::wal::write_atomic(&to_dir, &format!("{group}"), bytes)
        .await
        .map_err(ServerError::IO)
}

/// Move the tablet groups of a source's WAL that now host on a destination
///
/// Returns how many groups were moved and how many partial installs were dropped.
///
/// # Arguments
///
/// * `manifest` - The plan
/// * `conf` - The Shoal config
/// * `root` - The root of the storage directory
/// * `slots` - The slots the map puts this node's groups on
/// * `source` - The executor whose WAL is read
/// * `dest` - The executor whose WAL is appended to
#[instrument(name = "rehome::log_step", skip_all, fields(source, dest), err(Debug))]
async fn log_step(
    manifest: &Manifest,
    conf: &Conf,
    root: &Path,
    slots: &Slots,
    source: u16,
    dest: u16,
) -> Result<(u64, u64), ServerError> {
    let cluster = conf.cluster.clone().unwrap_or_default();
    let src_dir = root.join(WAL_DIR).join(shard_name(source));
    let dst_dir = root.join(WAL_DIR).join(shard_name(dest));
    // a source that never wrote a WAL has no groups to move
    if !src_dir.exists() {
        return Ok((0, 0));
    }
    // both WALs recovered, with a small cache: nothing is served from them here
    let src_wal = ShardWal::open(&src_dir, cluster.replication.segment_bytes, 1 << 20)
        .await
        .map_err(ServerError::IO)?;
    let dst_wal = ShardWal::open(&dst_dir, cluster.replication.segment_bytes, 1 << 20)
        .await
        .map_err(ServerError::IO)?;
    let src_checkpoint = Checkpoint::read(&src_dir).await.map_err(ServerError::IO)?;
    let src_retries = Retries::read(&src_dir).await.map_err(ServerError::IO)?;
    let mut dst_checkpoint = Checkpoint::read(&dst_dir).await.map_err(ServerError::IO)?;
    let mut dst_retries = Retries::read(&dst_dir).await.map_err(ServerError::IO)?;
    // every group the source holds anything for
    let mut groups: BTreeSet<GroupId> = src_wal.groups().into_iter().collect();
    groups.extend(src_checkpoint.groups.keys().filter_map(|hex| u64::from_str_radix(hex, 16).ok().map(GroupId)));
    groups.extend(src_retries.groups.keys().filter_map(|hex| u64::from_str_radix(hex, 16).ok().map(GroupId)));
    groups.extend(marker_groups(&src_dir.join(super::shard::repair::QUARANTINE_DIR)));
    groups.extend(marker_groups(&src_dir.join(super::shard::migrate::RETIRED_DIR)));
    let mut moved = 0u64;
    let mut dropped = 0u64;
    for group in groups {
        // only the groups whose slot now hosts on this destination
        if group_target(manifest, slots, source, group) != Some(dest) {
            continue;
        }
        let mut src_store = src_wal.store(group);
        let mut dst_store = dst_wal.store(group);
        // the entries above what the destination already holds, if the source has more
        let src_last = src_wal.last_log_id_of(group);
        let dst_last = dst_wal.last_log_id_of(group);
        if let Some(last) = &src_last {
            if dst_last.as_ref() != Some(last) {
                let from = dst_last.as_ref().map_or(0, |log_id| log_id.index + 1);
                let entries = src_store.try_get_log_entries(from..).await.map_err(ServerError::IO)?;
                if !entries.is_empty() {
                    dst_store
                        .blocking_append(entries)
                        .await
                        .map_err(|error| ServerError::GlommioGeneric(format!("appending group {group}'s entries: {error}")))?;
                }
            }
        }
        // the vote, the committed index and the purge point, each idempotent
        if let Some(vote) = src_wal.vote_of(group) {
            dst_store.save_vote(&vote).await.map_err(ServerError::IO)?;
        }
        if let Some(committed) = src_store.read_committed().await.map_err(ServerError::IO)? {
            dst_store.save_committed(Some(committed)).await.map_err(ServerError::IO)?;
        }
        if let Some(purged) = src_store.get_log_state().await.map_err(ServerError::IO)?.last_purged_log_id {
            dst_store.purge(purged).await.map_err(ServerError::IO)?;
        }
        // the checkpoint and the retry sidecar entries
        if let Some(point) = src_checkpoint.get(group) {
            dst_checkpoint.groups.insert(group.to_string(), point.clone());
        }
        if let Some(retries) = src_retries.groups.get(&group.to_string()) {
            dst_retries.groups.insert(group.to_string(), retries.clone());
        }
        // the markers that hold through a restart
        move_marker(&src_dir, &dst_dir, super::shard::repair::QUARANTINE_DIR, group).await?;
        move_marker(&src_dir, &dst_dir, super::shard::migrate::RETIRED_DIR, group).await?;
        // a partial install is not carried: the leader feeds the group again
        let installs = src_dir.join(super::replication::snapshot::INSTALL_DIR);
        for suffix in ["pending", "part"] {
            if remove_if_exists(&installs.join(format!("{group}.{suffix}")))? {
                dropped += 1;
            }
        }
        moved += 1;
    }
    // everything appended durable, then the sidecar before the checkpoint, as the shard does
    dst_wal.flush().await.map_err(ServerError::IO)?;
    dst_wal.close().await.map_err(ServerError::IO)?;
    src_wal.close().await.map_err(ServerError::IO)?;
    dst_retries.write(&dst_dir).await.map_err(ServerError::IO)?;
    dst_checkpoint.write(&dst_dir).await.map_err(ServerError::IO)?;
    event!(Level::INFO, msg = "moved a source's tablet groups to a destination", source, dest, groups = moved, installs_dropped = dropped);
    Ok((moved, dropped))
}

/// Delete what a source no longer holds
///
/// A vanished executor's files go whole; a live donor's moved entries leave its map and its
/// moved groups are forgotten in its WAL.
///
/// # Arguments
///
/// * `manifest` - The plan
/// * `conf` - The Shoal config
/// * `root` - The root of the storage directory
/// * `slots` - The slots the map puts this node's tablets and groups on
/// * `source` - The executor
/// * `tables` - The persistent tables
#[instrument(name = "rehome::reclaim_step", skip_all, fields(source), err(Debug))]
async fn reclaim_step(
    manifest: &Manifest,
    conf: &Conf,
    root: &Path,
    slots: &Slots,
    source: u16,
    tables: &[String],
) -> Result<(), ServerError> {
    let name = shard_name(source);
    if manifest.vanishes(source) {
        // every table's files of this executor: the archives its map names, then the map, its
        // intent log, its staged map and its intent logs
        for table in tables {
            let settings = table_settings(conf, table);
            settings.setup_paths(table).await?;
            let map = ArchiveMap::new(&name, table, &settings).await?;
            let archives: Vec<uuid::Uuid> = map.all_archives.borrow().iter().copied().collect();
            map.close_all().await?;
            let archive_dir = settings.get_archive_path(table);
            for archive in archives {
                remove_if_exists(&archive_dir.join(archive.to_string()))?;
            }
            sync_dir(&archive_dir)?;
            remove_if_exists(&settings.get_archive_map_path(table).join(&name))?;
            remove_if_exists(&settings.get_archive_map_temp_path(table).join(&name))?;
            remove_if_exists(&settings.get_archive_intent_path(table).join(&name))?;
            let intent_dir = settings.get_intent_path(table);
            if let Ok(entries) = std::fs::read_dir(&intent_dir) {
                for entry in entries.flatten() {
                    if entry.file_name().to_string_lossy().starts_with(&format!("{name}-")) {
                        remove_if_exists(&entry.path())?;
                    }
                }
            }
            sync_dir(&intent_dir)?;
            sync_dir(&settings.get_archive_map_path(table))?;
            sync_dir(&settings.get_archive_intent_path(table))?;
        }
        // and the WAL directory whole, on a cluster node
        let wal_dir = root.join(WAL_DIR).join(&name);
        remove_dir_if_exists(&wal_dir)?;
        sync_dir(&root.join(WAL_DIR))?;
        event!(Level::INFO, msg = "reclaimed a vanished executor's files", source);
        return Ok(());
    }
    // a live donor keeps its files and forgets what moved
    for table in tables {
        let settings = table_settings(conf, table);
        settings.setup_paths(table).await?;
        let map = ArchiveMap::new(&name, table, &settings).await?;
        let moved: Vec<u64> = map
            .to_archive
            .borrow()
            .keys()
            .filter(|key| record_target(manifest, slots, source, **key).is_some())
            .copied()
            .collect();
        for key in &moved {
            map.remove_partition(*key);
        }
        // the map saved whole with the entries gone, and a fresh intent log
        remove_if_exists(&map.temp_map_path)?;
        let mut intent_writer = map.compact_map().await?;
        intent_writer.close().await?;
        map.close_all().await?;
        event!(Level::INFO, msg = "a donor forgot the records that moved", source, table, records = moved.len());
    }
    if manifest.cluster {
        let wal_dir = root.join(WAL_DIR).join(&name);
        if wal_dir.exists() {
            let cluster = conf.cluster.clone().unwrap_or_default();
            let wal = ShardWal::open(&wal_dir, cluster.replication.segment_bytes, 1 << 20)
                .await
                .map_err(ServerError::IO)?;
            let mut checkpoint = Checkpoint::read(&wal_dir).await.map_err(ServerError::IO)?;
            let mut retries = Retries::read(&wal_dir).await.map_err(ServerError::IO)?;
            let mut groups: BTreeSet<GroupId> = wal.groups().into_iter().collect();
            groups.extend(checkpoint.groups.keys().filter_map(|hex| u64::from_str_radix(hex, 16).ok().map(GroupId)));
            groups.extend(retries.groups.keys().filter_map(|hex| u64::from_str_radix(hex, 16).ok().map(GroupId)));
            groups.extend(marker_groups(&wal_dir.join(super::shard::repair::QUARANTINE_DIR)));
            groups.extend(marker_groups(&wal_dir.join(super::shard::migrate::RETIRED_DIR)));
            let mut forgotten = 0u64;
            for group in groups {
                if group_target(manifest, slots, source, group).is_none() {
                    continue;
                }
                // the log forgotten with a marker frame, the sidecars and markers dropped
                if wal.last_log_id_of(group).is_some() || wal.vote_of(group).is_some() {
                    wal.forget(group).map_err(ServerError::IO)?;
                }
                checkpoint.groups.remove(&group.to_string());
                retries.groups.remove(&group.to_string());
                remove_if_exists(&wal_dir.join(super::shard::repair::QUARANTINE_DIR).join(format!("{group}")))?;
                remove_if_exists(&wal_dir.join(super::shard::migrate::RETIRED_DIR).join(format!("{group}")))?;
                forgotten += 1;
            }
            wal.flush().await.map_err(ServerError::IO)?;
            wal.close().await.map_err(ServerError::IO)?;
            retries.write(&wal_dir).await.map_err(ServerError::IO)?;
            checkpoint.write(&wal_dir).await.map_err(ServerError::IO)?;
            event!(Level::INFO, msg = "a donor forgot the groups that moved", source, groups = forgotten);
        }
    }
    Ok(())
}

/// The paths of every per executor file a table keeps, for a test of the reclaim
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `table` - The table
/// * `executor` - The executor
#[must_use]
pub fn table_files_of(conf: &Conf, table: &str, executor: u16) -> Vec<PathBuf> {
    let settings = table_settings(conf, table);
    let name = shard_name(executor);
    // the map and its intent log, which every executor with data has
    let mut files = vec![
        settings.get_archive_map_path(table).join(&name),
        settings.get_archive_intent_path(table).join(&name),
    ];
    // and every intent log of the executor's, active or inactive
    if let Ok(entries) = std::fs::read_dir(settings.get_intent_path(table)) {
        files.extend(
            entries
                .flatten()
                .filter(|entry| entry.file_name().to_string_lossy().starts_with(&format!("{name}-")))
                .map(|entry| entry.path()),
        );
    }
    files
}

/// Whether any file of an executor is still in the directory
///
/// The WAL directory on a cluster node, and every table's map or intent log
/// ([F47](../../../docs/src/features/local-rehome.md)'s reclaim assertion).
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `tables` - The persistent tables
/// * `executor` - The executor
#[must_use]
pub fn executor_has_files(conf: &Conf, tables: &[&str], executor: u16) -> bool {
    let root = &conf.storage.default.filesystem.latency_sensitive.path;
    // the WAL directory, on a cluster node
    if root.join(WAL_DIR).join(shard_name(executor)).exists() {
        return true;
    }
    // or any table's file of the executor's
    tables
        .iter()
        .any(|table| table_files_of(conf, table, executor).iter().any(|path| path.exists()))
}

/// The set of executors that have any file in the directory, for a test
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `tables` - The persistent tables
/// * `upto` - The highest executor to look for
#[must_use]
pub fn executors_with_files(conf: &Conf, tables: &[&str], upto: u16) -> Vec<u16> {
    // every executor up to the bound that has anything left
    (0..=upto).filter(|executor| executor_has_files(conf, tables, *executor)).collect()
}

/// What a test needs to know about a group's log after a move, read off a WAL directory
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupLogView {
    /// The last entry's index
    pub last: Option<u64>,
    /// The purge point's index
    pub purged: Option<u64>,
    /// Whether a vote is recorded
    pub voted: bool,
    /// Whether the checkpoint names the group
    pub checkpointed: bool,
}

/// Read what one WAL directory holds of a group, for a test
///
/// # Arguments
///
/// * `wal_dir` - The WAL directory
/// * `group` - The group
///
/// # Errors
///
/// Fails if the WAL cannot be opened.
#[instrument(name = "rehome::group_log_view", skip_all, fields(%group), err(Debug))]
pub async fn group_log_view(wal_dir: &Path, group: GroupId) -> Result<GroupLogView, ServerError> {
    // the WAL recovered and the checkpoint beside it
    let wal = ShardWal::open(wal_dir, 1 << 24, 1 << 20).await.map_err(ServerError::IO)?;
    let checkpoint = Checkpoint::read(wal_dir).await.map_err(ServerError::IO)?;
    // what they say about the group
    let view = GroupLogView {
        last: wal.last_log_id_of(group).map(|log_id| log_id.index),
        purged: wal.store(group).purged_index(),
        voted: wal.vote_of(group).is_some(),
        checkpointed: checkpoint.get(group).is_some(),
    };
    wal.close().await.map_err(ServerError::IO)?;
    Ok(view)
}

/// The keys an executor's map of a table names, for a test
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `table` - The table
/// * `executor` - The executor
///
/// # Errors
///
/// Fails if the map cannot be read.
#[instrument(name = "rehome::archived_keys_of", skip_all, fields(table, executor), err(Debug))]
pub async fn archived_keys_of(conf: &Conf, table: &str, executor: u16) -> Result<HashSet<u64>, ServerError> {
    // the executor's map of the table, as it lies on disk
    let settings = table_settings(conf, table);
    settings.setup_paths(table).await?;
    let map = ArchiveMap::new(&shard_name(executor), table, &settings).await?;
    // every key it names
    let keys = map.to_archive.borrow().keys().copied().collect();
    map.close_all().await?;
    Ok(keys)
}

#[cfg(test)]
mod tests;
