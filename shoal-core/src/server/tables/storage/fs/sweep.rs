//! Archives no map names, removed before a node's shards start
//!
//! A table's archives are one directory that every executor of a node shares, and each
//! executor's map knows only its own. An archive pass takes an archive out of its map before it
//! unlinks the file, so a crash between the two left a file no map names, which no pass would
//! weigh again; so did a job that crashed after writing the first records of a fresh active
//! archive and before logging any of them, and a build before [Resolved #339](../../../../../../docs/src/appendix/resolved/archives-known-after-restart.md)'s
//! fix, which left an archive written since its map's last commit unknown until it died
//! ([Resolved #340](../../../../../../docs/src/appendix/resolved/orphan-archives-swept.md)).
//!
//! This sweep runs once a start, after the rehome and before any shard opens a table, while the
//! pool holds every root's lock: no writer can create, name or remove an archive under it. It
//! runs in two phases, so a start that has nothing to remove reads no page of any index:
//!
//! 1. **Candidates.** Every map on disk is read as far as its manifest's counters and its intent
//!    log. An archive the manifest knows or counts, or the log names in an entry or a chain, is
//!    known, and one the log deletes is suspect. A file that is not known, or is suspect, is a
//!    candidate.
//! 2. **Confirmation**, only when there are candidates. Every map is opened as a shard opens it,
//!    its log replayed, and a candidate its replayed set or its counters know is kept, as is one
//!    any partition of its index names, found by one pass over the index. What is left is removed
//!    and counted.
//!
//! A table whose directory or maps cannot be read is left alone, and so is one whose archives
//! have no map at all: the sweep removes nothing it cannot prove dead, and never fails a start.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use glommio::{LocalExecutorBuilder, Placement};
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::conf::FileSystemTableConf;
use super::index::manifest::Manifest;
use super::map::{read_intents, ArchiveMap, MapIntent};
use crate::server::conf::Conf;
use crate::server::ServerError;

/// The memory the sweep's executor registers for its reads
///
/// It reads manifests, intent logs and, only to confirm a candidate, index pages, a few at a
/// time; a shard's default of 10 MiB would be charged to the user's locked memory for nothing,
/// and a read past it is served from the heap.
const SWEEP_IO_MEMORY: usize = 256 << 10;

/// The most executors a sweep runs on at once
///
/// A few: the shards take every one of their cpus once the sweep ends, and each executor's
/// registered memory is charged to the user's locked memory, which a test's login holds to 8 MiB.
const SWEEP_EXECUTORS: usize = 4;

/// The text between a map's name and a run's id in a run's file name
const RUN_INFIX: &str = ".run-";

/// What a node's sweep of its tables' archive directories found and removed
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// The tables whose directories were swept
    pub tables: u64,
    /// The archive files those directories held
    pub archives: u64,
    /// The files the first phase found no map to know, or a map's log to delete
    pub candidates: u64,
    /// The candidates a replayed map or its index named after all, which were kept
    pub kept: u64,
    /// The archives removed, since no map named them
    pub removed: u64,
    /// The bytes those archives held
    pub removed_bytes: u64,
    /// The tables left alone, each with why
    pub skipped: Vec<(String, String)>,
    /// How long the sweep took, in milliseconds
    pub millis: u64,
}

impl SweepReport {
    /// Fold one table's sweep into the node's
    ///
    /// # Arguments
    ///
    /// * `swept` - What the table's sweep found
    fn add(&mut self, swept: &TableSweep) {
        // one more table, and everything it counted
        self.tables += 1;
        self.archives += swept.archives;
        self.candidates += swept.candidates;
        self.kept += swept.kept;
        self.removed += swept.removed.len() as u64;
        self.removed_bytes += swept.removed_bytes;
    }

    /// Fold another executor's report of its tables into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The other executor's report
    fn merge(&mut self, other: SweepReport) {
        // every count summed, and every table it left alone kept
        self.tables += other.tables;
        self.archives += other.archives;
        self.candidates += other.candidates;
        self.kept += other.kept;
        self.removed += other.removed;
        self.removed_bytes += other.removed_bytes;
        self.skipped.extend(other.skipped);
    }
}

/// What one table's sweep found and removed
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSweep {
    /// The archive files the table's directory held
    pub archives: u64,
    /// The files the first phase found no map to know, or a map's log to delete
    pub candidates: u64,
    /// The candidates a replayed map or its index named after all, which were kept
    pub kept: u64,
    /// The archives removed, by id
    pub removed: Vec<Uuid>,
    /// The bytes those archives held
    pub removed_bytes: u64,
}

/// An archive file in a table's directory
#[derive(Debug, Clone)]
struct ArchiveFile {
    /// The archive's id, which names the file
    id: Uuid,
    /// The file
    path: PathBuf,
    /// Its length when it was listed
    bytes: u64,
}

/// What the first phase reads of one map: the archives it knows, and those its log deletes
#[derive(Debug, Default)]
struct KnownArchives {
    /// Every archive the manifest knows or counts, or the log names in an entry or a chain
    known: HashSet<Uuid>,
    /// Every archive the log deletes
    deleted: HashSet<Uuid>,
}

/// Whether a file name is an archive's: an id in its own hyphenated form, as one is written
///
/// # Arguments
///
/// * `name` - The file's name
fn archive_id(name: &str) -> Option<Uuid> {
    // an id parsed and written back the same, so no other form of an id is ever taken for one
    let id = Uuid::parse_str(name).ok()?;
    (id.to_string() == name).then_some(id)
}

/// Every archive file in a table's archive directory
///
/// Only regular files named as an archive is: never the map logs' directory, a link, or a file
/// of another name.
///
/// # Arguments
///
/// * `dir` - The table's archive directory
fn archive_files(dir: &Path) -> std::io::Result<Vec<ArchiveFile>> {
    // a directory that was never made holds no archive
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry?;
        // a regular file, as the entry names it without following a link
        if !entry.file_type()?.is_file() {
            continue;
        }
        // named as an archive is
        let name = entry.file_name();
        let Some(id) = name.to_str().and_then(archive_id) else {
            continue;
        };
        // and its length, which a removal counts
        let bytes = entry.metadata()?.len();
        files.push(ArchiveFile {
            id,
            path: entry.path(),
            bytes,
        });
    }
    Ok(files)
}

/// Whether a table's archive directory holds any archive file, read without an executor
///
/// It stops at the first one, and reads no file's length, since the sweep lists the directory
/// again once it has an executor.
///
/// # Arguments
///
/// * `dir` - The table's archive directory
fn holds_archives(dir: &Path) -> std::io::Result<bool> {
    // a directory that was never made holds no archive
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        // a regular file named as an archive is, as the listing judges one
        if entry.file_type()?.is_file() && entry.file_name().to_str().and_then(archive_id).is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The name of every map of a table on disk: each with a manifest, a run or an intent log
///
/// Every file in the maps directory is taken for a map's, so a file a map did not write is a map
/// the first phase fails to read, which leaves the table alone, rather than a map no one asks.
///
/// # Arguments
///
/// * `conf` - The table's storage settings
/// * `table` - The table
fn map_names(conf: &FileSystemTableConf, table: &str) -> std::io::Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    // the manifests and their runs in the maps directory, and the logs beside the archives
    for dir in [conf.get_archive_map_path(table), conf.get_archive_intent_path(table)] {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            // only files: a directory is the staged manifests' and holds none that counts
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            // a run is its map's name, the infix and its id; anything else is a map's own name
            let map = match name.rsplit_once(RUN_INFIX) {
                Some((map, id)) if id.parse::<u64>().is_ok() => map.to_owned(),
                _ => name,
            };
            names.insert(map);
        }
    }
    Ok(names)
}

/// Read what one map knows of its archives from its manifest's counters and its intent log
///
/// Nothing is replayed and no page of the index is read: an archive the manifest knows or counts,
/// or any entry or chain of the log names, is known, whatever came after it, and one the log
/// deletes is suspect.
///
/// # Arguments
///
/// * `conf` - The table's storage settings
/// * `table` - The table
/// * `name` - The map
#[instrument(name = "sweep::read_known", skip(conf), err(Debug))]
async fn read_known(
    conf: &FileSystemTableConf,
    table: &str,
    name: &str,
) -> Result<KnownArchives, ServerError> {
    let mut read = KnownArchives::default();
    // what the last commit knew and counted, if the map was ever committed
    let manifest_path = conf.get_archive_map_path(table).join(name);
    if let Some(manifest) = Manifest::load(&manifest_path).await? {
        read.known.extend(manifest.state.all_archives);
        read.known
            .extend(manifest.state.archive_bytes.iter().map(|(id, _)| *id));
    }
    // and every archive the log has named or deleted since
    let log = conf.get_archive_intent_path(table).join(name);
    if log.exists() {
        for intent in read_intents(&log).await? {
            match intent {
                MapIntent::Entry(entry) => {
                    read.known.insert(entry.archive);
                }
                MapIntent::Chain(chain) => {
                    read.known.insert(chain.base.archive);
                    read.known
                        .extend(chain.fragments.iter().map(|fragment| fragment.archive));
                }
                MapIntent::DeleteArchive(id) => {
                    read.deleted.insert(id);
                }
                MapIntent::Remove(_) => (),
            }
        }
    }
    Ok(read)
}

/// Keep every candidate one map, opened as a shard opens it, still names
///
/// A candidate is kept if the replayed map knows it, counts live records in it, or names it in any
/// partition of its index, which one pass over the index finds and is read only while a candidate
/// is left. Returns how many the index named that the counters did not, which a consistent map
/// never does.
///
/// # Arguments
///
/// * `map` - The map, replayed
/// * `left` - The candidates no map has named so far
#[instrument(name = "sweep::keep_named", skip_all, err(Debug))]
async fn keep_named(map: &ArchiveMap, left: &mut HashSet<Uuid>) -> Result<u64, ServerError> {
    // what the replayed set knows
    {
        let known = map.all_archives.borrow();
        left.retain(|id| !known.contains(id));
    }
    // and what the counters hold live records in
    let counted = map.archive_bytes();
    left.retain(|id| !counted.contains_key(id));
    // and anything the index names, which the counters should have said: one pass over it
    if left.is_empty() {
        return Ok(0);
    }
    let named = map.gather(left).await?;
    left.retain(|id| !named.contains_key(id));
    Ok(named.len() as u64)
}

/// Confirm the candidates no map on disk names, every map opened as a shard opens it
///
/// Returns the candidates left, which no map names, and how many the index named that the
/// counters did not.
///
/// # Arguments
///
/// * `conf` - The table's storage settings
/// * `table` - The table
/// * `names` - Every map of the table on disk
/// * `candidates` - What the first phase found
#[instrument(name = "sweep::confirm", skip(conf, names, candidates), err(Debug))]
async fn confirm(
    conf: &FileSystemTableConf,
    table: &str,
    names: &BTreeSet<String>,
    candidates: HashSet<Uuid>,
) -> Result<(HashSet<Uuid>, u64), ServerError> {
    let mut left = candidates;
    let mut uncounted = 0;
    for name in names {
        // nothing left to confirm needs no more maps opened
        if left.is_empty() {
            break;
        }
        // the map as a shard opens it: its manifest, its runs and its log replayed over them
        let map = ArchiveMap::new(name, table, conf).await?;
        let kept = keep_named(&map, &mut left).await;
        // its files closed whatever the check found
        let closed = map.close_all().await;
        uncounted += kept?;
        closed?;
    }
    Ok((left, uncounted))
}

/// Sweep one table's archive directory of the archives no map of it names
///
/// Run only while nothing else can touch the table's files: before any shard starts, with every
/// root's lock held.
///
/// # Arguments
///
/// * `conf` - The table's storage settings
/// * `table` - The table
///
/// # Errors
///
/// Fails if the directory, a map or its log cannot be read, before anything is removed; a removal
/// that fails is logged and the rest are tried.
#[instrument(name = "sweep::sweep_table", skip(conf), err(Debug))]
pub async fn sweep_table(
    conf: &FileSystemTableConf,
    table: &str,
) -> Result<TableSweep, ServerError> {
    let mut swept = TableSweep::default();
    // every archive file in the directory, listed before any map is read
    let archive_dir = conf.get_archive_path(table);
    let files = archive_files(&archive_dir)?;
    swept.archives = files.len() as u64;
    if files.is_empty() {
        return Ok(swept);
    }
    // every map of the table on disk; archives with none at all are left alone, since what would
    // name them is not where this node's settings look
    let names = map_names(conf, table)?;
    if names.is_empty() {
        return Err(ServerError::GlommioGeneric(format!(
            "{} holds {} archives and no map of the table is on disk",
            archive_dir.display(),
            files.len()
        )));
    }
    // what every map knows from its manifest and its log, and what its log deletes
    let mut known: HashSet<Uuid> = HashSet::new();
    let mut deleted: HashSet<Uuid> = HashSet::new();
    for name in &names {
        let read = read_known(conf, table, name).await?;
        known.extend(read.known);
        deleted.extend(read.deleted);
    }
    // a file no map knows, or one a map's log deleted, is a candidate
    let candidates: HashSet<Uuid> = files
        .iter()
        .map(|file| file.id)
        .filter(|id| !known.contains(id) || deleted.contains(id))
        .collect();
    swept.candidates = candidates.len() as u64;
    if candidates.is_empty() {
        return Ok(swept);
    }
    // the candidates every map, replayed, and its index leave
    let (orphans, uncounted) = confirm(conf, table, &names, candidates).await?;
    swept.kept = swept.candidates - orphans.len() as u64;
    // an index naming an archive its counters do not is a map whose counters are wrong
    if uncounted > 0 {
        event!(
            Level::WARN,
            msg = "an archive map's index names archives its counters hold nothing in; they were kept",
            table,
            uncounted,
        );
    }
    // every orphan removed: one already gone is no failure, and one that cannot be removed is
    // left for the next start, which finds it the same way. The directory is not synced: a
    // removal a crash undoes is made again by the next sweep
    for file in files.iter().filter(|file| orphans.contains(&file.id)) {
        match std::fs::remove_file(&file.path) {
            Ok(()) => {
                event!(
                    Level::INFO,
                    msg = "removed an archive no map names",
                    table,
                    path = %file.path.display(),
                    bytes = file.bytes,
                );
                swept.removed.push(file.id);
                swept.removed_bytes += file.bytes;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => {
                event!(
                    Level::WARN,
                    msg = "an archive no map names could not be removed",
                    table,
                    path = %file.path.display(),
                    error = %error,
                );
            }
        }
    }
    Ok(swept)
}

/// Sweep each table of a list in turn, a table that fails left alone and named
///
/// # Arguments
///
/// * `tables` - Each table and its storage settings
#[instrument(name = "sweep::sweep_tables", skip_all)]
async fn sweep_tables(tables: Vec<(String, FileSystemTableConf)>) -> SweepReport {
    let mut report = SweepReport::default();
    for (table, conf) in &tables {
        match sweep_table(conf, table).await {
            Ok(swept) => report.add(&swept),
            Err(error) => {
                // nothing of this table was removed, and the start goes on
                event!(
                    Level::WARN,
                    msg = "a table's archive directory was not swept; nothing of it was removed",
                    table = %table,
                    error = ?error,
                );
                report.skipped.push((table.clone(), format!("{error:?}")));
            }
        }
    }
    report
}

/// Sweep every persistent table's archive directory before any shard starts
///
/// The tables are dealt round at most [`SWEEP_EXECUTORS`] executors, each pinned to one of the
/// shards' cpus, which nothing else runs on yet, and each on a thread of its own so the caller's
/// affinity is left as it was: a map's log is read and decoded on a core, at about 150 ns an
/// intent, so a node of many busy tables would otherwise read every log on one. Only tables whose
/// directory holds an archive file are dealt, and a fresh node builds no executor. Returns once
/// every table is swept.
///
/// # Arguments
///
/// * `conf` - The Shoal config
/// * `tables` - The persistent tables, by name
/// * `cpus` - The cpus the shards will run on
#[instrument(name = "sweep::sweep_node", skip_all)]
pub fn sweep_node(conf: &Conf, tables: &[&str], cpus: &[usize]) -> SweepReport {
    let started = Instant::now();
    let mut report = SweepReport::default();
    // each table's settings, and only the tables with an archive file to judge
    let mut work: Vec<(String, FileSystemTableConf)> = Vec::new();
    for table in tables {
        let settings = conf.storage.settings_of(table).clone();
        match holds_archives(&settings.get_archive_path(table)) {
            Ok(true) => work.push(((*table).to_owned(), settings)),
            Ok(false) => (),
            Err(error) => {
                event!(Level::WARN, msg = "a table's archive directory could not be listed; nothing of it was removed", table, error = %error);
                report.skipped.push(((*table).to_owned(), error.to_string()));
            }
        }
    }
    // the tables dealt round as many executors as there are tables, cpus and the bound allow
    let executors = work.len().min(cpus.len()).min(SWEEP_EXECUTORS);
    let mut dealt: Vec<Vec<(String, FileSystemTableConf)>> = vec![Vec::new(); executors.max(1)];
    for (at, table) in work.into_iter().enumerate() {
        dealt[at % executors.max(1)].push(table);
    }
    // every executor started before any is waited for; nothing to judge starts none
    let mut running = Vec::with_capacity(executors);
    for (at, tables) in dealt.into_iter().enumerate().filter(|(_, tables)| !tables.is_empty()) {
        let names: Vec<String> = tables.iter().map(|(table, _)| table.clone()).collect();
        // pinned to a shard's cpu where there is one, and anywhere otherwise
        let placement = cpus.get(at).map_or(Placement::Unbound, |cpu| Placement::Fixed(*cpu));
        let spawned = LocalExecutorBuilder::new(placement)
            .name("shoal-sweep")
            .io_memory(SWEEP_IO_MEMORY)
            .spawn(move || sweep_tables(tables));
        running.push((names, spawned));
    }
    // and each one's report folded in, an executor that failed leaving its tables as they were
    for (names, spawned) in running {
        match spawned.and_then(glommio::ExecutorJoinHandle::join) {
            Ok(swept) => report.merge(swept),
            Err(error) => {
                event!(Level::WARN, msg = "some tables' archive directories were not swept; nothing of them was removed", tables = ?names, error = %error);
                report
                    .skipped
                    .extend(names.into_iter().map(|table| (table, error.to_string())));
            }
        }
    }
    report.millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    // a sweep that removed or kept something says so; one that found nothing is quiet
    if report.removed > 0 || report.kept > 0 {
        event!(
            Level::WARN,
            msg = "swept the archive directories of archives no map names",
            tables = report.tables,
            archives = report.archives,
            candidates = report.candidates,
            kept = report.kept,
            removed = report.removed,
            removed_bytes = report.removed_bytes,
            millis = report.millis,
        );
    } else {
        event!(
            Level::DEBUG,
            msg = "swept the archive directories",
            tables = report.tables,
            archives = report.archives,
            millis = report.millis,
        );
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tables::storage::fs::conf::{
        FileSystemLatencyWriterConf, FileSystemThroughputWriterConf,
    };
    use crate::server::tables::storage::fs::map::{ArchiveEntry, ChainEntry};
    use futures::AsyncWriteExt;
    use tempfile::TempDir;

    /// The table every test sweeps
    const TABLE: &str = "T";

    /// A scratch directory under cargo's target dir, never `/tmp`, which is usually a tmpfs where
    /// glommio silently disables direct I/O
    fn test_dir() -> TempDir {
        // beside the map's own tests' scratch directories
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/shoal-test-tmp");
        std::fs::create_dir_all(&base).expect("the scratch root");
        TempDir::new_in(&base).expect("a scratch dir")
    }

    /// The table's settings over a scratch directory, its paths made
    ///
    /// # Arguments
    ///
    /// * `dir` - The scratch directory
    async fn table_conf(dir: &TempDir) -> FileSystemTableConf {
        // both kinds of path in the one directory, as a node's default is
        let conf = FileSystemTableConf::default()
            .latency_sensitive(FileSystemLatencyWriterConf::default().path(dir.path()))
            .throughput_sensitive(FileSystemThroughputWriterConf::default().path(dir.path()));
        conf.setup_paths(TABLE).await.expect("the table's paths");
        conf
    }

    /// Write an archive file with a header and some bytes, as a job leaves one, and return its id
    ///
    /// # Arguments
    ///
    /// * `conf` - The table's settings
    fn write_archive(conf: &FileSystemTableConf) -> Uuid {
        // a fresh id, named as an archive is
        let id = Uuid::new_v4();
        let mut bytes = super::super::map::ArchiveFormat::header().to_vec();
        bytes.extend_from_slice(&[7u8; 4080]);
        std::fs::write(conf.get_archive_path(TABLE).join(id.to_string()), bytes)
            .expect("an archive");
        id
    }

    /// An entry of one record at the start of an archive's body
    ///
    /// # Arguments
    ///
    /// * `key` - The partition
    /// * `archive` - The archive it is in
    fn entry(key: u64, archive: Uuid) -> ArchiveEntry {
        ArchiveEntry {
            key,
            archive,
            offset: 16,
            size: 32,
        }
    }

    /// Commit a shard's map naming one partition in each archive given, its log begun again
    ///
    /// # Arguments
    ///
    /// * `conf` - The table's settings
    /// * `shard` - The shard
    /// * `archives` - The archives, one partition each
    async fn commit_map(conf: &FileSystemTableConf, shard: &str, archives: &[Uuid]) {
        let map = ArchiveMap::new(shard, TABLE, conf).await.expect("a map");
        for (key, archive) in archives.iter().enumerate() {
            // every partition named once, as a job's repoint names it
            let key = key as u64 + 1;
            let old = map.chain_of(key).await.expect("a lookup");
            map.all_archives.borrow_mut().insert(*archive);
            map.set_partition(key, entry(key, *archive), old.as_ref());
        }
        // committed, and its log begun again, as a compactor's start does
        let mut writer = map.compact_map().await.expect("a commit");
        writer.close().await.expect("the log closes");
        map.close_all().await.expect("the map closes");
    }

    /// Append intents to a shard's map log, as a compactor's job writes them
    ///
    /// # Arguments
    ///
    /// * `conf` - The table's settings
    /// * `shard` - The shard
    /// * `intents` - The intents, in order
    async fn log_intents(conf: &FileSystemTableConf, shard: &str, intents: &[MapIntent]) {
        // the frames a compactor stages, appended to whatever the log already holds
        let path = conf.get_archive_intent_path(TABLE).join(shard);
        let mut framed = std::fs::read(&path).unwrap_or_default();
        for intent in intents {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(intent).expect("an intent");
            framed.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            let mut hasher = gxhash::GxHasher::default();
            std::hash::Hasher::write(&mut hasher, &bytes);
            framed.extend_from_slice(&std::hash::Hasher::finish(&hasher).to_le_bytes());
            framed.extend_from_slice(&bytes);
        }
        std::fs::write(&path, framed).expect("the log");
    }

    /// The archive files left in the table's directory
    ///
    /// # Arguments
    ///
    /// * `conf` - The table's settings
    fn left(conf: &FileSystemTableConf) -> HashSet<Uuid> {
        archive_files(&conf.get_archive_path(TABLE))
            .expect("a listing")
            .into_iter()
            .map(|file| file.id)
            .collect()
    }

    /// Run a test's body on an executor of its own
    ///
    /// # Arguments
    ///
    /// * `body` - The test
    fn on_executor<F: std::future::Future<Output = ()> + 'static>(body: impl FnOnce() -> F + Send + 'static) {
        LocalExecutorBuilder::new(Placement::Unbound)
            .io_memory(SWEEP_IO_MEMORY)
            .spawn(body)
            .expect("an executor")
            .join()
            .expect("the test ran");
    }

    /// Only the archives no shard's map names go, across every shard sharing the directory
    #[test]
    fn only_archives_no_map_names_are_removed() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // two shards' maps, each naming archives of its own, committed
            let (first, second, third) = (write_archive(&conf), write_archive(&conf), write_archive(&conf));
            commit_map(&conf, "Shard-0", &[first]).await;
            commit_map(&conf, "Shard-1", &[second, third]).await;
            // a file no map ever named, as a job that died before logging its records leaves
            let orphan = write_archive(&conf);
            // and files that are not archives, which nothing touches
            let stray = conf.get_archive_path(TABLE).join("not-an-archive");
            std::fs::write(&stray, b"x").expect("a stray file");
            let upper = conf.get_archive_path(TABLE).join(Uuid::new_v4().to_string().to_uppercase());
            std::fs::write(&upper, b"x").expect("an id in another form");
            let swept = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!(swept.removed, vec![orphan]);
            assert_eq!(swept.removed_bytes, 4096);
            assert_eq!(swept.candidates, 1);
            assert_eq!(left(&conf), HashSet::from([first, second, third]));
            assert!(stray.exists() && upper.exists(), "a file that is not an archive was removed");
            // and a second sweep finds nothing
            let again = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!((again.candidates, again.removed.len()), (0, 0));
        });
    }

    /// An archive deleted in a map's log, its delete committed and the log begun again, goes
    #[test]
    fn an_archive_whose_delete_was_committed_is_removed() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // a map naming an old archive, then its partition moved to a new one
            let (old, new) = (write_archive(&conf), write_archive(&conf));
            commit_map(&conf, "Shard-0", &[old]).await;
            let map = ArchiveMap::new("Shard-0", TABLE, &conf).await.expect("a map");
            let held = map.chain_of(1).await.expect("a lookup");
            map.all_archives.borrow_mut().insert(new);
            map.set_partition(1, entry(1, new), held.as_ref());
            // the pass takes the old archive out of the map, and its delete is committed with
            // the log begun again, and the unlink never comes: item 340's window
            map.remove_archive(&old).await;
            let mut writer = map.compact_map().await.expect("a commit");
            writer.close().await.expect("the log closes");
            map.close_all().await.expect("the map closes");
            let swept = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!(swept.removed, vec![old]);
            assert_eq!(left(&conf), HashSet::from([new]));
        });
    }

    /// An archive a map's log deletes goes, while the log still holds its delete
    #[test]
    fn an_archive_its_log_deletes_is_removed() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // the old archive known to the commit, and the log moving its partition and deleting it
            let (old, new) = (write_archive(&conf), write_archive(&conf));
            commit_map(&conf, "Shard-0", &[old]).await;
            log_intents(
                &conf,
                "Shard-0",
                &[MapIntent::Entry(entry(1, new)), MapIntent::DeleteArchive(old)],
            )
            .await;
            let swept = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!(swept.removed, vec![old]);
            assert_eq!(left(&conf), HashSet::from([new]));
        });
    }

    /// An archive only a map's log names, since its last commit, is kept
    #[test]
    fn an_archive_only_the_log_names_is_kept() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // a commit that knows nothing, then an entry and a chain's fragment logged after it
            commit_map(&conf, "Shard-0", &[]).await;
            let (whole, base, fragment) = (write_archive(&conf), write_archive(&conf), write_archive(&conf));
            log_intents(
                &conf,
                "Shard-0",
                &[
                    MapIntent::Entry(entry(1, whole)),
                    MapIntent::Chain(ChainEntry {
                        base: entry(2, base),
                        fragments: vec![entry(2, fragment)],
                    }),
                ],
            )
            .await;
            let swept = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!((swept.candidates, swept.removed.len()), (0, 0));
            assert_eq!(left(&conf), HashSet::from([whole, base, fragment]));
        });
    }

    /// An archive a partition still names is kept, though no map's set knows it (item 339)
    ///
    /// A build before item 339's fix left an archive out of `all_archives`, and the commit that
    /// folded its entries left it there, counted and unknown: it is kept, and the map's open names
    /// it again, so a pass weighs it and deletes it once it dies. Once it is dead it goes.
    #[test]
    fn an_archive_a_partition_names_is_kept_and_known_again() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // a map whose partition is in an archive its set forgot, as item 339 left one
            let forgotten = write_archive(&conf);
            commit_map(&conf, "Shard-0", &[forgotten]).await;
            let map = ArchiveMap::new("Shard-0", TABLE, &conf).await.expect("a map");
            map.all_archives.borrow_mut().remove(&forgotten);
            let mut writer = map.compact_map().await.expect("a commit");
            writer.close().await.expect("the log closes");
            map.close_all().await.expect("the map closes");
            // kept, since its partition still names it
            let swept = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!(swept.removed, Vec::<Uuid>::new());
            assert_eq!(left(&conf), HashSet::from([forgotten]));
            // and known again once the map is opened
            let map = ArchiveMap::new("Shard-0", TABLE, &conf).await.expect("a map");
            assert!(map.all_archives.borrow().contains(&forgotten), "the counted archive is not known again");
            // its partition moved off it, and the map committed without it, as a build before
            // this fix would have committed it
            let moved = write_archive(&conf);
            let held = map.chain_of(1).await.expect("a lookup");
            map.all_archives.borrow_mut().insert(moved);
            map.set_partition(1, entry(1, moved), held.as_ref());
            map.all_archives.borrow_mut().remove(&forgotten);
            let mut writer = map.compact_map().await.expect("a commit");
            writer.close().await.expect("the log closes");
            map.close_all().await.expect("the map closes");
            // dead and unknown, it goes
            let swept = sweep_table(&conf, TABLE).await.expect("a sweep");
            assert_eq!(swept.removed, vec![forgotten]);
            assert_eq!(left(&conf), HashSet::from([moved]));
        });
    }

    /// Archives with no map on disk at all are left alone, and the sweep says why
    #[test]
    fn archives_with_no_map_are_left_alone() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // archives, and nothing that names them where the settings look
            let (first, second) = (write_archive(&conf), write_archive(&conf));
            let refused = sweep_table(&conf, TABLE).await;
            assert!(refused.is_err(), "archives with no map were swept: {refused:?}");
            assert_eq!(left(&conf), HashSet::from([first, second]));
        });
    }

    /// A map that cannot be read leaves its table alone
    #[test]
    fn an_unreadable_map_leaves_its_table_alone() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // one good map, a file in the maps directory no map wrote, and an orphan
            let named = write_archive(&conf);
            commit_map(&conf, "Shard-0", &[named]).await;
            std::fs::write(conf.get_archive_map_path(TABLE).join("Shard-0.bak"), b"not a manifest")
                .expect("a stray file");
            let orphan = write_archive(&conf);
            let refused = sweep_table(&conf, TABLE).await;
            assert!(refused.is_err(), "a table with an unreadable map was swept: {refused:?}");
            assert_eq!(left(&conf), HashSet::from([named, orphan]));
        });
    }

    /// A map's names are its manifest's, its runs' and its log's, and nothing in the temp directory
    #[test]
    fn a_maps_names_are_read_from_its_files() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            let maps = conf.get_archive_map_path(TABLE);
            std::fs::write(maps.join("Shard-0"), b"").expect("a manifest");
            std::fs::write(maps.join("Shard-1.run-7"), b"").expect("a run");
            std::fs::write(conf.get_archive_map_temp_path(TABLE).join("Shard-5"), b"").expect("a staged manifest");
            std::fs::write(conf.get_archive_intent_path(TABLE).join("Shard-2"), b"").expect("a log");
            let names = map_names(&conf, TABLE).expect("the names");
            assert_eq!(
                names,
                BTreeSet::from(["Shard-0".to_owned(), "Shard-1".to_owned(), "Shard-2".to_owned()])
            );
        });
    }

    /// A candidate the index names is kept though the map's set and its counters both miss it
    ///
    /// A consistent map's counters name every archive its index does, so the pass over the index
    /// finds nothing; it is what keeps a live archive when they are wrong, and says so.
    #[test]
    fn an_archive_the_index_names_is_kept_whatever_the_counters_say() {
        on_executor(|| async {
            let dir = test_dir();
            let conf = table_conf(&dir).await;
            // a map whose partition is in an archive its set and its counters have both lost
            let named = write_archive(&conf);
            commit_map(&conf, "Shard-0", &[named]).await;
            let map = ArchiveMap::new("Shard-0", TABLE, &conf).await.expect("a map");
            map.all_archives.borrow_mut().remove(&named);
            map.uncount(&named);
            // kept, by the index alone, and counted as named past the counters
            let mut candidates = HashSet::from([named]);
            let uncounted = keep_named(&map, &mut candidates).await.expect("a check");
            assert_eq!((candidates.len(), uncounted), (0, 1));
            map.close_all().await.expect("the map closes");
            assert_eq!(left(&conf), HashSet::from([named]));
        });
    }

    /// A node's tables are swept on several executors, their reports folded into one
    ///
    /// Five tables over two cpus: each with a map naming one archive and an orphan beside it, and
    /// a table with no archive at all, which no executor is started for.
    #[test]
    fn a_nodes_tables_are_swept_on_several_executors() {
        let dir = test_dir();
        let mut conf = Conf::default();
        conf.storage.default.filesystem.latency_sensitive.path = dir.path().to_path_buf();
        conf.storage.default.filesystem.throughput_sensitive.path = dir.path().to_path_buf();
        let tables = ["A", "B", "C", "D", "E"];
        // every table's map and its orphan, written on an executor of its own
        let settings = conf.storage.default.filesystem.clone();
        let orphans: Vec<(String, Uuid, Uuid)> = LocalExecutorBuilder::new(Placement::Unbound)
            .io_memory(SWEEP_IO_MEMORY)
            .spawn(move || async move {
                let mut written = Vec::new();
                for table in tables {
                    settings.setup_paths(table).await.expect("the table's paths");
                    let named = Uuid::new_v4();
                    let orphan = Uuid::new_v4();
                    for id in [named, orphan] {
                        std::fs::write(settings.get_archive_path(table).join(id.to_string()), [1u8; 4096])
                            .expect("an archive");
                    }
                    // its one partition in the archive its map names
                    let map = ArchiveMap::new("Shard-0", table, &settings).await.expect("a map");
                    map.all_archives.borrow_mut().insert(named);
                    map.set_partition(1, entry(1, named), None);
                    let mut writer = map.compact_map().await.expect("a commit");
                    writer.close().await.expect("the log closes");
                    map.close_all().await.expect("the map closes");
                    written.push((table.to_owned(), named, orphan));
                }
                written
            })
            .expect("an executor")
            .join()
            .expect("the tables were written");
        // swept with a sixth table that holds nothing, over two cpus
        let report = sweep_node(&conf, &["A", "B", "C", "D", "E", "Empty"], &[0, 1]);
        assert_eq!(
            (report.tables, report.archives, report.candidates, report.kept, report.removed),
            (5, 10, 5, 0, 5),
            "{report:?}"
        );
        assert_eq!(report.removed_bytes, 5 * 4096);
        assert!(report.skipped.is_empty(), "{report:?}");
        // every orphan gone and every named archive left
        for (table, named, orphan) in orphans {
            let dir = conf.storage.default.filesystem.get_archive_path(&table);
            assert!(dir.join(named.to_string()).exists(), "{table}'s named archive went");
            assert!(!dir.join(orphan.to_string()).exists(), "{table}'s orphan stayed");
        }
    }
}
