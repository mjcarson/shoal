//! The paged index under a table's archive map: where every partition's records are, on disk
//!
//! Before [F76](../../../../../../../docs/src/features/paged-archive-map.md) a shard held an
//! entry for every partition it had ever archived in memory, about fifty bytes each, and nothing
//! evicted it. The index is now a small log-structured merge of sorted runs:
//!
//! - the **delta**, every change since the last flush, in memory and backed by the map's intent
//!   log, bounded by `delta_entries`;
//! - **runs**, immutable files of 4 KiB pages in key order, newest first, each with its directory
//!   and filter in memory and its pages read as lookups need them, through a cache bounded by
//!   `page_cache_bytes`;
//! - a **manifest**, which names the runs and is the map's commit point.
//!
//! A flush writes the delta as a new run; runs are merged while the newest times `merge_ratio` is
//! at least the run below it, so their sizes grow geometrically and a merge into the oldest drops
//! the removals no older run can need. A lookup answers as of its start: it takes the delta's
//! answer and the runs as they stand before it awaits anything, so a flush or a merge while it
//! reads changes nothing it sees.

pub mod bloom;
pub mod cache;
pub mod manifest;
pub mod page;
pub mod run;

use futures::stream::{self, StreamExt};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tracing::{event, instrument, Level};

use crate::server::ServerError;

use super::conf::ArchiveMapConf;
use super::map::ChainEntry;
use cache::PageCache;
use manifest::{Manifest, MapState};
pub use page::Change;
use page::Page;
use run::{run_files, run_path, Run, RunCursor, RunWriter};

/// How many pages a batch of lookups reads at once
const LOOKUP_READS_IN_FLIGHT: usize = 32;

/// The entries a merged run holds from which its merge is logged at info, not debug
///
/// A merge holds the compactor while it runs, and one into the oldest run of a large map is the
/// longest thing it does, so it is worth a line in a node's log.
const LARGE_MERGE: u64 = 1_000_000;

/// What the index can say about a key without reading anything
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    /// The key has no records: the delta removed it, or every run rules it out
    Absent,
    /// The key's chain, from the delta or a cached page
    Found(ChainEntry),
    /// A run may hold the key and its page is not cached
    Unknown,
}

/// One source of entries in key order: the delta as it was, or a run
enum Source {
    /// The delta's entries in the scanned ranges, copied when the scan began
    Delta(VecDeque<(u64, Change)>),
    /// A run, read a few pages at a time
    Run(RunCursor),
}

impl Source {
    /// The source's next entry in key order
    async fn next(&mut self) -> Result<Option<(u64, Change)>, ServerError> {
        match self {
            Source::Delta(entries) => Ok(entries.pop_front()),
            Source::Run(cursor) => cursor.next().await,
        }
    }
}

/// Several sources' entries merged in key order, the newest source's entry winning a key
pub struct MergeCursor {
    /// The sources, newest first
    sources: Vec<Source>,
    /// Each source's next entry, read ahead
    heads: Vec<Option<(u64, Change)>>,
    /// Whether every source's first entry has been read
    primed: bool,
}

impl MergeCursor {
    /// A merge of sources, newest first
    ///
    /// # Arguments
    ///
    /// * `sources` - The sources, newest first
    fn new(sources: Vec<Source>) -> Self {
        let heads = sources.iter().map(|_| None).collect();
        MergeCursor {
            sources,
            heads,
            primed: false,
        }
    }

    /// The next key and what the newest source holding it says, removals included
    pub async fn next(&mut self) -> Result<Option<(u64, Change)>, ServerError> {
        // every source's first entry, once
        if !self.primed {
            for (head, source) in self.heads.iter_mut().zip(self.sources.iter_mut()) {
                *head = source.next().await?;
            }
            self.primed = true;
        }
        // the smallest key; a tie goes to the newest source, which comes first
        let mut best: Option<(usize, u64)> = None;
        for (at, head) in self.heads.iter().enumerate() {
            if let Some((key, _)) = head {
                if best.is_none_or(|(_, smallest)| *key < smallest) {
                    best = Some((at, *key));
                }
            }
        }
        let Some((winner, key)) = best else {
            return Ok(None);
        };
        // the winner's entry, and its next read ahead
        let entry = self.heads[winner].take();
        self.heads[winner] = self.sources[winner].next().await?;
        // the same key in an older source is shadowed and skipped; a newer one cannot hold it,
        // since the winner was the first with the smallest key
        for at in winner + 1..self.heads.len() {
            if self.heads[at]
                .as_ref()
                .is_some_and(|(older, _)| *older == key)
            {
                self.heads[at] = self.sources[at].next().await?;
            }
        }
        Ok(entry)
    }
}

/// The live chains of some key ranges in key order, as the index stood when the scan began
pub struct Scan {
    /// The merge of the delta and every run
    merge: MergeCursor,
}

impl Scan {
    /// The next live partition in key order and its chain
    pub async fn next(&mut self) -> Result<Option<(u64, ChainEntry)>, ServerError> {
        // a removal shadows what an older run holds and is never handed out
        loop {
            match self.merge.next().await? {
                Some((key, Change::Set(chain))) => return Ok(Some((key, chain))),
                Some((_, Change::Removed)) => continue,
                None => return Ok(None),
            }
        }
    }
}

/// Sort some inclusive key ranges and join those that touch
///
/// # Arguments
///
/// * `ranges` - The ranges, each inclusive
#[must_use]
pub fn normalize(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.retain(|(low, high)| low <= high);
    ranges.sort_unstable();
    let mut joined: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (low, high) in ranges {
        match joined.last_mut() {
            // a range starting inside or just after the last one extends it
            Some((_, last)) if low <= last.saturating_add(1) => *last = (*last).max(high),
            _ => joined.push((low, high)),
        }
    }
    joined
}

/// A table's index of where every partition's records are, paged to disk
#[derive(Debug)]
pub struct PagedIndex {
    /// The directory the manifest and the runs are in
    dir: PathBuf,
    /// The shard the map is for, which names its files
    shard: String,
    /// The manifest's file
    manifest_path: PathBuf,
    /// Where a manifest is written before it is renamed over the last
    temp_path: PathBuf,
    /// The map's settings
    conf: ArchiveMapConf,
    /// Every change since the last flush, newest per key
    delta: RefCell<BTreeMap<u64, Change>>,
    /// The runs, newest first; a lookup or a scan takes this handle and keeps it
    runs: RefCell<Rc<Vec<Rc<Run>>>>,
    /// The id the next run is written under
    next_run: Cell<u64>,
    /// The pages point lookups have read
    cache: RefCell<PageCache>,
    /// How many pages lookups have read from disk, for a test of what a lookup costs
    pub page_reads: Cell<u64>,
}

impl PagedIndex {
    /// Open a map's index: its manifest, the runs it names, and nothing else
    ///
    /// Every run file the manifest does not name is one a flush or a merge wrote before a crash
    /// and no committed map points at, and is removed here. Returns the index and what the
    /// manifest counted, which the map's intent log is then replayed over.
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory the manifest and runs are in
    /// * `temp_dir` - The directory a manifest is written to before it is renamed
    /// * `shard` - The shard the map is for
    /// * `conf` - The map's settings
    #[instrument(name = "PagedIndex::open", skip(conf), err(Debug))]
    pub async fn open(
        dir: &Path,
        temp_dir: &Path,
        shard: &str,
        conf: &ArchiveMapConf,
    ) -> Result<(Self, MapState), ServerError> {
        let manifest_path = dir.join(shard);
        let temp_path = temp_dir.join(shard);
        // the committed manifest, or an empty map if none was ever committed
        let manifest = Manifest::load(&manifest_path).await?.unwrap_or_default();
        // every run file the manifest does not name goes, before any new one is written
        let mut highest = manifest.next_run;
        for (id, path) in run_files(dir, shard) {
            highest = highest.max(id + 1);
            if !manifest.runs.contains(&id) {
                event!(Level::WARN, msg = "removed an archive map run no manifest names", path = %path.display());
                std::fs::remove_file(&path)?;
            }
        }
        // the runs it names, newest first
        let mut runs = Vec::with_capacity(manifest.runs.len());
        for id in &manifest.runs {
            runs.push(Rc::new(Run::open(run_path(dir, shard, *id), *id).await?));
        }
        let index = PagedIndex {
            dir: dir.to_path_buf(),
            shard: shard.to_owned(),
            manifest_path,
            temp_path,
            conf: conf.clone(),
            delta: RefCell::new(BTreeMap::new()),
            runs: RefCell::new(Rc::new(runs)),
            next_run: Cell::new(highest),
            cache: RefCell::new(PageCache::new(conf.page_cache_bytes)),
            page_reads: Cell::new(0),
        };
        Ok((index, manifest.state))
    }

    /// Every file a shard's map keeps in a directory: its manifest and its runs
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory the map's files are in
    /// * `shard` - The shard
    #[must_use]
    pub fn files(dir: &Path, shard: &str) -> Vec<PathBuf> {
        let mut files = vec![dir.join(shard)];
        files.extend(run_files(dir, shard).into_iter().map(|(_, path)| path));
        files
    }

    /// What the index can say about a key without reading anything
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[must_use]
    pub fn probe(&self, key: u64) -> Probe {
        // the delta is the newest word on a key
        if let Some(change) = self.delta.borrow().get(&key) {
            return match change {
                Change::Set(chain) => Probe::Found(chain.clone()),
                Change::Removed => Probe::Absent,
            };
        }
        // then each run, newest first, as far as its filter and the cache can say
        let runs = self.runs.borrow().clone();
        let mut cache = self.cache.borrow_mut();
        for run in runs.iter() {
            if !run.may_contain(key) {
                continue;
            }
            let Some(page) = run.page_for(key) else {
                continue;
            };
            let Some(page) = cache.get(run.id, page) else {
                // this run may hold it and only a read can say
                return Probe::Unknown;
            };
            let view = page.view();
            if let Some(at) = view.find(key) {
                return match view.change(at, run.archives()) {
                    Change::Set(chain) => Probe::Found(chain),
                    Change::Removed => Probe::Absent,
                };
            }
        }
        // every run ruled it out
        Probe::Absent
    }

    /// A run's page, from the cache or read and cached
    ///
    /// # Arguments
    ///
    /// * `run` - The run
    /// * `page` - The page
    async fn page(&self, run: &Run, page: u32) -> Result<Rc<Page>, ServerError> {
        // the borrow ends before the read is awaited
        let cached = self.cache.borrow_mut().get(run.id, page);
        if let Some(cached) = cached {
            return Ok(cached);
        }
        let read = Rc::new(run.read_page(page).await?);
        self.page_reads.set(self.page_reads.get() + 1);
        self.cache.borrow_mut().insert(run.id, page, read.clone());
        Ok(read)
    }

    /// A partition's chain, reading the pages it needs, as the index stood when this was called
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    pub async fn lookup(&self, key: u64) -> Result<Option<ChainEntry>, ServerError> {
        // the delta's answer and the runs, both taken before anything is awaited
        if let Some(change) = self.delta.borrow().get(&key) {
            return Ok(change.chain().cloned());
        }
        let runs = self.runs.borrow().clone();
        // each run newest first, the first that holds the key answering for it
        for run in runs.iter() {
            if !run.may_contain(key) {
                continue;
            }
            let Some(number) = run.page_for(key) else {
                continue;
            };
            let page = self.page(run, number).await?;
            let view = page.view();
            if let Some(at) = view.find(key) {
                return Ok(view.change(at, run.archives()).into_chain());
            }
        }
        Ok(None)
    }

    /// Many partitions' chains at once, each page read once and several in flight
    ///
    /// Returns the chain of every key that has one; a key with none is left out.
    ///
    /// # Arguments
    ///
    /// * `keys` - The partition keys
    pub async fn lookup_many(&self, keys: &[u64]) -> Result<HashMap<u64, ChainEntry>, ServerError> {
        let mut found = HashMap::with_capacity(keys.len());
        // the delta answers first, and the keys it says nothing about are pending
        let mut pending: Vec<u64> = Vec::new();
        {
            let delta = self.delta.borrow();
            for key in keys {
                match delta.get(key) {
                    Some(Change::Set(chain)) => {
                        found.insert(*key, chain.clone());
                    }
                    Some(Change::Removed) => (),
                    None => pending.push(*key),
                }
            }
        }
        // the runs as they stand now, newest first
        let runs = self.runs.borrow().clone();
        for run in runs.iter() {
            if pending.is_empty() {
                break;
            }
            // the pages this run needs for the keys still pending, and the keys it rules out
            let mut wanted: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
            let mut rest = Vec::new();
            for key in pending.drain(..) {
                match run.page_for(key) {
                    Some(page) if run.may_contain(key) => wanted.entry(page).or_default().push(key),
                    _ => rest.push(key),
                }
            }
            // every page read once, several at a time
            let reads: Vec<(u32, Result<Rc<Page>, ServerError>)> =
                stream::iter(wanted.keys().copied())
                    .map(|number| async move { (number, self.page(run, number).await) })
                    .buffer_unordered(LOOKUP_READS_IN_FLIGHT)
                    .collect()
                    .await;
            for (number, page) in reads {
                let page = page?;
                let view = page.view();
                for key in wanted.remove(&number).unwrap_or_default() {
                    match view.find(key) {
                        Some(at) => {
                            if let Change::Set(chain) = view.change(at, run.archives()) {
                                found.insert(key, chain);
                            }
                        }
                        // not in this run, so an older one may hold it
                        None => rest.push(key),
                    }
                }
            }
            pending = rest;
        }
        Ok(found)
    }

    /// Record a change to a partition, in the delta
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `change` - Its chain, or that it was removed
    pub fn apply(&self, key: u64, change: Change) {
        self.delta.borrow_mut().insert(key, change);
    }

    /// What the delta holds for a key, if anything
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[must_use]
    pub fn in_delta(&self, key: u64) -> Option<Change> {
        self.delta.borrow().get(&key).cloned()
    }

    /// How many partitions the delta holds
    #[must_use]
    pub fn delta_len(&self) -> usize {
        self.delta.borrow().len()
    }

    /// How many runs make up the index
    #[must_use]
    pub fn run_count(&self) -> usize {
        self.runs.borrow().len()
    }

    /// How many entries the runs hold, removals and entries an older run still holds included
    #[must_use]
    pub fn run_entries(&self) -> u64 {
        self.runs.borrow().iter().map(|run| run.entries).sum()
    }

    /// The live chains of some key ranges in key order, as the index stands now
    ///
    /// The delta's entries in the ranges are copied and the runs are taken before this returns,
    /// so what the scan hands out is the index at this moment however long it takes.
    ///
    /// # Arguments
    ///
    /// * `ranges` - The key ranges, each inclusive
    #[must_use]
    pub fn scan(&self, ranges: Vec<(u64, u64)>) -> Scan {
        let ranges = normalize(ranges);
        // the delta's entries in the ranges, which the delta cap bounds
        let delta = self.delta.borrow();
        let entries: VecDeque<(u64, Change)> = ranges
            .iter()
            .flat_map(|(low, high)| delta.range(*low..=*high))
            .map(|(key, change)| (*key, change.clone()))
            .collect();
        drop(delta);
        // then every run, newest first
        let mut sources = vec![Source::Delta(entries)];
        sources.extend(
            self.runs
                .borrow()
                .iter()
                .map(|run| Source::Run(RunCursor::new(run.clone(), &ranges))),
        );
        Scan {
            merge: MergeCursor::new(sources),
        }
    }

    /// The path the next run is written to, and its id
    fn next_run(&self) -> (u64, PathBuf) {
        let id = self.next_run.get();
        self.next_run.set(id + 1);
        (id, run_path(&self.dir, &self.shard, id))
    }

    /// Write the delta as a new run and take it out of the delta, naming the run in no manifest
    ///
    /// The run is part of the index in memory at once and of the map on disk only once a commit
    /// names it, so a crash before that leaves the intent log to replay what it holds. A rehome
    /// stages runs this way so its destination's map is committed once, at the step's end
    /// ([F47](../../../../../../../docs/src/features/local-rehome.md)).
    #[instrument(name = "PagedIndex::stage", skip_all, err(Debug))]
    pub async fn stage(&self) -> Result<(), ServerError> {
        // the delta as it is now, in key order
        let changes: Vec<(u64, Change)> = self
            .delta
            .borrow()
            .iter()
            .map(|(key, change)| (*key, change.clone()))
            .collect();
        if changes.is_empty() {
            return Ok(());
        }
        // a removal shadows only an older run, and with none there is nothing for it to shadow
        let drop_removed = self.runs.borrow().is_empty();
        let (id, path) = self.next_run();
        let mut writer = RunWriter::create(path, id, changes.len(), self.conf.filter_bits).await?;
        for (key, change) in &changes {
            if drop_removed && *change == Change::Removed {
                continue;
            }
            writer.push(*key, change.clone()).await?;
        }
        let run = writer.finish().await?;
        // installed and taken out of the delta together, with nothing awaited between: a reader
        // sees the change in the delta or in the run, never in neither
        let mut runs: Vec<Rc<Run>> = Vec::with_capacity(self.runs.borrow().len() + 1);
        if let Some(run) = run {
            runs.push(Rc::new(run));
        }
        runs.extend(self.runs.borrow().iter().cloned());
        *self.runs.borrow_mut() = Rc::new(runs);
        // only what was written leaves the delta; a change made while it was written stays
        let mut delta = self.delta.borrow_mut();
        for (key, change) in changes {
            if delta.get(&key) == Some(&change) {
                delta.remove(&key);
            }
        }
        Ok(())
    }

    /// Merge two adjacent runs into one, the newer one's entry winning a key
    ///
    /// # Arguments
    ///
    /// * `newer` - The newer run
    /// * `older` - The run below it
    /// * `drop_removed` - Whether the merged run is the oldest, so its removals shadow nothing
    #[instrument(name = "PagedIndex::merge", skip_all, fields(newer = newer.entries, older = older.entries), err(Debug))]
    async fn merge(
        &self,
        newer: &Rc<Run>,
        older: &Rc<Run>,
        drop_removed: bool,
    ) -> Result<Option<Run>, ServerError> {
        let (id, path) = self.next_run();
        // sized for every key of both, which is at most what it holds
        let keys = usize::try_from(newer.entries + older.entries).unwrap_or(usize::MAX);
        let mut writer = RunWriter::create(path, id, keys, self.conf.filter_bits).await?;
        let whole = [(0, u64::MAX)];
        let mut merge = MergeCursor::new(vec![
            Source::Run(RunCursor::new(newer.clone(), &whole)),
            Source::Run(RunCursor::new(older.clone(), &whole)),
        ]);
        while let Some((key, change)) = merge.next().await? {
            if drop_removed && change == Change::Removed {
                continue;
            }
            writer.push(key, change).await?;
        }
        writer.finish().await
    }

    /// Flush the delta and merge the runs that are due, then commit a manifest naming them
    ///
    /// Merges run while the newest run times `merge_ratio` is at least the run below it. Each
    /// merged run replaces its two in memory at once; their files go only once a durable manifest
    /// no longer names them, so a crash anywhere in here leaves the last manifest's runs whole.
    ///
    /// # Arguments
    ///
    /// * `state` - What the map counts, as it stands, for the manifest
    #[instrument(name = "PagedIndex::commit", skip_all, err(Debug))]
    pub async fn commit(&self, state: MapState) -> Result<(), ServerError> {
        // the delta first, as the newest run
        self.stage().await?;
        // then merges, until the sizes are geometric again
        let ratio = self.conf.merge_ratio.max(2);
        let mut retired: Vec<Rc<Run>> = Vec::new();
        loop {
            let runs = self.runs.borrow().clone();
            if runs.len() < 2 || runs[0].entries.saturating_mul(ratio) < runs[1].entries {
                break;
            }
            let oldest = runs.len() == 2;
            let started = std::time::Instant::now();
            let merged = self.merge(&runs[0], &runs[1], oldest).await?;
            // a merge holds the compactor for as long as it takes, so a large one says so
            let entries = merged.as_ref().map_or(0, |run| run.entries);
            let removed = merged.as_ref().map_or(0, |run| run.removed);
            let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            if entries >= LARGE_MERGE {
                event!(Level::INFO, msg = "merged two archive map runs", shard = %self.shard, newer = runs[0].entries, older = runs[1].entries, entries, removed, millis);
            } else {
                event!(Level::DEBUG, msg = "merged two archive map runs", shard = %self.shard, newer = runs[0].entries, older = runs[1].entries, entries, removed, millis);
            }
            // the merged run in place of the two, with nothing awaited between
            let mut next: Vec<Rc<Run>> = Vec::with_capacity(runs.len() - 1);
            if let Some(merged) = merged {
                next.push(Rc::new(merged));
            }
            next.extend(runs[2..].iter().cloned());
            *self.runs.borrow_mut() = Rc::new(next);
            retired.push(runs[0].clone());
            retired.push(runs[1].clone());
        }
        // the manifest naming the runs as they stand
        let manifest = Manifest {
            runs: self.runs.borrow().iter().map(|run| run.id).collect(),
            next_run: self.next_run.get(),
            state,
        };
        manifest.save(&self.manifest_path, &self.temp_path).await?;
        // no durable manifest names a retired run now, so its file goes; a lookup still reading
        // it holds its handle, which outlives the unlink
        for run in retired {
            self.cache.borrow_mut().forget(run.id);
            glommio::io::remove(&run.path).await?;
            if let Ok(run) = Rc::try_unwrap(run) {
                run.close().await?;
            }
        }
        Ok(())
    }

    /// The bytes the index holds in memory: the delta, every run's directory and filter, the cache
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        // a delta entry is its key and change in a B-tree node, and its fragments on the heap
        let delta = self.delta.borrow();
        let per_entry = std::mem::size_of::<(u64, Change)>() + 16;
        let fragments: usize = delta
            .values()
            .filter_map(Change::chain)
            .map(|chain| {
                chain.fragments.capacity() * std::mem::size_of::<super::map::ArchiveEntry>()
            })
            .sum();
        let runs: usize = self
            .runs
            .borrow()
            .iter()
            .map(|run| run.resident_bytes())
            .sum();
        delta.len() * per_entry + fragments + runs + self.cache.borrow().bytes()
    }

    /// How many pages are cached
    #[must_use]
    pub fn cached_pages(&self) -> usize {
        self.cache.borrow().len()
    }

    /// Close every run's handle
    pub async fn close(&self) -> Result<(), ServerError> {
        // the runs are let go, each closed if nothing else still holds it
        let runs = std::mem::take(&mut *self.runs.borrow_mut());
        if let Ok(runs) = Rc::try_unwrap(runs) {
            for run in runs {
                if let Ok(run) = Rc::try_unwrap(run) {
                    run.close().await?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use glommio::LocalExecutor;
    use std::collections::{BTreeMap, HashMap};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::manifest::MapState;
    use super::{Change, PagedIndex, Probe};
    use crate::server::errors::ShoalError;
    use crate::server::tables::storage::fs::conf::ArchiveMapConf;
    use crate::server::tables::storage::fs::map::{ArchiveEntry, ChainEntry};
    use crate::server::ServerError;

    /// Create a temp dir on a filesystem that supports direct IO
    ///
    /// `TempDir::new` uses `/tmp`, which is usually tmpfs, and glommio silently
    /// disables O_DIRECT there.
    fn test_dir() -> TempDir {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/shoal-test-tmp");
        std::fs::create_dir_all(&base).expect("Failed to create test tmp dir");
        TempDir::new_in(&base).expect("Failed to create temp dir")
    }

    /// Open an index under a directory, its maps and temp directories made
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory
    /// * `conf` - The map's settings
    async fn open(dir: &Path, conf: &ArchiveMapConf) -> PagedIndex {
        let maps = dir.join("maps");
        let temp = maps.join("temp");
        std::fs::create_dir_all(&temp).expect("dirs");
        PagedIndex::open(&maps, &temp, "Shard-0", conf)
            .await
            .expect("an index")
            .0
    }

    /// A chain of one record for a key, somewhere in an archive
    ///
    /// # Arguments
    ///
    /// * `key` - The partition's key
    /// * `archive` - The archive
    /// * `offset` - Where its record is
    fn chain(key: u64, archive: Uuid, offset: u64) -> ChainEntry {
        ChainEntry {
            base: ArchiveEntry {
                key,
                archive,
                offset,
                size: 100,
            },
            fragments: Vec::new(),
        }
    }

    /// Spread a counter over the key space the way partition keys are
    ///
    /// # Arguments
    ///
    /// * `at` - The counter
    fn key(at: u64) -> u64 {
        super::bloom::mix(at)
    }

    /// The newest word on a key wins: the delta over every run, a newer run over an older, and a
    /// removal over whatever an older run still holds
    #[test]
    fn a_lookup_finds_the_newest_value_across_the_delta_and_runs() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            // a run five times the one above it is not merged at a ratio of two, so each commit
            // leaves a run of its own
            let conf = ArchiveMapConf::builder().merge_ratio(2);
            let index = open(temp_dir.path(), &conf).await;
            let (old, new) = (Uuid::new_v4(), Uuid::new_v4());
            // the oldest run: keys 0..1000, all in the old archive
            for at in 0..1000 {
                index.apply(key(at), Change::Set(chain(key(at), old, at)));
            }
            index.commit(MapState::default()).await.expect("a commit");
            // a newer run: the first hundred moved, the next hundred removed
            for at in 0..100 {
                index.apply(key(at), Change::Set(chain(key(at), new, at)));
            }
            for at in 100..200 {
                index.apply(key(at), Change::Removed);
            }
            index.commit(MapState::default()).await.expect("a commit");
            assert_eq!(index.run_count(), 2, "the commits were merged");
            // and the delta: the first ten removed, and one key brought back
            for at in 0..10 {
                index.apply(key(at), Change::Removed);
            }
            index.apply(key(150), Change::Set(chain(key(150), new, 1)));
            // every key as its newest word has it, one at a time and all at once
            let keys: Vec<u64> = (0..1020).map(key).collect();
            let many = index.lookup_many(&keys).await.expect("lookups");
            for at in 0..1020 {
                let expected = match at {
                    0..10 => None,
                    150 => Some(chain(key(150), new, 1)),
                    10..100 => Some(chain(key(at), new, at)),
                    100..200 => None,
                    200..1000 => Some(chain(key(at), old, at)),
                    _ => None,
                };
                let found = index.lookup(key(at)).await.expect("a lookup");
                assert_eq!(found, expected, "key {at}");
                assert_eq!(many.get(&key(at)).cloned(), expected, "key {at} in a batch");
            }
            // a key the delta removed probes absent without a read
            assert_eq!(index.probe(key(3)), Probe::Absent);
            index.close().await.expect("a close");
        });
    }

    /// A scan of some tablets' ranges is the whole index filtered to them, in key order, across
    /// the delta, runs, merges and removals
    #[test]
    fn a_tablet_scan_equals_the_whole_index_filtered() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let conf = ArchiveMapConf::builder().merge_ratio(2);
            let index = open(temp_dir.path(), &conf).await;
            let archive = Uuid::new_v4();
            // what the index should hold, by key
            let mut model: BTreeMap<u64, ChainEntry> = BTreeMap::new();
            for round in 0..3000u64 {
                let at = key(round % 1100);
                if round % 5 == 4 {
                    index.apply(at, Change::Removed);
                    model.remove(&at);
                } else {
                    let held = chain(at, archive, round);
                    index.apply(at, Change::Set(held.clone()));
                    model.insert(at, held);
                }
                if round % 400 == 399 {
                    index.commit(MapState::default()).await.expect("a commit");
                }
            }
            // ranges over a few tablets: a tablet is a key's top twelve bits
            let ranges = vec![
                (0x0000_0000_0000_0000, 0x00FF_FFFF_FFFF_FFFF),
                (0x7000_0000_0000_0000, 0x7FFF_FFFF_FFFF_FFFF),
                (0xFFF0_0000_0000_0000, u64::MAX),
            ];
            let mut scan = index.scan(ranges.clone());
            let mut scanned = Vec::new();
            while let Some(held) = scan.next().await.expect("a scan") {
                scanned.push(held);
            }
            let expected: Vec<(u64, ChainEntry)> = model
                .iter()
                .filter(|(key, _)| ranges.iter().any(|(low, high)| low <= *key && *key <= high))
                .map(|(key, held)| (*key, held.clone()))
                .collect();
            assert!(!expected.is_empty());
            assert_eq!(scanned, expected);
            // and the whole of it is the model
            let mut scan = index.scan(vec![(0, u64::MAX)]);
            let mut whole = Vec::new();
            while let Some(held) = scan.next().await.expect("a scan") {
                whole.push(held);
            }
            assert_eq!(whole, model.into_iter().collect::<Vec<_>>());
            index.close().await.expect("a close");
        });
    }

    /// A lookup that started before a flush and a merge answers as the index stood when it
    /// started, though the merge removed the run file it was reading
    ///
    /// The same window the loader always allowed between a lookup and its read: what it finds is
    /// what the map named at some moment, never a mix.
    #[test]
    fn a_lookup_across_a_flush_and_a_merge_answers_as_of_its_start() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let conf = ArchiveMapConf::builder().merge_ratio(2);
            let (old, new) = (Uuid::new_v4(), Uuid::new_v4());
            // a committed run holding the key, opened again so no page is cached
            {
                let index = open(temp_dir.path(), &conf).await;
                for at in 0..500 {
                    index.apply(key(at), Change::Set(chain(key(at), old, at)));
                }
                index.commit(MapState::default()).await.expect("a commit");
                index.close().await.expect("a close");
            }
            let index = open(temp_dir.path(), &conf).await;
            assert_eq!(index.probe(key(7)), Probe::Unknown);
            // the lookup starts, and is waiting on its page read
            let mut lookup = Box::pin(index.lookup(key(7)));
            assert!(futures::poll!(&mut lookup).is_pending());
            // meanwhile the key moves, and a commit flushes it and merges the old run away
            for at in 0..500 {
                index.apply(key(at), Change::Set(chain(key(at), new, at)));
            }
            index.commit(MapState::default()).await.expect("a commit");
            assert_eq!(index.run_count(), 1, "the runs were not merged");
            // the started lookup answers as of its start, and a new one as of now
            assert_eq!(lookup.await.expect("a lookup"), Some(chain(key(7), old, 7)));
            assert_eq!(
                index.lookup(key(7)).await.expect("a lookup"),
                Some(chain(key(7), new, 7))
            );
            index.close().await.expect("a close");
        });
    }

    /// A run written but never named by a manifest is removed when the index is opened
    ///
    /// What a crash between a flush's run and its manifest leaves: the intent log still holds
    /// what the run did, and replays it.
    #[test]
    fn a_run_no_manifest_names_is_removed_at_open() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let conf = ArchiveMapConf::default();
            let archive = Uuid::new_v4();
            let maps = temp_dir.path().join("maps");
            {
                let index = open(temp_dir.path(), &conf).await;
                // one committed run, then one staged and never committed
                index.apply(key(1), Change::Set(chain(key(1), archive, 1)));
                index.commit(MapState::default()).await.expect("a commit");
                index.apply(key(2), Change::Set(chain(key(2), archive, 2)));
                index.stage().await.expect("a stage");
                assert_eq!(index.run_count(), 2);
                index.close().await.expect("a close");
            }
            assert_eq!(super::run::run_files(&maps, "Shard-0").len(), 2);
            // opened again, the staged run is gone and what it held with it
            let index = open(temp_dir.path(), &conf).await;
            assert_eq!(super::run::run_files(&maps, "Shard-0").len(), 1);
            assert_eq!(index.run_count(), 1);
            assert!(index.lookup(key(1)).await.expect("a lookup").is_some());
            assert_eq!(index.lookup(key(2)).await.expect("a lookup"), None);
            index.close().await.expect("a close");
        });
    }

    /// A page that does not match its checksum is refused as the map's corruption
    #[test]
    fn a_torn_page_is_refused() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let conf = ArchiveMapConf::default();
            let archive = Uuid::new_v4();
            let maps = temp_dir.path().join("maps");
            {
                let index = open(temp_dir.path(), &conf).await;
                for at in 0..1000 {
                    index.apply(key(at), Change::Set(chain(key(at), archive, at)));
                }
                index.commit(MapState::default()).await.expect("a commit");
                index.close().await.expect("a close");
            }
            // a flipped byte in the first page's entries
            let (_, path) = super::run::run_files(&maps, "Shard-0").remove(0);
            let mut bytes = std::fs::read(&path).expect("the run");
            bytes[100] ^= 0x10;
            std::fs::write(&path, &bytes).expect("the run");
            // the lowest key is on the first page
            let lowest = (0..1000).map(key).min().expect("a key");
            let index = open(temp_dir.path(), &conf).await;
            match index.lookup(lowest).await {
                Err(ServerError::Shoal(ShoalError::MapCorruption { .. })) => (),
                other => panic!("a torn page read as {other:?}"),
            }
            index.close().await.expect("a close");
        });
    }

    /// What the index holds in memory is bounded by its settings, not by how many partitions it
    /// names, apart from a filter of about a byte and a quarter a key; and every key is found
    ///
    /// The whole point of paging the map: before F76 each of these 200,000 partitions held about
    /// fifty bytes of index in memory for as long as the shard ran.
    #[test]
    fn resident_bytes_stay_bounded_as_partitions_grow() {
        LocalExecutor::default().run(async {
            let temp_dir = test_dir();
            let conf = ArchiveMapConf::builder()
                .delta_entries(1024)
                .page_cache_bytes(64 << 10);
            let index = open(temp_dir.path(), &conf).await;
            let archive = Uuid::new_v4();
            let partitions = 200_000u64;
            for at in 0..partitions {
                index.apply(key(at), Change::Set(chain(key(at), archive, at)));
                if index.delta_len() >= 1024 {
                    index.commit(MapState::default()).await.expect("a commit");
                }
            }
            index.commit(MapState::default()).await.expect("a commit");
            // a delta of at most 1024, 64 KiB of pages, the directories, and the filters
            let resident = index.resident_bytes();
            let filters = partitions as usize * 10 / 8;
            assert!(
                resident < filters + (64 << 10) + 1024 * 128 + (256 << 10),
                "{resident} bytes resident for {partitions} partitions"
            );
            // well under what the in-memory index held for them
            assert!(resident * 10 < partitions as usize * 50, "{resident} bytes");
            // every key is found, a page read at most a few keys
            let keys: Vec<u64> = (0..partitions).map(key).collect();
            let found: HashMap<u64, ChainEntry> = index.lookup_many(&keys).await.expect("lookups");
            assert_eq!(found.len(), partitions as usize);
            // and keys never written are ruled out by the filters, so few pages are read
            let before = index.page_reads.get();
            for at in partitions..partitions + 10_000 {
                assert_eq!(index.lookup(key(at)).await.expect("a lookup"), None);
            }
            let reads = index.page_reads.get() - before;
            assert!(reads < 500, "10,000 absent keys read {reads} pages");
            index.close().await.expect("a close");
        });
    }
}
