//! A table's rows as a benchmark uses them: keys to read and rows to insert
//!
//! Each table's file is scanned once, typed, through the row type its table derive handed back.
//! The scan counts and digests the file and keeps the read key of every row, and the file is then
//! split in file order: the first part is the **preload**, inserted before anything is measured
//! and the only rows a read asks for, so every read is of a row that exists; the rest is the
//! **insert pool**, streamed from the file again by a thread of its own whenever an arm inserts,
//! so a dataset larger than memory is never held.
//!
//! Everything a worker needs from a table is behind [`TableSource`], which erases the row type:
//! a worker holds one per table and builds queries of the database's kind without knowing what
//! any row is.

use serde::{Deserialize, Serialize};
use shoal::shared::dataset::{DatasetError, DatasetRow, DatasetSupport, DatasetVisitor};
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};

use crate::dataset::{DatasetFile, Format};
use crate::read::{for_each_row, RowError};

/// How many parsed rows a feed keeps ahead of the workers
const FEED_AHEAD: usize = 4096;

/// How many of a table's bad rows are kept to show
const KEPT_ERRORS: usize = 20;

/// How much of each table's file is loaded before anything is measured
///
/// Written `1000` for a count of rows or `"50%"` for a share of the file.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "PreloadRepr", into = "PreloadRepr")]
pub enum Preload {
    /// The first this many rows
    Rows(u64),
    /// The first this percent of the rows
    Percent(f64),
}

impl Default for Preload {
    /// Half of each file, so a mixed arm has as much to insert as it has to read
    fn default() -> Self {
        Preload::Percent(50.0)
    }
}

impl std::str::FromStr for Preload {
    type Err = String;

    /// Parse `1000` as rows or `50%` as a share of the file
    ///
    /// # Arguments
    ///
    /// * `raw` - The value as written
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // a trailing percent sign makes it a share
        let raw = raw.trim();
        if let Some(percent) = raw.strip_suffix('%') {
            let percent: f64 = percent
                .trim()
                .parse()
                .map_err(|error| format!("{raw:?} is not a percent: {error}"))?;
            if !(0.0..=100.0).contains(&percent) {
                return Err(format!("{raw:?} is not between 0% and 100%"));
            }
            return Ok(Preload::Percent(percent));
        }
        raw.parse()
            .map(Preload::Rows)
            .map_err(|error| format!("{raw:?} is neither a row count nor a percent: {error}"))
    }
}

/// How a preload is written: a bare count of rows, or text for a share
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum PreloadRepr {
    /// A count of rows
    Rows(u64),
    /// A count or a share, as text
    Text(String),
}

impl TryFrom<PreloadRepr> for Preload {
    type Error = String;

    /// Read a preload as it was written
    ///
    /// # Arguments
    ///
    /// * `repr` - The preload as written
    fn try_from(repr: PreloadRepr) -> Result<Self, Self::Error> {
        // a number is rows, text is parsed
        match repr {
            PreloadRepr::Rows(rows) => Ok(Preload::Rows(rows)),
            PreloadRepr::Text(text) => text.parse(),
        }
    }
}

impl From<Preload> for PreloadRepr {
    /// Write a preload back the way it is read
    ///
    /// # Arguments
    ///
    /// * `preload` - The preload to write
    fn from(preload: Preload) -> Self {
        // rows stay a number, a share is text with its sign
        match preload {
            Preload::Rows(rows) => PreloadRepr::Rows(rows),
            Preload::Percent(percent) => PreloadRepr::Text(format!("{percent}%")),
        }
    }
}

impl Preload {
    /// How many of a file's rows this preloads
    ///
    /// # Arguments
    ///
    /// * `rows` - How many rows the file parsed to
    #[must_use]
    pub fn rows(&self, rows: u64) -> u64 {
        // never more rows than there are
        match self {
            Preload::Rows(count) => (*count).min(rows),
            Preload::Percent(percent) => ((rows as f64) * percent / 100.0).floor() as u64,
        }
    }
}

/// How a table's file is scanned and split
#[derive(Debug, Clone, PartialEq)]
pub struct ScanOptions {
    /// How much of the file is loaded before anything is measured
    pub preload: Preload,
    /// Whether an insert skips a row whose key an earlier row already had
    pub dedupe: bool,
    /// The share of a file's rows that may fail to parse before it is refused, in percent
    pub max_parse_errors: f64,
}

impl Default for ScanOptions {
    /// Half preloaded, duplicates inserted as overwrites, one percent of bad rows tolerated
    fn default() -> Self {
        ScanOptions {
            preload: Preload::default(),
            dedupe: false,
            max_parse_errors: 1.0,
        }
    }
}

/// What scanning one table's file found
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableScan {
    /// The table
    pub table: String,
    /// The file
    pub path: PathBuf,
    /// Its format
    pub format: Format,
    /// Whether the table is sorted
    pub sorted: bool,
    /// How many rows parsed
    pub rows: u64,
    /// How many rows did not parse, and are skipped the same way every time
    pub parse_errors: u64,
    /// The first few rows that did not parse, with where they were
    pub first_errors: Vec<String>,
    /// How many distinct read keys the rows have
    pub distinct_keys: u64,
    /// How many rows have a key an earlier row already had
    pub duplicate_rows: u64,
    /// How many bytes the file holds
    pub bytes: u64,
    /// The sha256 of the file
    pub sha256: String,
    /// How many rows are preloaded
    pub preload_rows: u64,
    /// How many distinct keys the preloaded rows have, which is what reads choose from
    pub read_keys: u64,
    /// How many rows an arm can insert before the pool is exhausted
    pub insert_rows: u64,
}

impl TableScan {
    /// The mean size of a row in the file, as a stand in for its size on the wire
    #[must_use]
    pub fn mean_row_bytes(&self) -> u64 {
        // the file's bytes over its rows, never dividing by zero
        self.bytes / self.rows.max(1)
    }
}

/// Why a table's file could not be used
#[derive(Debug)]
pub enum ScanError {
    /// The table could not be found or did not opt in
    Dataset(DatasetError),
    /// The file could not be read
    Io {
        /// The file
        path: PathBuf,
        /// Why
        error: std::io::Error,
    },
    /// Too many of its rows did not parse to trust the rest
    TooManyParseErrors {
        /// The table
        table: String,
        /// How many rows did not parse
        errors: u64,
        /// How many did
        rows: u64,
        /// The first few that did not
        first: Vec<String>,
    },
    /// No row parsed at all
    NoRows {
        /// The table
        table: String,
        /// The first few rows that did not parse, if any
        first: Vec<String>,
    },
}

impl std::fmt::Display for ScanError {
    /// Say which table and why
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // each names the table or file it is about
        match self {
            ScanError::Dataset(error) => write!(f, "{error}"),
            ScanError::Io { path, error } => write!(f, "{} cannot be read: {error}", path.display()),
            ScanError::TooManyParseErrors {
                table,
                errors,
                rows,
                first,
            } => write!(
                f,
                "{table}: {errors} rows did not parse beside {rows} that did, more than \
                 --max-parse-errors allows; the first: {}",
                first.join("; ")
            ),
            ScanError::NoRows { table, first } => {
                write!(f, "{table}: no row parsed")?;
                if !first.is_empty() {
                    write!(f, "; the first failures: {}", first.join("; "))?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ScanError {}

/// Which part of a file a feed streams
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRange {
    /// The preloaded rows, inserted before anything is measured
    Preload,
    /// The insert pool, which a measured arm inserts from
    Inserts,
}

/// What an insert pulled from a feed got
pub enum Take<K> {
    /// A row's insert, and the sequence it was taken at, which acknowledges it later
    Row {
        /// The insert
        query: K,
        /// The order it was taken in, within this feed
        seq: u64,
    },
    /// The feed is still reading and has nothing parsed yet
    Stalled,
    /// The feed's range is spent
    Exhausted,
}

/// What a feed did over an arm
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedFacts {
    /// How many inserts were taken from it
    pub taken: u64,
    /// How many of those the cluster acknowledged
    pub acked: u64,
    /// The insert at which a wrapping feed started over, so later inserts overwrite
    pub wrapped_at: Option<u64>,
    /// Why the feed's reader stopped early, if it did
    pub error: Option<String>,
}

/// Everything a worker needs from one table, with its row type erased
pub trait TableSource<K>: Send + Sync {
    /// What the scan of the table's file found
    fn scan(&self) -> &TableScan;

    /// Build one get of the read keys at these places in the pool
    ///
    /// # Arguments
    ///
    /// * `indices` - Places in `0..scan().read_keys`
    fn read_query(&self, indices: &[u64]) -> K;

    /// Start streaming a range of the file, replacing any feed already open
    ///
    /// # Arguments
    ///
    /// * `range` - Which part of the file to stream
    /// * `wrap` - Whether to start over from the range's beginning when it is spent
    fn open_feed(&self, range: FeedRange, wrap: bool);

    /// Stop the open feed's reader, keeping what it did
    fn close_feed(&self);

    /// Take the next insert from the open feed
    fn take_insert(&self) -> Take<K>;

    /// Record that the cluster acknowledged an insert
    ///
    /// # Arguments
    ///
    /// * `seq` - The sequence it was taken at
    fn ack(&self, seq: u64);

    /// What the open or last feed did
    fn feed_facts(&self) -> FeedFacts;

    /// One read of each acknowledged insert's row, to show none was lost
    fn verify_queries(&self) -> Vec<K>;
}

/// A feed's shared state, between its reader thread and the workers
struct FeedState<R> {
    /// Rows parsed and not yet taken
    rows: VecDeque<R>,
    /// Whether the reader reached the end of its range and will not start over
    done: bool,
    /// Whether the reader was told to stop
    stop: bool,
    /// The insert at which a wrapping feed started over
    wrapped_at: Option<u64>,
    /// How many rows the reader has handed over in total
    produced: u64,
    /// Why the reader stopped early, if it did
    error: Option<String>,
}

/// A range of a file streamed by a reader thread into a bounded queue
struct Feed<R> {
    /// The queue and its flags
    state: Mutex<FeedState<R>>,
    /// Signalled when the queue has room or the feed is told to stop
    room: Condvar,
}

/// One table's typed rows, behind the erased [`TableSource`]
struct Typed<R: DatasetRow<K>, K> {
    /// What the scan found
    scan: TableScan,
    /// The distinct read keys of the preloaded rows, in file order
    keys: Vec<R::ReadKey>,
    /// Whether each parsed row is the first with its key
    first: Arc<Vec<bool>>,
    /// Whether an insert skips a row whose key an earlier row had
    dedupe: bool,
    /// The open feed, if any
    feed: Mutex<Option<Arc<Feed<R>>>>,
    /// The read key of every insert taken from the open feed, by sequence
    taken: Mutex<Vec<R::ReadKey>>,
    /// The sequences of the inserts the cluster acknowledged
    acked: Mutex<Vec<u64>>,
    /// The query kinds this table's queries are built as
    kind: PhantomData<fn() -> K>,
}

/// Hash a read key, for counting distinct keys
///
/// # Arguments
///
/// * `key` - The key to hash
fn key_hash<T: Hash>(key: &T) -> u64 {
    // the hasher partition keys are hashed with, though any would do for counting
    let mut hasher = shoal::gxhash::GxHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Lock a mutex, taking over its value if a thread panicked holding it
///
/// # Arguments
///
/// * `mutex` - The mutex to lock
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // a worker that panicked has nothing to leave half done in here
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Scan a table's file and keep its read keys, through the row type its table handed back
struct Prepare<'a> {
    /// The file
    file: &'a DatasetFile,
    /// How it is scanned and split
    options: &'a ScanOptions,
}

impl<K: Send + Sync + 'static> DatasetVisitor<K> for Prepare<'_> {
    /// The table, ready for a worker
    type Output = Result<Arc<dyn TableSource<K>>, ScanError>;

    /// Scan the file as rows of this type
    ///
    /// # Arguments
    ///
    /// * `table` - The table the rows are for
    fn visit<R: DatasetRow<K>>(self, table: &'static str) -> Self::Output {
        // every parsed row's key, and whether it is the first with that key
        let mut keys: Vec<R::ReadKey> = Vec::new();
        let mut first: Vec<bool> = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        let mut errors = 0u64;
        let mut first_errors: Vec<RowError> = Vec::new();
        let facts = for_each_row::<R, _>(&self.file.path, self.file.format, |row| {
            match row {
                Ok(row) => {
                    let key = row.read_key();
                    first.push(seen.insert(key_hash(&key)));
                    keys.push(key);
                }
                Err(error) => {
                    errors += 1;
                    if first_errors.len() < KEPT_ERRORS {
                        first_errors.push(error);
                    }
                }
            }
            ControlFlow::Continue(())
        })
        .map_err(|error| ScanError::Io {
            path: self.file.path.clone(),
            error,
        })?
        .expect("a scan reads the whole file");
        let rows = keys.len() as u64;
        let shown: Vec<String> = first_errors.iter().map(ToString::to_string).collect();
        // a file that is mostly bad rows is a file read as the wrong table
        if rows == 0 {
            return Err(ScanError::NoRows {
                table: table.to_string(),
                first: shown,
            });
        }
        if errors as f64 * 100.0 > self.options.max_parse_errors * (rows + errors) as f64 {
            return Err(ScanError::TooManyParseErrors {
                table: table.to_string(),
                errors,
                rows,
                first: shown,
            });
        }
        // split the file: reads choose from the distinct keys of the preload
        let preload_rows = self.options.preload.rows(rows);
        let preload = preload_rows as usize;
        let pool: Vec<R::ReadKey> = keys[..preload]
            .iter()
            .zip(&first[..preload])
            .filter(|(_, first)| **first)
            .map(|(key, _)| key.clone())
            .collect();
        // and inserts take the rest, less any duplicates being skipped
        let insert_rows = if self.options.dedupe {
            first[preload..].iter().filter(|first| **first).count() as u64
        } else {
            rows - preload_rows
        };
        let distinct_keys = seen.len() as u64;
        let scan = TableScan {
            table: table.to_string(),
            path: self.file.path.clone(),
            format: self.file.format,
            sorted: R::SORTED,
            rows,
            parse_errors: errors,
            first_errors: shown,
            distinct_keys,
            duplicate_rows: rows - distinct_keys,
            bytes: facts.bytes,
            sha256: facts.sha256,
            preload_rows,
            read_keys: pool.len() as u64,
            insert_rows,
        };
        Ok(Arc::new(Typed::<R, K> {
            scan,
            keys: pool,
            first: Arc::new(first),
            dedupe: self.options.dedupe,
            feed: Mutex::new(None),
            taken: Mutex::new(Vec::new()),
            acked: Mutex::new(Vec::new()),
            kind: PhantomData,
        }))
    }
}

/// Scan one table's file, through the row type the database has for it
///
/// # Arguments
///
/// * `file` - The table's file
/// * `options` - How it is scanned and split
///
/// # Errors
///
/// When the table cannot be loaded from a dataset, its file cannot be read, or too many of its
/// rows did not parse.
pub fn prepare<S>(
    file: &DatasetFile,
    options: &ScanOptions,
) -> Result<Arc<dyn TableSource<S::QueryKinds>>, ScanError>
where
    S: DatasetSupport,
    S::QueryKinds: Send + Sync + Clone + 'static,
{
    // find the table's row type and scan the file as it
    S::visit_table(file.table, Prepare { file, options }).map_err(ScanError::Dataset)?
}

impl<R: DatasetRow<K>, K: Send + Sync + 'static> Typed<R, K> {
    /// Stream one range of the file into a feed until it is spent or told to stop
    ///
    /// # Arguments
    ///
    /// * `feed` - The feed to fill
    /// * `path` - The file
    /// * `format` - Its format
    /// * `first` - Whether each parsed row is the first with its key
    /// * `range` - The rows to stream, by parsed index
    /// * `dedupe` - Whether to skip a row whose key an earlier row had
    /// * `wrap` - Whether to start over when the range is spent
    #[allow(clippy::too_many_arguments)]
    fn read_range(
        feed: &Feed<R>,
        path: &std::path::Path,
        format: Format,
        first: &[bool],
        range: std::ops::Range<u64>,
        dedupe: bool,
        wrap: bool,
    ) {
        loop {
            // walk the file, counting only rows that parse, as the scan did
            let mut index = 0u64;
            let mut handed = 0u64;
            let walked = for_each_row::<R, _>(path, format, |row| {
                let Ok(row) = row else {
                    return ControlFlow::Continue(());
                };
                let at = index;
                index += 1;
                // rows before the range are parsed and dropped; past it the walk is done
                if at < range.start {
                    return ControlFlow::Continue(());
                }
                if at >= range.end {
                    return ControlFlow::Break(());
                }
                if dedupe && !first[at as usize] {
                    return ControlFlow::Continue(());
                }
                // wait for room, unless the feed was told to stop
                let mut state = lock(&feed.state);
                while state.rows.len() >= FEED_AHEAD && !state.stop {
                    state = feed
                        .room
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                if state.stop {
                    return ControlFlow::Break(());
                }
                state.rows.push_back(row);
                state.produced += 1;
                handed += 1;
                ControlFlow::Continue(())
            });
            let mut state = lock(&feed.state);
            // a file that cannot be read again ends the feed with why
            if let Err(error) = walked {
                state.error = Some(error.to_string());
                state.done = true;
                return;
            }
            // a stopped feed, a spent one, and one with nothing to start over on are all done
            if state.stop || !wrap || handed == 0 {
                state.done = true;
                return;
            }
            // a wrapping feed starts over, and every insert after this point is an overwrite
            if state.wrapped_at.is_none() {
                state.wrapped_at = Some(state.produced);
            }
        }
    }
}

impl<R: DatasetRow<K>, K: Send + Sync + 'static> TableSource<K> for Typed<R, K> {
    /// What the scan found
    fn scan(&self) -> &TableScan {
        &self.scan
    }

    /// One get of the keys at these places in the pool
    ///
    /// # Arguments
    ///
    /// * `indices` - Places in the pool
    fn read_query(&self, indices: &[u64]) -> K {
        // gather the keys, then build one get of all of them
        let keys: Vec<R::ReadKey> = indices
            .iter()
            .map(|index| self.keys[*index as usize].clone())
            .collect();
        R::read_query(&keys)
    }

    /// Start a reader thread over a range of the file
    ///
    /// # Arguments
    ///
    /// * `range` - Which part of the file
    /// * `wrap` - Whether to start over when it is spent
    fn open_feed(&self, range: FeedRange, wrap: bool) {
        // one feed at a time, and a new one forgets what the last one took
        self.close_feed();
        lock(&self.taken).clear();
        lock(&self.acked).clear();
        let feed = Arc::new(Feed {
            state: Mutex::new(FeedState {
                rows: VecDeque::with_capacity(FEED_AHEAD),
                done: false,
                stop: false,
                wrapped_at: None,
                produced: 0,
                error: None,
            }),
            room: Condvar::new(),
        });
        // the rows the range covers, by parsed index
        let rows = match range {
            FeedRange::Preload => 0..self.scan.preload_rows,
            FeedRange::Inserts => self.scan.preload_rows..self.scan.rows,
        };
        // the preload is loaded once and never wraps
        let wrap = wrap && range == FeedRange::Inserts;
        let reader = feed.clone();
        let path = self.scan.path.clone();
        let format = self.scan.format;
        let first = self.first.clone();
        let dedupe = self.dedupe;
        let name = format!("feed-{}", self.scan.table);
        std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                Self::read_range(&reader, &path, format, &first, rows, dedupe, wrap);
                // wake anyone waiting on a feed that will never produce again
                reader.room.notify_all();
            })
            .expect("a feed's reader thread starts");
        *lock(&self.feed) = Some(feed);
    }

    /// Tell the open feed's reader to stop
    fn close_feed(&self) {
        // the feed stays, so what it did can still be read
        if let Some(feed) = lock(&self.feed).as_ref() {
            lock(&feed.state).stop = true;
            feed.room.notify_all();
        }
    }

    /// Take the next parsed row as an insert
    fn take_insert(&self) -> Take<K> {
        // no feed open is a feed with nothing in it
        let Some(feed) = lock(&self.feed).clone() else {
            return Take::Exhausted;
        };
        let row = {
            let mut state = lock(&feed.state);
            match state.rows.pop_front() {
                Some(row) => row,
                None if state.done => return Take::Exhausted,
                None => return Take::Stalled,
            }
        };
        // there is room for the reader again
        feed.room.notify_one();
        // remember the row's key, so an acknowledged insert can be read back
        let mut taken = lock(&self.taken);
        let seq = taken.len() as u64;
        taken.push(row.read_key());
        drop(taken);
        Take::Row {
            query: row.insert_query(),
            seq,
        }
    }

    /// Record an acknowledged insert
    ///
    /// # Arguments
    ///
    /// * `seq` - The sequence it was taken at
    fn ack(&self, seq: u64) {
        lock(&self.acked).push(seq);
    }

    /// What the feed did
    fn feed_facts(&self) -> FeedFacts {
        // the counts are the table's, the flags the feed's
        let taken = lock(&self.taken).len() as u64;
        let acked = lock(&self.acked).len() as u64;
        let (wrapped_at, error) = match lock(&self.feed).as_ref() {
            Some(feed) => {
                let state = lock(&feed.state);
                (state.wrapped_at, state.error.clone())
            }
            None => (None, None),
        };
        FeedFacts {
            taken,
            acked,
            wrapped_at,
            error,
        }
    }

    /// One read a distinct acknowledged row
    fn verify_queries(&self) -> Vec<K> {
        // a key acknowledged twice, by a wrapping feed or a duplicate row, is read once
        let taken = lock(&self.taken);
        let acked = lock(&self.acked);
        let mut seen = HashSet::new();
        acked
            .iter()
            .map(|seq| &taken[*seq as usize])
            .filter(|key| seen.insert(key_hash(*key)))
            .map(|key| R::read_query(std::slice::from_ref(key)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{prepare, FeedRange, Preload, ScanError, ScanOptions, Take, TableSource};
    use crate::dataset::Dataset;
    use crate::testing::{CatalogClient, CatalogQueryKinds};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Write a dataset of `Item.csv` with these ids, one row each
    ///
    /// # Arguments
    ///
    /// * `ids` - The ids, in file order
    /// * `bad` - How many unparseable rows to put after the first
    fn items(ids: &[u64], bad: usize) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let mut body = String::from("id,name\n");
        for (at, id) in ids.iter().enumerate() {
            body.push_str(&format!("{id},item {id}\n"));
            if at == 0 {
                for _ in 0..bad {
                    body.push_str("not a number,x\n");
                }
            }
        }
        std::fs::write(dir.path().join("Item.csv"), body).unwrap();
        dir
    }

    /// Scan the only file in a folder
    ///
    /// # Arguments
    ///
    /// * `dir` - The folder
    /// * `options` - How to scan it
    fn scan(
        dir: &tempfile::TempDir,
        options: &ScanOptions,
    ) -> Result<Arc<dyn TableSource<CatalogQueryKinds>>, ScanError> {
        let dataset = Dataset::open::<CatalogClient>(dir.path()).unwrap();
        prepare::<CatalogClient>(&dataset.files[0], options)
    }

    /// Take every insert a feed has, waiting out stalls
    ///
    /// # Arguments
    ///
    /// * `table` - The table whose feed is open
    fn drain(table: &dyn TableSource<CatalogQueryKinds>) -> Vec<String> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut taken = Vec::new();
        loop {
            match table.take_insert() {
                Take::Row { query, seq } => {
                    assert_eq!(seq, taken.len() as u64);
                    taken.push(format!("{query:?}"));
                }
                Take::Stalled => {
                    assert!(Instant::now() < deadline, "the feed never finished");
                    std::thread::sleep(Duration::from_millis(1));
                }
                Take::Exhausted => return taken,
            }
        }
    }

    /// The preload is the head of the file, and inserts are the rest of it
    #[test]
    fn a_file_is_split_into_preload_and_inserts() {
        let dir = items(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0);
        let options = ScanOptions {
            preload: Preload::Percent(30.0),
            ..ScanOptions::default()
        };
        let table = scan(&dir, &options).unwrap();
        let facts = table.scan();
        assert_eq!((facts.rows, facts.preload_rows, facts.read_keys, facts.insert_rows), (10, 3, 3, 7));
        assert_eq!(facts.sha256.len(), 64);
        // the preload feed is the first three rows and the insert feed the other seven
        table.open_feed(FeedRange::Preload, false);
        assert_eq!(drain(table.as_ref()).len(), 3);
        table.open_feed(FeedRange::Inserts, false);
        let inserts = drain(table.as_ref());
        assert_eq!(inserts.len(), 7);
        assert!(inserts[0].contains("id: 4"), "{}", inserts[0]);
    }

    /// A duplicate key is counted, and skipped by an insert only when asked
    #[test]
    fn duplicates_are_counted_and_skipped_on_request() {
        // 2 repeats in the preload, 1 repeats into the insert pool, and 5 repeats in the pool
        let dir = items(&[1, 2, 2, 3, 1, 5, 5, 6], 0);
        let preload = ScanOptions {
            preload: Preload::Rows(4),
            ..ScanOptions::default()
        };
        let table = scan(&dir, &preload).unwrap();
        assert_eq!(table.scan().distinct_keys, 5);
        assert_eq!(table.scan().duplicate_rows, 3);
        // reads choose from the distinct preloaded keys
        assert_eq!(table.scan().read_keys, 3);
        assert_eq!(table.scan().insert_rows, 4);
        // and with dedupe only the first 5 and the 6 are new
        let dedupe = ScanOptions {
            dedupe: true,
            ..preload
        };
        let table = scan(&dir, &dedupe).unwrap();
        assert_eq!(table.scan().insert_rows, 2);
        table.open_feed(FeedRange::Inserts, false);
        assert_eq!(drain(table.as_ref()).len(), 2);
    }

    /// A wrapping feed starts over and says where
    #[test]
    fn a_wrapping_feed_starts_over() {
        let dir = items(&[1, 2, 3, 4], 0);
        let options = ScanOptions {
            preload: Preload::Rows(2),
            ..ScanOptions::default()
        };
        let table = scan(&dir, &options).unwrap();
        table.open_feed(FeedRange::Inserts, true);
        // take five of a two row pool
        let mut seqs = Vec::new();
        while seqs.len() < 5 {
            if let Take::Row { seq, .. } = table.take_insert() {
                seqs.push(seq);
            }
        }
        table.close_feed();
        assert_eq!(table.feed_facts().wrapped_at, Some(2));
        assert_eq!(table.feed_facts().taken, 5);
    }

    /// Acknowledged inserts are read back once each
    #[test]
    fn acknowledged_inserts_are_read_back_once_each() {
        let dir = items(&[1, 2, 3, 4, 5], 0);
        let options = ScanOptions {
            preload: Preload::Rows(1),
            ..ScanOptions::default()
        };
        let table = scan(&dir, &options).unwrap();
        table.open_feed(FeedRange::Inserts, false);
        let taken = drain(table.as_ref());
        assert_eq!(taken.len(), 4);
        // two of the four were acknowledged
        table.ack(0);
        table.ack(2);
        let reads: Vec<String> = table
            .verify_queries()
            .iter()
            .map(|query| format!("{query:?}"))
            .collect();
        // the pool starts after the one preloaded row, so sequences 0 and 2 are ids 2 and 4
        let expected: Vec<String> = [2u64, 4]
            .into_iter()
            .map(|id| {
                let get: CatalogQueryKinds = crate::testing::ItemGet::new(vec![id]).into();
                format!("{get:?}")
            })
            .collect();
        assert_eq!(reads, expected);
        assert_eq!(table.feed_facts().acked, 2);
    }

    /// A few bad rows are skipped and kept to show; too many refuse the file
    #[test]
    fn bad_rows_are_skipped_up_to_a_cap() {
        // one bad row in 101 is under one percent
        let ids: Vec<u64> = (0..100).collect();
        let dir = items(&ids, 1);
        let table = scan(&dir, &ScanOptions::default()).unwrap();
        assert_eq!(table.scan().parse_errors, 1);
        assert_eq!(table.scan().first_errors.len(), 1);
        // ten in 110 is not
        let dir = items(&ids, 10);
        assert!(matches!(
            scan(&dir, &ScanOptions::default()),
            Err(ScanError::TooManyParseErrors { errors: 10, .. })
        ));
    }

    /// A read names the keys at the places it was given
    #[test]
    fn a_read_names_the_keys_it_was_given() {
        let dir = items(&[10, 20, 30, 40], 0);
        let options = ScanOptions {
            preload: Preload::Rows(4),
            ..ScanOptions::default()
        };
        let table = scan(&dir, &options).unwrap();
        let read = format!("{:?}", table.read_query(&[2, 0]));
        let expected: CatalogQueryKinds = crate::testing::ItemGet::new(vec![30, 10]).into();
        assert_eq!(read, format!("{expected:?}"));
    }

    /// A share or a count of rows parses, and nonsense does not
    #[test]
    fn a_preload_parses_as_a_count_or_a_share() {
        assert_eq!("1000".parse::<Preload>(), Ok(Preload::Rows(1000)));
        assert_eq!("50%".parse::<Preload>(), Ok(Preload::Percent(50.0)));
        assert!("150%".parse::<Preload>().is_err());
        assert!("half".parse::<Preload>().is_err());
        assert_eq!(Preload::Percent(50.0).rows(7), 3);
        assert_eq!(Preload::Rows(10).rows(7), 7);
    }
}
