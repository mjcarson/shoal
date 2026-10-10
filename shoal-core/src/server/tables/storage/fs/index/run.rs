//! A run of the paged archive map: an immutable file of index pages in key order
//!
//! A run is written once, by a flush of the delta or a merge of two runs, and never changed. What
//! a run keeps in memory is its directory - the first key of every page - its filter and its
//! archive table, which its footer holds; its pages are read when a lookup needs them
//! ([F76](../../../../../../../docs/src/features/paged-archive-map.md)).
//!
//! ```text
//! [page 0][page 1]...[page n-1][footer, padded to a page, its last 32 bytes the trailer]
//! trailer [footer offset u64][footer length u64][footer checksum u64][magic u64]
//! ```

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use gxhash::GxHasher;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::VecDeque;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tracing::{event, Level};
use uuid::Uuid;

use crate::server::errors::ShoalError;
use crate::server::ServerError;

use super::bloom::Bloom;
use super::page::{ArchiveTable, Change, Page, PageBuilder, PageView, PAGE_SIZE};

/// The magic a run's trailer ends with
const RUN_MAGIC: u64 = u64::from_le_bytes(*b"SHOALRUN");

/// The length of a run's trailer
const TRAILER_LEN: usize = 32;

/// How many pages a scan or a merge reads at once, 64 KiB
const SCAN_PAGES: u32 = 16;

/// The buffer a run is written through
const WRITE_BUFFER: usize = 128 << 10;

/// What a run's footer holds: everything about the run that is kept in memory
#[derive(Debug, Archive, Serialize, Deserialize)]
struct RunFooter {
    /// The first key of every page, in page order
    first_keys: Vec<u64>,
    /// The last key of the last page
    last_key: u64,
    /// The archives the run's entries name, by the run's own numbers
    archives: Vec<Uuid>,
    /// The filter of every key the run holds, removals included
    filter: Bloom,
    /// How many entries the run holds, removals included
    entries: u64,
    /// How many of them are removals
    removed: u64,
}

/// The path of a run's file
///
/// # Arguments
///
/// * `dir` - The directory a map's files are in
/// * `shard` - The shard the map is for
/// * `id` - The run's id
#[must_use]
pub fn run_path(dir: &Path, shard: &str, id: u64) -> PathBuf {
    dir.join(format!("{shard}.run-{id}"))
}

/// Every run file of a shard's map in a directory, with its id
///
/// # Arguments
///
/// * `dir` - The directory a map's files are in
/// * `shard` - The shard the map is for
#[must_use]
pub fn run_files(dir: &Path, shard: &str) -> Vec<(u64, PathBuf)> {
    // a run is named for its shard and its id, so another shard's run never matches
    let prefix = format!("{shard}.run-");
    let mut runs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name
                .strip_prefix(&prefix)
                .and_then(|id| id.parse::<u64>().ok())
            {
                runs.push((id, entry.path()));
            }
        }
    }
    runs
}

/// A run, open for reads
#[derive(Debug)]
pub struct Run {
    /// The run's id, which names its file and its pages in the cache
    pub id: u64,
    /// The run's file
    pub path: PathBuf,
    /// The handle every read of its pages goes through
    file: DmaFile,
    /// The first key of every page
    first_keys: Vec<u64>,
    /// The last key the run holds
    last_key: u64,
    /// The archives its entries name
    archives: Vec<Uuid>,
    /// The filter of every key it holds
    filter: Bloom,
    /// How many entries it holds, removals included
    pub entries: u64,
    /// How many of them are removals
    pub removed: u64,
}

impl Run {
    /// Open a run that was written whole, reading its footer
    ///
    /// # Arguments
    ///
    /// * `path` - The run's file
    /// * `id` - The run's id
    pub async fn open(path: PathBuf, id: u64) -> Result<Self, ServerError> {
        // the file, read only: a run never changes once it is written
        let file = OpenOptions::new().read(true).dma_open(&path).await?;
        // a run is whole pages, its trailer in the last 32 bytes of the last
        let size = file.file_size().await?;
        let page = PAGE_SIZE as u64;
        if size < page || size % page != 0 {
            file.close().await?;
            return Err(Self::corrupt(&path, "it is not whole pages"));
        }
        let last = file.read_at_aligned(size - page, PAGE_SIZE).await?;
        let trailer = &last[PAGE_SIZE - TRAILER_LEN..];
        let word = |at: usize| u64::from_le_bytes(trailer[at..at + 8].try_into().unwrap_or([0; 8]));
        let (offset, len, checksum, magic) = (word(0), word(8), word(16), word(24));
        if magic != RUN_MAGIC || offset + len > size {
            file.close().await?;
            return Err(Self::corrupt(&path, "its trailer is not a run's"));
        }
        // the footer, checked against its checksum before it is read as one
        // truncation cannot happen: a footer is bounded far below usize by the file it is in
        #[allow(clippy::cast_possible_truncation)]
        let read = file.read_at(offset, len as usize).await?;
        let mut hasher = GxHasher::default();
        hasher.write(&read);
        if hasher.finish() != checksum {
            file.close().await?;
            return Err(Self::corrupt(
                &path,
                "its footer does not match its checksum",
            ));
        }
        // copied to an aligned buffer, since rkyv reads it in place
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(read.len());
        aligned.extend_from_slice(&read);
        let archived = rkyv::access::<ArchivedRunFooter, rkyv::rancor::Error>(&aligned)?;
        let footer = rkyv::deserialize::<RunFooter, rkyv::rancor::Error>(archived)?;
        Ok(Run {
            id,
            path,
            file,
            first_keys: footer.first_keys,
            last_key: footer.last_key,
            archives: footer.archives,
            filter: footer.filter,
            entries: footer.entries,
            removed: footer.removed,
        })
    }

    /// The error a run that cannot be read is reported as, logged with what was wrong
    ///
    /// # Arguments
    ///
    /// * `path` - The run's file
    /// * `why` - What was wrong with it
    fn corrupt(path: &Path, why: &str) -> ServerError {
        // the error carries no path, so the event names the file
        event!(Level::ERROR, msg = "an archive map run cannot be read", path = %path.display(), why);
        ServerError::Shoal(ShoalError::MapCorruption {
            found: 0,
            expected: 0,
        })
    }

    /// How many pages the run holds
    #[must_use]
    pub fn pages(&self) -> u32 {
        u32::try_from(self.first_keys.len()).unwrap_or(u32::MAX)
    }

    /// The archives the run's entries name, by its own numbers
    #[must_use]
    pub fn archives(&self) -> &[Uuid] {
        &self.archives
    }

    /// The page that would hold a key, if the run's keys span it
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn page_for(&self, key: u64) -> Option<u32> {
        // a key outside the run's span is on none of its pages
        if self.first_keys.first().is_none_or(|first| key < *first) || key > self.last_key {
            return None;
        }
        // the last page whose first key is at or below it
        let at = self.first_keys.partition_point(|first| *first <= key);
        u32::try_from(at.saturating_sub(1)).ok()
    }

    /// Whether the run may hold a key: false means it certainly does not
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn may_contain(&self, key: u64) -> bool {
        self.filter.may_contain(key)
    }

    /// Read one page and check it
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    pub async fn read_page(&self, page: u32) -> Result<Page, ServerError> {
        // one aligned read of the page
        let read = self
            .file
            .read_at_aligned(u64::from(page) * PAGE_SIZE as u64, PAGE_SIZE)
            .await?;
        Page::new(&read).inspect_err(|_| self.report(page))
    }

    /// Say which page of which run failed its check
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    fn report(&self, page: u32) {
        event!(Level::ERROR, msg = "an archive map page failed its checksum", path = %self.path.display(), page);
    }

    /// The bytes the run holds in memory: its directory, its archive table and its filter
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.first_keys.capacity() * std::mem::size_of::<u64>()
            + self.archives.capacity() * std::mem::size_of::<Uuid>()
            + self.filter.bytes()
            + std::mem::size_of::<Self>()
    }

    /// Close the run's handle
    pub async fn close(self) -> Result<(), ServerError> {
        self.file.close().await?;
        Ok(())
    }
}

/// Writes a run, a page at a time, from entries handed to it in key order
pub struct RunWriter {
    /// The id the run is written under
    id: u64,
    /// Its file
    path: PathBuf,
    /// The stream its pages go through
    writer: DmaStreamWriter,
    /// The page being filled
    page: PageBuilder,
    /// The first key of every page written
    first_keys: Vec<u64>,
    /// The last key pushed
    last_key: Option<u64>,
    /// The archives the entries name
    archives: ArchiveTable,
    /// The filter of every key pushed
    filter: Bloom,
    /// How many entries were pushed
    entries: u64,
    /// How many of them were removals
    removed: u64,
}

impl RunWriter {
    /// Start a run's file
    ///
    /// # Arguments
    ///
    /// * `path` - The file, which must not exist
    /// * `id` - The run's id
    /// * `keys` - The most keys the run will hold, which sizes its filter
    /// * `bits_per_key` - The filter's bits a key
    pub async fn create(
        path: PathBuf,
        id: u64,
        keys: usize,
        bits_per_key: u32,
    ) -> Result<Self, ServerError> {
        // a run is written once: a file already there is somebody else's
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .dma_open(&path)
            .await?;
        let writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(WRITE_BUFFER)
            .build();
        Ok(RunWriter {
            id,
            path,
            writer,
            page: PageBuilder::default(),
            first_keys: Vec::new(),
            last_key: None,
            archives: ArchiveTable::default(),
            filter: Bloom::new(keys, bits_per_key),
            entries: 0,
            removed: 0,
        })
    }

    /// Add an entry, whose key is above every key pushed before it
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `change` - What the index holds for it
    pub async fn push(&mut self, key: u64, change: Change) -> Result<(), ServerError> {
        // a chain no page can hold is refused rather than written torn
        if !PageBuilder::fits_alone(&change) {
            return Err(ServerError::GlommioGeneric(format!(
                "partition {key:016x} has a chain longer than an archive map page holds; lower fragment_max_chain"
            )));
        }
        // a full page is written before this entry starts the next
        if !self.page.fits(&change) {
            self.write_page().await?;
        }
        // counted, filtered and added
        self.entries += 1;
        if change == Change::Removed {
            self.removed += 1;
        }
        self.filter.insert(key);
        self.last_key = Some(key);
        self.page.push(key, change);
        Ok(())
    }

    /// Write the page being filled
    async fn write_page(&mut self) -> Result<(), ServerError> {
        // nothing pushed is nothing to write
        let Some(first) = self.page.first_key() else {
            return Ok(());
        };
        self.first_keys.push(first);
        let bytes = self.page.encode(&mut self.archives);
        self.writer.write_all(&bytes).await?;
        Ok(())
    }

    /// Write the last page and the footer, make the file durable, and open it as a run
    ///
    /// A run nothing was pushed to is no run: its file is removed and nothing is returned.
    pub async fn finish(mut self) -> Result<Option<Run>, ServerError> {
        // an empty run is not kept
        let Some(last_key) = self.last_key else {
            self.writer.close().await?;
            glommio::io::remove(&self.path).await?;
            return Ok(None);
        };
        self.write_page().await?;
        // the footer after the last page, padded so the trailer ends the last page of the file
        let offset = self.first_keys.len() as u64 * PAGE_SIZE as u64;
        let footer = RunFooter {
            first_keys: std::mem::take(&mut self.first_keys),
            last_key,
            archives: std::mem::take(&mut self.archives.ids),
            filter: std::mem::take(&mut self.filter),
            entries: self.entries,
            removed: self.removed,
        };
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&footer)?;
        let mut hasher = GxHasher::default();
        hasher.write(&archived);
        let region = (archived.len() + TRAILER_LEN).div_ceil(PAGE_SIZE) * PAGE_SIZE;
        let mut tail = vec![0u8; region];
        tail[..archived.len()].copy_from_slice(&archived);
        let trailer = &mut tail[region - TRAILER_LEN..];
        trailer[0..8].copy_from_slice(&offset.to_le_bytes());
        trailer[8..16].copy_from_slice(&(archived.len() as u64).to_le_bytes());
        trailer[16..24].copy_from_slice(&hasher.finish().to_le_bytes());
        trailer[24..32].copy_from_slice(&RUN_MAGIC.to_le_bytes());
        self.writer.write_all(&tail).await?;
        // durable before any manifest names it
        self.writer.sync().await?;
        self.writer.close().await?;
        // and opened for reads, its footer read back as any open reads it
        Ok(Some(Run::open(self.path, self.id).await?))
    }
}

/// Reads a run's entries in key order, within some key ranges
///
/// A scan or a merge reads a run this way, a few pages at a time and never through the cache, so
/// a pass over a whole run does not evict what point lookups have cached.
pub struct RunCursor {
    /// The run
    run: Rc<Run>,
    /// The ranges still to read, each inclusive, in key order
    ranges: VecDeque<(u64, u64)>,
    /// The range being read
    current: Option<(u64, u64)>,
    /// The next page to read of the current range
    next_page: u32,
    /// One past the last page of the current range
    end_page: u32,
    /// Entries read and not yet handed out
    buffer: VecDeque<(u64, Change)>,
}

impl RunCursor {
    /// A cursor over some ranges of a run
    ///
    /// # Arguments
    ///
    /// * `run` - The run
    /// * `ranges` - The key ranges, each inclusive, in key order and disjoint
    #[must_use]
    pub fn new(run: Rc<Run>, ranges: &[(u64, u64)]) -> Self {
        RunCursor {
            run,
            ranges: ranges.iter().copied().collect(),
            current: None,
            next_page: 0,
            end_page: 0,
            buffer: VecDeque::new(),
        }
    }

    /// Move to the next range that has pages, or report there is none
    fn next_range(&mut self) -> bool {
        while let Some((low, high)) = self.ranges.pop_front() {
            // a range wholly outside the run's span reads nothing
            let Some(first) = self.run.first_keys.first() else {
                return false;
            };
            if high < *first || low > self.run.last_key {
                continue;
            }
            // from the page holding its low end, or the first page, to the one holding its high
            let start = self.run.page_for(low).unwrap_or(0);
            let end = self.run.page_for(high).unwrap_or(self.run.pages() - 1) + 1;
            self.current = Some((low, high));
            self.next_page = start;
            self.end_page = end;
            return true;
        }
        false
    }

    /// The next entry in key order, or none once every range is read
    pub async fn next(&mut self) -> Result<Option<(u64, Change)>, ServerError> {
        loop {
            // an entry already read is handed out first
            if let Some(entry) = self.buffer.pop_front() {
                return Ok(Some(entry));
            }
            // a range read to its end moves on to the next
            if self.current.is_none() || self.next_page >= self.end_page {
                if !self.next_range() {
                    return Ok(None);
                }
                continue;
            }
            // a few pages at once, in one read
            let count = (self.end_page - self.next_page).min(SCAN_PAGES);
            let read = self
                .run
                .file
                .read_at_aligned(
                    u64::from(self.next_page) * PAGE_SIZE as u64,
                    count as usize * PAGE_SIZE,
                )
                .await?;
            let (low, high) = self.current.unwrap_or((0, u64::MAX));
            for at in 0..count as usize {
                // every page checked, and its entries inside the range kept
                let bytes = read
                    .get(at * PAGE_SIZE..(at + 1) * PAGE_SIZE)
                    .unwrap_or(&[]);
                let view = PageView::parse(bytes)
                    .inspect_err(|_| self.run.report(self.next_page + at as u32))?;
                let start = view.lower_bound(low);
                for index in start..view.len() {
                    let key = view.key(index);
                    if key > high {
                        break;
                    }
                    self.buffer
                        .push_back((key, view.change(index, &self.run.archives)));
                }
            }
            self.next_page += count;
        }
    }
}
