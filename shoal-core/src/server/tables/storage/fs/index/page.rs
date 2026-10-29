//! A page of a run: partitions' entries in key order, in four kibibytes
//!
//! A page is a header, then a fixed size slot for every entry in key order, then the fragments
//! of the entries that have any. Every slot is the same length, so a lookup is a binary search of
//! the page as it was read, and nothing is decoded but the entry found
//! ([F76](../../../../../../../docs/src/features/paged-archive-map.md)).
//!
//! ```text
//! header   [checksum u64][entries u16][fragments u16][reserved u32]
//! entry    [key u64][offset u64][archive u32][size u32][first fragment u16][fragments u8][flags u8]
//! fragment [offset u64][archive u32][size u32]
//! ```

use gxhash::GxHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use uuid::Uuid;

use crate::server::errors::ShoalError;
use crate::server::ServerError;

use super::super::map::{ArchiveEntry, ChainEntry};

/// The size of one page of a run, which is one direct read
pub const PAGE_SIZE: usize = 4096;

/// The length of a page's header: its checksum, its entry and fragment counts, and a reserved word
const HEADER_LEN: usize = 16;

/// The length of one entry's slot
const ENTRY_LEN: usize = 28;

/// The length of one fragment
const FRAGMENT_LEN: usize = 16;

/// The flag an entry carries when the partition it names was removed
const REMOVED: u8 = 1;

/// What the index holds for a partition: where its records are, or that it was removed
///
/// A removal is held as itself until a merge reaches the oldest run, so a newer run or the delta
/// can shadow an entry an older run still holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// The partition's base record and the fragments written over it
    Set(ChainEntry),
    /// The partition has no records any more
    Removed,
}

impl Change {
    /// The bytes this change takes in a page: its slot and its fragments
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        match self {
            Change::Set(chain) => ENTRY_LEN + FRAGMENT_LEN * chain.fragments.len(),
            Change::Removed => ENTRY_LEN,
        }
    }

    /// The chain this change sets, if it sets one
    #[must_use]
    pub fn chain(&self) -> Option<&ChainEntry> {
        match self {
            Change::Set(chain) => Some(chain),
            Change::Removed => None,
        }
    }

    /// The chain this change sets, taken, if it sets one
    #[must_use]
    pub fn into_chain(self) -> Option<ChainEntry> {
        match self {
            Change::Set(chain) => Some(chain),
            Change::Removed => None,
        }
    }
}

/// The archives a run's entries name, each by a number of the run's own
#[derive(Debug, Default)]
pub struct ArchiveTable {
    /// Each archive, by its number
    pub ids: Vec<Uuid>,
    /// Each archive's number
    numbers: HashMap<Uuid, u32>,
}

impl ArchiveTable {
    /// An archive's number, adding it to the table if it has none
    ///
    /// # Arguments
    ///
    /// * `archive` - The archive
    pub fn number(&mut self, archive: Uuid) -> u32 {
        // an archive already numbered keeps its number
        if let Some(number) = self.numbers.get(&archive) {
            return *number;
        }
        // otherwise it takes the next one
        let number = u32::try_from(self.ids.len()).unwrap_or(u32::MAX);
        self.ids.push(archive);
        self.numbers.insert(archive, number);
        number
    }
}

/// The entries of one page while it is built, in key order
#[derive(Debug)]
pub struct PageBuilder {
    /// The entries, each pushed with a key above the last
    entries: Vec<(u64, Change)>,
    /// The bytes they take, the header included
    used: usize,
}

impl Default for PageBuilder {
    /// An empty page
    fn default() -> Self {
        PageBuilder {
            entries: Vec::with_capacity(PAGE_SIZE / ENTRY_LEN),
            used: HEADER_LEN,
        }
    }
}

impl PageBuilder {
    /// Whether nothing has been pushed
    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The first key of the page
    #[must_use]
    pub fn first_key(&self) -> Option<u64> {
        self.entries.first().map(|(key, _)| *key)
    }

    /// Whether a change still fits in this page
    ///
    /// # Arguments
    ///
    /// * `change` - The change
    #[must_use]
    pub fn fits(&self, change: &Change) -> bool {
        self.used + change.encoded_len() <= PAGE_SIZE
    }

    /// Whether a change fits in a page at all
    ///
    /// # Arguments
    ///
    /// * `change` - The change
    #[must_use]
    pub fn fits_alone(change: &Change) -> bool {
        // its fragment count is one byte, and the whole of it has to fit an empty page
        let fragments = change.chain().map_or(0, |chain| chain.fragments.len());
        fragments <= usize::from(u8::MAX) && HEADER_LEN + change.encoded_len() <= PAGE_SIZE
    }

    /// Add an entry to this page, whose key is above every key pushed before it
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `change` - What the index holds for it
    pub fn push(&mut self, key: u64, change: Change) {
        debug_assert!(
            self.entries.last().is_none_or(|(last, _)| *last < key),
            "a page's keys are pushed in order"
        );
        self.used += change.encoded_len();
        self.entries.push((key, change));
    }

    /// Lay this page out as the bytes written to disk, and empty it
    ///
    /// # Arguments
    ///
    /// * `archives` - The run's archive table, which every archive named here is added to
    pub fn encode(&mut self, archives: &mut ArchiveTable) -> Vec<u8> {
        // a whole page of zeros, so the padding is always the same bytes
        let mut page = vec![0u8; PAGE_SIZE];
        // the slots come first and every fragment after the last of them
        let count = self.entries.len();
        let mut fragment_at = HEADER_LEN + count * ENTRY_LEN;
        let mut fragments = 0usize;
        for (index, (key, change)) in self.entries.drain(..).enumerate() {
            let slot = HEADER_LEN + index * ENTRY_LEN;
            page[slot..slot + 8].copy_from_slice(&key.to_le_bytes());
            match change {
                Change::Set(chain) => {
                    // the base record where the slot says, its archive by the run's number
                    page[slot + 8..slot + 16].copy_from_slice(&chain.base.offset.to_le_bytes());
                    page[slot + 16..slot + 20]
                        .copy_from_slice(&archives.number(chain.base.archive).to_le_bytes());
                    page[slot + 20..slot + 24]
                        .copy_from_slice(&saturate(chain.base.size).to_le_bytes());
                    // where its fragments start among the page's, and how many it has
                    let first = u16::try_from(fragments).unwrap_or(u16::MAX);
                    page[slot + 24..slot + 26].copy_from_slice(&first.to_le_bytes());
                    page[slot + 26] = u8::try_from(chain.fragments.len()).unwrap_or(u8::MAX);
                    // and each fragment, oldest first
                    for fragment in &chain.fragments {
                        page[fragment_at..fragment_at + 8]
                            .copy_from_slice(&fragment.offset.to_le_bytes());
                        page[fragment_at + 8..fragment_at + 12]
                            .copy_from_slice(&archives.number(fragment.archive).to_le_bytes());
                        page[fragment_at + 12..fragment_at + 16]
                            .copy_from_slice(&saturate(fragment.size).to_le_bytes());
                        fragment_at += FRAGMENT_LEN;
                        fragments += 1;
                    }
                }
                // a removal is its key and its flag
                Change::Removed => page[slot + 27] = REMOVED,
            }
        }
        // the counts, then the checksum over everything after it
        page[8..10].copy_from_slice(&u16::try_from(count).unwrap_or(u16::MAX).to_le_bytes());
        page[10..12].copy_from_slice(&u16::try_from(fragments).unwrap_or(u16::MAX).to_le_bytes());
        let checksum = page_checksum(&page);
        page[..8].copy_from_slice(&checksum.to_le_bytes());
        self.used = HEADER_LEN;
        page
    }
}

/// A record's size as a slot holds it
///
/// rkyv's relative pointers bound a record below 4 GiB, so this never saturates; if it did, the
/// read would come back short and be refused as a torn record
/// ([O83](../../../../../../../docs/src/appendix/optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition)).
///
/// # Arguments
///
/// * `size` - The record's payload length
fn saturate(size: usize) -> u32 {
    u32::try_from(size).unwrap_or(u32::MAX)
}

/// The checksum a page carries over everything after it
///
/// # Arguments
///
/// * `page` - The whole page
fn page_checksum(page: &[u8]) -> u64 {
    // seeded like every other checksum on disk
    let mut hasher = GxHasher::default();
    hasher.write(&page[8..]);
    hasher.finish()
}

/// A page as it was read, checked against its checksum
#[derive(Debug, Clone, Copy)]
pub struct PageView<'a> {
    /// The page's bytes
    bytes: &'a [u8],
    /// How many entries it holds
    count: usize,
}

impl<'a> PageView<'a> {
    /// Check a page's bytes and read its counts
    ///
    /// A page that does not hash to its checksum, or whose counts run past its end, is the map's
    /// corruption: the same error a whole map that failed its hash was before the map was paged.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The page's bytes, a whole page
    pub fn parse(bytes: &'a [u8]) -> Result<Self, ServerError> {
        // a short page is a torn one
        if bytes.len() < PAGE_SIZE {
            return Err(ServerError::Shoal(ShoalError::TruncatedIntentLog));
        }
        let bytes = &bytes[..PAGE_SIZE];
        // the checksum covers everything after it
        let expected = u64::from_le_bytes(bytes[..8].try_into()?);
        let found = page_checksum(bytes);
        if expected != found {
            return Err(ServerError::Shoal(ShoalError::MapCorruption {
                found,
                expected,
            }));
        }
        // and the counts have to fit inside the page
        let count = usize::from(u16::from_le_bytes([bytes[8], bytes[9]]));
        let fragments = usize::from(u16::from_le_bytes([bytes[10], bytes[11]]));
        if HEADER_LEN + count * ENTRY_LEN + fragments * FRAGMENT_LEN > PAGE_SIZE {
            return Err(ServerError::Shoal(ShoalError::MapCorruption {
                found,
                expected,
            }));
        }
        Ok(PageView { bytes, count })
    }

    /// How many entries this page holds
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// The key of one entry
    ///
    /// # Arguments
    ///
    /// * `index` - The entry, below `len`
    #[must_use]
    pub fn key(&self, index: usize) -> u64 {
        let slot = HEADER_LEN + index * ENTRY_LEN;
        u64::from_le_bytes(read8(&self.bytes[slot..slot + 8]))
    }

    /// The first entry whose key is at least a key, or `len` if none is
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn lower_bound(&self, key: u64) -> usize {
        // a binary search of the slots, which are in key order
        let (mut low, mut high) = (0, self.count);
        while low < high {
            let mid = (low + high) / 2;
            if self.key(mid) < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    /// The entry holding a key, if this page holds it
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn find(&self, key: u64) -> Option<usize> {
        let at = self.lower_bound(key);
        (at < self.count && self.key(at) == key).then_some(at)
    }

    /// What one entry holds, its archives named through the run's table
    ///
    /// # Arguments
    ///
    /// * `index` - The entry, below `len`
    /// * `archives` - The run's archive table
    #[must_use]
    pub fn change(&self, index: usize, archives: &[Uuid]) -> Change {
        let slot = HEADER_LEN + index * ENTRY_LEN;
        let key = self.key(index);
        // a removal is its key and its flag
        if self.bytes[slot + 27] & REMOVED != 0 {
            return Change::Removed;
        }
        // an archive number the table does not hold names no archive, and its read is refused
        let archive = |number: u32| archives.get(number as usize).copied().unwrap_or_default();
        let base = ArchiveEntry {
            key,
            offset: u64::from_le_bytes(read8(&self.bytes[slot + 8..slot + 16])),
            archive: archive(u32::from_le_bytes(read4(&self.bytes[slot + 16..slot + 20]))),
            size: u32::from_le_bytes(read4(&self.bytes[slot + 20..slot + 24])) as usize,
        };
        // and each fragment over it, from where the slot says they start
        let first = usize::from(u16::from_le_bytes([
            self.bytes[slot + 24],
            self.bytes[slot + 25],
        ]));
        let count = usize::from(self.bytes[slot + 26]);
        let start = HEADER_LEN + self.count * ENTRY_LEN + first * FRAGMENT_LEN;
        let fragments = (0..count)
            .map(|at| {
                let fragment = start + at * FRAGMENT_LEN;
                ArchiveEntry {
                    key,
                    offset: u64::from_le_bytes(read8(&self.bytes[fragment..fragment + 8])),
                    archive: archive(u32::from_le_bytes(read4(
                        &self.bytes[fragment + 8..fragment + 12],
                    ))),
                    size: u32::from_le_bytes(read4(&self.bytes[fragment + 12..fragment + 16]))
                        as usize,
                }
            })
            .collect();
        Change::Set(ChainEntry { base, fragments })
    }
}

/// Eight bytes as an array, from a slice that is eight bytes long
///
/// # Arguments
///
/// * `bytes` - The slice
fn read8(bytes: &[u8]) -> [u8; 8] {
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    out
}

/// Four bytes as an array, from a slice that is four bytes long
///
/// # Arguments
///
/// * `bytes` - The slice
fn read4(bytes: &[u8]) -> [u8; 4] {
    let mut out = [0u8; 4];
    out.copy_from_slice(bytes);
    out
}

/// A page read and checked, owned, as the cache holds it
#[derive(Debug)]
pub struct Page {
    /// The page's bytes, a whole page
    bytes: Box<[u8]>,
}

impl Page {
    /// Check a page and keep a copy of it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The page's bytes
    pub fn new(bytes: &[u8]) -> Result<Self, ServerError> {
        // checked before it is kept, so a cached page is always a good one
        PageView::parse(bytes)?;
        Ok(Page {
            bytes: bytes[..PAGE_SIZE].into(),
        })
    }

    /// The page, to search
    #[must_use]
    pub fn view(&self) -> PageView<'_> {
        // checked when it was kept, so its counts are known to fit
        let count = usize::from(u16::from_le_bytes([self.bytes[8], self.bytes[9]]));
        PageView {
            bytes: &self.bytes,
            count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ArchiveTable, Change, Page, PageBuilder, PageView, PAGE_SIZE};
    use crate::server::tables::storage::fs::map::{ArchiveEntry, ChainEntry};
    use uuid::Uuid;

    /// A page round-trips its entries: whole records, chains of fragments, and removals
    #[test]
    fn a_page_round_trips_chains_and_removals() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let at = |key: u64, archive: Uuid, offset: u64, size: usize| ArchiveEntry {
            key,
            archive,
            offset,
            size,
        };
        let changes = vec![
            (
                3,
                Change::Set(ChainEntry {
                    base: at(3, a, 16, 700),
                    fragments: vec![],
                }),
            ),
            (9, Change::Removed),
            (
                12,
                Change::Set(ChainEntry {
                    base: at(12, b, 4096, 9000),
                    fragments: vec![at(12, a, 800, 40), at(12, b, 99_999, 41)],
                }),
            ),
            (
                u64::MAX,
                Change::Set(ChainEntry {
                    base: at(u64::MAX, a, 1, 2),
                    fragments: vec![],
                }),
            ),
        ];
        let mut builder = PageBuilder::default();
        for (key, change) in &changes {
            assert!(builder.fits(change));
            builder.push(*key, change.clone());
        }
        let mut archives = ArchiveTable::default();
        let bytes = builder.encode(&mut archives);
        assert_eq!(bytes.len(), PAGE_SIZE);
        assert!(builder.is_empty(), "encoding empties the builder");
        // every entry reads back as it was pushed, found by its key
        let page = Page::new(&bytes).expect("a good page");
        let view = page.view();
        assert_eq!(view.len(), changes.len());
        for (key, change) in &changes {
            let index = view.find(*key).expect("a key the page holds");
            assert_eq!(&view.change(index, &archives.ids), change);
        }
        // and keys it does not hold are not found
        assert_eq!(view.find(4), None);
        assert_eq!(view.find(0), None);
        assert_eq!(view.lower_bound(10), 2);
        // a flipped byte anywhere is refused
        let mut torn = bytes.clone();
        torn[2000] ^= 1;
        assert!(PageView::parse(&torn).is_err());
    }

    /// A page fills to its size and no further, and a chain too long for any page is told apart
    #[test]
    fn a_page_holds_what_fits() {
        let entry = Change::Set(ChainEntry {
            base: ArchiveEntry {
                key: 0,
                archive: Uuid::nil(),
                offset: 0,
                size: 1,
            },
            fragments: vec![],
        });
        let mut builder = PageBuilder::default();
        let mut key = 0;
        while builder.fits(&entry) {
            builder.push(key, entry.clone());
            key += 1;
        }
        // 4080 bytes of 28 byte slots
        assert_eq!(key, 145);
        let long = Change::Set(ChainEntry {
            base: ArchiveEntry {
                key: 0,
                archive: Uuid::nil(),
                offset: 0,
                size: 1,
            },
            fragments: vec![
                ArchiveEntry {
                    key: 0,
                    archive: Uuid::nil(),
                    offset: 0,
                    size: 1
                };
                300
            ],
        });
        assert!(!PageBuilder::fits_alone(&long));
        assert!(PageBuilder::fits_alone(&entry));
    }
}
