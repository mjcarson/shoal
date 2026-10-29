//! A cache of the units reads verified, on the object executor that owns their slices, so a read of
//! the same units again is answered without its device (F102)
//!
//! A unit is held by its slice, its chunk, the label it was read at and its index. The bytes of a
//! chunk at a label never change once written: a label's tag is a try's own, a whole chunk never
//! lands over a newer one, and an object's id is never registered again once retired
//! ([F102](../../../../../docs/src/features/read-unit-cache.md)). So a unit held is never wrong for a
//! reader that names its label, and a change to a chunk only leaves what is held of it unreachable:
//! the slice drops a chunk's units at every change it makes to the chunk, so they are not kept for
//! nothing.
//!
//! # Invariants
//!
//! - A unit is put in only once a read verified it, and only at the label the read made.
//! - A read is answered from here only when every unit its range covers is held at one label it
//!   asked for; never a range of units at two labels.
//! - The units held never take more than the cache's bound: the executor's share of
//!   `resources.object_cache`, which a node past its memory lowers to have the cache give units
//!   back before any shard evicts rows ([F107](../../../../../docs/src/features/cache-default-share.md)). A bound is never
//!   raised again.
//! - The cache is touched only by the executor that owns it: a `RefCell`, never shared.
//! - A hit is slices of the units held, never a copy, and a unit's bytes never change while any
//!   slice of them is held: a change to a chunk drops the cache's units, never the bytes a reader
//!   holds ([F111](../../../../../docs/src/features/lane-buffers.md)).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use super::chunk::ChunkId;
use crate::shared::devices::SliceId;
use crate::shared::objects::Label;

/// What one executor's cache did, readable from any thread
#[derive(Debug, Default)]
pub struct CacheFigures {
    /// The reads answered from the cache, every unit of their range held
    pub hits: AtomicU64,
    /// The reads that looked and were not answered
    pub misses: AtomicU64,
    /// The units put in
    pub filled: AtomicU64,
    /// The units given up to make room
    pub evicted: AtomicU64,
    /// The units dropped because their chunk changed
    pub dropped: AtomicU64,
    /// The bytes of units held now
    pub held: AtomicU64,
    /// The most bytes of units it may hold: its share of the node's cache, lowered by a node past
    /// its memory (F107)
    pub limit: AtomicU64,
}

impl CacheFigures {
    /// The figures as a report carries them, read now
    #[must_use]
    pub fn report(&self) -> CacheReport {
        // each counter read on its own: a report is a sample, not a snapshot
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        CacheReport {
            hits: load(&self.hits),
            misses: load(&self.misses),
            filled: load(&self.filled),
            evicted: load(&self.evicted),
            dropped: load(&self.dropped),
            held: load(&self.held),
            limit: load(&self.limit),
        }
    }
}

/// What a node's caches did, summed over its executors, which a node's figures carry since F104
pub use crate::shared::protocol::stats::CacheReport;

/// A chunk at one label, as it is held: by its slice, its chunk, and the label's sequence and tag
type ChunkKey = (SliceId, ChunkId, u64, u64);

/// One unit held, and whether a read took it since the clock's hand last passed
struct Unit {
    /// The unit's bytes, verified by the read that put them in, shared with the hits that hold
    /// slices of them (F111)
    bytes: Bytes,
    /// Whether a read took it since the hand last passed, which spares it once
    taken: Cell<bool>,
}

/// A range the cache answered: slices of the units it holds, in order, never copied (F111)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedRange {
    /// The part of each unit the range covers, in order
    pub parts: Vec<Bytes>,
}

impl CachedRange {
    /// How many bytes the range is
    #[must_use]
    pub fn len(&self) -> usize {
        self.parts.iter().map(Bytes::len).sum()
    }

    /// Whether it is no bytes
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Its bytes in one buffer of their own, copied
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        for part in &self.parts {
            out.extend_from_slice(part);
        }
        out
    }
}

/// One chunk's units at one label
struct Held {
    /// The bytes of one unit of the chunk
    unit_bytes: u32,
    /// The chunk's bytes, past which a read is cut
    chunk_bytes: u64,
    /// Its units held, by index
    units: BTreeMap<u32, Unit>,
}

/// What the cache holds, behind its cell
#[derive(Default)]
struct Inner {
    /// The bytes of units held
    used: u64,
    /// Every chunk held at a label
    chunks: HashMap<ChunkKey, Held>,
    /// The labels each chunk is held at, so a change to the chunk drops them all
    labels: HashMap<(SliceId, ChunkId), Vec<(u64, u64)>>,
    /// Every unit in the order it was put in, the clock's hand at the front; a unit dropped since
    /// is passed over when the hand reaches it
    clock: VecDeque<(ChunkKey, u32)>,
}

/// The units one executor's reads verified, bounded by its share of `resources.object_cache`
pub struct UnitCache {
    /// What it did, for whoever reads the node's figures, its bound among them, which another
    /// thread may lower (F107)
    figures: Arc<CacheFigures>,
    /// What it holds
    inner: RefCell<Inner>,
}

impl UnitCache {
    /// A cache that holds at most this many bytes of units
    ///
    /// # Arguments
    ///
    /// * `limit` - The most bytes of units it holds
    /// * `figures` - Where it counts what it does
    #[must_use]
    pub fn new(limit: u64, figures: Arc<CacheFigures>) -> Self {
        // the bound is part of the figures, so a report says what each cache may hold
        figures.limit.store(limit, Ordering::Relaxed);
        UnitCache {
            figures,
            inner: RefCell::new(Inner::default()),
        }
    }

    /// The most bytes of units it may hold now
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.figures.limit.load(Ordering::Relaxed)
    }

    /// Give up units until what it holds is within its bound, which a node past its memory
    /// lowers, the units read least recently first; the bytes given up ([F107](../../../../../docs/src/features/cache-default-share.md))
    pub fn shrink(&self) -> u64 {
        let limit = self.limit();
        let mut guard = self.inner.borrow_mut();
        let inner = &mut *guard;
        let before = inner.used;
        // the units read least recently go, as they would for room
        while inner.used > limit {
            if !Self::evict_one(inner, &self.figures) {
                break;
            }
        }
        self.figures.held.store(inner.used, Ordering::Relaxed);
        before - inner.used
    }

    /// A range of a chunk, if every unit it covers is held at one of the labels asked for: the
    /// first label in the reader's order that holds them all, and the range cut from them
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice the chunk is on
    /// * `id` - The chunk
    /// * `labels` - The labels the reader asks for, in its order
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    #[must_use]
    pub fn read(
        &self,
        slice: SliceId,
        id: &ChunkId,
        labels: &[Label],
        offset: u64,
        len: u64,
    ) -> Option<(Label, CachedRange)> {
        let inner = self.inner.borrow();
        for label in labels {
            // the chunk at this label, if any of it is held
            let Some(held) = inner.chunks.get(&(slice, *id, label.sequence, label.tag)) else {
                continue;
            };
            // the range is cut at the chunk's end, as a read off its device is
            let end = (offset + len).min(held.chunk_bytes);
            if offset >= end {
                self.figures.hits.fetch_add(1, Ordering::Relaxed);
                return Some((*label, CachedRange::default()));
            }
            // every unit the range covers, or none of them
            let unit_bytes = u64::from(held.unit_bytes.max(1));
            let first = u32::try_from(offset / unit_bytes).unwrap_or(u32::MAX);
            let last = u32::try_from((end - 1) / unit_bytes).unwrap_or(u32::MAX);
            if (first..=last).any(|unit| !held.units.contains_key(&unit)) {
                continue;
            }
            // the part of each unit the range covers, in order, as a slice of the unit held (F111)
            let mut parts = Vec::with_capacity((last - first + 1) as usize);
            for unit in first..=last {
                let held_unit = &held.units[&unit];
                held_unit.taken.set(true);
                let unit_start = u64::from(unit) * unit_bytes;
                let from = usize::try_from(offset.max(unit_start) - unit_start).unwrap_or(0);
                let to = usize::try_from(end.min(unit_start + unit_bytes) - unit_start).unwrap_or(0);
                let to = to.min(held_unit.bytes.len());
                parts.push(held_unit.bytes.slice(from.min(to)..to));
            }
            self.figures.hits.fetch_add(1, Ordering::Relaxed);
            return Some((*label, CachedRange { parts }));
        }
        // no label it asked for holds the whole range
        self.figures.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Put in the units a read verified at a label, giving up the units read least recently for
    /// room; a unit already held is kept as it is
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice the chunk is on
    /// * `id` - The chunk
    /// * `label` - The label the read made
    /// * `unit_bytes` - The bytes of one of the chunk's units
    /// * `chunk_bytes` - The chunk's bytes, past which a read is cut
    /// * `units` - The units the read verified, whole, by index
    pub fn fill(
        &self,
        slice: SliceId,
        id: &ChunkId,
        label: Label,
        unit_bytes: u32,
        chunk_bytes: u64,
        units: BTreeMap<u32, Vec<u8>>,
    ) {
        // a cache of no room holds nothing, its bound read once for the fill
        let limit = self.limit();
        if limit == 0 {
            return;
        }
        let mut guard = self.inner.borrow_mut();
        let inner = &mut *guard;
        let key = (slice, *id, label.sequence, label.tag);
        for (index, bytes) in units {
            let size = bytes.len() as u64;
            // a unit larger than the whole cache is never held
            if size > limit {
                continue;
            }
            // one already held is the same bytes, since a label's bytes never change
            if inner.chunks.get(&key).is_some_and(|held| held.units.contains_key(&index)) {
                continue;
            }
            // room for it, the units read least recently given up first
            while inner.used + size > limit {
                if !Self::evict_one(inner, &self.figures) {
                    return;
                }
            }
            // the chunk at this label, held from now, and its label noted for its chunk's changes
            let held = inner.chunks.entry(key).or_insert_with(|| Held {
                unit_bytes,
                chunk_bytes,
                units: BTreeMap::new(),
            });
            held.units.insert(
                index,
                Unit {
                    bytes: Bytes::from(bytes),
                    taken: Cell::new(false),
                },
            );
            let labels = inner.labels.entry((slice, *id)).or_default();
            if !labels.contains(&(label.sequence, label.tag)) {
                labels.push((label.sequence, label.tag));
            }
            inner.used += size;
            inner.clock.push_back((key, index));
            self.figures.filled.fetch_add(1, Ordering::Relaxed);
        }
        self.figures.held.store(inner.used, Ordering::Relaxed);
    }

    /// Drop every unit of a chunk, at every label, since the slice changed it
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice the chunk is on
    /// * `id` - The chunk
    pub fn drop_chunk(&self, slice: SliceId, id: &ChunkId) {
        let mut guard = self.inner.borrow_mut();
        let inner = &mut *guard;
        // the labels it is held at; a chunk held at none has nothing to drop
        let Some(labels) = inner.labels.remove(&(slice, *id)) else {
            return;
        };
        for (sequence, tag) in labels {
            if let Some(held) = inner.chunks.remove(&(slice, *id, sequence, tag)) {
                let bytes: u64 = held.units.values().map(|unit| unit.bytes.len() as u64).sum();
                inner.used = inner.used.saturating_sub(bytes);
                self.figures.dropped.fetch_add(held.units.len() as u64, Ordering::Relaxed);
            }
        }
        // the clock keeps the dropped units' places until its hand passes them; when they come to
        // outnumber the units held, they are swept out at once so the clock stays bounded
        let live: usize = inner.chunks.values().map(|held| held.units.len()).sum();
        if inner.clock.len() > 2 * live + 1024 {
            let Inner { chunks, clock, .. } = &mut *inner;
            clock.retain(|(key, index)| chunks.get(key).is_some_and(|held| held.units.contains_key(index)));
        }
        self.figures.held.store(inner.used, Ordering::Relaxed);
    }

    /// The bytes of units held now
    #[must_use]
    pub fn used(&self) -> u64 {
        self.inner.borrow().used
    }

    /// The units held of a chunk, at every label
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice the chunk is on
    /// * `id` - The chunk
    #[must_use]
    pub fn units_of(&self, slice: SliceId, id: &ChunkId) -> usize {
        let inner = self.inner.borrow();
        inner.labels.get(&(slice, *id)).map_or(0, |labels| {
            labels
                .iter()
                .filter_map(|(sequence, tag)| inner.chunks.get(&(slice, *id, *sequence, *tag)))
                .map(|held| held.units.len())
                .sum()
        })
    }

    /// Give up the unit the clock's hand reaches first that no read took since it last passed,
    /// sparing each one taken once; whether one was given up
    ///
    /// # Arguments
    ///
    /// * `inner` - What the cache holds
    /// * `figures` - Where it counts what it gives up
    fn evict_one(inner: &mut Inner, figures: &CacheFigures) -> bool {
        // twice round at most: the first pass clears every unit's mark, the second finds one
        let mut budget = inner.clock.len().saturating_mul(2).saturating_add(1);
        while let Some((key, index)) = inner.clock.pop_front() {
            budget = budget.saturating_sub(1);
            // a place whose unit was dropped since is passed over
            let Some(held) = inner.chunks.get_mut(&key) else {
                if budget == 0 {
                    return false;
                }
                continue;
            };
            let Some(unit) = held.units.get(&index) else {
                if budget == 0 {
                    return false;
                }
                continue;
            };
            // a unit taken since the hand last passed is spared this once
            if unit.taken.get() && budget > 0 {
                unit.taken.set(false);
                inner.clock.push_back((key, index));
                continue;
            }
            // this one goes, and its chunk at this label with it if it was the last
            let size = unit.bytes.len() as u64;
            held.units.remove(&index);
            let emptied = held.units.is_empty();
            inner.used = inner.used.saturating_sub(size);
            figures.evicted.fetch_add(1, Ordering::Relaxed);
            if emptied {
                inner.chunks.remove(&key);
                let (slice, id, sequence, tag) = key;
                if let Some(labels) = inner.labels.get_mut(&(slice, id)) {
                    labels.retain(|label| *label != (sequence, tag));
                    if labels.is_empty() {
                        inner.labels.remove(&(slice, id));
                    }
                }
            }
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::poolmap::PlacementGroup;

    /// A chunk of one object, at a stripe and a position
    fn chunk(object: u128) -> ChunkId {
        ChunkId {
            consumer: 1,
            pg: PlacementGroup { tablet: 5, sub: 0 },
            object,
            stripe: 0,
            position: 0,
        }
    }

    /// A label at a sequence
    fn label(sequence: u64) -> Label {
        Label { sequence, tag: 7 }
    }

    /// Units of four bytes each, filled with their own index
    fn units(indices: &[u32]) -> BTreeMap<u32, Vec<u8>> {
        indices.iter().map(|index| (*index, vec![*index as u8; 4])).collect()
    }

    /// A hit's bytes, flat, for a test to compare
    ///
    /// # Arguments
    ///
    /// * `read` - What a read of the cache answered
    fn flat(read: Option<(Label, CachedRange)>) -> Option<(Label, Vec<u8>)> {
        read.map(|(label, range)| (label, range.to_vec()))
    }

    /// A hit is slices of the units the cache holds, never a copy of them, and a change to the
    /// chunk drops the cache's units without touching the bytes a reader still holds (F111)
    #[test]
    fn a_hit_is_slices_of_the_units_held() {
        let cache = UnitCache::new(1 << 20, Arc::new(CacheFigures::default()));
        let slice = SliceId(uuid::Uuid::from_u128(1));
        let id = chunk(1);
        cache.fill(slice, &id, label(1), 4, 16, units(&[0, 1, 2]));
        // a range across units 0, 1 and 2, one slice of each
        let (_, first) = cache.read(slice, &id, &[label(1)], 2, 8).expect("a hit");
        assert_eq!(first.parts.len(), 3);
        assert_eq!(first.to_vec(), vec![0, 0, 1, 1, 1, 1, 2, 2]);
        // a second hit of unit 1 points at the bytes the first one's slice does
        let (_, second) = cache.read(slice, &id, &[label(1)], 4, 4).expect("a hit");
        assert_eq!(second.parts[0].as_ptr(), first.parts[1].as_ptr(), "a hit was copied");
        // a change drops the units, and the slices held keep their bytes
        cache.drop_chunk(slice, &id);
        assert_eq!(cache.read(slice, &id, &[label(1)], 2, 8), None);
        assert_eq!(first.to_vec(), vec![0, 0, 1, 1, 1, 1, 2, 2]);
    }

    /// A range is answered only when every unit it covers is held at a label asked for, the
    /// first in the reader's order, and cut at the chunk's end
    #[test]
    fn a_range_is_answered_whole_from_one_label_or_not_at_all() {
        let cache = UnitCache::new(1 << 20, Arc::new(CacheFigures::default()));
        let slice = SliceId(uuid::Uuid::from_u128(1));
        let id = chunk(1);
        // units 0 and 1 at label 1, unit 2 at label 2, of a chunk of ten bytes
        cache.fill(slice, &id, label(1), 4, 10, units(&[0, 1]));
        cache.fill(slice, &id, label(2), 4, 10, units(&[2]));
        // a range in units 0 and 1 is answered at label 1
        let read = flat(cache.read(slice, &id, &[label(1)], 2, 4));
        assert_eq!(read, Some((label(1), vec![0, 0, 1, 1])));
        // a range reaching unit 2 is not, at label 1, since unit 2 is held only at label 2
        assert_eq!(cache.read(slice, &id, &[label(1)], 2, 8), None);
        // a reader asking for label 2 first is answered there, cut at the chunk's ten bytes
        let read = flat(cache.read(slice, &id, &[label(2), label(1)], 8, 100));
        assert_eq!(read, Some((label(2), vec![2, 2])));
        // a label nobody read is never answered, however much of the chunk is held
        assert_eq!(cache.read(slice, &id, &[label(3)], 0, 4), None);
        // nor another slice's copy of the chunk
        assert_eq!(cache.read(SliceId(uuid::Uuid::from_u128(2)), &id, &[label(1)], 0, 4), None);
        let figures = cache.figures.report();
        assert_eq!((figures.hits, figures.misses, figures.filled), (2, 3, 3));
    }

    /// A change to a chunk drops its units at every label, and nothing of another chunk
    #[test]
    fn a_changed_chunk_drops_every_label_it_is_held_at() {
        let cache = UnitCache::new(1 << 20, Arc::new(CacheFigures::default()));
        let slice = SliceId(uuid::Uuid::from_u128(1));
        cache.fill(slice, &chunk(1), label(1), 4, 16, units(&[0, 1]));
        cache.fill(slice, &chunk(1), label(2), 4, 16, units(&[0]));
        cache.fill(slice, &chunk(2), label(1), 4, 16, units(&[0]));
        cache.drop_chunk(slice, &chunk(1));
        assert_eq!(cache.units_of(slice, &chunk(1)), 0);
        assert_eq!(cache.units_of(slice, &chunk(2)), 1);
        assert_eq!(cache.used(), 4);
        assert_eq!(cache.figures.report().dropped, 3);
        assert_eq!(cache.read(slice, &chunk(1), &[label(1), label(2)], 0, 4), None);
    }

    /// The units held never take more than the bound, and a unit read since the hand passed is
    /// spared over one that was not
    #[test]
    fn the_cache_holds_its_bound_and_gives_up_what_was_not_read() {
        // room for three units of four bytes
        let cache = UnitCache::new(12, Arc::new(CacheFigures::default()));
        let slice = SliceId(uuid::Uuid::from_u128(1));
        cache.fill(slice, &chunk(1), label(1), 4, 4, units(&[0]));
        cache.fill(slice, &chunk(2), label(1), 4, 4, units(&[0]));
        cache.fill(slice, &chunk(3), label(1), 4, 4, units(&[0]));
        // the first is read again, so the next fill gives up the second
        assert!(cache.read(slice, &chunk(1), &[label(1)], 0, 4).is_some());
        cache.fill(slice, &chunk(4), label(1), 4, 4, units(&[0]));
        assert_eq!(cache.used(), 12);
        assert_eq!(cache.units_of(slice, &chunk(1)), 1);
        assert_eq!(cache.units_of(slice, &chunk(2)), 0);
        assert_eq!(cache.units_of(slice, &chunk(4)), 1);
        assert_eq!(cache.figures.report().evicted, 1);
        // and a cache of no room holds nothing
        let none = UnitCache::new(0, Arc::new(CacheFigures::default()));
        none.fill(slice, &chunk(1), label(1), 4, 4, units(&[0]));
        assert_eq!(none.used(), 0);
    }

    /// A bound lowered from another thread, as a node past its memory lowers it, has the cache give
    /// units back down to it, those read least recently first, and a later fill keep to it (F107)
    #[test]
    fn a_lowered_bound_gives_units_back() {
        let figures = Arc::new(CacheFigures::default());
        let cache = UnitCache::new(16, figures.clone());
        let slice = SliceId(uuid::Uuid::from_u128(1));
        // four units of four bytes, the first read since they were put in
        cache.fill(slice, &chunk(1), label(1), 4, 16, units(&[0, 1, 2, 3]));
        assert!(cache.read(slice, &chunk(1), &[label(1)], 0, 4).is_some());
        assert_eq!(cache.used(), 16);
        // the bound lowered to two units: two given up, the one read spared
        figures.limit.store(8, Ordering::Relaxed);
        assert_eq!(cache.limit(), 8);
        assert_eq!(cache.shrink(), 8);
        assert_eq!((cache.used(), figures.held.load(Ordering::Relaxed)), (8, 8));
        assert!(cache.read(slice, &chunk(1), &[label(1)], 0, 4).is_some(), "the unit read was given up first");
        // a shrink within the bound gives up nothing, and a fill keeps to the lowered bound
        assert_eq!(cache.shrink(), 0);
        cache.fill(slice, &chunk(2), label(1), 4, 12, units(&[0, 1, 2]));
        assert!(cache.used() <= 8, "a fill took the cache past its lowered bound: {}", cache.used());
    }
}
