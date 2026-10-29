//! A table's resident partitions, each tablet's in a map of its own
//!
//! A table kept every partition it held in memory in one map, so anything asking for one tablet's
//! partitions - a scrub's cut, a volatile group's snapshot, a snapshot install's eviction - read
//! every partition of the table to find them. Here each tablet's partitions are a map of their own,
//! reached through an index of the tablets present, so the same questions read one tablet's
//! partitions and nothing else ([F87](../../../../docs/src/features/tablet-walk.md)).
//!
//! # Invariants
//!
//! **A key lives in the map of its own tablet and nowhere else.** The tablet is
//! [`tablet_of`], the placement rule both peers compute, so every lookup and every walk of a
//! tablet agree about where a key is.
//!
//! **A slot names its tablet's map, or nothing.** `slots[t]` is one more than the position of
//! tablet `t`'s map in `maps`, and zero when the tablet has none; removing a map moves the last
//! one into its place, and the moved tablet's slot is repointed in the same step.
//!
//! **Nothing here is traced or profiled.** Every query reaches a partition through it.

use std::collections::hash_map::{self, HashMap};

use crate::shared::placement::{tablet_of, TABLET_COUNT};

/// The fewest buckets a tablet's map is shrunk to, below which shrinking frees nothing worth a rehash
const SHRINK_FLOOR: usize = 64;

/// The bytes the index of tablets takes once anything is resident
const SLOTS_BYTES: usize = TABLET_COUNT * std::mem::size_of::<u16>();

/// One tablet's resident partitions
#[derive(Debug)]
struct TabletPartitions<V> {
    /// The tablet these partitions are in
    tablet: u16,
    /// The partitions, by key
    map: HashMap<u64, V>,
}

/// A table's resident partitions, each tablet's in a map of its own
#[derive(Debug)]
pub struct ResidentPartitions<V> {
    /// One more than the position of each tablet's map in `maps`, zero for none
    ///
    /// Allocated at the first insert, so a table that never holds anything costs nothing.
    slots: Option<Box<[u16; TABLET_COUNT]>>,
    /// The map of every tablet holding a partition, or having held one since the last shrink
    maps: Vec<TabletPartitions<V>>,
}

impl<V> Default for ResidentPartitions<V> {
    /// An empty set of resident partitions
    fn default() -> Self {
        Self::new()
    }
}

impl<V> ResidentPartitions<V> {
    /// An empty set of resident partitions, which allocates nothing
    #[must_use]
    pub fn new() -> Self {
        ResidentPartitions {
            slots: None,
            maps: Vec::new(),
        }
    }

    /// The tablet a key is in, as the index holds it
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    fn tablet(key: u64) -> usize {
        // the placement rule, so a lookup and a walk of a tablet agree
        tablet_of(key)
    }

    /// The map of the tablet a key is in, if that tablet has one
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    fn map_of(&self, key: u64) -> Option<&HashMap<u64, V>> {
        // no index means nothing was ever resident
        let slots = self.slots.as_ref()?;
        // zero names no map
        let slot = slots[Self::tablet(key)];
        if slot == 0 {
            return None;
        }
        Some(&self.maps[usize::from(slot) - 1].map)
    }

    /// The map of the tablet a key is in, mutably, if that tablet has one
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    fn map_of_mut(&mut self, key: u64) -> Option<&mut HashMap<u64, V>> {
        // no index means nothing was ever resident
        let slots = self.slots.as_ref()?;
        // zero names no map
        let slot = slots[Self::tablet(key)];
        if slot == 0 {
            return None;
        }
        Some(&mut self.maps[usize::from(slot) - 1].map)
    }

    /// The map of the tablet a key is in, made if that tablet has none yet
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    fn map_for(&mut self, key: u64) -> &mut HashMap<u64, V> {
        // the index is made at the first partition ever held
        let slots = self
            .slots
            .get_or_insert_with(|| Box::new([0u16; TABLET_COUNT]));
        let tablet = Self::tablet(key);
        let mut slot = slots[tablet];
        // a tablet with no map gets one at the end, and its slot names it
        if slot == 0 {
            // a tablet id is twelve bits, so it fits
            #[allow(clippy::cast_possible_truncation)]
            self.maps.push(TabletPartitions {
                tablet: tablet as u16,
                map: HashMap::new(),
            });
            // there are at most TABLET_COUNT maps, so one more than a position fits a u16
            #[allow(clippy::cast_possible_truncation)]
            {
                slot = self.maps.len() as u16;
            }
            slots[tablet] = slot;
        }
        &mut self.maps[usize::from(slot) - 1].map
    }

    /// The partition under a key, if it is resident
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    #[must_use]
    pub fn get(&self, key: &u64) -> Option<&V> {
        // the key's tablet's map, then the key in it
        self.map_of(*key)?.get(key)
    }

    /// The partition under a key, mutably, if it is resident
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    pub fn get_mut(&mut self, key: &u64) -> Option<&mut V> {
        // the key's tablet's map, then the key in it
        self.map_of_mut(*key)?.get_mut(key)
    }

    /// Whether a partition is resident
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    #[must_use]
    pub fn contains_key(&self, key: &u64) -> bool {
        // the key's tablet's map, then the key in it
        self.map_of(*key).is_some_and(|map| map.contains_key(key))
    }

    /// Hold a partition, returning the one it replaced
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `value` - The partition
    #[inline]
    pub fn insert(&mut self, key: u64, value: V) -> Option<V> {
        // into the key's tablet's map, made if it has none
        self.map_for(key).insert(key, value)
    }

    /// Stop holding a partition, returning it
    ///
    /// A tablet left with no partitions keeps its map until [`Self::shrink_if_sparse`].
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    pub fn remove(&mut self, key: &u64) -> Option<V> {
        // out of the key's tablet's map
        self.map_of_mut(*key)?.remove(key)
    }

    /// The entry of a partition, held or not
    ///
    /// The tablet's map is made if it has none, so an entry left vacant leaves an empty map until
    /// [`Self::shrink_if_sparse`].
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[inline]
    pub fn entry(&mut self, key: u64) -> hash_map::Entry<'_, u64, V> {
        // the key's tablet's map, made if it has none, then the key in it
        self.map_for(key).entry(key)
    }

    /// How many partitions are resident, in every tablet
    #[must_use]
    pub fn len(&self) -> usize {
        // every tablet's count
        self.maps.iter().map(|tablet| tablet.map.len()).sum()
    }

    /// Whether no partition is resident
    #[must_use]
    pub fn is_empty(&self) -> bool {
        // a map left empty by a removal holds nothing
        self.maps.iter().all(|tablet| tablet.map.is_empty())
    }

    /// Every resident partition and its key, tablet by tablet, in no order within one
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &V)> {
        // each tablet's map in turn
        self.maps.iter().flat_map(|tablet| tablet.map.iter())
    }

    /// Every resident partition's key, tablet by tablet, in no order within one
    pub fn keys(&self) -> impl Iterator<Item = &u64> {
        // each tablet's map in turn
        self.maps.iter().flat_map(|tablet| tablet.map.keys())
    }

    /// Every resident partition, tablet by tablet, in no order within one
    pub fn values(&self) -> impl Iterator<Item = &V> {
        // each tablet's map in turn
        self.maps.iter().flat_map(|tablet| tablet.map.values())
    }

    /// One tablet's resident partitions, if it has any map
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn tablet_map(&self, tablet: u16) -> Option<&HashMap<u64, V>> {
        // a tablet past the key space has no slot, and so no map
        let slots = self.slots.as_ref()?;
        let slot = *slots.get(usize::from(tablet))?;
        if slot == 0 {
            return None;
        }
        Some(&self.maps[usize::from(slot) - 1].map)
    }

    /// Stop holding every partition of one tablet, returning them
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    pub fn take_tablet(&mut self, tablet: u16) -> Option<HashMap<u64, V>> {
        // a tablet with no slot holds nothing
        let slots = self.slots.as_mut()?;
        let slot = *slots.get(usize::from(tablet))?;
        if slot == 0 {
            return None;
        }
        // the map leaves, and the last one moves into its place
        let position = usize::from(slot) - 1;
        let taken = self.maps.swap_remove(position);
        slots[usize::from(tablet)] = 0;
        // the moved tablet's slot names its new place, in the same step
        if let Some(moved) = self.maps.get(position) {
            slots[usize::from(moved.tablet)] = slot;
        }
        Some(taken.map)
    }

    /// Give back the room of tablets that hold nothing or little
    ///
    /// A `HashMap` keeps the capacity of the most entries it ever held. A tablet's map grows to
    /// hold every partition a load or a scan made resident, and after eviction keeps those
    /// buckets, which no eviction budget counts: at ten times the lab's dataset they were
    /// gigabytes of a node
    /// ([cluster testing](../../../../docs/src/cluster-testing/performance.md#memory-at-ten-times-the-dataset)).
    /// A tablet holding nothing gives its map back, one under a quarter of its room is shrunk to
    /// twice what it holds, and the index goes once no tablet has a map.
    pub fn shrink_if_sparse(&mut self) {
        // every map in turn, from the end so a removal never moves one not yet looked at
        let mut position = self.maps.len();
        while position > 0 {
            position -= 1;
            let held = self.maps[position].map.len();
            // a tablet holding nothing gives its map and its slot back
            if held == 0 {
                let tablet = self.maps[position].tablet;
                let _ = self.take_tablet(tablet);
                continue;
            }
            // a map at a quarter of its room, and large enough that its buckets matter
            let map = &mut self.maps[position].map;
            if map.capacity() > SHRINK_FLOOR && held.saturating_mul(4) < map.capacity() {
                map.shrink_to(held.saturating_mul(2).max(SHRINK_FLOOR));
            }
        }
        // no tablet holds anything, so the index goes too
        if self.maps.is_empty() {
            self.slots = None;
            self.maps = Vec::new();
        }
    }

    /// The bytes this index holds, estimated from every map's capacity
    ///
    /// The tablets' index, the list of their maps, and each map's buckets: a bucket is its key,
    /// its value and a control byte, at hashbrown's load factor of 7/8.
    #[must_use]
    pub fn index_bytes(&self) -> usize {
        // the tablets' index, once anything was resident
        let slots = if self.slots.is_some() { SLOTS_BYTES } else { 0 };
        // the list of maps
        let list = self
            .maps
            .capacity()
            .saturating_mul(std::mem::size_of::<TabletPartitions<V>>());
        // every map's buckets
        let bucket = std::mem::size_of::<(u64, V)>() + 1;
        let buckets: usize = self
            .maps
            .iter()
            .map(|tablet| tablet.map.capacity().saturating_mul(bucket) / 7 * 8)
            .sum();
        slots + list + buckets
    }

    /// How many tablets have a map, for a test of what the index keeps
    #[cfg(test)]
    fn maps(&self) -> usize {
        self.maps.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::placement::TABLET_BITS;

    /// A key in a tablet, its low bits set apart by an index
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    /// * `index` - What sets keys of one tablet apart
    fn key(tablet: u16, index: u64) -> u64 {
        // the tablet in the top twelve bits, the index in the rest
        (u64::from(tablet) << (u64::BITS - TABLET_BITS)) | index
    }

    /// Every key is found under its own tablet's map, and every operation reaches it there
    #[test]
    fn every_key_lives_in_its_tablets_map() {
        let mut resident = ResidentPartitions::new();
        // keys spread over tablets at both ends of the key space and between
        let tablets = [0u16, 1, 7, 2048, 4095];
        for tablet in tablets {
            for index in 0..10 {
                assert!(resident.insert(key(tablet, index), index).is_none());
            }
        }
        // each found where it was put, and in its tablet's map alone
        for tablet in tablets {
            let map = resident
                .tablet_map(tablet)
                .expect("a tablet that was written");
            assert_eq!(map.len(), 10);
            for index in 0..10 {
                assert_eq!(resident.get(&key(tablet, index)), Some(&index));
                assert!(map.contains_key(&key(tablet, index)));
                assert_eq!(tablet_of(key(tablet, index)), usize::from(tablet));
            }
        }
        // a tablet never written has no map, and a key in it is not found
        assert!(resident.tablet_map(3).is_none());
        assert!(resident.get(&key(3, 0)).is_none());
        // an entry, a mutation and a removal reach the same place
        *resident.get_mut(&key(7, 3)).unwrap() = 70;
        assert_eq!(resident.get(&key(7, 3)), Some(&70));
        match resident.entry(key(7, 4)) {
            hash_map::Entry::Occupied(entry) => assert_eq!(*entry.get(), 4),
            hash_map::Entry::Vacant(_) => panic!("a held key was vacant"),
        }
        assert_eq!(resident.remove(&key(2048, 9)), Some(9));
        assert!(!resident.contains_key(&key(2048, 9)));
        // every key is iterated once
        assert_eq!(resident.iter().count(), 49);
        assert_eq!(resident.keys().count(), 49);
        assert_eq!(resident.values().count(), 49);
    }

    /// Taking a tablet repoints the tablet moved into its place, so none of its keys are lost
    #[test]
    fn an_emptied_tablet_gives_its_map_back() {
        let mut resident = ResidentPartitions::new();
        // three tablets, so taking the first moves the last into its place
        for tablet in [10u16, 20, 30] {
            for index in 0..5 {
                resident.insert(key(tablet, index), u64::from(tablet));
            }
        }
        let taken = resident.take_tablet(10).expect("a tablet that was written");
        assert_eq!(taken.len(), 5);
        assert_eq!(resident.maps(), 2);
        // the moved tablet is still found where its keys say
        for index in 0..5 {
            assert!(resident.get(&key(10, index)).is_none());
            assert_eq!(resident.get(&key(20, index)), Some(&20));
            assert_eq!(resident.get(&key(30, index)), Some(&30));
        }
        // and a tablet emptied by removals gives its map back at the next shrink
        for index in 0..5 {
            resident.remove(&key(20, index));
        }
        resident.shrink_if_sparse();
        assert_eq!(resident.maps(), 1);
        assert!(resident.tablet_map(20).is_none());
        assert_eq!(resident.get(&key(30, 4)), Some(&30));
        // and once every tablet is gone the index goes too
        let _ = resident.take_tablet(30);
        resident.shrink_if_sparse();
        assert_eq!(resident.index_bytes(), 0);
        // a tablet taken twice, or never written, gives nothing
        assert!(resident.take_tablet(30).is_none());
        assert!(resident.take_tablet(4095).is_none());
    }

    /// A tablet's map that grew and was mostly evicted is shrunk to twice what it holds
    #[test]
    fn a_sparse_tablet_map_is_shrunk() {
        let mut resident = ResidentPartitions::new();
        // grow one tablet's map well past the floor, and keep another small
        for index in 0..20_000 {
            resident.insert(key(5, index), index);
        }
        resident.insert(key(6, 0), 0);
        let grown = resident.tablet_map(5).unwrap().capacity();
        // evict all but a few, leaving the map at a sliver of its room
        for index in 100..20_000 {
            resident.remove(&key(5, index));
        }
        // a removal leaves its bucket a tombstone, so the map keeps nearly all of its room
        assert!(resident.tablet_map(5).unwrap().capacity() > grown / 2);
        resident.shrink_if_sparse();
        // shrunk to about twice what it holds, and nothing it holds is lost
        let shrunk = resident.tablet_map(5).unwrap().capacity();
        assert!(shrunk < grown / 10, "{shrunk} of {grown}");
        assert!(shrunk >= 200);
        for index in 0..100 {
            assert_eq!(resident.get(&key(5, index)), Some(&index));
        }
        // a map under the floor is left as it is
        let small = resident.tablet_map(6).unwrap().capacity();
        resident.shrink_if_sparse();
        assert_eq!(resident.tablet_map(6).unwrap().capacity(), small);
    }

    /// The counts are of partitions, so an entry left vacant does not make a table look full
    #[test]
    fn len_and_is_empty_count_partitions_not_maps() {
        let mut resident: ResidentPartitions<u64> = ResidentPartitions::new();
        assert!(resident.is_empty());
        assert_eq!(resident.len(), 0);
        // an entry looked at and left vacant makes a map and holds nothing
        assert!(matches!(
            resident.entry(key(9, 1)),
            hash_map::Entry::Vacant(_)
        ));
        assert_eq!(resident.maps(), 1);
        assert!(resident.is_empty());
        assert_eq!(resident.len(), 0);
        // partitions in two tablets count once each
        resident.insert(key(9, 1), 1);
        resident.insert(key(10, 1), 1);
        assert_eq!(resident.len(), 2);
        assert!(!resident.is_empty());
    }

    /// The bytes counted are the index's, the list's and every map's buckets
    #[test]
    fn index_bytes_counts_the_slots_and_every_map() {
        let mut resident: ResidentPartitions<[u8; 40]> = ResidentPartitions::new();
        // nothing resident costs nothing
        assert_eq!(resident.index_bytes(), 0);
        // one partition costs the index, the list and one small map
        resident.insert(key(1, 1), [0; 40]);
        let one = resident.index_bytes();
        assert!(one > SLOTS_BYTES, "{one}");
        // a thousand more in another tablet cost at least their buckets
        for index in 0..1000 {
            resident.insert(key(2, index), [0; 40]);
        }
        let bucket = std::mem::size_of::<(u64, [u8; 40])>() + 1;
        let many = resident.index_bytes();
        assert!(many >= one + 1000 * bucket, "{many} after {one}");
    }
}
