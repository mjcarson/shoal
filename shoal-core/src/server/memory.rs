//! A node's rows against its resident floor
//!
//! A node judges its budget against the process's resident set
//! ([Resolved #149](../../../docs/src/appendix/resolved/node-memory-budget.md)), since almost nothing
//! a node holds is rows. Some of what it holds is no row's and never leaves: its executors and their
//! rings, its groups, its indexes, the allocator's pages. That is the node's **floor**, and a node
//! whose `node_memory` is below it is past its budget for good: every shard once evicted two fifths
//! of its rows at every check, four times a second, and the node stayed over, reading back from disk
//! what it had just evicted ([item 338](../../../docs/src/appendix/resolved/resident-floor-reserve.md)).
//!
//! So every shard keeps what its eviction list names summed ([`EvictionList`]) and publishes it with
//! its rows ([`NodeRows`]), and a check that finds the process past the node's memory asks first
//! whether evicting everything evictable on the node could bring it under ([`judge`]). Where it
//! could, the shard evicts as #149 has it. Where it could not, the node is past its floor: the check
//! is counted and warned of, and each shard holds its evictable rows to a reserve, an eighth of its
//! budget ([`ROW_RESERVE_SHARE`]), evicting only what passes it. A row past its last archive is never
//! evictable, so it is judged as part of the floor: judged as a row, a shard whose unarchived rows
//! alone passed its reserve evicted every row the foreground read back at every check.

use std::hash::{BuildHasherDefault, Hash};
use std::sync::atomic::{AtomicU64, Ordering};

use gxhash::GxHasher;
use lru::LruCache;
use tracing::{event, Level};

use crate::server::objects::inflight::{Throttle, WARN_EVERY};

/// A shard's evictable partitions, least recently used first, and what they hold together
///
/// The partitions a pass may evict, each by the size its table charged it when it was marked
/// evictable, kept with their sum so a check of the node's memory reads what eviction could free
/// without walking the list: a shard's rows past their last archive are no row eviction can free,
/// and judged as rows they once had a node past its floor evict every row the foreground read back
/// at every check ([item 338](../../../docs/src/appendix/resolved/resident-floor-reserve.md)).
#[derive(Debug)]
pub struct EvictionList<K: Hash + Eq> {
    /// The evictable partitions, by their keys, each with its charged size
    lru: LruCache<K, usize, BuildHasherDefault<GxHasher>>,
    /// The sizes of every partition the list names, summed
    bytes: usize,
}

impl<K: Hash + Eq> EvictionList<K> {
    /// An empty list with no bound on its length
    #[must_use]
    pub fn unbounded() -> Self {
        // gxhash keys, as every shard's map of partitions has them
        EvictionList {
            lru: LruCache::unbounded_with_hasher(BuildHasherDefault::<GxHasher>::default()),
            bytes: 0,
        }
    }

    /// Name a partition evictable at a size, the most recently used, and say the size it had if it
    /// was named already
    ///
    /// # Arguments
    ///
    /// * `key` - The partition, by its table and key
    /// * `size` - Its size as its table charges it
    pub fn put(&mut self, key: K, size: usize) -> Option<usize> {
        // a partition named again replaces its old size in the sum
        let old = self.lru.put(key, size);
        self.bytes = self.bytes.saturating_sub(old.unwrap_or(0)).saturating_add(size);
        old
    }

    /// Take a partition out of the list, and say its size if it was in it
    ///
    /// # Arguments
    ///
    /// * `key` - The partition, by its table and key
    pub fn pop(&mut self, key: &K) -> Option<usize> {
        // its size leaves the sum with it
        let size = self.lru.pop(key);
        self.bytes = self.bytes.saturating_sub(size.unwrap_or(0));
        size
    }

    /// Make a partition the most recently used, if the list names it; its size stays as it was
    ///
    /// # Arguments
    ///
    /// * `key` - The partition, by its table and key
    pub fn promote(&mut self, key: &K) {
        // the order moves and the sum does not
        self.lru.promote(key);
    }

    /// Take the least recently used partition out of the list, with its size
    pub fn pop_lru(&mut self) -> Option<(K, usize)> {
        // its size leaves the sum with it
        let popped = self.lru.pop_lru();
        if let Some((_, size)) = &popped {
            self.bytes = self.bytes.saturating_sub(*size);
        }
        popped
    }

    /// How many partitions the list names
    #[must_use]
    pub fn len(&self) -> usize {
        self.lru.len()
    }

    /// Whether the list names no partition
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lru.is_empty()
    }

    /// What every partition the list names holds together, as their tables charged them
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// The share of its row budget a shard keeps past its node's floor: an eighth
///
/// A node past its floor cannot get under its memory by evicting rows, so whatever it keeps is
/// over the budget already; the reserve bounds that overshoot to an eighth of the rows' budgets
/// while leaving the shards a cache of their hottest rows, rather than none.
pub const ROW_RESERVE_SHARE: usize = 8;

/// One shard's rows, on a cache line of its own, so a shard's publish touches no other's line
#[derive(Debug, Default)]
#[repr(align(128))]
struct Slot {
    /// Every row the shard counts and its eviction list, in bytes
    rows: AtomicU64,
    /// What of them eviction could free: the partitions its eviction list names and the list
    evictable: AtomicU64,
}

/// What every shard of a node holds in rows, as each last published it
///
/// A shard publishes at every check of its node's memory, at most every quarter second, and before
/// it reports itself ready, so a check reads the node's rows at most a check old and the node's
/// readiness reads them as its shards started. Two figures a shard: every row it counts, which the
/// node's readiness takes out of its floor, and what eviction could free of them, which a check
/// judges by: a row past its last archive is held whatever is evicted.
#[derive(Debug)]
pub struct NodeRows {
    /// Each shard's rows and what of them eviction could free, one slot a shard
    slots: Box<[Slot]>,
}

impl NodeRows {
    /// A board of a node's shards, each holding nothing yet
    ///
    /// # Arguments
    ///
    /// * `shards` - How many shards the node runs
    #[must_use]
    pub fn new(shards: usize) -> Self {
        // one slot a shard, each its own cache line
        let slots = (0..shards.max(1)).map(|_| Slot::default()).collect();
        NodeRows { slots }
    }

    /// Publish what one shard's rows hold
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard, by its id; one past the board is ignored
    /// * `rows` - Every row it counts and its eviction list, in bytes
    /// * `evictable` - What of them eviction could free, in bytes
    pub fn publish(&self, shard: usize, rows: usize, evictable: usize) {
        // a shard the board was not made for has nowhere to write
        if let Some(slot) = self.slots.get(shard) {
            slot.rows.store(u64::try_from(rows).unwrap_or(u64::MAX), Ordering::Relaxed);
            slot.evictable.store(u64::try_from(evictable).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
    }

    /// Every row every shard counts, together, as each last published them
    #[must_use]
    pub fn rows(&self) -> u64 {
        // every slot, saturating rather than wrapping
        self.slots
            .iter()
            .fold(0u64, |total, slot| total.saturating_add(slot.rows.load(Ordering::Relaxed)))
    }

    /// What eviction could free of every shard's rows, together, as each last published it
    #[must_use]
    pub fn evictable(&self) -> u64 {
        // every slot, saturating rather than wrapping
        self.slots
            .iter()
            .fold(0u64, |total, slot| total.saturating_add(slot.evictable.load(Ordering::Relaxed)))
    }
}

/// What one check of the node's memory decided for one shard
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Judgement {
    /// The process is inside the node's memory
    Under,
    /// Past it by no more than eviction could free of the node's rows, so evicting can bring it
    /// under: the shard evicts as #149 has it
    Evict,
    /// Past it by more than eviction could free of every row of the node: the node is past its
    /// floor, and the shard evicts only when what it could free is above its reserve
    Floor {
        /// Whether the shard's rows are above its reserve
        evict: bool,
    },
}

/// The rows a shard keeps past its node's floor, out of its row budget
///
/// # Arguments
///
/// * `budget` - The shard's row budget
#[must_use]
pub fn reserve(budget: usize) -> usize {
    // an eighth of what the shard may hold
    budget / ROW_RESERVE_SHARE
}

/// What a pass past the node's floor evicts: what the shard's evictable rows hold above three fifths
/// of its reserve, so the pass after it waits until they pass the reserve again, as a budget's pass
/// leaves three fifths of its rows
///
/// # Arguments
///
/// * `evictable` - What the shard's eviction list names, as their tables charged them
/// * `reserve` - The evictable rows the shard keeps past the floor
#[must_use]
pub fn floor_need(evictable: usize, reserve: usize) -> usize {
    // down to three fifths of the reserve, and nothing of a list already under it
    evictable.saturating_sub(reserve / 5 * 3)
}

/// Judge one check of the node's memory for one shard
///
/// # Arguments
///
/// * `resident` - The process's resident bytes, as the kernel counts them
/// * `node_memory` - The node's memory budget
/// * `node_rows` - What eviction could free of every shard's rows, as last published
/// * `shard_rows` - What eviction could free of this shard's rows
/// * `reserve` - The evictable rows this shard keeps past the node's floor
#[must_use]
pub fn judge(resident: usize, node_memory: usize, node_rows: u64, shard_rows: usize, reserve: usize) -> Judgement {
    // inside its memory, nothing to judge
    let Some(excess) = resident.checked_sub(node_memory).filter(|excess| *excess > 0) else {
        return Judgement::Under;
    };
    // evicting every evictable row of the node would free what it holds: enough, and the pass is
    // worth it
    if node_rows >= u64::try_from(excess).unwrap_or(u64::MAX) {
        return Judgement::Evict;
    }
    // not enough: the node is past its floor, and the shard keeps its reserve
    Judgement::Floor {
        evict: shard_rows > reserve,
    }
}

/// Warn that the node is past its floor, at most once every [`WARN_EVERY`] across its shards
static FLOOR_WARNINGS: Throttle = Throttle::new();

/// Warn that a check found the node past its floor, unless one did lately
///
/// # Arguments
///
/// * `resident` - The process's resident bytes
/// * `node_memory` - The node's memory budget
/// * `node_rows` - What eviction could free of every shard's rows
/// * `reserve` - The evictable rows the warning shard keeps past the floor
pub fn warn_floor(resident: usize, node_memory: usize, node_rows: u64, reserve: usize) {
    // one a quiet spell, and a count of the checks held back since
    let Some(held) = FLOOR_WARNINGS.pass(WARN_EVERY) else {
        return;
    };
    // what holds the node over its memory once every row it could evict is gone
    let floor = u64::try_from(resident).unwrap_or(u64::MAX).saturating_sub(node_rows);
    event!(
        Level::WARN,
        msg = "this node is past its memory by more than its rows hold, so evicting rows cannot bring it under: \
               its shards keep their rows to a reserve until it is under; raise resources.node_memory past the floor",
        resident,
        node_memory,
        node_rows,
        floor,
        reserve_a_shard = reserve,
        checks_held_back = held,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node past its memory by more than eviction could free is past its floor, and a shard there
    /// evicts only while its rows are above its reserve: never at a check that finds them under it,
    /// which is what every check did before ([item 338](../../../docs/src/appendix/resolved/resident-floor-reserve.md))
    #[test]
    fn a_node_past_its_floor_evicts_only_above_its_reserve() {
        // a node of 1 GiB holding 1.6 GiB with 100 MiB of rows: 600 MiB over, the rows a sixth of it
        let (gib, mib) = (1usize << 30, 1usize << 20);
        let resident = gib + 600 * mib;
        let rows = 100 * mib as u64;
        // a shard of 20 MiB of rows keeps them past a reserve of 30
        assert_eq!(judge(resident, gib, rows, 20 * mib, 30 * mib), Judgement::Floor { evict: false });
        // and one of 40 evicts what passes it
        assert_eq!(judge(resident, gib, rows, 40 * mib, 30 * mib), Judgement::Floor { evict: true });
        // a node with no rows at all is past its floor too, and evicts nothing
        assert_eq!(judge(resident, gib, 0, 0, 30 * mib), Judgement::Floor { evict: false });
        // the reserve is an eighth of a shard's budget, and a pass there leaves three fifths of it
        assert_eq!(reserve(160 * mib), 20 * mib);
        assert_eq!(floor_need(40 * mib, 20 * mib), 28 * mib);
        assert_eq!(floor_need(10 * mib, 20 * mib), 0);
    }

    /// A node its rows can bring back under its memory is judged as #149 judged it, and a node
    /// inside its memory not at all
    #[test]
    fn a_node_its_rows_can_bring_under_is_judged_as_before() {
        // 1 GiB of budget, 50 MiB over, 100 MiB of rows: every shard evicts, whatever its share
        let (gib, mib) = (1usize << 30, 1usize << 20);
        assert_eq!(judge(gib + 50 * mib, gib, 100 * mib as u64, 0, 30 * mib), Judgement::Evict);
        // over by exactly what the rows hold is still worth the pass
        assert_eq!(judge(gib + 100 * mib, gib, 100 * mib as u64, 10, 30 * mib), Judgement::Evict);
        // at the budget or under it, nothing
        assert_eq!(judge(gib, gib, 0, 40 * mib, 30 * mib), Judgement::Under);
        assert_eq!(judge(gib - mib, gib, 0, 40 * mib, 30 * mib), Judgement::Under);
    }

    /// The board sums every shard's rows, and what of them eviction could free, as each last
    /// published them, and ignores a shard it was not made for
    #[test]
    fn a_nodes_rows_are_every_shards_last_published() {
        // three shards, each publishing, one twice
        let board = NodeRows::new(3);
        board.publish(0, 10, 4);
        board.publish(1, 20, 20);
        board.publish(2, 30, 0);
        board.publish(1, 5, 1);
        assert_eq!((board.rows(), board.evictable()), (45, 5));
        // a shard past the board writes nowhere
        board.publish(7, 1_000, 1_000);
        assert_eq!((board.rows(), board.evictable()), (45, 5));
    }

    /// The eviction list keeps the sum of what it names through every way in and out: a partition
    /// named again counts once at its new size, and one taken out by key or as the least recently
    /// used takes its size with it
    #[test]
    fn an_eviction_lists_bytes_follow_what_it_names() {
        let mut list = EvictionList::<(u8, u64)>::unbounded();
        assert_eq!(list.put((0, 1), 100), None);
        assert_eq!(list.put((0, 2), 50), None);
        // named again, at its new size
        assert_eq!(list.put((0, 1), 70), Some(100));
        assert_eq!((list.len(), list.bytes()), (2, 120));
        // out by key, and a key it never named takes nothing
        assert_eq!(list.pop(&(0, 2)), Some(50));
        assert_eq!(list.pop(&(0, 9)), None);
        assert_eq!(list.bytes(), 70);
        // and the least recently used, with its size
        assert_eq!(list.pop_lru(), Some(((0, 1), 70)));
        assert!(list.is_empty());
        assert_eq!(list.bytes(), 0);
    }
}
