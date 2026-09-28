//! The tablets whose copy on this node is installing a snapshot, which every shard routes around
//!
//! An install takes its group's copy out of service until it ends: what is resident is the old
//! generation and what is on disk is half of the new one, so the copy answers no read
//! ([F43](../../../docs/src/features/node-recovery.md)). A shard routes a query for a tablet this
//! node holds to its own copy whatever that copy's state, since the read ring is built from the
//! map and an install is one shard's own business. So after a partition longer than its peers'
//! retention, every read through the node catching up was refused for each group it installed
//! ([#184](../../../docs/src/appendix/resolved/installing-copy-reads-elsewhere.md)).
//!
//! The shard installing a group counts the group's tablets here while the install runs, and
//! every shard on the node reads the counts when it routes: a tablet counted is sent to another
//! holder that is up, as a tablet this node holds no copy of is. The counts are shared by the
//! node's shards through an `Arc`, with a generation a shard compares against the one it last
//! routed under, so a shard rebuilds its routes only when an install began or ended.

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use crate::server::ring::TABLET_COUNT;

/// How many installs on this node cover each tablet, and a generation that moves with them
#[derive(Debug)]
pub struct InstallingTablets {
    /// Installs running over each tablet, indexed by tablet
    counts: Vec<AtomicU16>,
    /// Moved by every begin and end, so a shard knows when to route again
    generation: AtomicU64,
}

impl Default for InstallingTablets {
    /// No tablet installing, at generation zero
    fn default() -> Self {
        InstallingTablets {
            counts: (0..TABLET_COUNT).map(|_| AtomicU16::new(0)).collect(),
            generation: AtomicU64::new(0),
        }
    }
}

impl InstallingTablets {
    /// Count an install over these tablets
    ///
    /// # Arguments
    ///
    /// * `tablets` - The installing group's tablets
    pub fn begin(&self, tablets: &[u16]) {
        // every tablet counted before the generation moves, so a shard that sees the new
        // generation sees every count behind it
        for tablet in tablets {
            if let Some(count) = self.counts.get(usize::from(*tablet)) {
                count.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Stop counting an install over these tablets
    ///
    /// # Arguments
    ///
    /// * `tablets` - The group's tablets, as `begin` was given them
    pub fn end(&self, tablets: &[u16]) {
        // a count never goes below zero, whatever an unmatched end says
        for tablet in tablets {
            if let Some(count) = self.counts.get(usize::from(*tablet)) {
                let _ = count.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    Some(count.saturating_sub(1))
                });
            }
        }
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// The generation the counts are at
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Every tablet an install covers now
    #[must_use]
    pub fn tablets(&self) -> Vec<usize> {
        self.counts
            .iter()
            .enumerate()
            .filter(|(_, count)| count.load(Ordering::Acquire) > 0)
            .map(|(tablet, _)| tablet)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Installs over a tablet are counted, so it is clear only when the last one ends, and every
    /// begin and end moves the generation
    #[test]
    fn installs_are_counted_per_tablet() {
        let installing = InstallingTablets::default();
        assert!(installing.tablets().is_empty());
        let start = installing.generation();
        // two groups over one shared tablet
        installing.begin(&[3, 4]);
        installing.begin(&[4, 5]);
        assert_eq!(installing.tablets(), vec![3, 4, 5]);
        installing.end(&[3, 4]);
        assert_eq!(installing.tablets(), vec![4, 5]);
        installing.end(&[4, 5]);
        assert!(installing.tablets().is_empty());
        assert_eq!(installing.generation(), start + 4);
        // an end with no begin leaves nothing below zero
        installing.end(&[9]);
        installing.begin(&[9]);
        assert_eq!(installing.tablets(), vec![9]);
    }
}
