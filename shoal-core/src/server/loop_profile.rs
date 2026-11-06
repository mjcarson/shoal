//! What a shard's loop is held by, and how long what waits for it waits: an attribution build's
//! figures, behind `shoal-core`'s `loop-profile` feature and never in a product build
//! ([O140](../../../docs/src/appendix/optimizations.md#o140-a-resident-reads-p99-beside-the-cells-inserts-is-six-times-scylladbs-on-the-same-hosts))
//!
//! A shard's loop takes one message at a time, and between two messages it yields to its
//! executor's other tasks, runs its second inbox's steps, sweeps its segments, flushes its tables'
//! staged writes and checks its memory. A resident get that arrives behind any of these waits for
//! all of it. The loop marks the clock at the end of each of these and of each message, counting
//! the time since its last mark against what it just did, and counts how long a client's bundle
//! waited from its last byte off the socket, and a routed query from its routing, to be taken.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::shared::protocol::stats::{LoopHistogram, ShardLoopProfile};

/// One shard's loop profile: a histogram a kind, by its name
#[derive(Debug, Default)]
pub struct LoopProfile {
    /// Each kind's histogram
    kinds: BTreeMap<&'static str, LoopHistogram>,
}

impl LoopProfile {
    /// Count the time since the last mark against a kind, and mark the clock again
    ///
    /// # Arguments
    ///
    /// * `kind` - What held the loop since the last mark
    /// * `at` - The last mark, moved to now
    pub fn mark(&mut self, kind: &'static str, at: &mut Instant) {
        let now = Instant::now();
        self.count(kind, now.saturating_duration_since(*at));
        *at = now;
    }

    /// Count one duration against a kind
    ///
    /// # Arguments
    ///
    /// * `kind` - What it was
    /// * `took` - How long
    pub fn count(&mut self, kind: &'static str, took: Duration) {
        let us = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        self.kinds.entry(kind).or_default().record(us);
    }

    /// Count a value that is not a time against a kind, as though it were microseconds
    ///
    /// For the sizes beside a hold, a group or an entry count, whose kind says so by its `count:`
    /// prefix (O159).
    ///
    /// # Arguments
    ///
    /// * `kind` - What was counted, named `count:...`
    /// * `value` - How many
    pub fn count_value(&mut self, kind: &'static str, value: u64) {
        self.kinds.entry(kind).or_default().record(value);
    }

    /// What this shard's loop has counted so far, for its report
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard
    #[must_use]
    pub fn figures(&self, shard: usize) -> ShardLoopProfile {
        ShardLoopProfile {
            shard: u32::try_from(shard).unwrap_or(u32::MAX),
            kinds: self
                .kinds
                .iter()
                .map(|(kind, histogram)| ((*kind).to_string(), histogram.clone()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LoopProfile;
    use std::time::{Duration, Instant};

    /// A mark counts the time since the last against its kind, in the bucket of its bit length
    #[test]
    fn a_mark_counts_the_time_since_the_last_against_its_kind() {
        let mut profile = LoopProfile::default();
        let mut at = Instant::now() - Duration::from_micros(300);
        profile.mark("message:Client", &mut at);
        profile.count("wait:client", Duration::from_micros(5));
        let figures = profile.figures(3);
        assert_eq!(figures.shard, 3);
        let held = &figures.kinds["message:Client"];
        assert_eq!(held.count, 1);
        assert!(held.total_us >= 300, "{held:?}");
        // 300 µs has a bit length of 9, and a slow clock read may carry it past 512 into the next
        assert_eq!(held.buckets[9] + held.buckets[10], 1, "{held:?}");
        let waited = &figures.kinds["wait:client"];
        assert_eq!(waited.buckets[3], 1, "5 µs has a bit length of 3");
    }
}
