//! The clock the stage profiler reads
//!
//! These two types are the only part of stage profiling that both peers touch: the server
//! stamps a query through its stages, and the client stamps when the response came off the
//! socket. They live here rather than beside the rest of the profiler so that a client can
//! hold a stamp without linking an engine - see `crate::client::ClientStamps` in `shoal-client`.
//!
//! Both are `#[inline]`, because after F15 every caller is in another crate and this workspace
//! builds without LTO. See `docs/src/features/client-server-split.md`.

use std::time::Instant;

/// One clock reading
///
/// Every stamp goes through this rather than through [`Instant`] directly, so swapping the
/// clock for a TSC backed one later is a change to this file alone. Only ever meaningful as a
/// difference against another stamp from the same clock.
///
/// On Linux x86-64 this is a vDSO `CLOCK_MONOTONIC` read of roughly twenty nanoseconds, and
/// `CLOCK_MONOTONIC` is system wide, so readings taken on different threads are comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp(Instant);

impl Stamp {
    /// Read the clock now
    #[must_use]
    #[inline]
    pub fn now() -> Self {
        Stamp(Instant::now())
    }

    /// Get the nanoseconds between an earlier stamp and this one
    ///
    /// Saturates rather than wrapping, since a wrapped duration reads as a fast query.
    ///
    /// # Arguments
    ///
    /// * `earlier` - The stamp to measure from
    #[must_use]
    #[inline]
    pub fn since(self, earlier: Stamp) -> u64 {
        // a later stamp can never precede an earlier one on a monotonic clock, but
        // saturating here means a clock that misbehaves reports zero rather than a
        // nonsensically huge interval
        self.0
            .saturating_duration_since(earlier.0)
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    /// Get the underlying instant, for joining against a client side clock
    #[must_use]
    #[inline]
    pub fn into_inner(self) -> Instant {
        self.0
    }

    /// Get the stamp a number of nanoseconds after this one
    ///
    /// A record stores offsets rather than stamps, so this is what turns one back into an
    /// absolute reading that can be differenced against a client side one.
    ///
    /// # Arguments
    ///
    /// * `nanos` - How far past this stamp to move
    #[must_use]
    #[inline]
    pub fn plus_nanos(self, nanos: u64) -> Self {
        Stamp(self.0 + std::time::Duration::from_nanos(nanos))
    }

    /// Get the stamp a number of nanoseconds before this one
    ///
    /// # Arguments
    ///
    /// * `nanos` - How far before this stamp to move
    #[must_use]
    #[inline]
    pub fn minus_nanos(self, nanos: u64) -> Self {
        Stamp(self.0 - std::time::Duration::from_nanos(nanos))
    }

    /// Measure what a single clock reading costs
    ///
    /// Several stages here are queue hops of tens of nanoseconds, which is the same order as
    /// the two clock reads they are differenced from. A report that prints those as numbers
    /// is reporting the cost of its own instrument, so the report needs this value in order
    /// to mark them as being at the floor instead.
    ///
    /// # Arguments
    ///
    /// * `samples` - How many back to back readings to take
    #[must_use]
    #[inline]
    pub fn measure_overhead(samples: usize) -> u64 {
        // a measurement of nothing tells us nothing
        if samples == 0 {
            return 0;
        }
        // take back to back readings and keep how far the clock moved between each pair
        let mut deltas = Vec::with_capacity(samples);
        let mut prior = Stamp::now();
        for _ in 0..samples {
            // read the clock again and record the gap
            let next = Stamp::now();
            deltas.push(next.since(prior));
            prior = next;
        }
        // take the median rather than the mean, since one preemption in the middle of this
        // loop would drag a mean up by orders of magnitude
        deltas.sort_unstable();
        deltas[deltas.len() / 2]
    }
}

/// A stage offset that may not have been reached
///
/// Nanoseconds from a record's base stamp. A get never reaches the durability stages and a
/// query whose partitions were all resident never reaches the load ones, so unset is the
/// normal case — and it cannot be spelled zero, because a stage really can land in the same
/// nanosecond as the base.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(u32);

impl Offset {
    /// This stage was never reached
    pub const UNSET: Offset = Offset(u32::MAX);

    /// This stage landed further from the base than a `u32` of nanoseconds can hold
    ///
    /// Kept apart from [`Offset::UNSET`] so a report can drop the record and say it did,
    /// rather than print a wrapped number as a fast query. The limit is about 4.29 seconds,
    /// which no healthy query comes near and a pathological one can exceed.
    pub const SATURATED: Offset = Offset(u32::MAX - 1);

    /// Build an offset from a base stamp and a later one
    ///
    /// # Arguments
    ///
    /// * `base` - The stamp this offset is measured from
    /// * `at` - The stamp this stage was reached at
    #[must_use]
    #[inline]
    pub fn between(base: Stamp, at: Stamp) -> Self {
        // measure how far past our base this stage landed
        let nanos = at.since(base);
        // anything that does not fit is saturated rather than truncated, since a truncated
        // offset is indistinguishable from a genuinely fast stage
        if nanos >= u64::from(Offset::SATURATED.0) {
            Offset::SATURATED
        } else {
            // this cast cannot lose anything, we just checked the range
            Offset(nanos as u32)
        }
    }

    /// Check whether this stage was ever reached
    #[must_use]
    #[inline]
    pub fn is_set(self) -> bool {
        self != Offset::UNSET
    }

    /// Check whether this offset ran past what it can represent
    #[must_use]
    #[inline]
    pub fn is_saturated(self) -> bool {
        self == Offset::SATURATED
    }

    /// Get this offset in nanoseconds if it holds a real measurement
    #[must_use]
    #[inline]
    pub fn nanos(self) -> Option<u32> {
        // neither sentinel is a measurement, so neither answers this
        if self.is_set() && !self.is_saturated() {
            Some(self.0)
        } else {
            None
        }
    }
}

impl Default for Offset {
    fn default() -> Self {
        // a stage nothing has reached yet is unset, not zero
        Offset::UNSET
    }
}
