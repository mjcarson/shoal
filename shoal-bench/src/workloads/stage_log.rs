//! The client half of a run's stage records, gathered the same way by every driver
//!
//! # Why this is a type rather than a block of code in the driver
//!
//! It used to be a block of code in the driver, and only in one of them.
//! [`drive_with`](crate::workloads::harness::driver::drive_with) kept a map of submitted queries,
//! filled it as each bundle went out and closed each record out as its response came back. Every
//! other driver sends one query at a time through `send_one` and had none of that, so a workload
//! whose measured phase used one of them produced a stage report with the right schema, the right
//! workload name, a plausible join block and no data in it at all
//! ([Resolved #76](../../../../docs/src/appendix/resolved/stage-join.md)).
//!
//! Copying the block into the driver that was missing it would have fixed the three arms that were
//! broken and left the next driver to be written with the same hole. So the bookkeeping lives here
//! and the drivers call it.
//!
//! # It costs nothing when nobody is profiling
//!
//! [`StageLog`] is declared twice, the same way
//! [`StageStamps`](shoal::server::stage_profile::StageStamps) and `ClientStamps` are: the real
//! one under the `stage-profile` feature and a zero sized one without it, whose methods are all
//! empty. That is what lets [`Measurement`](crate::workloads::workload::Measurement) hold one
//! unconditionally and every driver call into it with no `#[cfg]` of its own — which is the
//! property that stops the next driver from being written without it.

#[cfg(feature = "stage-profile")]
use std::collections::HashMap;

#[cfg(feature = "stage-profile")]
use uuid::Uuid;

use shoal::client::messages::BatchStamps;

use crate::workloads::schema::BenchClient;

/// One response, as a driver hands it to the log
///
/// Aliased so the signatures below say what they mean rather than repeating the schema parameter.
type Response = shoal::ShoalResponse<BenchClient>;

/// The client half of every sampled query this run has sent
///
/// A record is opened when its query goes out and closed when its response comes back. The two
/// happen in one call on the one shot path, where a bundle holds a single query, and in two on the
/// streaming path, where a bundle holds a hundred of them and they are answered in any order.
#[cfg(feature = "stage-profile")]
#[derive(Debug)]
pub struct StageLog {
    /// How many queries this run keeps one record for
    ///
    /// Read once at construction rather than per query, since it comes out of the environment.
    sample: usize,
    /// The records whose responses have not come back yet, keyed the way the report joins
    pending: HashMap<(Uuid, usize), crate::workloads::stages::ClientRecord>,
    /// The records whose responses have come back, ready to be joined
    done: Vec<crate::workloads::stages::ClientRecord>,
}

/// The client half of every sampled query, which this build does not record
#[cfg(not(feature = "stage-profile"))]
#[derive(Debug, Default)]
pub struct StageLog;

// this sits on every `Measurement`, and every driver calls into it once per query, so it is only
// acceptable as an unconditional field if it really is free when off
#[cfg(not(feature = "stage-profile"))]
const _: () = assert!(
    std::mem::size_of::<StageLog>() == 0,
    "StageLog must be zero sized when the stage-profile feature is off"
);

#[cfg(feature = "stage-profile")]
impl Default for StageLog {
    /// Opens a log that keeps whatever share of queries this run was configured for
    ///
    /// Written out rather than derived: a derived one would leave the sample rate at zero, and
    /// the rate is a divisor.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "stage-profile")]
impl StageLog {
    /// Opens a log that keeps whatever share of queries this run was configured for
    #[must_use]
    pub fn new() -> Self {
        // the same rule the server samples by, read from the same environment variable. the two
        // have to agree or each half keeps a disjoint set and the join finds nothing
        Self::with_sample(crate::workloads::stages::sample_rate())
    }

    /// Opens a log that keeps one query in every `sample`
    ///
    /// Split out from [`new`](Self::new) so a test can name a rate rather than setting the
    /// environment variable the whole process shares - two tests doing that at once would each
    /// see the other's rate.
    ///
    /// # Arguments
    ///
    /// * `sample` - How many queries to keep one record for
    #[must_use]
    fn with_sample(sample: usize) -> Self {
        StageLog {
            // a rate of zero would divide by zero below, and means "keep everything" everywhere
            // else it is read
            sample: sample.max(1),
            pending: HashMap::new(),
            done: Vec::new(),
        }
    }

    /// Opens a record for every sampled query in a bundle that has just been sent
    ///
    /// Every stage in `stamps` was paid once for the whole bundle and is shared by every query in
    /// it, which is why the report labels them as batch level rather than folding them in with the
    /// per query stages.
    ///
    /// # Arguments
    ///
    /// * `id` - The stream this bundle went out on
    /// * `base` - The index of the first query in the bundle
    /// * `count` - How many queries the bundle held
    /// * `stamps` - What sending the bundle cost
    pub fn sent(&mut self, id: Uuid, base: usize, count: usize, stamps: BatchStamps) {
        // open one record per query in this bundle
        for offset in 0..count {
            let index = base + offset;
            // sample on the index, so the server keeps the same queries and the two halves still
            // have something to join on
            if index % self.sample != 0 {
                continue;
            }
            self.pending.insert(
                (id, index),
                crate::workloads::stages::ClientRecord {
                    id,
                    index,
                    submitted: stamps.entered,
                    serialized: stamps.serialized,
                    pooled: stamps.pooled,
                    written: stamps.written,
                    // filled in when this query's response comes back
                    arrived: stamps.written,
                },
            );
        }
    }

    /// Closes out the record a response belongs to, if this run kept one for it
    ///
    /// The arrival stamp comes off the response itself rather than being read here, so a response
    /// that waited in a channel is charged for that wait rather than having it hidden in the gap
    /// between the socket and the driver picking it up.
    ///
    /// # Arguments
    ///
    /// * `response` - The response that just came back
    pub fn answered(&mut self, response: &Response) {
        // a response the log never opened a record for is one this run did not sample
        let key = (response.get_query_id(), response.get_index());
        if let Some(mut record) = self.pending.remove(&key) {
            record.arrived = response.stamps().arrived();
            self.done.push(record);
        }
    }

    /// Records one query that was sent and answered on the one shot path
    ///
    /// A bundle of one is opened and closed in the same breath, so there is nothing to hold in
    /// [`pending`](Self::pending) between the two. The key comes off the response because that is
    /// the only place it exists: `send_one` mints the bundle's id inside the send and hands back
    /// no stream to read it from.
    ///
    /// **The sample rate does not apply here**, and cannot: a one query bundle's index is always
    /// zero, so every query on this path satisfies the rule both halves sample by. The server
    /// keeps all of them too, which is what matters — the two halves still agree.
    ///
    /// # Arguments
    ///
    /// * `stamps` - What sending the query cost
    /// * `response` - The response it came back with
    pub fn one(&mut self, stamps: BatchStamps, response: &Response) {
        self.done.push(crate::workloads::stages::ClientRecord {
            id: response.get_query_id(),
            index: response.get_index(),
            submitted: stamps.entered,
            serialized: stamps.serialized,
            pooled: stamps.pooled,
            written: stamps.written,
            arrived: response.stamps().arrived(),
        });
    }

    /// Takes in everything another log gathered
    ///
    /// The per query drivers give each of their slots its own measurement and pool them at the
    /// end, so a run's records arrive in as many pieces as it had slots.
    ///
    /// # Arguments
    ///
    /// * `other` - The log to absorb
    pub fn absorb(&mut self, other: StageLog) {
        self.done.extend(other.done);
        self.pending.extend(other.pending);
    }

    /// The records ready to be joined against the server's half
    #[must_use]
    pub fn records(&self) -> &[crate::workloads::stages::ClientRecord] {
        &self.done
    }

    /// How many queries went out and were never answered
    ///
    /// Reported rather than dropped. A record left open is a query the driver sent and never saw a
    /// response for, and the server's half of it will turn up in the report as a server only
    /// record - which reads like a join that failed rather than like a response that never came.
    #[must_use]
    pub fn unanswered(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(not(feature = "stage-profile"))]
impl StageLog {
    /// Opens a log, which this build has nothing to put in
    #[inline(always)]
    #[must_use]
    pub fn new() -> Self {
        StageLog
    }

    /// Opens a record per query in a bundle, which this build does not record
    ///
    /// # Arguments
    ///
    /// * `id` - Unused
    /// * `base` - Unused
    /// * `count` - Unused
    /// * `stamps` - Unused
    #[inline(always)]
    pub fn sent(&mut self, id: uuid::Uuid, base: usize, count: usize, stamps: BatchStamps) {
        let _ = (id, base, count, stamps);
    }

    /// Closes out a response's record, which this build does not record
    ///
    /// # Arguments
    ///
    /// * `response` - Unused
    #[inline(always)]
    pub fn answered(&mut self, response: &Response) {
        let _ = response;
    }

    /// Records one query sent on the one shot path, which this build does not record
    ///
    /// # Arguments
    ///
    /// * `stamps` - Unused
    /// * `response` - Unused
    #[inline(always)]
    pub fn one(&mut self, stamps: BatchStamps, response: &Response) {
        let _ = (stamps, response);
    }

    /// Takes in another log, which holds nothing in this build
    ///
    /// # Arguments
    ///
    /// * `other` - Unused
    #[inline(always)]
    pub fn absorb(&mut self, other: StageLog) {
        let _ = other;
    }

    /// How many queries went out and were never answered, which this build does not track
    #[inline(always)]
    #[must_use]
    pub fn unanswered(&self) -> usize {
        0
    }
}

#[cfg(all(test, feature = "stage-profile"))]
mod tests {
    use super::StageLog;
    use shoal::client::messages::BatchStamps;
    use shoal::uuid::Uuid;

    /// A bundle that was sent and never answered is counted rather than vanishing
    ///
    /// The block this type replaced dropped its unanswered records on the floor when the driver
    /// returned, so a run that lost half its responses produced a report with half the joins it
    /// should have had and nothing anywhere saying why.
    #[test]
    fn an_unanswered_record_is_counted() {
        let mut log = StageLog::new();
        log.sent(Uuid::new_v4(), 0, 4, BatchStamps::entered_now());
        assert_eq!(log.records().len(), 0, "nothing has been answered yet");
        assert_eq!(log.unanswered(), 4, "four queries went out unanswered");
    }

    /// Two slots' logs pool into one
    ///
    /// The per query drivers give every slot its own measurement and absorb them at the end, so a
    /// log that did not merge would report only whichever slot finished last.
    #[test]
    fn absorb_pools_both_halves() {
        let (mut first, mut second) = (StageLog::new(), StageLog::new());
        first.sent(Uuid::new_v4(), 0, 2, BatchStamps::entered_now());
        second.sent(Uuid::new_v4(), 0, 3, BatchStamps::entered_now());
        first.absorb(second);
        assert_eq!(first.unanswered(), 5, "both slots' records are held");
    }

    /// A sampled run opens a record for one query in every `sample`, not for all of them
    ///
    /// Both halves sample on the index and have to keep the same queries. If this kept more than
    /// the server did, every extra record would be reported as client only.
    #[test]
    fn the_sample_rate_decides_which_queries_are_kept() {
        let mut log = StageLog::with_sample(4);
        log.sent(Uuid::new_v4(), 0, 16, BatchStamps::entered_now());
        assert_eq!(log.unanswered(), 4, "one query in every four was kept");
    }
}
