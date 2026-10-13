//! What a shard's clients were answered, of what kind, and how long they waited
//!
//! Counted at the front door: the shard a client connected to reads its bundles and writes its
//! answers, whichever shards and nodes served the queries in between, so a query is counted
//! once, on the node the client reached ([F65](../../../../docs/src/features/query-figures-home-tab.md)).
//! The coordinator notes when each bundle frame came off the socket; the client's write relay
//! reads the kind off each answer it writes, counts it and its bytes, and times it against its
//! bundle's arrival. The shard's report carries the counters to the control thread, which turns
//! them into rates and percentiles on the node's figures.
//!
//! ```text
//!  client_rx_relay ── bytes in ──▶ QueryMeter ◀── answered: kind, bytes, wait ── write_replies
//!        │                             ▲
//!        ▼                             │ arrived: a bundle frame's clock
//!  Coordinator::handle_client ─────────┘
//! ```
//!
//! # Invariants
//!
//! **Only a client connection's relays hold the meter.** A peer's forwarded query is answered on
//! a peer lane, which never sees it, so a query served for another node is counted there and not
//! here as well.
//!
//! **A bundle frame's clock is noted before any of its queries is routed.** An answer can only
//! find a clock noted before it was written; one routed first would be counted and never timed.
//!
//! **An answer's kind is read only off bytes this node sealed.** The write relay reads it
//! without validating, so a forwarded query's whole answer, which is a peer's bytes passed on
//! unvalidated, carries the kind of the query this node validated instead (`Reply::op`).
//!
//! **No borrow is held across an `.await`.** The relays and the shard share this on one
//! executor, and every method borrows and releases within itself.
//!
//! **A clock is released by its last answer or its client's end.** Every query is answered
//! exactly once, which retires its frame's clock; a relay that ends forgets the clocks of the
//! answers it will never write, and an answer a cancel left unwritten releases its share of its
//! clock without being counted ([F75](../../../../docs/src/features/client-cancel.md)).
//!
//! **What a cancel saved is counted here too.** The relays and the shard loop both hold this, so
//! the cancels read, the queries answered `Cancelled` and the answers left unwritten are counted
//! in one place and ride the shard's report beside its answers.

use gxhash::GxHashMap;
use std::cell::{Cell, RefCell};
use uuid::Uuid;

use crate::server::replication::QueryCounters;
use crate::server::stage_profile::Stamp;
use crate::shared::protocol::stats::{query_op_index, CancelCounters, QUERY_OPS};
use crate::shared::responses::ResponseActionNames;

/// How many kinds answers are counted by
pub const OPS: usize = QUERY_OPS.len();

/// One in how many bundle frames has its answers timed
///
/// Every answer is counted and its bytes summed whatever this is; only the clock is sampled.
/// Raising it is how the latency figure is estimated at a lower cost, should timing every frame
/// show on a measurement (F65).
pub const LATENCY_SAMPLE_EVERY: u64 = 1;

/// How many latency buckets there are: four to every power of two of microseconds, from one
/// microsecond to about eighteen minutes
pub const LATENCY_BUCKETS: usize = 120;

/// How many sub-buckets each power of two is split into
const SUB_BUCKETS: u64 = 4;

/// The most bundle frames a shard times at once
///
/// A frame's clock is released by its last answer, and a client that stops reading stops being
/// read, so this is never reached in practice; it bounds what a bug in either would cost.
const MAX_CLOCKS: usize = 65_536;

/// The latency bucket a wait falls in
///
/// Waits under four microseconds get a bucket each; past that, each power of two is split into
/// four equal buckets, so a bucket is never wider than a quarter of its lower bound.
///
/// # Arguments
///
/// * `micros` - The wait, in microseconds
#[must_use]
pub fn bucket_of(micros: u64) -> usize {
    // the first few are exact
    if micros < SUB_BUCKETS {
        return usize::try_from(micros).unwrap_or(0);
    }
    // the power of two the wait is past, and which quarter of it
    let octave = u64::from(63 - micros.leading_zeros());
    let quarter = (micros >> (octave - 2)) & (SUB_BUCKETS - 1);
    let bucket = SUB_BUCKETS * (octave - 1) + quarter;
    // anything past the last bucket is counted in it
    usize::try_from(bucket)
        .unwrap_or(LATENCY_BUCKETS)
        .min(LATENCY_BUCKETS - 1)
}

/// The waits a bucket holds, as its lower bound and the bound after its upper one, in
/// microseconds
///
/// # Arguments
///
/// * `bucket` - The bucket
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn bucket_bounds(bucket: usize) -> (f64, f64) {
    let bucket = u64::try_from(bucket).unwrap_or(u64::MAX);
    // the first few hold one microsecond each
    if bucket < SUB_BUCKETS {
        return (bucket as f64, (bucket + 1) as f64);
    }
    // the rest are a quarter of their power of two
    let octave = bucket / SUB_BUCKETS + 1;
    let quarter = bucket % SUB_BUCKETS;
    let width = 1u64 << (octave - 2);
    let low = (SUB_BUCKETS + quarter) << (octave - 2);
    (low as f64, (low + width) as f64)
}

/// A quantile of the waits a set of buckets holds, in microseconds, or none when they hold
/// less than one wait
///
/// The wait is placed inside its bucket by how far into the bucket's count the quantile falls,
/// so a quantile is never further off than the bucket's width.
///
/// # Arguments
///
/// * `buckets` - How many waits fell in each bucket; weighted counts are fine
/// * `quantile` - The quantile, between zero and one
#[must_use]
pub fn percentile(buckets: &[f64], quantile: f64) -> Option<f64> {
    // fewer than one wait is not enough to say anything about
    let total: f64 = buckets.iter().sum();
    if total < 1.0 {
        return None;
    }
    // walk up to the bucket the quantile falls in
    let target = quantile.clamp(0.0, 1.0) * total;
    let mut below = 0.0;
    for (bucket, count) in buckets.iter().enumerate() {
        if *count > 0.0 && below + count >= target {
            // and place it as far into the bucket as it is into the bucket's count
            let (low, high) = bucket_bounds(bucket);
            let into = ((target - below) / count).clamp(0.0, 1.0);
            return Some(low + into * (high - low));
        }
        below += count;
    }
    // rounding left the quantile past the last count, which is the highest wait there is
    buckets
        .iter()
        .rposition(|count| *count > 0.0)
        .map(|bucket| bucket_bounds(bucket).1)
}

/// When one bundle frame came off its client's socket, and which of its answers are still owed
#[derive(Debug, Clone, Copy)]
struct BundleClock {
    /// When the frame's last byte came off the socket
    base: Stamp,
    /// The index of the frame's first query
    base_index: usize,
    /// How many queries the frame held
    len: usize,
    /// How many of their answers are still to be written
    left: usize,
}

impl BundleClock {
    /// Whether an answer at an index belongs to this frame
    ///
    /// # Arguments
    ///
    /// * `index` - The answer's index in its bundle's stream
    fn holds(&self, index: usize) -> bool {
        index >= self.base_index && index - self.base_index < self.len
    }
}

/// The clocks of one bundle id: almost always one frame, several for a streamed query
#[derive(Debug)]
enum Clocks {
    /// One frame, which needs no allocation of its own
    One(BundleClock),
    /// A streamed query's frames, each with its own range of indexes
    Many(Vec<BundleClock>),
}

/// What a shard's clients were answered, and how long they waited
#[derive(Debug)]
pub struct QueryMeter {
    /// Answers written, by kind
    answers: [Cell<u64>; OPS],
    /// Bytes of those answers, by kind
    bytes_out: [Cell<u64>; OPS],
    /// Bytes of the bundles the clients sent
    bytes_in: Cell<u64>,
    /// How many timed answers fell in each latency bucket, by kind
    latency: Box<[[Cell<u64>; LATENCY_BUCKETS]; OPS]>,
    /// How many bundle frames have arrived, which picks the ones that are timed
    frames: Cell<u64>,
    /// The clocks of the frames being timed, by client and bundle id
    clocks: RefCell<GxHashMap<(Uuid, Uuid), Clocks>>,
    /// What this shard's clients cancelled and what that saved
    /// ([F75](../../../../docs/src/features/client-cancel.md))
    cancels: Cell<CancelCounters>,
}

impl Default for QueryMeter {
    /// A meter that has counted nothing
    fn default() -> Self {
        QueryMeter {
            answers: std::array::from_fn(|_| Cell::new(0)),
            bytes_out: std::array::from_fn(|_| Cell::new(0)),
            bytes_in: Cell::new(0),
            latency: Box::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| Cell::new(0))
            })),
            frames: Cell::new(0),
            clocks: RefCell::new(GxHashMap::default()),
            cancels: Cell::new(CancelCounters::default()),
        }
    }
}

/// Add to a counter, saturating rather than wrapping
///
/// # Arguments
///
/// * `cell` - The counter
/// * `by` - How much to add
fn bump(cell: &Cell<u64>, by: u64) {
    cell.set(cell.get().saturating_add(by));
}

impl QueryMeter {
    /// Count a bundle's bytes as they came off a client's socket
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bundle's length
    pub fn read(&self, bytes: usize) {
        bump(&self.bytes_in, u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    /// Note that a bundle frame arrived, so its answers can be timed against it
    ///
    /// One frame in [`LATENCY_SAMPLE_EVERY`] is noted; the rest are counted when answered and
    /// never timed.
    ///
    /// # Arguments
    ///
    /// * `client` - The client that sent it
    /// * `bundle` - The bundle's id
    /// * `base_index` - The index of its first query
    /// * `len` - How many queries it holds
    /// * `base` - When its last byte came off the socket
    pub fn arrived(&self, client: Uuid, bundle: Uuid, base_index: usize, len: usize, base: Stamp) {
        // only one frame in so many is timed, and an empty one has nothing to time
        let frame = self.frames.get();
        self.frames.set(frame.wrapping_add(1));
        if frame % LATENCY_SAMPLE_EVERY != 0 || len == 0 {
            return;
        }
        let clock = BundleClock {
            base,
            base_index,
            len,
            left: len,
        };
        let mut clocks = self.clocks.borrow_mut();
        // a shard already timing as many frames as it should ever hold times no more
        if clocks.len() >= MAX_CLOCKS {
            return;
        }
        // a second frame under the same id is a streamed query's next one
        match clocks.get_mut(&(client, bundle)) {
            Some(Clocks::Many(frames)) => frames.push(clock),
            Some(entry @ Clocks::One(_)) => {
                let Clocks::One(first) = *entry else {
                    return;
                };
                *entry = Clocks::Many(vec![first, clock]);
            }
            None => {
                clocks.insert((client, bundle), Clocks::One(clock));
            }
        }
    }

    /// Count one answer written to a client, and time it if its frame is being timed
    ///
    /// # Arguments
    ///
    /// * `client` - The client it was written to
    /// * `bundle` - The bundle it answers
    /// * `index` - Its index in the bundle's stream
    /// * `op` - Its kind, as an index into [`QUERY_OPS`]
    /// * `bytes` - How many bytes of it were written
    /// * `now` - When it was written
    pub fn answered(
        &self,
        client: Uuid,
        bundle: Uuid,
        index: usize,
        op: usize,
        bytes: usize,
        now: Stamp,
    ) {
        // every answer is counted, timed or not, and a kind this build does not know is a
        // failure: `refused` follows `error` since F68, so the last kind is no longer the one
        let op = if op < OPS {
            op
        } else {
            query_op_index(&ResponseActionNames::Error)
        };
        bump(&self.answers[op], 1);
        bump(
            &self.bytes_out[op],
            u64::try_from(bytes).unwrap_or(u64::MAX),
        );
        // and its wait, against the frame it belongs to if that frame is timed
        self.release(client, bundle, index, Some((op, now)));
    }

    /// Release an answer's share of its frame's clock without counting it as an answer
    ///
    /// The answer a cancel left unwritten: nobody was answered, so nothing is counted or timed,
    /// but the frame's clock still waits on it, and a clock nothing releases is held until its
    /// client leaves ([F75](../../../../docs/src/features/client-cancel.md)).
    ///
    /// # Arguments
    ///
    /// * `client` - The client it was owed to
    /// * `bundle` - The bundle it answers
    /// * `index` - Its index in the bundle's stream
    pub fn abandoned(&self, client: Uuid, bundle: Uuid, index: usize) {
        self.release(client, bundle, index, None);
    }

    /// Release one answer's share of its frame's clock, timing it when it was written
    ///
    /// # Arguments
    ///
    /// * `client` - The client it was owed to
    /// * `bundle` - The bundle it answers
    /// * `index` - Its index in the bundle's stream
    /// * `written` - Its kind and when it was written, or none for an answer never written
    fn release(&self, client: Uuid, bundle: Uuid, index: usize, written: Option<(usize, Stamp)>) {
        // find the clock of the frame it belongs to, if that frame is timed
        let mut clocks = self.clocks.borrow_mut();
        let Some(entry) = clocks.get_mut(&(client, bundle)) else {
            return;
        };
        let (clock, position) = match entry {
            Clocks::One(clock) if clock.holds(index) => (clock, None),
            Clocks::One(_) => return,
            Clocks::Many(frames) => match frames.iter().position(|clock| clock.holds(index)) {
                Some(position) => (&mut frames[position], Some(position)),
                None => return,
            },
        };
        // the wait from the frame's arrival to this answer's write, if it was written
        if let Some((op, now)) = written {
            let micros = now.since(clock.base) / 1000;
            bump(&self.latency[op][bucket_of(micros)], 1);
        }
        // the frame's last answer releases its clock
        clock.left = clock.left.saturating_sub(1);
        if clock.left > 0 {
            return;
        }
        let empty = match (entry, position) {
            (Clocks::Many(frames), Some(position)) => {
                frames.swap_remove(position);
                frames.is_empty()
            }
            _ => true,
        };
        if empty {
            clocks.remove(&(client, bundle));
        }
    }

    /// Forget every clock of a client whose relay ended, whose answers will never be written
    ///
    /// # Arguments
    ///
    /// * `client` - The client
    pub fn forget(&self, client: Uuid) {
        self.clocks
            .borrow_mut()
            .retain(|(owner, _), _| *owner != client);
    }

    /// Count something a cancel did
    ///
    /// # Arguments
    ///
    /// * `count` - What to add to the counters
    pub fn count_cancels(&self, count: impl FnOnce(&mut CancelCounters)) {
        let mut cancels = self.cancels.get();
        count(&mut cancels);
        self.cancels.set(cancels);
    }

    /// What this shard's clients cancelled since it started, as its report carries it
    #[must_use]
    pub fn cancels(&self) -> CancelCounters {
        self.cancels.get()
    }

    /// How many bundle ids have frames being timed
    #[must_use]
    pub fn timing(&self) -> usize {
        self.clocks.borrow().len()
    }

    /// What this shard has counted since it started, as its report carries it
    #[must_use]
    pub fn counters(&self) -> QueryCounters {
        QueryCounters {
            answers: std::array::from_fn(|op| self.answers[op].get()),
            bytes_out: std::array::from_fn(|op| self.bytes_out[op].get()),
            bytes_in: self.bytes_in.get(),
            // each kind's buckets, the empty ones past its slowest wait left off
            latency: std::array::from_fn(|op| {
                let buckets = &self.latency[op];
                let used = buckets
                    .iter()
                    .rposition(|bucket| bucket.get() > 0)
                    .map_or(0, |last| last + 1);
                buckets[..used].iter().map(Cell::get).collect()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stamp some microseconds after another
    ///
    /// # Arguments
    ///
    /// * `base` - The earlier stamp
    /// * `micros` - How long after it
    fn after(base: Stamp, micros: u64) -> Stamp {
        base.plus_nanos(micros * 1000)
    }

    /// Every wait lands in the bucket whose bounds hold it, the buckets tile the line, and a
    /// quantile is placed within its bucket
    #[test]
    fn buckets_tile_and_percentiles_interpolate() {
        // the first few are exact, and each power of two is split in four
        assert_eq!(bucket_of(0), 0);
        assert_eq!(bucket_of(3), 3);
        assert_eq!(bucket_of(4), 4);
        assert_eq!(bucket_of(7), 7);
        assert_eq!(bucket_of(8), 8);
        assert_eq!(bucket_of(15), 11);
        assert_eq!(bucket_of(16), 12);
        assert_eq!(bucket_of(u64::MAX), LATENCY_BUCKETS - 1);
        // every bucket starts where the one before it ends, and holds what falls in it
        for bucket in 1..LATENCY_BUCKETS {
            assert_eq!(
                bucket_bounds(bucket).0,
                bucket_bounds(bucket - 1).1,
                "bucket {bucket}"
            );
            let (low, high) = bucket_bounds(bucket);
            assert_eq!(bucket_of(low as u64), bucket);
            assert_eq!(bucket_of(high as u64 - 1), bucket);
            // and is never wider than a quarter of its lower bound
            assert!(high - low <= (low / 4.0).max(1.0), "bucket {bucket}");
        }
        // the last bucket reaches past a quarter of an hour
        assert!(bucket_bounds(LATENCY_BUCKETS - 1).1 > 15.0 * 60.0 * 1e6);
        // a hundred waits of 100us and one of 10ms: the median is near 100us, the tail is 10ms
        let mut buckets = vec![0.0; LATENCY_BUCKETS];
        buckets[bucket_of(100)] += 100.0;
        buckets[bucket_of(10_000)] += 1.0;
        let median = percentile(&buckets, 0.5).expect("a median");
        let (low, high) = bucket_bounds(bucket_of(100));
        assert!(median >= low && median <= high, "{median}");
        let top = percentile(&buckets, 1.0).expect("a maximum");
        assert!((8192.0..=10_240.0).contains(&top), "{top}");
        // the 99th percentile of 101 waits is still among the hundred fast ones
        assert!(percentile(&buckets, 0.99).expect("a p99") <= high);
        // less than one wait says nothing
        assert_eq!(percentile(&[0.5], 0.5), None);
        assert_eq!(percentile(&[], 0.5), None);
    }

    /// Each answer is counted by its kind, a timed frame's answers are timed once each and its
    /// clock released by the last, a streamed query's frames are told apart by index, an
    /// untimed answer is counted without a wait, and a relay's end forgets its client's clocks
    #[test]
    fn answers_are_counted_and_their_frames_timed() {
        let meter = QueryMeter::default();
        let client = Uuid::new_v4();
        let other = Uuid::new_v4();
        let bundle = Uuid::new_v4();
        let base = Stamp::now();
        // a bundle of three came in, and its three gets were answered 100us, 200us and 1ms on
        meter.read(300);
        meter.arrived(client, bundle, 0, 3, base);
        assert_eq!(meter.timing(), 1);
        meter.answered(client, bundle, 0, 0, 40, after(base, 100));
        meter.answered(client, bundle, 1, 0, 40, after(base, 200));
        assert_eq!(
            meter.timing(),
            1,
            "a frame with an answer owed keeps its clock"
        );
        meter.answered(client, bundle, 2, 0, 40, after(base, 1000));
        assert_eq!(meter.timing(), 0, "the last answer releases the clock");
        let counters = meter.counters();
        assert_eq!(counters.answers[0], 3);
        assert_eq!(counters.bytes_out[0], 120);
        assert_eq!(counters.bytes_in, 300);
        assert_eq!(counters.latency[0].iter().sum::<u64>(), 3);
        assert_eq!(counters.latency[0].len(), bucket_of(1000) + 1);
        assert_eq!(counters.latency[0][bucket_of(200)], 1);
        // an answer of a frame never noted is counted, and not timed
        meter.answered(client, Uuid::new_v4(), 0, 2, 16, after(base, 50));
        let counters = meter.counters();
        assert_eq!(counters.answers[2], 1);
        assert!(counters.latency[2].is_empty());
        // a streamed query's two frames under one id are told apart by their indexes
        let stream = Uuid::new_v4();
        meter.arrived(client, stream, 0, 2, base);
        meter.arrived(client, stream, 2, 2, after(base, 5000));
        meter.answered(client, stream, 3, 2, 8, after(base, 5010));
        meter.answered(client, stream, 2, 2, 8, after(base, 5020));
        assert_eq!(meter.timing(), 1, "the first frame is still owed");
        let counters = meter.counters();
        // the second frame's answers waited 10us and 20us from its own arrival, not the first's
        assert_eq!(counters.latency[2][bucket_of(10)], 1);
        assert_eq!(counters.latency[2][bucket_of(20)], 1);
        meter.answered(client, stream, 0, 2, 8, after(base, 30));
        meter.answered(client, stream, 1, 2, 8, after(base, 40));
        assert_eq!(meter.timing(), 0);
        // the same id from another client is another bundle
        meter.arrived(client, bundle, 0, 1, base);
        meter.arrived(other, bundle, 0, 1, base);
        assert_eq!(meter.timing(), 2);
        // a relay that ends forgets only its own client's clocks
        meter.forget(client);
        assert_eq!(meter.timing(), 1);
        meter.answered(other, bundle, 0, 5, 12, after(base, 70));
        assert_eq!(meter.timing(), 0);
        assert_eq!(meter.counters().answers[5], 1);
        // a kind past the last is counted as a failure, not as whichever kind is last
        meter.answered(other, bundle, 0, 99, 0, base);
        assert_eq!(meter.counters().answers[5], 2);
        assert_eq!(meter.counters().answers[6], 0);
    }

    /// An answer a cancel left unwritten releases its frame's clock and is neither counted nor
    /// timed, and the cancel counters add up ([F75](../../../../docs/src/features/client-cancel.md))
    #[test]
    fn an_abandoned_answer_releases_its_clock() {
        let meter = QueryMeter::default();
        let client = Uuid::new_v4();
        let bundle = Uuid::new_v4();
        let base = Stamp::now();
        // a bundle of two, one answered and one cancelled before it was written
        meter.arrived(client, bundle, 0, 2, base);
        meter.answered(client, bundle, 0, 0, 40, after(base, 100));
        assert_eq!(meter.timing(), 1);
        meter.abandoned(client, bundle, 1);
        assert_eq!(meter.timing(), 0, "the unwritten answer released the clock");
        // only the written one was counted and timed
        let counters = meter.counters();
        assert_eq!(counters.answers[0], 1);
        assert_eq!(counters.latency[0].iter().sum::<u64>(), 1);
        // an abandoned answer of a frame never timed changes nothing
        meter.abandoned(client, Uuid::new_v4(), 0);
        assert_eq!(meter.counters().answers[0], 1);
        // and what a cancel saved is summed where it is counted
        meter.count_cancels(|cancels| cancels.received += 1);
        meter.count_cancels(|cancels| {
            cancels.dropped += 2;
            cancels.dropped_bytes += 80;
        });
        let cancels = meter.cancels();
        assert_eq!(cancels.received, 1);
        assert_eq!(cancels.dropped, 2);
        assert_eq!(cancels.dropped_bytes, 80);
    }

    /// What recording costs, printed rather than asserted: run by hand on the bench host
    /// ([F65](../../../../docs/src/features/query-figures-home-tab.md#performance))
    #[test]
    #[ignore = "a timing, not a check: run with --ignored --nocapture on the bench host"]
    fn meter_cost() {
        let meter = QueryMeter::default();
        let client = Uuid::new_v4();
        let rounds: u32 = 1_000_000;
        // one frame of one query per round, which is the most the clock can cost per answer
        let bundles: Vec<Uuid> = (0..1024).map(|_| Uuid::new_v4()).collect();
        let started = std::time::Instant::now();
        for round in 0..rounds {
            let bundle = bundles[(round as usize) % bundles.len()];
            let base = Stamp::now();
            meter.read(64);
            meter.arrived(client, bundle, 0, 1, base);
            meter.answered(client, bundle, 0, 0, 128, Stamp::now());
        }
        let per_query = started.elapsed().as_nanos() / u128::from(rounds);
        // and counting alone, with no frame timed
        let started = std::time::Instant::now();
        for round in 0..rounds {
            let bundle = bundles[(round as usize) % bundles.len()];
            meter.answered(client, bundle, 0, 0, 128, Stamp::now());
        }
        let per_answer = started.elapsed().as_nanos() / u128::from(rounds);
        println!(
            "meter: {per_query}ns per timed one-query bundle, {per_answer}ns per untimed answer"
        );
        assert_eq!(meter.timing(), 0);
    }
}
