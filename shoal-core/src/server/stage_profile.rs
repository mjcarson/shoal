//! Per query stage timings for the macro benchmark
//!
//! This exists to answer where a query's latency went, not to serve queries. It is off
//! unless the `stage-profile` feature is on, and a build with it on is an attribution build
//! whose absolute latencies are not comparable to one without it — the same rule that already
//! applies to `hotpath`. See `docs/src/operations/benchmarking.md`.
//!
//! The whole design rests on one thing: [`StageStamps`] is a zero sized type when the feature
//! is off. That is what lets a stamp ride along in every response tuple without a `#[cfg]` at
//! each of the fifty places those tuples are built or destructured — the arity never changes,
//! only what one of the elements costs. Do not delete the feature-off definition without also
//! unwinding all of that plumbing.
//!
//! Call sites never touch a field. They call a marker method per stage and a setter per flag,
//! and both forms of every one of those is generated from the same list below, so the two
//! forms cannot drift apart.

use std::time::Instant;
use uuid::Uuid;

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
    pub fn plus_nanos(self, nanos: u64) -> Self {
        Stamp(self.0 + std::time::Duration::from_nanos(nanos))
    }

    /// Get the stamp a number of nanoseconds before this one
    ///
    /// # Arguments
    ///
    /// * `nanos` - How far before this stamp to move
    #[must_use]
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
    pub fn is_set(self) -> bool {
        self != Offset::UNSET
    }

    /// Check whether this offset ran past what it can represent
    #[must_use]
    pub fn is_saturated(self) -> bool {
        self == Offset::SATURATED
    }

    /// Get this offset in nanoseconds if it holds a real measurement
    #[must_use]
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

/// The kind of query a stage record came from
///
/// This keeps the split [`crate::shared::responses::ResponseAction`] already makes, because
/// pooling the kinds makes a percentile report where the boundary between two distributions
/// landed rather than anything about either one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOp {
    /// A row was written
    Insert,
    /// Rows were read back
    Get,
    /// A partition was asked whether it holds a row
    Exists,
    /// A row was tombstoned
    Delete,
    /// A row's fields were changed
    Update,
    /// Some other query we do not break out
    Other,
}

impl StageOp {
    /// Get the name this op is reported under
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StageOp::Insert => "insert",
            StageOp::Get => "get",
            StageOp::Exists => "exists",
            StageOp::Delete => "delete",
            StageOp::Update => "update",
            StageOp::Other => "other",
        }
    }
}

/// How the intent log a query wrote to is made durable
///
/// Under [`StageDurability::Async`] there is no fdatasync stage at all, so a report that
/// showed one would be fiction. A record carries which mode it ran under rather than letting
/// the reader assume the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageDurability {
    /// This query waited on an fdatasync covering its bytes
    Fsync,
    /// This query was acknowledged once its write landed, with no sync
    Async,
    /// This query never touched the intent log
    None,
}

impl StageDurability {
    /// Get the name this durability mode is reported under
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StageDurability::Fsync => "fsync",
            StageDurability::Async => "async",
            StageDurability::None => "none",
        }
    }
}

/// What a query was, and what happened to it on the way
#[derive(Debug, Clone, Copy)]
pub struct StageFlags {
    /// The kind of query this was
    pub op: StageOp,
    /// How the write behind this query was made durable
    pub durability: StageDurability,
    /// The shard that executed this query
    pub shard: u16,
    /// This query's position in the batch it arrived in
    ///
    /// Uninterpretable without [`StageFlags::batch_len`] beside it — position four means
    /// something very different in a batch of five than in a batch of five hundred.
    pub batch_pos: u16,
    /// How many queries were in that batch
    pub batch_len: u16,
    /// Whether this response was released by a log rotation rather than by a watermark
    ///
    /// A rotation restarts positions at zero and its durability windows go with the old file,
    /// so these records carry no durability stages and are reported as their own population
    /// rather than interpolated.
    pub rotated: bool,
    /// Whether this query had to wait on a partition being read from disk
    pub loaded_from_disk: bool,
    /// Whether this record is one shard's share of a query split across shards
    ///
    /// A split query produces one of these per shard plus one client visible record, so a
    /// report that counted them all would multiply count it.
    pub share_of_gathered: bool,
    /// Whether this query's durability window had already been evicted when it was released
    ///
    /// Counted rather than guessed at. A run with many of these needs a bigger window deque
    /// before its durability numbers mean anything.
    pub window_missing: bool,
}

impl Default for StageFlags {
    fn default() -> Self {
        StageFlags {
            op: StageOp::Other,
            durability: StageDurability::None,
            shard: 0,
            batch_pos: 0,
            batch_len: 0,
            rotated: false,
            loaded_from_disk: false,
            share_of_gathered: false,
            window_missing: false,
        }
    }
}

/// Declare a stage marker method in the form the feature asks for
///
/// A marker takes no arguments and records that its stage was reached now. Generating both
/// forms from one invocation is what keeps the profiling build and the shipping build from
/// drifting apart as stages are added.
macro_rules! stage_marker {
    ($(#[$meta:meta])* $name:ident => $field:ident) => {
        $(#[$meta])*
        #[cfg(feature = "stage-profile")]
        pub fn $name(&mut self) {
            // measure how far past our base this stage landed
            self.$field = Offset::between(self.base, Stamp::now());
        }

        $(#[$meta])*
        #[cfg(not(feature = "stage-profile"))]
        #[inline(always)]
        pub fn $name(&mut self) {}
    };
}

/// Declare a stage setter that takes the moment the stage was reached
///
/// The durability phases are intervals of the intent log rather than properties of a query,
/// so they are looked up after the fact from a stamp taken when they actually happened. A
/// marker method cannot express that, because by the time a response knows it is durable the
/// sync that made it so has already returned.
macro_rules! stage_at {
    ($(#[$meta:meta])* $name:ident => $field:ident) => {
        $(#[$meta])*
        #[cfg(feature = "stage-profile")]
        pub fn $name(&mut self, at: Stamp) {
            // measure how far past our base this stage landed
            self.$field = Offset::between(self.base, at);
        }

        $(#[$meta])*
        #[cfg(not(feature = "stage-profile"))]
        #[inline(always)]
        pub fn $name(&mut self, _at: Stamp) {}
    };
}

/// Declare a flag setter in the form the feature asks for
///
/// The same reasoning as [`stage_marker`], for the fields that carry what happened to a query
/// rather than when it happened.
macro_rules! stage_setter {
    ($(#[$meta:meta])* $name:ident($ty:ty) => $field:ident) => {
        $(#[$meta])*
        #[cfg(feature = "stage-profile")]
        pub fn $name(&mut self, value: $ty) {
            self.flags.$field = value;
        }

        $(#[$meta])*
        #[cfg(not(feature = "stage-profile"))]
        #[inline(always)]
        pub fn $name(&mut self, _value: $ty) {}
    };
}

/// When a query reached each server side stage
///
/// This holds one base [`Stamp`] and a set of `u32` offsets from it rather than a stamp per
/// stage. [`crate::server::messages::QueryMetadata`] is cloned unconditionally for every
/// query and again for every partition a query blocks on, so a dozen instants here would make
/// those clones cost more than the work hanging off them.
#[cfg(feature = "stage-profile")]
#[derive(Debug, Clone, Copy)]
pub struct StageStamps {
    /// The clock reading every offset in here is measured from
    ///
    /// Taken when this query's bundle finished being read off the socket, which is the first
    /// moment the server knows the query exists.
    pub base: Stamp,
    /// When the shard dequeued the bundle this query arrived in
    pub bundle_dequeued: Offset,
    /// When that bundle finished being deserialized
    pub decoded: Offset,
    /// When this query was handed to the shard that owns its partitions
    pub routed: Offset,
    /// When that shard dequeued it
    pub exec_dequeued: Offset,
    /// When its synchronous work finished
    ///
    /// For a get that is where the response was built. For a write that is where `commit`
    /// returned, so the write behind backpressure inside it lands in this stage rather than
    /// in a durability one.
    pub exec_done: Offset,
    /// When the write carrying this query was handed to io_uring
    ///
    /// The gap between this and `exec_done` is time the query spent sitting in the DMA
    /// staging buffer, unsubmitted, which nothing before this measured.
    pub write_submitted: Offset,
    /// When that write landed
    pub write_completed: Offset,
    /// When the fdatasync covering this query claimed its slot
    pub sync_issued: Offset,
    /// When that fdatasync returned
    pub sync_completed: Offset,
    /// When this query's response was released back to the shard
    pub released: Offset,
    /// When the response finished being serialized
    pub replied: Offset,
    /// When it was queued to the client relay
    pub queued_to_client: Offset,
    /// When its last byte was handed to the socket
    pub socket_written: Offset,
    /// The intent log offset this query becomes durable at
    ///
    /// This is what matches a parked response back to the write that carried it, since the
    /// durability phases are intervals of the log rather than properties of a query.
    pub commit_pos: u64,
    /// This query's index within the stream it arrived in
    ///
    /// Half of the key a record is joined on. It rides here rather than beside the response
    /// in the reply channel so that a build with the feature off does not pay eight bytes per
    /// response for something only the profile ever reads.
    pub index: usize,
    /// What this query was, and what happened to it on the way
    pub flags: StageFlags,
}

/// When a query reached each server side stage
///
/// This is the form the stamps take when the `stage-profile` feature is off: a zero sized
/// type whose every method is a no-op. Carrying it through the response tuples costs nothing
/// at runtime, which is what makes the plumbing acceptable in a build nobody is profiling.
#[cfg(not(feature = "stage-profile"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct StageStamps;

// carrying a stamp through every response tuple is only free if the feature-off form really
// is free, so this is checked rather than assumed
#[cfg(not(feature = "stage-profile"))]
const _: () = assert!(
    std::mem::size_of::<StageStamps>() == 0,
    "StageStamps must be zero sized when the stage-profile feature is off"
);

impl StageStamps {
    /// Start a new set of stamps from the moment a bundle finished being read
    ///
    /// # Arguments
    ///
    /// * `base` - The stamp every offset in these is measured from
    #[cfg(feature = "stage-profile")]
    #[must_use]
    pub fn new(base: Stamp) -> Self {
        StageStamps {
            base,
            bundle_dequeued: Offset::UNSET,
            decoded: Offset::UNSET,
            routed: Offset::UNSET,
            exec_dequeued: Offset::UNSET,
            exec_done: Offset::UNSET,
            write_submitted: Offset::UNSET,
            write_completed: Offset::UNSET,
            sync_issued: Offset::UNSET,
            sync_completed: Offset::UNSET,
            released: Offset::UNSET,
            index: 0,
            replied: Offset::UNSET,
            queued_to_client: Offset::UNSET,
            socket_written: Offset::UNSET,
            commit_pos: 0,
            flags: StageFlags::default(),
        }
    }

    /// Start a new set of stamps, which records nothing in this build
    ///
    /// # Arguments
    ///
    /// * `base` - Ignored, since nothing is measured from it here
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    #[must_use]
    pub fn new(_base: Stamp) -> Self {
        StageStamps
    }

    stage_marker!(
        /// Record that the shard dequeued the bundle this query arrived in
        mark_bundle_dequeued => bundle_dequeued
    );
    stage_marker!(
        /// Record that this query's bundle finished being deserialized
        mark_decoded => decoded
    );
    stage_marker!(
        /// Record that this query was handed to the shard that owns its partitions
        mark_routed => routed
    );
    stage_marker!(
        /// Record that the executing shard dequeued this query
        mark_exec_dequeued => exec_dequeued
    );
    stage_marker!(
        /// Record that this query's synchronous work finished
        mark_exec_done => exec_done
    );
    stage_at!(
        /// Record when the write carrying this query was handed to io_uring
        set_write_submitted => write_submitted
    );
    stage_at!(
        /// Record when the write carrying this query landed
        set_write_completed => write_completed
    );
    stage_at!(
        /// Record when the fdatasync covering this query claimed its slot
        set_sync_issued => sync_issued
    );
    stage_at!(
        /// Record when the fdatasync covering this query returned
        set_sync_completed => sync_completed
    );
    stage_marker!(
        /// Record that this query's response was released back to the shard
        mark_released => released
    );
    stage_marker!(
        /// Record that this query's response finished being serialized
        mark_replied => replied
    );
    stage_marker!(
        /// Record that this query's response was queued to the client relay
        mark_queued_to_client => queued_to_client
    );
    stage_marker!(
        /// Record that this query's response was handed to the socket
        mark_socket_written => socket_written
    );

    stage_setter!(
        /// Record what kind of query this was
        set_op(StageOp) => op
    );
    stage_setter!(
        /// Record how the write behind this query is made durable
        set_durability(StageDurability) => durability
    );
    stage_setter!(
        /// Record which shard executed this query
        set_shard(u16) => shard
    );
    stage_setter!(
        /// Record that this response was released by a rotation, not by a watermark
        set_rotated(bool) => rotated
    );
    stage_setter!(
        /// Record that this query waited on a partition being read from disk
        set_loaded_from_disk(bool) => loaded_from_disk
    );
    stage_setter!(
        /// Record that this is one shard's share of a query split across shards
        set_share_of_gathered(bool) => share_of_gathered
    );
    stage_setter!(
        /// Record that this query's durability window had already been evicted
        set_window_missing(bool) => window_missing
    );

    /// Record where in its batch this query arrived
    ///
    /// A position is uninterpretable without the length beside it, so the two are set
    /// together rather than through separate setters that could be called apart.
    ///
    /// # Arguments
    ///
    /// * `pos` - This query's index within its batch
    /// * `len` - How many queries that batch held
    #[cfg(feature = "stage-profile")]
    pub fn set_batch(&mut self, pos: usize, len: usize) {
        // a batch larger than a u16 would wrap into a small number, which reads as a query
        // near the head of its batch rather than as an unrepresentable one
        self.flags.batch_pos = u16::try_from(pos).unwrap_or(u16::MAX);
        self.flags.batch_len = u16::try_from(len).unwrap_or(u16::MAX);
    }

    /// Record where in its batch this query arrived, which does nothing in this build
    ///
    /// # Arguments
    ///
    /// * `pos` - Ignored, since there is nothing to record it on
    /// * `len` - Ignored, for the same reason
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    pub fn set_batch(&mut self, _pos: usize, _len: usize) {}

    /// Record the intent log offset this query becomes durable at
    ///
    /// # Arguments
    ///
    /// * `pos` - The offset all of this query's bytes are below
    #[cfg(feature = "stage-profile")]
    pub fn set_commit_pos(&mut self, pos: u64) {
        self.commit_pos = pos;
    }

    /// Record this query's durable offset, which does nothing in this build
    ///
    /// # Arguments
    ///
    /// * `pos` - Ignored, since there is nothing to record it on
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    pub fn set_commit_pos(&mut self, _pos: u64) {}

    /// Get the intent log offset this query becomes durable at
    #[cfg(feature = "stage-profile")]
    #[must_use]
    pub fn commit_pos(&self) -> u64 {
        self.commit_pos
    }

    /// Record this query's index within the stream it arrived in
    ///
    /// # Arguments
    ///
    /// * `index` - The index this query answers under
    #[cfg(feature = "stage-profile")]
    pub fn set_index(&mut self, index: usize) {
        self.index = index;
    }

    /// Record this query's index, which does nothing in this build
    ///
    /// # Arguments
    ///
    /// * `index` - Ignored, since there is nothing to record it on
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    pub fn set_index(&mut self, _index: usize) {}
}

/// One query's journey, as it left the server
///
/// Emitted from the client relay after the response bytes are handed to the socket, which is
/// the last moment the server knows anything about the query.
#[cfg(feature = "stage-profile")]
#[derive(Debug, Clone, Copy)]
pub struct StageRecord {
    /// Which phase of the run this record came from
    ///
    /// A harness warms up before it measures, and the warmup's records have to be thrown
    /// away with the warmup's samples. Threads buffer records locally before handing them
    /// over, so by the time a phase ends most of its records are not yet reachable to be
    /// dropped — carrying the phase on the record is what lets them be discarded whenever
    /// they do arrive.
    pub epoch: u64,
    /// The stream this query belonged to
    ///
    /// Every query in a worker's stream shares one `id`, so this and the index the stamps
    /// carry are together the key — the same pair the tables' pending maps and the shard's
    /// gather map are keyed on.
    pub id: Uuid,
    /// When this query reached each stage
    pub stamps: StageStamps,
}

#[cfg(feature = "stage-profile")]
mod sink {
    use super::{StageRecord, StageStamps};
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};
    use uuid::Uuid;

    /// How many records a thread buffers before handing them to the global collector
    ///
    /// Bounded so a long run does not grow one unbounded vec per shard, and so the collector
    /// holds nearly everything by the time the run ends rather than only at shutdown.
    const FLUSH_AT: usize = 8192;

    /// The environment variable that sets how many queries to keep one record for
    ///
    /// This arrives as an environment variable rather than through the config because it is
    /// a property of a benchmark run rather than of a database, and because the client half
    /// of the sample has to be decided by the harness with the identical rule. A config
    /// field would be a permanent knob for something only a profiling build reads.
    pub const SAMPLE_ENV: &str = "SHOAL_STAGE_SAMPLE";

    /// How many queries this run keeps one record for
    static SAMPLE: OnceLock<usize> = OnceLock::new();

    /// Get the sample rate this run was started with
    ///
    /// Read once and cached. A rate of one keeps everything, which is the default.
    fn sample_rate() -> usize {
        *SAMPLE.get_or_init(|| {
            // an unset, unparseable, or zero rate all mean "keep everything", since a rate
            // of zero would otherwise divide by zero and a partial profile is worse than a
            // large one
            std::env::var(SAMPLE_ENV)
                .ok()
                .and_then(|raw| raw.parse::<usize>().ok())
                .filter(|rate| *rate > 0)
                .unwrap_or(1)
        })
    }

    thread_local! {
        /// The records this thread has emitted but not yet handed over
        ///
        /// A glommio executor is one thread, so a shard, its client relay, and its writer
        /// tasks all share this with no handle plumbing at all.
        static LOCAL: RefCell<Vec<StageRecord>> = const { RefCell::new(Vec::new()) };
    }

    /// Every record every thread has handed over so far
    static COLLECTED: OnceLock<Mutex<Vec<StageRecord>>> = OnceLock::new();

    /// Which phase of the run is currently being recorded
    static EPOCH: AtomicU64 = AtomicU64::new(0);

    /// Discard everything recorded so far and start a new phase
    ///
    /// Called at the end of a warmup, so the measured run's report holds only its own
    /// queries. Records already buffered on a thread carry the old epoch and are dropped
    /// when that thread eventually hands them over, which is what makes this correct without
    /// having to reach into every thread's buffer.
    pub fn reset() {
        // move to the next phase before clearing, so a record emitted concurrently with this
        // lands in the new phase rather than in one that is being thrown away
        EPOCH.fetch_add(1, Ordering::SeqCst);
        // drop everything the old phase already handed over
        if let Ok(mut collected) = collected().lock() {
            collected.clear();
        }
    }

    /// Get the global collector, creating it on first use
    fn collected() -> &'static Mutex<Vec<StageRecord>> {
        COLLECTED.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Record one query's journey
    ///
    /// # Arguments
    ///
    /// * `id` - The stream this query belonged to
    /// * `stamps` - When this query reached each stage, and the index it answers under
    pub fn emit(id: Uuid, stamps: StageStamps) {
        // drop this record if it is not one of the sampled queries
        //
        // the sample is taken on the index rather than at random so the client half of the
        // harness keeps exactly the same queries. Two sides sampling independently would
        // leave almost nothing to join.
        if stamps.index % sample_rate() != 0 {
            return;
        }
        LOCAL.with(|local| {
            // buffer on our own thread first, since the global collector is behind a mutex
            // that every shard would otherwise contend on once per query
            let mut local = local.borrow_mut();
            local.push(StageRecord {
                epoch: EPOCH.load(Ordering::Relaxed),
                id,
                stamps,
            });
            // hand our buffer over once it is full enough to be worth taking the lock
            if local.len() >= FLUSH_AT {
                flush_local(&mut local);
            }
        });
    }

    /// Hand one thread's buffered records to the global collector
    ///
    /// # Arguments
    ///
    /// * `local` - The buffer to drain
    fn flush_local(local: &mut Vec<StageRecord>) {
        // a poisoned collector means another thread panicked mid flush, and losing the
        // profile is not worth turning that into a second panic here
        match collected().lock() {
            Ok(mut collected) => collected.append(local),
            // we still have to clear our buffer, or every later flush retries these records
            Err(_) => local.clear(),
        }
    }

    /// Hand this thread's buffered records over without waiting for it to fill
    ///
    /// Called when a shard shuts down, so the tail of a run is not lost.
    pub fn flush() {
        LOCAL.with(|local| flush_local(&mut local.borrow_mut()));
    }

    /// Take every record collected so far
    ///
    /// This only sees what threads have already handed over, so it has to be called after the
    /// pool has exited. Called before then it returns a partial profile with no sign that it
    /// is one.
    #[must_use]
    pub fn drain_stage_records() -> Vec<StageRecord> {
        // take everything that has been handed over
        let mut records = match collected().lock() {
            Ok(mut collected) => std::mem::take(&mut *collected),
            // a poisoned collector still holds every record written before the panic
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        // drop anything left over from a phase that was thrown away
        //
        // a warmup's records trickle in long after the warmup ends, since threads buffer
        // before handing over. Without this they would arrive as unjoinable records and make
        // a healthy run look like one that lost half its client side.
        let epoch = EPOCH.load(Ordering::SeqCst);
        records.retain(|record| record.epoch == epoch);
        records
    }
}

#[cfg(feature = "stage-profile")]
pub use sink::{drain_stage_records, emit, flush, reset, SAMPLE_ENV};

/// Record one query's journey, which does nothing in this build
///
/// # Arguments
///
/// * `id` - Ignored, since nothing is collected here
/// * `stamps` - Ignored, for the same reason
#[cfg(not(feature = "stage-profile"))]
#[inline(always)]
pub fn emit(_id: Uuid, _stamps: StageStamps) {}

/// Hand this thread's records over, which does nothing in this build
#[cfg(not(feature = "stage-profile"))]
#[inline(always)]
pub fn flush() {}

#[cfg(test)]
mod tests {
    use super::{Offset, Stamp};

    #[test]
    /// An unset stage is not a stage that took no time
    fn unset_is_not_zero() {
        // a stage that landed in the same nanosecond as its base is a real measurement
        let base = Stamp::now();
        let immediate = Offset::between(base, base);
        assert_eq!(immediate.nanos(), Some(0));
        assert!(immediate.is_set());
        // a stage nothing reached is not, and must not read as a fast one
        assert!(!Offset::UNSET.is_set());
        assert_eq!(Offset::UNSET.nanos(), None);
        // which is also what defaulting gives us
        assert!(!Offset::default().is_set());
    }

    #[test]
    /// An offset too large to hold is saturated rather than truncated
    fn a_long_stage_saturates() {
        // a truncated offset is indistinguishable from a genuinely fast stage, so the
        // sentinel is kept apart from both a real measurement and from unset
        assert!(Offset::SATURATED.is_saturated());
        assert_eq!(Offset::SATURATED.nanos(), None);
        assert!(Offset::SATURATED.is_set());
        // and it is not the same value as unset, or a report could not tell them apart
        assert_ne!(Offset::SATURATED, Offset::UNSET);
    }

    #[test]
    /// A stamp taken later never measures as earlier
    fn stamps_are_monotonic() {
        let first = Stamp::now();
        let second = Stamp::now();
        // the clock only moves forward, and a stamp measured against a later one saturates
        // to zero rather than wrapping into a huge interval
        assert!(second.since(first) < u64::MAX);
        assert_eq!(first.since(second), 0);
    }

    #[test]
    /// Measuring the clock's own cost gives a usable floor
    fn clock_overhead_is_measurable() {
        // several stages here are queue hops of the same order as a clock read, so the
        // report needs this number to mark them as being at the floor
        let overhead = Stamp::measure_overhead(1024);
        // a read that measured as free would mean the clock has no resolution at all
        assert!(overhead > 0, "a clock read should cost something");
        // and one that took a microsecond means we are not on a vDSO clock
        assert!(
            overhead < 1_000,
            "a clock read should not cost {overhead}ns"
        );
        // asking for nothing measures nothing rather than panicking on an empty median
        assert_eq!(Stamp::measure_overhead(0), 0);
    }
}
