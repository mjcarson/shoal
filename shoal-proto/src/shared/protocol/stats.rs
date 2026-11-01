//! What a cluster's nodes are doing, as the `Stats` admin read answers it
//!
//! ```text
//!  shard ── GroupReport.writes (cumulative) ──▶ node control thread ── EWMA ──▶ NodeStats
//!                                                        │
//!                                    StatusReport.stats  ▼
//!                                              control leader ── Stats ──▶ ClusterStatsView
//! ```
//!
//! Every counter here is cumulative since the shard that holds it started, and every rate is a
//! trailing estimate the node derives from the counters' change between two of its report ticks
//! ([F52](../../../../docs/src/features/cluster-stats.md)). Nothing here is committed: a node's
//! figures ride its status report to the control leader, which keeps the newest of each in
//! memory, so a figure is always as old as the report it came in and the view says how old.
//!
//! The bodies are JSON like every other admin read, and every field decodes from a frame that
//! leaves it out, so a build from before a field reads a newer frame and the other way round.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::shared::identity::NodeId;
use crate::shared::responses::ResponseActionNames;

/// The kinds a node's answers to its clients are counted by, in the order every array of them
/// is kept ([F65](../../../../docs/src/features/query-figures-home-tab.md))
///
/// The five query kinds a client can send, `error` for an answer that failed, whatever the
/// query was: a failure is read out of the answer, which no longer says what was asked. And
/// `refused` for a conditional write whose condition did not hold, read out of the answer the
/// same way ([F68](../../../../docs/src/features/conditional-writes.md)). A conditional write that
/// was applied is counted as the insert, update or delete it was.
pub const QUERY_OPS: [&str; 7] = [
    "get", "exists", "insert", "update", "delete", "error", "refused",
];

/// The kinds among [`QUERY_OPS`] that read a table
pub const READ_OPS: [&str; 2] = ["get", "exists"];

/// The kinds among [`QUERY_OPS`] that change a table
pub const WRITE_OPS: [&str; 3] = ["insert", "update", "delete"];

/// Where an answer of a kind is counted among [`QUERY_OPS`]
///
/// # Arguments
///
/// * `kind` - The kind the answer says it is
#[must_use]
pub fn query_op_index(kind: &ResponseActionNames) -> usize {
    match kind {
        ResponseActionNames::Get => 0,
        ResponseActionNames::Exists => 1,
        ResponseActionNames::Insert => 2,
        ResponseActionNames::Update => 3,
        ResponseActionNames::Delete => 4,
        ResponseActionNames::Error => 5,
        ResponseActionNames::Refused => 6,
    }
}

/// How many rows and bytes a group's writes have applied, since its shard started
///
/// Counted once per committed command on every replica that applies it. The bytes are the
/// replicated intent's size - the row for an insert or update, the key for a delete - which is
/// what the log carries, not what the archives end up holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WriteCounters {
    /// Rows inserted
    pub inserts: u64,
    /// Rows updated
    pub updates: u64,
    /// Rows deleted
    pub deletes: u64,
    /// Bytes of the inserts' intents
    pub insert_bytes: u64,
    /// Bytes of the updates' intents
    pub update_bytes: u64,
    /// Bytes of the deletes' intents
    pub delete_bytes: u64,
    /// Updates and deletes that found no row to change
    pub misses: u64,
}

impl WriteCounters {
    /// Add another set of counters to this one
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &WriteCounters) {
        // every counter is summed on its own
        self.inserts = self.inserts.saturating_add(other.inserts);
        self.updates = self.updates.saturating_add(other.updates);
        self.deletes = self.deletes.saturating_add(other.deletes);
        self.insert_bytes = self.insert_bytes.saturating_add(other.insert_bytes);
        self.update_bytes = self.update_bytes.saturating_add(other.update_bytes);
        self.delete_bytes = self.delete_bytes.saturating_add(other.delete_bytes);
        self.misses = self.misses.saturating_add(other.misses);
    }

    /// What these counters gained since an earlier reading of them, or `None` if any went back
    ///
    /// A counter going backwards means the shard holding it started again, which the caller
    /// treats as a reset rather than as a negative rate.
    ///
    /// # Arguments
    ///
    /// * `prev` - The earlier reading
    #[must_use]
    pub fn delta(&self, prev: &WriteCounters) -> Option<WriteCounters> {
        // any counter below its earlier reading is a restart of the shard it lives on
        Some(WriteCounters {
            inserts: self.inserts.checked_sub(prev.inserts)?,
            updates: self.updates.checked_sub(prev.updates)?,
            deletes: self.deletes.checked_sub(prev.deletes)?,
            insert_bytes: self.insert_bytes.checked_sub(prev.insert_bytes)?,
            update_bytes: self.update_bytes.checked_sub(prev.update_bytes)?,
            delete_bytes: self.delete_bytes.checked_sub(prev.delete_bytes)?,
            misses: self.misses.checked_sub(prev.misses)?,
        })
    }

    /// Whether nothing has been counted
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == WriteCounters::default()
    }
}

/// The hops a node took for its clients' queries, since its shards started
///
/// A query a client sends to the node that serves it takes none of them; one sent anywhere
/// else is forwarded to a holder, or proposed through a leader on another node, or waits on a
/// read barrier another node grants. A client routing by topology exists to keep all three at
/// zero, and these are what show whether it does
/// ([F74](../../../../docs/src/features/client-routing.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct HopCounters {
    /// Queries and shares this node forwarded to another node, which held no copy here
    pub forwarded: u64,
    /// Writes this node proposed through their group's leader on another node
    pub proposals_hopped: u64,
    /// Strong reads whose read barrier this node asked of a leader on another node
    pub barriers_hopped: u64,
}

impl HopCounters {
    /// Add another shard's counters to these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &HopCounters) {
        // every counter is summed on its own
        self.forwarded = self.forwarded.saturating_add(other.forwarded);
        self.proposals_hopped = self.proposals_hopped.saturating_add(other.proposals_hopped);
        self.barriers_hopped = self.barriers_hopped.saturating_add(other.barriers_hopped);
    }

    /// What these counters gained since an earlier reading, a counter below its earlier reading
    /// read whole
    ///
    /// A counter going backwards means its shard started again, which counts from zero.
    ///
    /// # Arguments
    ///
    /// * `prev` - The earlier reading
    #[must_use]
    pub fn since(&self, prev: &HopCounters) -> HopCounters {
        // a counter that went backwards belongs to a shard that started again
        let gained = |now: u64, then: u64| now.checked_sub(then).unwrap_or(now);
        HopCounters {
            forwarded: gained(self.forwarded, prev.forwarded),
            proposals_hopped: gained(self.proposals_hopped, prev.proposals_hopped),
            barriers_hopped: gained(self.barriers_hopped, prev.barriers_hopped),
        }
    }

    /// Whether nothing has been counted
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == HopCounters::default()
    }
}

/// Where a node's replication lanes were read, since its shards started
/// ([F84](../../../../docs/src/features/replication-lane-handoff.md))
///
/// A replication lane names the slot its requests are for in its hello, and the shard that
/// accepts it hands it to the executor hosting that slot, so every append is read where it is
/// applied. These say whether that happened: on a node whose peers all name a slot,
/// `requests_crossed` stays at zero, and a lane from a build before F84 is `unnamed` and keeps
/// the hop every lane took before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationLanes {
    /// Lanes this node accepted on one executor and handed to the executor hosting their slot
    pub handed_out: u64,
    /// Lanes an executor took in from the one that accepted them
    pub taken_in: u64,
    /// Lanes accepted by the executor that hosts their slot, served where they landed
    pub in_place: u64,
    /// Lanes whose hello named no slot, or one this node does not have, served where they landed
    pub unnamed: u64,
    /// Requests read on the executor hosting the slot they name
    pub requests_here: u64,
    /// Requests read on another executor and handed to their slot's host as a buffer
    pub requests_crossed: u64,
    /// The payload bytes of those crossing requests
    pub bytes_crossed: u64,
}

impl ReplicationLanes {
    /// Add another shard's counters to these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &ReplicationLanes) {
        // every counter is summed on its own
        self.handed_out = self.handed_out.saturating_add(other.handed_out);
        self.taken_in = self.taken_in.saturating_add(other.taken_in);
        self.in_place = self.in_place.saturating_add(other.in_place);
        self.unnamed = self.unnamed.saturating_add(other.unnamed);
        self.requests_here = self.requests_here.saturating_add(other.requests_here);
        self.requests_crossed = self.requests_crossed.saturating_add(other.requests_crossed);
        self.bytes_crossed = self.bytes_crossed.saturating_add(other.bytes_crossed);
    }

    /// Whether nothing has been counted, which is a standalone node or a build before F84
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == ReplicationLanes::default()
    }
}

/// What a node's clients cancelled and what that saved, since its shards started
/// ([F75](../../../../docs/src/features/client-cancel.md))
///
/// Counted where each thing happens: a cancel read off a client's socket on the shard that
/// coordinates its connection, a query answered `Cancelled` on the shard that would have run it,
/// an answer or a stream left unwritten in the relay that would have written it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CancelCounters {
    /// Cancels this node's clients sent it
    pub received: u64,
    /// Cancels this node passed to another node that held shares of the bundle
    pub forwarded: u64,
    /// Queries this node answered `Cancelled` instead of running, its own clients' or a peer's
    pub refused: u64,
    /// Answers the relays dropped because their bundle was cancelled
    pub dropped: u64,
    /// Bytes of those answers that were never written
    pub dropped_bytes: u64,
    /// Streamed answers cut after their first frame was written
    pub cut: u64,
    /// Cancels the node's board was too full to record, which stopped answers and no work
    pub unrecorded: u64,
}

impl CancelCounters {
    /// Add another shard's counters to these
    ///
    /// # Arguments
    ///
    /// * `other` - The counters to add
    pub fn absorb(&mut self, other: &CancelCounters) {
        // every counter is summed on its own
        self.received = self.received.saturating_add(other.received);
        self.forwarded = self.forwarded.saturating_add(other.forwarded);
        self.refused = self.refused.saturating_add(other.refused);
        self.dropped = self.dropped.saturating_add(other.dropped);
        self.dropped_bytes = self.dropped_bytes.saturating_add(other.dropped_bytes);
        self.cut = self.cut.saturating_add(other.cut);
        self.unrecorded = self.unrecorded.saturating_add(other.unrecorded);
    }

    /// What these counters gained since an earlier reading, a counter below its earlier reading
    /// read whole
    ///
    /// A counter going backwards means its shard started again, which counts from zero.
    ///
    /// # Arguments
    ///
    /// * `prev` - The earlier reading
    #[must_use]
    pub fn since(&self, prev: &CancelCounters) -> CancelCounters {
        // a counter that went backwards belongs to a shard that started again
        let gained = |now: u64, then: u64| now.checked_sub(then).unwrap_or(now);
        CancelCounters {
            received: gained(self.received, prev.received),
            forwarded: gained(self.forwarded, prev.forwarded),
            refused: gained(self.refused, prev.refused),
            dropped: gained(self.dropped, prev.dropped),
            dropped_bytes: gained(self.dropped_bytes, prev.dropped_bytes),
            cut: gained(self.cut, prev.cut),
            unrecorded: gained(self.unrecorded, prev.unrecorded),
        }
    }

    /// Whether nothing has been counted
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == CancelCounters::default()
    }
}

/// A rate as three trailing estimates, per second
///
/// Each is an exponentially weighted moving average with the time constant its name says, so
/// `r10s` answers a change within seconds and `r5m` smooths over the minutes a rebalance takes.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Rates {
    /// Over roughly the last ten seconds
    pub r10s: f64,
    /// Over roughly the last minute
    pub r1m: f64,
    /// Over roughly the last five minutes
    pub r5m: f64,
}

impl Rates {
    /// Add another set of rates to this one
    ///
    /// # Arguments
    ///
    /// * `other` - The rates to add
    pub fn absorb(&mut self, other: &Rates) {
        // an average of a sum is the sum of the averages
        self.r10s += other.r10s;
        self.r1m += other.r1m;
        self.r5m += other.r5m;
    }

    /// Whether every window reads zero
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.r10s == 0.0 && self.r1m == 0.0 && self.r5m == 0.0
    }
}

/// The rate of every write counter, per second
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WriteRates {
    /// Rows inserted
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub inserts: Rates,
    /// Rows updated
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub updates: Rates,
    /// Rows deleted
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub deletes: Rates,
    /// Bytes inserted
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub insert_bytes: Rates,
    /// Bytes updated
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub update_bytes: Rates,
    /// Bytes deleted
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub delete_bytes: Rates,
    /// Updates and deletes that found no row
    #[serde(skip_serializing_if = "Rates::is_zero")]
    pub misses: Rates,
}

impl WriteRates {
    /// Add another set of rates to this one
    ///
    /// # Arguments
    ///
    /// * `other` - The rates to add
    pub fn absorb(&mut self, other: &WriteRates) {
        // every rate is summed on its own
        self.inserts.absorb(&other.inserts);
        self.updates.absorb(&other.updates);
        self.deletes.absorb(&other.deletes);
        self.insert_bytes.absorb(&other.insert_bytes);
        self.update_bytes.absorb(&other.update_bytes);
        self.delete_bytes.absorb(&other.delete_bytes);
        self.misses.absorb(&other.misses);
    }

    /// Whether every rate reads zero
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.inserts.is_zero()
            && self.updates.is_zero()
            && self.deletes.is_zero()
            && self.insert_bytes.is_zero()
            && self.update_bytes.is_zero()
            && self.delete_bytes.is_zero()
            && self.misses.is_zero()
    }
}

/// What a node answered its clients of one kind
///
/// Counted where a client's query arrived and was answered, so a query is counted once however
/// many shards and nodes served it, and on the node the client reached rather than the ones that
/// hold its rows ([F65](../../../../docs/src/features/query-figures-home-tab.md)).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OpStats {
    /// The kind, one of [`QUERY_OPS`]
    pub op: String,
    /// Answers per second
    pub rate: Rates,
    /// Bytes of those answers per second, as written to the clients
    pub bytes_out: Rates,
    /// The median time from a query's bundle arriving to its answer being written, in
    /// milliseconds, over roughly the last ten seconds; none when too few were timed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<f64>,
    /// The 99th percentile of the same
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_ms: Option<f64>,
    /// Answers since the node's shards started
    pub answers_total: u64,
    /// Bytes of those answers since the node's shards started
    pub bytes_out_total: u64,
}

/// What a node answered its clients, by kind, and how long they waited
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryStats {
    /// Every kind the node has answered, in the order of [`QUERY_OPS`]
    pub ops: Vec<OpStats>,
    /// Bytes of the bundles the node's clients sent, per second
    pub bytes_in: Rates,
    /// Bytes of those bundles since the node's shards started
    pub bytes_in_total: u64,
    /// The median wait over every kind but `error`, in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<f64>,
    /// The 99th percentile of the same
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_ms: Option<f64>,
    /// One in how many bundles was timed; every answer is counted whatever this is
    pub sampled_every: u32,
}

impl QueryStats {
    /// Whether these are no figures at all, which is what a build from before F65 sends
    ///
    /// A node from F65 on always names its sampling, one or more, even before any client has
    /// sent it anything, so an idle node and an older build are told apart by this alone.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sampled_every == 0 && self.ops.is_empty() && self.bytes_in_total == 0
    }

    /// One kind's figures, if the node has answered any of it
    ///
    /// # Arguments
    ///
    /// * `op` - The kind, one of [`QUERY_OPS`]
    #[must_use]
    pub fn op(&self, op: &str) -> Option<&OpStats> {
        self.ops.iter().find(|stats| stats.op == op)
    }

    /// The answers per second of several kinds together, over their ten second windows
    ///
    /// # Arguments
    ///
    /// * `ops` - The kinds, each one of [`QUERY_OPS`]
    #[must_use]
    pub fn rate_of(&self, ops: &[&str]) -> f64 {
        // folded from a positive zero, since an empty sum of floats is a negative one
        ops.iter()
            .filter_map(|op| self.op(op))
            .fold(0.0, |sum, stats| sum + stats.rate.r10s)
    }

    /// The answer bytes per second of several kinds together, over their ten second windows
    ///
    /// # Arguments
    ///
    /// * `ops` - The kinds, each one of [`QUERY_OPS`]
    #[must_use]
    pub fn bytes_out_of(&self, ops: &[&str]) -> f64 {
        // folded from a positive zero, since an empty sum of floats is a negative one
        ops.iter()
            .filter_map(|op| self.op(op))
            .fold(0.0, |sum, stats| sum + stats.bytes_out.r10s)
    }
}

/// The hops a node took for its clients' queries, per second and since its shards started
/// ([F74](../../../../docs/src/features/client-routing.md))
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct HopStats {
    /// Queries and shares forwarded to another node, per second
    pub forwarded: Rates,
    /// Writes proposed through a leader on another node, per second
    pub proposals_hopped: Rates,
    /// Read barriers asked of a leader on another node, per second
    pub barriers_hopped: Rates,
    /// Every hop since the node's shards started
    pub totals: HopCounters,
}

impl HopStats {
    /// Whether these are no figures at all: a build from before F74, or a node nothing hopped on
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.totals.is_zero()
            && self.forwarded.is_zero()
            && self.proposals_hopped.is_zero()
            && self.barriers_hopped.is_zero()
    }

    /// Every hop per second together, over the ten second windows
    #[must_use]
    pub fn per_sec(&self) -> f64 {
        self.forwarded.r10s + self.proposals_hopped.r10s + self.barriers_hopped.r10s
    }
}

/// What a node's clients cancelled, per second and since its shards started
/// ([F75](../../../../docs/src/features/client-cancel.md))
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CancelStats {
    /// Cancels the node's clients sent it, per second
    pub received: Rates,
    /// Queries the node answered `Cancelled` instead of running, per second
    pub refused: Rates,
    /// Answer bytes the node's relays left unwritten, per second
    pub dropped_bytes: Rates,
    /// Everything counted since the node's shards started
    pub totals: CancelCounters,
}

impl CancelStats {
    /// Whether these are no figures at all: a build from before F75, or a node nothing was
    /// cancelled on
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.totals.is_zero()
            && self.received.is_zero()
            && self.refused.is_zero()
            && self.dropped_bytes.is_zero()
    }
}

/// What one node holds and does for one table, or for every table together
///
/// Every figure is counted twice: over every copy the node hosts, and over the copies whose
/// group it leads. A cluster's hosted figures count a row once per replica; its led figures
/// count it once, which is what a sum across the members should use.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableStats {
    /// The table, by the name the schema spells it; empty for a node's total
    pub table: String,
    /// Tablet groups the node hosts
    pub groups: u64,
    /// Tablet groups the node leads
    pub groups_led: u64,
    /// Tablets the node hosts
    pub tablets: u64,
    /// Tablets the node leads
    pub tablets_led: u64,
    /// Partitions the node's archives hold; unarchived writes are not counted until compacted
    pub partitions: u64,
    /// Partitions the archives of the groups it leads hold
    pub partitions_led: u64,
    /// Partitions the node's archives hold as a base with fragments over it
    pub chained: u64,
    /// Bytes the node's archives hold
    pub bytes: u64,
    /// Bytes the archives of the groups it leads hold
    pub bytes_led: u64,
    /// The write rates over every copy the node hosts
    #[serde(skip_serializing_if = "WriteRates::is_zero")]
    pub applied: WriteRates,
    /// The write rates over the copies whose group it leads
    #[serde(skip_serializing_if = "WriteRates::is_zero")]
    pub led: WriteRates,
    /// The write counters over every copy the node hosts
    #[serde(skip_serializing_if = "WriteCounters::is_zero")]
    pub applied_total: WriteCounters,
    /// The write counters over the copies whose group it leads
    #[serde(skip_serializing_if = "WriteCounters::is_zero")]
    pub led_total: WriteCounters,
}

impl TableStats {
    /// Add another table's figures to this one
    ///
    /// # Arguments
    ///
    /// * `other` - The figures to add
    pub fn absorb(&mut self, other: &TableStats) {
        // placement and size figures are summed
        self.groups += other.groups;
        self.groups_led += other.groups_led;
        self.tablets += other.tablets;
        self.tablets_led += other.tablets_led;
        self.partitions += other.partitions;
        self.partitions_led += other.partitions_led;
        self.chained += other.chained;
        self.bytes += other.bytes;
        self.bytes_led += other.bytes_led;
        // and so are rates and counters
        self.applied.absorb(&other.applied);
        self.led.absorb(&other.led);
        self.applied_total.absorb(&other.applied_total);
        self.led_total.absorb(&other.led_total);
    }

    /// Whether the table holds nothing on the node and nothing was ever written to it there
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.groups == 0
            && self.partitions == 0
            && self.bytes == 0
            && self.applied_total.is_zero()
            && self.applied.is_zero()
    }
}

/// What one node holds and does, as the node itself last derived it
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeStats {
    /// The node
    pub node: NodeId,
    /// When the node derived these, in milliseconds since the epoch by its own clock
    #[serde(default)]
    pub at_ms: u64,
    /// How long the node has been deriving rates, in milliseconds; a window longer than this
    /// is still warming up
    #[serde(default)]
    pub observed_ms: u64,
    /// How many shards reported
    #[serde(default)]
    pub shards: u64,
    /// The name the node's machine gives itself (`gethostname`), which is how the admin tools
    /// name the member ([F64](../../../../docs/src/features/stats-tui.md))
    ///
    /// Empty from a build before F64, or when the node could not read it; a reader falls back
    /// to another name for the member then.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    /// Every table with anything to say, by name
    #[serde(default)]
    pub tables: Vec<TableStats>,
    /// Every table together
    #[serde(default)]
    pub total: TableStats,
    /// Snapshot bytes the node streamed out, per second
    #[serde(default)]
    pub stream_sent: Rates,
    /// Snapshot bytes the node streamed in, per second
    #[serde(default)]
    pub stream_received: Rates,
    /// Snapshot bytes the node streamed out since its shards started
    #[serde(default)]
    pub stream_sent_total: u64,
    /// Snapshot bytes the node streamed in since its shards started
    #[serde(default)]
    pub stream_received_total: u64,
    /// The free bytes on the node's tightest storage root, zero when it could not read them
    ///
    /// The root that fills first is the one that stops the node taking a move; every root's
    /// figures are in the node's status report and the leader's `Members` answer
    /// ([F82](../../../../docs/src/features/root-capacity.md)).
    #[serde(default)]
    pub free_bytes: u64,
    /// Bytes every volatile group's log holds in memory
    #[serde(default)]
    pub volatile_bytes: u64,
    /// The groups this node leads that wrote the most over the last interval, busiest first
    ///
    /// Every member applies every write, so the applied rates cannot say which leader does the
    /// work; a leader's own groups' rates can
    /// ([O64](../../../../docs/src/appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)).
    #[serde(default)]
    pub hot_groups: Vec<GroupRate>,
    /// Bytes of rows the shards hold in memory, which their eviction budgets bound
    #[serde(default)]
    pub memory_bytes: u64,
    /// The shards' eviction budgets together
    #[serde(default)]
    pub memory_budget: u64,
    /// Bytes the shards' archive maps hold in memory, which no budget counts
    ///
    /// Since F76 the map is paged: this is each map's delta, its runs' directories and filters,
    /// and its cached index pages, bounded by its settings apart from the filters' bits a key.
    #[serde(default)]
    pub archive_map_bytes: u64,
    /// Bytes the shards' tables' partition indexes hold, which no budget counts
    #[serde(default)]
    pub table_index_bytes: u64,
    /// Bytes the shards' WAL indexes of their retained entries hold, which no budget counts
    #[serde(default)]
    pub wal_index_bytes: u64,
    /// Bytes the shards' eviction lists hold, an entry per evictable partition, which no
    /// budget counts
    #[serde(default)]
    pub lru_bytes: u64,
    /// The process's resident memory: the rows, and everything the eviction budget does not
    /// count - the archive maps, the logs' caches, the groups' state and every buffer
    #[serde(default)]
    pub resident_bytes: u64,
    /// WAL batches synced per second over the last interval, one `fdatasync` each
    #[serde(default)]
    pub wal_syncs_per_sec: f64,
    /// WAL bytes synced per second over the last interval
    #[serde(default)]
    pub wal_bytes_per_sec: f64,
    /// Segments the shards' WALs hold
    #[serde(default)]
    pub wal_segments: u64,
    /// Sealed segments handed to a compactor and not yet merged by every table in them: the
    /// compactors' backlog, in segments
    #[serde(default)]
    pub compacting_segments: u64,
    /// Entries committed and not yet applied, over every copy the node hosts
    #[serde(default)]
    pub apply_lag: u64,
    /// Bytes proposed through the node and not yet answered
    #[serde(default)]
    pub pending_bytes: u64,
    /// Each shard's writes applied per second over the last interval, by shard
    ///
    /// A shard is one core, and every copy it hosts applies on it, so a node whose busiest
    /// shard applies far more than its others is paced by that one core
    /// ([O64](../../../../docs/src/appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)).
    #[serde(default)]
    pub shard_writes_per_sec: Vec<f64>,
    /// How many groups each shard leads, by shard
    ///
    /// A leader proposes, replicates and answers for its group on its shard's core, so the
    /// leads a node holds can sit on a few of its cores however evenly the node's share is
    /// counted (O64).
    #[serde(default)]
    pub shard_groups_led: Vec<u32>,
    /// The mean time one WAL batch took to write and sync over the last interval, in
    /// milliseconds
    ///
    /// A device that flushes its cache on every sync shows here, and so does one that is
    /// failing (O64).
    #[serde(default)]
    pub wal_sync_ms: f64,
    /// The mean appends one WAL sync carried over the last interval
    #[serde(default)]
    pub wal_appends_per_sync: f64,
    /// The share of the last interval's WAL syncs in each size bucket: under 4 KiB, 16 KiB,
    /// 64 KiB, 256 KiB and 1 MiB, and the rest
    ///
    /// A group commit that settles on small batches shows as the first buckets filling (O64).
    #[serde(default)]
    pub wal_sync_sizes: Vec<f64>,
    /// What the node answered its clients, by kind, and how long they waited
    /// ([F65](../../../../docs/src/features/query-figures-home-tab.md))
    ///
    /// Empty from a build before F65, which is the only build that leaves it out of the frame:
    /// a node from F65 on sends it even when no client has sent it anything.
    #[serde(default, skip_serializing_if = "QueryStats::is_empty")]
    pub queries: QueryStats,
    /// The hops the node took for its clients' queries
    /// ([F74](../../../../docs/src/features/client-routing.md))
    ///
    /// Left out of the frame while nothing has hopped, so a node no client ever sent elsewhere
    /// writes what a build from before F74 did.
    #[serde(default, skip_serializing_if = "HopStats::is_empty")]
    pub hops: HopStats,
    /// Where the node's replication lanes were read, since its shards started
    /// ([F84](../../../../docs/src/features/replication-lane-handoff.md))
    ///
    /// Left out of the frame while nothing was counted, as a standalone node's are.
    #[serde(default, skip_serializing_if = "ReplicationLanes::is_empty")]
    pub replication_lanes: ReplicationLanes,
    /// What the node's clients cancelled, and the work and bytes that saved
    /// ([F75](../../../../docs/src/features/client-cancel.md))
    ///
    /// Left out of the frame while nothing was cancelled, so a node no client ever cancelled on
    /// writes what a build from before F75 did.
    #[serde(default, skip_serializing_if = "CancelStats::is_empty")]
    pub cancels: CancelStats,
    /// What the node's recovery found and did, by pool
    /// ([F88](../../../../docs/src/features/recovery-and-moves.md))
    ///
    /// Stale and overflowed positions are counted from the records of the groups the node leads,
    /// so each stale position is counted on one member; the rest since the node started.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub recovery: std::collections::BTreeMap<String, RecoveryFigures>,
    /// What the node's object executors did with the erasure code
    /// ([F90](../../../../docs/src/features/erasure-coded-pools.md))
    ///
    /// Left out of the frame while nothing was coded, as a node of replicated pools alone is.
    #[serde(default, skip_serializing_if = "CodecFigures::is_empty")]
    pub codec: CodecFigures,
    /// What each bucket holds and what its clients were answered, by bucket
    /// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub buckets: std::collections::BTreeMap<String, BucketFigures>,
    /// The rows the shards evicted under memory pressure since they started, by what began each
    /// pass ([F104](../../../../docs/src/features/memory-in-a-capture.md))
    ///
    /// Left out of the frame while nothing was evicted, and zero from a build before F104.
    #[serde(default, skip_serializing_if = "EvictionFigures::is_zero")]
    pub evictions: EvictionFigures,
    /// The node's walk pages and scans since it started, by class
    /// ([M23](../../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load))
    ///
    /// Left out of the frame while the node walked nothing, and zero from a build before M23.
    #[serde(default, skip_serializing_if = "ScanFigures::is_zero")]
    pub scans: ScanFigures,
    /// What the node's cache of read units did since it started, summed over its object
    /// executors ([F102](../../../../docs/src/features/read-unit-cache.md), F104)
    ///
    /// Left out of the frame on a node that keeps none, and zero from a build before F104.
    #[serde(default, skip_serializing_if = "CacheReport::is_zero")]
    pub object_cache: CacheReport,
    /// The object work the node had in flight when it reported, and what it refused or gave up
    /// since it started ([F108](../../../../docs/src/features/object-work-in-flight.md))
    ///
    /// Left out of the frame on a node with no object executors, and zero from a build before
    /// F108.
    #[serde(default, skip_serializing_if = "ObjectWork::is_zero")]
    pub objects: ObjectWork,
}

/// What began a shard's pass of evictions ([F104](../../../../docs/src/features/memory-in-a-capture.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// The shard's own rows, as it counts them, past its budget
    Budget,
    /// The process's resident memory past the node's, read at most every quarter second
    Resident,
}

/// The rows a node's shards evicted under memory pressure, by what began each pass
/// ([F104](../../../../docs/src/features/memory-in-a-capture.md))
///
/// A shard evicts when its rows pass its own budget or when the process's resident memory passes
/// the node's. The second binds on the lab, where a shard's count of its rows is a tenth of what
/// the process holds. Each pass frees about two fifths of the rows the shard counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EvictionFigures {
    /// Passes begun by a shard's rows past its own budget
    pub budget_passes: u64,
    /// The partitions those passes evicted
    pub budget_partitions: u64,
    /// The bytes of rows those passes evicted, as the shards count rows
    pub budget_bytes: u64,
    /// Passes begun by the process's resident memory past the node's
    pub resident_passes: u64,
    /// The partitions those passes evicted
    pub resident_partitions: u64,
    /// The bytes of rows those passes evicted, as the shards count rows
    pub resident_bytes: u64,
    /// Checks that found the process past the node's memory and had its caches of read units give
    /// units back instead of evicting rows ([F107](../../../../docs/src/features/cache-default-share.md))
    pub cache_yields: u64,
}

impl EvictionFigures {
    /// Count one pass of evictions under what began it
    ///
    /// # Arguments
    ///
    /// * `pressure` - What began the pass
    /// * `partitions` - The partitions it evicted
    /// * `bytes` - The bytes of rows it evicted
    pub fn count(&mut self, pressure: MemoryPressure, partitions: u64, bytes: u64) {
        // the pass and what it freed, under what began it
        let (passes, evicted, freed) = match pressure {
            MemoryPressure::Budget => (
                &mut self.budget_passes,
                &mut self.budget_partitions,
                &mut self.budget_bytes,
            ),
            MemoryPressure::Resident => (
                &mut self.resident_passes,
                &mut self.resident_partitions,
                &mut self.resident_bytes,
            ),
        };
        *passes += 1;
        *evicted += partitions;
        *freed += bytes;
    }

    /// Add another shard's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The other shard's figures
    pub fn absorb(&mut self, other: &EvictionFigures) {
        self.budget_passes += other.budget_passes;
        self.budget_partitions += other.budget_partitions;
        self.budget_bytes += other.budget_bytes;
        self.resident_passes += other.resident_passes;
        self.resident_partitions += other.resident_partitions;
        self.resident_bytes += other.resident_bytes;
        self.cache_yields += other.cache_yields;
    }

    /// Whether no pass evicted anything
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == EvictionFigures::default()
    }
}

/// The steps of a loop's hold a figure counts past, in microseconds: W12's budget fails a scan whose
/// step passes the first ([M23](../../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load))
pub const STEP_BOUNDS_US: [u64; 3] = [100, 1_000, 10_000];

/// The kinds of step a walk page holds its shard's loop for
/// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepKind {
    /// A page's capture: its tablet's resident keys looked at, its own selected, each captured or
    /// planned, and its archive's view taken
    Capture,
    /// An encode step: one planned partition looked up again and a piece of it, the whole of it or
    /// its archive's bytes taken
    Encode,
}

/// One step a walk page held its shard's loop for, as its clock read it
/// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StepSample {
    /// How long it held the loop by the wall, in nanoseconds
    pub wall_ns: u64,
    /// How much of that its thread spent on its own cpu, in nanoseconds, never more than its wall:
    /// the rest the thread was off its cpu, preempted by another thread or the kernel
    pub cpu_ns: u64,
    /// The resident keys it looked at: every resident key of a capture's tablet, one for an
    /// encode step's fresh lookup
    pub keys: u64,
    /// The rows it copied off the loop: a piece's, a whole partition's live rows; none for a
    /// planned page's capture, which copies nothing
    pub rows: u64,
    /// The bytes it copied off the loop
    pub bytes: u64,
}

impl StepSample {
    /// The whole microseconds it held the loop by the wall, as every step bound is judged
    #[must_use]
    pub fn wall_us(&self) -> u64 {
        self.wall_ns / 1_000
    }

    /// The whole microseconds it spent on its own cpu
    #[must_use]
    pub fn cpu_us(&self) -> u64 {
        self.cpu_ns / 1_000
    }
}

/// A run of steps of one kind summed: how many, their wall time and own cpu, and what they looked
/// at and copied ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StepTotals {
    /// Steps
    pub steps: u64,
    /// Their wall time, in nanoseconds
    pub wall_ns: u64,
    /// Their own cpu, in nanoseconds
    pub cpu_ns: u64,
    /// The resident keys they looked at
    pub keys: u64,
    /// The rows they copied off the loop
    pub rows: u64,
    /// The bytes they copied off the loop
    pub bytes: u64,
}

impl StepTotals {
    /// Add one step
    ///
    /// # Arguments
    ///
    /// * `sample` - The step, as its clock read it
    pub fn add(&mut self, sample: &StepSample) {
        // every figure saturates rather than wrap: they only grow
        self.steps = self.steps.saturating_add(1);
        self.wall_ns = self.wall_ns.saturating_add(sample.wall_ns);
        self.cpu_ns = self.cpu_ns.saturating_add(sample.cpu_ns);
        self.keys = self.keys.saturating_add(sample.keys);
        self.rows = self.rows.saturating_add(sample.rows);
        self.bytes = self.bytes.saturating_add(sample.bytes);
    }

    /// Add another shard's totals to these
    ///
    /// # Arguments
    ///
    /// * `other` - The other shard's totals
    pub fn absorb(&mut self, other: &StepTotals) {
        // every figure summed, saturating
        self.steps = self.steps.saturating_add(other.steps);
        self.wall_ns = self.wall_ns.saturating_add(other.wall_ns);
        self.cpu_ns = self.cpu_ns.saturating_add(other.cpu_ns);
        self.keys = self.keys.saturating_add(other.keys);
        self.rows = self.rows.saturating_add(other.rows);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

/// What one kind of a class's steps did since its node started: every step, those past W12's
/// 100 µs by the wall, the bounds they passed by the wall and by their own cpu, and the longest
/// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
///
/// A step's own cpu is never more than its wall, so a step past a bound by its own cpu is past it
/// by the wall too, and the steps past 100 µs by the wall alone, whose thread was off its cpu for
/// part of them, are `past[0] - past_cpu[0]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StepFigures {
    /// Every step
    pub all: StepTotals,
    /// The steps past the first of [`STEP_BOUNDS_US`] by the wall
    pub long: StepTotals,
    /// The steps past each of [`STEP_BOUNDS_US`] by the wall
    pub past: [u64; 3],
    /// The steps past each of [`STEP_BOUNDS_US`] by their own cpu
    pub past_cpu: [u64; 3],
    /// The longest step by the wall since the node started, whole
    pub longest: StepSample,
}

impl StepFigures {
    /// Count one step
    ///
    /// # Arguments
    ///
    /// * `sample` - The step, as its clock read it
    pub fn count(&mut self, sample: &StepSample) {
        // the step among every step, and among the long ones if it passed the first bound
        self.all.add(sample);
        let (wall_us, cpu_us) = (sample.wall_us(), sample.cpu_us());
        if wall_us > STEP_BOUNDS_US[0] {
            self.long.add(sample);
        }
        // the bounds it passed by the wall and by its own cpu, each judged in whole microseconds
        for (index, bound) in STEP_BOUNDS_US.into_iter().enumerate() {
            if wall_us > bound {
                self.past[index] = self.past[index].saturating_add(1);
            }
            if cpu_us > bound {
                self.past_cpu[index] = self.past_cpu[index].saturating_add(1);
            }
        }
        // and the longest, kept whole
        if sample.wall_ns > self.longest.wall_ns {
            self.longest = *sample;
        }
    }

    /// Add another shard's figures to these, the longest step kept whole from the longer
    ///
    /// # Arguments
    ///
    /// * `other` - The other shard's figures
    pub fn absorb(&mut self, other: &StepFigures) {
        // the totals and the bounds summed
        self.all.absorb(&other.all);
        self.long.absorb(&other.long);
        for (past, more) in self.past.iter_mut().zip(other.past) {
            *past = past.saturating_add(more);
        }
        for (past, more) in self.past_cpu.iter_mut().zip(other.past_cpu) {
            *past = past.saturating_add(more);
        }
        // and the longer of the two longest steps
        if other.longest.wall_ns > self.longest.wall_ns {
            self.longest = other.longest;
        }
    }

    /// The steps past 100 µs by the wall and not by their own cpu: held past the bound only because
    /// their thread was off its cpu for part of them
    #[must_use]
    pub fn past_by_wall_alone(&self) -> u64 {
        self.past[0].saturating_sub(self.past_cpu[0])
    }
}

/// What one class of a node's walk pages did since it started: a driver's, which the object store's
/// drivers and an operator's walk ask, or a scan's
/// ([M23](../../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load))
///
/// Every figure is a count since the node started, so a run's are its last sample's less its
/// first's; `longest_step_us` alone is the most since the start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WalkClassFigures {
    /// Pages answered
    pub pages: u64,
    /// Pages refused before they were read because the shard read as many of the class as it admits
    pub shed_full: u64,
    /// Pages refused for any other cause than a shed, a copy that could not answer, a tablet that
    /// moved, an install begun under the page; a page the scan budget shed is the node's
    /// `budget_shed`, which counts every draw it shed, a page's own included
    pub refused: u64,
    /// Answered pages that ended at their bound of partitions
    pub ended_limit: u64,
    /// Answered pages that ended at their bound of bytes
    pub ended_bytes: u64,
    /// Answered pages that ended short, their budget spent
    pub ended_short: u64,
    /// Answered pages that ended at the end of their range
    pub ended_range: u64,
    /// Partitions the pages read from memory
    pub resident: u64,
    /// Partitions the pages read from archives
    pub archived: u64,
    /// Partitions handed out
    pub partitions: u64,
    /// Live rows handed out
    pub rows: u64,
    /// Bytes handed out
    pub bytes: u64,
    /// Bytes copied off the loop from resident partitions
    pub resident_bytes: u64,
    /// Archive reads issued, a chain's records a read each
    pub reads: u64,
    /// Steps the pages held their loop for: captures, and since M23 encode steps
    pub steps: u64,
    /// Those of them that held it past each of [`STEP_BOUNDS_US`]
    pub steps_past: [u64; 3],
    /// The longest a step held its loop since the node started, in microseconds
    pub longest_step_us: u64,
    /// The steps that were pages' captures, each with its own cpu, keys and bytes
    /// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
    pub captures: StepFigures,
    /// The steps that were encode steps, each with its own cpu, rows and bytes (F130)
    pub encodes: StepFigures,
}

impl WalkClassFigures {
    /// Count a step of a kind as its clock read it: among every step, by the wall in whole
    /// microseconds as before F130, and among its kind's with its own cpu and what it held
    ///
    /// # Arguments
    ///
    /// * `kind` - What kind of step it was
    /// * `sample` - The step, as its clock read it
    pub fn step(&mut self, kind: StepKind, sample: &StepSample) {
        // the step, the bounds it passed, and the longest, by the wall alone
        let held_us = sample.wall_us();
        self.steps += 1;
        for (past, bound) in self.steps_past.iter_mut().zip(STEP_BOUNDS_US) {
            if held_us > bound {
                *past += 1;
            }
        }
        self.longest_step_us = self.longest_step_us.max(held_us);
        // and among its kind's, whole
        match kind {
            StepKind::Capture => self.captures.count(sample),
            StepKind::Encode => self.encodes.count(sample),
        }
    }

    /// Add another shard's figures to these, the longest step kept as the longer
    ///
    /// # Arguments
    ///
    /// * `other` - The other shard's figures
    pub fn absorb(&mut self, other: &WalkClassFigures) {
        // every count summed
        self.pages += other.pages;
        self.shed_full += other.shed_full;
        self.refused += other.refused;
        self.ended_limit += other.ended_limit;
        self.ended_bytes += other.ended_bytes;
        self.ended_short += other.ended_short;
        self.ended_range += other.ended_range;
        self.resident += other.resident;
        self.archived += other.archived;
        self.partitions += other.partitions;
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.resident_bytes += other.resident_bytes;
        self.reads += other.reads;
        self.steps += other.steps;
        for (past, more) in self.steps_past.iter_mut().zip(other.steps_past) {
            *past += more;
        }
        // and the longest step the longer of the two
        self.longest_step_us = self.longest_step_us.max(other.longest_step_us);
        // each kind's own figures (F130)
        self.captures.absorb(&other.captures);
        self.encodes.absorb(&other.encodes);
    }
}

/// A node's walk pages and its scans since it started, a class each, and the node's scan budget and
/// pace ([M23](../../../../docs/src/future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load),
/// [W10](../../../../docs/src/future-work/scans/api.md#node-counters))
///
/// Each shard counts its own pages; the budget and the pace are the node's, which every shard
/// reports alike, so a node's figures sum the classes and take the budget's and the pace's from
/// the shard that read the most.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ScanFigures {
    /// A driver's pages: the object store's drivers and an operator's walk
    pub driver: WalkClassFigures,
    /// A scan's pages
    pub scan: WalkClassFigures,
    /// The most bytes the node's scan budget held at once ([F118](../../../../docs/src/features/scan-budget.md))
    pub budget_peak: u64,
    /// The draws on it that were shed
    pub budget_shed: u64,
    /// The bytes scans read in their devices' idle time, every device of the node's
    /// ([F119](../../../../docs/src/features/scan-pacing.md))
    pub idle_bytes: u64,
    /// The bytes scans read on their trickle, beside a foreground read
    pub trickled_bytes: u64,
    /// The scans' busy time on their devices in microseconds, a device busy while any scan read is
    /// in flight on it, every device's summed ([F128](../../../../docs/src/features/device-time-pace.md))
    pub busy_us: u64,
}

impl ScanFigures {
    /// Add another shard's figures to these: its classes summed, and the node's budget and pace
    /// kept from whichever shard read them the later
    ///
    /// # Arguments
    ///
    /// * `other` - The other shard's figures
    pub fn absorb(&mut self, other: &ScanFigures) {
        // each class's own counts
        self.driver.absorb(&other.driver);
        self.scan.absorb(&other.scan);
        // the node's own figures only ever grow, so the later read is the larger
        self.budget_peak = self.budget_peak.max(other.budget_peak);
        self.budget_shed = self.budget_shed.max(other.budget_shed);
        self.idle_bytes = self.idle_bytes.max(other.idle_bytes);
        self.trickled_bytes = self.trickled_bytes.max(other.trickled_bytes);
        self.busy_us = self.busy_us.max(other.busy_us);
    }

    /// Whether the node walked nothing and its scans read nothing
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == ScanFigures::default()
    }
}

/// What a node's caches of read units did, summed over its object executors
/// ([F102](../../../../docs/src/features/read-unit-cache.md))
///
/// Kept by each executor's cache and read by `ObjectStore::cache_figures`; since F104 a node's
/// figures carry it, and a capture keeps each member's.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheReport {
    /// The reads answered from a cache
    pub hits: u64,
    /// The reads that looked and were not answered
    pub misses: u64,
    /// The units put in
    pub filled: u64,
    /// The units given up to make room
    pub evicted: u64,
    /// The units dropped because their chunk changed
    pub dropped: u64,
    /// The bytes of units held
    pub held: u64,
    /// The most bytes of units the caches may hold
    pub limit: u64,
}

impl CacheReport {
    /// Add another executor's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The other executor's figures
    pub fn add(&mut self, other: &CacheReport) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.filled += other.filled;
        self.evicted += other.evicted;
        self.dropped += other.dropped;
        self.held += other.held;
        self.limit += other.limit;
    }

    /// Whether the node keeps no cache and none did anything
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == CacheReport::default()
    }
}

/// The object work a node has in flight, and what it refused or gave up on the way
/// ([F108](../../../../docs/src/features/object-work-in-flight.md))
///
/// Read from counters the object executors, their slices and the shards keep in atomics and
/// cells, never through an executor's mailbox, so a node whose executor stopped answering still
/// says what it holds: item 305's put stalled for minutes and nothing said so. A figure "now" is
/// read when the node reports; every other counts from its start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectWork {
    /// Bytes drawn from the object executors' budgets now
    pub budget_used: u64,
    /// The bytes the budgets may hold together
    pub budget_limit: u64,
    /// Draws the budgets shed, each one that found no room within its wait
    pub budget_shed: u64,
    /// Object lanes this node dialled that are up now, one a remote slice an executor
    pub links: u64,
    /// Requests sent on those lanes and not answered yet, now
    pub waiting: u64,
    /// Requests sent on those lanes
    pub sent: u64,
    /// Requests a lane refused: its holder wrote nothing back for a while, the request was past
    /// its frame, or a reader's read had no answer within its bound (item 316)
    pub refused: u64,
    /// Lanes this node serves for other nodes' executors now
    pub served_lanes: u64,
    /// Answers the served lanes owe now: requests taken and not yet written back
    pub owed: u64,
    /// Requests the served lanes took
    pub served: u64,
    /// Foreground requests in flight on this node's slices now
    pub foreground: u64,
    /// Bytes staged on this node's slices and not yet applied, now
    pub staged_bytes: u64,
    /// Rebuild and move requests in flight on its slices now
    pub recovery: u64,
    /// Scrub requests in flight on its slices now
    pub scrubs: u64,
    /// Applies in flight on its slices now
    pub applying: u64,
    /// Object operations running on this node's shards now
    pub tasks: u64,
    /// Object operations a shard refused because it ran as many as it may
    pub tasks_refused: u64,
    /// Puts a shard gave up after their stripes failed, each abandoned and answered by name
    pub puts_given_up: u64,
}

impl ObjectWork {
    /// Whether the node has no object executors and its shards ran and refused nothing
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == ObjectWork::default()
    }
}

/// What a node's object executors did with the erasure code, since it started (F90)
///
/// Every encode, decode, fold and summary runs on an object executor, and each counts the bytes
/// it took in and the time its calls of the code took, without the yields between them, so a
/// rate read from two of these is a core's rate inside a node
/// ([F90](../../../../docs/src/features/erasure-coded-pools.md)).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CodecFigures {
    /// Bytes of data encoded into parity
    #[serde(default)]
    pub encoded_bytes: u64,
    /// Nanoseconds the encodes took
    #[serde(default)]
    pub encode_ns: u64,
    /// Bytes of surviving chunks a decode read to make a missing one
    #[serde(default)]
    pub decoded_bytes: u64,
    /// Nanoseconds the decodes took
    #[serde(default)]
    pub decode_ns: u64,
    /// Bytes of change a parity holder folded into its parity
    #[serde(default)]
    pub folded_bytes: u64,
    /// Nanoseconds the folds took
    #[serde(default)]
    pub fold_ns: u64,
    /// Bytes of verified units folded into a parity check's summaries
    #[serde(default)]
    pub summarised_bytes: u64,
    /// The kernel set the code runs with on this node's cpu
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kernel: String,
}

impl CodecFigures {
    /// Whether nothing was counted
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.encoded_bytes == 0
            && self.decoded_bytes == 0
            && self.folded_bytes == 0
            && self.summarised_bytes == 0
    }

    /// The rate encodes ran at, in bytes of data a second, or none before any
    #[must_use]
    pub fn encode_rate(&self) -> Option<f64> {
        (self.encode_ns > 0).then(|| self.encoded_bytes as f64 * 1e9 / self.encode_ns as f64)
    }
}

/// What one pool's recovery found and did on a node (F88)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RecoveryFigures {
    /// Stripe positions the missed records of the groups led here name stale
    #[serde(default)]
    pub stale: u64,
    /// Positions whose record dropped stripes past its bound, until a backfill makes it whole
    #[serde(default)]
    pub overflowed: u64,
    /// Positions made current by a rebuild
    #[serde(default)]
    pub rebuilt: u64,
    /// Overflowed positions a backfill made whole
    #[serde(default)]
    pub backfilled: u64,
    /// Placement groups switched to their pool's target
    #[serde(default)]
    pub moves_switched: u64,
    /// Placement group moves given up, for a later one to try again
    #[serde(default)]
    pub moves_aborted: u64,
    /// Placement groups the groups led here have still to move to their pool's target, as the
    /// move driver's last pass found them: zero on every member once a pool's moves are done
    #[serde(default)]
    pub behind: u64,
    /// What its scrubs found and did ([F89](../../../../docs/src/features/scrub-and-repair.md))
    #[serde(default, skip_serializing_if = "crate::shared::scrub::ScrubFigures::is_empty")]
    pub scrub: crate::shared::scrub::ScrubFigures,
    /// What its reclamation removed ([F92](../../../../docs/src/features/reclamation-and-device-lifecycle.md))
    #[serde(default, skip_serializing_if = "ReclaimFigures::is_empty")]
    pub reclaim: ReclaimFigures,
}

/// What one kind of a bucket's operation came to, counted where its coordinator ran it (F93)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketOpFigures {
    /// How many were answered
    pub count: u64,
    /// How many of them were refused, or failed, rather than done
    pub refused: u64,
    /// The bytes their clients sent
    pub bytes_in: u64,
    /// The bytes they were answered with
    pub bytes_out: u64,
    /// The microseconds they took, summed, which a mean is read from
    pub total_us: u64,
    /// The longest one took, in microseconds
    pub max_us: u64,
}

impl BucketOpFigures {
    /// Add another node's or shard's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The figures to add
    pub fn absorb(&mut self, other: &BucketOpFigures) {
        self.count += other.count;
        self.refused += other.refused;
        self.bytes_in += other.bytes_in;
        self.bytes_out += other.bytes_out;
        self.total_us += other.total_us;
        self.max_us = self.max_us.max(other.max_us);
    }

    /// The mean an operation took, in microseconds, or none before any was answered
    #[must_use]
    pub fn mean_us(&self) -> Option<u64> {
        (self.count > 0).then(|| self.total_us / self.count)
    }
}

/// What a bucket holds and what its clients were answered
/// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
///
/// Its objects and bytes are counted by each reclamation pass over the entries the node's shards
/// lead, so each object is counted on one member, as of that pass; its operations are counted at
/// the front door where their coordinators ran them, since the node started.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketFigures {
    /// The live objects it holds, as of the last reclamation pass
    pub objects: u64,
    /// The bytes they hold
    pub bytes: u64,
    /// Its operations by kind - `put`, `read`, `stat`, `delete`, `write_at`, `append`, `truncate`
    #[serde(default)]
    pub ops: std::collections::BTreeMap<String, BucketOpFigures>,
}

impl BucketFigures {
    /// Add another node's or shard's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The figures to add
    pub fn absorb(&mut self, other: &BucketFigures) {
        self.objects += other.objects;
        self.bytes += other.bytes;
        for (kind, figures) in &other.ops {
            self.ops.entry(kind.clone()).or_default().absorb(figures);
        }
    }
}

/// What a pool's reclamation removed, counted on the shards that drove it (M20)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimFigures {
    /// Retired objects whose every stripe was reclaimed and whose id left its entry
    #[serde(default)]
    pub objects: u64,
    /// Stripes reclaimed: a retired object's, or one a floor hid
    #[serde(default)]
    pub stripes: u64,
    /// Removals the holders made: one a holder of each placement group of a retired object, under
    /// its seal, and one a holder of each stripe a floor hid, through its sequence. A slice's own
    /// report counts the chunks and bytes it holds
    #[serde(default)]
    pub chunks: u64,
    /// Floors dropped once every stripe they hid was reclaimed
    #[serde(default)]
    pub floors: u64,
    /// Slices a placement group left, released of what they held of it
    #[serde(default)]
    pub released: u64,
    /// Chunks a light scrub found that nothing names, removed once their consumer said so
    #[serde(default)]
    pub unexplained: u64,
    /// Retired objects the entries name and the reclaimer has yet to finish, as its last pass found
    #[serde(default)]
    pub pending: u64,
    /// Reclamations put off: a holder that did not answer, or a seal that lapsed under it
    #[serde(default)]
    pub deferred: u64,
}

impl ReclaimFigures {
    /// Whether nothing was reclaimed and nothing waits, so a report written before M20 is the same
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Add another's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The figures added
    pub fn absorb(&mut self, other: &ReclaimFigures) {
        self.objects += other.objects;
        self.stripes += other.stripes;
        self.chunks += other.chunks;
        self.floors += other.floors;
        self.released += other.released;
        self.unexplained += other.unexplained;
        self.pending += other.pending;
        self.deferred += other.deferred;
    }
}

impl RecoveryFigures {
    /// Add another's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The figures added
    pub fn absorb(&mut self, other: &RecoveryFigures) {
        self.stale += other.stale;
        self.overflowed += other.overflowed;
        self.rebuilt += other.rebuilt;
        self.backfilled += other.backfilled;
        self.moves_switched += other.moves_switched;
        self.moves_aborted += other.moves_aborted;
        self.behind += other.behind;
        self.scrub.absorb(&other.scrub);
        self.reclaim.absorb(&other.reclaim);
    }
}

/// How busy one group a node leads was over the last interval
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupRate {
    /// The group
    pub group: u64,
    /// The table it serves, by the name the schema spells it
    pub table: String,
    /// Inserts, updates and deletes applied per second
    pub writes_per_sec: f64,
    /// Bytes of their intents per second
    pub bytes_per_sec: f64,
}

impl NodeStats {
    /// Empty figures for a node
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    #[must_use]
    pub fn empty(node: NodeId) -> Self {
        NodeStats {
            node,
            at_ms: 0,
            observed_ms: 0,
            shards: 0,
            hostname: String::new(),
            tables: Vec::new(),
            total: TableStats::default(),
            stream_sent: Rates::default(),
            stream_received: Rates::default(),
            stream_sent_total: 0,
            stream_received_total: 0,
            free_bytes: 0,
            volatile_bytes: 0,
            hot_groups: Vec::new(),
            memory_bytes: 0,
            memory_budget: 0,
            archive_map_bytes: 0,
            table_index_bytes: 0,
            wal_index_bytes: 0,
            lru_bytes: 0,
            resident_bytes: 0,
            wal_syncs_per_sec: 0.0,
            wal_bytes_per_sec: 0.0,
            wal_segments: 0,
            compacting_segments: 0,
            apply_lag: 0,
            pending_bytes: 0,
            shard_writes_per_sec: Vec::new(),
            shard_groups_led: Vec::new(),
            wal_sync_ms: 0.0,
            wal_appends_per_sync: 0.0,
            wal_sync_sizes: Vec::new(),
            queries: QueryStats::default(),
            hops: HopStats::default(),
            replication_lanes: ReplicationLanes::default(),
            cancels: CancelStats::default(),
            recovery: std::collections::BTreeMap::new(),
            codec: CodecFigures::default(),
            buckets: std::collections::BTreeMap::new(),
            evictions: EvictionFigures::default(),
            scans: ScanFigures::default(),
            object_cache: CacheReport::default(),
            objects: ObjectWork::default(),
        }
    }

    /// These figures narrowed to one table, whose figures become the total
    ///
    /// # Arguments
    ///
    /// * `table` - The table, by the name the schema spells it
    #[must_use]
    pub fn narrowed(&self, table: &str) -> NodeStats {
        // the table's own row, or an empty one if the node has nothing of it
        let row = self
            .tables
            .iter()
            .find(|row| row.table == table)
            .cloned()
            .unwrap_or_else(|| TableStats {
                table: table.to_string(),
                ..TableStats::default()
            });
        NodeStats {
            tables: vec![row.clone()],
            total: TableStats {
                table: String::new(),
                ..row
            },
            ..self.clone()
        }
    }
}

/// One member of the cluster, with its standing and its figures
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberStats {
    /// The member
    pub node: NodeId,
    /// Where clients reach it
    #[serde(default)]
    pub client: String,
    /// Voter or learner
    #[serde(default)]
    pub role: String,
    /// Joining, up or down
    #[serde(default)]
    pub health: String,
    /// Member, leaving, removing or removed
    #[serde(default)]
    pub phase: String,
    /// The one word state a person reads first
    #[serde(default)]
    pub state: String,
    /// Whether an operator suspended the grace a down member is removed after
    #[serde(default)]
    pub maintenance: bool,
    /// How much of a down member's grace is left, if it is in one
    #[serde(default)]
    pub grace_remaining_ms: Option<u64>,
    /// The shards that have failed on it, by index
    #[serde(default)]
    pub shards_failed: Vec<u16>,
    /// How long ago the answering node heard its figures, if it has
    #[serde(default)]
    pub report_age_ms: Option<u64>,
    /// Whether its figures are too old for their rates to be read as current
    #[serde(default)]
    pub stale: bool,
    /// Its figures, if the answering node holds them
    #[serde(default)]
    pub stats: Option<NodeStats>,
}

/// How far a plan has got, and how fast
///
/// Folded from the plan's committed record and the committed records of the moves it issued,
/// whose timings the group leaders that drove them wrote, plus the stream rates of the members
/// the steps move from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanProgress {
    /// The operation
    pub op: Uuid,
    /// Rebalance, decommission, remove or expiry
    #[serde(default)]
    pub kind: String,
    /// Where it stands
    #[serde(default)]
    pub phase: String,
    /// Why it cannot go on, if it cannot
    #[serde(default)]
    pub blocked: Option<String>,
    /// What it came to, once done
    #[serde(default)]
    pub outcome: Option<String>,
    /// How many steps it holds
    #[serde(default)]
    pub steps_total: u64,
    /// Steps not issued yet
    #[serde(default)]
    pub pending: u64,
    /// Steps whose move is running
    #[serde(default)]
    pub moving: u64,
    /// Steps whose move is done
    #[serde(default)]
    pub moved: u64,
    /// Steps whose move failed
    #[serde(default)]
    pub failed: u64,
    /// The bytes every live and moved step held when planned
    #[serde(default)]
    pub bytes_planned: u64,
    /// The planned bytes of the steps that moved
    #[serde(default)]
    pub bytes_moved: u64,
    /// Snapshot bytes the plan's moves streamed, as their records last committed it
    #[serde(default)]
    pub bytes_streamed: u64,
    /// When its first move started, in milliseconds since the epoch
    #[serde(default)]
    pub started_ms: Option<u64>,
    /// How long it has run, or ran
    #[serde(default)]
    pub elapsed_ms: Option<u64>,
    /// How long a finished step took on average
    #[serde(default)]
    pub mean_step_ms: Option<u64>,
    /// The planned bytes moved per second of the plan's elapsed time
    #[serde(default)]
    pub throughput_avg_bps: Option<f64>,
    /// The bytes per second its source members are streaming right now, over a minute
    #[serde(default)]
    pub throughput_now_bps: Option<f64>,
    /// How long it is estimated to have left
    #[serde(default)]
    pub eta_ms: Option<u64>,
}

/// The whole of a `Stats` read's answer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatsView {
    /// `leader` when the answering node leads and holds every member's figures, `local` when it
    /// holds only its own
    pub source: String,
    /// The node that answered
    pub answered_by: NodeId,
    /// The control leader, if one is known
    #[serde(default)]
    pub leader: Option<NodeId>,
    /// Where clients reach the leader, if it is known
    #[serde(default)]
    pub leader_client: Option<String>,
    /// The topology version the answer was built at
    #[serde(default)]
    pub version: u64,
    /// The table the figures were narrowed to, if one was asked for
    #[serde(default)]
    pub table: Option<String>,
    /// When the answer was built, in milliseconds since the epoch by the answering node's clock
    #[serde(default)]
    pub at_ms: u64,
    /// Every member not removed
    #[serde(default)]
    pub members: Vec<MemberStats>,
    /// Every plan the control state holds, open ones first
    #[serde(default)]
    pub plans: Vec<PlanProgress>,
}

impl ClusterStatsView {
    /// Whether the answer holds every member's figures
    #[must_use]
    pub fn is_leader_view(&self) -> bool {
        self.source == "leader"
    }

    /// Every member's figures summed, over the copies each leads, so a row is counted once
    #[must_use]
    pub fn cluster_total(&self) -> TableStats {
        // every member's total is summed as reported
        let mut total = TableStats::default();
        for member in &self.members {
            if let Some(stats) = &member.stats {
                total.absorb(&stats.total);
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A counter below its earlier reading is a reset, and the rest subtract
    #[test]
    fn write_counters_delta_and_absorb() {
        // two readings a few writes apart
        let prev = WriteCounters {
            inserts: 3,
            insert_bytes: 300,
            ..WriteCounters::default()
        };
        let now = WriteCounters {
            inserts: 5,
            insert_bytes: 520,
            deletes: 1,
            delete_bytes: 8,
            ..WriteCounters::default()
        };
        let delta = now.delta(&prev).expect("nothing went back");
        assert_eq!(delta.inserts, 2);
        assert_eq!(delta.insert_bytes, 220);
        assert_eq!(delta.deletes, 1);
        // a reading below the earlier one is a restart
        assert_eq!(prev.delta(&now), None);
        // and absorbing sums every field
        let mut sum = prev;
        sum.absorb(&delta);
        assert_eq!(sum.inserts, 5);
        assert_eq!(sum.delete_bytes, 8);
        assert!(!sum.is_zero());
        assert!(WriteCounters::default().is_zero());
    }

    /// Every stats frame decodes from one that leaves its optional fields out
    #[test]
    fn stats_frames_decode_from_older_shapes() {
        // a view with nothing but its identity
        let node = NodeId(Uuid::new_v4());
        let view: ClusterStatsView = serde_json::from_value(serde_json::json!({
            "source": "local", "answered_by": node
        }))
        .expect("a bare view decodes");
        assert!(view.members.is_empty());
        assert!(!view.is_leader_view());
        // a node's figures with nothing but the node
        let stats: NodeStats =
            serde_json::from_value(serde_json::json!({ "node": node })).expect("decodes");
        assert_eq!(stats, NodeStats::empty(node));
        // and a full one round trips
        let mut full = NodeStats::empty(node);
        full.tables.push(TableStats {
            table: "notes".to_string(),
            partitions: 7,
            ..TableStats::default()
        });
        full.total.partitions = 7;
        full.stream_sent.r1m = 12.5;
        full.hostname = "hyperion".to_string();
        full.queries = QueryStats {
            ops: vec![OpStats {
                op: "get".to_string(),
                rate: Rates {
                    r10s: 40.0,
                    ..Rates::default()
                },
                p50_ms: Some(0.2),
                p99_ms: Some(1.5),
                answers_total: 400,
                bytes_out_total: 40_000,
                ..OpStats::default()
            }],
            bytes_in_total: 9_000,
            p99_ms: Some(1.5),
            sampled_every: 1,
            ..QueryStats::default()
        };
        let json = serde_json::to_value(&full).expect("encodes");
        let back: NodeStats = serde_json::from_value(json).expect("decodes");
        assert_eq!(back, full);
        // a frame without a hostname, as a build from before F64 sends it, names none
        assert!(stats.hostname.is_empty());
        // and one without query figures, as a build from before F65 sends it, has none, where
        // a node from F65 on that has answered nothing still names its sampling
        assert!(stats.queries.is_empty());
        let idle = QueryStats {
            sampled_every: 1,
            ..QueryStats::default()
        };
        assert!(!idle.is_empty());
        assert!(serde_json::to_value(&idle).expect("encodes").get("sampled_every").is_some());
        // and an empty one is left out of the frame rather than sent as an empty string
        let bare = serde_json::to_value(NodeStats::empty(node)).expect("encodes");
        assert!(bare.get("hostname").is_none(), "{bare}");
        // nor are empty query figures, or a percentile nothing was timed for
        assert!(bare.get("queries").is_none(), "{bare}");
        let json = serde_json::to_value(&full.queries).expect("encodes");
        assert!(json.get("p50_ms").is_none(), "{json}");
    }

    /// A node's figures from before F75 carry no cancels, a node nothing was cancelled on leaves
    /// them out, and the counters read a restart whole
    /// ([F75](../../../../docs/src/features/client-cancel.md))
    #[test]
    fn node_stats_from_before_f75_decode() {
        // a frame with no cancels decodes as none
        let node = NodeId(Uuid::new_v4());
        let stats: NodeStats =
            serde_json::from_value(serde_json::json!({ "node": node })).expect("decodes");
        assert!(stats.cancels.is_empty());
        // and none are written back out
        let bare = serde_json::to_value(NodeStats::empty(node)).expect("encodes");
        assert!(bare.get("cancels").is_none(), "{bare}");
        // a node that counted some round trips them
        let mut full = NodeStats::empty(node);
        full.cancels = CancelStats {
            refused: Rates {
                r10s: 3.0,
                ..Rates::default()
            },
            totals: CancelCounters {
                received: 2,
                refused: 30,
                dropped: 4,
                dropped_bytes: 4096,
                cut: 1,
                ..CancelCounters::default()
            },
            ..CancelStats::default()
        };
        let json = serde_json::to_value(&full).expect("encodes");
        assert!(json.get("cancels").is_some(), "{json}");
        let back: NodeStats = serde_json::from_value(json).expect("decodes");
        assert_eq!(back, full);
        // a counter below its earlier reading is a shard that started again
        let prev = full.cancels.totals;
        let now = CancelCounters {
            received: 5,
            refused: 7,
            ..prev
        };
        let gained = now.since(&prev);
        assert_eq!(gained.received, 3);
        assert_eq!(gained.refused, 7);
        assert_eq!(gained.dropped, 0);
        let mut sum = prev;
        sum.absorb(&gained);
        assert_eq!(sum.received, 5);
        assert!(CancelCounters::default().is_zero());
    }

    /// Every answer kind is counted in its own place, and a node's query figures stay small on
    /// the wire
    #[test]
    fn query_figures_name_every_kind_and_stay_small() {
        // every kind has a place of its own, and the reads and writes are among them
        let kinds = [
            ResponseActionNames::Get,
            ResponseActionNames::Exists,
            ResponseActionNames::Insert,
            ResponseActionNames::Update,
            ResponseActionNames::Delete,
            ResponseActionNames::Error,
        ];
        for kind in &kinds {
            let index = query_op_index(kind);
            assert_eq!(QUERY_OPS[index], format!("{kind:?}").to_lowercase());
        }
        assert!(READ_OPS.iter().chain(WRITE_OPS.iter()).all(|op| QUERY_OPS.contains(op)));
        // a node that answered every kind at full precision
        let busy = |op: &str| OpStats {
            op: op.to_string(),
            rate: Rates {
                r10s: 123_456.789_012,
                r1m: 123_456.789_012,
                r5m: 123_456.789_012,
            },
            bytes_out: Rates {
                r10s: 98_765_432.109_876,
                r1m: 98_765_432.109_876,
                r5m: 98_765_432.109_876,
            },
            p50_ms: Some(0.123_456_789),
            p99_ms: Some(12.345_678_9),
            answers_total: u64::MAX / 3,
            bytes_out_total: u64::MAX / 3,
        };
        let queries = QueryStats {
            ops: QUERY_OPS.iter().map(|op| busy(op)).collect(),
            bytes_in: busy("get").rate,
            bytes_in_total: u64::MAX / 3,
            p50_ms: Some(0.123_456_789),
            p99_ms: Some(12.345_678_9),
            sampled_every: 16,
        };
        // rides one status report in four, so it is held to under two and a quarter kilobytes:
        // two until F68's seventh kind, `refused`, which a node that answered every kind at the
        // largest figures takes past two by about a hundred bytes
        let bytes = serde_json::to_vec(&queries).expect("encodes").len();
        assert!(bytes < 2304, "{bytes} bytes");
        // and the rates of several kinds add up
        assert!((queries.rate_of(&READ_OPS) - 2.0 * 123_456.789_012).abs() < 1e-6);
        // and nothing adds up to a zero that is written as one
        assert!(queries.bytes_out_of(&["nothing"]).is_sign_positive());
        assert_eq!(QueryStats::default().rate_of(&READ_OPS).to_string(), "0");
    }

    /// Narrowing keeps one table and makes it the total, and a view sums what members lead
    #[test]
    fn narrowing_and_cluster_totals() {
        // a node with two tables
        let node = NodeId(Uuid::new_v4());
        let mut stats = NodeStats::empty(node);
        stats.tables = vec![
            TableStats {
                table: "a".to_string(),
                partitions: 2,
                ..TableStats::default()
            },
            TableStats {
                table: "b".to_string(),
                partitions: 5,
                ..TableStats::default()
            },
        ];
        stats.total.partitions = 7;
        let narrowed = stats.narrowed("b");
        assert_eq!(narrowed.tables.len(), 1);
        assert_eq!(narrowed.total.partitions, 5);
        assert!(narrowed.total.table.is_empty());
        // a table the node has nothing of narrows to an empty row
        assert_eq!(stats.narrowed("c").total.partitions, 0);
        // a view of two members sums their totals
        let member = |stats: Option<NodeStats>| MemberStats {
            node,
            client: String::new(),
            role: String::new(),
            health: String::new(),
            phase: String::new(),
            state: String::new(),
            maintenance: false,
            grace_remaining_ms: None,
            shards_failed: Vec::new(),
            report_age_ms: None,
            stale: false,
            stats,
        };
        let view = ClusterStatsView {
            source: "leader".to_string(),
            answered_by: node,
            leader: Some(node),
            leader_client: None,
            version: 1,
            table: None,
            at_ms: 0,
            members: vec![
                member(Some(stats.clone())),
                member(Some(stats)),
                member(None),
            ],
            plans: Vec::new(),
        };
        assert_eq!(view.cluster_total().partitions, 14);
    }

    /// A pass of evictions is counted under what began it and shards' figures add up; a node's
    /// figures from a build before F104 read as nothing evicted and no cache (F104)
    #[test]
    fn evictions_are_counted_by_what_began_them() {
        // two passes of one shard, one of each
        let mut shard = EvictionFigures::default();
        assert!(shard.is_zero());
        shard.count(MemoryPressure::Budget, 3, 300);
        shard.count(MemoryPressure::Resident, 5, 500);
        assert_eq!((shard.budget_passes, shard.budget_partitions, shard.budget_bytes), (1, 3, 300));
        assert_eq!(
            (shard.resident_passes, shard.resident_partitions, shard.resident_bytes),
            (1, 5, 500)
        );
        // and a second shard's added to them
        let mut node = shard;
        node.absorb(&shard);
        assert_eq!((node.budget_passes, node.resident_partitions), (2, 10));
        // a node's figures without either field, as a build before F104 sends them
        let mut old = serde_json::to_value(NodeStats::empty(crate::shared::identity::NodeId(uuid::Uuid::nil())))
            .expect("figures as json");
        let fields = old.as_object_mut().expect("an object");
        fields.remove("evictions");
        fields.remove("object_cache");
        let read: NodeStats = serde_json::from_value(old).expect("read without them");
        assert!(read.evictions.is_zero() && read.object_cache.is_zero());
        // and nothing evicted or cached is left out of the frame
        let written = serde_json::to_string(&read).expect("written");
        assert!(!written.contains("evictions") && !written.contains("object_cache"), "{written}");
    }

    /// A step that held its loop so many microseconds, all of them on its own cpu
    ///
    /// # Arguments
    ///
    /// * `us` - How long it held the loop
    fn held(us: u64) -> StepSample {
        StepSample {
            wall_ns: us * 1_000,
            cpu_ns: us * 1_000,
            ..StepSample::default()
        }
    }

    /// A step is counted among its kind's past each bound by the wall and by its own cpu, the long
    /// ones summed apart with what they held, the longest kept whole; every step still counts among
    /// the figures before F130 by the wall as it did, shards' kinds add up with the longer longest
    /// kept, and a node's figures from a build before F130 read as no step of either kind
    /// ([F130](../../../../docs/src/features/loop-steps-by-kind.md))
    #[test]
    fn a_step_is_counted_by_its_kind_wall_and_own_cpu() {
        let mut figures = WalkClassFigures::default();
        // a short capture, and an encode step held 300 µs by its own work
        figures.step(StepKind::Capture, &StepSample { wall_ns: 40_000, cpu_ns: 38_000, keys: 900, rows: 0, bytes: 0 });
        figures.step(StepKind::Encode, &StepSample { wall_ns: 300_000, cpu_ns: 290_000, keys: 1, rows: 40, bytes: 60_000 });
        // an encode step held 250 µs by the wall, its thread off its cpu for most of it
        figures.step(StepKind::Encode, &StepSample { wall_ns: 250_000, cpu_ns: 30_000, keys: 1, rows: 10, bytes: 9_000 });
        // and one of 2 ms on its own cpu, the longest
        figures.step(StepKind::Encode, &StepSample { wall_ns: 2_000_000, cpu_ns: 1_990_000, keys: 1, rows: 64, bytes: 65_000 });
        // the figures before F130 count every step by the wall alone, as they did
        assert_eq!((figures.steps, figures.steps_past, figures.longest_step_us), (4, [3, 1, 0], 2_000));
        // a capture under every bound
        assert_eq!(figures.captures.all.steps, 1);
        assert_eq!((figures.captures.all.keys, figures.captures.long.steps), (900, 0));
        assert_eq!((figures.captures.past, figures.captures.past_cpu), ([0, 0, 0], [0, 0, 0]));
        // three encode steps past 100 µs by the wall, two by their own cpu, one by the wall alone
        let encodes = figures.encodes;
        assert_eq!((encodes.all.steps, encodes.long.steps), (3, 3));
        assert_eq!((encodes.past, encodes.past_cpu), ([3, 1, 0], [2, 1, 0]));
        assert_eq!(encodes.past_by_wall_alone(), 1);
        assert_eq!((encodes.long.rows, encodes.long.bytes, encodes.long.cpu_ns), (114, 134_000, 2_310_000));
        assert_eq!(encodes.all.wall_ns, 2_550_000);
        assert_eq!(encodes.longest, StepSample { wall_ns: 2_000_000, cpu_ns: 1_990_000, keys: 1, rows: 64, bytes: 65_000 });
        // a bound is judged in whole microseconds, as the figures before F130 judge it
        let mut edge = StepFigures::default();
        edge.count(&StepSample { wall_ns: 100_900, cpu_ns: 100_900, ..StepSample::default() });
        assert_eq!((edge.past, edge.long.steps), ([0, 0, 0], 0));
        // a second shard's: a shorter longest leaves the first's, a longer one replaces it whole
        let mut other = WalkClassFigures::default();
        other.step(StepKind::Encode, &held(150));
        let mut node = figures;
        node.absorb(&other);
        assert_eq!((node.encodes.all.steps, node.encodes.past[0], node.encodes.past_cpu[0]), (4, 4, 3));
        assert_eq!(node.encodes.longest.wall_ns, 2_000_000);
        other.step(StepKind::Encode, &StepSample { wall_ns: 9_000_000, cpu_ns: 100_000, keys: 1, rows: 2, bytes: 3 });
        let mut node = figures;
        node.absorb(&other);
        assert_eq!(node.encodes.longest, StepSample { wall_ns: 9_000_000, cpu_ns: 100_000, keys: 1, rows: 2, bytes: 3 });
        assert_eq!(node.longest_step_us, 9_000);
        // a class's figures from a build before F130 read as no step of either kind
        let mut old = serde_json::to_value(figures).expect("figures as json");
        let fields = old.as_object_mut().expect("an object");
        fields.remove("captures");
        fields.remove("encodes");
        let read: WalkClassFigures = serde_json::from_value(old).expect("read without them");
        assert_eq!((read.steps, read.captures, read.encodes), (4, StepFigures::default(), StepFigures::default()));
    }

    /// A step is counted past every bound it held its loop beyond, shards' classes add up with the
    /// longest step kept, the node's budget and pace are the later read, and a node's figures from a
    /// build before M23 read as no walk at all, a node that walked nothing leaving them out (M23)
    #[test]
    fn a_nodes_scan_figures_are_its_shards_summed() {
        // one shard's steps: under every bound, past the first, past all three
        let mut one = ScanFigures::default();
        assert!(one.is_zero());
        one.scan.step(StepKind::Capture, &held(40));
        one.scan.step(StepKind::Encode, &held(250));
        one.scan.step(StepKind::Encode, &held(12_000));
        one.scan.pages = 3;
        one.driver.pages = 1;
        one.budget_peak = 4 << 20;
        one.idle_bytes = 100;
        assert_eq!((one.scan.steps, one.scan.steps_past, one.scan.longest_step_us), (3, [2, 1, 1], 12_000));
        // a second shard's, read later: more of the budget and the pace seen
        let mut two = ScanFigures::default();
        two.scan.step(StepKind::Capture, &held(500));
        two.scan.pages = 2;
        two.budget_peak = 5 << 20;
        two.idle_bytes = 150;
        let mut node = one;
        node.absorb(&two);
        assert_eq!((node.scan.pages, node.driver.pages, node.scan.steps), (5, 1, 4));
        assert_eq!((node.scan.steps_past, node.scan.longest_step_us), ([3, 1, 1], 12_000));
        assert_eq!((node.budget_peak, node.idle_bytes), (5 << 20, 150));
        // a node's figures without them, as a build before M23 sends them, read as none
        let mut old = serde_json::to_value(NodeStats::empty(crate::shared::identity::NodeId(uuid::Uuid::nil())))
            .expect("figures as json");
        old.as_object_mut().expect("an object").remove("scans");
        let read: NodeStats = serde_json::from_value(old).expect("read without them");
        assert!(read.scans.is_zero());
        // and a node that walked nothing leaves them out of its frame
        let written = serde_json::to_string(&read).expect("written");
        assert!(!written.contains("\"scans\""), "{written}");
    }

    /// A node's figures from a build before F108 read as no object work at all, and a node with
    /// none leaves the field out of its frame (F108)
    #[test]
    fn object_work_from_a_build_before_it_reads_as_none() {
        // a node's figures without the field, as a build before F108 sends them
        let mut old = serde_json::to_value(NodeStats::empty(crate::shared::identity::NodeId(
            uuid::Uuid::nil(),
        )))
        .expect("figures as json");
        old.as_object_mut().expect("an object").remove("objects");
        let read: NodeStats = serde_json::from_value(old).expect("read without it");
        assert!(read.objects.is_zero());
        // and a node with no object work writes none
        let written = serde_json::to_string(&read).expect("written");
        assert!(!written.contains("\"objects\""), "{written}");
        // a field this build does not know of yet is read as nothing, the rest as written
        let partial: ObjectWork =
            serde_json::from_str(r#"{"waiting":3,"owed":2}"#).expect("read a part");
        assert_eq!((partial.waiting, partial.owed, partial.tasks), (3, 2, 0));
        assert!(!partial.is_zero());
    }

    /// A check a node's caches answered by giving units back is counted apart from the passes that
    /// evicted rows, shards' counts add up, and figures from a build before F107 read as none
    /// (F107)
    #[test]
    fn a_cache_yield_is_counted_apart_from_evictions() {
        let mut shard = EvictionFigures::default();
        shard.cache_yields += 1;
        assert!(!shard.is_zero(), "a yield alone reads as nothing done");
        let mut node = EvictionFigures::default();
        node.absorb(&shard);
        node.absorb(&shard);
        assert_eq!(node.cache_yields, 2);
        assert_eq!((node.resident_passes, node.resident_partitions), (0, 0));
        let before: EvictionFigures = serde_json::from_str(r#"{"resident_passes":1}"#).expect("read without it");
        assert_eq!((before.resident_passes, before.cache_yields), (1, 0));
    }
}
