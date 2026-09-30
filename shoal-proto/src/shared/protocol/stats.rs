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
/// The five query kinds a client can send, and `error` for an answer that failed, whatever the
/// query was: a failure is read out of the answer, which no longer says what was asked.
pub const QUERY_OPS: [&str; 6] = ["get", "exists", "insert", "update", "delete", "error"];

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
    /// The free bytes on the node's storage, zero when it could not read them
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
    /// Bytes the shards' archive maps' indexes hold, which no budget counts
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
        // rides one status report in four, so it is held to under two kilobytes
        let bytes = serde_json::to_vec(&queries).expect("encodes").len();
        assert!(bytes < 2048, "{bytes} bytes");
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
}
