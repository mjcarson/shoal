//! Every figure the stats view can chart, and what each one means
//!
//! One catalog is read by the metric list, the chart, the table under it and the help page, so
//! none of them can name a figure the others do not explain
//! ([F64](../../../../docs/src/features/stats-tui.md)). Every column `--basic` prints is named
//! either by a metric here or by a [`Term`], and a test holds the two to that.

use shoal::shared::protocol::stats::{ClusterStatsView, NodeStats, Rates, WriteRates};

use super::{byte_rate, rate};
use crate::cluster::model::bytes;

/// How a metric's values are written
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// Events per second
    PerSec,
    /// Bytes per second
    BytesPerSec,
    /// Bytes
    Bytes,
    /// Milliseconds
    Millis,
    /// A count of things
    Count,
    /// One figure over another
    Ratio,
}

impl Unit {
    /// A value as the tables write it
    ///
    /// # Arguments
    ///
    /// * `value` - The value
    #[must_use]
    pub fn format(self, value: f64) -> String {
        match self {
            // a count under a thousand is written whole, and scaled like a rate past it
            Unit::Count if value < 1000.0 => format!("{:.0}", value.max(0.0)),
            Unit::PerSec | Unit::Count => rate(value.max(0.0)),
            Unit::BytesPerSec => byte_rate(value),
            Unit::Bytes => bytes(value.max(0.0) as u64),
            Unit::Millis => format!("{value:.2}ms"),
            Unit::Ratio => format!("{value:.1}"),
        }
    }

    /// The unit in words, as the help page names it
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            Unit::PerSec => "per second",
            Unit::BytesPerSec => "bytes per second",
            Unit::Bytes => "bytes",
            Unit::Millis => "milliseconds",
            Unit::Count => "count",
            Unit::Ratio => "ratio",
        }
    }
}

/// Where a metric's value is read from
#[derive(Debug, Clone, Copy)]
pub enum Reader {
    /// One value per member, from its figures
    Member(fn(&NodeStats) -> f64),
    /// One value for the whole cluster, from the answer
    Cluster(fn(&ClusterStatsView) -> f64),
}

/// One figure the view can chart
#[derive(Debug, Clone, Copy)]
pub struct Metric {
    /// A name that never changes, for tests and for keeping a selection
    pub key: &'static str,
    /// What the list and the chart call it
    pub name: &'static str,
    /// The group it is listed under, one of [`GROUPS`]
    pub group: &'static str,
    /// How its values are written
    pub unit: Unit,
    /// The `--basic` columns that print it, if any
    pub columns: &'static [&'static str],
    /// What it measures and how to read it
    pub help: &'static str,
    /// Where its value comes from
    pub read: Reader,
}

/// A word the figures use that is not charted, and what it means
#[derive(Debug, Clone, Copy)]
pub struct Term {
    /// The word
    pub name: &'static str,
    /// The section of the help page it is listed in, one of [`TERM_SECTIONS`]
    pub section: &'static str,
    /// The `--basic` columns and titles it explains, if any
    pub columns: &'static [&'static str],
    /// What it means
    pub help: &'static str,
}

/// The groups metrics are listed under, in order, with what each one is about
pub const GROUPS: [(&str, &str); 6] = [
    (
        "cluster",
        "The whole cluster, counted once per row through each group's leader.",
    ),
    (
        "writes",
        "What each member applies, over every copy it hosts unless the name says led.",
    ),
    (
        "streams",
        "Snapshot bytes each member sends and receives while sets move.",
    ),
    ("placement", "What each member holds."),
    (
        "memory",
        "What each member holds in memory, and what its eviction budget does and does not count.",
    ),
    (
        "storage",
        "Each member's write-ahead log, compaction and apply pipeline.",
    ),
];

/// The sections terms are listed in, in order, with their headings
pub const TERM_SECTIONS: [(&str, &str); 3] = [
    ("figures", "Other words in the figures"),
    ("tables", "The --basic tables"),
    ("plans", "Plans"),
];

/// The three write kinds summed, in rows
///
/// # Arguments
///
/// * `rates` - The write rates
fn rows(rates: &WriteRates) -> f64 {
    rates.inserts.r10s + rates.updates.r10s + rates.deletes.r10s
}

/// The three write kinds' intent bytes summed
///
/// # Arguments
///
/// * `rates` - The write rates
fn row_bytes(rates: &WriteRates) -> f64 {
    rates.insert_bytes.r10s + rates.update_bytes.r10s + rates.delete_bytes.r10s
}

/// A rate's ten second window, which is what a chart samples
///
/// # Arguments
///
/// * `rates` - The rate's windows
fn now(rates: &Rates) -> f64 {
    rates.r10s
}

/// A count as a value to chart
///
/// # Arguments
///
/// * `count` - The count
fn count(count: u64) -> f64 {
    count as f64
}

/// Every metric the view can chart, in the order it lists them
pub const METRICS: &[Metric] = &[
    // the cluster, once per row
    Metric {
        key: "cluster_writes",
        name: "cluster writes/s",
        group: "cluster",
        unit: Unit::PerSec,
        columns: &[],
        help: "Rows inserted, updated and deleted per second across the cluster. It sums \
               the members' led rates, so a row counts once however many copies it has. This \
               is the cluster's real write rate, and it does not spike while a node catches up.",
        read: Reader::Cluster(|view| rows(&view.cluster_total().led)),
    },
    Metric {
        key: "cluster_bytes",
        name: "cluster bytes/s",
        group: "cluster",
        unit: Unit::BytesPerSec,
        columns: &[],
        help: "Bytes of write intents per second across the cluster, once per row. An intent \
               is the row for an insert or an update and the key for a delete. It is what the \
               logs carry, not what the archives end up holding.",
        read: Reader::Cluster(|view| row_bytes(&view.cluster_total().led)),
    },
    Metric {
        key: "cluster_partitions",
        name: "cluster partitions",
        group: "cluster",
        unit: Unit::Count,
        columns: &[],
        help: "Partitions the cluster's archives hold, once per row through each group's \
               leader. A write is only counted once the compactor has merged it into an \
               archive, so a busy cluster reads low until it compacts.",
        read: Reader::Cluster(|view| count(view.cluster_total().partitions_led)),
    },
    Metric {
        key: "cluster_archived",
        name: "cluster archived",
        group: "cluster",
        unit: Unit::Bytes,
        columns: &[],
        help: "Bytes the cluster's archives hold, once per row through each group's leader. \
               Like the partitions, it lags a write until the write is compacted.",
        read: Reader::Cluster(|view| count(view.cluster_total().bytes_led)),
    },
    // what each member applies
    Metric {
        key: "applied",
        name: "applied writes/s",
        group: "writes",
        unit: Unit::PerSec,
        columns: &[],
        help: "Rows the member applied per second over every copy it hosts, leader or not. \
               Every replica applies every write, so the members' applied rates add up to the \
               cluster's rate times the replication factor. A node catching up (a learner \
               being fed, or one back from an outage) applies a backlog in a burst, so a \
               spike here on one member is not new load.",
        read: Reader::Member(|stats| rows(&stats.total.applied)),
    },
    Metric {
        key: "inserts",
        name: "inserts/s",
        group: "writes",
        unit: Unit::PerSec,
        columns: &["insert", "ins/s"],
        help: "Rows inserted per second over every copy the member hosts.",
        read: Reader::Member(|stats| now(&stats.total.applied.inserts)),
    },
    Metric {
        key: "updates",
        name: "updates/s",
        group: "writes",
        unit: Unit::PerSec,
        columns: &["update", "upd/s"],
        help: "Rows updated per second over every copy the member hosts.",
        read: Reader::Member(|stats| now(&stats.total.applied.updates)),
    },
    Metric {
        key: "deletes",
        name: "deletes/s",
        group: "writes",
        unit: Unit::PerSec,
        columns: &["delete", "del/s"],
        help: "Rows deleted per second over every copy the member hosts.",
        read: Reader::Member(|stats| now(&stats.total.applied.deletes)),
    },
    Metric {
        key: "led_writes",
        name: "led writes/s",
        group: "writes",
        unit: Unit::PerSec,
        columns: &[],
        help: "Rows written per second through the groups this member leads. A leader \
               proposes, replicates and answers for its groups, so this is the write work the \
               member does as a leader. The members' led rates sum to the cluster's rate. One \
               member far above the others leads more than its share of the busy groups.",
        read: Reader::Member(|stats| rows(&stats.total.led)),
    },
    Metric {
        key: "bytes_in",
        name: "bytes in/s",
        group: "writes",
        unit: Unit::BytesPerSec,
        columns: &["bytes in", "in B/s"],
        help: "Bytes of write intents the member applied per second over every copy it \
               hosts: rows for inserts and updates, keys for deletes.",
        read: Reader::Member(|stats| row_bytes(&stats.total.applied)),
    },
    Metric {
        key: "misses",
        name: "misses/s",
        group: "writes",
        unit: Unit::PerSec,
        columns: &[],
        help: "Updates and deletes per second that found no row to change. A steady rate \
               under an update load means clients are updating keys that do not exist.",
        read: Reader::Member(|stats| now(&stats.total.applied.misses)),
    },
    // snapshot streams
    Metric {
        key: "stream_out",
        name: "stream out/s",
        group: "streams",
        unit: Unit::BytesPerSec,
        columns: &["stream out", "stream/s"],
        help: "Snapshot bytes per second the member sends to other members: moving a set in \
               a rebalance or a rebuild, or feeding a copy that fell behind the log. It is \
               idle otherwise. A copy fed from the log shows little here, so a plan's steps \
               are then the better measure of its progress.",
        read: Reader::Member(|stats| now(&stats.stream_sent)),
    },
    Metric {
        key: "stream_in",
        name: "stream in/s",
        group: "streams",
        unit: Unit::BytesPerSec,
        columns: &[],
        help: "Snapshot bytes per second the member receives: the other end of stream out, \
               on a member being rebalanced onto or rebuilt.",
        read: Reader::Member(|stats| now(&stats.stream_received)),
    },
    // what each member holds
    Metric {
        key: "groups",
        name: "groups",
        group: "placement",
        unit: Unit::Count,
        columns: &["groups/led"],
        help: "Tablet groups the member hosts a copy of. A group is one Raft group \
               replicating a set of one table's tablets.",
        read: Reader::Member(|stats| count(stats.total.groups)),
    },
    Metric {
        key: "groups_led",
        name: "groups led",
        group: "placement",
        unit: Unit::Count,
        columns: &["groups/led"],
        help: "Tablet groups the member leads. A lead costs a core its group's proposals and \
               replication, so leads should spread across the members in proportion to their \
               lead weights.",
        read: Reader::Member(|stats| count(stats.total.groups_led)),
    },
    Metric {
        key: "tablets",
        name: "tablets",
        group: "placement",
        unit: Unit::Count,
        columns: &["tablets/led"],
        help: "Tablets in the groups the member hosts a copy of.",
        read: Reader::Member(|stats| count(stats.total.tablets)),
    },
    Metric {
        key: "tablets_led",
        name: "tablets led",
        group: "placement",
        unit: Unit::Count,
        columns: &["tablets/led"],
        help: "Tablets in the groups the member leads.",
        read: Reader::Member(|stats| count(stats.total.tablets_led)),
    },
    Metric {
        key: "partitions",
        name: "partitions",
        group: "placement",
        unit: Unit::Count,
        columns: &["partitions/led", "partitions"],
        help: "Partitions the member's archives hold over every copy it hosts. Archived \
               figures lag a write until the compactor merges it, and ephemeral tables have \
               no archive, so they report none.",
        read: Reader::Member(|stats| count(stats.total.partitions)),
    },
    Metric {
        key: "partitions_led",
        name: "partitions led",
        group: "placement",
        unit: Unit::Count,
        columns: &["partitions/led"],
        help: "Partitions the archives of the groups the member leads hold. Summed over the \
               members this counts each partition once.",
        read: Reader::Member(|stats| count(stats.total.partitions_led)),
    },
    Metric {
        key: "archived",
        name: "archived",
        group: "placement",
        unit: Unit::Bytes,
        columns: &["archived"],
        help: "Bytes the member's archives hold over every copy it hosts. It lags a write \
               until the write is compacted, so a freshly fed member reads low until then.",
        read: Reader::Member(|stats| count(stats.total.bytes)),
    },
    Metric {
        key: "free",
        name: "free",
        group: "placement",
        unit: Unit::Bytes,
        columns: &["free"],
        help: "Free bytes on the member's storage. The rebalance planner and a move's \
               receiver check it against disk_reserve. Zero when the node could not read it.",
        read: Reader::Member(|stats| count(stats.free_bytes)),
    },
    Metric {
        key: "chained",
        name: "chained",
        group: "placement",
        unit: Unit::Count,
        columns: &["chained"],
        help: "Partitions held as a base record with fragments chained after it (F61): a \
               large sorted partition takes a batch of inserts and deletes as a fragment \
               instead of being rewritten whole. Counted over every copy. An update, a full \
               chain or the archive pass folds a chain back into one record.",
        read: Reader::Member(|stats| count(stats.total.chained)),
    },
    // memory
    Metric {
        key: "rows",
        name: "rows memory",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["rows"],
        help: "Bytes of rows the member's shards hold in memory, which their eviction budgets \
               bound. Reaching the budget evicts 40% of what is held.",
        read: Reader::Member(|stats| count(stats.memory_bytes)),
    },
    Metric {
        key: "budget",
        name: "budget",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["budget"],
        help: "The shards' eviction budgets together: the node's configured memory. Rows \
               memory is read against it.",
        read: Reader::Member(|stats| count(stats.memory_budget)),
    },
    Metric {
        key: "archive_maps",
        name: "archive maps",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["archive maps"],
        help: "Bytes the shards' archive maps hold, estimated from their sizes: the index of \
               where every archived partition lives on disk. No budget counts them, so they \
               grow with the data held.",
        read: Reader::Member(|stats| count(stats.archive_map_bytes)),
    },
    Metric {
        key: "table_maps",
        name: "table maps",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["table maps"],
        help: "Bytes the tables' in-memory partition indexes hold. No budget counts them.",
        read: Reader::Member(|stats| count(stats.table_index_bytes)),
    },
    Metric {
        key: "wal_index",
        name: "wal index",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["wal index"],
        help: "Bytes the write-ahead log's index of its retained entries holds. No budget \
               counts it. It grows with the log each group keeps for a slow copy.",
        read: Reader::Member(|stats| count(stats.wal_index_bytes)),
    },
    Metric {
        key: "lru",
        name: "lru",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["lru"],
        help: "Bytes the eviction lists hold, one entry per evictable partition. No budget \
               counts them.",
        read: Reader::Member(|stats| count(stats.lru_bytes)),
    },
    Metric {
        key: "resident",
        name: "resident",
        group: "memory",
        unit: Unit::Bytes,
        columns: &["resident"],
        help: "The process's resident memory: the rows, and everything the budget does not \
               count, such as the maps, the logs' caches, the groups' state and every buffer. \
               Resident far above rows plus the indexes points at buffers or the allocator.",
        read: Reader::Member(|stats| count(stats.resident_bytes)),
    },
    Metric {
        key: "volatile",
        name: "volatile log",
        group: "memory",
        unit: Unit::Bytes,
        columns: &[],
        help: "Bytes the logs of the ephemeral tables' groups hold in memory.",
        read: Reader::Member(|stats| count(stats.volatile_bytes)),
    },
    // the storage pipeline
    Metric {
        key: "wal_syncs",
        name: "wal syncs/s",
        group: "storage",
        unit: Unit::PerSec,
        columns: &["syncs/s"],
        help: "Write-ahead log batches written and synced per second, one fdatasync each. \
               Every committed write waits for one.",
        read: Reader::Member(|stats| stats.wal_syncs_per_sec),
    },
    Metric {
        key: "wal_bytes",
        name: "wal bytes/s",
        group: "storage",
        unit: Unit::BytesPerSec,
        columns: &["wal/s"],
        help: "Bytes written to the write-ahead log and synced per second.",
        read: Reader::Member(|stats| stats.wal_bytes_per_sec),
    },
    Metric {
        key: "sync_ms",
        name: "sync ms",
        group: "storage",
        unit: Unit::Millis,
        columns: &["sync ms"],
        help: "The mean time one log batch took to write and sync. A device that flushes its \
               cache on every sync shows here, and so does a failing one. Sync time rising \
               while syncs per second stay flat points at the device, not the load.",
        read: Reader::Member(|stats| stats.wal_sync_ms),
    },
    Metric {
        key: "per_sync",
        name: "appends/sync",
        group: "storage",
        unit: Unit::Ratio,
        columns: &["per sync"],
        help: "The mean appends one log sync carried, which is how well group commit batches. \
               Near one means each write pays for a whole sync. wal_commit_delay trades \
               latency for larger batches.",
        read: Reader::Member(|stats| stats.wal_appends_per_sync),
    },
    Metric {
        key: "segments",
        name: "wal segments",
        group: "storage",
        unit: Unit::Count,
        columns: &["segments"],
        help: "Segments the shards' write-ahead logs hold, sealed and open. Segments are kept \
               until every group has applied and purged past them.",
        read: Reader::Member(|stats| count(stats.wal_segments)),
    },
    Metric {
        key: "compacting",
        name: "compacting",
        group: "storage",
        unit: Unit::Count,
        columns: &["compacting"],
        help: "Sealed log segments handed to a compactor and not yet merged by every table in \
               them: the compactors' backlog. A steady climb means the archives cannot keep \
               up with the writes.",
        read: Reader::Member(|stats| count(stats.compacting_segments)),
    },
    Metric {
        key: "apply_lag",
        name: "apply lag",
        group: "storage",
        unit: Unit::Count,
        columns: &["apply lag"],
        help: "Entries committed and not yet applied, over every copy the member hosts. It is \
               above zero for a moment under load. A lasting lag means a shard cannot apply as \
               fast as its groups commit.",
        read: Reader::Member(|stats| count(stats.apply_lag)),
    },
    Metric {
        key: "pending",
        name: "pending",
        group: "storage",
        unit: Unit::Bytes,
        columns: &["pending"],
        help: "Bytes proposed through the member and not yet answered: the writes in flight. \
               Growing means the member takes writes faster than its groups commit them.",
        read: Reader::Member(|stats| count(stats.pending_bytes)),
    },
    Metric {
        key: "busiest_shard",
        name: "busiest shard/s",
        group: "storage",
        unit: Unit::PerSec,
        columns: &["shard writes/s"],
        help: "Writes per second the member's busiest shard applied. A shard is one core and \
               every copy it hosts applies on it, so a busiest shard far above the others \
               paces the whole node. --basic prints this as quietest..busiest.",
        read: Reader::Member(|stats| {
            stats.shard_writes_per_sec.iter().copied().fold(0.0, f64::max)
        }),
    },
];

/// Every word the figures use that is not a metric, with what it means
pub const TERMS: &[Term] = &[
    Term {
        name: "member",
        section: "figures",
        columns: &["member"],
        help: "The member's name: the hostname its figures carry, else the name the \
               deployment gave it, else the first eight characters of its node id. A name two \
               members share is written name(id).",
    },
    Term {
        name: "state",
        section: "figures",
        columns: &["state"],
        help: "The one word to read first: up, down, joining, leaving or removing. (m) means \
               an operator suspended the grace after which a down member is removed \
               (maintenance).",
    },
    Term {
        name: "age",
        section: "figures",
        columns: &["age"],
        help: "How long ago the answering node heard the member's figures. A member sends \
               them about every two seconds. The age is marked ! when the figures are stale, \
               three of their intervals old; a stale member's rates are not printed and not \
               charted.",
    },
    Term {
        name: "hosted and led",
        section: "figures",
        columns: &[],
        help: "Most figures are counted twice: over every copy the member hosts, and over \
               the copies whose group it leads. Every row has exactly one leader, so the led \
               figures summed over the members count it once, while the hosted ones count it \
               once per replica. A pair such as groups/led is hosted/led.",
    },
    Term {
        name: "rate windows",
        section: "figures",
        columns: &[],
        help: "Every rate is a trailing average over roughly ten seconds, a minute and five \
               minutes (10s/1m/5m). --basic prints all three. The chart samples the ten \
               second one on every poll, so its history is the chart itself.",
    },
    Term {
        name: "local view",
        section: "figures",
        columns: &[],
        help: "Only the control leader holds every member's figures. A node that is not the \
               leader answers its own and names the leader, which is dialed and asked \
               instead. When that fails the header says why, and the other members show no \
               figures.",
    },
    Term {
        name: "applied/s",
        section: "tables",
        columns: &["applied/s"],
        help: "Each member's rates over every copy it hosts, 10s/1m/5m: rows inserted, updated \
               and deleted, the intent bytes of all three, and snapshot bytes streamed out.",
    },
    Term {
        name: "memory",
        section: "tables",
        columns: &["memory"],
        help: "Each member's rows against its eviction budget, the indexes held beside them \
               that no budget counts, and the process's resident memory.",
    },
    Term {
        name: "storage",
        section: "tables",
        columns: &["storage"],
        help: "Each member's log syncs, compaction backlog, apply lag, writes in flight, and \
               how its shards share the work: what tells a slow node's cause apart.",
    },
    Term {
        name: "sizes <4K..>1M %",
        section: "tables",
        columns: &["sizes <4K..>1M %"],
        help: "The share of the interval's log syncs by batch size, in percent: under 4 KiB, \
               16 KiB, 64 KiB, 256 KiB and 1 MiB, and the rest. A group commit that settles on \
               small batches fills the first buckets.",
    },
    Term {
        name: "led by shard",
        section: "tables",
        columns: &["led by shard"],
        help: "How many groups each shard leads, in shard order. A leader works on its \
               shard's core, so leads bunched on a few shards pace the node however evenly its \
               share is counted.",
    },
    Term {
        name: "busiest groups",
        section: "tables",
        columns: &["busiest groups", "table", "led by", "writes/s", "bytes/s"],
        help: "The groups the members lead that wrote the most over the last interval: the \
               group, the table it serves, the member leading it, and its rows and intent \
               bytes per second. Every member applies every write, so only a leader's own \
               groups say where the work is.",
    },
    Term {
        name: "table",
        section: "tables",
        columns: &[
            "table",
            "partitions",
            "archived",
            "chained",
            "insert/s",
            "update/s",
            "delete/s",
        ],
        help: "Each table summed over the members through its leaders, so a row counts once, \
               with its write rates 10s/1m/5m. Printed when there is more than one table or \
               --table names one. chained is over every copy.",
    },
    Term {
        name: "moved",
        section: "plans",
        columns: &[],
        help: "A plan's steps whose move is done, out of all its steps. moving, pending and \
               failed count the rest: running, not issued yet, and failed.",
    },
    Term {
        name: "bytes",
        section: "plans",
        columns: &[],
        help: "The planned bytes of the steps that moved, out of the bytes every step held \
               when planned. streamed is the snapshot bytes the moves actually sent.",
    },
    Term {
        name: "elapsed and /step",
        section: "plans",
        columns: &[],
        help: "How long the plan has run since its first move started, and how long a \
               finished step took on average. Both use the group leaders' clocks.",
    },
    Term {
        name: "avg and now",
        section: "plans",
        columns: &[],
        help: "avg is the planned bytes moved per second of the plan's elapsed time. now is \
               what the members the running steps move from are streaming, over a minute.",
    },
    Term {
        name: "eta",
        section: "plans",
        columns: &[],
        help: "The larger of two estimates: the bytes left at the current rate, and the steps \
               left at the mean step time. Every step waits out its source's retire_after, so \
               a plan of small sets is paced by steps, not bytes.",
    },
    Term {
        name: "blocked",
        section: "plans",
        columns: &[],
        help: "Why the plan cannot go on, such as no member with room for a step.",
    },
];

/// The indexes into [`METRICS`] of one group's metrics, in the order they are listed
///
/// # Arguments
///
/// * `group` - The group, one of [`GROUPS`]
#[must_use]
pub fn in_group(group: &str) -> Vec<usize> {
    METRICS
        .iter()
        .enumerate()
        .filter(|(_, metric)| metric.group == group)
        .map(|(index, _)| index)
        .collect()
}

/// The index of a metric by its key
///
/// # Arguments
///
/// * `key` - The metric's key
#[must_use]
pub fn index_of(key: &str) -> Option<usize> {
    METRICS.iter().position(|metric| metric.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::stats::{
        APPLIED_COLUMNS, COMPACT_COLUMNS, HOT_COLUMNS, MEMBER_COLUMNS, MEMORY_COLUMNS,
        STORAGE_COLUMNS, TABLE_COLUMNS, TABLE_TITLES,
    };
    use std::collections::HashSet;

    /// Every metric has a key of its own, a known group and help, and every column `--basic`
    /// prints is explained by a metric or a term
    #[test]
    fn every_metric_and_column_has_help() {
        // keys are unique, since a selection is kept by them
        let keys: HashSet<&str> = METRICS.iter().map(|metric| metric.key).collect();
        assert_eq!(keys.len(), METRICS.len());
        // every metric is listed under a group the view draws, and says what it means
        let groups: HashSet<&str> = GROUPS.iter().map(|(group, _)| *group).collect();
        for metric in METRICS {
            assert!(groups.contains(metric.group), "{} has no group", metric.key);
            assert!(metric.help.len() > 20, "{} has no help", metric.key);
        }
        // every group has at least one metric, so no heading is drawn empty
        for (group, _) in GROUPS {
            assert!(METRICS.iter().any(|metric| metric.group == group), "{group} is empty");
        }
        // every term sits in a section the help page draws
        let sections: HashSet<&str> = TERM_SECTIONS.iter().map(|(section, _)| *section).collect();
        for term in TERMS {
            assert!(sections.contains(term.section), "{} has no section", term.name);
            assert!(term.help.len() > 20, "{} has no help", term.name);
        }
        // every column and title of every table is explained somewhere
        let explained: HashSet<&str> = METRICS
            .iter()
            .flat_map(|metric| metric.columns.iter().copied())
            .chain(TERMS.iter().flat_map(|term| term.columns.iter().copied()))
            .collect();
        let printed = MEMBER_COLUMNS
            .iter()
            .chain(APPLIED_COLUMNS.iter())
            .chain(MEMORY_COLUMNS.iter())
            .chain(STORAGE_COLUMNS.iter())
            .chain(HOT_COLUMNS.iter())
            .chain(TABLE_COLUMNS.iter())
            .chain(COMPACT_COLUMNS.iter())
            .chain(TABLE_TITLES.iter());
        for column in printed {
            assert!(explained.contains(column), "the column {column} has no help");
        }
        // every metric is in exactly one group's tab, in the catalog's order
        let mut tabbed: Vec<usize> = GROUPS.iter().flat_map(|(group, _)| in_group(group)).collect();
        tabbed.sort_unstable();
        assert_eq!(tabbed, (0..METRICS.len()).collect::<Vec<_>>());
        assert_eq!(in_group("streams").len(), 2);
        assert!(in_group("nothing").is_empty());
        // and a key finds its metric
        assert_eq!(index_of("applied").map(|index| METRICS[index].name), Some("applied writes/s"));
        assert_eq!(index_of("nothing"), None);
    }

    /// Every unit writes its values the way the tables do
    #[test]
    fn units_write_values_as_the_tables_do() {
        assert_eq!(Unit::PerSec.format(12_500.0), "12.5k");
        assert_eq!(Unit::BytesPerSec.format(2048.0), "2.0KiB/s");
        assert_eq!(Unit::Bytes.format(1024.0 * 1024.0), "1.0MiB");
        assert_eq!(Unit::Millis.format(1.5), "1.50ms");
        assert_eq!(Unit::Count.format(3.0), "3");
        assert_eq!(Unit::Count.format(12_500.0), "12.5k");
        assert_eq!(Unit::Ratio.format(2.26), "2.3");
        // a negative reading is never written as one
        assert_eq!(Unit::Bytes.format(-5.0), "0B");
    }
}
