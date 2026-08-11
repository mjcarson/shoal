//! Turns per query stage records into a report of where a query's latency went
//!
//! A workload answers *how long* a query took. This answers *where* it went. The two are kept
//! apart on purpose: a stage profile is captured from a separate `stage-profile` build whose
//! absolute latencies are not comparable to a normal one, exactly like a `hotpath` profile, so it
//! must never become the source of a baseline number.
//!
//! # This used to be a mirror, and is not one now
//!
//! This module lived in `shoal/src/stages.rs` and defined its own `StageReport`, which
//! [`crate::model::stages`] then re-declared field for field so the runner could read what it
//! wrote. Moving it here lets it build *those* structs, so the writer and the reader are the same
//! types and there is nothing left to drift - the same trade the macro artifact made when the
//! workloads moved into this crate.
//!
//! It is compiled only under `stage-profile`, because only a build with that feature has any
//! records to report on. The model it builds is always compiled, because the runner has to read a
//! committed report whether or not this build could have produced one.
//!
//! The shape of the report is the part worth understanding. Percentiles of individual stages
//! do not add up to the percentile of the total — the query that had the worst `durable_sync`
//! is usually not the query that had the worst total — so a table of per stage percentiles
//! reads like an explanation while being none. Instead, records are ranked by their **total**
//! latency, a window of records around each rank is taken, and the mean of each stage over
//! that window is reported. That answers "what were the slow queries actually waiting on",
//! which is the question.

use crate::model::stages::{Bucket, JoinStats, OpReport, StageCost, StageReport};
use shoal_core::server::stage_profile::{StageOp, StageRecord, Stamp};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use uuid::Uuid;

/// The schema version of a written stage report
///
/// Defined once, in the model, so the writer and the reader cannot disagree about it. Bump it
/// there whenever [`StageReport`] changes shape, or whenever what a stage *measures* changes - the
/// second is the one that bites, since a report that parses cleanly while its stages mean
/// something else is worse than one that does not parse at all.
use crate::model::stages::STAGE_REPORT_VERSION as REPORT_VERSION;

/// The fraction of records taken around each rank to form a bucket
///
/// A bucket has to be a window rather than a single record, or the "mean breakdown" at p999
/// is the breakdown of exactly one query and reads as though it were representative.
const BUCKET_FRACTION: f64 = 0.005;

/// The fewest records a bucket is built from when the sample set is small
///
/// [`BUCKET_FRACTION`] of a few thousand records is a handful, so this floor is what keeps a
/// short run's report from being a list of individual queries.
const MIN_BUCKET: usize = 100;

/// How many times the clock's own cost a stage has to exceed to be reported as a number
///
/// Several stages here are queue hops of tens of nanoseconds, differenced from two clock
/// reads that each cost about that much. Below this multiple the measurement is dominated by
/// the instrument, so it is reported as being at the floor instead of as a value.
const FLOOR_MULTIPLE: f64 = 2.0;

/// The ranks a report breaks each operation down at
///
/// `None` is the whole population, which is both a useful summary and the sanity check that
/// the buckets are not hiding anything — if every rank looks like the whole population, the
/// bucketing found no structure.
const RANKS: [(&str, Option<f64>); 6] = [
    ("all", None),
    ("p50", Some(0.50)),
    ("p90", Some(0.90)),
    ("p99", Some(0.99)),
    ("p999", Some(0.999)),
    ("max", Some(1.0)),
];

/// One stage of a query's journey, named in the order they happen
///
/// The order here is the order they are reported in, and it is the order the offsets are
/// differenced in, so it has to stay the real order of the pipeline.
const STAGE_NAMES: [&str; 19] = [
    "client_serialize",
    "client_pool",
    "client_write",
    "net_in",
    "shard_queue_in",
    "decode",
    "route",
    "exec_queue",
    "execute",
    "durable_staged",
    "durable_write",
    "durable_sync_wait",
    "durable_sync",
    "durable_rotated",
    "release_wake",
    "reply_serialize",
    "reply_queue",
    "socket_write",
    "net_out",
];

/// Which stages are paid once per batch and shared by every query in it
///
/// Marked in the report rather than silently folded in with the per query stages. A reader
/// who does not know a stage is batch level will read a large `decode` as "deserializing this
/// query was slow" when it means "deserializing the hundred queries it arrived with was".
const BATCH_STAGES: [&str; 4] = ["client_serialize", "client_pool", "client_write", "decode"];

/// How many queries this run keeps one client side record for
///
/// **This must agree exactly with `shoal_core::server::stage_profile`'s own sample rate.** Both
/// sides sample on the query index rather than at random, so that the client and the server keep
/// the *same* queries and the join has something to work with; if the two disagreed, each half
/// would keep a disjoint set and the report would join nothing at all.
///
/// Agreement is by construction rather than by discipline: both read the same environment
/// variable, which is set once before the shards start. `shoal-core`'s reader is private, which is
/// the only reason this is a second function rather than a call to that one.
pub fn sample_rate() -> usize {
    // an unset, unparseable, or zero rate all mean "keep everything", matching the server exactly.
    // a rate of zero would otherwise divide by zero, and a partial profile is worse than a large
    // one.
    std::env::var(shoal_core::server::stage_profile::SAMPLE_ENV)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|rate| *rate > 0)
        .unwrap_or(1)
}

/// What the client side recorded about one query
///
/// The server's own record covers from the socket read to the socket write. These are the
/// pieces on either side of that, which the client is the only one that can see.
#[derive(Debug, Clone, Copy)]
pub struct ClientRecord {
    /// The stream this query belonged to
    pub id: Uuid,
    /// This query's index within that stream
    pub index: usize,
    /// When the bundle carrying this query was handed to `send`
    pub submitted: Stamp,
    /// When that bundle finished being serialized
    pub serialized: Stamp,
    /// When a pooled connection was acquired for it
    pub pooled: Stamp,
    /// When its last byte was handed to the socket
    pub written: Stamp,
    /// When this query's response came off the socket
    pub arrived: Stamp,
}

// The five structs a report is made of - `StageCost`, `Bucket`, `OpReport`, `JoinStats` and
// `StageReport` - used to be declared here and re-declared in `crate::model::stages` so the
// runner could read them. They are imported from there now, which is the whole point of this
// module having moved: one definition, built by the writer and read by the reader.
//
// The model's versions key their maps with a `BTreeMap` rather than a `HashMap`. That is not
// cosmetic - the rendered results page is committed and verified byte for byte, so a map that
// iterates in a per process order would make the page differ between two runs over identical
// artifacts.

/// One query's stages, in nanoseconds, ready to be summarized
///
/// Offsets are turned into durations here, once, rather than at every place a bucket is
/// built. A stage a query never reached stays `None` — it is not the same as a stage that
/// took no time, and a report that spelled it zero would report a get as having a very fast
/// fdatasync rather than none at all.
#[derive(Debug, Clone)]
struct Journey {
    /// The kind of query this was
    op: StageOp,
    /// Whether this response came out on a rotation rather than on a watermark
    rotated: bool,
    /// The total nanoseconds from the client handing this query to `send` until its response
    /// came back
    total_ns: u64,
    /// What each stage cost, in the order of [`STAGE_NAMES`]
    stages: [Option<u64>; STAGE_NAMES.len()],
}

/// Get the nanoseconds between two stamps, or nothing if either is missing
///
/// # Arguments
///
/// * `from` - The stamp the stage started at
/// * `to` - The stamp the stage ended at
fn span(from: Option<Stamp>, to: Option<Stamp>) -> Option<u64> {
    // a stage is only a measurement when both of its ends are
    match (from, to) {
        (Some(from), Some(to)) => Some(to.since(from)),
        _ => None,
    }
}

/// Build the report for a run from its two halves
///
/// The join is by `(id, index)`, the pair that uniquely identifies one query within one
/// stream. Both halves have to come from the same process, since the stamps are
/// `CLOCK_MONOTONIC` readings and comparing them across machines is meaningless.
///
/// # Arguments
///
/// * `server` - The records the shards emitted
/// * `client` - The records the workers recorded
/// * `label` - The name this run was captured under
/// * `clock_overhead_ns` - What one clock reading costs on this machine
pub fn build_report(
    server: &[StageRecord],
    client: &[ClientRecord],
    label: Option<String>,
    clock_overhead_ns: u64,
) -> StageReport {
    // count everything the join does rather than dropping any of it
    let mut join = JoinStats::default();
    // index the client side by the key both halves share
    let mut by_key: HashMap<(Uuid, usize), ClientRecord> = HashMap::with_capacity(client.len());
    for record in client {
        by_key.insert((record.id, record.index), *record);
    }
    // remember which client records we matched, so the unmatched ones can be counted
    let mut matched: HashMap<(Uuid, usize), usize> = HashMap::with_capacity(server.len());
    // turn each pair of records into one journey
    let mut journeys = Vec::with_capacity(server.len());
    for record in server {
        let key = (record.id, record.stamps.index);
        // a query is owed exactly one response, so a repeated key means one was answered
        // twice - count it rather than letting it quietly inflate a bucket
        let seen = matched.entry(key).or_insert(0);
        *seen += 1;
        if *seen > 1 {
            join.duplicates += 1;
            continue;
        }
        // find the client half of this query
        let Some(client) = by_key.get(&key) else {
            join.server_only += 1;
            continue;
        };
        // note when a durability window had already aged out, since that record's durability
        // stages are missing rather than fast
        if record.stamps.flags.window_missing {
            join.window_missing += 1;
        }
        // build this queries journey, dropping it if any stage ran off the end of an offset
        match journey(record, client) {
            Some(journey) => {
                join.joined += 1;
                journeys.push(journey);
            }
            None => join.saturated += 1,
        }
    }
    // every client record we never matched is one the server has no record of
    join.client_only = by_key
        .keys()
        .filter(|key| !matched.contains_key(key))
        .count();
    // break each kind of query down on its own, since pooling them makes a percentile report
    // where the boundary between two distributions landed
    let mut ops: BTreeMap<String, OpReport> = BTreeMap::new();
    for op in [
        StageOp::Insert,
        StageOp::Get,
        StageOp::Exists,
        StageOp::Delete,
        StageOp::Update,
        StageOp::Other,
    ] {
        // gather every journey this operation produced
        let mut mine: Vec<&Journey> = journeys.iter().filter(|entry| entry.op == op).collect();
        // an operation a run never issued has nothing to report
        if mine.is_empty() {
            continue;
        }
        // rank by total latency, which is what the buckets are windows into
        mine.sort_unstable_by_key(|entry| entry.total_ns);
        // count how many of these came out on a rotation and so have no durability stages
        let rotated = mine.iter().filter(|entry| entry.rotated).count();
        // build a bucket at each rank
        let buckets = RANKS
            .iter()
            .filter_map(|(name, rank)| bucket(&mine, name, *rank, clock_overhead_ns))
            .collect();
        ops.insert(
            op.as_str().to_string(),
            OpReport {
                count: mine.len(),
                rotated,
                buckets,
            },
        );
    }
    StageReport {
        version: REPORT_VERSION,
        label,
        clock: "std::time::Instant".to_string(),
        clock_overhead_ns,
        join,
        ops,
    }
}

/// Turn one pair of records into the stages of a single journey
///
/// Returns nothing when any stage ran past what an offset can hold, since a saturated offset
/// is not a measurement and a bucket built from one would be reporting a number it does not
/// have.
///
/// # Arguments
///
/// * `server` - The record the shard emitted
/// * `client` - The record the worker recorded
fn journey(server: &StageRecord, client: &ClientRecord) -> Option<Journey> {
    let stamps = &server.stamps;
    // a stage that ran off the end of an offset makes the whole journey unusable
    for offset in [
        stamps.bundle_dequeued,
        stamps.decoded,
        stamps.routed,
        stamps.exec_dequeued,
        stamps.exec_done,
        stamps.released,
        stamps.replied,
        stamps.queued_to_client,
        stamps.socket_written,
    ] {
        if offset.is_saturated() {
            return None;
        }
    }
    // turn each server side offset back into an absolute stamp, so client and server stages
    // can be differenced against each other
    let base = stamps.base;
    let at = |offset: shoal_core::server::stage_profile::Offset| -> Option<Stamp> {
        offset
            .nanos()
            .map(|nanos| base.plus_nanos(u64::from(nanos)))
    };
    // the server side stamps, in pipeline order
    let bundle_dequeued = at(stamps.bundle_dequeued);
    let decoded = at(stamps.decoded);
    let routed = at(stamps.routed);
    let exec_dequeued = at(stamps.exec_dequeued);
    let exec_done = at(stamps.exec_done);
    let write_submitted = at(stamps.write_submitted);
    let write_completed = at(stamps.write_completed);
    let sync_issued = at(stamps.sync_issued);
    let sync_completed = at(stamps.sync_completed);
    let released = at(stamps.released);
    let replied = at(stamps.replied);
    let queued = at(stamps.queued_to_client);
    let written = at(stamps.socket_written);
    // the moment the durability wait ends is whichever of these a write actually reached,
    // since a table running without fdatasync never reaches the sync stages at all
    let durable_end = released;
    // each stage runs from the end of the one before it to its own end
    let stages = [
        // client_serialize
        span(Some(client.submitted), Some(client.serialized)),
        // client_pool
        span(Some(client.serialized), Some(client.pooled)),
        // client_write
        span(Some(client.pooled), Some(client.written)),
        // net_in - the wire time between the clients write and the servers read
        Some(base.since(client.written)),
        // shard_queue_in
        span(Some(base), bundle_dequeued),
        // decode
        span(bundle_dequeued, decoded),
        // route
        span(decoded, routed),
        // exec_queue
        span(routed, exec_dequeued),
        // execute
        span(exec_dequeued, exec_done),
        // durable_staged - time spent in the DMA buffer before anything submitted it
        span(exec_done, write_submitted),
        // durable_write
        span(write_submitted, write_completed),
        // durable_sync_wait - time between a write landing and a sync claiming it
        span(write_completed, sync_issued),
        // durable_sync
        span(sync_issued, sync_completed),
        // durable_rotated - the whole wait, for a response a rotation released
        //
        // a rotation fdatasyncs the old log and throws its timeline away, so these records
        // have no phase breakdown at all. Naming the wait rather than leaving it in the
        // residual is what stops it reading as a stage nobody accounted for.
        if write_submitted.is_none() {
            span(exec_done, durable_end)
        } else {
            None
        },
        // release_wake - the shard noticing the watermark moved and sweeping
        span(
            sync_completed.or(write_completed).or(write_submitted),
            durable_end,
        ),
        // reply_serialize
        span(durable_end.or(exec_done), replied),
        // reply_queue
        span(replied, queued),
        // socket_write
        span(queued, written),
        // net_out - the wire time between the servers write and the clients read
        span(written, Some(client.arrived)),
    ];
    Some(Journey {
        op: stamps.flags.op,
        rotated: stamps.flags.rotated,
        // the total is what the client actually waited, end to end
        total_ns: client.arrived.since(client.submitted),
        stages,
    })
}

/// Build one rank's bucket from a set of journeys already sorted by total latency
///
/// A bucket is a window around the rank rather than the single record at it, because the
/// mean of one record is that record and reporting it as a breakdown of "the p999 query"
/// invites reading noise as structure.
///
/// # Arguments
///
/// * `sorted` - Every journey for one operation, sorted ascending by total
/// * `name` - The name this rank is reported under
/// * `rank` - The rank to centre this bucket on, or nothing for the whole population
/// * `clock_overhead_ns` - What one clock reading costs on this machine
fn bucket(
    sorted: &[&Journey],
    name: &str,
    rank: Option<f64>,
    clock_overhead_ns: u64,
) -> Option<Bucket> {
    // an empty population has no ranks
    if sorted.is_empty() {
        return None;
    }
    // pick the slice of records this bucket summarizes
    let window: &[&Journey] = match rank {
        // the whole population is its own bucket, and doubles as the check that the ranked
        // buckets are not hiding anything
        None => sorted,
        Some(rank) => {
            // find the record at this rank
            let centre = ((sorted.len() as f64 * rank).ceil() as usize)
                .saturating_sub(1)
                .min(sorted.len() - 1);
            // take a window of records around it, floored so a small run still gets a
            // population rather than a single sample
            let width = std::cmp::max(
                (sorted.len() as f64 * BUCKET_FRACTION).ceil() as usize,
                MIN_BUCKET,
            );
            // clamp the window into the population, keeping it against the top for `max`
            let half = width / 2;
            let start = centre
                .saturating_sub(half)
                .min(sorted.len().saturating_sub(width.min(sorted.len())));
            let end = (start + width).min(sorted.len());
            &sorted[start..end]
        }
    };
    // the mean total this bucket's stages have to add up to
    let total_ns = mean(window.iter().map(|entry| Some(entry.total_ns)))?;
    // summarize each stage over the records in this bucket that reached it
    let mut stages = BTreeMap::new();
    // track what the stages accounted for, so the residual can be reported rather than hidden
    let mut accounted: i64 = 0;
    for (index, stage) in STAGE_NAMES.iter().enumerate() {
        // gather what this stage cost for every record that reached it
        let samples = window
            .iter()
            .filter_map(|entry| entry.stages[index])
            .count();
        // a stage nothing in this bucket reached is left out rather than reported as zero
        let Some(mean_ns) = mean(window.iter().map(|entry| entry.stages[index])) else {
            continue;
        };
        // a stage is only counted towards the total for the records that reached it, so
        // scale its mean by how much of the bucket that was
        accounted += (mean_ns as f64 * samples as f64 / window.len() as f64) as i64;
        stages.insert(
            (*stage).to_string(),
            StageCost {
                mean_ns,
                share: if total_ns > 0 {
                    mean_ns as f64 / total_ns as f64
                } else {
                    0.0
                },
                per_batch: BATCH_STAGES.contains(stage),
                // a stage of the same order as the clock read it was measured with is
                // reporting the instrument rather than the pipeline
                at_floor: (mean_ns as f64) < clock_overhead_ns as f64 * FLOOR_MULTIPLE,
                samples,
            },
        );
    }
    Some(Bucket {
        rank: name.to_string(),
        total_ns,
        samples: window.len(),
        stages,
        // whatever the stages did not explain, stated outright
        unaccounted_ns: total_ns as i64 - accounted,
    })
}

/// Get the mean of the values that exist, or nothing if none do
///
/// # Arguments
///
/// * `values` - The samples to average, each of which may be missing
fn mean(values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    // sum only the samples that are measurements
    let (sum, count) = values
        .flatten()
        .fold((0u128, 0usize), |(sum, count), value| {
            (sum + u128::from(value), count + 1)
        });
    // a stage nothing reached has no mean, which is not the same as a mean of zero
    if count == 0 {
        return None;
    }
    Some((sum / count as u128) as u64)
}

/// Write a report to disk as pretty JSON
///
/// # Arguments
///
/// * `report` - The report to write
/// * `path` - Where to write it
pub fn write_report(report: &StageReport, path: &Path) -> std::io::Result<()> {
    // pretty printed so a human can read a stage breakdown without a tool
    let body = serde_json::to_string_pretty(report)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    std::fs::write(path, body)
}

/// Read a report back, refusing one written by a different schema version
///
/// # Arguments
///
/// * `path` - The report to read
pub fn read_report(path: &Path) -> Result<StageReport, String> {
    // read the file itself
    let body = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    // parse it before deciding whether we can use it
    let report: StageReport = serde_json::from_str(&body).map_err(|error| error.to_string())?;
    // a report that parses cleanly while its stages mean something else is worse than one
    // that does not parse at all, so the version is checked rather than assumed
    if report.version != REPORT_VERSION {
        return Err(format!(
            "stage report is version {} but this build writes version {REPORT_VERSION}",
            report.version
        ));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::{
        build_report, read_report, write_report, ClientRecord, BUCKET_FRACTION, MIN_BUCKET,
        REPORT_VERSION, STAGE_NAMES,
    };
    use shoal_core::server::stage_profile::{Offset, StageOp, StageRecord, StageStamps, Stamp};
    use uuid::Uuid;

    /// Build one server record whose stages each take a known number of nanoseconds
    ///
    /// # Arguments
    ///
    /// * `id` - The stream this query belonged to
    /// * `index` - This query's index within that stream
    /// * `op` - The kind of query this was
    /// * `step` - How long each server side stage takes
    /// * `base` - The stamp this query's offsets are measured from
    fn server_record(id: Uuid, index: usize, op: StageOp, step: u64, base: Stamp) -> StageRecord {
        // walk the stages forward by a fixed step so every one has a known cost
        let mut stamps = StageStamps::new(base);
        stamps.set_index(index);
        stamps.set_op(op);
        let mut at = 0;
        let mut next = |at: &mut u64| {
            *at += step;
            Offset::between(base, base.plus_nanos(*at))
        };
        stamps.bundle_dequeued = next(&mut at);
        stamps.decoded = next(&mut at);
        stamps.routed = next(&mut at);
        stamps.exec_dequeued = next(&mut at);
        stamps.exec_done = next(&mut at);
        // only a write parks on the intent log, so only a write reaches the durability
        // stages or is ever released - which is the case that proves an unset stage is not
        // reported as a fast one
        if op == StageOp::Insert {
            stamps.write_submitted = next(&mut at);
            stamps.write_completed = next(&mut at);
            stamps.sync_issued = next(&mut at);
            stamps.sync_completed = next(&mut at);
            stamps.released = next(&mut at);
        }
        stamps.replied = next(&mut at);
        stamps.queued_to_client = next(&mut at);
        stamps.socket_written = next(&mut at);
        StageRecord {
            epoch: 0,
            id,
            stamps,
        }
    }

    /// Build the client record that pairs with a server one
    ///
    /// # Arguments
    ///
    /// * `id` - The stream this query belonged to
    /// * `index` - This query's index within that stream
    /// * `step` - How long each client side stage takes
    /// * `base` - The stamp the server started measuring from
    /// * `server_span` - How long the server side of this query took
    fn client_record(
        id: Uuid,
        index: usize,
        step: u64,
        base: Stamp,
        server_span: u64,
    ) -> ClientRecord {
        ClientRecord {
            id,
            index,
            // the client starts three steps before the server's base
            submitted: base.minus_nanos(step * 3),
            serialized: base.minus_nanos(step * 2),
            pooled: base.minus_nanos(step),
            written: base,
            arrived: base.plus_nanos(server_span + step),
        }
    }

    /// Build a matched pair of halves for a set of queries
    ///
    /// # Arguments
    ///
    /// * `count` - How many queries to build
    /// * `op` - The kind of query they were
    fn run(count: usize, op: StageOp) -> (Vec<StageRecord>, Vec<ClientRecord>) {
        let id = Uuid::new_v4();
        let base = Stamp::now();
        let mut server = Vec::with_capacity(count);
        let mut client = Vec::with_capacity(count);
        for index in 0..count {
            // give each query a different step so the totals spread out and rank
            let step = 100 + index as u64;
            // a write walks thirteen server side steps, a read walks eight
            let server_stages = if op == StageOp::Insert { 13 } else { 8 };
            server.push(server_record(id, index, op, step, base));
            client.push(client_record(id, index, step, base, step * server_stages));
        }
        (server, client)
    }

    #[test]
    /// A bucket's stage means add up to its total, with the residual stated
    ///
    /// This is the invariant that makes the report readable at all. Stages that do not sum to
    /// their total mean one is missing or one is double counted, and a report that rounded
    /// that away would hide the bug instead of showing it.
    fn bucket_means_reconcile() {
        let (server, client) = run(1000, StageOp::Get);
        let report = build_report(&server, &client, None, 20);
        let get = report.ops.get("get").expect("no get records were reported");
        for bucket in &get.buckets {
            // the stages have to explain the total to within a rounding error per stage
            let slack = STAGE_NAMES.len() as i64;
            assert!(
                bucket.unaccounted_ns.abs() <= slack,
                "{} left {}ns unaccounted for out of {}ns",
                bucket.rank,
                bucket.unaccounted_ns,
                bucket.total_ns
            );
        }
    }

    #[test]
    /// A bucket is a window of records, not the single record at its rank
    fn a_bucket_is_a_window_not_a_point() {
        let (server, client) = run(10_000, StageOp::Get);
        let report = build_report(&server, &client, None, 20);
        let get = report.ops.get("get").expect("no get records were reported");
        // every rank has to summarize a population, or its "mean" is one query
        let expected = std::cmp::max((10_000.0 * BUCKET_FRACTION).ceil() as usize, MIN_BUCKET);
        for bucket in &get.buckets {
            if bucket.rank == "all" {
                assert_eq!(bucket.samples, 10_000);
            } else {
                assert_eq!(
                    bucket.samples, expected,
                    "{} was built from {} records",
                    bucket.rank, bucket.samples
                );
            }
        }
    }

    #[test]
    /// A stage a query never reached is absent rather than reported as instant
    fn unset_is_not_zero() {
        let (server, client) = run(500, StageOp::Get);
        let report = build_report(&server, &client, None, 20);
        let get = report.ops.get("get").expect("no get records were reported");
        let all = &get.buckets[0];
        // a get never touches the intent log, so it has no durability stages at all -
        // reporting them as zero would say a get syncs faster than a write rather than that
        // it never syncs
        for stage in [
            "durable_staged",
            "durable_write",
            "durable_sync_wait",
            "durable_sync",
        ] {
            assert!(
                !all.stages.contains_key(stage),
                "a get reported a {stage} stage"
            );
        }
        // and the stages it does reach are there
        assert!(all.stages.contains_key("execute"));
    }

    #[test]
    /// A write reports the durability stages a get does not
    fn a_write_reports_its_durability_stages() {
        let (server, client) = run(500, StageOp::Insert);
        let report = build_report(&server, &client, None, 20);
        let insert = report
            .ops
            .get("insert")
            .expect("no insert records were reported");
        let all = &insert.buckets[0];
        // these four are the split that makes the whole exercise worth doing, so their
        // absence is a regression rather than a cosmetic one
        for stage in [
            "durable_staged",
            "durable_write",
            "durable_sync_wait",
            "durable_sync",
        ] {
            assert!(
                all.stages.contains_key(stage),
                "an insert did not report a {stage} stage"
            );
        }
    }

    #[test]
    /// Every record is accounted for, matched or not
    fn join_accounts_for_every_record() {
        let (mut server, mut client) = run(300, StageOp::Get);
        // a server record whose client half never arrived
        let orphan_id = Uuid::new_v4();
        server.push(server_record(orphan_id, 0, StageOp::Get, 100, Stamp::now()));
        // a client record whose server half never arrived
        client.push(client_record(Uuid::new_v4(), 7, 100, Stamp::now(), 900));
        // a duplicate of a key we already have, which is what a double reply looks like
        let duplicate = server[0];
        server.push(duplicate);
        let report = build_report(&server, &client, None, 20);
        assert_eq!(report.join.joined, 300);
        assert_eq!(report.join.server_only, 1);
        assert_eq!(report.join.client_only, 1);
        assert_eq!(report.join.duplicates, 1);
    }

    #[test]
    /// A stage close to the cost of a clock read is marked rather than reported as a value
    fn a_stage_at_the_clock_floor_is_marked() {
        let (server, client) = run(500, StageOp::Get);
        // claim a clock overhead far above every stage in this run, so all of them are floor
        let report = build_report(&server, &client, None, 1_000_000);
        let get = report.ops.get("get").expect("no get records were reported");
        for stage in get.buckets[0].stages.values() {
            assert!(
                stage.at_floor,
                "a stage below the clock floor was reported as a measurement"
            );
        }
    }

    #[test]
    /// A batch level stage is labelled as one
    fn batch_stages_are_labelled() {
        let (server, client) = run(500, StageOp::Get);
        let report = build_report(&server, &client, None, 20);
        let all = &report.ops.get("get").unwrap().buckets[0];
        // charging a batch level cost to a query without saying so reads as a per query cost
        assert!(all.stages.get("decode").unwrap().per_batch);
        assert!(all.stages.get("client_write").unwrap().per_batch);
        // while a genuinely per query stage is not labelled
        assert!(!all.stages.get("execute").unwrap().per_batch);
    }

    #[test]
    /// A report from a different schema version is refused rather than compared
    ///
    /// The same rule the benchmark baseline follows, for the same reason: a file that parses
    /// cleanly while measuring something else costs a wrong answer.
    fn a_report_from_another_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stages.json");
        let (server, client) = run(200, StageOp::Get);
        let mut report = build_report(&server, &client, Some("round-trip".into()), 20);
        // a report of our own version round trips
        write_report(&report, &path).unwrap();
        let read = read_report(&path).expect("a freshly written report did not load");
        assert_eq!(read.version, REPORT_VERSION);
        assert_eq!(read.label.as_deref(), Some("round-trip"));
        // one claiming another version parses and is still refused
        report.version = REPORT_VERSION + 1;
        write_report(&report, &path).unwrap();
        assert!(
            read_report(&path).is_err(),
            "a report from another version was accepted"
        );
    }
}
