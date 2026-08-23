//! Driving a stream of queries at the server and timing what comes back
//!
//! # What a per batch sample is, and is not
//!
//! This driver saturates. It buffers queries into a batch, sends the batch, and keeps a bounded
//! number of them outstanding so the pipeline never empties. One timestamp covers a whole batch,
//! so every query in it is charged for the ones ahead of it - a sample here is a batch completion
//! time, not a service time.
//!
//! That is a deliberate limitation of this driver rather than a defect in it. Saturating is the
//! only way to measure throughput, and stamping each query individually under saturation measures
//! queueing rather than service. Workloads that want a service time declare
//! [`Timing::PerQuery`](crate::model::macro_layer::Timing::PerQuery) and run at a bounded
//! concurrency instead, which costs throughput to get latency. The artifact records which of the
//! two a number came from, and a comparison never joins one to the other.
//!
//! The number worth reading off a per batch workload is its wall clock.
//!
//! # Every driver here gathers its stage records the same way
//!
//! Each of them hands what it sent and what came back to
//! [`Measurement::stages`](crate::workloads::workload::Measurement::stages), which is a
//! [`StageLog`](crate::workloads::stage_log::StageLog) and is a zero sized type unless this is a
//! profiling build. That is deliberate and it is load bearing: the bookkeeping used to live inline
//! in [`drive_with`] and nowhere else, so every workload whose measured phase ran through one of
//! the per query drivers produced a stage report with nothing in it
//! ([Resolved #76](../../../../docs/src/appendix/resolved/stage-join.md)). A driver added here
//! that forgets to call the log has the same hole, so calling it is part of writing one.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use shoal::client::QuerySuceededOpts;
use shoal::shared::responses::ResponseActionNames;
use shoal::Shoal;

use crate::workloads::schema::BenchClient;
use crate::workloads::workload::Measurement;

/// How many queries to buffer before sending them as one batch
///
/// The same 100 the workload this replaces used, which is what makes the two wall clocks
/// comparable in shape even though they are not comparable in value.
pub const BATCH: usize = 100;

/// How many queries a driver may have outstanding at once
///
/// A high water mark rather than a drain: sending resumes as soon as the count is one under this,
/// so the pipeline stays full. Gating near the batch size instead would empty it every cycle, and
/// the idle time would be measured as server latency.
///
/// This has to stay well above [`BATCH`] for the same reason, which is asserted below.
pub const IN_FLIGHT: usize = 4096;

/// A batch of queries to send, and what to count when its responses come back
pub struct Batch {
    /// The queries themselves, already built
    pub queries: shoal::shared::queries::Queries<BenchClient>,
}

/// Which of the client's two streaming modes a driver drains
///
/// The two differ in one thing that shows up in a distribution: an ordered stream buffers a
/// response until every earlier one has arrived, so one slow query holds back every sample behind
/// it. That is a property worth measuring rather than avoiding, which is why this is a parameter
/// and not a constant — `macro/transport/stream` and `macro/transport/stream_unordered` are the
/// same workload with this flipped.
///
/// Named `StreamMode` rather than `Ordering` because this module already imports
/// [`std::sync::atomic::Ordering`], and two things called `Ordering` in one file is how the wrong
/// one gets used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// Responses come back in the order their queries were sent
    Ordered,
    /// Responses come back as they arrive
    Unordered,
}

/// A result stream of either ordering, so one loop can drain both
///
/// The client returns a different type from `stream` and from `stream_unordered` and neither
/// implements a shared trait, so telling them apart here is what keeps the driver from existing
/// twice.
enum Results {
    /// Responses in the order their queries were sent
    Ordered(shoal::client::ShoalResultStream<BenchClient>),
    /// Responses in the order they arrived
    Unordered(shoal::ShoalUnorderedResultStream<BenchClient>),
}

impl Results {
    /// Waits for the next response off whichever stream this is
    async fn next(&mut self) -> Result<Option<shoal::ShoalResponse<BenchClient>>, shoal::Errors> {
        // the two arms have the same signature and different buffering, which is the whole
        // difference this enum exists to carry
        match self {
            Results::Ordered(stream) => stream.next().await,
            Results::Unordered(stream) => stream.next().await,
        }
    }
}

/// Runs a stream of batches at the server, timing each batch and counting each response
///
/// Drains an unordered stream at the default gate, which is what every seeding phase wants. A
/// workload that needs the ordered stream, or a gate small enough to hold a MiB row's responses in
/// memory, calls [`drive_with`] instead.
///
/// Returns once every batch the producer yielded has been answered.
///
/// # Arguments
///
/// * `client` - The client to send on
/// * `batches` - Produces the next batch to send, or `None` when there are no more
/// * `op` - The operation name to record samples under
/// * `warmup` - How many responses to discard before sampling starts
pub async fn drive<P>(
    client: &Shoal<BenchClient>,
    batches: P,
    op: &str,
    warmup: u64,
) -> Result<Measurement>
where
    P: FnMut() -> Option<Batch>,
{
    // the gate only works if it is above a batch, or a single batch fills it and the driver waits
    // for the whole batch to come back before sending the next one
    debug_assert!(
        IN_FLIGHT > BATCH * 4,
        "the in flight gate must be well above the batch size"
    );
    // the shape every workload but the transport pair wants
    drive_with(client, batches, op, warmup, StreamMode::Unordered, IN_FLIGHT).await
}

/// Runs a stream of batches at the server over a named stream mode and gate
///
/// # Arguments
///
/// * `client` - The client to send on
/// * `batches` - Produces the next batch to send, or `None` when there are no more
/// * `op` - The operation name to record samples under
/// * `warmup` - How many responses to discard before sampling starts
/// * `ordering` - Which of the client's two streaming modes to drain
/// * `in_flight` - How many queries may be outstanding at once
///
/// # Invariants
///
/// **The gate bounds outstanding responses, not just outstanding queries.** At the default gate a
/// workload reading MiB rows would have four gigabytes of responses in memory at once, so a
/// workload whose rows are large has to lower it. That is why it is a parameter rather than the
/// constant it used to be.
pub async fn drive_with<P>(
    client: &Shoal<BenchClient>,
    mut batches: P,
    op: &str,
    warmup: u64,
    ordering: StreamMode,
    in_flight_gate: usize,
) -> Result<Measurement>
where
    P: FnMut() -> Option<Batch>,
{
    // a gate of zero would send nothing and wait forever, which is a hang rather than an error
    assert!(in_flight_gate > 0, "the in flight gate cannot be zero");
    // open the stream this run was asked for
    //
    // unordered is the default everywhere else, because ordered streaming buffers a response until
    // every earlier one has arrived and a single slow query would hold back every sample behind it,
    // making the distribution describe the buffering rather than the server. the transport pair
    // asks for ordered precisely to measure that
    let (mut queries_tx, mut results_rx) = match ordering {
        StreamMode::Ordered => {
            let (tx, rx) = client.stream().context("failed to open a query stream")?;
            (tx, Results::Ordered(rx))
        }
        StreamMode::Unordered => {
            let (tx, rx) = client
                .stream_unordered()
                .context("failed to open a query stream")?;
            (tx, Results::Unordered(rx))
        }
    };
    let mut measured = Measurement::default();
    // when each outstanding batch was sent, keyed by the index of its first query
    let mut sent_at: std::collections::BTreeMap<usize, Instant> = std::collections::BTreeMap::new();
    // every query this driver sends shares one stream id, so it is the index beside it that makes
    // a record's key unique
    let stream_id = queries_tx.id;
    let mut in_flight = 0usize;
    let mut answered = 0u64;
    let mut drained = false;
    loop {
        // top the pipeline up unless it is already full or the producer has run dry
        while in_flight < in_flight_gate && !drained {
            match batches() {
                Some(batch) => {
                    let count = batch.queries.len();
                    // one timestamp for the whole batch, which is what makes this a batch sample
                    let base = queries_tx.base_index;
                    sent_at.insert(base, Instant::now());
                    // these stamps are a zero sized type unless this is a profiling build, so a
                    // caller that ignores them pays nothing for them
                    let stamps = queries_tx
                        .send(batch.queries)
                        .await
                        .context("failed to send a batch")?;
                    // open the client side record of every sampled query in this bundle
                    measured.stages.sent(stream_id, base, count, stamps);
                    in_flight += count;
                }
                // no more work, so stop topping up and drain what is outstanding
                None => drained = true,
            }
        }
        // everything sent and everything answered means the run is done
        if drained && in_flight == 0 {
            break;
        }
        // wait for the next response, whichever query it belongs to
        let Some(response) = results_rx
            .next()
            .await
            .context("failed to read a response")?
        else {
            // the stream ended, which after a drain is the normal way out
            break;
        };
        in_flight = in_flight.saturating_sub(1);
        // close out this query's client side record
        measured.stages.answered(&response);
        // a query that failed makes every number after it meaningless, so stop rather than
        // recording a fast response that did nothing
        response
            .suceeded(QuerySuceededOpts::default())
            .context("a query failed")?;
        // count what came back, by what it was
        match response.kind() {
            ResponseActionNames::Insert => measured.count("inserted", 1),
            ResponseActionNames::Get => {
                // a get answers with the rows it found, which is what to count
                measured.count("retrieved", rows_in(&response));
            }
            _ => {}
        }
        answered += 1;
        // charge this response to the batch it belonged to, once the warmup is behind us
        //
        // the batch's timestamp stays until the batch is fully answered, so every query in it is
        // charged from the same send
        if answered > warmup
            && let Some((_, at)) = sent_at.range(..=response.get_index()).next_back()
        {
            measured.record(op, at.elapsed());
        }
    }
    // close the stream so the server stops holding its channel open
    queries_tx.close().await.context("failed to close a stream")?;
    Ok(measured)
}

/// Runs queries one at a time per slot, timing each one on its own
///
/// This is the other half of [`drive`], and it measures a different thing. Here a fixed number of
/// slots each hold exactly one outstanding query, so the time from send to response is the time
/// that query took and nothing else - no batch ahead of it, no queue behind it. **This is a
/// service time**, which nothing in this repository has previously been able to produce: the
/// workload this harness replaces took one `Instant` per batch and installed it for every query in
/// that batch, so every committed percentile before now is a batch completion time.
///
/// It costs throughput to measure. At a concurrency of sixteen the server is nowhere near
/// saturated, so the wall clock of a per query workload is not a throughput figure and must not be
/// read as one. The artifact records which of the two a workload is, and a comparison never joins
/// one to the other.
///
/// # Arguments
///
/// * `client` - The client to send on
/// * `concurrency` - How many queries may be outstanding at once, one per slot
/// * `total` - How many queries to send in all
/// * `warmup` - How many to send before sampling starts
/// * `op` - The operation name to record samples under
/// * `build` - Builds the query with a given index
pub async fn drive_per_query<F, Q>(
    client: Arc<Shoal<BenchClient>>,
    concurrency: u32,
    total: u64,
    warmup: u64,
    op: &str,
    build: F,
) -> Result<Measurement>
where
    F: Fn(u64) -> Q + Send + Sync + 'static,
    Q: Into<crate::workloads::schema::BenchQueryKinds> + Send,
{
    // one client, which is what makes this a depth measurement rather than a client one
    drive_per_query_across(&[client], concurrency, total, warmup, op, build).await
}

/// Runs queries one at a time per slot, spread across several independent clients
///
/// The same measurement [`drive_per_query`] takes, with the slots dealt round robin across a set
/// of clients rather than all sharing one. That is the difference between *how deep* the load is
/// and *how many callers* are producing it, and the two are separate axes: one client at a depth
/// of eight has one connection pool, one set of TLS handshakes and one response map, where eight
/// clients at a depth of one have eight of each.
///
/// **The slot count is the concurrency, not the client count.** Slots are dealt to clients in
/// turn, so a concurrency below the client count leaves the later clients idle — which is a
/// legitimate thing to measure, since an idle client has still paid for its pool.
///
/// # Arguments
///
/// * `clients` - The clients to spread the slots across, in order
/// * `concurrency` - How many queries may be outstanding at once in total, one per slot
/// * `total` - How many queries to send in all
/// * `warmup` - How many to send before sampling starts
/// * `op` - The operation name to record samples under
/// * `build` - Builds the query with a given index
pub async fn drive_per_query_across<F, Q>(
    clients: &[Arc<Shoal<BenchClient>>],
    concurrency: u32,
    total: u64,
    warmup: u64,
    op: &str,
    build: F,
) -> Result<Measurement>
where
    F: Fn(u64) -> Q + Send + Sync + 'static,
    Q: Into<crate::workloads::schema::BenchQueryKinds> + Send,
{
    assert!(!clients.is_empty(), "a run needs at least one client");
    // one shared cursor, so the slots share the work rather than each taking a fixed slice. a
    // fixed slice would let one slow slot leave the others idle at the end of the run.
    let next = Arc::new(AtomicU64::new(0));
    let build = Arc::new(build);
    let mut slots = tokio::task::JoinSet::new();
    for slot in 0..concurrency.max(1) {
        // deal this slot to a client, walking them in turn so the load is spread evenly
        let client = clients[slot as usize % clients.len()].clone();
        let next = next.clone();
        let build = build.clone();
        let op = op.to_string();
        slots.spawn(async move {
            let mut measured = Measurement::default();
            loop {
                // claim the next query, and stop when they have all been claimed
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    break;
                }
                // one timestamp either side of one query, which is what makes this a service time
                let started = Instant::now();
                // the stamps are a zero sized type unless this is a profiling build, so the
                // stamped call costs a caller that is not profiling nothing over the plain one
                let (response, stamps) = client
                    .send_one_stamped(build(index))
                    .await
                    .context("a query failed")?;
                let elapsed = started.elapsed();
                // this query's client side record, opened and closed in one call because a bundle
                // of one is answered by exactly one response
                measured.stages.one(stamps, &response);
                // count the rows that came back, whatever kind of row they are
                let rows = rows_in(&response);
                measured.count("retrieved", rows);
                // the warmup covers connection establishment and the first cold partitions, and
                // is counted on the claimed index so every slot agrees on where it ends
                if index >= warmup {
                    measured.record(&op, elapsed);
                }
            }
            Ok::<Measurement, anyhow::Error>(measured)
        });
    }
    // pool what every slot gathered
    let mut measured = Measurement::default();
    while let Some(slot) = slots.join_next().await {
        measured.absorb(slot.context("a query slot panicked")??);
    }
    Ok(measured)
}

/// Runs a mixture of reads and writes, keeping each kind's latencies apart
///
/// The same bounded-concurrency measurement [`drive_per_query_across`] takes, with two differences,
/// both of which exist because the queries are not all the same kind:
///
/// - **The build closure names the operation it built.** A single `op` name would pool a read's
///   service time with a write's, and the pooled p99 of a 70/30 mixture is a number describing
///   neither - which is exactly the blending
///   [F8](../../../../docs/src/features/purpose-built-workloads.md) removed. The two land under
///   `read` and `write` and are never added together.
/// - **Queries are counted as well as rows.** `retrieved` and `inserted` count *rows*, and a read
///   answering with one row and a write acknowledging one are not comparable to a fan-out query
///   answering with two hundred and fifty six. `reads` and `writes` count queries, which is what
///   [`ops_per_sec`](crate::model::macro_layer::WorkloadCapture::ops_per_sec) sums.
///
/// # Arguments
///
/// * `clients` - The clients to spread the slots across, in order
/// * `concurrency` - How many queries may be outstanding at once in total, one per slot
/// * `total` - How many queries to send in all
/// * `warmup` - How many to send before sampling starts
/// * `build` - Builds the query at a given index, and names the operation it is
///
/// # Invariants
///
/// **The operation name the closure returns must be one the caller can find again.** It is the key
/// in the artifact's `ops` map and therefore the key a comparison joins on, so it is as much a
/// stable identifier as the workload's own name is.
pub async fn drive_mixed_per_query<F, Q>(
    clients: &[Arc<Shoal<BenchClient>>],
    concurrency: u32,
    total: u64,
    warmup: u64,
    build: F,
) -> Result<Measurement>
where
    F: Fn(u64) -> (&'static str, Q) + Send + Sync + 'static,
    Q: Into<crate::workloads::schema::BenchQueryKinds> + Send,
{
    assert!(!clients.is_empty(), "a run needs at least one client");
    // one shared cursor, so a slow slot does not leave the others idle at the end of the run
    let next = Arc::new(AtomicU64::new(0));
    let build = Arc::new(build);
    let mut slots = tokio::task::JoinSet::new();
    for slot in 0..concurrency.max(1) {
        // deal this slot to a client, walking them in turn so the load is spread evenly
        let client = clients[slot as usize % clients.len()].clone();
        let next = next.clone();
        let build = build.clone();
        slots.spawn(async move {
            let mut measured = Measurement::default();
            loop {
                // claim the next query, and stop when they have all been claimed
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    break;
                }
                // the index decides both which query this is and which kind it is, so the mixture
                // is a function of the index rather than of which slot got there first. two runs
                // of the same arm therefore send the same queries in the same proportions
                let (op, query) = build(index);
                // one timestamp either side of one query, which is what makes this a service time
                let started = Instant::now();
                // the stamps are a zero sized type unless this is a profiling build, so the
                // stamped call costs a caller that is not profiling nothing over the plain one
                let (response, stamps) = client
                    .send_one_stamped(query)
                    .await
                    .context("a query failed")?;
                let elapsed = started.elapsed();
                // this query's client side record, opened and closed in one call because a bundle
                // of one is answered by exactly one response
                measured.stages.one(stamps, &response);
                // count the queries by what they were, and the rows by what came back
                match response.kind() {
                    ResponseActionNames::Insert => {
                        measured.count("writes", 1);
                        measured.count("inserted", 1);
                    }
                    ResponseActionNames::Get => {
                        measured.count("reads", 1);
                        measured.count("retrieved", rows_in(&response));
                    }
                    _ => {}
                }
                // the warmup is counted on the claimed index so every slot agrees where it ends
                if index >= warmup {
                    measured.record(op, elapsed);
                }
            }
            Ok::<Measurement, anyhow::Error>(measured)
        });
    }
    // pool what every slot gathered, which pools each operation under its own name
    let mut measured = Measurement::default();
    while let Some(slot) = slots.join_next().await {
        measured.absorb(slot.context("a query slot panicked")??);
    }
    Ok(measured)
}

/// How many rows a response carried
///
/// # Arguments
///
/// * `response` - The response to count
fn rows_in(response: &shoal::ShoalResponse<BenchClient>) -> u64 {
    // a response holds one table's rows, and a workload reads one table, so whichever of these
    // accesses succeeds is the one this response is
    //
    // every row type the schema declares has to be listed here. A row type that is missing does
    // not fail - it counts zero, so the workload reports having retrieved nothing while its
    // latencies look perfectly healthy, which reads like a workload that queried an empty table.
    if let Ok(Some(rows)) = response.access::<crate::workloads::schema::Event>() {
        return rows.len() as u64;
    }
    if let Ok(Some(rows)) = response.access::<crate::workloads::schema::Item>() {
        return rows.len() as u64;
    }
    if let Ok(Some(rows)) = response.access::<crate::workloads::schema::MemEvent>() {
        return rows.len() as u64;
    }
    if let Ok(Some(rows)) = response.access::<crate::workloads::schema::MemItem>() {
        return rows.len() as u64;
    }
    0
}

/// How long a closure took to run
///
/// # Arguments
///
/// * `work` - The work to time
pub async fn timed<F, T>(work: F) -> (T, Duration)
where
    F: std::future::Future<Output = T>,
{
    // one timestamp either side, which is all a wall clock is
    let started = Instant::now();
    let out = work.await;
    (out, started.elapsed())
}

#[cfg(test)]
mod tests {
    use super::{BATCH, IN_FLIGHT};

    /// The in flight gate stays well above a batch
    ///
    /// If it fell to a batch or below, the driver would send one batch, block until all of it came
    /// back, and then send the next. The pipeline would be empty for the whole round trip of every
    /// batch, and that idle time would be recorded as server latency.
    #[test]
    fn the_gate_stays_above_the_batch() {
        assert!(
            IN_FLIGHT > BATCH * 4,
            "in flight {IN_FLIGHT} is not well above batch {BATCH}"
        );
    }
}
