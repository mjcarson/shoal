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

/// Runs a stream of batches at the server, timing each batch and counting each response
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
    mut batches: P,
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
    // open a stream that hands responses back as they arrive rather than in index order
    //
    // ordered streaming buffers a response until every earlier one has arrived, so a single slow
    // query would hold back every sample behind it and the distribution would describe the
    // buffering rather than the server
    let (mut queries_tx, mut results_rx) = client
        .stream_unordered()
        .context("failed to open a query stream")?;
    let mut measured = Measurement::default();
    // when each outstanding batch was sent, keyed by the index of its first query
    let mut sent_at: std::collections::BTreeMap<usize, Instant> = std::collections::BTreeMap::new();
    // the client half of each sampled query, waiting for its response to close it out
    #[cfg(feature = "stage-profile")]
    let mut submitted: std::collections::HashMap<usize, crate::workloads::stages::ClientRecord> =
        std::collections::HashMap::new();
    // every query this driver sends shares one stream id, so it is the index beside it that makes
    // a record's key unique
    #[cfg(feature = "stage-profile")]
    let stream_id = queries_tx.id;
    #[cfg(feature = "stage-profile")]
    let stage_sample = crate::workloads::stages::sample_rate();
    let mut in_flight = 0usize;
    let mut answered = 0u64;
    let mut drained = false;
    loop {
        // top the pipeline up unless it is already full or the producer has run dry
        while in_flight < IN_FLIGHT && !drained {
            match batches() {
                Some(batch) => {
                    let count = batch.queries.len();
                    // one timestamp for the whole batch, which is what makes this a batch sample
                    let base = queries_tx.base_index;
                    sent_at.insert(base, Instant::now());
                    // these stamps are a zero sized type unless this is a profiling build, so a
                    // caller that ignores them pays nothing for them
                    #[cfg_attr(not(feature = "stage-profile"), allow(unused_variables))]
                    let stamps = queries_tx
                        .send(batch.queries)
                        .await
                        .context("failed to send a batch")?;
                    // record the client side of every sampled query in this bundle
                    //
                    // every stage in `stamps` is paid once for the whole bundle and shared by
                    // every query in it, which is why the report labels them as batch level
                    #[cfg(feature = "stage-profile")]
                    for offset in 0..count {
                        let index = base + offset;
                        // sample on the index, so the server keeps the same queries and the two
                        // halves still have something to join on
                        if index % stage_sample == 0 {
                            submitted.insert(
                                index,
                                crate::workloads::stages::ClientRecord {
                                    id: stream_id,
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
        //
        // the arrival stamp comes off the response itself rather than being read here, so a
        // response that waited in a channel is charged for that wait rather than having it hidden
        // in the gap between the socket and this loop picking it up
        #[cfg(feature = "stage-profile")]
        if let Some(mut record) = submitted.remove(&response.get_index()) {
            record.arrived = response.stamps().arrived();
            measured.stage_records.push(record);
        }
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
                let rows = response
                    .access::<crate::workloads::schema::Item>()
                    .ok()
                    .flatten()
                    .map_or(0, |rows| rows.len() as u64);
                measured.count("retrieved", rows);
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
    // one shared cursor, so the slots share the work rather than each taking a fixed slice. a
    // fixed slice would let one slow slot leave the others idle at the end of the run.
    let next = Arc::new(AtomicU64::new(0));
    let build = Arc::new(build);
    let mut slots = tokio::task::JoinSet::new();
    for _ in 0..concurrency.max(1) {
        let client = client.clone();
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
                let response = client
                    .send_one(build(index))
                    .await
                    .context("a query failed")?;
                let elapsed = started.elapsed();
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

/// How many rows a response carried
///
/// # Arguments
///
/// * `response` - The response to count
fn rows_in(response: &shoal::ShoalResponse<BenchClient>) -> u64 {
    // a response holds one table's rows, and a workload reads one table, so whichever of the two
    // accesses succeeds is the one this response is
    if let Ok(Some(rows)) = response.access::<crate::workloads::schema::Event>() {
        return rows.len() as u64;
    }
    if let Ok(Some(rows)) = response.access::<crate::workloads::schema::Item>() {
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
