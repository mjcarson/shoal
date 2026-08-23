//! Running one workload: start a server, wait for it, drive it, write what it measured
//!
//! # One process, one workload, one run
//!
//! The runner spawns this binary once per workload per run rather than looping inside it. Four
//! reasons, in order of how much they weigh:
//!
//! - `hotpath` emits one profile when the process exits, so attributing a profile to a workload
//!   needs a process per workload. A loop would produce one blended profile and call it four.
//! - Glommio pins its shards to cores at start and there is no supported way to tear that down and
//!   redo it in one process, so a second `ShoalPool::start` is a risk with no upside.
//! - `StorageMeta::claim` keys a directory to the shard count that wrote it, and workloads
//!   deliberately run with different shard counts.
//! - A workload that panics or hangs costs its own run instead of the rest of the capture.

pub mod conf;
pub mod driver;
pub mod keys;
pub mod ready;
pub mod rows;
pub mod seed;
pub mod timer;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context as _, Result};
use shoal::{Shoal, ShoalPool};

use crate::model::macro_layer::{MacroCaptureV2, WorkloadCapture};
use crate::workloads::schema::{Bench, BenchClient, ItemExists};
use crate::workloads::workload::{Context, Workload};

/// What one run of one workload was asked for
#[derive(Debug, Clone)]
pub struct RunRequest {
    /// The base configuration to start from
    pub conf: std::path::PathBuf,
    /// The seed every row derives from
    pub seed: u64,
    /// How large a run to take
    pub scale: seed::Scale,
    /// The port to bind, when a workload needs a server
    pub port: u16,
    /// The name to record this run under
    pub label: Option<String>,
    /// Where to write a per query stage breakdown, when one was asked for
    pub stage_json: Option<std::path::PathBuf>,
    /// Keep a stage record for one in every this many queries
    pub stage_sample: usize,
}

/// Runs one workload and returns the capture describing it
///
/// The server is started and stopped around the workload rather than by it, so a workload cannot
/// forget to stop one and cannot measure its own startup.
///
/// # Arguments
///
/// * `workload` - The workload to run
/// * `request` - What this run was asked for
pub fn run(workload: &dyn Workload, request: &RunRequest) -> Result<MacroCaptureV2> {
    // refuse an impossible request before starting a server for it
    //
    // this has to be first. the report itself can only be built once the shards have shut down and
    // handed their records over, so the write happens at the very end - and a run that did all its
    // work and only then discovered it could not produce the report it was asked for has wasted
    // the whole run.
    check_stage_support(request)?;
    // work out what this workload needs before anything is started
    let plan = workload.plan(request.scale);
    // tell the shards how many queries to keep a stage record for
    //
    // this has to be set before the pool starts, and it has to be the rule the client half uses
    // too, or the two halves of a sampled run keep different queries and nothing joins
    #[cfg(feature = "stage-profile")]
    if request.stage_sample > 1 {
        // SAFETY: nothing has been spawned yet - the shards start below and the client runtime
        // after them - so no other thread can be reading the environment concurrently with this
        unsafe {
            std::env::set_var(
                shoal::server::stage_profile::SAMPLE_ENV,
                request.stage_sample.to_string(),
            );
        }
    }
    // resolve the configuration once, which also gives this workload its own storage directory.
    // a restart reuses it exactly, so the server that comes back up claims the same store.
    let (conf_facts, addr, conf) = match plan.server.overrides() {
        None => (None, String::new(), None),
        Some(overrides) => {
            let conf = conf::resolve(&request.conf, workload.id(), overrides, request.port)?;
            let addr = format!("{}:{}", conf.networking.interface, conf.networking.port);
            (Some(conf::facts(&conf)), addr, Some(conf))
        }
    };
    // build the client runtime. the shards own their own cores, so this stays on the rest
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("workload-client")
        .build()
        .context("failed to build the client runtime")?;
    // start the shards. this returns before they have bound, which is what the readiness probe is
    // for
    let mut pool = start(conf.clone(), &runtime, &addr)?;
    // put whatever this workload reads into the server. deliberately outside the timing below:
    // a read workload's numbers must describe reading, not the writing that had to happen first.
    let ctx = Context {
        addr: addr.clone(),
        seed: request.seed,
        scale: plan.scale.clone(),
        warmup: plan.warmup,
        conf: conf_facts.clone(),
        // what a client needs to reach this server, which is nothing at all unless the workload
        // asked for an encrypted one
        tls: conf.as_ref().and_then(conf::client_tls),
    };
    let seeded = runtime.block_on(workload.seed(&ctx));
    // cycle the server when the workload needs its data on disk rather than in memory. a shutdown
    // flushes and compacts, and the server that comes back holds nothing, so every read that
    // follows has to find its partition in an archive.
    if seeded.is_ok() && plan.server.restarts() {
        stop(pool.take())?;
        pool = start(conf.clone(), &runtime, &addr)?;
    }
    // throw away everything the seed phase stamped, so the report describes the measured phase
    //
    // seeding is untimed setup and its client half is already discarded - a workload's `seed`
    // returns no measurement and has nowhere to put one. Without this the server's half of it
    // survives into the report as tens of thousands of server only records, which reads like a
    // join that failed rather than like a phase nobody asked about. It has to come after the
    // restart above, or it throws away the phase it was meant to keep.
    #[cfg(feature = "stage-profile")]
    shoal::server::stage_profile::reset();
    // drive the workload, keeping the result rather than unwrapping it, so the server is stopped
    // on the failing path as well as the succeeding one
    let outcome = seeded.and_then(|()| {
        runtime.block_on(async {
            let (measured, wall_clock) = driver::timed(workload.run(&ctx)).await;
            measured.map(|measured| (measured, wall_clock))
        })
    });
    // stop the server whatever happened, so a failing run does not leave shards holding cores
    stop(pool)?;
    let (mut measured, wall_clock) = outcome?;
    // build the stage report now that every shard has handed its records over
    //
    // this has to come after the server has stopped. shards flush their buffered records in
    // batches as the run proceeds and hand the tail over on shutdown, so building the report any
    // earlier silently omits that tail.
    write_stage_report(workload, request, &measured)?;
    // a workload that recorded nothing has not measured anything, and an artifact saying so
    // reads exactly like one from a workload that was fast
    if measured.ops.values().all(|samples| samples.is_empty()) {
        bail!(
            "{} recorded no samples, so this run measured nothing",
            workload.id()
        );
    }
    // a workload that timed gets but retrieved no rows read an empty table, and its latencies
    // describe how quickly the server can find nothing
    //
    // this catches a real and quiet failure: the driver counts rows by trying each row type the
    // schema declares, so a schema gaining a row type the driver was not taught about produces a
    // run with healthy looking percentiles over an answer that was never there
    //
    // both spellings of the read operation are checked: an isolating workload records `get` and a
    // mixture records `read`, and a guard that knew only the first would let every read arm of the
    // grid report an empty table as a fast one
    let timed_reads = measured.ops.contains_key("get") || measured.ops.contains_key("read");
    if timed_reads && measured.counters.get("retrieved").copied() == Some(0) {
        bail!(
            "{} timed gets but retrieved no rows, so its samples measure lookups that found nothing",
            workload.id()
        );
    }
    // summarize every operation's samples into the distribution the artifact stores
    let mut ops = BTreeMap::new();
    for (op, samples) in &mut measured.ops {
        ops.insert(op.clone(), samples.summarize());
    }
    // one run, so there is no interval and no median to pick yet - the runner folds those
    let capture = WorkloadCapture {
        timing: workload.timing(),
        seed: request.seed,
        scale: plan.scale,
        conf: conf_facts,
        counters: measured.counters,
        ops,
        runs: None,
        wall_clock_ns: Some(vec![wall_clock.as_nanos() as u64]),
        spread_pct: None,
        runs_detail: None,
    };
    let mut out = MacroCaptureV2::new(request.label.clone());
    out.workloads.insert(workload.id().to_string(), capture);
    Ok(out)
}

/// Starts a server and waits until it can actually answer a query
///
/// Returns `None` when the workload asked for no server at all.
///
/// # Arguments
///
/// * `conf` - The resolved configuration, or `None` if this workload needs no server
/// * `runtime` - The client runtime to run the readiness probe on
/// * `addr` - The address the server should come up on
fn start(
    conf: Option<shoal::Conf>,
    runtime: &tokio::runtime::Runtime,
    addr: &str,
) -> Result<Option<ShoalPool<Bench>>> {
    // a workload driving engine internals in process needs nothing started
    let Some(conf) = conf else {
        return Ok(None);
    };
    // the probe below has to reach this server the same way the workload will, so an encrypted
    // arm needs the certificate before the server is even started. without this the probe
    // connects in plaintext, is refused by its own server, and the arm times out looking exactly
    // like a server that never came up
    let tls = conf::client_tls(&conf);
    let pool = ShoalPool::<Bench>::start(conf)
        .map_err(|error| anyhow::anyhow!("failed to start a server: {error:?}"))?;
    // wait until it answers, rather than for a fixed number of seconds
    runtime.block_on(ready::wait_until_answering(addr, |addr| {
        // cloned per attempt, since the probe is an `Fn` and may be called several times
        let tls = tls.clone();
        async move {
        // a real query against a key no workload generates, so it is answered out of an empty
        // table and costs nothing measurable
        //
        // an exists rather than a get, because the probe is asking whether the server can answer
        // and not whether the row is there. `send_one` treats a get that found nothing as a
        // failed query, so an empty table would look like an unready server and the probe would
        // time out against a server that was working perfectly.
            let options = match tls {
                Some(tls) => shoal::client::ClientOptions::new().tls(tls),
                None => shoal::client::ClientOptions::new(),
            };
            let client = Shoal::<BenchClient>::with_options(&addr, options).await?;
            client.exists(ItemExists::new(u64::MAX)).await?;
            Ok(())
        }
    }))?;
    Ok(Some(pool))
}

/// Stops a server, if one was started
///
/// # Arguments
///
/// * `pool` - The server to stop
fn stop(pool: Option<ShoalPool<Bench>>) -> Result<()> {
    // nothing to stop when the workload never wanted one
    let Some(pool) = pool else {
        return Ok(());
    };
    // this is what flushes and compacts, so a workload that reads from disk depends on it
    pool.exit()
        .map_err(|error| anyhow::anyhow!("failed to stop the server: {error:?}"))?;
    // the shards release their port as they wind down, and the next server binds the same one
    std::thread::sleep(std::time::Duration::from_millis(250));
    Ok(())
}

/// Joins the two halves of this run's stage records and writes the report
///
/// # Arguments
///
/// * `workload` - The workload these records came from, which the report names
/// * `request` - What this run was asked for
/// * `measured` - What the workload produced, carrying the client half of the records
#[cfg(feature = "stage-profile")]
fn write_stage_report(
    workload: &dyn Workload,
    request: &RunRequest,
    measured: &crate::workloads::workload::Measurement,
) -> Result<()> {
    // nothing to do unless this run was asked for a report
    let Some(path) = request.stage_json.as_deref() else {
        return Ok(());
    };
    // take every record the shards handed over
    let server_records = shoal::server::stage_profile::drain_stage_records();
    // measure what a clock read costs here, so stages of that order are marked as being at the
    // floor rather than printed as though they were measurements
    let overhead = shoal::server::stage_profile::Stamp::measure_overhead(4096);
    // join the two halves and summarize them
    let report = crate::workloads::stages::build_report(
        &server_records,
        measured.stages.records(),
        request.label.clone(),
        Some(workload.id().to_string()),
        overhead,
    );
    // report what the join actually managed, since a report that matched half a run is not a
    // report about that run
    //
    // the unanswered count is printed beside them because it explains the server only ones: a
    // query the driver sent and never saw a response for leaves the server's half of it with
    // nothing to join to, which otherwise reads like a join that failed
    println!(
        "stage profile: {} joined, {} server only, {} client only, {} duplicates, {} saturated, \
         {} unanswered",
        report.join.joined,
        report.join.server_only,
        report.join.client_only,
        report.join.duplicates,
        report.join.saturated,
        measured.stages.unanswered()
    );
    crate::workloads::stages::write_report(&report, path)
        .with_context(|| format!("failed to write {}", path.display()))?;
    println!("stage profile written to {}", path.display());
    Ok(())
}

/// Does nothing, since this build records no stage records to write
///
/// # Arguments
///
/// * `workload` - Unused
/// * `request` - Unused
/// * `measured` - Unused
#[cfg(not(feature = "stage-profile"))]
fn write_stage_report(
    workload: &dyn Workload,
    request: &RunRequest,
    measured: &crate::workloads::workload::Measurement,
) -> Result<()> {
    // the request was already refused by `check_stage_support` before anything started
    let _ = (workload, request, measured);
    Ok(())
}

/// Refuses a stage report from a build that records none, before any work is done
///
/// A report that is silently missing its entries reads as "this code was never called", which is
/// the same failure the `hotpath` `limit = 0` note records. Asking a build without the feature for
/// a report is a mistake worth failing on rather than answering with an empty file - and worth
/// failing on immediately rather than after a run that cannot produce what it was asked for.
///
/// # Arguments
///
/// * `request` - What this run was asked for
#[cfg(not(feature = "stage-profile"))]
fn check_stage_support(request: &RunRequest) -> Result<()> {
    // only a build with the feature can answer this
    if request.stage_json.is_some() {
        bail!(
            "--stage-json needs a binary built with --features stage-profile, this one records \
             no stages"
        );
    }
    Ok(())
}

/// Accepts a stage report request, since this build records the stages
///
/// # Arguments
///
/// * `request` - Unused, since this build can answer any of them
#[cfg(feature = "stage-profile")]
fn check_stage_support(request: &RunRequest) -> Result<()> {
    let _ = request;
    Ok(())
}

/// Writes a capture where the runner will collect it from
///
/// # Arguments
///
/// * `capture` - The capture to write
/// * `path` - Where to write it
pub fn write(capture: &MacroCaptureV2, path: &Path) -> Result<()> {
    // make sure the directory this is going into exists, since scratch paths are created by us
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    // pretty, because a run artifact that nobody can read without a program is one nobody checks
    let body = serde_json::to_vec_pretty(capture).context("failed to serialize the capture")?;
    std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}
