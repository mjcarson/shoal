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

pub mod background;
pub mod cluster;
pub mod conf;
pub mod driver;
pub mod catchup;
pub mod fault;
pub mod keys;
pub mod metrics;
pub mod ready;
pub mod rows;
pub mod seed;
pub mod timer;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context as _, Result};
use shoal::{Shoal, ShoalPool};

use crate::model::macro_layer::{ClusterFacts, MacroCaptureV2, WorkloadCapture};
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
    /// Where the server comes from
    pub server: ServerSource,
    /// The cluster the server belongs to, when the caller started one; recorded verbatim
    pub cluster: Option<ClusterFacts>,
}

/// Where a workload's server comes from
///
/// In process is what every capture has done since [F8](../../../docs/src/features/purpose-built-workloads.md).
/// External is the separate load driver [C10](../../../docs/src/distributed/performance.md) asks
/// for: somebody else - a cluster fixture, a person - started the server, and this process only
/// drives it. The configuration file is still resolved, because the client needs its TLS
/// settings and the capture records its facts, but nothing checks that the server was started
/// from it: the facts are the caller's claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerSource {
    /// Start a `ShoalPool` in this process and stop it afterwards
    InProcess,
    /// Drive a server somebody else started, at this address
    External(String),
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
    //
    // a workload that places peers gets its cluster staged here too: identities minted, markers
    // written, cores and ports decided, and node zero's own configuration applied on top of the
    // resolved one ([F38](../../../docs/src/features/inter-node-transport.md)). nothing is started
    // yet - the peers come up below, before node zero does
    let (mut conf_facts, addr, conf, staged) = match plan.server.overrides() {
        None => (None, String::new(), None, None),
        Some(overrides) => {
            // an arm asking for more copies than it places nodes is an availability test the
            // fixture runs, not a throughput arm at a settled factor
            // ([C10](../../../docs/src/distributed/performance.md))
            if let Some(cluster) = &overrides.cluster {
                if let Err(reason) = cluster.feasibility() {
                    bail!("{}: {reason}", workload.id());
                }
            }
            let resolved = conf::resolve(&request.conf, workload.id(), overrides, request.port)?;
            let (conf, staged) = match overrides.cluster.as_ref().filter(|c| !c.peers.is_empty()) {
                Some(_) if request.server != ServerSource::InProcess => bail!(
                    "{} places peers and has to start its own cluster; --server cannot drive it",
                    workload.id()
                ),
                Some(_) => {
                    let staged = cluster::stage(&resolved, workload.id(), overrides, request.port)?;
                    (cluster::apply(resolved, &staged.nodes[0])?, Some(staged))
                }
                None => (resolved, None),
            };
            let addr = format!("{}:{}", conf.networking.interface, conf.networking.port);
            (Some(conf::facts(&conf)), addr, Some(conf), staged)
        }
    };
    // an external server has to be one the workload could have started for itself, and one it
    // never needs to restart: a server it does not own is one it cannot cycle
    let addr = match &request.server {
        ServerSource::InProcess => addr,
        ServerSource::External(external) => {
            if conf.is_none() {
                bail!(
                    "{} drives engine internals in process and cannot run against an external server",
                    workload.id()
                );
            }
            if plan.server.restarts() {
                bail!(
                    "{} needs a fresh server between seeding and measuring, which an external server cannot give",
                    workload.id()
                );
            }
            external.clone()
        }
    };
    // build the client runtime. the shards own their own cores, so this stays on the rest
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("workload-client")
        .build()
        .context("failed to build the client runtime")?;
    // start every placed peer first, so node zero's first forward has somewhere to dial. the
    // children are killed when this drops, which is after the server below has stopped; it is
    // shared because a fault arm's thread kills one and starts it again mid-run
    // ([F42](../../../docs/src/features/primary-failover.md))
    let peers = std::sync::Arc::new(std::sync::Mutex::new(match &staged {
        Some(staged) => cluster::spawn_peers(
            staged,
            workload.id(),
            &request.conf,
            request.scale.as_str(),
        )?,
        None => Vec::new(),
    }));
    // a fault is done to a placed peer, so an arm asking for one without peers is refused
    // before its server starts
    let fault = workload.fault(request.scale);
    if fault.is_some() && staged.is_none() {
        bail!("{} asks for a fault and places no peers to inject it into", workload.id());
    }
    // a background repair is asked of a placed cluster's control plane, so the same refusal
    // ([F44](../../docs/src/features/repair.md))
    let background_spec = workload.background(request.scale);
    if background_spec.is_some() && staged.is_none() {
        bail!("{} asks for a background repair and places no peers to run it over", workload.id());
    }
    // start the shards and wait until they answer - or, for a server somebody else started,
    // only wait until it answers
    let mut pool = match &request.server {
        ServerSource::InProcess => start(conf.clone(), &runtime, &addr)?,
        ServerSource::External(_) => {
            probe(&runtime, &addr, conf.as_ref().and_then(conf::client_tls))?;
            None
        }
    };
    // a placed cluster is initialized once every peer has joined, which is what an operator
    // does and what the arm's keys assume; a cluster of one is placed on itself for the same
    // reason ([F39](../../../docs/src/features/membership.md))
    match (&staged, pool.as_ref()) {
        (Some(staged), Some(pool)) => {
            cluster::initialize(staged, pool)?;
            // and every peer holding it too, or the seed's first forwards find no group
            cluster::wait_peers_placed(staged, &runtime)?;
        }
        (None, Some(pool)) if conf.as_ref().is_some_and(|conf| conf.cluster.is_some()) => {
            cluster::initialize_alone(pool)?;
        }
        _ => {}
    }
    // a cluster node this process started records what its control plane committed; a server
    // somebody else started carries whatever record they handed over, and a standalone one none.
    // the link counters are read again after the run, since they are what the run did
    let mut cluster_facts = match (&pool, &conf) {
        (Some(pool), Some(conf)) => match conf::cluster_facts(conf, pool)? {
            Some(mut facts) => {
                // what the arm scheduled: every arm here is a closed loop at its plan's depth
                // ([F40](../../../docs/src/features/replication.md))
                facts.offered_load = Some(crate::model::macro_layer::OfferedLoad {
                    mode: "closed".to_string(),
                    outstanding: plan.scale.concurrency,
                    rate: None,
                });
                match &staged {
                    Some(staged) => Some(cluster::placed_facts(staged, pool, conf, facts)?),
                    None => Some(facts),
                }
            }
            None => None,
        },
        _ => None,
    };
    // put whatever this workload reads into the server. deliberately outside the timing below:
    // a read workload's numbers must describe reading, not the writing that had to happen first.
    let mut ctx = Context {
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
    //
    // a rehome arm comes back at another executor count, so the start between moves the vanished
    // executors' files first, and what that moved is the arm's record
    // ([F47](../../docs/src/features/local-rehome.md))
    if seeded.is_ok() && plan.server.restarts() {
        stop(pool.take())?;
        let restart_shards = plan.server.overrides().and_then(|overrides| overrides.restart_shards);
        let restart_conf = match (conf.clone(), restart_shards) {
            (Some(mut conf), Some(shards)) => {
                conf.resources.cores = Some(shards);
                Some(conf)
            }
            (conf, _) => conf,
        };
        pool = start(restart_conf.clone(), &runtime, &addr)?;
        // the server measured is the one that came back, so the facts describe it
        if let Some(restart_conf) = &restart_conf {
            conf_facts = Some(conf::facts(restart_conf));
            ctx.conf = conf_facts.clone();
        }
        if let (Some(pool), Some(facts)) = (pool.as_ref(), cluster_facts.as_mut()) {
            facts.rehome = pool.rehome().map(rehome_facts);
        }
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
    // a fault arm's schedule starts with the measured phase: the thread counts from here, and
    // is joined after the run whatever the run did, so a child is never left half restarted
    let run_started = std::time::Instant::now();
    let injected = match (&fault, &staged, seeded.is_ok()) {
        (Some(spec), Some(staged), true) => Some(fault::inject(
            spec,
            peers.clone(),
            staged,
            workload.id(),
            &request.conf,
            request.scale.as_str(),
            run_started,
            // a catch-up arm has the returning node sampled until the run ends
            // ([F43](../../docs/src/features/node-recovery.md))
            workload.catchup().then_some(run_started + spec.run_for),
        )?),
        _ => None,
    };
    // a background arm's repair is asked for on its schedule and polled until the run ends;
    // the integrity counters before the run are what its cost is read against. Only a repair
    // reads them: a plan's arm may have killed a node for good, which has no report to give
    let reads_integrity = matches!(background_spec.as_ref().map(|spec| &spec.kind), Some(crate::workloads::workload::BackgroundKind::Repair));
    // a node the arm kills and never starts again is not asked for a report afterwards
    let dead = fault.as_ref().filter(|spec| !spec.restart).map(|spec| spec.node);
    let integrity_before = match (reads_integrity, pool.as_ref(), conf.as_ref(), staged.as_ref()) {
        (true, Some(pool), Some(conf), Some(staged)) => Some(cluster::integrity_sum(&cluster::node_reports(staged, pool, conf, &runtime, None)?)),
        _ => None,
    };
    let background_injected = match (&background_spec, pool.as_ref(), seeded.is_ok()) {
        (Some(spec), Some(pool), true) => {
            let admin = pool
                .admin_sender()
                .with_context(|| format!("{} asks for a background operation on a node with no control plane", workload.id()))?;
            let nodes = staged
                .as_ref()
                .map(|staged| {
                    staged
                        .nodes
                        .iter()
                        .map(|node| node.node.parse().map(shoal::shared::identity::NodeId))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()
                .context("a staged node's identity does not parse")?
                .unwrap_or_default();
            // a backup's files go under the workload's own storage root, which the next run wipes
            let backup_dir = conf
                .as_ref()
                .map(|conf| conf.storage.default.filesystem.latency_sensitive.path.join("backup"))
                .with_context(|| format!("{} asks for a background operation with no configuration", workload.id()))?;
            Some(background::inject(spec, admin, run_started, nodes, backup_dir)?)
        }
        _ => None,
    };
    // drive the workload, keeping the result rather than unwrapping it, so the server is stopped
    // on the failing path as well as the succeeding one
    let outcome = seeded.and_then(|()| {
        runtime.block_on(async {
            let (measured, wall_clock) = driver::timed(workload.run(&ctx)).await;
            measured.map(|measured| (measured, wall_clock))
        })
    });
    // the fault's marks, waited for before the servers are read so the restarted peer is in
    // the reports
    let marks = injected.map(fault::Injected::finish).transpose();
    // and the background repair's, which stops its polling
    let background_marks = background_injected.map(background::Injected::finish).transpose();
    let integrity_after = match (reads_integrity, pool.as_ref(), conf.as_ref(), staged.as_ref()) {
        (true, Some(pool), Some(conf), Some(staged)) => Some(cluster::integrity_sum(&cluster::node_reports(staged, pool, conf, &runtime, dead)?)),
        _ => None,
    };
    // what the links did during the run, and where every replica ended, read before the
    // servers that hold them stop
    if let (Some(facts), Some(pool), Some(conf), Some(staged)) =
        (cluster_facts.as_mut(), pool.as_ref(), conf.as_ref(), staged.as_ref())
    {
        facts.transport = Some(cluster::transport_facts(pool, conf)?);
        let reports = cluster::node_reports(staged, pool, conf, &runtime, dead)?;
        let replicas = cluster::replica_facts(&reports);
        facts.outcomes = Some(cluster::outcome_facts(&replicas));
        facts.replicas = replicas;
        // a read arm records what its reads waited on ([F41](../../docs/src/features/read-consistency.md))
        facts.reads = staged.read.as_ref().map(|arm| cluster::read_facts(arm, &reports));
    }
    // stop the server whatever happened, so a failing run does not leave shards holding cores
    stop(pool)?;
    let (mut measured, wall_clock) = outcome?;
    // a fault arm cuts what its client saw at the marks, on the driver's own clock
    // ([F42](../../docs/src/features/primary-failover.md))
    if let (Some(spec), Some(facts)) = (&fault, cluster_facts.as_mut()) {
        let marks = marks?.context("the fault arm ran without its fault")?;
        let started = measured
            .started
            .with_context(|| format!("{} injects a fault but its driver keeps no timeline", workload.id()))?;
        facts.fault = Some(fault::facts(
            "kill",
            spec.node,
            started,
            &marks,
            &measured.timeline,
            wall_clock,
        ));
        // and how the returning node caught up, if the arm asked for it to be watched
        if let (Some(samples), Some(restarted)) = (&marks.catchup, marks.restarted_at) {
            facts.catchup = Some(catchup::cut(restarted.saturating_duration_since(started), samples));
        }
    }
    // a background arm cuts what its client saw at the repair's marks
    // ([F44](../../docs/src/features/repair.md))
    if let (Some(spec), Some(facts)) = (&background_spec, cluster_facts.as_mut()) {
        let marks = background_marks?.context("the background arm ran without its operation")?;
        let started = measured
            .started
            .with_context(|| format!("{} runs a background operation but its driver keeps no timeline", workload.id()))?;
        match spec.kind {
            crate::workloads::workload::BackgroundKind::Repair => {
                let (partitions, bytes) = match (integrity_before, integrity_after) {
                    (Some(before), Some(after)) => (after.0.saturating_sub(before.0), after.1.saturating_sub(before.1)),
                    _ => (0, 0),
                };
                facts.background = Some(background::facts(started, &marks, &measured.timeline, spec.run_for, partitions, bytes));
            }
            // a migration arm records the move's marks, phases and transfer the same way
            // ([F45](../../docs/src/features/replica-migration.md))
            crate::workloads::workload::BackgroundKind::Move { .. } => {
                facts.migration = Some(background::migration_facts(started, &marks, &measured.timeline, spec.run_for));
            }
            // a backup arm records the backup's marks, files and bytes the same way
            // ([F49](../../docs/src/features/backup-and-recovery.md))
            crate::workloads::workload::BackgroundKind::Backup => {
                facts.backup = Some(background::backup_facts(started, &marks, &measured.timeline, spec.run_for));
            }
            // a rebalance arm records its plan's marks, steps and blocked reason the same way
            // ([F46](../../docs/src/features/capacity-rebalancing.md))
            crate::workloads::workload::BackgroundKind::Rebalance
            | crate::workloads::workload::BackgroundKind::Decommission { .. }
            | crate::workloads::workload::BackgroundKind::Expire { .. } => {
                // a decommission built with nowhere to go is the blocked case, and the record
                // is named for what the arm was built to show
                let kind = match &spec.kind {
                    crate::workloads::workload::BackgroundKind::Rebalance => "rebalance",
                    crate::workloads::workload::BackgroundKind::Expire { .. } => "expiry",
                    crate::workloads::workload::BackgroundKind::Decommission { blocked: true, .. } => "capacity_blocked",
                    _ => "decommission",
                };
                facts.rebalance = Some(background::rebalance_facts(kind, started, &marks, &measured.timeline, spec.run_for));
            }
        }
    }
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
    if timed_reads && workload.expects_rows() && measured.counters.get("retrieved").copied() == Some(0) {
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
        // the cluster record is the caller's, carried whole, or the one this process's own
        // cluster node reported; a standalone run has none
        cluster: request.cluster.clone().or(cluster_facts),
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
    let mut pool = ShoalPool::<Bench>::start(conf)
        .map_err(|error| anyhow::anyhow!("failed to start a server: {error:?}"))?;
    // wait until every shard has bound, so a shard that cannot start is reported by name here
    // rather than as thirty seconds of refused connections below (item 58)
    pool.ready(ready::TIMEOUT)
        .map_err(|error| anyhow::anyhow!("a shard failed to start: {error:?}"))?;
    // then wait until it answers, the way the workload will reach it: the probe is what proves
    // an encrypted arm's handshake, which a bound listener alone does not
    probe(runtime, addr, tls)?;
    Ok(Some(pool))
}

/// Waits until a server answers a query, the way the workload will reach it
///
/// # Arguments
///
/// * `runtime` - The client runtime to probe on
/// * `addr` - Where the server is
/// * `tls` - What a client needs to reach it encrypted, if it is
fn probe(
    runtime: &tokio::runtime::Runtime,
    addr: &str,
    tls: Option<shoal::shared::tls::TlsClientOptions>,
) -> Result<()> {
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
    }))
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

/// The rehome record the pool reported, as the artifact carries it
///
/// # Arguments
///
/// * `report` - What the start moved
fn rehome_facts(report: &shoal::server::RehomeReport) -> crate::model::macro_layer::RehomeFacts {
    // every count carried as it was reported, widened to the artifact's integers
    crate::model::macro_layer::RehomeFacts {
        from: report.from as u64,
        to: report.to as u64,
        tablets_moved: report.tablets_moved as u64,
        slots_moved: report.slots_moved as u64,
        groups: report.groups,
        records: report.records,
        bytes: report.bytes,
        folded: report.folded,
        installs_dropped: report.installs_dropped,
        steps_redone: report.steps_redone,
        millis: report.millis,
    }
}

#[cfg(test)]
mod rehome_tests {
    /// A rehome record carries every count the pool reported, so the artifact says what moved
    #[test]
    fn rehome_capture_records_the_move() {
        let report = shoal::server::RehomeReport {
            from: 12,
            to: 8,
            tablets_moved: 0,
            slots_moved: 4,
            groups: 8,
            records: 4096,
            bytes: 1 << 20,
            installs_dropped: 0,
            folded: 0,
            millis: 1234,
            steps_redone: 0,
        };
        let facts = super::rehome_facts(&report);
        assert_eq!((facts.from, facts.to), (12, 8));
        assert_eq!(facts.slots_moved, 4);
        assert_eq!(facts.groups, 8);
        assert_eq!(facts.records, 4096);
        assert_eq!(facts.bytes, 1 << 20);
        assert_eq!(facts.millis, 1234);
        assert_eq!(facts.steps_redone, 0);
        // and round trips through the artifact's json under its own key
        let json = serde_json::to_value(&facts).expect("json");
        assert_eq!(json["millis"], 1234);
        let back: crate::model::macro_layer::RehomeFacts = serde_json::from_value(json).expect("back");
        assert_eq!(back, facts);
        // a capture from before the record reads back with none
        let older: crate::model::macro_layer::ClusterFacts = serde_json::from_str(
            r#"{"nodes":1,"desired_rf":1,"active_rf":1,"write_policy":"quorum","read_policy":"one","durability":"fsync","driver":"in-process","cores":[],"tables":1,"tablets":4096,"emulated":true}"#,
        )
        .expect("an older record");
        assert!(older.rehome.is_none());
    }
}
