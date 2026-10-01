//! Running a benchmark: the cluster, the preload, every arm, and the capture
//!
//! The run is one future on a thread and runtime of its own (see [`super::run`]): a deployment
//! is not `Sync`, bootstrapping blocks on ssh, and neither may stall whatever is drawing the run.
//! It sends what it is doing over a [`Progress`] and is told to stop through a watch of
//! [`Control`].
//!
//! In order, for the bench's own cluster:
//!
//! 1. every table's file is scanned, before any host is touched;
//! 2. the inventory is copied into the bench's own ([`super::owned`]), every host is probed, and
//!    a host running anything the bench was not told it may stop is refused;
//! 3. units named by `--stop-unit` are stopped and governors set, each recorded in a
//!    [`Restore`] that puts them back however the run ends;
//! 4. the cluster is bootstrapped, its roots judged by the wipe guard first;
//! 5. each arm runs on a cluster in the state it needs: after an arm that wrote, ran an event or
//!    ran under another override, the cluster is wiped, bootstrapped and preloaded again;
//! 6. the capture is written after every arm, so a run that dies keeps what it measured;
//! 7. the cluster is destroyed (or kept with `--keep-cluster`) and the hosts are restored.

use color_eyre::eyre::{bail, eyre, WrapErr};
use rkyv::Archive;
use shoal::shared::dataset::DatasetSupport;
use shoal::shared::identity::NodeId;
use shoal::shared::traits::QuerySupport;
use shoal::Shoal;
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::driver::{send_options, ArmClock, ArmOutcome, ArmSettings, Driver};
use shoal_loadgen::events::{cut_background, cut_fault, p99_ratio_permille, Cut, Mark};
use shoal_loadgen::feed::{prepare, ScanOptions, TableSource};
use shoal_loadgen::pick::Picker;
use shoal_loadgen::progress::{BenchEvent, Control, Phase, Progress};
use shoal_loadgen::results::{
    Capture, CodeFacts, DatasetFacts, EventFacts, NodeFacts, Provenance, RunResult, SchemaFacts,
    SecondPhase, SecondSample, ServerSample, FORMAT,
};
use shoal_loadgen::spec::{ArmPlan, BenchSpec, EventKind, Mode, Override, Reads};
use shoal_loadgen::window::{Window, WindowSummary};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use super::args::BenchRunArgs;
use super::events::{EventPlan, EventRun};
use super::hosts::{self, Restore};
use super::owned::{self, BenchInventory, DeriveOptions};
use super::provenance::rfc3339;
use crate::cli::ProjectArgs;
use crate::deploy::remote::Host;
use crate::deploy::state::ClusterRecord;
use crate::deploy::{Deployment, Inventory};

/// How long a node has to answer once it is up
const CONNECT_TIMEOUT: Duration = Duration::from_secs(120);

/// How often the nodes' own figures are read during an arm
const SERVER_EVERY: Duration = Duration::from_secs(2);

/// How many preloaded rows a table is sampled for, to check an attached cluster holds them
const PRELOADED_SAMPLE: u64 = 1000;

/// Everything a run was asked for, decided before it starts
#[derive(Clone)]
pub struct Context {
    /// What the command line said about the project
    pub project: ProjectArgs,
    /// The run's flags
    pub args: BenchRunArgs,
    /// The spec they resolve to
    pub spec: BenchSpec,
    /// The dataset folder, judged
    pub dataset: Dataset,
    /// Where the capture goes
    pub dir: PathBuf,
    /// The capture's label
    pub label: String,
    /// The inventory, for the bench's own cluster or an attached one
    pub inventory: Option<PathBuf>,
    /// The project's code
    pub project_facts: CodeFacts,
    /// Shoal's code
    pub shoal_facts: CodeFacts,
}

impl Context {
    /// What a deployment of the bench's own cluster is told about the project: a profile build
    /// when `--profile` was given
    fn hint(&self) -> crate::deploy::ProjectHint {
        crate::deploy::ProjectHint {
            flavor: if self.args.profile {
                crate::build::Flavor::Profile
            } else {
                crate::build::Flavor::Release
            },
            ..self.project.hint()
        }
    }

    /// Bring a profile build's heap dumps back from every node, if this is one
    ///
    /// # Arguments
    ///
    /// * `progress` - Where a failure is said
    fn collect_profiles(&self, progress: &Progress) {
        // only a profile build writes dumps, and only the bench's own cluster runs one
        if !self.args.profile {
            return;
        }
        let Ok(inventory) = Inventory::read(&self.dir.join("inventory.bench.yml")) else {
            return;
        };
        for failure in super::profile::collect(&inventory, &self.dir.join("prof")) {
            progress.log(format!("heap dumps: {failure}"));
        }
    }
}

/// The cluster a run drives
enum Cluster {
    /// The bench's own, copied from the inventory
    Owned {
        /// The deployment of the copy
        deployment: Deployment,
        /// The copy, and the inventory it came from
        bench: BenchInventory,
        /// The inventory it was copied from
        base_path: PathBuf,
        /// The override the cluster is running under now
        current: Option<String>,
    },
    /// The inventory's own cluster, as it is
    Attach {
        /// Its deployment
        deployment: Deployment,
    },
    /// One node started by hand
    Addr {
        /// Its client address
        addr: String,
    },
}

/// The schema's database name, read off its client's type name
fn db_name<S>() -> String {
    // `my_crate::module::CatalogClient` is the `Catalog` database
    let full = std::any::type_name::<S>();
    let name = full.rsplit("::").next().unwrap_or(full);
    name.strip_suffix("Client").unwrap_or(name).to_string()
}

impl Cluster {
    /// The deployment, unless this is one node started by hand
    fn deployment(&self) -> Option<&Deployment> {
        match self {
            Cluster::Owned { deployment, .. } | Cluster::Attach { deployment } => Some(deployment),
            Cluster::Addr { .. } => None,
        }
    }

    /// A client of every member: the deployed nodes', or the one address
    ///
    /// # Errors
    ///
    /// When no member answers.
    async fn clients<S>(&self) -> color_eyre::Result<Vec<Arc<Shoal<S>>>>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let deployment = match self {
            Cluster::Addr { addr } => {
                // a node started by hand, with no credentials
                let shoal = Shoal::<S>::new(addr.as_str())
                    .await
                    .map_err(|error| eyre!("could not connect to {addr}: {error:?}"))?;
                return Ok(vec![Arc::new(shoal)]);
            }
            Cluster::Owned { deployment, .. } | Cluster::Attach { deployment } => deployment,
        };
        // every deployed node as the admin, skipping one that does not answer
        let record = deployment.state.record()?;
        let mut clients = Vec::new();
        let mut errors = Vec::new();
        for (name, node) in &record.nodes {
            let address: std::net::IpAddr = node.address.parse()?;
            let addr = crate::deploy::inventory::socket(address, deployment.inventory.ports.client);
            match deployment
                .connect::<S>(&addr, Instant::now() + CONNECT_TIMEOUT)
                .await
            {
                Ok(shoal) => clients.push(shoal),
                Err(error) => errors.push(format!("{name}: {error}")),
            }
        }
        if clients.is_empty() {
            bail!("no member answered: {}", errors.join("; "));
        }
        Ok(clients)
    }

    /// The name each deployed node was given, by its id
    fn names(&self) -> BTreeMap<NodeId, String> {
        self.deployment()
            .and_then(|deployment| deployment.state.record().ok())
            .map(|record| crate::deploy::ops::record_names(&record))
            .unwrap_or_default()
    }
}

/// Run a benchmark to its end, putting the hosts back however it ends
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `progress` - Where what it is doing is sent
/// * `control` - What it is told
///
/// # Errors
///
/// When the run could not finish; the capture holds what it measured before that.
pub async fn orchestrate<S>(
    ctx: Context,
    progress: Progress,
    control: tokio::sync::watch::Receiver<Control>,
    pollers: Option<Pollers<S>>,
) -> color_eyre::Result<PathBuf>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // every table scanned, before any host is touched
    progress.send(BenchEvent::Phase(Phase::Scan));
    let options = ScanOptions {
        preload: ctx.spec.preload,
        dedupe: ctx.spec.dedupe,
        max_parse_errors: ctx.spec.max_parse_errors,
    };
    let files = ctx.dataset.files.clone();
    let tables = tokio::task::spawn_blocking(move || {
        files
            .iter()
            .map(|file| prepare::<S>(file, &options))
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(|error| eyre!("the scan failed: {error}"))?
    .map_err(|error| eyre!("{error}"))?;
    for table in &tables {
        let scan = table.scan();
        progress.log(format!(
            "{}: {} rows ({} bad), {} preloaded with {} keys to read, {} to insert",
            scan.table, scan.rows, scan.parse_errors, scan.preload_rows, scan.read_keys, scan.insert_rows
        ));
    }
    // the capture, written before anything else so a run that dies early still says why
    let mut capture = Capture {
        format: FORMAT,
        label: ctx.label.clone(),
        provenance: Provenance {
            tool: env!("CARGO_PKG_VERSION").to_string(),
            started_at: rfc3339(SystemTime::now()),
            finished_at: None,
            mode: Some(ctx.spec.mode),
            flavor: if ctx.args.profile { "profile" } else { "release" }.to_string(),
            project: ctx.project_facts.clone(),
            shoal: ctx.shoal_facts.clone(),
            rustc: super::provenance::rustc(),
            rustflags: std::env::var("RUSTFLAGS").ok(),
            schema: SchemaFacts {
                db: db_name::<S>(),
                fingerprint: S::SCHEMA_FINGERPRINT,
            },
            driver: hosts::local_facts().unwrap_or_default(),
            neighbours_allowed: ctx.args.allow_neighbours,
            ..Provenance::default()
        },
        spec: ctx.spec.clone(),
        spec_digest: ctx.spec.digest(),
        dataset: DatasetFacts::new(tables.iter().map(|table| table.scan().clone()).collect()),
        preload: None,
        arms: Vec::new(),
        complete: false,
        error: None,
    };
    capture.write(&ctx.dir)?;
    // everything changed on the hosts is recorded here and put back on every way out
    let restore = Arc::new(Mutex::new(Restore::default()));
    let result = drive::<S>(&ctx, &mut capture, tables, &restore, &progress, control, pollers).await;
    // a profile build's last dumps, before the teardown deletes the directory they are in
    let collecting = ctx.clone();
    let collector = progress.clone();
    let _ = tokio::task::spawn_blocking(move || collecting.collect_profiles(&collector)).await;
    // the cluster down, then the hosts back, off the runtime since ssh blocks
    progress.send(BenchEvent::Phase(Phase::Teardown));
    let failures = tokio::task::spawn_blocking(move || {
        restore
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finish()
    })
    .await
    .unwrap_or_else(|error| vec![format!("restoring the hosts panicked: {error}")]);
    for failure in &failures {
        progress.log(format!("restore: {failure}"));
    }
    // the capture, finished
    capture.provenance.finished_at = Some(rfc3339(SystemTime::now()));
    let mut errors = Vec::new();
    if let Err(error) = &result {
        errors.push(format!("{error:#}"));
    }
    errors.extend(failures.iter().map(|failure| format!("restore: {failure}")));
    if !errors.is_empty() {
        capture.error = Some(errors.join("; "));
    }
    capture.write(&ctx.dir)?;
    result?;
    if !failures.is_empty() {
        bail!("the run finished, and the hosts were not all put back: {}", failures.join("; "));
    }
    Ok(ctx.dir.clone())
}

/// Bring the cluster up, run every arm, and record each in the capture
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `capture` - The capture being written
/// * `tables` - Every table, scanned
/// * `restore` - Where changes to the hosts are recorded
/// * `progress` - Where what it is doing is sent
/// * `control` - What it is told
/// * `pollers` - Where a screen is handed a poller of each cluster the run brings up
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn drive<S>(
    ctx: &Context,
    capture: &mut Capture,
    tables: Vec<Arc<dyn TableSource<S::QueryKinds>>>,
    restore: &Arc<Mutex<Restore>>,
    progress: &Progress,
    control: tokio::sync::watch::Receiver<Control>,
    pollers: Option<Pollers<S>>,
) -> color_eyre::Result<()>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let spec = &ctx.spec;
    let aborted = || *control.borrow() == Control::Abort;
    // every mix must have something to act on, checked before the cluster exists
    let scans: Vec<_> = tables.iter().map(|table| table.scan().clone()).collect();
    let scan_refs: Vec<_> = scans.iter().collect();
    for mix in &spec.mixes {
        Picker::new(mix, &scan_refs, &spec.tables, spec.distribution, spec.read_keys, spec.seed, "check")
            .map_err(|error| eyre!(error))?;
    }
    let arms = spec.arms();
    progress.send(BenchEvent::Planned {
        label: ctx.label.clone(),
        arms: arms.clone(),
    });
    // the cluster
    let mut cluster = open::<S>(ctx, capture, restore, progress).await?;
    capture.write(&ctx.dir)?;
    hand_poller::<S>(&cluster, pollers.as_ref()).await;
    let mut driver: Option<Driver<S>> = None;
    let mut loaded = false;
    let mut disturbed = false;
    for (index, arm) in arms.iter().enumerate() {
        if aborted() {
            bail!("aborted before {} run {}", arm.id, arm.run);
        }
        // the bench's own cluster is put back to a fresh preload whenever the last arm changed it
        if let Cluster::Owned { current, .. } = &cluster {
            let wanted = arm.overrides.as_ref().map(|over| over.name.clone());
            if disturbed || *current != wanted {
                progress.send(BenchEvent::Phase(Phase::Reset));
                // a reset stops every node, so their dumps so far come back first
                ctx.collect_profiles(progress);
                reset::<S>(ctx, &mut cluster, arm.overrides.as_ref(), progress).await?;
                hand_poller::<S>(&cluster, pollers.as_ref()).await;
                driver = None;
                loaded = false;
            }
        }
        // the driver, over a client of every member
        if driver.is_none() {
            let clients = cluster.clients::<S>().await?;
            driver = Some(Driver::new(
                clients,
                tables.clone(),
                send_options(spec.read_level),
                spec.workers,
            ));
        }
        // the preload, or the check that an attached cluster holds it
        if !loaded {
            let current = driver.as_ref().expect("the driver was just made");
            if spec.mode == Mode::Attach && spec.preloaded {
                check_preloaded(current, progress).await?;
            } else {
                let bundle = spec.bundles.iter().copied().max().unwrap_or(64).max(64);
                let started = Instant::now();
                let (seconds, took) = current.preload(bundle, spec.in_flight_for(bundle), progress).await;
                let window = Window::sum(&seconds);
                let summary = window.summary(took);
                if summary.insert.failed() > 0 {
                    bail!(
                        "the preload failed {} inserts: {:?}",
                        summary.insert.failed(),
                        summary.insert.errors
                    );
                }
                progress.log(format!(
                    "preloaded {} rows in {:.1}s",
                    summary.insert.ok,
                    started.elapsed().as_secs_f64()
                ));
                capture.preload.get_or_insert(summary);
            }
            // cold reads start from storage: every node is restarted and the clients made again
            if spec.reads == Reads::Cold {
                progress.send(BenchEvent::Phase(Phase::Restart));
                restart_all::<S>(&cluster).await?;
                driver = Some(Driver::new(
                    cluster.clients::<S>().await?,
                    tables.clone(),
                    send_options(spec.read_level),
                    spec.workers,
                ));
            }
            loaded = true;
        }
        let current = driver.as_ref().expect("the driver was made");
        // the arm, with its event and the nodes' own figures beside it
        let result = run_arm::<S>(ctx, &cluster, current, arm, index, restore, progress, &control).await?;
        progress.send(BenchEvent::ArmDone {
            index,
            summary: result.measured.clone(),
            ended_early: result.ended_early.as_ref().map(|ended| ended.reason.clone()),
        });
        capture.arm_mut(arm).runs.push(result);
        capture.write(&ctx.dir)?;
        // the next arm on this cluster finds what this one left
        disturbed = arm.disturbs();
        // an event can leave the cluster without the members the driver went through
        if arm.event != EventKind::None {
            driver = None;
        }
    }
    capture.complete = true;
    Ok(())
}

/// Where a screen is handed a poller of each cluster the run brings up
pub type Pollers<S> = tokio::sync::mpsc::Sender<crate::cluster::stats::tui::Poller<S>>;

/// Hand a screen a poller of the cluster as it is now, if a screen wants one
///
/// A reset brings up a new cluster under new identities, so the screen is told to read that one
/// rather than a member that no longer exists.
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `pollers` - Where the screen takes its pollers, if one is drawn
async fn hand_poller<S>(cluster: &Cluster, pollers: Option<&Pollers<S>>)
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // one node by hand, or no screen, has nothing to read
    let (Some(pollers), Some(deployment)) = (pollers, cluster.deployment()) else {
        return;
    };
    let Ok(record) = deployment.state.record() else {
        return;
    };
    let (Ok(admin), Ok(password)) = (deployment.any_member::<S>(&record).await, deployment.state.password()) else {
        return;
    };
    let poller = crate::cluster::stats::tui::Poller::new(
        admin,
        None,
        cluster.names(),
        deployment.inventory.admin.clone(),
        password,
    );
    // a screen that has gone away wants nothing
    let _ = pollers.send(poller).await;
}

/// Bring the cluster a run drives up, or find it
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `capture` - The capture, whose provenance is filled in
/// * `restore` - Where changes to the hosts are recorded
/// * `progress` - Where what it is doing is sent
async fn open<S>(
    ctx: &Context,
    capture: &mut Capture,
    restore: &Arc<Mutex<Restore>>,
    progress: &Progress,
) -> color_eyre::Result<Cluster>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // one node by hand has nothing to probe or bring up
    if let Some(addr) = &ctx.args.addr {
        return Ok(Cluster::Addr { addr: addr.clone() });
    }
    let inventory_path = ctx
        .inventory
        .clone()
        .ok_or_else(|| eyre!("no inventory was given or found"))?;
    capture.provenance.inventory_digest = Some(owned::file_digest(&inventory_path)?);
    capture.provenance.inventory_shape = Some(owned::shape_digest(&inventory_path)?);
    // an attached cluster is found as it is
    if ctx.spec.mode == Mode::Attach {
        let deployment = Deployment::attach(&inventory_path)?;
        if deployment.state.record()?.nodes.is_empty() {
            bail!("{} has not been deployed from this machine's state", deployment.inventory.name);
        }
        capture.provenance.flavor = "attached".to_string();
        probe_nodes(&deployment.inventory, capture)?;
        return Ok(Cluster::Attach { deployment });
    }
    // the bench's own: copied, probed, its neighbours judged, then brought up
    let bench = derive(ctx, &inventory_path, None)?;
    let deployment = Deployment::open(&bench.path, ctx.hint())?;
    capture.provenance.bench_inventory_digest = Some(owned::file_digest(&bench.path)?);
    probe_nodes(&bench.inventory, capture)?;
    prepare_hosts(ctx, &bench, capture, restore)?;
    // its teardown runs first on the way out, whatever else happens
    let teardown_path = bench.path.clone();
    let keep = ctx.args.keep_cluster;
    restore
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .on_teardown(move || {
            if keep {
                eprintln!("leaving the bench's cluster up: `shoaladm destroy -i {} --yes` removes it", teardown_path.display());
                return Ok(());
            }
            Deployment::attach(&teardown_path)?.destroy()
        });
    progress.send(BenchEvent::Phase(Phase::Build));
    let mut cluster = Cluster::Owned {
        deployment,
        bench,
        base_path: inventory_path,
        current: None,
    };
    progress.send(BenchEvent::Phase(Phase::Bootstrap));
    bring_up::<S>(&mut cluster).await?;
    // which program each node runs, now that it was built; a profile build's beside its dumps,
    // which are read against it
    if let Cluster::Owned { deployment, .. } = &cluster {
        for (name, facts) in &mut capture.provenance.nodes {
            if let Ok(program) = deployment.program(name) {
                facts.program_sha256 = crate::deploy::ops::digest(&program).ok();
                if ctx.args.profile {
                    let bin = ctx.dir.join("prof").join("bin");
                    std::fs::create_dir_all(&bin)?;
                    if let Some(file) = program.file_name() {
                        std::fs::copy(&program, bin.join(file))?;
                    }
                }
            }
        }
    }
    Ok(cluster)
}

/// Copy the inventory into the bench's own, under an override if one
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `base` - The inventory
/// * `overrides` - The override the cluster runs under, if one
fn derive(ctx: &Context, base: &std::path::Path, overrides: Option<&Override>) -> color_eyre::Result<BenchInventory> {
    // a profile build is built from the project, so it needs the project even over a `server:`
    let from_project = (ctx.args.from_project || ctx.args.profile)
        .then(|| ctx.project.dir())
        .transpose()?;
    let options = DeriveOptions {
        port_offset: ctx.args.port_offset,
        bench_storage: ctx.args.bench_storage.clone(),
        from_project,
        overrides: overrides.cloned(),
        spare: ctx.spec.spare.clone(),
    };
    owned::derive(base, &options, &ctx.dir.join("inventory.bench.yml"))
}

/// Read every node's machine into the capture, and say whether the driver is one of them
///
/// # Arguments
///
/// * `inventory` - The inventory whose nodes are probed
/// * `capture` - The capture
fn probe_nodes(inventory: &Inventory, capture: &mut Capture) -> color_eyre::Result<()> {
    for spec in &inventory.nodes {
        let host = Host {
            target: spec.target().to_string(),
        };
        let facts = hosts::probe(&host).wrap_err_with(|| format!("probing {}", spec.name))?;
        if facts.hostname == capture.provenance.driver.hostname {
            capture.provenance.driver_shares_host = true;
        }
        capture.provenance.nodes.insert(
            spec.name.clone(),
            NodeFacts {
                host: facts,
                governor_ran: None,
                target_cpu: inventory.resolve_target_cpu(spec),
                program_sha256: None,
            },
        );
    }
    Ok(())
}

/// Judge every host's neighbours, stop the units the bench may, and set governors
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `bench` - The bench's inventory
/// * `capture` - The capture, which records what was changed
/// * `restore` - Where every change is recorded
fn prepare_hosts(
    ctx: &Context,
    bench: &BenchInventory,
    capture: &mut Capture,
    restore: &Arc<Mutex<Restore>>,
) -> color_eyre::Result<()> {
    let inventory = &bench.inventory;
    let ports = [inventory.ports.client, inventory.ports.peer, inventory.ports.control];
    let own = inventory.unit_name();
    let mut refusals = Vec::new();
    let mut to_stop = Vec::new();
    for spec in &inventory.nodes {
        let host = Host {
            target: spec.target().to_string(),
        };
        let output = host.run(&hosts::neighbours_script(&ports))?;
        let neighbours = hosts::parse_neighbours(&output, &own);
        // a port already bound would fail the bootstrap half way
        for port in neighbours.ports {
            refusals.push(format!("{}: something already listens on {port}", spec.name));
        }
        // another cluster on the host is stopped only when named, or run beside when allowed
        for unit in neighbours.units {
            let named = ctx
                .args
                .stop_unit
                .iter()
                .any(|stop| stop == &unit || format!("{stop}.service") == unit);
            if named {
                to_stop.push((host.clone(), unit));
            } else if !ctx.args.allow_neighbours {
                refusals.push(format!(
                    "{}: {unit} is running; name it with --stop-unit to stop it for the run, or pass --allow-neighbours",
                    spec.name
                ));
            }
        }
    }
    if !refusals.is_empty() {
        bail!("the bench cannot share these hosts:\n  - {}", refusals.join("\n  - "));
    }
    let mut restore = restore.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    for (host, unit) in to_stop {
        eprintln!("[{}] stopping {unit} for the run", host.target);
        hosts::stop_unit(&mut restore, &host, &unit)?;
        capture.provenance.stopped_units.push(format!("{}:{unit}", host.target));
    }
    // the governor every node runs under, recorded as found and as run
    if let Some(governor) = &ctx.args.governor {
        for spec in &inventory.nodes {
            let host = Host {
                target: spec.target().to_string(),
            };
            let Some(facts) = capture.provenance.nodes.get_mut(&spec.name) else {
                continue;
            };
            hosts::set_governor(&mut restore, &host, &facts.host.governor, governor)?;
            facts.governor_ran = Some(governor.clone());
        }
    }
    Ok(())
}

/// Judge every node's roots before anything is wiped, against what the bench recorded
///
/// # Arguments
///
/// * `deployment` - The bench's deployment
///
/// # Errors
///
/// When any root holds something that is not the bench's.
fn wipe_guard(deployment: &Deployment) -> color_eyre::Result<()> {
    let record = deployment.state.record()?;
    let mut refused = Vec::new();
    for spec in &deployment.inventory.nodes {
        let host = Host {
            target: spec.target().to_string(),
        };
        let (storage, _) = deployment.inventory.resolve_storage(spec);
        let output = host.run(&hosts::markers_script(&storage.roots()))?;
        let own = record.nodes.get(&spec.name).map(|node| node.node.as_str());
        refused.extend(hosts::judge_wipe(&spec.name, &output, record.cluster.as_deref(), own));
    }
    if !refused.is_empty() {
        bail!("the bench will not wipe these roots:\n  - {}", refused.join("\n  - "));
    }
    Ok(())
}

/// Bootstrap the bench's cluster, with its roots judged first
///
/// # Arguments
///
/// * `cluster` - The bench's cluster
async fn bring_up<S>(cluster: &mut Cluster) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let Cluster::Owned { deployment, .. } = cluster else {
        return Ok(());
    };
    // whatever is on the roots is the bench's own or it is left alone
    wipe_guard(deployment)?;
    // the record forgets the last cluster; its authority and password are kept
    deployment.state.create()?;
    deployment.state.save(&ClusterRecord::default())?;
    deployment.bootstrap::<S>(true).await
}

/// Wipe the bench's cluster and bring it up again, under another override if one is asked for
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `cluster` - The bench's cluster
/// * `overrides` - The override the next arm runs under
/// * `progress` - Where what it is doing is sent
async fn reset<S>(
    ctx: &Context,
    cluster: &mut Cluster,
    overrides: Option<&Override>,
    progress: &Progress,
) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let Cluster::Owned {
        deployment,
        bench,
        base_path,
        current,
    } = cluster
    else {
        return Ok(());
    };
    let wanted = overrides.map(|over| over.name.clone());
    // another override is another inventory, under the same cluster name and state
    if *current != wanted {
        progress.log(format!(
            "reconfiguring the bench's cluster for {}",
            wanted.as_deref().unwrap_or("the inventory as it is")
        ));
        let record = deployment.state.record()?;
        *bench = derive(ctx, base_path, overrides)?;
        let reopened = Deployment::open(&bench.path, ctx.hint())?;
        // the record carries over, so the wipe guard knows the cluster it is wiping
        reopened.state.save(&record)?;
        *deployment = reopened;
        *current = wanted;
    }
    bring_up::<S>(cluster).await
}

/// Restart every node of the cluster and wait for it to take writes again
///
/// # Arguments
///
/// * `cluster` - The cluster
async fn restart_all<S>(cluster: &Cluster) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let deployment = cluster
        .deployment()
        .ok_or_else(|| eyre!("cold reads restart every node, which needs an inventory"))?;
    deployment.systemctl("restart", None)?;
    let record = deployment.state.record()?;
    let shoal = deployment.any_member::<S>(&record).await;
    // a node still starting refuses; the members wait covers it
    let shoal = match shoal {
        Ok(shoal) => shoal,
        Err(_) => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            deployment.any_member::<S>(&record).await?
        }
    };
    crate::deploy::ops::wait_for_writes(&shoal).await
}

/// Read a sample of each table's preloaded rows from an attached cluster, refusing a miss
///
/// # Arguments
///
/// * `driver` - The driver
/// * `progress` - Where it is shown
async fn check_preloaded<S>(driver: &Driver<S>, progress: &Progress) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // an even spread over each table's read keys
    let mut queries = VecDeque::new();
    for table in driver.tables() {
        let keys = table.scan().read_keys;
        let step = (keys / PRELOADED_SAMPLE).max(1);
        for index in (0..keys).step_by(step as usize).take(PRELOADED_SAMPLE as usize) {
            queries.push_back(table.read_query(&[index]));
        }
    }
    let sampled = queries.len();
    let window = driver.read_all(queries, 64, 256, progress).await;
    if window.read.misses > 0 || !window.read.errors.is_empty() {
        bail!(
            "--preloaded, but {} of {sampled} sampled preloaded rows were not found and {:?} failed; \
             the cluster does not hold this dataset's preload",
            window.read.misses,
            window.read.errors
        );
    }
    progress.log(format!("checked {sampled} preloaded rows are in the cluster"));
    Ok(())
}

/// The nodes' own figures as one sample: answers a second by kind, and the slowest p99
///
/// # Arguments
///
/// * `model` - The leader's figures
/// * `at_ms` - When, on the arm's clock
fn server_sample(model: &crate::cluster::stats::StatsModel, at_ms: u64) -> ServerSample {
    let mut sample = ServerSample {
        at_ms,
        ..ServerSample::default()
    };
    for member in &model.view.members {
        let Some(stats) = crate::cluster::stats::live(member) else {
            continue;
        };
        for op in &stats.queries.ops {
            *sample.answers_per_sec.entry(op.op.clone()).or_default() += op.rate.r10s;
        }
        if let Some(p99) = stats.queries.p99_ms {
            sample.p99_ms = Some(sample.p99_ms.map_or(p99, |max: f64| max.max(p99)));
        }
    }
    sample
}

/// The plan of an arm's event: which node, which host, when
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `cluster` - The cluster
/// * `arm` - The arm
/// * `admin` - A client to ask the cluster who leads
async fn event_plan<S>(
    ctx: &Context,
    cluster: &Cluster,
    arm: &ArmPlan,
    admin: &Arc<Shoal<S>>,
) -> color_eyre::Result<EventPlan>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let spec = &ctx.spec;
    let deployment = cluster
        .deployment()
        .ok_or_else(|| eyre!("an event needs an inventory"))?;
    let record = deployment.state.record()?;
    let measured = Duration::from_secs(spec.duration);
    let warmup = Duration::from_secs(spec.warmup);
    let at = warmup + measured.mul_f64(spec.event_at / 100.0);
    let restart_at = warmup + measured.mul_f64(spec.restart_at / 100.0);
    // the node: the one named, the leader, or the last deployed
    let node = match arm.event {
        EventKind::Rebalance | EventKind::Repair | EventKind::Backup | EventKind::None => None,
        _ => {
            let victim = spec.victim.clone().unwrap_or_else(|| "last".to_string());
            let names = crate::deploy::ops::record_names(&record);
            let name = match victim.as_str() {
                "last" => record.nodes.keys().last().cloned(),
                "leader" => {
                    let model = crate::cluster::poll(admin).await.map_err(|error| eyre!(error))?;
                    model
                        .leader
                        .as_deref()
                        .and_then(|leader| leader.parse().ok().map(|id: uuid::Uuid| NodeId(id)))
                        .and_then(|id| names.get(&id).cloned())
                }
                named => Some(named.to_string()),
            };
            Some(name.ok_or_else(|| eyre!("no node is the event's victim {victim:?}"))?)
        }
    };
    let (node_id, target) = match &node {
        Some(name) => {
            let found = record
                .nodes
                .get(name)
                .ok_or_else(|| eyre!("{name} is not a deployed node of the cluster"))?;
            (Some(found.node.clone()), Some(found.target.clone()))
        }
        None => (None, None),
    };
    let table = spec
        .event_table
        .clone()
        .or_else(|| ctx.dataset.files.first().map(|file| file.table.to_string()));
    Ok(EventPlan {
        kind: arm.event,
        node,
        node_id,
        target,
        unit: deployment.inventory.unit_name(),
        table,
        backup_dir: format!("{}/bench-backups", deployment.inventory.remote_dir()),
        at,
        restart_at,
        timeout: Duration::from_secs(spec.event_timeout),
    })
}

/// Make ready what an event arm needs before it starts: a spare joined, a wire version activated
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `cluster` - The cluster
/// * `arm` - The arm
/// * `admin` - A client of a member
async fn ready_event<S>(
    ctx: &Context,
    cluster: &Cluster,
    arm: &ArmPlan,
    admin: &Arc<Shoal<S>>,
) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let Some(deployment) = cluster.deployment() else {
        return Ok(());
    };
    // a move needs its spare in the cluster and empty, which a fresh bootstrap leaves out
    if arm.event.needs_spare() {
        let spare = ctx.spec.spare.clone().ok_or_else(|| eyre!("the event needs --spare"))?;
        if !deployment.state.record()?.nodes.contains_key(&spare) {
            wipe_guard(deployment)?;
            deployment.add::<S>(&spare, true, false).await?;
        }
    }
    // a backup needs every member on the wire version that carries it
    if arm.event == EventKind::Backup {
        let model = crate::cluster::poll(admin).await.map_err(|error| eyre!(error))?;
        if model.activated_wire < model.wire_range.1 {
            let line = format!("activate {}", model.wire_range.1);
            deployment.admin(admin, &line, Duration::from_secs(120)).await?;
        }
    }
    Ok(())
}

/// Run one arm against the cluster, with its event and the nodes' figures beside it
///
/// # Arguments
///
/// * `ctx` - What was asked for
/// * `cluster` - The cluster
/// * `driver` - The driver
/// * `arm` - The arm
/// * `index` - Its place in the plan
/// * `restore` - Where changes to the hosts are recorded
/// * `progress` - Where what it is doing is sent
/// * `control` - What the run is told
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_arm<S>(
    ctx: &Context,
    cluster: &Cluster,
    driver: &Driver<S>,
    arm: &ArmPlan,
    index: usize,
    restore: &Arc<Mutex<Restore>>,
    progress: &Progress,
    control: &tokio::sync::watch::Receiver<Control>,
) -> color_eyre::Result<RunResult>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let spec = &ctx.spec;
    let warmup = Duration::from_secs(spec.warmup);
    let duration = Duration::from_secs(spec.duration);
    // a client of a member an event leaves up, for the figures and the event's admin operations
    let admin = match cluster.deployment() {
        Some(deployment) => {
            let record = deployment.state.record()?;
            Some(deployment.any_member::<S>(&record).await?)
        }
        None => None,
    };
    // what the event needs, ready before the clock starts
    let plan = match (&admin, arm.event) {
        (_, EventKind::None) | (None, _) => None,
        (Some(admin), _) => {
            ready_event::<S>(ctx, cluster, arm, admin).await?;
            Some(event_plan::<S>(ctx, cluster, arm, admin).await?)
        }
    };
    let picker = Picker::new(
        &arm.mix,
        &driver.tables().iter().map(|table| table.scan()).collect::<Vec<_>>(),
        &spec.tables,
        spec.distribution,
        spec.read_keys,
        spec.seed,
        &format!("{}/{}", arm.id, arm.run),
    )
    .map_err(|error| eyre!(error))?;
    let settings = ArmSettings {
        bundle: arm.bundle,
        in_flight: arm.in_flight,
        warmup,
        duration,
        on_exhaust: spec.on_exhaust,
        picker,
        inserts: arm.mix.writes(),
    };
    progress.send(BenchEvent::ArmStarted {
        index,
        arm: arm.clone(),
        warmup: spec.warmup,
        duration: spec.duration,
    });
    let started_at = rfc3339(SystemTime::now());
    let clock = ArmClock::start(warmup + duration);
    // an abort stops the arm where it is
    let watcher = {
        let clock = clock.clone();
        let mut control = control.clone();
        tokio::spawn(async move {
            while control.changed().await.is_ok() {
                if *control.borrow() == Control::Abort {
                    clock.stop("aborted");
                    return;
                }
            }
        })
    };
    // the nodes' own figures, every two seconds, beside the arm
    let sampling = Arc::new(AtomicBool::new(true));
    let sampler = admin.clone().map(|admin| {
        let deployment = cluster.deployment().expect("a client means a deployment");
        let names = cluster.names();
        let password = deployment.state.password().unwrap_or_default();
        let mut poller = crate::cluster::stats::tui::Poller::new(
            admin,
            None,
            names,
            deployment.inventory.admin.clone(),
            password,
        );
        let clock = clock.clone();
        let sampling = sampling.clone();
        tokio::spawn(async move {
            let mut samples = Vec::new();
            while sampling.load(Ordering::Relaxed) {
                if let Ok(model) = poller.poll().await {
                    samples.push(server_sample(&model, clock.elapsed_ms()));
                }
                tokio::time::sleep(SERVER_EVERY).await;
            }
            samples
        })
    });
    // the event, on the arm's clock
    let event = match (plan, &admin) {
        (Some(plan), Some(admin)) => Some(tokio::spawn(super::events::run::<S>(
            plan,
            admin.clone(),
            clock.clone(),
            restore.clone(),
            progress.clone(),
        ))),
        _ => None,
    };
    progress.send(BenchEvent::Phase(if spec.warmup > 0 { Phase::Warmup } else { Phase::Measure }));
    let outcome = driver.run_arm(&settings, clock.clone(), progress).await;
    // the event is waited for, so a node it took down is back before anything else happens
    let event_run = match event {
        Some(handle) => Some(handle.await.unwrap_or_else(|error| EventRun {
            outcome: format!("failed: the event panicked: {error}"),
            ..EventRun::default()
        })),
        None => None,
    };
    sampling.store(false, Ordering::Relaxed);
    let server_series = match sampler {
        Some(handle) => handle.await.unwrap_or_default(),
        None => Vec::new(),
    };
    watcher.abort();
    // every acknowledged insert read back
    let verify = if arm.mix.writes() && spec.verify_acks {
        progress.send(BenchEvent::Phase(Phase::Verify));
        let bundle = arm.bundle.max(64);
        Some(driver.verify(bundle, bundle * 4, progress).await)
    } else {
        None
    };
    if let Some(verify) = &verify {
        if verify.lost > 0 {
            progress.log(format!("{} run {}: {} acknowledged inserts were lost", arm.id, arm.run, verify.lost));
        }
    }
    Ok(result(spec, arm, index, started_at, outcome, event_run, verify, server_series, progress))
}

/// Make the run's record of an arm from what it did
///
/// # Arguments
///
/// * `spec` - The spec
/// * `arm` - The arm
/// * `index` - Its place in the plan
/// * `started_at` - When it started
/// * `outcome` - What the driver recorded
/// * `event_run` - What its event did
/// * `verify` - The read back of its inserts
/// * `server_series` - The nodes' own figures
/// * `progress` - How many events the screen dropped
#[allow(clippy::too_many_arguments)]
fn result(
    spec: &BenchSpec,
    arm: &ArmPlan,
    index: usize,
    started_at: String,
    outcome: ArmOutcome,
    event_run: Option<EventRun>,
    verify: Option<shoal_loadgen::results::VerifyFacts>,
    server_series: Vec<ServerSample>,
    progress: &Progress,
) -> RunResult {
    let warm = spec.warmup as usize;
    let seconds = &outcome.seconds;
    // the measured time, cut short where the arm ended early
    let measured_secs = match &outcome.ended_early {
        Some(ended) => (ended.at_secs - spec.warmup as f64).clamp(0.001, spec.duration as f64),
        None => spec.duration as f64,
    };
    let measured = Window::sum(seconds.iter().skip(warm)).summary(Duration::from_secs_f64(measured_secs));
    let warmup = Window::sum(seconds.iter().take(warm)).summary(Duration::from_secs(spec.warmup.max(1)));
    let end = warm + spec.duration as usize;
    let series: Vec<SecondSample> = seconds
        .iter()
        .enumerate()
        .map(|(at, window)| SecondSample {
            at: at as u64,
            phase: if at < warm {
                SecondPhase::Warmup
            } else if at < end {
                SecondPhase::Measure
            } else {
                SecondPhase::Drain
            },
            summary: window.summary(Duration::from_secs(1)),
            driver_cpu_pct: outcome.cpu.get(at).copied().unwrap_or_default(),
        })
        .collect();
    let event = event_run.map(|run| event_facts(arm, seconds, warm, run));
    let feeds: BTreeMap<_, _> = outcome.feeds.into_iter().collect();
    let wrapped = feeds.values().any(|facts| facts.wrapped_at.is_some());
    RunResult {
        run: arm.run,
        order: index,
        started_at,
        measured,
        warmup,
        series,
        ended_early: outcome.ended_early,
        feeds,
        wrapped,
        verify,
        event,
        server_series,
        driver_cpu_peak_pct: outcome.cpu.iter().copied().fold(0.0, f64::max),
        progress_dropped: progress.dropped(),
    }
}

/// Cut an event arm's seconds at its marks into the windows it is read in
///
/// # Arguments
///
/// * `arm` - The arm
/// * `seconds` - Its seconds
/// * `from` - Its first measured second
/// * `run` - What its event did
fn event_facts(arm: &ArmPlan, seconds: &[Window], from: usize, run: EventRun) -> EventFacts {
    // a fault is cut where the client felt it, anything else at its own marks
    let ok: Vec<u64> = seconds
        .iter()
        .map(|second| second.read.latency.len() + second.insert.latency.len())
        .collect();
    let failed: Vec<u64> = seconds
        .iter()
        .map(|second| second.read.failed() + second.insert.failed())
        .collect();
    let first = |kind: &str| run.marks.iter().find(|mark| mark.kind == kind).map(Mark::second);
    let cut: Option<Cut> = match arm.event {
        EventKind::Kill | EventKind::Stop | EventKind::Remove => {
            first("kill").or_else(|| first("stop")).map(|mark| cut_fault(&ok, &failed, from, mark))
        }
        _ => first("requested").map(|requested| {
            cut_background(seconds.len(), from, requested, first("done").or_else(|| first("failed")))
        }),
    };
    let mut facts = EventFacts {
        kind: arm.event,
        target: run.marks.iter().find_map(|mark| mark.note.clone()).filter(|_| arm.event.disrupts()),
        marks: run.marks,
        outcome: run.outcome,
        catchup: run.catchup,
        ..EventFacts::default()
    };
    if let Some(cut) = cut {
        let window = |range: std::ops::Range<usize>| -> WindowSummary {
            let secs = range.len().max(1) as u64;
            Window::sum(seconds.get(range).unwrap_or_default()).summary(Duration::from_secs(secs))
        };
        let before = window(cut.before.clone());
        let during = window(cut.during.clone());
        facts.p99_ratio_permille = p99_ratio_permille(
            before.read.latency.p99_ms.max(before.insert.latency.p99_ms),
            during.read.latency.p99_ms.max(during.insert.latency.p99_ms),
        );
        facts.windows.insert("before".to_string(), before);
        facts.windows.insert("during".to_string(), during);
        facts.windows.insert("after".to_string(), window(cut.after.clone()));
        facts.failure_at = cut.failure;
        facts.recovery_at = cut.recovery;
    }
    facts
}
