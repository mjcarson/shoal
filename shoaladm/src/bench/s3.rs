//! `shoaladm bench s3`: another store's S3 endpoints driven by the bench's own object driver
//! ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! The same spec, the same arms in the same order, the same seeded bytes, windows, read back and
//! capture as `shoaladm bench run` makes of a Shoal cluster, so the two captures are read side by
//! side. What differs is only what the bench cannot do to a store it did not deploy: it neither
//! deploys nor resets the store, so every run deletes what its arm put ([`super::between`]), and
//! the hosts' counters follow the devices and processes it is told of rather than an inventory's.
//! Host preparation - a governor, a unit stopped - is the lab script's, as it is for every store.
//!
//! # Invariants
//!
//! **The keys come from the environment and go nowhere else**: never into the spec, the capture
//! or the log. **A run starts on a bucket of its own**: one that exists is refused unless it is
//! named as existing, since what a bucket already holds changes what a store does.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::Args;
use color_eyre::eyre::{bail, eyre};
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::driver::ArmClock;
use shoal_loadgen::objects::s3::{S3Endpoint, S3Settings};
use shoal_loadgen::objects::sigv4::{Credentials, Signer, UNSIGNED_PAYLOAD};
use shoal_loadgen::objects::{ObjectDriver, ObjectMix, ObjectPreload};
use shoal_loadgen::progress::{BenchEvent, Control, Phase, Progress};
use shoal_loadgen::results::{
    Capture, DatasetFacts, NodeFacts, Provenance, SchemaFacts, StoreFacts, FORMAT,
};
use shoal_loadgen::spec::{BenchSpec, BetweenArms, EventKind};

use super::args::{byte_sizes, list, object_mixes, refuse, List, RootArg};
use super::devices::{self, HostRoots};
use super::orchestrate::{devices_unread_line, result, Sampled};
use super::provenance::{self, rfc3339};
use super::{between, headless, hosts, store};
use crate::cli::ProjectArgs;
use crate::deploy::remote::Host;
use shoal::shared::pools::ByteSize;

/// How many progress events may wait for the screen before one is dropped
const PROGRESS_DEPTH: usize = 4096;

/// The environment variables a store's key is read from
const KEY_VARIABLES: [&str; 2] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"];

/// The flags of `shoaladm bench s3`
#[derive(Args, Debug, Clone, Default)]
pub struct BenchS3Args {
    /// A spec file, which the flags given are laid over
    #[clap(long)]
    pub spec: Option<PathBuf>,
    /// The dataset folder: one `<Bucket>.yaml` description a bucket
    #[clap(long)]
    pub dataset: Option<PathBuf>,
    /// The object mixes: put100, get100, rw50, stat100, or get:N,put:M,stat:K; writes in place
    /// are refused, since S3 has none
    #[clap(long, value_parser = object_mixes)]
    pub objects: Option<List<ObjectMix>>,
    /// The object sizes each mix runs at, comma separated, as 4KiB or 1MiB
    #[clap(long, value_parser = byte_sizes)]
    pub object_sizes: Option<List<ByteSize>>,
    /// How many operations an object arm keeps in flight, comma separated
    #[clap(long, value_parser = list::<usize>)]
    pub concurrency: Option<List<usize>>,
    /// Keep only the object arms whose ids start with one of these, comma separated
    #[clap(long, value_parser = list::<String>)]
    pub only: Option<List<String>>,
    /// How long each arm is measured, in seconds
    #[clap(long)]
    pub duration: Option<u64>,
    /// How long each arm runs before it is measured, in seconds
    #[clap(long)]
    pub warmup: Option<u64>,
    /// How many times each arm is run
    #[clap(long)]
    pub runs: Option<u32>,
    /// The seed every choice is drawn from
    #[clap(long)]
    pub seed: Option<u64>,
    /// How many of each description's objects are put before any arm: a count, or a percent
    #[clap(long)]
    pub preload: Option<shoal_loadgen::feed::Preload>,
    /// Do not read back a sample of what each arm put, nor the preload at the end
    #[clap(long)]
    pub no_verify_acks: bool,
    /// Drop every host's page cache before each run
    #[clap(long)]
    pub drop_caches: bool,
    /// The store's S3 endpoints, comma separated, as http://host:port; the driver's streams are
    /// spread over them in this order
    #[clap(long, value_parser = list::<String>)]
    pub endpoints: Option<List<String>>,
    /// The region every request is signed for
    #[clap(long, default_value = "us-east-1")]
    pub region: String,
    /// The store's bucket for a dataset's, as `Posters=posters`; repeatable, and each dataset
    /// bucket's own name in lower case where none is given
    #[clap(long = "bucket")]
    pub buckets: Vec<String>,
    /// Where the store places a bucket it makes, as its CreateBucket names it, such as Ceph's
    /// `default:ec21`
    #[clap(long)]
    pub location_constraint: Option<String>,
    /// Run in a bucket that is there already, rather than refusing one
    #[clap(long)]
    pub existing_bucket: bool,
    /// What the store is, by a name of one word: ceph, minio, rustfs, seaweedfs
    #[clap(long)]
    pub system: String,
    /// The version deployed
    #[clap(long)]
    pub version: String,
    /// How the store keeps a bucket's objects, as rep3 or ec21
    #[clap(long)]
    pub redundancy: String,
    /// Anything else to keep on record, as `name=value`; repeatable
    #[clap(long = "note")]
    pub notes: Vec<String>,
    /// A host the store runs on, as ssh reaches it; repeatable
    #[clap(long = "host")]
    pub hosts: Vec<String>,
    /// A store device on a host, as `host:path`; repeatable. A path that is a block device is
    /// counted as itself, a directory as the device under it
    #[clap(long = "device")]
    pub devices: Vec<String>,
    /// A name the store's processes run under, their program's file name; repeatable
    #[clap(long = "process")]
    pub processes: Vec<String>,
    /// A command run locally under `sh -c` after each cleanup until the store has given back
    /// what was deleted
    #[clap(long)]
    pub settle: Option<String>,
    /// Refuse to start an arm while a device's filesystem is fuller than this, in percent
    #[clap(long)]
    pub max_used: Option<f64>,
    /// The longest one request may take, in seconds
    #[clap(long, default_value_t = 120)]
    pub timeout: u64,
    /// The capture's name; the time and commit if not given
    #[clap(long)]
    pub label: Option<String>,
    /// Where the capture goes
    #[clap(long)]
    pub out: Option<PathBuf>,
    /// Where captures are kept
    #[clap(flatten)]
    pub root: RootArg,
    /// Replace a capture of the same label
    #[clap(long)]
    pub overwrite: bool,
    /// Measure with uncommitted changes, recording that it was
    #[clap(long)]
    pub allow_dirty: bool,
    /// Print the plan and every refusal, and touch nothing
    #[clap(long)]
    pub dry_run: bool,
}

impl BenchS3Args {
    /// The spec this run asks for: the file's, with every flag given laid over it, objects
    /// deleted between arms and no event, as any store the bench did not deploy is run
    ///
    /// # Errors
    ///
    /// When the spec file cannot be read or does not parse.
    pub fn spec(&self) -> color_eyre::Result<BenchSpec> {
        // the file, or every default
        let mut spec = match &self.spec {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|error| eyre!("{}: {error}", path.display()))?;
                serde_yaml::from_str(&text).map_err(|error| eyre!("{}: {error}", path.display()))?
            }
            None => BenchSpec::default(),
        };
        // no table, no event, and every arm's objects deleted after it
        spec.workloads.clear();
        spec.events = vec![EventKind::None];
        spec.between_arms = BetweenArms::Delete;
        // every flag that was given, as `bench run` lays them
        if let Some(dataset) = &self.dataset {
            spec.dataset.clone_from(dataset);
        }
        if let Some(List(objects)) = &self.objects {
            spec.objects.clone_from(objects);
        }
        if let Some(List(sizes)) = &self.object_sizes {
            spec.object_sizes.clone_from(sizes);
        }
        if let Some(List(concurrency)) = &self.concurrency {
            spec.concurrency.clone_from(concurrency);
        }
        if let Some(List(only)) = &self.only {
            spec.object_only.clone_from(only);
        }
        macro_rules! take {
            ($($field:ident),*) => {
                $(if let Some(value) = &self.$field { spec.$field = value.clone(); })*
            };
        }
        take!(duration, warmup, runs, seed, preload);
        if self.no_verify_acks {
            spec.verify_acks = false;
        }
        spec.drop_caches |= self.drop_caches;
        Ok(spec)
    }

    /// Every problem the flags have, beside the spec's own
    ///
    /// # Arguments
    ///
    /// * `spec` - The spec they resolve to
    #[must_use]
    pub fn problems(&self, spec: &BenchSpec) -> Vec<String> {
        // the spec's own, then what an S3 store cannot do
        let mut problems = spec.problems();
        if let Some(mix) = spec.objects.iter().find(|mix| mix.patch > 0) {
            problems.push(format!("{} writes objects in place, which no S3 operation does", mix.name));
        }
        // a read level is a Shoal read's, which S3 cannot name, and a capture must not claim one (F95)
        if spec.read_level != shoal_loadgen::spec::ReadLevel::Default {
            problems.push(format!(
                "a read level of {:?} is a Shoal read's, which no S3 operation names",
                spec.read_level
            ));
        }
        if self.endpoints.as_ref().is_none_or(|List(endpoints)| endpoints.is_empty()) {
            problems.push("no --endpoints were given: the store's S3 endpoints, as http://host:port".to_string());
        }
        for (flag, value) in [("--system", &self.system), ("--version", &self.version), ("--redundancy", &self.redundancy)] {
            if value.is_empty() || value.contains('/') || value.contains(char::is_whitespace) {
                problems.push(format!("{flag} must be one word with no '/': it names the capture's store"));
            }
        }
        for bucket in &self.buckets {
            if bucket.split_once('=').is_none_or(|(ours, theirs)| ours.is_empty() || theirs.is_empty()) {
                problems.push(format!("--bucket {bucket} is not <dataset bucket>=<store bucket>"));
            }
        }
        for device in &self.devices {
            match device.split_once(':') {
                Some((host, path)) if self.hosts.iter().any(|known| known == host) && path.starts_with('/') => {}
                _ => problems.push(format!("--device {device} is not <host>:<absolute path> of a --host")),
            }
        }
        for note in &self.notes {
            if note.split_once('=').is_none() {
                problems.push(format!("--note {note} is not name=value"));
            }
        }
        problems
    }

    /// The store's bucket for each dataset bucket the flags name
    #[must_use]
    pub fn bucket_map(&self) -> BTreeMap<String, String> {
        self.buckets
            .iter()
            .filter_map(|bucket| bucket.split_once('='))
            .map(|(ours, theirs)| (ours.to_string(), theirs.to_string()))
            .collect()
    }

    /// Every host with its store devices and the store's process names, for the counters
    #[must_use]
    pub fn host_roots(&self) -> Vec<HostRoots> {
        // each host in the order given, its devices labelled as a node's pool devices are
        self.hosts
            .iter()
            .map(|host| HostRoots {
                target: host.clone(),
                nodes: vec![host.clone()],
                roots: self
                    .devices
                    .iter()
                    .filter_map(|device| device.split_once(':'))
                    .filter(|(on, _)| on == host)
                    .map(|(_, path)| (format!("{host} device {path}"), path.to_string()))
                    .collect(),
                processes: self.processes.clone(),
            })
            .collect()
    }
}

/// The store's key, from the environment
///
/// # Errors
///
/// When either variable is not set.
fn credentials() -> color_eyre::Result<Credentials> {
    // both, or neither is any use
    let read = |name: &str| std::env::var(name).map_err(|_| eyre!("{name} is not set: the store's key is read from the environment"));
    Ok(Credentials {
        access_key: read(KEY_VARIABLES[0])?,
        secret_key: read(KEY_VARIABLES[1])?,
    })
}

/// Run `shoaladm bench s3`
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `args` - The flags
///
/// # Errors
///
/// When the run is refused or fails; the capture holds what was measured before that.
pub async fn run(project: &ProjectArgs, args: BenchS3Args) -> color_eyre::Result<()> {
    // the spec, every problem refused at once
    let mut spec = args.spec()?;
    refuse(&args.problems(&spec))?;
    let dataset = Dataset::open_described(&spec.dataset).map_err(|refusals| eyre!("{refusals}"))?;
    if spec.objects.is_empty() {
        spec.objects = ObjectMix::defaults();
    }
    let credentials = credentials()?;
    // the bench's code, refused if it is in no commit
    let dir = project.dir()?;
    let code = provenance::code_facts(&dir);
    provenance::refuse_dirty(&code, &code, args.allow_dirty)?;
    // where the capture goes
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| format!("{}-{}-{}", args.system, args.redundancy, provenance::default_label(SystemTime::now(), &code)));
    let root = store::root(project, &args.root).unwrap_or_else(|_| dir.join("target").join("shoaladm-bench"));
    if args.dry_run {
        println!("{label}: {} arms against {:?}", spec.arms().len(), args.endpoints);
        for arm in spec.arms() {
            println!("  {} run {}", arm.id, arm.run);
        }
        return Ok(());
    }
    let capture_dir = store::capture_dir(&root, args.out.as_deref(), &label, args.overwrite)?;
    std::fs::write(capture_dir.join("spec.yml"), serde_yaml::to_string(&spec)?)?;
    let log = std::fs::File::create(capture_dir.join("log.txt"))?;
    // the run on a task of its own, its lines printed here
    let (tx, rx) = tokio::sync::mpsc::channel(PROGRESS_DEPTH);
    let (control_tx, control_rx) = tokio::sync::watch::channel(Control::Run);
    let progress = Progress::new(tx);
    let runner = {
        let progress = progress.clone();
        let capture_dir = capture_dir.clone();
        tokio::spawn(async move {
            let result = drive(args, spec, dataset, credentials, code, label, &capture_dir, &progress, control_rx).await;
            let finished = result.as_ref().map(|()| capture_dir.clone()).map_err(|error| format!("{error:#}"));
            progress.send_wait(BenchEvent::Finished(finished)).await;
            result
        })
    };
    drop(progress);
    headless::print(rx, control_tx, Some(log)).await;
    match runner.await.map_err(|error| eyre!("the run panicked: {error}"))? {
        Ok(()) => {
            println!("the capture is in {}", capture_dir.display());
            Ok(())
        }
        Err(error) => Err(eyre!("{error:#}\nwhat was measured before it stopped is in {}", capture_dir.display())),
    }
}

/// Make the bucket, run every arm, and record each in the capture
///
/// # Arguments
///
/// * `args` - The flags
/// * `spec` - The spec
/// * `dataset` - The bucket descriptions
/// * `credentials` - The store's key
/// * `code` - The bench's code
/// * `label` - The capture's label
/// * `dir` - The capture's directory
/// * `progress` - Where what it is doing is sent
/// * `control` - What it is told
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn drive(
    args: BenchS3Args,
    spec: BenchSpec,
    dataset: Dataset,
    credentials: Credentials,
    code: shoal_loadgen::results::CodeFacts,
    label: String,
    dir: &std::path::Path,
    progress: &Progress,
    control: tokio::sync::watch::Receiver<Control>,
) -> color_eyre::Result<()> {
    let endpoints: Vec<String> = args.endpoints.clone().map(|List(endpoints)| endpoints).unwrap_or_default();
    let buckets = args.bucket_map();
    // every dataset bucket's store bucket, by its name or its own name lower case
    let store_buckets: BTreeMap<String, String> = dataset
        .objects
        .iter()
        .map(|object| {
            let theirs = buckets.get(&object.bucket).cloned().unwrap_or_else(|| object.bucket.to_ascii_lowercase());
            (object.bucket.clone(), theirs)
        })
        .collect();
    // every host's facts, off the runtime since ssh blocks
    progress.send(BenchEvent::Phase(Phase::Scan));
    let probed = {
        let hosts = args.hosts.clone();
        tokio::task::spawn_blocking(move || {
            hosts
                .iter()
                .map(|target| hosts::probe(&Host { target: target.clone() }).map(|facts| (target.clone(), facts)))
                .collect::<color_eyre::Result<Vec<_>>>()
        })
        .await
        .map_err(|error| eyre!("probing the hosts panicked: {error}"))??
    };
    let driver_facts = hosts::local_facts().unwrap_or_default();
    let nodes: BTreeMap<String, NodeFacts> = probed
        .into_iter()
        .map(|(target, facts)| {
            (
                target,
                NodeFacts {
                    governor_ran: Some(facts.governor.clone()),
                    host: facts,
                    target_cpu: None,
                    program_sha256: None,
                },
            )
        })
        .collect();
    let driver_shares_host = nodes.values().any(|node| node.host.hostname == driver_facts.hostname);
    // the capture, written before anything else so a run that dies early still says why
    let mut capture = Capture {
        format: FORMAT,
        label: label.clone(),
        provenance: Provenance {
            tool: env!("CARGO_PKG_VERSION").to_string(),
            started_at: rfc3339(SystemTime::now()),
            finished_at: None,
            mode: None,
            flavor: "s3".to_string(),
            project: code.clone(),
            shoal: code,
            rustc: provenance::rustc(),
            rustflags: std::env::var("RUSTFLAGS").ok(),
            schema: SchemaFacts::default(),
            driver: driver_facts,
            nodes,
            driver_shares_host,
            store: Some(StoreFacts {
                system: args.system.clone(),
                version: args.version.clone(),
                redundancy: args.redundancy.clone(),
                endpoints: endpoints.clone(),
                signing: format!("SigV4, {UNSIGNED_PAYLOAD}, path style, HTTP/1.1 plaintext"),
                region: args.region.clone(),
                buckets: store_buckets.clone(),
                location_constraint: args.location_constraint.clone(),
                processes: args.processes.clone(),
                notes: args
                    .notes
                    .iter()
                    .filter_map(|note| note.split_once('='))
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
            }),
            ..Provenance::default()
        },
        spec_digest: spec.digest(),
        spec: spec.clone(),
        dataset: DatasetFacts::new(Vec::new()).with_objects(dataset.objects.clone()),
        preload: None,
        arms: Vec::new(),
        complete: false,
        error: None,
        verify_end: None,
    };
    capture.write(dir)?;
    let result = run_arms(&args, &spec, &dataset, credentials, &endpoints, &store_buckets, &mut capture, dir, progress, &control).await;
    // the capture, finished either way
    capture.provenance.finished_at = Some(rfc3339(SystemTime::now()));
    if let Err(error) = &result {
        capture.error = Some(format!("{error:#}"));
    }
    capture.write(dir)?;
    result
}

/// Make each bucket, then run every arm of the spec against the endpoints
///
/// # Arguments
///
/// * `args` - The flags
/// * `spec` - The spec
/// * `dataset` - The bucket descriptions
/// * `credentials` - The store's key
/// * `endpoints` - The store's endpoints, in order
/// * `store_buckets` - The store's bucket for each dataset bucket
/// * `capture` - The capture being written
/// * `dir` - Its directory
/// * `progress` - Where what it is doing is sent
/// * `control` - What it is told
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_arms(
    args: &BenchS3Args,
    spec: &BenchSpec,
    dataset: &Dataset,
    credentials: Credentials,
    endpoints: &[String],
    store_buckets: &BTreeMap<String, String>,
    capture: &mut Capture,
    dir: &std::path::Path,
    progress: &Progress,
    control: &tokio::sync::watch::Receiver<Control>,
) -> color_eyre::Result<()> {
    // one endpoint each, with a client of its own
    let signer = Arc::new(Signer::new(credentials, args.region.clone()));
    let settings = S3Settings {
        timeout: Duration::from_secs(args.timeout),
        ..S3Settings::default()
    };
    let clients = endpoints
        .iter()
        .map(|endpoint| S3Endpoint::new(endpoint, signer.clone(), store_buckets.clone(), settings).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| eyre!(error))?;
    // each bucket made through the first endpoint, then heard of through every one
    progress.send(BenchEvent::Phase(Phase::Bootstrap));
    for bucket in store_buckets.values() {
        match clients[0].create_bucket(bucket, args.location_constraint.as_deref()).await {
            Ok(()) => progress.log(format!("made bucket {bucket}")),
            Err(error) if args.existing_bucket && error.code.starts_with("409") => {
                progress.log(format!("running in bucket {bucket}, which was there already"));
            }
            Err(error) => {
                bail!("bucket {bucket}: {} ({})", error.code, error.message);
            }
        }
        for client in &clients {
            client
                .head_bucket(bucket)
                .await
                .map_err(|error| eyre!("bucket {bucket} through {}: {} ({})", client.base(), error.code, error.message))?;
        }
    }
    // a count is that many objects of each description, a share a share of them
    let preload = ObjectPreload::from(spec.preload);
    // and a bucket's own, where the spec names one (F104)
    let preloads = spec
        .preload_files
        .iter()
        .map(|(bucket, preload)| (bucket.clone(), ObjectPreload::from(*preload)))
        .collect();
    let objects = ObjectDriver::new(clients, dataset.objects.clone(), preload).preloading(preloads);
    let hosts = args.host_roots();
    let arms = spec.arms();
    progress.send(BenchEvent::Planned {
        label: capture.label.clone(),
        arms: arms.clone(),
    });
    let warmup = Duration::from_secs(spec.warmup);
    let duration = Duration::from_secs(spec.duration);
    let told = std::sync::atomic::AtomicBool::new(false);
    for (index, arm) in arms.iter().enumerate() {
        if *control.borrow() == Control::Abort {
            bail!("aborted before {} run {}", arm.id, arm.run);
        }
        let object = arm.object.as_ref().ok_or_else(|| eyre!("{} is not an object arm", arm.id))?;
        if let Some(refusal) = objects.refuses(object) {
            bail!("{}: {refusal}", arm.id);
        }
        // the size's preload, before the arm's clock starts
        if let Some((seconds, took)) = objects.preload(object.size, object.concurrency.max(8), progress).await {
            let summary = shoal_loadgen::window::Window::sum(&seconds).summary(took);
            let failed: u64 = summary.every_kind().map(|(_, kind)| kind.failed()).sum();
            if failed > 0 {
                bail!("the object preload at {} failed {failed} puts: {:?}", shoal_loadgen::objects::size_label(object.size), summary.kinds);
            }
            let ok: u64 = summary.every_kind().map(|(_, kind)| kind.ok).sum();
            progress.log(format!(
                "preloaded {ok} objects at {} in {:.1}s",
                shoal_loadgen::objects::size_label(object.size),
                took.as_secs_f64()
            ));
        }
        // the hosts readied and read, then the arm on its clock
        let hosts_given = (!hosts.is_empty()).then_some(hosts.as_slice());
        let before = between::before_run(hosts_given, spec, args.max_used).await?;
        progress.send(BenchEvent::ArmStarted {
            index,
            arm: arm.clone(),
            warmup: spec.warmup,
            duration: spec.duration,
        });
        let started_at = rfc3339(SystemTime::now());
        let clock = ArmClock::start(warmup + duration);
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
        progress.send(BenchEvent::Phase(if spec.warmup > 0 { Phase::Warmup } else { Phase::Measure }));
        let outcome = objects
            .run_arm(object, warmup, duration, spec.seed, &format!("{}/{}", arm.id, arm.run), clock, progress)
            .await;
        watcher.abort();
        // the hosts again once the last answer is in, before the read back
        let after = match hosts_given {
            Some(hosts) => devices::read_all(hosts).await,
            None => Err("no --host was given, so no host's devices can be read".to_string()),
        };
        let (counted, unread) = match (before, after) {
            (Ok(before), Ok(after)) => (devices::deltas(&hosts, &before, &after), None),
            (Err(error), _) | (_, Err(error)) => {
                if !told.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    progress.log(devices_unread_line(&error));
                }
                (Vec::new(), Some(error))
            }
        };
        let wrapped = objects.took_wrap();
        // a sample of what it put read back, all of it deleted, and the store settled
        let (verify, cleanup) = between::after_object_arm(&objects, object, spec, args.settle.as_deref(), progress).await;
        let sampled = Sampled {
            unread: Some("an S3 store keeps no figures the bench reads".to_string()),
            ..Sampled::default()
        };
        let mut run = result(spec, arm, index, started_at, outcome, None, verify.clone(), sampled, progress);
        run.devices = counted;
        run.devices_unread = unread;
        run.cleanup = Some(cleanup);
        run.wrapped |= wrapped;
        progress.send(BenchEvent::ArmDone {
            index,
            summary: run.measured.clone(),
            ended_early: run.ended_early.as_ref().map(|ended| ended.reason.clone()),
        });
        for line in store::run_notes(&run) {
            progress.log(format!("{} run {}: {line}", arm.id, arm.run));
        }
        capture.arm_mut(arm).runs.push(run);
        capture.write(dir)?;
        // an acknowledged object lost or read back wrong fails the run, once its record is kept
        if let Some(verify) = verify.filter(|verify| verify.lost > 0 || verify.misread > 0 || !verify.errors.is_empty()) {
            bail!(
                "{} run {}: of {} acknowledged objects read back, {} were lost, {} held other bytes than were put, \
                 and {:?} could not be read",
                arm.id, arm.run, verify.checked, verify.lost, verify.misread, verify.errors
            );
        }
    }
    // the whole preload read back once, at the end
    if spec.verify_acks {
        let verify = objects.verify(between::CLEANUP_CONCURRENCY, progress).await;
        let failed = verify.lost > 0 || verify.misread > 0 || !verify.errors.is_empty();
        capture.verify_end = Some(verify.clone());
        capture.write(dir)?;
        if failed {
            bail!(
                "of the {} preloaded objects read back at the end, {} were lost, {} held other bytes than were put, \
                 and {:?} could not be read",
                verify.checked, verify.lost, verify.misread, verify.errors
            );
        }
    }
    capture.complete = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::BenchS3Args;
    use crate::bench::args::List;

    /// The flags of a run as the lab script gives them
    fn args() -> BenchS3Args {
        BenchS3Args {
            dataset: Some("dataset".into()),
            objects: Some(List(vec!["put100".parse().unwrap(), "get100".parse().unwrap()])),
            object_sizes: Some(List(vec![shoal::shared::pools::ByteSize(4096)])),
            concurrency: Some(List(vec![1, 16])),
            preload: Some(shoal_loadgen::feed::Preload::Rows(1024)),
            duration: Some(30),
            warmup: Some(5),
            runs: Some(4),
            drop_caches: true,
            endpoints: Some(List(vec!["http://172.16.2.10:9000".to_string()])),
            system: "minio".to_string(),
            version: "RELEASE.2025".to_string(),
            redundancy: "ec21".to_string(),
            hosts: vec!["titan".to_string()],
            devices: vec!["titan:/srv/stores".to_string()],
            // what clap gives these when they are not named
            region: "us-east-1".to_string(),
            timeout: 120,
            ..BenchS3Args::default()
        }
    }

    /// `bench s3` and `bench run` given the same flags make one spec, digest for digest, which
    /// is what lets a capture of each be read beside the other (F94)
    #[test]
    fn s3_and_run_make_one_spec_digest() {
        use clap::Parser;
        let s3 = args().spec().unwrap();
        let cli = crate::cli::Cli::try_parse_from([
            "shoaladm", "bench", "run", "--dataset", "dataset", "--objects", "put100,get100", "--object-sizes", "4KiB",
            "--concurrency", "1,16", "--preload", "1024", "--duration", "30", "--warmup", "5", "--runs", "4",
            "--drop-caches", "--between-arms", "delete",
        ])
        .unwrap();
        let crate::cli::Command::Bench(crate::bench::args::BenchCommand::Run(run)) = cli.command else {
            panic!("a run");
        };
        let mut shoal = run.spec().unwrap();
        // an object-only dataset clears the table workloads in `bench run` too
        shoal.workloads.clear();
        assert_eq!(s3.digest(), shoal.digest());
        assert_eq!(s3.arms(), shoal.arms());
    }

    /// A write in place, an endpoint missing, a store named in two words and a device on no
    /// host are each refused by name
    #[test]
    fn patch_and_bad_names_are_refused_by_name() {
        let mut bad = args();
        bad.objects = Some(List(vec!["patch100".parse().unwrap()]));
        bad.endpoints = None;
        bad.system = "two words".to_string();
        bad.devices.push("europa:/srv/stores".to_string());
        bad.buckets.push("Posters".to_string());
        let problems = bad.problems(&bad.spec().unwrap()).join("\n");
        for expected in ["writes objects in place", "no --endpoints", "--system must be one word", "--device europa", "--bucket Posters"] {
            assert!(problems.contains(expected), "{expected}: {problems}");
        }
        assert!(args().problems(&args().spec().unwrap()).is_empty(), "{:?}", args().problems(&args().spec().unwrap()));
    }

    /// A spec that reads at a level, which only a Shoal read can, is refused by name, so no S3
    /// capture claims a level it never asked for (F95)
    #[test]
    fn a_read_level_is_refused_by_name() {
        for level in [shoal_loadgen::spec::ReadLevel::One, shoal_loadgen::spec::ReadLevel::Quorum] {
            let mut spec = args().spec().unwrap();
            spec.read_level = level;
            let problems = args().problems(&spec).join("\n");
            assert!(problems.contains("read level"), "{level:?}: {problems}");
        }
    }

    /// A run against a store on a local port makes its bucket, preloads, runs every arm with
    /// its objects deleted after it, reads the preload back at the end, and writes a capture
    /// that says all of it, with no host to count and no key in it (F94)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_against_local_servers_writes_a_capture() {
        use shoal_loadgen::objects::s3_local::LocalStore;
        use shoal_loadgen::objects::sigv4::{Credentials, Signer};
        use std::sync::Arc;
        let credentials = Credentials {
            access_key: "bench".to_string(),
            secret_key: "a secret of the test's".to_string(),
        };
        let store = LocalStore::start(Arc::new(Signer::new(credentials.clone(), "us-east-1"))).await;
        let other = LocalStore::start(Arc::new(Signer::new(credentials.clone(), "us-east-1"))).await;
        // a dataset of one bucket, and the run's flags over two endpoints of one store
        let folder = tempfile::tempdir().unwrap();
        std::fs::write(folder.path().join("Posters.yaml"), "objects: 16777216\nsizes: {fixed: 4KiB}\nseed: 13\n").unwrap();
        let mut args = args();
        args.dataset = Some(folder.path().to_path_buf());
        args.objects = Some(List(vec!["put100".parse().unwrap(), "get100".parse().unwrap()]));
        args.concurrency = Some(List(vec![1, 2]));
        args.preload = Some(shoal_loadgen::feed::Preload::Rows(6));
        args.duration = Some(1);
        args.warmup = Some(0);
        args.runs = Some(2);
        args.drop_caches = false;
        args.hosts.clear();
        args.devices.clear();
        args.endpoints = Some(List(vec![store.address.clone()]));
        let spec = args.spec().unwrap();
        let dataset = shoal_loadgen::dataset::Dataset::open_described(folder.path()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4096);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let (_control_tx, control_rx) = tokio::sync::watch::channel(shoal_loadgen::progress::Control::Run);
        let progress = shoal_loadgen::progress::Progress::new(tx);
        super::drive(
            args,
            spec,
            dataset,
            credentials,
            shoal_loadgen::results::CodeFacts::default(),
            "local".to_string(),
            dir.path(),
            &progress,
            control_rx,
        )
        .await
        .unwrap();
        drop(progress);
        let _ = drain.await;
        let capture = shoal_loadgen::results::Capture::read(dir.path()).unwrap();
        // every arm ran twice, and each put arm's objects were deleted after it
        assert!(capture.complete, "{:?}", capture.error);
        assert_eq!(capture.arms.len(), 4);
        for arm in &capture.arms {
            assert_eq!(arm.runs.len(), 2, "{}", arm.id);
            for run in &arm.runs {
                let cleanup = run.cleanup.as_ref().expect("a cleanup after every run");
                assert!(cleanup.errors.is_empty(), "{cleanup:?}");
                assert_eq!(run.figures_unread.as_deref(), Some("an S3 store keeps no figures the bench reads"));
                assert!(run.devices_unread.is_some(), "{:?}", run.devices_unread);
                assert!(!run.wrapped);
                assert!(run.measured.kinds.values().all(|kind| kind.ok > 0 && kind.failed() == 0), "{}: {:?}", arm.id, run.measured.kinds);
            }
            if arm.id.0.starts_with("put100") {
                assert!(arm.runs.iter().all(|run| run.verify.as_ref().is_some_and(|verify| verify.checked > 0 && verify.lost == 0)));
            }
        }
        // the preload, and nothing else, was read back at the end
        let end = capture.verify_end.expect("the preload read back");
        assert_eq!((end.checked, end.lost, end.misread), (6, 0, 0));
        // the store is named, its endpoints in order, and no key is anywhere in the capture
        let store_facts = capture.provenance.store.as_ref().expect("the store");
        assert_eq!((store_facts.system.as_str(), store_facts.redundancy.as_str()), ("minio", "ec21"));
        let text = std::fs::read_to_string(dir.path().join("bench.json")).unwrap();
        assert!(!text.contains("a secret of the test's"));
        // and the second store, which nothing was run against, saw nothing
        assert!(other.requests().is_empty());
        assert!(store.requests().iter().any(|request| request == "PUT /posters"), "{:?}", &store.requests()[..3]);
    }

    /// The keys are read from the environment alone and appear in no spec, so in no capture
    #[test]
    fn keys_never_reach_the_capture() {
        let spec = args().spec().unwrap();
        let written = serde_yaml::to_string(&spec).unwrap() + &serde_json::to_string(&spec).unwrap();
        assert!(!written.to_ascii_lowercase().contains("secret") && !written.contains("AWS_"), "{written}");
        // and a host's devices are labelled as a node's pool devices are
        let hosts = args().host_roots();
        assert_eq!(hosts[0].roots, vec![("titan device /srv/stores".to_string(), "/srv/stores".to_string())]);
    }
}
