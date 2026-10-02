//! `shoaladm bench`: benchmark the project's own schema against a dataset folder ([F66](../../docs/src/features/dataset-benchmarks.md))
//!
//! Run in a project, `shoaladm bench run` is handed to the schema's admin program the way every
//! command that connects is, so the driver is compiled against the schema and nothing here
//! names a row. The driver, the dataset, the run plan and the capture are
//! [`shoal_loadgen`]'s; this module brings a cluster up for them, runs them, and puts the hosts
//! back:
//!
//! - [`args`] is the command line and how it lays over a spec file;
//! - [`owned`] copies the inventory into the bench's own cluster and keeps the two apart;
//! - [`hosts`] probes the hosts, stops what it is told it may, sets governors, guards wipes,
//!   and records every change to undo;
//! - [`events`] does things to the cluster while an arm runs;
//! - [`orchestrate`] is the run itself, on a thread and runtime of its own;
//! - [`headless`] prints a run as lines, and [`store`] lists, shows and compares captures;
//! - [`wizard`] chooses a run that names no workload, on a terminal.

pub mod args;
pub mod events;
pub mod headless;
pub mod hosts;
pub mod orchestrate;
pub mod owned;
pub mod profile;
pub mod provenance;
pub mod store;
pub mod wizard;

use color_eyre::eyre::eyre;
use rkyv::Archive;
use shoal::shared::dataset::DatasetSupport;
use shoal::shared::traits::QuerySupport;
use shoal_loadgen::dataset::Dataset;
use shoal_loadgen::progress::{BenchEvent, Control, Progress};
use shoal_loadgen::spec::{BenchSpec, EventKind, Mode};
use std::io::{IsTerminal, Write};

pub use args::{BenchCommand, BenchRunArgs, RootArg};

use crate::cli::ProjectArgs;
use orchestrate::Context;

/// How many progress events may wait for the screen before new ones are dropped
const PROGRESS_DEPTH: usize = 4096;

/// The process's stderr pointed at a file until this is dropped
struct StderrTo {
    /// A copy of the stderr there was, put back on drop
    saved: std::os::fd::OwnedFd,
}

impl StderrTo {
    /// Point stderr at a file
    ///
    /// # Arguments
    ///
    /// * `file` - The file
    ///
    /// # Errors
    ///
    /// When the descriptors cannot be duplicated.
    fn file(file: &std::fs::File) -> std::io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd};
        // SAFETY: dup of the process's own stderr returns a new descriptor or -1, checked below
        let saved = unsafe { libc::dup(2) };
        if saved < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `saved` is a descriptor this function just opened and nothing else owns
        let saved = unsafe { std::os::fd::OwnedFd::from_raw_fd(saved) };
        // SAFETY: both descriptors are open for the length of the call
        if unsafe { libc::dup2(file.as_raw_fd(), 2) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(StderrTo { saved })
    }
}

impl Drop for StderrTo {
    /// Put stderr back as it was
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        // SAFETY: the saved descriptor is open until self is dropped, after this call
        unsafe {
            libc::dup2(self.saved.as_raw_fd(), 2);
        }
    }
}

/// Print what a run would do, and touch nothing
///
/// # Arguments
///
/// * `ctx` - What was asked for
fn dry_run(ctx: &Context) {
    // the dataset, the arms in order, and what the time will at least be
    let spec = &ctx.spec;
    println!("dataset {}:", spec.dataset.display());
    for file in &ctx.dataset.files {
        println!("  {} <- {} ({})", file.table, file.path.display(), file.format.as_str());
    }
    let arms = spec.arms();
    let mut resets = 0;
    let mut disturbed = false;
    for (index, arm) in arms.iter().enumerate() {
        // the bench's own cluster is reset and preloaded again after any arm that changed it
        let reset = spec.mode == Mode::Owned && disturbed;
        resets += usize::from(reset);
        println!(
            "  [{}/{}] {} run {}{}",
            index + 1,
            arms.len(),
            arm.id,
            arm.run,
            if reset { " (after a reset and preload)" } else { "" }
        );
        disturbed = arm.disturbs();
    }
    let measured = arms.len() as u64 * (spec.warmup + spec.duration);
    println!(
        "{} arms: at least {}m{}s of arms, plus a bootstrap and {resets} resets, each with a preload",
        arms.len(),
        measured / 60,
        measured % 60
    );
    if arms.iter().any(|arm| arm.event != EventKind::None) {
        println!("event arms run until their event is done, up to {}s past their time", spec.event_timeout);
    }
    println!("the capture would be written to {}", ctx.dir.display());
}

/// Choose a run's workloads, and everything else about it, in the wizard
///
/// The wizard opens on the spec the flags left, and hands back the spec and flags to run, or
/// nothing when the operator leaves ([F67](../../docs/src/features/bench-run-wizard.md)).
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `spec` - The spec the flags and spec file left
/// * `args` - The flags
///
/// # Errors
///
/// When the project's directory cannot be found or the terminal fails.
async fn choose<S>(
    project: &ProjectArgs,
    spec: BenchSpec,
    args: BenchRunArgs,
) -> color_eyre::Result<Option<(BenchSpec, BenchRunArgs)>>
where
    S: DatasetSupport,
{
    // the dataset's tables, for the weights; a folder that is not a dataset is refused after
    let tables = Dataset::open::<S>(&spec.dataset)
        .map(|dataset| dataset.files.iter().map(|file| file.table.to_string()).collect())
        .unwrap_or_default();
    // a spec is saved beside the project by default
    let save_path = project.dir()?.join("bench.yml");
    let wizard = wizard::form::Wizard::new(spec, args, tables, save_path);
    match wizard::run(wizard).await? {
        wizard::Choice::Run(spec, args) => Ok(Some((*spec, *args))),
        wizard::Choice::Quit => Ok(None),
    }
}

/// Run a bench command for a schema
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `command` - The command
///
/// # Errors
///
/// When the run is refused, fails, or a capture cannot be read.
pub async fn run<S>(project: &ProjectArgs, command: BenchCommand) -> color_eyre::Result<()>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + Clone + 'static,
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
    // the commands that read captures run here
    let Some(BenchCommand::Run(args)) = store::run_local(project, command)? else {
        return Ok(());
    };
    let mut args = *args;
    // the spec, every problem with it refused at once
    let mut spec = args.spec()?;
    // a run that names no workload: chosen in the wizard on a terminal, the defaults otherwise
    let mut defaulted = None;
    if !args.workloads_chosen()? {
        let terminal = !args.basic && std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
        if terminal {
            match choose::<S>(project, spec, args).await? {
                Some((chosen, flags)) => (spec, args) = (chosen, flags),
                None => {
                    println!("left the wizard; nothing was run");
                    return Ok(());
                }
            }
        } else {
            // said here and in the run's log, so a capture's workloads are never a surprise
            let names: Vec<&str> = spec.workloads.iter().map(|workload| workload.name.as_str()).collect();
            let line = format!("no --workloads given: running the defaults {}", names.join(", "));
            eprintln!("{line}");
            defaulted = Some(line);
        }
    }
    args::refuse(&args.problems(&spec))?;
    let dataset = Dataset::open::<S>(&spec.dataset).map_err(|refusals| eyre!("{refusals}"))?;
    // the inventory, unless one node was named by hand
    let inventory = match &args.addr {
        Some(_) => None,
        None => Some(args.inventory.resolve(project)?),
    };
    // the code being measured, refused if it is in no commit
    let dir = project.dir()?;
    let project_facts = provenance::code_facts(&dir);
    let shoal_facts = provenance::shoal_facts(&dir);
    provenance::refuse_dirty(&project_facts, &shoal_facts, args.allow_dirty)?;
    // where the capture goes
    let started = std::time::SystemTime::now();
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| provenance::default_label(started, &project_facts));
    let root = store::root(project, &RootArg::default())?;
    let capture_dir = if args.dry_run {
        args.out.clone().unwrap_or_else(|| root.join(&label))
    } else {
        store::capture_dir(&root, args.out.as_deref(), &label, args.overwrite)?
    };
    let ctx = Context {
        project: project.clone(),
        args,
        spec,
        dataset,
        dir: capture_dir.clone(),
        label,
        inventory,
        project_facts,
        shoal_facts,
    };
    if ctx.args.dry_run {
        dry_run(&ctx);
        return Ok(());
    }
    // the spec as it ran, beside the capture
    std::fs::write(capture_dir.join("spec.yml"), serde_yaml::to_string(&ctx.spec)?)?;
    let mut log = std::fs::File::create(capture_dir.join("log.txt"))?;
    if let Some(line) = &defaulted {
        writeln!(log, "{line}")?;
    }
    // the full screen view on a terminal that did not ask for lines
    let tui = !ctx.args.basic && std::io::IsTerminal::is_terminal(&std::io::stdout());
    let title = ctx
        .inventory
        .as_ref()
        .and_then(|path| crate::deploy::Inventory::read(path).ok())
        .map_or_else(|| ctx.args.addr.clone().unwrap_or_default(), |inventory| inventory.name);
    // the run on a thread and runtime of its own, the screen here
    let (tx, rx) = tokio::sync::mpsc::channel(PROGRESS_DEPTH);
    let (control_tx, control_rx) = tokio::sync::watch::channel(Control::Run);
    let (poller_tx, poller_rx) = tokio::sync::mpsc::channel(4);
    let pollers = tui.then_some(poller_tx);
    let progress = Progress::new(tx);
    let runner = std::thread::Builder::new()
        .name("shoaladm-bench".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("bench")
                .build()?;
            runtime.block_on(async move {
                let result = orchestrate::orchestrate::<S>(ctx, progress.clone(), control_rx, pollers).await;
                // the end is the one event that is never dropped
                let finished = result.as_ref().map(Clone::clone).map_err(|error| format!("{error:#}"));
                progress.send_wait(BenchEvent::Finished(finished)).await;
                result
            })
        })?;
    if tui {
        // deploying prints its steps to stderr, which would draw over the screen: they go to
        // the run's log until the screen is gone
        let redirect = StderrTo::file(&log.try_clone()?)?;
        let drawn = crate::cluster::stats::tui::run_with::<S>(
            None,
            format!("{title} · bench"),
            std::time::Duration::from_secs(2),
            Some(crate::cluster::stats::tui::BenchChannels {
                progress: rx,
                pollers: poller_rx,
                control: control_tx,
                log: Some(log),
            }),
        )
        .await;
        drop(redirect);
        drawn?;
    } else {
        headless::print(rx, control_tx, Some(log)).await;
    }
    // the run's own answer, whatever the screen saw
    let result = runner
        .join()
        .map_err(|_| eyre!("the bench's run panicked; the hosts were put back as far as Drop could"))?;
    match result {
        Ok(dir) => {
            println!("the capture is in {}; `shoaladm bench show {}` prints it", dir.display(), dir.display());
            Ok(())
        }
        Err(error) => Err(eyre!("{error:#}\nwhat was measured before it stopped is in {}", capture_dir.display())),
    }
}
