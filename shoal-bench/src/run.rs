//! Capturing a run and storing it with its provenance
//!
//! This is `scripts/bench.sh`, with four things it did not have: a filter, a record of what the
//! capture was taken on, a guard on the recursive delete, and the guarantee that the restore
//! happens even when a phase fails.

pub mod exec;
pub mod plan;
pub mod storage;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::{Context, Result, bail};

use crate::cli::RunArgs;
use crate::collect;
use crate::fingerprint::{self, RealFacts};
use crate::model::meta::{CaptureMeta, LayerRecord, META_VERSION};
use crate::registry::{Layer, Registry};
use crate::run::plan::{Phase, Plan, PlanInputs, Step};
use crate::store::Store;

/// Runs `shoal-bench run`
///
/// # Arguments
///
/// * `store` - The artifact tree to work in
/// * `args` - What the caller asked for
pub fn run_capture(store: &Store, args: &RunArgs) -> Result<i32> {
    // work out what was selected before anything is built, so a bad filter costs nothing
    let registry = Registry::load(store, false)?;
    let selected = registry.select(&args.selection)?;
    let layers: Vec<Layer> = Layer::ALL
        .into_iter()
        .filter(|layer| selected.iter().any(|entry| entry.layer == *layer))
        .collect();
    // the criterion filter is built from the resolved id set rather than from what was typed, so
    // that both halves of the command agree on what was selected
    let all_micro = registry.micro_ids();
    let picked_micro: Vec<&str> = selected
        .iter()
        .filter(|entry| entry.layer == Layer::Micro)
        .map(|entry| entry.id.as_str())
        .collect();
    let criterion_filter = crate::registry::criterion_filter(&picked_micro, &all_micro);
    // the workloads the macro layer should run, in the order the registry declares them
    let workloads: Vec<String> = selected
        .iter()
        .filter(|entry| entry.layer == Layer::Macro)
        .map(|entry| entry.id.clone())
        .collect();
    // and the workloads each instrumented layer should run, mapped back from the instrumented
    // identifier a filter selects on to the workload the runner has to invoke. kept per layer
    // rather than unioned: the two profile different sets, and a union would have run the stage
    // layer's three widths under the hotpath layer as well
    let instrumented: std::collections::BTreeMap<Layer, Vec<String>> =
        [Layer::Hotpath, Layer::Stages]
            .into_iter()
            .filter(|layer| layers.contains(layer))
            .map(|layer| {
                (
                    layer,
                    registry
                        .instrumented_workloads(layer)
                        .into_iter()
                        .map(String::from)
                        .collect(),
                )
            })
            .collect();
    let partial = selected.len() != registry.len();
    // refuse a dirty tree unless the caller has accepted it. every capture in this tree today was
    // taken on one, so this will bite immediately - which is the point: a measurement of bytes
    // that exist in no commit cannot be located in history.
    let facts = RealFacts::new(store.root());
    let fingerprint = fingerprint::current(store, &facts, args.allow_dirty)?;
    if fingerprint.code.dirty && !args.allow_dirty && !args.dry_run {
        bail!(
            "the working tree has {} uncommitted paths, so this capture could not be located in \
             history afterwards. Commit first, or pass --allow-dirty and have it recorded.",
            fingerprint.code.dirty_paths
        );
    }
    // where everything is going
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| store.runs_dir());
    let conf = if args.conf.is_absolute() {
        args.conf.clone()
    } else {
        store.root().join(&args.conf)
    };
    // the storage directories the workload writes into, which are the ones that get emptied
    let storage_dirs = if layers.iter().any(|layer| *layer != Layer::Micro) {
        storage::storage_dirs(&conf)?
    } else {
        Vec::new()
    };
    // scratch space for the per run results, which are folded and then thrown away
    let scratch = store.root().join("target/shoal-bench/scratch");
    let inputs = PlanInputs {
        root: store.root().to_path_buf(),
        label: args.label.clone(),
        layers: layers.clone(),
        criterion_filter,
        workloads,
        instrumented,
        runs: args.runs,
        conf,
        seed: args.seed,
        scale: args.scale.as_str().to_string(),
        out: out.clone(),
        scratch: scratch.clone(),
        storage: storage_dirs.clone(),
        keep_criterion: args.keep_criterion,
        no_restore: args.no_restore,
    };
    let plan = plan::build_plan(&inputs);
    // a dry run is the same plan, printed instead of executed
    if args.dry_run {
        print!("{}", plan.display());
        println!(
            "\n{} of {} benchmarks selected across {} layer(s){}",
            selected.len(),
            registry.len(),
            layers.len(),
            if partial { ", a partial capture" } else { "" }
        );
        return Ok(0);
    }
    // check every directory before running anything, so a capture cannot fail three phases in
    // because of a path it could have rejected at the start
    for dir in &storage_dirs {
        storage::check_wipeable(dir, args.force_wipe)?;
    }
    std::fs::create_dir_all(&scratch)
        .with_context(|| format!("creating {}", scratch.display()))?;
    // anything criterion wrote before this instant is left over from an earlier capture
    let started = SystemTime::now();
    // execute, keeping the restore for afterwards whatever happens
    let outcome = execute(&plan, &inputs, args, started);
    if let Some(restore) = &plan.restore {
        println!("\n=== restoring the uninstrumented build ===");
        // a restore that fails is reported and does not mask the failure that got us here
        if let Err(err) = exec::run(restore) {
            eprintln!("warning: could not restore the uninstrumented build: {err:#}");
        }
    }
    let records = outcome?;
    // and finally the provenance, written last so that it is only ever present beside artifacts
    // that were actually produced
    let meta = CaptureMeta {
        version: META_VERSION,
        label: args.label.clone(),
        captured: crate::clock::now_rfc3339(),
        tool_version: format!("shoal-bench {}", env!("CARGO_PKG_VERSION")),
        partial,
        filter: args.selection.filters.clone(),
        exact: args.selection.exact,
        selected: selected.len(),
        registry_total: registry.len(),
        layers: records,
        code: fingerprint.code,
        env: fingerprint.env,
    };
    let meta_path = plan::meta_artifact(&inputs);
    crate::store::write_json(&meta_path, &meta)?;
    println!("\n=== done ===");
    for layer in &layers {
        println!("  {}", plan::artifact(&inputs, *layer).display());
    }
    println!("  {}", meta_path.display());
    Ok(0)
}

/// Runs every phase of a plan
///
/// # Arguments
///
/// * `plan` - What to run
/// * `inputs` - What the capture was asked for
/// * `args` - The caller's arguments, for the wipe guard
/// * `started` - When the capture began, which is what makes a criterion result fresh
fn execute(
    plan: &Plan,
    inputs: &PlanInputs,
    args: &RunArgs,
    started: SystemTime,
) -> Result<BTreeMap<Layer, LayerRecord>> {
    let mut records = BTreeMap::new();
    let total = plan.phases.len();
    for (index, phase) in plan.phases.iter().enumerate() {
        println!("\n=== [{}/{total}] {} ===", index + 1, phase.title);
        let record = run_phase(phase, inputs, args, started)?;
        // only a phase that produced a layer contributes a record
        if let (Some(layer), Some(record)) = (phase.layer, record) {
            records.insert(layer, record);
        }
    }
    Ok(records)
}

/// Runs one phase
///
/// # Arguments
///
/// * `phase` - The phase to run
/// * `inputs` - What the capture was asked for
/// * `args` - The caller's arguments, for the wipe guard
/// * `started` - When the capture began
fn run_phase(
    phase: &Phase,
    inputs: &PlanInputs,
    args: &RunArgs,
    started: SystemTime,
) -> Result<Option<LayerRecord>> {
    let mut record = None;
    // the scratch files this phase's runs wrote, per workload, in the order they were written
    let mut produced: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for step in &phase.steps {
        match step {
            Step::Command(command) => {
                // remember the result file each run was told to write, filed under the workload
                // that wrote it, so the fold below picks each workload's median from its own runs
                if let (Some(id), Some(json)) = (workload_argument(command), json_argument(command))
                {
                    produced.entry(id).or_default().push(json);
                }
                exec::run(command)?;
            }
            Step::ClearCriterion(path) => {
                // criterion keeps a directory per benchmark id forever, so a renamed or removed
                // benchmark otherwise reports its last value into every capture taken afterwards
                if path.is_dir() {
                    std::fs::remove_dir_all(path)
                        .with_context(|| format!("clearing {}", path.display()))?;
                }
            }
            Step::WipeStorage(path) => {
                storage::wipe(path, args.force_wipe)?;
            }
            Step::Collect { layer, into } => {
                record = Some(collect_layer(*layer, into, inputs, &produced, started)?);
            }
        }
    }
    Ok(record)
}

/// The result file a command was told to write, if it was told to write one
///
/// # Arguments
///
/// * `command` - The command to inspect
fn json_argument(command: &plan::CommandPlan) -> Option<PathBuf> {
    // every workload run is handed a `--json` path to write its result to
    command
        .args
        .iter()
        .position(|arg| arg == "--json")
        .and_then(|at| command.args.get(at + 1))
        .map(PathBuf::from)
}

/// The workload a command was told to run, if it was told to run one
///
/// Read back off the command rather than tracked alongside the plan, so that the plan stays a
/// plain description of what will be run and there is one place where a workload's identifier is
/// decided.
///
/// # Arguments
///
/// * `command` - The command to inspect
fn workload_argument(command: &plan::CommandPlan) -> Option<String> {
    // every workload run names itself with `--id`
    command
        .args
        .iter()
        .position(|arg| arg == "--id")
        .and_then(|at| command.args.get(at + 1))
        .cloned()
}

/// Folds one layer's output into its durable artifact
///
/// # Arguments
///
/// * `layer` - Which layer to collect
/// * `into` - Where its artifact goes
/// * `inputs` - What the capture was asked for
/// * `produced` - The scratch files this phase's runs wrote
/// * `started` - When the capture began
fn collect_layer(
    layer: Layer,
    into: &std::path::Path,
    inputs: &PlanInputs,
    produced: &BTreeMap<String, Vec<PathBuf>>,
    started: SystemTime,
) -> Result<LayerRecord> {
    // each layer is folded differently, and two of them were written by the run itself
    match layer {
        Layer::Micro => {
            let capture = collect::micro::collect(
                &collect::micro::criterion_dir(&inputs.root),
                &crate::clock::now_rfc3339(),
                Some(started),
            )?;
            let ids = capture.benchmarks.len();
            crate::store::write_json(into, &capture)?;
            println!("  {ids} benchmarks -> {}", into.display());
            Ok(LayerRecord {
                ids: Some(ids),
                complete: inputs.criterion_filter.is_none(),
                artifact: file_name(into),
                ..LayerRecord::default()
            })
        }
        Layer::Macro => {
            let folded = collect::macro_layer::collect(produced, Some(inputs.label.clone()))?;
            let workloads = folded.workloads.len();
            crate::store::write_json(into, &folded)?;
            println!("{}", collect::macro_layer::describe(into)?);
            println!(
                "  {workloads} workload(s), median of {} runs each -> {}",
                inputs.runs,
                into.display()
            );
            Ok(LayerRecord {
                ids: Some(workloads),
                runs: Some(inputs.runs),
                complete: workloads == crate::workload_ids::IDS.len(),
                artifact: file_name(into),
                ..LayerRecord::default()
            })
        }
        Layer::Hotpath => {
            // written by the run itself, so all that is left is to confirm it is a profile
            println!("  {}", collect::hotpath::check(into)?);
            Ok(LayerRecord {
                complete: true,
                artifact: file_name(into),
                ..LayerRecord::default()
            })
        }
        Layer::Stages => {
            // each run wrote its own report, because only it can join the two halves of a record.
            // what is left is to fold them into one artifact and check the joins they describe.
            // named from the plan rather than found by walking scratch: scratch is never cleared,
            // so it holds every run of every capture ever taken in this tree
            let wrote: Vec<PathBuf> = crate::run::plan::instrumented_for(inputs, Layer::Stages)
                .iter()
                .map(|id| crate::run::plan::scratch_stages(inputs, id))
                .collect();
            let reports = collect::stages::collect(&wrote, into)?;
            println!("  {}", collect::stages::check(into)?);
            println!(
                "  {} workload(s) profiled -> {}",
                reports.reports.len(),
                into.display()
            );
            Ok(LayerRecord {
                complete: true,
                artifact: file_name(into),
                join: Some(reports.join()),
                ..LayerRecord::default()
            })
        }
    }
}

/// The file name of a path, for recording in the provenance
///
/// # Arguments
///
/// * `path` - The path to name
fn file_name(path: &std::path::Path) -> String {
    // relative to the run directory, since the absolute path is a fact about this machine
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::plan::{CommandPlan, Stdout};

    /// The result file a macro run was told to write is the one that gets folded
    #[test]
    fn the_json_argument_is_found() {
        let command = CommandPlan {
            program: "tmdb".to_string(),
            args: vec![
                "--no-wait".to_string(),
                "--json".to_string(),
                "/scratch/run-1.json".to_string(),
            ],
            cwd: PathBuf::from("/repo"),
            stdout: Stdout::Capture,
            env: Vec::new(),
        };
        assert_eq!(
            json_argument(&command),
            Some(PathBuf::from("/scratch/run-1.json"))
        );
    }

    /// A command with no result file contributes nothing to the fold
    #[test]
    fn a_command_without_a_json_argument_contributes_nothing() {
        let command = CommandPlan {
            program: "cargo".to_string(),
            args: vec!["build".to_string()],
            cwd: PathBuf::from("/repo"),
            stdout: Stdout::Inherit,
            env: Vec::new(),
        };
        assert_eq!(json_argument(&command), None);
    }

    /// A `--json` with nothing after it is not a path
    #[test]
    fn a_dangling_json_flag_is_not_a_path() {
        let command = CommandPlan {
            program: "tmdb".to_string(),
            args: vec!["--json".to_string()],
            cwd: PathBuf::from("/repo"),
            stdout: Stdout::Capture,
            env: Vec::new(),
        };
        assert_eq!(json_argument(&command), None);
    }
}
