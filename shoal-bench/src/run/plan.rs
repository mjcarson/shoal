//! What a capture is going to do, worked out before it does any of it
//!
//! Building the plan is pure: it touches no filesystem and spawns no process, so `--dry-run` is
//! the same code path as a real capture with the execution left out, and every argument this tool
//! would pass can be asserted in a unit test on any machine.
//!
//! # The order, and the one rule about it
//!
//! The phases are the five `scripts/bench.sh` ran, in the same order and for the same reasons,
//! plus a sixth that the script also had:
//!
//! 1. build the example uninstrumented
//! 2. the micro benchmarks, after clearing criterion's previous output
//! 3. the macro workload, several times, wiping storage before each
//! 4. the hotpath profile, from a separate instrumented build
//! 5. the stage profile, from another separate instrumented build
//! 6. **restore the uninstrumented build**
//!
//! Phase 6 has to happen on every path out of a capture, including a failed one. Without it the
//! tree is left holding whichever profiling build ran last, and the next manual run of the example
//! measures an instrumented binary while looking exactly like a normal run. That is the single
//! easiest way to produce a confident wrong number, which is why [`Plan::restore`] is separate
//! from the phase list rather than being the last item in it - a list can be truncated by an error
//! and a field cannot.

use std::path::PathBuf;

use crate::registry::Layer;

/// Where a step's standard output should go
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stdout {
    /// Straight to the terminal, for anything whose progress is worth watching
    Inherit,
    /// Captured, because the step's output is the artifact
    Capture,
    /// Captured and reduced to its last line, which is where `hotpath` prints its profile
    LastLine(PathBuf),
}

/// One command a capture runs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandPlan {
    /// The program to run
    pub program: String,
    /// The arguments to run it with
    pub args: Vec<String>,
    /// The directory to run it in
    pub cwd: PathBuf,
    /// Where its standard output should go
    pub stdout: Stdout,
    /// Environment variables to set for it
    pub env: Vec<(String, String)>,
}

impl CommandPlan {
    /// Renders this command the way a shell would show it
    pub fn display(&self) -> String {
        // the environment first, then the program and its arguments, quoted only where needed
        let mut rendered = String::new();
        for (key, value) in &self.env {
            rendered.push_str(&format!("{key}={} ", quote(value)));
        }
        rendered.push_str(&self.program);
        for arg in &self.args {
            rendered.push(' ');
            rendered.push_str(&quote(arg));
        }
        // and where its output is going, when that is not the terminal
        match &self.stdout {
            Stdout::Inherit => {}
            Stdout::Capture => rendered.push_str("  # stdout captured"),
            Stdout::LastLine(path) => {
                rendered.push_str(&format!("  # last stdout line -> {}", path.display()))
            }
        }
        rendered
    }
}

/// Quotes an argument if a shell would need it quoted
///
/// # Arguments
///
/// * `arg` - The argument to quote
fn quote(arg: &str) -> String {
    // anything with a shell metacharacter in it gets quoted, so a printed plan can be pasted
    if arg.is_empty()
        || arg
            .chars()
            .any(|ch| ch.is_whitespace() || "|&;<>()$`\\\"'*?[]#~".contains(ch))
    {
        format!("'{}'", arg.replace('\'', r"'\''"))
    } else {
        arg.to_string()
    }
}

/// One thing a capture does
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Run a command
    Command(CommandPlan),
    /// Clear criterion's previous output
    ///
    /// Criterion keeps a directory per benchmark id forever, so a benchmark that was renamed or
    /// removed keeps reporting its last result into every capture taken afterwards - a stale
    /// number that looks exactly like a fresh one.
    ClearCriterion(PathBuf),
    /// Empty a storage directory
    WipeStorage(PathBuf),
    /// Fold what a phase produced into a durable artifact
    Collect {
        /// Which layer is being collected
        layer: Layer,
        /// Where its artifact goes
        into: PathBuf,
    },
}

impl Step {
    /// Renders this step for a printed plan
    pub fn display(&self) -> String {
        // each kind of step says what it is, so a dry run reads as a description of the capture
        match self {
            Step::Command(command) => command.display(),
            Step::ClearCriterion(path) => format!("rm -rf {}", path.display()),
            Step::WipeStorage(path) => format!("wipe {}/*", path.display()),
            Step::Collect { layer, into } => {
                format!("collect {layer} -> {}", into.display())
            }
        }
    }
}

/// One phase of a capture
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Phase {
    /// What this phase is called, for the banner it prints
    pub title: String,
    /// Which layer it produces, if it produces one
    pub layer: Option<Layer>,
    /// What it does
    pub steps: Vec<Step>,
}

/// Everything a capture is going to do
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The phases, in order
    pub phases: Vec<Phase>,
    /// The build that leaves the tree holding an uninstrumented binary
    ///
    /// Separate from the phases because it must run whether or not they succeeded. See the module
    /// docs.
    pub restore: Option<CommandPlan>,
}

impl Plan {
    /// Renders the whole plan, one line per step
    pub fn display(&self) -> String {
        let mut out = String::new();
        // each phase, numbered the way the banners will be
        for (index, phase) in self.phases.iter().enumerate() {
            out.push_str(&format!(
                "=== [{}/{}] {} ===\n",
                index + 1,
                self.phases.len(),
                phase.title
            ));
            for step in &phase.steps {
                out.push_str(&format!("    {}\n", step.display()));
            }
        }
        // then the restore, which is not one of the numbered phases because it is not optional
        if let Some(restore) = &self.restore {
            out.push_str("=== always, even after a failure ===\n");
            out.push_str(&format!("    {}\n", restore.display()));
        }
        out
    }
}

/// Everything the plan builder needs to know
#[derive(Debug, Clone)]
pub struct PlanInputs {
    /// The repository root
    pub root: PathBuf,
    /// The name the capture is being taken under
    pub label: String,
    /// Which layers were selected
    pub layers: Vec<Layer>,
    /// The filter to hand criterion, if the micro selection is not everything
    pub criterion_filter: Option<String>,
    /// The workloads the macro layer should run, in the order they run
    pub workloads: Vec<String>,
    /// The workloads each instrumented layer should run
    pub instrumented: Vec<String>,
    /// How many times to run each workload
    pub runs: u32,
    /// The server configuration to run against
    pub conf: PathBuf,
    /// The seed every workload derives its rows from
    pub seed: u64,
    /// How large a run to take
    pub scale: String,
    /// Where the artifacts go
    pub out: PathBuf,
    /// A directory for files that only matter during the capture
    pub scratch: PathBuf,
    /// Every storage directory to empty between runs
    pub storage: Vec<PathBuf>,
    /// Whether to leave criterion's previous output in place
    pub keep_criterion: bool,
    /// Whether to skip restoring the uninstrumented build
    pub no_restore: bool,
}

/// The port the first workload's server binds
///
/// Each workload gets the next port up, so a server whose socket is still in `TIME_WAIT` cannot
/// stop the next workload from binding. They run one at a time, so this is about the tail of the
/// previous one rather than about running two at once.
pub const BASE_PORT: u16 = 12000;

/// The binary every workload runs in
///
/// A second binary of this crate rather than a separate crate: the instrumented layers are built
/// with a feature that changes the code being measured, and `hotpath` has to annotate a `main`.
/// Building this with `--features hotpath` leaves the runner's own binary untouched on disk.
pub const WORKLOAD_BIN: &str = "shoal-workload";

/// Builds the command that compiles the workload binary
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
/// * `feature` - The instrumentation feature to build with, if any
fn build(inputs: &PlanInputs, feature: Option<&str>) -> CommandPlan {
    // release, always: a debug build of a workload measures the debug build
    //
    // `--bin` and not just `--features`, so that building the instrumented workload does not also
    // rewrite the runner binary that is executing this plan
    let mut args = vec![
        "build".to_string(),
        "--release".to_string(),
        "--bin".to_string(),
        WORKLOAD_BIN.to_string(),
    ];
    // each instrumented layer is a separate build, because its instrumentation changes the thing
    // being measured and must not be present in the build that produces the numbers
    if let Some(feature) = feature {
        args.push("--features".to_string());
        args.push(feature.to_string());
    }
    CommandPlan {
        program: "cargo".to_string(),
        args,
        cwd: inputs.root.clone(),
        stdout: Stdout::Inherit,
        env: Vec::new(),
    }
}

/// Builds the command that runs one workload
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
/// * `id` - The workload to run
/// * `extra` - Arguments particular to the phase this run belongs to
/// * `stdout` - Where the run's standard output should go
fn workload(inputs: &PlanInputs, id: &str, extra: Vec<String>, stdout: Stdout) -> CommandPlan {
    // the flags every phase shares. there is no `--dataset` and no `--limit`: a workload builds
    // its own rows from `--seed`, so a capture needs nothing that is not in the repository.
    let mut args = vec![
        "run".to_string(),
        "--id".to_string(),
        id.to_string(),
        "--conf".to_string(),
        inputs.conf.display().to_string(),
        "--seed".to_string(),
        inputs.seed.to_string(),
        "--scale".to_string(),
        inputs.scale.clone(),
        "--port".to_string(),
        port_for(id).to_string(),
    ];
    args.extend(extra);
    CommandPlan {
        program: inputs
            .root
            .join(format!("target/release/{WORKLOAD_BIN}"))
            .display()
            .to_string(),
        args,
        cwd: inputs.root.clone(),
        stdout,
        env: Vec::new(),
    }
}

/// The port a workload's server binds
///
/// Derived from the workload's position in the declared list rather than from a counter, so the
/// same workload binds the same port in every phase of a capture and in every capture.
///
/// # Arguments
///
/// * `id` - The workload to find a port for
fn port_for(id: &str) -> u16 {
    // the declared order is stable, so this is stable
    let offset = crate::workload_ids::IDS
        .iter()
        .position(|declared| *declared == id)
        .unwrap_or(0);
    BASE_PORT + offset as u16
}

/// Where one run of one workload writes its result
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
/// * `id` - The workload being run
/// * `run` - Which run of it this is, from one
fn scratch_result(inputs: &PlanInputs, id: &str, run: u32) -> PathBuf {
    // the workload is in the name, so a leftover file from another workload cannot be folded into
    // this one's numbers - and `collect` refuses one anyway, by reading which workload it holds
    inputs
        .scratch
        .join(format!("run-{}-{run}.json", slug(id)))
}

/// Turns a workload identifier into something that can be a file name
///
/// # Arguments
///
/// * `id` - The workload identifier to convert
fn slug(id: &str) -> String {
    // the same mapping the workload's storage directory uses, for the same reason
    id.replace('/', "-")
}

/// Every step that empties the storage directories
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
fn wipe_steps(inputs: &PlanInputs) -> Vec<Step> {
    // one per configured directory, which is usually one directory named twice
    inputs
        .storage
        .iter()
        .map(|dir| Step::WipeStorage(dir.clone()))
        .collect()
}

/// Works out everything a capture will do
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
pub fn build_plan(inputs: &PlanInputs) -> Plan {
    let mut phases = Vec::new();
    let wants = |layer: Layer| inputs.layers.contains(&layer);
    // anything that runs a workload needs the workload binary built first
    let needs_workloads = wants(Layer::Macro) || wants(Layer::Hotpath) || wants(Layer::Stages);
    if needs_workloads {
        phases.push(Phase {
            title: "building".to_string(),
            layer: None,
            steps: vec![Step::Command(build(inputs, None))],
        });
    }
    // the micro layer, which is criterion and does not touch the server at all
    if wants(Layer::Micro) {
        let mut steps = Vec::new();
        if !inputs.keep_criterion {
            steps.push(Step::ClearCriterion(inputs.root.join("target/criterion")));
        }
        // the same invocation bench.sh used, with a filter appended only when the selection is
        // narrower than the whole registry
        let mut args = vec![
            "bench".to_string(),
            "-p".to_string(),
            "shoal".to_string(),
            "--features".to_string(),
            "bench".to_string(),
            "--bench".to_string(),
            "partitions".to_string(),
            "--".to_string(),
            "--save-baseline".to_string(),
            inputs.label.clone(),
        ];
        if let Some(filter) = &inputs.criterion_filter {
            args.push(filter.clone());
        }
        steps.push(Step::Command(CommandPlan {
            program: "cargo".to_string(),
            args,
            cwd: inputs.root.clone(),
            stdout: Stdout::Inherit,
            env: Vec::new(),
        }));
        steps.push(Step::Collect {
            layer: Layer::Micro,
            into: artifact(inputs, Layer::Micro),
        });
        phases.push(Phase {
            title: "micro benchmarks".to_string(),
            layer: Some(Layer::Micro),
            steps,
        });
    }
    // the macro layer: every selected workload, run several times because one run says very little
    //
    // the loop is workload-outer and run-inner, so a workload's five runs are consecutive. that
    // matters for the fold: the runs being compared to pick a median are as close together in
    // time as the capture can make them, rather than being spread across every other workload.
    if wants(Layer::Macro) {
        let mut steps = Vec::new();
        for id in &inputs.workloads {
            for run in 1..=inputs.runs {
                steps.extend(wipe_steps(inputs));
                steps.push(Step::Command(workload(
                    inputs,
                    id,
                    vec![
                        "--label".to_string(),
                        inputs.label.clone(),
                        "--json".to_string(),
                        scratch_result(inputs, id, run).display().to_string(),
                    ],
                    Stdout::Capture,
                )));
            }
        }
        steps.push(Step::Collect {
            layer: Layer::Macro,
            into: artifact(inputs, Layer::Macro),
        });
        phases.push(Phase {
            title: format!(
                "macro benchmarks, {} workload(s) x {} runs",
                inputs.workloads.len(),
                inputs.runs
            ),
            layer: Some(Layer::Macro),
            steps,
        });
    }
    // the hotpath profile, from its own build, whose profile is the last line it prints
    if wants(Layer::Hotpath) {
        let mut steps = vec![Step::Command(build(inputs, Some("hotpath")))];
        for id in &inputs.instrumented {
            steps.extend(wipe_steps(inputs));
            steps.push(Step::Command(workload(
                inputs,
                id,
                vec![
                    "--json".to_string(),
                    scratch_result(inputs, id, 0).display().to_string(),
                ],
                Stdout::LastLine(artifact(inputs, Layer::Hotpath)),
            )));
        }
        steps.push(Step::Collect {
            layer: Layer::Hotpath,
            into: artifact(inputs, Layer::Hotpath),
        });
        phases.push(Phase {
            title: "hotpath profile (separate build, attribution only)".to_string(),
            layer: Some(Layer::Hotpath),
            steps,
        });
    }
    // the stage profile, from its own build, which writes its own report because only it can join
    // the client and server halves of a record
    if wants(Layer::Stages) {
        let mut steps = vec![Step::Command(build(inputs, Some("stage-profile")))];
        for id in &inputs.instrumented {
            steps.extend(wipe_steps(inputs));
            steps.push(Step::Command(workload(
                inputs,
                id,
                vec![
                    "--label".to_string(),
                    inputs.label.clone(),
                    "--json".to_string(),
                    scratch_result(inputs, id, 0).display().to_string(),
                    "--stage-json".to_string(),
                    artifact(inputs, Layer::Stages).display().to_string(),
                ],
                Stdout::Capture,
            )));
        }
        steps.push(Step::Collect {
            layer: Layer::Stages,
            into: artifact(inputs, Layer::Stages),
        });
        phases.push(Phase {
            title: "stage profile (separate build, attribution only)".to_string(),
            layer: Some(Layer::Stages),
            steps,
        });
    }
    // and the restore, whenever an instrumented build could have been the last one made
    let instrumented = wants(Layer::Hotpath) || wants(Layer::Stages);
    let restore = (instrumented && !inputs.no_restore).then(|| build(inputs, None));
    Plan { phases, restore }
}

/// Where one layer's artifact goes
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
/// * `layer` - Which layer's artifact to address
pub fn artifact(inputs: &PlanInputs, layer: Layer) -> PathBuf {
    // the flat `<label>.<layer>.json` naming bench.sh established
    inputs.out.join(format!("{}.{layer}.json", inputs.label))
}

/// Where a capture's provenance goes
///
/// # Arguments
///
/// * `inputs` - What the capture was asked for
pub fn meta_artifact(inputs: &PlanInputs) -> PathBuf {
    // a sibling of the layer artifacts, so micro.json never needs a schema version bump
    inputs.out.join(format!("{}.meta.json", inputs.label))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds inputs for a capture of every layer
    ///
    /// # Arguments
    ///
    /// * `layers` - Which layers to capture
    fn inputs(layers: &[Layer]) -> PlanInputs {
        PlanInputs {
            root: PathBuf::from("/repo"),
            label: "L".to_string(),
            layers: layers.to_vec(),
            criterion_filter: None,
            // the real declared list, so a workload added to it is covered by these tests
            workloads: crate::workload_ids::IDS
                .iter()
                .map(|id| id.to_string())
                .collect(),
            instrumented: crate::registry::PROFILED_WORKLOADS
                .iter()
                .map(|id| id.to_string())
                .collect(),
            runs: 5,
            conf: PathBuf::from("shoal.yml"),
            seed: 42,
            scale: "full".to_string(),
            out: PathBuf::from("/repo/docs/perf/runs"),
            scratch: PathBuf::from("/scratch"),
            storage: vec![PathBuf::from("/opt/shoal")],
            keep_criterion: false,
            no_restore: false,
        }
    }

    /// Every command in a plan, in order
    ///
    /// # Arguments
    ///
    /// * `plan` - The plan to walk
    fn commands(plan: &Plan) -> Vec<String> {
        let mut found: Vec<String> = plan
            .phases
            .iter()
            .flat_map(|phase| &phase.steps)
            .map(Step::display)
            .collect();
        if let Some(restore) = &plan.restore {
            found.push(restore.display());
        }
        found
    }

    /// A full capture runs the same five phases, in the same order, as the script it replaces
    #[test]
    fn a_full_capture_matches_the_five_phases() {
        let plan = build_plan(&inputs(&Layer::ALL));
        let titles: Vec<&str> = plan
            .phases
            .iter()
            .map(|phase| phase.title.as_str())
            .collect();
        assert_eq!(
            titles,
            vec![
                "building",
                "micro benchmarks",
                &format!("macro benchmarks, {} workload(s) x 5 runs", crate::workload_ids::IDS.len()),
                "hotpath profile (separate build, attribution only)",
                "stage profile (separate build, attribution only)",
            ]
        );
    }

    /// The restore is last, and it is not one of the phases
    #[test]
    fn the_restore_is_last_and_is_not_a_phase() {
        let plan = build_plan(&inputs(&Layer::ALL));
        // it exists
        let restore = plan.restore.as_ref().expect("a full capture restores");
        assert_eq!(restore.args, vec!["build", "--release", "--bin", "shoal-workload"]);
        // it is the last thing in the plan
        assert_eq!(commands(&plan).last().map(String::as_str), Some(restore.display().as_str()));
        // and no phase contains it, so an error truncating the phase list cannot skip it
        assert!(
            plan.phases
                .iter()
                .flat_map(|phase| &phase.steps)
                .all(|step| step != &Step::Command(restore.clone())
                    || phase_is_build(plan.phases.first())),
            "the restore must not be inside a phase"
        );
    }

    /// Whether the first phase is the initial build, used by the restore test
    ///
    /// # Arguments
    ///
    /// * `phase` - The phase to check
    fn phase_is_build(phase: Option<&Phase>) -> bool {
        phase.is_some_and(|phase| phase.title == "building")
    }

    /// A capture with no instrumented layer has nothing to restore from
    #[test]
    fn a_capture_without_instrumentation_does_not_restore() {
        let plan = build_plan(&inputs(&[Layer::Micro, Layer::Macro]));
        assert!(plan.restore.is_none());
    }

    /// Either instrumented layer on its own is enough to require a restore
    #[test]
    fn one_instrumented_layer_is_enough_to_restore() {
        assert!(build_plan(&inputs(&[Layer::Hotpath])).restore.is_some());
        assert!(build_plan(&inputs(&[Layer::Stages])).restore.is_some());
    }

    /// A micro only capture builds nothing and touches no storage
    #[test]
    fn a_micro_only_capture_stays_out_of_the_server() {
        let plan = build_plan(&inputs(&[Layer::Micro]));
        assert_eq!(plan.phases.len(), 1);
        // nothing is wiped, since nothing writes to the store
        assert!(
            !plan
                .phases
                .iter()
                .flat_map(|phase| &phase.steps)
                .any(|step| matches!(step, Step::WipeStorage(_)))
        );
        // and the workload binary is never built, since no workload is run
        assert!(!commands(&plan).iter().any(|line| line.contains("--bin shoal-workload")));
    }

    /// A full micro selection passes no filter, so the invocation is the one always used
    #[test]
    fn a_full_micro_selection_passes_no_filter() {
        let plan = build_plan(&inputs(&[Layer::Micro]));
        let bench = commands(&plan)
            .into_iter()
            .find(|line| line.contains("cargo bench"))
            .expect("the micro phase runs cargo bench");
        assert!(bench.ends_with("-- --save-baseline L"), "{bench}");
    }

    /// A narrowed micro selection appends the anchored alternation
    #[test]
    fn a_narrowed_micro_selection_passes_its_filter() {
        let mut narrowed = inputs(&[Layer::Micro]);
        narrowed.criterion_filter = Some("^(?:a/1|b/2)$".to_string());
        let plan = build_plan(&narrowed);
        let bench = commands(&plan)
            .into_iter()
            .find(|line| line.contains("cargo bench"))
            .expect("the micro phase runs cargo bench");
        // quoted, since the pattern is full of shell metacharacters
        assert!(bench.contains(r"'^(?:a/1|b/2)$'"), "{bench}");
    }

    /// Storage is wiped before every macro run, not just the first
    #[test]
    fn storage_is_wiped_before_every_macro_run() {
        let plan = build_plan(&inputs(&[Layer::Macro]));
        let macro_phase = plan
            .phases
            .iter()
            .find(|phase| phase.layer == Some(Layer::Macro))
            .expect("the macro phase is planned");
        let wipes = macro_phase
            .steps
            .iter()
            .filter(|step| matches!(step, Step::WipeStorage(_)))
            .count();
        // one wipe before every run of every workload, not one before each workload
        assert_eq!(wipes, crate::workload_ids::IDS.len() * 5);
    }

    /// Every run of every workload writes its own scratch file
    ///
    /// The file names carry the workload as well as the run number, so two workloads' runs cannot
    /// collide and a leftover file from one cannot be folded into the other.
    #[test]
    fn each_macro_run_writes_its_own_result() {
        let plan = build_plan(&inputs(&[Layer::Macro]));
        let lines = commands(&plan);
        for id in crate::workload_ids::IDS {
            for run in 1..=5 {
                let expected = format!("/scratch/run-{}-{run}.json", slug(id));
                assert!(
                    lines.iter().any(|line| line.contains(&expected)),
                    "{id} run {run} has no result file"
                );
            }
        }
    }

    /// Every run names the workload it is running and the seed it derives its rows from
    ///
    /// There is no `--dataset` and no `--limit` any more: a workload builds its own rows, so a
    /// capture needs nothing that is not in the repository. That is the whole reason a clean
    /// checkout can now reproduce this layer.
    #[test]
    fn every_run_names_its_workload_and_seed() {
        let plan = build_plan(&inputs(&Layer::ALL));
        let runs: Vec<String> = commands(&plan)
            .into_iter()
            .filter(|line| line.contains("target/release/shoal-workload"))
            .collect();
        assert!(!runs.is_empty(), "no workload is ever run");
        for line in runs {
            assert!(line.contains("--id macro/"), "{line}");
            assert!(line.contains("--seed 42"), "{line}");
            assert!(line.contains("--scale full"), "{line}");
            // the flags that needed a dataset nobody had are gone
            assert!(!line.contains("--dataset"), "{line}");
            assert!(!line.contains("--limit"), "{line}");
        }
    }

    /// A workload always binds the same port, in every phase and every capture
    ///
    /// Derived from the declared order rather than from a counter, so the hotpath run of a
    /// workload uses the same port its macro runs did.
    #[test]
    fn a_workload_always_binds_the_same_port() {
        let plan = build_plan(&inputs(&Layer::ALL));
        let first = crate::workload_ids::IDS[0];
        let expected = format!("--port {BASE_PORT}");
        for line in commands(&plan) {
            if line.contains("target/release/shoal-workload") && line.contains(first) {
                assert!(line.contains(&expected), "{line}");
            }
        }
        // and two workloads never share one
        let ports: std::collections::BTreeSet<u16> = crate::workload_ids::IDS
            .iter()
            .map(|id| port_for(id))
            .collect();
        assert_eq!(ports.len(), crate::workload_ids::IDS.len());
    }

    /// A smaller scale reaches every run of every workload
    #[test]
    fn a_scale_reaches_every_run() {
        let mut smoke = inputs(&Layer::ALL);
        smoke.scale = "smoke".to_string();
        let plan = build_plan(&smoke);
        let runs: Vec<String> = commands(&plan)
            .into_iter()
            .filter(|line| line.contains("target/release/shoal-workload"))
            .collect();
        assert!(!runs.is_empty());
        for line in runs {
            assert!(line.contains("--scale smoke"), "{line}");
        }
    }

    /// Only the workloads that opted into attribution are run under the instrumented layers
    ///
    /// `hotpath` emits one profile per process, so every workload opting in would make attribution
    /// cost more than the rest of a capture for profiles that mostly repeat each other.
    #[test]
    fn the_instrumented_layers_run_only_what_opted_in() {
        let plan = build_plan(&inputs(&[Layer::Hotpath]));
        let runs: Vec<String> = commands(&plan)
            .into_iter()
            .filter(|line| line.contains("target/release/shoal-workload"))
            .collect();
        assert_eq!(runs.len(), crate::registry::PROFILED_WORKLOADS.len());
        for id in crate::registry::PROFILED_WORKLOADS {
            assert!(runs.iter().any(|line| line.contains(id)), "{id} is not profiled");
        }
    }

    /// The hotpath profile is taken from the last line of its run's output
    #[test]
    fn the_hotpath_profile_is_the_last_line_of_stdout() {
        let plan = build_plan(&inputs(&[Layer::Hotpath]));
        let run = plan
            .phases
            .iter()
            .flat_map(|phase| &phase.steps)
            .find_map(|step| match step {
                Step::Command(command) if command.program.contains(WORKLOAD_BIN) => {
                    Some(command.clone())
                }
                _ => None,
            })
            .expect("the hotpath phase runs a workload");
        assert_eq!(
            run.stdout,
            Stdout::LastLine(PathBuf::from("/repo/docs/perf/runs/L.hotpath.json"))
        );
    }

    /// Each instrumented layer is built with its own feature, and never with the other's
    #[test]
    fn each_instrumented_layer_gets_its_own_build() {
        let plan = build_plan(&inputs(&Layer::ALL));
        let lines = commands(&plan);
        assert!(lines.iter().any(|line| line.contains("--features hotpath")));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("--features stage-profile"))
        );
        // and no build carries both, which would measure the two instrumentations together
        assert!(
            !lines
                .iter()
                .any(|line| line.contains("hotpath") && line.contains("stage-profile"))
        );
    }

    /// Keeping criterion's output skips the clear, and is not the default
    #[test]
    fn criterion_output_is_cleared_by_default() {
        let plan = build_plan(&inputs(&[Layer::Micro]));
        assert!(
            plan.phases[0]
                .steps
                .iter()
                .any(|step| matches!(step, Step::ClearCriterion(_)))
        );
        let mut kept = inputs(&[Layer::Micro]);
        kept.keep_criterion = true;
        let plan = build_plan(&kept);
        assert!(
            !plan.phases[0]
                .steps
                .iter()
                .any(|step| matches!(step, Step::ClearCriterion(_)))
        );
    }

    /// A printed plan names every phase and ends with the restore
    #[test]
    fn a_printed_plan_ends_with_the_restore() {
        let rendered = build_plan(&inputs(&Layer::ALL)).display();
        assert!(rendered.contains("=== [1/5] building ==="));
        assert!(rendered.contains("=== always, even after a failure ==="));
        let restore_at = rendered
            .find("=== always, even after a failure ===")
            .expect("the restore is announced");
        assert!(
            rendered[restore_at..].contains("cargo build --release --bin shoal-workload"),
            "the restore must be the last thing printed"
        );
    }
}
