//! Where captures are kept, and `bench list`, `show` and `compare`
//!
//! A capture is a directory of its own under the project's `target/shoaladm-bench/`, named by
//! its label, never under `docs/perf/runs`: these are an operator's measurements of their own
//! schema, not the repository's corpus. `cargo clean` deletes them; `--out` keeps one anywhere.

use color_eyre::eyre::{bail, eyre};
use shoal_loadgen::compare::{compare, Verdict};
use shoal_loadgen::results::{Capture, RunResult, CAPTURE_FILE};
use std::path::{Path, PathBuf};

use super::args::{BenchCommand, RootArg};
use super::provenance::code_facts;
use crate::cli::ProjectArgs;
use crate::project::Project;

/// The directory under a project's target directory captures are kept in
pub const RESULTS_DIR: &str = "shoaladm-bench";

/// Where captures are kept: the flag's directory, or the project's
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `root` - The flag, if given
///
/// # Errors
///
/// When neither is given and the current directory is not a project.
pub fn root(project: &ProjectArgs, root: &RootArg) -> color_eyre::Result<PathBuf> {
    // the flag wins; otherwise the project's target directory, wherever cargo puts it
    if let Some(root) = &root.root {
        return Ok(root.clone());
    }
    let located = Project::locate(&project.dir()?)?;
    Ok(located.target_directory.join(RESULTS_DIR))
}

/// A capture named by its label under the root, or by a path
///
/// # Arguments
///
/// * `root` - Where captures are kept
/// * `capture` - A label or a path
#[must_use]
pub fn resolve(root: &Path, capture: &str) -> PathBuf {
    // a path that exists is taken as one
    let path = PathBuf::from(capture);
    if path.exists() {
        return path;
    }
    root.join(capture)
}

/// The lines `bench list` prints
///
/// # Arguments
///
/// * `root` - Where captures are kept
///
/// # Errors
///
/// When the directory cannot be read.
pub fn list_lines(root: &Path) -> color_eyre::Result<Vec<String>> {
    // nothing captured yet is an empty list, said
    if !root.is_dir() {
        return Ok(vec![format!("no captures under {}", root.display())]);
    }
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.join(CAPTURE_FILE).is_file())
        .collect();
    dirs.sort();
    let mut lines = vec![format!(
        "{:<36} {:<20} {:<9} {:<8} {:<7} {:>4} {:<12} {}",
        "label", "started", "commit", "flavor", "mode", "arms", "dataset", "fresh"
    )];
    for dir in dirs {
        let label = dir.file_name().map(|name| name.to_string_lossy().to_string()).unwrap_or_default();
        match Capture::read(&dir) {
            Ok(capture) => {
                let provenance = &capture.provenance;
                let commit = provenance.project.commit.as_deref().unwrap_or("-");
                // a capture of a commit the project has moved past no longer describes the code
                let fresh = freshness(&capture);
                lines.push(format!(
                    "{:<36} {:<20} {:<9} {:<8} {:<7} {:>4} {:<12} {}",
                    label,
                    provenance.started_at,
                    commit.chars().take(8).collect::<String>(),
                    provenance.flavor,
                    provenance.mode.map_or("-".to_string(), |mode| format!("{mode:?}").to_lowercase()),
                    capture.arms.len(),
                    capture.dataset.digest.chars().take(12).collect::<String>(),
                    fresh
                ));
            }
            Err(error) => lines.push(format!("{label:<36} unreadable: {error}")),
        }
    }
    Ok(lines)
}

/// Whether a capture still describes the code it was taken of
///
/// # Arguments
///
/// * `capture` - The capture
fn freshness(capture: &Capture) -> String {
    // a capture with no commit cannot be judged
    let Some(commit) = capture.provenance.project.commit.as_deref() else {
        return "unknown".to_string();
    };
    let here = code_facts(Path::new("."));
    match here.commit.as_deref() {
        Some(head) if head == commit && !capture.provenance.project.dirty => "fresh".to_string(),
        Some(_) => "stale".to_string(),
        None => "unknown".to_string(),
    }
}

/// The lines `bench show` prints
///
/// # Arguments
///
/// * `capture` - The capture
#[must_use]
pub fn show_lines(capture: &Capture) -> Vec<String> {
    // the provenance in a few lines, then a line a run of each arm
    let provenance = &capture.provenance;
    let mut lines = vec![
        format!("{} ({})", capture.label, if capture.complete { "complete" } else { "incomplete" }),
        format!(
            "  {} {} {}, project {} shoal {}",
            provenance.started_at,
            provenance.flavor,
            provenance.mode.map_or("-".to_string(), |mode| format!("{mode:?}").to_lowercase()),
            provenance.project.commit.as_deref().unwrap_or("-"),
            provenance
                .shoal
                .commit
                .as_deref()
                .or(provenance.shoal.version.as_deref())
                .unwrap_or("-"),
        ),
        format!(
            "  driver {} ({}, {} cpus){}",
            provenance.driver.hostname,
            provenance.driver.cpu,
            provenance.driver.cores,
            if provenance.driver_shares_host { ", on a node" } else { "" }
        ),
        format!("  dataset {}", capture.dataset.digest),
    ];
    for table in &capture.dataset.tables {
        lines.push(format!(
            "    {} {} rows, {} preloaded, {} to insert, {} bad",
            table.table, table.rows, table.preload_rows, table.insert_rows, table.parse_errors
        ));
    }
    if let Some(error) = &capture.error {
        lines.push(format!("  stopped: {error}"));
    }
    for arm in &capture.arms {
        lines.push(format!("{}", arm.id));
        for run in &arm.runs {
            let mut line = format!("  run {}: {}", run.run, run.measured.line());
            if let Some(ended) = &run.ended_early {
                line.push_str(&format!(" (ended at {:.1}s: {})", ended.at_secs, ended.reason));
            }
            if let Some(verify) = &run.verify {
                line.push_str(&format!(" | acks {} lost {}", verify.checked, verify.lost));
            }
            lines.push(line);
            // a server series that leaves members out, or was never read, says so
            if !run.unfigured.is_empty() {
                lines.push(format!(
                    "    no query figures from {}: a build from before F65, left out of the server series",
                    run.unfigured.join(", ")
                ));
            }
            if let Some(why) = &run.figures_unread {
                lines.push(format!("    the nodes' own figures were not read: {why}"));
            }
            if let Some(why) = &run.devices_unread {
                lines.push(format!("    the hosts' devices were not read: {why}"));
            }
            // what the devices and the members' memory did, and the paced stream beside it
            lines.extend(run_notes(run).into_iter().map(|note| format!("    {note}")));
            if let Some(event) = &run.event {
                // in the order they happened, not the order their names sort
                for name in ["before", "during", "after"] {
                    if let Some(window) = event.windows.get(name) {
                        lines.push(format!("    {name}: {}", window.line()));
                    }
                }
                if let Some(ratio) = event.p99_ratio_permille {
                    lines.push(format!("    p99 during/before: {:.2}x", ratio as f64 / 1000.0));
                }
                lines.push(format!("    {}", event.outcome));
            }
        }
    }
    lines
}

/// A count of bytes as a person reads it, in binary units
///
/// # Arguments
///
/// * `bytes` - The count
fn bytes(bytes: u64) -> String {
    // the largest unit that leaves a number of at least one
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

/// What a run's hosts' devices, its members' memory and its paced stream did, a line each
///
/// `bench show` prints these under a run, and a run in progress logs them once its arm ends.
/// Why the devices could not be read is not among them: a run says that once, not every arm
/// ([F71](../../../docs/src/features/bench-device-memory.md),
/// [F72](../../../docs/src/features/bench-paced-stream.md)).
///
/// # Arguments
///
/// * `run` - The run
#[must_use]
pub fn run_notes(run: &RunResult) -> Vec<String> {
    let mut notes = Vec::new();
    // each host's devices, then what they wrote for each byte the driver sent
    if !run.devices.is_empty() {
        let mut hosts: Vec<String> = run
            .devices
            .iter()
            .map(|host| {
                let devices: Vec<String> = host
                    .devices
                    .iter()
                    .map(|device| {
                        format!(
                            "{} wrote {} read {}",
                            device.device,
                            bytes(device.written_bytes),
                            bytes(device.read_bytes)
                        )
                    })
                    .collect();
                format!("{} {}", host.host, if devices.is_empty() { "-".to_string() } else { devices.join(", ") })
            })
            .collect();
        if let Some(ratio) = run.device_bytes_per_sent_byte() {
            hosts.push(format!("{ratio:.2} bytes written a byte sent"));
        }
        notes.push(format!("devices: {}", hosts.join(" | ")));
        let unresolved: Vec<&str> = run
            .devices
            .iter()
            .flat_map(|host| host.unresolved.iter().map(String::as_str))
            .collect();
        if !unresolved.is_empty() {
            notes.push(format!("roots on no device the kernel counts: {}", unresolved.join(", ")));
        }
    }
    // each member's largest resident set, and its indexes once the run was done
    let peaks = run.peak_resident();
    if !peaks.is_empty() {
        let mut members: Vec<String> = peaks
            .iter()
            .map(|(member, resident)| format!("{member} {}", bytes(*resident)))
            .collect();
        if let Some(memory) = run.last_memory() {
            let indexes: Vec<String> = memory
                .iter()
                .map(|(member, memory)| {
                    format!(
                        "{member} {} (archive map {})",
                        bytes(memory.index_bytes()),
                        bytes(memory.archive_map_bytes)
                    )
                })
                .collect();
            members.push(format!("indexes {}", indexes.join(", ")));
        }
        notes.push(format!("resident peak {}", members.join(", ")));
    }
    // the paced stream: what it offered, what it got, and its worst second
    if let Some(paced) = &run.paced {
        let mut line = format!(
            "paced {} {} at {}/s: {}",
            paced.table,
            paced.workload,
            paced.per_sec,
            paced.measured.line()
        );
        if let Some(worst) = paced.worst_second_p99_ms() {
            line.push_str(&format!(" | worst second p99 {worst:.2}ms"));
        }
        if let Some(verify) = &paced.verify {
            line.push_str(&format!(" | acks {} lost {}", verify.checked, verify.lost));
        }
        notes.push(line);
    }
    notes
}

/// What is missing from a capture's server series, if anything: members on a build without
/// query figures, or figures never read
///
/// # Arguments
///
/// * `capture` - The capture
fn figures_note(capture: &Capture) -> Option<String> {
    // every run's members without figures, and whether any run read none at all
    let runs = capture.arms.iter().flat_map(|arm| &arm.runs);
    let unfigured: std::collections::BTreeSet<&str> = runs
        .clone()
        .flat_map(|run| run.unfigured.iter().map(String::as_str))
        .collect();
    let unread = runs.clone().filter(|run| run.figures_unread.is_some()).count();
    match (unfigured.is_empty(), unread) {
        (true, 0) => None,
        (false, _) => Some(format!(
            "server series leaves out {}, on a build from before F65",
            unfigured.into_iter().collect::<Vec<_>>().join(", ")
        )),
        (true, unread) => Some(format!("server series was not read in {unread} runs")),
    }
}

/// The lines `bench compare` prints
///
/// # Arguments
///
/// * `baseline` - The capture compared against
/// * `candidate` - The capture compared
/// * `allow` - The facts allowed to differ
///
/// # Errors
///
/// When the two cannot be compared, naming every difference.
pub fn compare_lines(baseline: &Capture, candidate: &Capture, allow: &[String]) -> color_eyre::Result<Vec<String>> {
    // refused on anything that would make the numbers mean something else
    let comparison = compare(baseline, candidate, allow).map_err(|differences| {
        let lines: Vec<String> = differences.iter().map(|difference| format!("  - {difference}")).collect();
        eyre!(
            "{} and {} cannot be compared:\n{}\npass --allow <fact> for each difference you mean to compare across",
            baseline.label,
            candidate.label,
            lines.join("\n")
        )
    })?;
    let mut lines = vec![format!("{} against {}", candidate.label, baseline.label)];
    for waived in &comparison.waived {
        lines.push(format!("  waived: {waived}"));
    }
    // the nodes' own figures are not compared, but a side whose series is partial is said
    for capture in [baseline, candidate] {
        if let Some(note) = figures_note(capture) {
            lines.push(format!("  note: {}'s {note}", capture.label));
        }
    }
    for arm in &comparison.arms {
        lines.push(arm.id.clone());
        for metric in &arm.metrics {
            let interval = |interval: &Option<shoal_loadgen::compare::Interval>| {
                interval.map_or("-".to_string(), |i| format!("{:.2}..{:.2}", i.low, i.high))
            };
            let verdict = match metric.verdict {
                Verdict::Better { gap_pct } => format!("better by at least {gap_pct:.1}%"),
                Verdict::Worse { gap_pct } => format!("worse by at least {gap_pct:.1}%"),
                Verdict::NoDifference => "no difference established".to_string(),
                Verdict::Absent => "not on both sides".to_string(),
            };
            lines.push(format!(
                "  {:<14} {:>22} -> {:<22} {verdict}",
                metric.metric,
                interval(&metric.baseline),
                interval(&metric.candidate)
            ));
        }
    }
    Ok(lines)
}

/// Run a bench command that connects to no cluster, or hand back one that does
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `command` - The command
///
/// # Errors
///
/// When a capture cannot be read, or two cannot be compared.
pub fn run_local(project: &ProjectArgs, command: BenchCommand) -> color_eyre::Result<Option<BenchCommand>> {
    match command {
        BenchCommand::List(root_arg) => {
            for line in list_lines(&root(project, &root_arg)?)? {
                println!("{line}");
            }
        }
        BenchCommand::Show { capture, root: root_arg } => {
            let path = resolve(&root(project, &root_arg)?, &capture);
            let capture = Capture::read(&path).map_err(|error| eyre!(error))?;
            for line in show_lines(&capture) {
                println!("{line}");
            }
        }
        BenchCommand::Compare {
            baseline,
            candidate,
            allow,
            root: root_arg,
        } => {
            let root = root(project, &root_arg)?;
            let baseline = Capture::read(&resolve(&root, &baseline)).map_err(|error| eyre!(error))?;
            let candidate = Capture::read(&resolve(&root, &candidate)).map_err(|error| eyre!(error))?;
            for line in compare_lines(&baseline, &candidate, &allow)? {
                println!("{line}");
            }
        }
        run @ BenchCommand::Run(_) => return Ok(Some(run)),
    }
    Ok(None)
}

/// Where a run's capture goes, refusing one that exists unless asked to replace it
///
/// # Arguments
///
/// * `root` - Where captures are kept
/// * `out` - The flag, if given
/// * `label` - The capture's label
/// * `overwrite` - Whether an existing capture may be replaced
///
/// # Errors
///
/// When a capture is already there and may not be replaced.
pub fn capture_dir(root: &Path, out: Option<&Path>, label: &str, overwrite: bool) -> color_eyre::Result<PathBuf> {
    // the flag names the directory itself
    let dir = out.map_or_else(|| root.join(label), Path::to_path_buf);
    if dir.join(CAPTURE_FILE).exists() && !overwrite {
        bail!("{} already holds a capture; pass --overwrite or another --label", dir.display());
    }
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
