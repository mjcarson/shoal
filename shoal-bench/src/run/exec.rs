//! The only place in this crate that spawns a process
//!
//! Everything else works out *what* to run, in code that can be unit tested on any machine.
//! Keeping the spawning to one module is what makes `--dry-run` trustworthy: it is the same plan,
//! printed instead of passed to here.

use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};

use super::plan::{CommandPlan, Stdout};

/// Runs one planned command
///
/// # Arguments
///
/// * `plan` - The command to run
pub fn run(plan: &CommandPlan) -> Result<()> {
    // set up the process exactly as the plan describes it
    let mut command = Command::new(&plan.program);
    command.current_dir(&plan.cwd).args(&plan.args);
    for (key, value) in &plan.env {
        command.env(key, value);
    }
    match &plan.stdout {
        // progress worth watching goes straight to the terminal
        Stdout::Inherit => {
            command.stdout(Stdio::inherit());
        }
        // output that is itself an artifact is captured
        Stdout::Capture | Stdout::LastLine(_) => {
            command.stdout(Stdio::piped());
        }
    }
    // stderr always reaches the terminal: a failing build or a panicking run has to be visible
    command.stderr(Stdio::inherit());
    let output = command
        .output()
        .with_context(|| format!("running {}", plan.display()))?;
    // a step that failed stops the capture, naming what it was
    if !output.status.success() {
        bail!("{} failed with {}", plan.display(), output.status);
    }
    // the hotpath profile is the last line of a run's output, everything before it being the
    // run's own reporting
    if let Stdout::LastLine(target) = &plan.stdout {
        let text = String::from_utf8_lossy(&output.stdout);
        let last = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .next_back()
            .unwrap_or_default();
        // a run that printed nothing produced no profile, and writing an empty file would hide
        // that behind a parse error later
        if last.is_empty() {
            bail!(
                "{} printed nothing, so there is no profile to write to {}",
                plan.display(),
                target.display()
            );
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(target, format!("{last}\n"))
            .with_context(|| format!("writing {}", target.display()))?;
    }
    Ok(())
}
