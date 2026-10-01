//! A running benchmark as lines: what `--basic` prints, and what every run writes to its log
//!
//! One line a phase, a line an arm, and a line a second, so a run under a pipe or over ssh says
//! what it is doing and an outage shows up as the seconds it lasted. Ctrl-C asks the run to
//! stop; it then tears the cluster down and puts the hosts back before it exits.

use shoal_loadgen::progress::{BenchEvent, Control};
use shoal_loadgen::results::SecondPhase;
use std::io::Write;
use std::path::PathBuf;

/// The line an event is shown as, if it is shown at all
///
/// # Arguments
///
/// * `event` - The event
/// * `total` - How many arms the run has, once it is known
#[must_use]
pub fn line(event: &BenchEvent, total: &mut usize) -> Option<String> {
    // each kind of event as one line
    match event {
        BenchEvent::Planned { label, arms } => {
            *total = arms.len();
            Some(format!("{label}: {} arms", arms.len()))
        }
        BenchEvent::Phase(phase) => Some(format!("== {}", phase.label())),
        BenchEvent::ArmStarted { index, arm, warmup, duration } => Some(format!(
            "[{}/{}] {} run {} ({warmup}s warmup, {duration}s measured, bundle {}, {} in flight)",
            index + 1,
            total,
            arm.id,
            arm.run,
            arm.bundle,
            arm.in_flight
        )),
        BenchEvent::Second(sample) => {
            let phase = match sample.phase {
                SecondPhase::Warmup => "warmup",
                SecondPhase::Measure => "measure",
                SecondPhase::Drain => "drain",
            };
            Some(format!(
                "  {:>4}s {phase:<7} {} | driver {:.0}%",
                sample.at, sample.summary.line(), sample.driver_cpu_pct
            ))
        }
        BenchEvent::Mark(mark) => Some(format!(
            "  ** {} at {:.1}s{}",
            mark.kind,
            mark.at_ms as f64 / 1000.0,
            mark.note.as_ref().map(|note| format!(": {note}")).unwrap_or_default()
        )),
        BenchEvent::ArmDone { index, summary, ended_early } => Some(format!(
            "[{}/{}] done: {}{}",
            index + 1,
            total,
            summary.line(),
            ended_early.as_ref().map(|why| format!(" (ended early: {why})")).unwrap_or_default()
        )),
        BenchEvent::Log(line) => Some(line.clone()),
        BenchEvent::Finished(Ok(dir)) => Some(format!("finished: {}", dir.display())),
        BenchEvent::Finished(Err(error)) => Some(format!("failed: {error}")),
    }
}

/// Print a run's progress until it finishes, asking it to stop on Ctrl-C
///
/// # Arguments
///
/// * `rx` - The run's progress
/// * `control` - What the run is told
/// * `log` - The run's log file
pub async fn print(
    mut rx: tokio::sync::mpsc::Receiver<BenchEvent>,
    control: tokio::sync::watch::Sender<Control>,
    mut log: Option<std::fs::File>,
) -> Option<Result<PathBuf, String>> {
    let mut total = 0usize;
    let mut finished = None;
    loop {
        tokio::select! {
            event = rx.recv() => {
                // a closed channel is a run that has ended
                let Some(event) = event else {
                    return finished;
                };
                if let Some(line) = line(&event, &mut total) {
                    println!("{line}");
                    if let Some(log) = log.as_mut() {
                        let _ = writeln!(log, "{line}");
                    }
                }
                if let BenchEvent::Finished(result) = event {
                    finished = Some(result);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                // the run stops at once and puts everything back; a second Ctrl-C is the same ask
                eprintln!("stopping: the cluster is torn down and the hosts put back before this exits");
                let _ = control.send(Control::Abort);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::line;
    use shoal_loadgen::progress::{BenchEvent, Phase};

    /// Every kind of event is a line, and an arm's line says where it is in the run
    #[test]
    fn events_are_lines() {
        let mut total = 0;
        let planned = BenchEvent::Planned {
            label: "x".to_string(),
            arms: Vec::new(),
        };
        assert_eq!(line(&planned, &mut total).unwrap(), "x: 0 arms");
        total = 4;
        let phase = BenchEvent::Phase(Phase::Preload { done: 5, total: 10 });
        assert_eq!(line(&phase, &mut total).unwrap(), "== preload 5/10");
        let done = BenchEvent::ArmDone {
            index: 1,
            summary: Default::default(),
            ended_early: Some("inserts exhausted".to_string()),
        };
        assert_eq!(line(&done, &mut total).unwrap(), "[2/4] done: idle (ended early: inserts exhausted)");
    }
}
