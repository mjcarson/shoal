//! Closed-loop workers at a depth, every write timed in its parts
//!
//! X10's (`shoal-spike-rows/src/drive.rs`), with a write in up to three parts and the wait before
//! it apart. A cell is a number of workers, its depth, each sending one write, waiting for its
//! answer and sending the next. Each worker owns its keys, so a conditional commit is never
//! refused by a sibling and a refusal is a defect. A write's latency is counted when it completes
//! inside the window that follows the warm-up, and the cell's rate is those completions over the
//! window.
//!
//! A write is timed from its first send to its acknowledgement, in parts where it has them: the
//! read of the stripe's row, the stage to two holders of three, and the commit. The wait before
//! it, for its holders' gates or its key's last deferred work, is counted apart and never in its
//! latency ([`crate::deferred`]). And every write the cell completed, warm-up and tail included,
//! is counted once more, exactly: that is what every figure a write is divided by, since the
//! counters around a cell see all of them.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shoal::shared::queries::ConditionRefusal;
use shoal::Errors;
use tokio::sync::Mutex;

use crate::stats::Samples;

/// What one write was answered
#[derive(Debug, Clone)]
pub enum Answer {
    /// Applied
    Applied,
    /// A conditional commit refused, and why
    Refused(ConditionRefusal),
    /// Anything else, with what was said
    Failed(String),
}

impl Answer {
    /// What a send's answer comes to
    ///
    /// # Arguments
    ///
    /// * `sent` - What the client answered
    #[must_use]
    pub fn of<T>(sent: &Result<T, Errors>) -> Answer {
        match sent {
            Ok(_) => Answer::Applied,
            // a refused condition fails its response by name, with why
            Err(Errors::Refused { reason, .. }) => Answer::Refused(reason.clone()),
            Err(error) => Answer::Failed(format!("{error:?}")),
        }
    }
}

/// What one write came to, and how long it and each of its parts took
#[derive(Debug, Clone)]
pub struct Outcome {
    /// What it was answered
    pub answer: Answer,
    /// How long the whole write took, from its first send to its acknowledgement
    pub took: Duration,
    /// How long it waited before its first send, for gates and its key's last deferred work
    pub wait: Duration,
    /// How long its read of the row took, for a write that reads first
    pub read: Option<Duration>,
    /// How long its stage to two holders of three took, for a staged write
    pub stage: Option<Duration>,
    /// How long its commit took, for a write that commits after a read
    pub commit: Option<Duration>,
}

impl Outcome {
    /// An outcome of a write in one part
    ///
    /// # Arguments
    ///
    /// * `answer` - What it was answered
    /// * `took` - How long it took
    /// * `wait` - How long it waited first
    #[must_use]
    pub fn whole(answer: Answer, took: Duration, wait: Duration) -> Self {
        Outcome {
            answer,
            took,
            wait,
            read: None,
            stage: None,
            commit: None,
        }
    }
}

/// One write, ready to be awaited
pub type Op = Pin<Box<dyn Future<Output = Outcome> + Send>>;

/// A worker's source of writes
pub trait Work: Send + 'static {
    /// The worker's next write, or none when it has nothing left to write
    ///
    /// # Arguments
    ///
    /// * `last` - What its previous write came to, none before its first
    fn next(&mut self, last: Option<&Outcome>) -> Option<Op>;
}

/// How long a cell warms up and how long it is measured
#[derive(Debug, Clone, Copy)]
pub struct Plan {
    /// How long answers are not counted, from the first send
    pub warmup: Duration,
    /// How long answers are counted, after the warm-up
    pub window: Duration,
}

/// When a cell's window is, which work a write leaves behind is counted against too
#[derive(Debug, Clone, Copy)]
pub struct Window {
    /// When it starts
    pub from: Instant,
    /// When it ends
    pub end: Instant,
}

impl Window {
    /// The window of a plan that starts now
    ///
    /// # Arguments
    ///
    /// * `plan` - The warm-up and the window
    #[must_use]
    pub fn of(plan: Plan) -> Self {
        let from = Instant::now() + plan.warmup;
        Window {
            from,
            end: from + plan.window,
        }
    }

    /// Whether a moment is inside the window
    ///
    /// # Arguments
    ///
    /// * `at` - The moment
    #[must_use]
    pub fn holds(&self, at: Instant) -> bool {
        at >= self.from && at <= self.end
    }
}

/// What a cell did inside its window, and how many writes it completed in all
#[derive(Debug, Clone, Default)]
pub struct Cell {
    /// Writes applied
    pub applied: u64,
    /// Writes refused
    pub refused: u64,
    /// Writes that failed
    pub failed: u64,
    /// The first few failures and refusals, as they were said
    pub errors: Vec<String>,
    /// The seconds the count is over: the window
    pub secs: f64,
    /// Every write's latency
    pub latency: Samples,
    /// Every read's, for writes that read first
    pub read: Samples,
    /// Every stage's to two holders of three, for staged writes
    pub stage: Samples,
    /// Every commit's, for writes that commit after a read
    pub commit: Samples,
    /// Every wait before a write
    pub wait: Samples,
    /// Every write the cell completed, from its first send to its last answer, whatever it was
    /// answered
    pub all_completed: u64,
}

impl Cell {
    /// Writes a second inside the window, whatever they were answered
    #[must_use]
    pub fn per_sec(&self) -> f64 {
        // nothing counted over no time is no rate
        if self.secs <= 0.0 {
            return 0.0;
        }
        (self.applied + self.refused + self.failed) as f64 / self.secs
    }
}

/// Run a cell: every worker sends, waits and sends again until the window ends
///
/// # Arguments
///
/// * `window` - The window, which started its warm-up when it was made
/// * `workers` - The workers, one write in flight a worker
pub async fn run(window: Window, workers: Vec<Box<dyn Work>>) -> Cell {
    let shared = Arc::new(Mutex::new(Cell::default()));
    let mut tasks = Vec::with_capacity(workers.len());
    for work in workers {
        tasks.push(tokio::spawn(drive_worker(work, shared.clone(), window)));
    }
    // every worker to its end
    for task in tasks {
        let _ = task.await;
    }
    let mut cell = Arc::try_unwrap(shared)
        .map(Mutex::into_inner)
        .unwrap_or_else(|_| panic!("every worker has ended"));
    cell.secs = window.end.duration_since(window.from).as_secs_f64();
    cell
}

/// Drive one worker until the window ends, then fold what it saw in
///
/// # Arguments
///
/// * `work` - The worker
/// * `shared` - Its cell
/// * `window` - When answers are counted
async fn drive_worker(mut work: Box<dyn Work>, shared: Arc<Mutex<Cell>>, window: Window) {
    // a worker's own figures, merged once at its end
    let mut mine = Cell::default();
    let mut last: Option<Outcome> = None;
    while Instant::now() < window.end {
        // the next write, or the end of this worker's
        let Some(op) = work.next(last.as_ref()) else {
            break;
        };
        let outcome = op.await;
        let done = Instant::now();
        mine.all_completed += 1;
        // counted only when it completes inside the window
        if window.holds(done) {
            mine.latency.push(outcome.took);
            mine.wait.push(outcome.wait);
            for (part, samples) in [
                (outcome.read, &mut mine.read),
                (outcome.stage, &mut mine.stage),
                (outcome.commit, &mut mine.commit),
            ] {
                if let Some(took) = part {
                    samples.push(took);
                }
            }
            match &outcome.answer {
                Answer::Applied => mine.applied += 1,
                Answer::Refused(reason) => {
                    mine.refused += 1;
                    if mine.errors.len() < 5 {
                        mine.errors.push(format!("refused: {reason:?}"));
                    }
                }
                Answer::Failed(error) => {
                    mine.failed += 1;
                    if mine.errors.len() < 5 {
                        mine.errors.push(error.clone());
                    }
                }
            }
        }
        last = Some(outcome);
    }
    // folded into the cell
    let mut cell = shared.lock().await;
    cell.latency.extend(&mine.latency);
    cell.read.extend(&mine.read);
    cell.stage.extend(&mine.stage);
    cell.commit.extend(&mine.commit);
    cell.wait.extend(&mine.wait);
    cell.applied += mine.applied;
    cell.refused += mine.refused;
    cell.failed += mine.failed;
    cell.all_completed += mine.all_completed;
    for error in mine.errors {
        if cell.errors.len() < 5 {
            cell.errors.push(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worker of writes that each take a fixed time
    struct Fixed {
        /// How long each takes
        takes: Duration,
    }

    impl Work for Fixed {
        /// The next write: a sleep
        fn next(&mut self, _last: Option<&Outcome>) -> Option<Op> {
            let takes = self.takes;
            Some(Box::pin(async move {
                tokio::time::sleep(takes).await;
                Outcome::whole(Answer::Applied, takes, Duration::ZERO)
            }))
        }
    }

    /// Every write the cell completed is counted, warm-up and tail included, while the window
    /// counts only its own
    #[tokio::test]
    async fn every_completion_is_counted_and_the_window_only_its_own() {
        let plan = Plan {
            warmup: Duration::from_millis(100),
            window: Duration::from_millis(200),
        };
        let workers: Vec<Box<dyn Work>> = (0..2)
            .map(|_| {
                Box::new(Fixed {
                    takes: Duration::from_millis(10),
                }) as Box<dyn Work>
            })
            .collect();
        let cell = run(Window::of(plan), workers).await;
        // about thirty writes a worker in all, about twenty of them in the window
        let counted = cell.applied;
        assert!(cell.all_completed > counted, "{} against {counted}", cell.all_completed);
        assert!((25..=45).contains(&counted), "{counted}");
        assert!((50..=70).contains(&cell.all_completed), "{}", cell.all_completed);
        assert_eq!(cell.latency.len(), counted);
        assert!((cell.secs - 0.2).abs() < 1e-9);
    }
}
