//! Closed-loop workers at a depth, every operation timed
//!
//! A cell is a number of workers, its depth, each sending one operation, waiting for its answer
//! and sending the next. Each worker owns its keys, so a conditional write is never refused by a
//! sibling and a refusal is a defect. An operation's latency is counted when it completes inside
//! the window that follows the warm-up, and the cell's rate is those completions over the window.
//!
//! An operation is one query, or a read and then a write, as S7's coordinator reads a stripe's
//! row before it commits. Its parts are timed apart, so a read's cost and a commit's are not
//! confused.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shoal::client::SendOptions;
use shoal::shared::protocol::read::ReadLevel;
use shoal::shared::queries::ConditionRefusal;
use shoal::shared::traits::QuerySupport;
use shoal::Errors;
use tokio::sync::Mutex;

use crate::cluster::Client;
use crate::stats::Samples;
use crate::RowsClient;

/// A query of the spike's schema, in the form a client sends
pub type Query = <RowsClient as QuerySupport>::QueryKinds;

/// What one operation was answered
#[derive(Debug, Clone)]
pub enum Answer {
    /// Applied, or for a read, answered with its rows
    Applied,
    /// A conditional write refused, and why
    Refused(ConditionRefusal),
    /// Anything else, with what was said
    Failed(String),
}

/// What one operation came to, and how long it and its first part took
#[derive(Debug, Clone)]
pub struct Outcome {
    /// What it was answered
    pub answer: Answer,
    /// How long the whole operation took
    pub took: Duration,
    /// How long its first part took, for an operation in two
    pub part: Option<Duration>,
}

/// One operation, ready to be awaited
pub type Op = Pin<Box<dyn Future<Output = Outcome> + Send>>;

/// A worker's source of operations
pub trait Work: Send + 'static {
    /// The worker's next operation, or none when it has no keys left
    ///
    /// # Arguments
    ///
    /// * `last` - What its previous operation came to, none before its first
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

/// What a cell did inside its window
#[derive(Debug, Clone, Default)]
pub struct Cell {
    /// Operations applied
    pub applied: u64,
    /// Operations refused
    pub refused: u64,
    /// Operations that failed
    pub failed: u64,
    /// The first few failures, as they were said
    pub errors: Vec<String>,
    /// The seconds the count is over: the window, or less when keys ran out
    pub secs: f64,
    /// Every operation's latency
    pub latency: Samples,
    /// Every operation's first part's latency, for an operation in two
    pub part: Samples,
    /// Every operation's latency after its first part, for an operation in two
    pub rest: Samples,
    /// Whether a worker ran out of keys before the window ended
    pub exhausted: bool,
}

impl Cell {
    /// Operations a second inside the window, whatever they were answered
    #[must_use]
    pub fn per_sec(&self) -> f64 {
        // nothing counted over no time is no rate
        if self.secs <= 0.0 {
            return 0.0;
        }
        (self.applied + self.refused + self.failed) as f64 / self.secs
    }
}

/// What every worker records into, shared
#[derive(Default)]
struct Shared {
    /// The cell so far
    cell: Cell,
    /// The last completion counted
    last: Option<Instant>,
}

/// Run a cell: every worker sends, waits and sends again until the window ends
///
/// # Arguments
///
/// * `plan` - The warm-up and the window
/// * `workers` - The workers, one an operation in flight
pub async fn run(plan: Plan, workers: Vec<Box<dyn Work>>) -> Cell {
    // one set of workers is one cell
    run_beside(plan, vec![workers]).await.pop().unwrap_or_default()
}

/// Run several sets of workers at once on one clock, each set counted as a cell of its own
///
/// This is how a writer is measured beside another: both run over the same window, and each
/// one's latencies are its own.
///
/// # Arguments
///
/// * `plan` - The warm-up and the window
/// * `sets` - The sets of workers, each one an operation in flight a worker
pub async fn run_beside(plan: Plan, sets: Vec<Vec<Box<dyn Work>>>) -> Vec<Cell> {
    // one clock for every worker of every set
    let started = Instant::now();
    let from = started + plan.warmup;
    let end = from + plan.window;
    let mut shared_sets = Vec::with_capacity(sets.len());
    let mut tasks = Vec::new();
    for workers in sets {
        let shared = Arc::new(Mutex::new(Shared::default()));
        for work in workers {
            tasks.push(tokio::spawn(drive_worker(work, shared.clone(), from, end)));
        }
        shared_sets.push(shared);
    }
    // every worker to its end
    for task in tasks {
        let _ = task.await;
    }
    shared_sets
        .into_iter()
        .map(|shared| {
            let mut shared = Arc::try_unwrap(shared)
                .map(Mutex::into_inner)
                .unwrap_or_else(|_| panic!("every worker has ended"));
            // a cell whose keys ran out is counted to its last completion
            shared.cell.secs = if shared.cell.exhausted {
                shared.last.map_or(0.0, |last| last.duration_since(from).as_secs_f64())
            } else {
                plan.window.as_secs_f64()
            };
            shared.cell
        })
        .collect()
}

/// Drive one worker until the window ends or its keys run out, then fold what it saw in
///
/// # Arguments
///
/// * `work` - The worker
/// * `shared` - Its cell
/// * `from` - When the window starts
/// * `end` - When it ends
async fn drive_worker(mut work: Box<dyn Work>, shared: Arc<Mutex<Shared>>, from: Instant, end: Instant) {
    // a worker's own latencies, merged once at its end
    let mut latency = Samples::default();
    let mut part = Samples::default();
    let mut rest = Samples::default();
    let mut counts = (0u64, 0u64, 0u64);
    let mut errors = Vec::new();
    let mut last_done = None;
    let mut exhausted = false;
    let mut last: Option<Outcome> = None;
    while Instant::now() < end {
        // the next operation, or the end of this worker's keys
        let Some(op) = work.next(last.as_ref()) else {
            exhausted = true;
            break;
        };
        let outcome = op.await;
        let done = Instant::now();
        // counted only when it completes inside the window
        if done >= from && done <= end {
            last_done = Some(done);
            latency.push(outcome.took);
            if let Some(first) = outcome.part {
                part.push(first);
                rest.push(outcome.took.saturating_sub(first));
            }
            match &outcome.answer {
                Answer::Applied => counts.0 += 1,
                Answer::Refused(_) => counts.1 += 1,
                Answer::Failed(error) => {
                    counts.2 += 1;
                    if errors.len() < 5 {
                        errors.push(error.clone());
                    }
                }
            }
        }
        last = Some(outcome);
    }
    // folded into the cell
    let mut shared = shared.lock().await;
    shared.cell.latency.extend(&latency);
    shared.cell.part.extend(&part);
    shared.cell.rest.extend(&rest);
    shared.cell.applied += counts.0;
    shared.cell.refused += counts.1;
    shared.cell.failed += counts.2;
    for error in errors {
        if shared.cell.errors.len() < 5 {
            shared.cell.errors.push(error);
        }
    }
    shared.cell.exhausted |= exhausted;
    shared.last = match (shared.last, last_done) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
}

/// What a send's answer comes to
///
/// # Arguments
///
/// * `sent` - What the client answered
fn answer_of<T>(sent: Result<T, Errors>) -> Answer {
    match sent {
        Ok(_) => Answer::Applied,
        // a refused condition fails its response by name, with why
        Err(Errors::Refused { reason, .. }) => Answer::Refused(reason),
        Err(error) => Answer::Failed(format!("{error:?}")),
    }
}

/// One write through one member
///
/// # Arguments
///
/// * `client` - The member it is sent through
/// * `query` - The write
#[must_use]
pub fn write(client: Client, query: Query) -> Op {
    Box::pin(async move {
        // timed from the send to the answer
        let started = Instant::now();
        let answer = answer_of(client.send_one(query).await);
        Outcome {
            answer,
            took: started.elapsed(),
            part: None,
        }
    })
}

/// One read through one member at a level
///
/// # Arguments
///
/// * `client` - The member it is sent through
/// * `query` - The read
/// * `level` - What it is served at
#[must_use]
pub fn read(client: Client, query: Query, level: ReadLevel) -> Op {
    Box::pin(async move {
        // timed from the send to the answer
        let started = Instant::now();
        let options = SendOptions::new().read(level);
        let answer = answer_of(client.send_one_with(query, &options).await);
        Outcome {
            answer,
            took: started.elapsed(),
            part: None,
        }
    })
}

/// A read through some members, then a write: S7's read of a stripe's row before its commit
///
/// The reads are sent one after another, each through its own member, so each member's copy
/// of the row is read, and the write follows once all are answered. The reads together are the
/// operation's first part.
///
/// # Arguments
///
/// * `readers` - The members the read is sent through, in order
/// * `query` - The read
/// * `level` - What it is served at
/// * `writer` - The member the write is sent through
/// * `write` - The write
#[must_use]
pub fn read_then_write(
    readers: Vec<Client>,
    query: Query,
    level: ReadLevel,
    writer: Client,
    write: Query,
) -> Op {
    Box::pin(async move {
        let started = Instant::now();
        let options = SendOptions::new().read(level);
        // every read, and the first failure among them ends the operation
        for reader in &readers {
            if let Answer::Failed(error) = answer_of(reader.send_one_with(query.clone(), &options).await) {
                return Outcome {
                    answer: Answer::Failed(format!("the read: {error}")),
                    took: started.elapsed(),
                    part: None,
                };
            }
        }
        let read = started.elapsed();
        // then the write the read was for
        let answer = answer_of(writer.send_one(write).await);
        Outcome {
            answer,
            took: started.elapsed(),
            part: Some(read),
        }
    })
}
