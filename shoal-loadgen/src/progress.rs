//! What a running benchmark tells whoever is watching it, and what it is told back
//!
//! The run sends [`BenchEvent`]s over a bounded tokio channel with `try_send`: a screen that
//! falls behind loses a second's numbers, never stalls the run, and the capture is built from
//! the run's own records rather than from these. It is told to stop through a
//! `tokio::sync::watch` of [`Control`].

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::events::Mark;
use crate::results::SecondSample;
use crate::spec::ArmPlan;
use crate::window::WindowSummary;

/// What a run is doing between arms, and inside one
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Reading and checking the dataset
    Scan,
    /// Building the programs the cluster runs
    Build,
    /// Bringing the bench's cluster up
    Bootstrap,
    /// Wiping and bringing it up again between arms
    Reset,
    /// Restarting every node so reads start cold
    Restart,
    /// Loading the preload
    Preload {
        /// Rows inserted so far
        done: u64,
        /// Rows to insert
        total: u64,
    },
    /// Running an arm before it is measured
    Warmup,
    /// Measuring an arm
    Measure,
    /// Reading back what an arm's inserts were acknowledged for
    Verify,
    /// Tearing the cluster down
    Teardown,
    /// Putting the hosts back as they were found
    Restore,
}

impl Phase {
    /// What the phase is called on a screen
    #[must_use]
    pub fn label(&self) -> String {
        // a preload shows how far it got
        match self {
            Phase::Scan => "scan".to_string(),
            Phase::Build => "build".to_string(),
            Phase::Bootstrap => "bootstrap".to_string(),
            Phase::Reset => "reset".to_string(),
            Phase::Restart => "restart".to_string(),
            Phase::Preload { done, total } => format!("preload {done}/{total}"),
            Phase::Warmup => "warmup".to_string(),
            Phase::Measure => "measure".to_string(),
            Phase::Verify => "verify".to_string(),
            Phase::Teardown => "teardown".to_string(),
            Phase::Restore => "restore".to_string(),
        }
    }
}

/// Something a running benchmark wants shown
#[derive(Debug, Clone)]
pub enum BenchEvent {
    /// The arms the run will go through, in order
    Planned {
        /// The capture's label
        label: String,
        /// Every arm of every run
        arms: Vec<ArmPlan>,
    },
    /// The run moved to another phase
    Phase(Phase),
    /// An arm started
    ArmStarted {
        /// Its place in the plan
        index: usize,
        /// The arm
        arm: ArmPlan,
        /// Its warmup, in seconds
        warmup: u64,
        /// Its measured time, in seconds
        duration: u64,
    },
    /// The arm's last second
    Second(SecondSample),
    /// Something was done to the cluster
    Mark(Mark),
    /// An arm finished
    ArmDone {
        /// Its place in the plan
        index: usize,
        /// Its measured numbers
        summary: WindowSummary,
        /// Why it ended before its time, if it did
        ended_early: Option<String>,
    },
    /// A line worth reading
    Log(String),
    /// The run is over: where its capture is, or why it failed
    Finished(Result<PathBuf, String>),
}

/// What a running benchmark is told
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    /// Carry on
    Run,
    /// Stop as soon as it is safe and put everything back
    Abort,
}

/// The sending half of a run's progress, which never blocks the run
#[derive(Debug, Clone)]
pub struct Progress {
    /// The channel to whoever is watching, if anyone is
    tx: Option<tokio::sync::mpsc::Sender<BenchEvent>>,
    /// How many events were dropped because the watcher fell behind
    dropped: Arc<AtomicU64>,
}

impl Progress {
    /// Progress sent to a watcher
    ///
    /// # Arguments
    ///
    /// * `tx` - The channel to send on
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<BenchEvent>) -> Self {
        Progress {
            tx: Some(tx),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Progress nobody watches
    #[must_use]
    pub fn none() -> Self {
        Progress {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Send an event if there is room for it, and count it if there is not
    ///
    /// # Arguments
    ///
    /// * `event` - The event to send
    pub fn send(&self, event: BenchEvent) {
        // a full or closed channel loses the event, never the run's time
        if let Some(tx) = &self.tx {
            if tx.try_send(event).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Send a line worth reading
    ///
    /// # Arguments
    ///
    /// * `line` - The line
    pub fn log(&self, line: impl Into<String>) {
        self.send(BenchEvent::Log(line.into()));
    }

    /// How many events were dropped
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
