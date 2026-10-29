//! The loop's second inbox: steps a shard's background work takes on its loop, when the loop has
//! nothing else to do ([F117](../../../../docs/src/features/scan-class.md))
//!
//! Some of a background read has to run on the loop, since the partitions it reads belong to it: a
//! scan page's capture today, M23's encoding of resident partitions in steps next. Those steps wait
//! here rather than in the main inbox, and the loop takes one only once no message is waiting, so
//! no query queued before a step waits behind it. A shard whose inbox is never empty would starve
//! them, so a step also runs ahead of a waiting message once one is due: no sooner than
//! [`FORCED_EVERY`] after the last such step, nor than [`FORCED_DUTY`] times its hold, so a forced
//! step takes at most a fiftieth of a busy loop.
//!
//! # Invariants
//!
//! **Only the loop fills or drains this**, so it is a plain queue on the shard and no receive is
//! ever raced against another (`shoal-channel/tests/no_raced_receives.rs`).
//!
//! **A step never awaits**, and spawns its task with `spawn_local_into`, which only schedules it:
//! `spawn_local` would poll the task before it returned, under the step's borrows (item 242).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::walk::QueuedCapture;

/// The least time between two steps run ahead of a waiting message
pub const FORCED_EVERY: Duration = Duration::from_millis(10);

/// How many times a forced step's hold must pass before the next forced step, so forced steps
/// take at most one part in this many of a loop that is never idle
pub const FORCED_DUTY: u32 = 50;

/// One step a background read takes on the loop
#[derive(Debug)]
pub enum IdleStep {
    /// A scan page's capture, admitted and waiting for the loop to be idle
    Capture(Box<QueuedCapture>),
}

/// The steps waiting for the loop, and when one may next run ahead of a waiting message
#[derive(Debug)]
pub struct IdleSteps {
    /// The steps, in the order they were queued
    steps: VecDeque<IdleStep>,
    /// When the last step run ahead of a waiting message ended
    last_forced: Instant,
    /// How long that step held the loop
    last_held: Duration,
}

impl Default for IdleSteps {
    /// No step waiting, and the first forced step due a bound from now
    fn default() -> Self {
        IdleSteps {
            steps: VecDeque::new(),
            last_forced: Instant::now(),
            last_held: Duration::ZERO,
        }
    }
}

impl IdleSteps {
    /// Whether no step waits, which is all the loop asks while it is empty: one length check
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// How many steps wait
    #[must_use]
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Queue a step behind those waiting
    ///
    /// # Arguments
    ///
    /// * `step` - The step
    pub fn push(&mut self, step: IdleStep) {
        self.steps.push_back(step);
    }

    /// Take the step that waited longest
    pub fn pop(&mut self) -> Option<IdleStep> {
        self.steps.pop_front()
    }

    /// Take every waiting step, for a shard that is stopping to refuse
    pub fn drain(&mut self) -> impl Iterator<Item = IdleStep> + '_ {
        self.steps.drain(..)
    }

    /// Whether a step may run ahead of a message waiting in the main inbox
    ///
    /// # Arguments
    ///
    /// * `now` - The time now
    #[must_use]
    pub fn forced_due(&self, now: Instant) -> bool {
        // a bound since the last, and the duty its hold allows
        let wait = FORCED_EVERY.max(self.last_held * FORCED_DUTY);
        now.saturating_duration_since(self.last_forced) >= wait
    }

    /// Note that a step ran, and when, if it ran ahead of a waiting message
    ///
    /// # Arguments
    ///
    /// * `held` - How long it held the loop
    /// * `forced` - Whether a message was waiting
    /// * `now` - When it ended
    pub fn ran(&mut self, held: Duration, forced: bool, now: Instant) {
        // only a forced step moves the next forced step's time
        if forced {
            self.last_forced = now;
            self.last_held = held;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IdleSteps, FORCED_DUTY, FORCED_EVERY};
    use std::time::{Duration, Instant};

    /// A step runs ahead of a waiting message only once its bound is due: never sooner than ten
    /// milliseconds after the last forced step, nor than fifty times that step's hold
    #[test]
    fn an_idle_step_runs_ahead_of_a_queued_message_only_when_due() {
        // the queue's own clock starts when it is made, so the test's starts after
        let mut idle = IdleSteps::default();
        let start = Instant::now();
        // a fresh shard waits a bound before its first forced step
        assert!(!idle.forced_due(start));
        assert!(idle.forced_due(start + FORCED_EVERY));
        // a quick forced step: the next one a bound later
        let at = start + FORCED_EVERY;
        idle.ran(Duration::from_micros(20), true, at);
        assert!(!idle.forced_due(at + FORCED_EVERY / 2));
        assert!(idle.forced_due(at + FORCED_EVERY));
        // a step that held the loop 4 ms: the next forced one fifty holds later, 200 ms
        let later = at + FORCED_EVERY;
        idle.ran(Duration::from_millis(4), true, later);
        assert!(!idle.forced_due(later + Duration::from_millis(199)));
        assert!(idle.forced_due(later + Duration::from_millis(4) * FORCED_DUTY));
        // a step run while the inbox was empty moves nothing
        let quiet = later + Duration::from_millis(500);
        idle.ran(Duration::from_secs(1), false, quiet);
        assert!(idle.forced_due(quiet));
        assert!(idle.is_empty() && idle.len() == 0);
    }
}
