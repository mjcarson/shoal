//! The bound on work a write leaves behind it: B's apply and the inline path's fold
//!
//! A write is acknowledged before its holders apply or fold it, and that work loads their devices
//! while the next writes run. Left unbounded it would queue past anything a node could hold, and
//! the arm's rate would be the rate the acknowledgements came back at rather than the rate the
//! devices kept up with. So every holder has a gate of permits, twice the cell's depth: a write
//! takes one permit of each holder it touches before it starts, and a holder's permit comes back
//! when that holder's share of the write is durable, its stage and apply, or its fold. A worker
//! that finds a gate empty waits, and the wait is timed apart from the write's latency: deferred
//! work that cannot keep up slows the arm instead of growing a queue.
//!
//! Twice the depth lets a worker's next write overlap its last one's apply, as a holder's work
//! would overlap, and no further: at depth one the second write after an apply waits for it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One gate a holder, each of twice the cell's depth in permits
pub struct Gate {
    /// Each holder's permits, in the holders' order
    holders: Vec<Arc<Semaphore>>,
    /// The permits each holder's gate holds
    bound: usize,
}

impl Gate {
    /// A gate for each holder
    ///
    /// # Arguments
    ///
    /// * `holders` - How many holders
    /// * `bound` - The permits each holder's gate holds
    #[must_use]
    pub fn new(holders: usize, bound: usize) -> Arc<Gate> {
        Arc::new(Gate {
            holders: (0..holders).map(|_| Arc::new(Semaphore::new(bound))).collect(),
            bound,
        })
    }

    /// The permits each holder's gate holds
    #[must_use]
    pub fn bound(&self) -> usize {
        self.bound
    }

    /// Take one permit of every holder, returning them and how long the taking waited
    pub async fn enter(&self) -> (Vec<OwnedSemaphorePermit>, Duration) {
        let started = Instant::now();
        let mut permits = Vec::with_capacity(self.holders.len());
        // in the holders' order, so two writes never hold each other's permits waiting
        for gate in &self.holders {
            permits.push(gate.clone().acquire_owned().await.expect("a gate is never closed"));
        }
        (permits, started.elapsed())
    }

    /// Wait until every permit of every holder is back: every write's deferred work is durable
    ///
    /// Returns how long it waited.
    pub async fn drain(&self) -> Duration {
        let started = Instant::now();
        for gate in &self.holders {
            // every permit at once, then given straight back
            let all = u32::try_from(self.bound).expect("a bound fits");
            drop(gate.acquire_many(all).await.expect("a gate is never closed"));
        }
        started.elapsed()
    }

    /// How many of each holder's permits are out now, in the holders' order
    #[must_use]
    pub fn out(&self) -> Vec<usize> {
        self.holders
            .iter()
            .map(|gate| self.bound - gate.available_permits())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A writer that finds a gate empty waits until a permit comes back, and a drain returns
    /// only when every permit is back
    #[tokio::test]
    async fn an_empty_gate_holds_a_writer_until_a_permit_returns() {
        let gate = Gate::new(3, 2);
        // two writes' worth of permits taken at once
        let (first, waited) = gate.enter().await;
        assert!(waited < Duration::from_millis(50));
        let (second, _) = gate.enter().await;
        assert_eq!(gate.out(), vec![2, 2, 2]);
        // a third write waits until the first's permits come back
        let entering = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.enter().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!entering.is_finished(), "the third write went past an empty gate");
        drop(first);
        let (third, waited) = entering.await.expect("the third write enters");
        assert!(waited >= Duration::from_millis(40), "{waited:?}");
        // a drain waits for every permit
        let draining = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.drain().await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!draining.is_finished(), "a drain returned with permits out");
        drop(second);
        drop(third);
        draining.await.expect("the drain returns");
        assert_eq!(gate.out(), vec![0, 0, 0]);
    }
}
