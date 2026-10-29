//! A budget of memory carved out of `node_memory` for buffers that are not rows
//! ([F118](../../../docs/src/features/scan-budget.md))
//!
//! Object work holds memory that is not a row, and so does a scan: a page being read, the records
//! read off an archive for it, the answer waiting to be written. Without a budget each grows the
//! resident set past `node_memory`, and every shard then evicts forty percent of its rows for bytes
//! that are not theirs, item 191's failure with another guest
//! ([S13](../../../docs/src/object-storage/isolation.md#memory),
//! [W7](../../../docs/src/future-work/scans/isolation.md#memory)). So each kind of buffer is drawn
//! from a budget of its own, carved out of the node's memory before the rows' budget is: an object
//! executor's ([`crate::server::objects::budget`]), or a node's scans'.
//!
//! # Invariants
//!
//! **Nothing grows past a budget.** A draw that does not fit waits a bounded time, then is shed
//! by name, and the bytes it would have held are never allocated.
//!
//! **A draw is returned when what it held is dropped**: a [`Permit`] gives its bytes back, on
//! whichever thread drops it.
//!
//! **Each budget says its own sheds**: a budget's warnings are throttled by the budget, so one
//! budget shedding never silences another's.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{event, Level};

use super::objects::inflight::{Throttle, WARN_EVERY};

/// How long a draw that does not fit waits for room before it is shed
pub const DRAW_WAIT: Duration = Duration::from_millis(250);

/// A draw that found no room: what it needed, and what the budget holds at most
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shed {
    /// The bytes it asked for
    pub needed: u64,
    /// The bytes the budget may hold
    pub limit: u64,
}

impl Shed {
    /// Whether no wait could ever have satisfied the draw: it asked for more than the budget holds
    #[must_use]
    pub fn never_fits(&self) -> bool {
        self.needed > self.limit
    }
}

/// A budget of memory, every buffer of its kind drawn from it before it is allocated
#[derive(Debug)]
pub struct Budget {
    /// What the budget is, as its warnings name it, such as "an object executor's budget"
    name: &'static str,
    /// The bytes it may hold
    limit: u64,
    /// The bytes drawn now
    used: AtomicU64,
    /// The most bytes it ever held at once
    peak: AtomicU64,
    /// The draws shed since it was made
    shed: AtomicU64,
    /// Its warnings that a draw was shed, one logged every [`WARN_EVERY`] with the count of the rest
    warned: Throttle,
}

/// Bytes drawn from a budget, given back when dropped
#[derive(Debug)]
pub struct Permit {
    /// The budget
    budget: Arc<Budget>,
    /// The bytes
    bytes: u64,
}

impl Permit {
    /// The bytes this permit holds
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Permit {
    /// Give the bytes back
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl Budget {
    /// A budget of so many bytes
    ///
    /// # Arguments
    ///
    /// * `name` - What it is, as its warnings name it
    /// * `limit` - The bytes it may hold
    #[must_use]
    pub fn new(name: &'static str, limit: u64) -> Arc<Self> {
        Arc::new(Budget {
            name,
            limit,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            shed: AtomicU64::new(0),
            warned: Throttle::new(),
        })
    }

    /// Draw bytes if they fit now
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    #[must_use]
    pub fn try_draw(self: &Arc<Self>, bytes: u64) -> Option<Permit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let after = used.checked_add(bytes)?;
            if after > self.limit {
                return None;
            }
            match self
                .used
                .compare_exchange_weak(used, after, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    // the most it ever held, for a run's figures
                    self.peak.fetch_max(after, Ordering::Relaxed);
                    return Some(Permit {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(now) => used = now,
            }
        }
    }

    /// Draw bytes, waiting up to a bound for room, then shed by name
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    /// * `wait` - How long to wait for room
    ///
    /// # Errors
    ///
    /// [`Shed`] once the wait runs out, or at once for a draw past the whole budget.
    pub async fn draw(self: &Arc<Self>, bytes: u64, wait: Duration) -> Result<Permit, Shed> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(permit) = self.try_draw(bytes) {
                return Ok(permit);
            }
            // a draw no wait can ever satisfy is shed at once
            if bytes > self.limit || Instant::now() >= deadline {
                return Err(self.note_shed(bytes));
            }
            glommio::timer::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Count a draw that found no room, and say so once a while, since a full budget sheds every
    /// draw after it ([F108](../../../docs/src/features/object-work-in-flight.md))
    ///
    /// # Arguments
    ///
    /// * `bytes` - What the draw asked for
    pub fn note_shed(&self, bytes: u64) -> Shed {
        self.shed.fetch_add(1, Ordering::Relaxed);
        if let Some(besides) = self.warned.pass(WARN_EVERY) {
            // the object store's warning reads as it did before this budget was shared (F118)
            event!(
                Level::WARN,
                msg = %format_args!("{} shed a draw that found no room in its wait", self.name),
                needed = bytes,
                used = self.used(),
                limit = self.limit,
                shed_besides = besides,
            );
        }
        Shed {
            needed: bytes,
            limit: self.limit,
        }
    }

    /// What the budget is, as its warnings name it
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The bytes drawn now
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// The most bytes it ever held at once
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// The bytes it may hold
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// The draws shed since it was made
    #[must_use]
    pub fn shed(&self) -> u64 {
        self.shed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{Budget, DRAW_WAIT};
    use glommio::LocalExecutorBuilder;
    use std::time::Instant;

    /// A draw fits or it does not, a permit gives its bytes back on drop, and the peak is kept
    #[test]
    fn a_budget_holds_what_is_drawn_and_no_more() {
        let budget = Budget::new("a test's budget", 10 << 20);
        let first = budget.try_draw(6 << 20).expect("fits");
        assert!(budget.try_draw(6 << 20).is_none());
        assert_eq!(budget.used(), 6 << 20);
        assert_eq!(first.bytes(), 6 << 20);
        drop(first);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak(), 6 << 20);
    }

    /// A draw that never fits is shed at once, one that does not fit now after its wait, and two
    /// budgets count their sheds apart
    #[test]
    fn a_draw_is_shed_by_its_own_budget() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let budget = Budget::new("a test's budget", 1 << 20);
                let other = Budget::new("another test's budget", 1 << 20);
                // past the whole budget: at once, never fitting
                let started = Instant::now();
                let never = budget.draw(2 << 20, DRAW_WAIT).await.expect_err("drew past the budget");
                assert!(never.never_fits());
                assert!(started.elapsed() < DRAW_WAIT);
                // held full: shed after the wait, fitting once room is made
                let held = budget.try_draw(1 << 20).expect("fits");
                let started = Instant::now();
                let later = budget.draw(1, DRAW_WAIT).await.expect_err("drew past what was free");
                assert!(!later.never_fits());
                assert!(started.elapsed() >= DRAW_WAIT);
                drop(held);
                assert!(budget.draw(1, DRAW_WAIT).await.is_ok());
                assert_eq!((budget.shed(), other.shed()), (2, 0));
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }
}
