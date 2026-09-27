//! Admission into a group's log: a bounded number of writes in openraft, and a wait before it
//! that ends in a definite refusal
//!
//! A write on a cluster node waits in openraft's queue until its group commits it, and one that
//! does not commit within `replication.write_timeout` is answered `OutcomeUnknown`: the leader
//! may yet commit it, so the client may retry it only under the same identity. A client that
//! kept more writes outstanding than its groups commit in that time got exactly that for every
//! write at the back of the queue ([Resolved #129](../../../../docs/src/appendix/resolved/overload-sheds.md)).
//! The byte bound (`replication.pending_bytes`) never fired first at the lab's row sizes.
//!
//! The gate keeps the queue that can end in an unknown outcome short. At most
//! [`PROPOSALS_IN_FLIGHT`] of a group's writes are handed to openraft on a shard at once; the
//! rest wait in the gate, in order, where nothing has been appended. A write that waits there
//! longer than a quarter of its budget is refused `Shedding`, which is definite, so overload
//! comes back as refusals a client may retry at once rather than as timeouts. A write handed
//! to openraft has the rest of its budget and at most the bound's writes ahead of it.
//!
//! The bound follows what the group commits. Each write handed through reports how long it
//! spent in openraft: one that took more than three quarters of its budget halves the bound, and one
//! that took less raises it by one, so the queue in openraft holds about what the group commits
//! in that time whatever the host, the device or the load. It starts at
//! [`INITIAL_IN_FLIGHT`], which a group on the lab's hosts commits in a fraction of a second.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use tracing::{event, Level};

/// The most of a group's writes a shard hands to openraft at once
///
/// openraft batches whatever is queued into each append, so the bound only has to cover a
/// group's commit rate times its commit latency. The lab's hot keyword groups needed more than a
/// thousand at the loader's default gate ([Resolved #129](../../../../docs/src/appendix/resolved/overload-sheds.md)).
pub const PROPOSALS_IN_FLIGHT: usize = 4096;

/// How many of a group's writes a shard hands to openraft at once before it has seen any commit
///
/// No slow start: a burst to a fresh group is what a load begins with, and a group whose bound
/// started at 64 shed the lab's load at its default gate, which the tree before the gate took
/// with no retry at all.
pub const INITIAL_IN_FLIGHT: usize = 1024;

/// The fewest of a group's writes a shard hands to openraft at once, however slowly it commits
///
/// A group that commits nothing holds this many writes to an unknown outcome and no more.
pub const MIN_IN_FLIGHT: usize = 64;

/// The share of a write's budget it may spend in openraft before the bound is halved, as a
/// numerator over four
///
/// Three quarters: a commit that slow still ended inside the budget, and a write that also
/// waited its quarter at the gate would not have, so the queue in openraft is cut before one
/// ends unknown. Halving at half the budget cut the bound on every stall of a Zen1 host's
/// compactor, and with it the batches and the commit rate.
pub const SLOW_COMMIT_QUARTERS: u32 = 3;

/// The share of a write's budget it may spend waiting at the gate, as a divisor
///
/// A quarter: a write that leaves the gate has three quarters to commit in, and the bound is
/// halved long before a commit takes that.
pub const GATE_WAIT_DIVISOR: u32 = 4;

/// A group's gate on one shard
#[derive(Debug, Clone)]
pub struct ProposalGate {
    /// What the gate and its permits share
    inner: Rc<RefCell<GateInner>>,
}

/// What a gate and its permits share
#[derive(Debug)]
struct GateInner {
    /// How many writes the gate lets into openraft at once now
    bound: usize,
    /// The most it ever lets in at once
    max: usize,
    /// The fewest it ever lets in at once
    least: usize,
    /// When the bound was last halved, so one slow round halves it once
    halved: Option<Instant>,
    /// How many are in openraft now
    in_flight: usize,
    /// The writes waiting their turn, oldest first; a waiter that gave up has dropped its end
    waiting: VecDeque<oneshot::Sender<()>>,
    /// How many writes were refused at the gate
    shed: u64,
}

/// A write's place in openraft, given back when it is dropped
#[derive(Debug)]
pub struct ProposalPermit {
    /// The gate it was let through
    inner: Rc<RefCell<GateInner>>,
    /// When it was let through
    entered: Instant,
    /// How long it may take in openraft before the gate takes it as a sign of a queue too long
    slow_after: Duration,
}

impl Default for ProposalGate {
    /// A gate at the default bound
    fn default() -> Self {
        Self::new(PROPOSALS_IN_FLIGHT)
    }
}

impl ProposalGate {
    /// A gate that lets at most this many writes into openraft at once
    ///
    /// # Arguments
    ///
    /// * `max` - The most writes that may be in openraft at once, at least one
    #[must_use]
    pub fn new(max: usize) -> Self {
        let max = max.max(1);
        ProposalGate {
            inner: Rc::new(RefCell::new(GateInner {
                bound: INITIAL_IN_FLIGHT.min(max),
                max,
                least: MIN_IN_FLIGHT,
                halved: None,
                in_flight: 0,
                waiting: VecDeque::new(),
                shed: 0,
            })),
        }
    }

    /// Let a write through, waiting at most a quarter of its budget for its turn
    ///
    /// # Arguments
    ///
    /// * `budget` - What is left of the write's budget
    ///
    /// # Errors
    ///
    /// How long it waited, when its turn did not come in time; nothing was appended.
    pub async fn enter(&self, budget: Duration) -> Result<ProposalPermit, Duration> {
        // a quarter of the budget at the gate, and three quarters of it the mark of a slow commit
        let wait = budget / GATE_WAIT_DIVISOR;
        let slow_after = budget * SLOW_COMMIT_QUARTERS / 4;
        // a free place and nobody ahead: straight through
        let turn = {
            let mut inner = self.inner.borrow_mut();
            if inner.in_flight < inner.bound && inner.waiting.is_empty() {
                inner.in_flight += 1;
                None
            } else {
                // otherwise wait in line for a place handed over by a permit that is let go
                let (tx, rx) = oneshot::channel();
                inner.waiting.push_back(tx);
                Some(rx)
            }
        };
        let Some(mut turn) = turn else {
            return Ok(self.permit(slow_after));
        };
        // the place is handed over with the in-flight count already taken for this write
        let started = std::time::Instant::now();
        let handed = glommio::timer::timeout(wait, async { Ok((&mut turn).await.is_ok()) }).await;
        if matches!(handed, Ok(true)) {
            return Ok(self.permit(slow_after));
        }
        // out of time: closed first, so no place can be handed over from here on, and then
        // asked whether one was handed over before the close, which is this write's to keep
        turn.close();
        if let Ok(Some(())) = turn.try_recv() {
            return Ok(self.permit(slow_after));
        }
        self.inner.borrow_mut().shed += 1;
        Err(started.elapsed())
    }

    /// A permit on this gate, whose place is already counted
    ///
    /// # Arguments
    ///
    /// * `slow_after` - How long its write may take in openraft before that is a slow commit
    fn permit(&self, slow_after: Duration) -> ProposalPermit {
        ProposalPermit {
            inner: self.inner.clone(),
            entered: Instant::now(),
            slow_after,
        }
    }

    /// How many writes the gate lets into openraft at once now
    #[must_use]
    pub fn bound(&self) -> usize {
        self.inner.borrow().bound
    }

    /// How many writes are in openraft through this gate now
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.inner.borrow().in_flight
    }

    /// How many writes are waiting at the gate, counting any that gave up and are not yet
    /// passed over
    #[must_use]
    pub fn waiting(&self) -> usize {
        self.inner.borrow().waiting.len()
    }

    /// How many writes were refused at the gate since it was made
    #[must_use]
    pub fn shed(&self) -> u64 {
        self.inner.borrow().shed
    }
}

impl Drop for ProposalPermit {
    /// Move the bound by how long the write took, then give the place back or hand it to the
    /// oldest write still waiting
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        // a slow commit halves the bound, once a slow round; a quick one raises it by one
        let took = self.entered.elapsed();
        if took > self.slow_after {
            let recently = inner
                .halved
                .is_some_and(|halved| halved.elapsed() < self.slow_after);
            if !recently {
                inner.bound = (inner.bound / 2).max(inner.least.min(inner.max));
                inner.halved = Some(Instant::now());
                event!(Level::DEBUG, msg = "a slow commit halved a group's gate", ?took, bound = inner.bound, in_flight = inner.in_flight, waiting = inner.waiting.len());
            }
        } else if inner.bound < inner.max {
            inner.bound += 1;
        }
        // a place is handed on only while fewer than the bound are in openraft
        if inner.in_flight <= inner.bound {
            // a waiter that gave up has dropped its end and is passed over
            while let Some(next) = inner.waiting.pop_front() {
                if next.send(()).is_ok() {
                    // the place moves to that write with the count unchanged
                    return;
                }
            }
        }
        inner.in_flight = inner.in_flight.saturating_sub(1);
        // a raised bound lets in as many more waiters as it has room for
        while inner.in_flight < inner.bound {
            let Some(next) = inner.waiting.pop_front() else {
                break;
            };
            if next.send(()).is_ok() {
                inner.in_flight += 1;
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Run a future on a glommio executor of its own
    ///
    /// # Arguments
    ///
    /// * `future` - What to run
    fn run<F: std::future::Future<Output = ()>>(future: F) {
        glommio::LocalExecutor::default().run(future);
    }

    /// Writes past the bound wait, one is let through as each permit is let go, and one that
    /// waits too long is refused with nothing counted in flight for it
    #[test]
    fn a_full_gate_hands_places_over_and_sheds_the_late() {
        run(async {
            let gate = ProposalGate::new(2);
            // two places, both taken at once
            let first = gate.enter(Duration::ZERO).await.expect("a free place");
            let second = gate.enter(Duration::ZERO).await.expect("a free place");
            assert_eq!(gate.in_flight(), 2);
            // a third waits, and is refused when its wait runs out
            let late = gate.enter(Duration::from_millis(20)).await;
            assert!(late.is_err(), "a write past the bound was let through");
            assert_eq!(gate.shed(), 1);
            assert_eq!(gate.in_flight(), 2);
            // a fourth waits, and gets the place the first gives back
            let waiter = {
                let gate = gate.clone();
                glommio::spawn_local(async move { gate.enter(Duration::from_secs(5)).await })
            };
            glommio::timer::sleep(Duration::from_millis(10)).await;
            drop(first);
            let fourth = waiter.await.expect("the waiter got the freed place");
            assert_eq!(gate.in_flight(), 2);
            // every place given back leaves the gate empty
            drop(second);
            drop(fourth);
            assert_eq!(gate.in_flight(), 0);
            assert_eq!(gate.waiting(), 0);
        });
    }

    /// A write that gave up leaves its place in line to the next one rather than holding it
    #[test]
    fn a_waiter_that_gave_up_is_passed_over() {
        run(async {
            let gate = ProposalGate::new(1);
            let held = gate.enter(Duration::ZERO).await.expect("a free place");
            // one gives up while another is still waiting behind it
            assert!(gate.enter(Duration::from_millis(5)).await.is_err());
            let waiter = {
                let gate = gate.clone();
                glommio::spawn_local(async move { gate.enter(Duration::from_secs(5)).await })
            };
            glommio::timer::sleep(Duration::from_millis(10)).await;
            drop(held);
            let _next = waiter.await.expect("the live waiter got the place");
            assert_eq!(gate.in_flight(), 1);
        });
    }

    /// Quick commits raise the bound one at a time up to the most, and a slow one halves it
    /// once a slow round, never under the least
    #[test]
    fn the_bound_follows_how_long_commits_take() {
        run(async {
            let gate = ProposalGate::new(PROPOSALS_IN_FLIGHT);
            assert_eq!(gate.bound(), INITIAL_IN_FLIGHT);
            // quick commits under a long budget raise it by one each
            for _ in 0..10 {
                drop(gate.enter(Duration::from_secs(60)).await.expect("a free place"));
            }
            assert_eq!(gate.bound(), INITIAL_IN_FLIGHT + 10);
            // slow ones under a short budget halve it, once for the round
            let slow: Vec<_> = futures::future::join_all(
                (0..4).map(|_| gate.enter(Duration::from_millis(4))),
            )
            .await
            .into_iter()
            .map(|entered| entered.expect("a free place"))
            .collect();
            glommio::timer::sleep(Duration::from_millis(5)).await;
            drop(slow);
            assert_eq!(gate.bound(), (INITIAL_IN_FLIGHT + 10) / 2);
            // and never under the least, however many slow rounds
            for _ in 0..20 {
                let entered = gate.enter(Duration::from_millis(4)).await.expect("a free place");
                glommio::timer::sleep(Duration::from_millis(5)).await;
                drop(entered);
            }
            assert_eq!(gate.bound(), MIN_IN_FLIGHT);
        });
    }
}
