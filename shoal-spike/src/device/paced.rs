//! Work offered at a rate, and bytes held to a budget, for X7's cells that put a foreground beside
//! a background on one disk
//!
//! A foreground that waits for its own last operation before it issues the next is a closed loop,
//! and a closed loop under a slow disk simply asks less often: its tail hides the very wait being
//! measured. So every foreground here is open: an operation is due at its slot, a fixed interval
//! after the last one's slot, whether or not the last has finished, and its latency is counted from
//! the slot, as F72's paced stream counts it. A background that reads at a budget takes its bytes
//! from a bucket that fills at the budget's rate.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::stats::Samples;

/// Sleep on the executor until an instant, or not at all if it has passed
///
/// # Arguments
///
/// * `at` - The instant
pub async fn until(at: Instant) {
    let now = Instant::now();
    if at > now {
        glommio::timer::sleep(at - now).await;
    }
}

/// The edges of a cell's window: when its work starts, when counting starts, when it ends
#[derive(Debug, Clone, Copy)]
pub struct Window {
    /// When every task starts
    pub start: Instant,
    /// When counting starts, after the warm-up
    pub warm: Instant,
    /// When counting and issuing stop
    pub end: Instant,
}

impl Window {
    /// A window starting shortly, with a warm-up and a counted length
    ///
    /// # Arguments
    ///
    /// * `warm_for` - The warm-up
    /// * `count_for` - How long it is counted
    #[must_use]
    pub fn new(warm_for: Duration, count_for: Duration) -> Self {
        // a short lead, so every task is spawned before the first is due
        let start = Instant::now() + Duration::from_millis(200);
        Window { start, warm: start + warm_for, end: start + warm_for + count_for }
    }

    /// A window starting after a lead, with a warm-up and a counted length, for cells whose tasks
    /// open files on executors of their own before the first is due
    ///
    /// # Arguments
    ///
    /// * `lead` - How long after now it starts
    /// * `warm_for` - The warm-up
    /// * `count_for` - How long it is counted
    #[must_use]
    pub fn after(lead: Duration, warm_for: Duration, count_for: Duration) -> Self {
        let start = Instant::now() + lead;
        Window { start, warm: start + warm_for, end: start + warm_for + count_for }
    }

    /// The counted length in seconds
    #[must_use]
    pub fn secs(&self) -> f64 {
        self.end.duration_since(self.warm).as_secs_f64()
    }

    /// Whether an operation that began and ended at these instants is counted
    ///
    /// # Arguments
    ///
    /// * `began` - When it began, or was due
    /// * `ended` - When it ended
    #[must_use]
    pub fn counts(&self, began: Instant, ended: Instant) -> bool {
        began >= self.warm && ended <= self.end
    }
}

/// What an open loop saw
#[derive(Debug, Default)]
pub struct Paced {
    /// The latency of every counted operation, from its slot to its end
    pub samples: Samples,
    /// Operations due in the counted window
    pub due: usize,
    /// The most operations in flight at once
    pub most_in_flight: usize,
}

/// Issue an operation at every slot of a rate, each on a task of its own, and time each from
/// its slot
///
/// An operation due in the counted window is counted whenever it ends, so one that a slow disk
/// holds past the window's end is still in the tail; the loop waits for every one before it
/// returns.
///
/// # Arguments
///
/// * `rate` - Operations a second
/// * `window` - The cell's window; operations are due from its start to its end
/// * `op` - The operation, given its number
pub async fn open_loop<F, Fut>(rate: f64, window: Window, op: F) -> Paced
where
    F: Fn(u64) -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let op = Rc::new(op);
    let samples = Rc::new(RefCell::new(Samples::default()));
    let in_flight = Rc::new(Cell::new(0_usize));
    let most = Rc::new(Cell::new(0_usize));
    let mut due = 0;
    let mut nth = 0_u64;
    loop {
        // the slot, from the start and the rate, never from when the last one ended
        let slot = window.start + Duration::from_secs_f64(nth as f64 / rate);
        if slot >= window.end {
            break;
        }
        until(slot).await;
        if slot >= window.warm {
            due += 1;
        }
        in_flight.set(in_flight.get() + 1);
        most.set(most.get().max(in_flight.get()));
        let (op, samples, in_flight) = (op.clone(), samples.clone(), in_flight.clone());
        glommio::spawn_local(async move {
            op(nth).await;
            // counted from the slot, so a wait to be issued is part of the latency
            if slot >= window.warm {
                samples.borrow_mut().push(slot.elapsed());
            }
            in_flight.set(in_flight.get() - 1);
        })
        .detach();
        nth += 1;
    }
    // every operation issued is waited for, the slow ones included
    while in_flight.get() > 0 {
        glommio::timer::sleep(Duration::from_millis(1)).await;
    }
    let samples = samples.take();
    Paced { samples, due, most_in_flight: most.get() }
}

/// Bytes allowed at a rate, from a start: the scrub's budget
pub struct Bucket {
    /// Bytes a second, or `None` for no bound
    rate: Option<f64>,
    /// When the bucket started filling
    start: Instant,
    /// Bytes taken so far
    taken: u64,
}

impl Bucket {
    /// A bucket filling at a rate from now
    ///
    /// # Arguments
    ///
    /// * `mib_s` - The rate in MiB a second, or `None` for no bound
    /// * `start` - When it starts filling
    #[must_use]
    pub fn new(mib_s: Option<f64>, start: Instant) -> Self {
        Bucket { rate: mib_s.map(|mib| mib * f64::from(1 << 20)), start, taken: 0 }
    }

    /// Wait until a run of bytes is allowed, then take it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The run's length
    pub async fn take(&mut self, bytes: u64) {
        // the instant by which the bucket has filled with everything taken and this run
        if let Some(rate) = self.rate {
            let at = self.start + Duration::from_secs_f64((self.taken + bytes) as f64 / rate);
            until(at).await;
        }
        self.taken += bytes;
    }
}
