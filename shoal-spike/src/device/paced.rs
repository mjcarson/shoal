//! Work offered at a rate, and bytes held to a budget, for X7's and X12's cells that put a
//! foreground beside a background on one device
//!
//! A foreground that waits for its own last operation before it issues the next is a closed loop,
//! and a closed loop under a slow disk simply asks less often: its tail hides the very wait being
//! measured. So every foreground here is open: an operation is due at its slot, a fixed interval
//! after the last one's slot, whether or not the last has finished, and its latency is counted from
//! the slot, as F72's paced stream counts it. A background that reads at a budget takes its bytes
//! from a bucket that fills at the budget's rate.
//!
//! X12 adds what a rebuild's and a scrub's pacing needs. X7's `Bucket` keeps a schedule from its
//! start, so after a stall it issues back to back until it has caught up, which is exactly when the
//! foreground has just been slow; `Tokens` holds no more than one piece, so a stall is lost rather
//! than repaid. `Gauge` counts the foreground's operations in flight on the slice's executor, which
//! is what pacing by the arm's idle time waits on, and `Pacer` puts the two together as one of four
//! paces. And the foreground's slots can be drawn from a seeded Poisson process, so a periodic
//! foreground cannot fall into step with a periodic background.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use super::stats::{Rng, Samples};

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
    /// How late every counted operation started, from its slot to its first poll: the time the
    /// executor was held by something else. X7's loops leave it empty
    pub lag: Samples,
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
    Paced { samples, due, most_in_flight: most.get(), lag: Samples::default() }
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

/// Bytes allowed at a rate, holding at most a cap: X12's budget
///
/// The allowance fills at the rate up to the cap, and a run of bytes is charged when it is asked
/// for, so a run larger than what is held leaves a debt the caller waits out. A stall leaves the
/// allowance at the cap, one piece, so time lost to the foreground is never made up in a burst.
#[derive(Debug, Clone)]
pub struct Tokens {
    /// Bytes a second
    rate: f64,
    /// The most bytes held
    cap: f64,
    /// Bytes held now, below zero while a debt is owed
    have: f64,
    /// When `have` was last brought up to date
    at: Instant,
}

impl Tokens {
    /// An allowance at a rate, full to its cap from now
    ///
    /// # Arguments
    ///
    /// * `mib_s` - The rate, MiB a second
    /// * `cap` - The most bytes held, one piece
    /// * `now` - When it starts
    #[must_use]
    pub fn new(mib_s: f64, cap: u64, now: Instant) -> Self {
        Tokens { rate: mib_s * f64::from(1 << 20), cap: cap as f64, have: cap as f64, at: now }
    }

    /// Charge a run of bytes and say how long the caller waits before issuing it
    ///
    /// # Arguments
    ///
    /// * `now` - The time it is asked for
    /// * `bytes` - The run's length
    pub fn delay(&mut self, now: Instant, bytes: u64) -> Duration {
        // filled since it was last read, never past the cap
        let filled = now.saturating_duration_since(self.at).as_secs_f64() * self.rate;
        self.have = (self.have + filled).min(self.cap);
        self.at = now;
        // charged at once, so a run past what is held is a debt to wait out
        self.have -= bytes as f64;
        if self.have >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-self.have / self.rate)
        }
    }

    /// Wait until a run of bytes is allowed, then take it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The run's length
    pub async fn take(&mut self, bytes: u64) {
        let wait = self.delay(Instant::now(), bytes);
        if !wait.is_zero() {
            glommio::timer::sleep(wait).await;
        }
    }
}

/// The foreground's operations in flight on a slice's executor
///
/// An operation enters at its slot, before it is spawned, and leaves when it ends, so a background
/// that waits for the gauge to read zero issues only while the slice has no foreground work
/// outstanding. That is S11's pacing by the arm's idle time, measured on the one executor that
/// owns the slice.
#[derive(Debug, Default)]
pub struct Gauge {
    /// Operations in flight
    in_flight: Cell<usize>,
    /// Tasks waiting for it to read zero
    waiters: RefCell<Vec<Waker>>,
}

impl Gauge {
    /// An operation enters
    pub fn enter(&self) {
        self.in_flight.set(self.in_flight.get() + 1);
    }

    /// An operation leaves, waking every waiter once none is left
    pub fn leave(&self) {
        let left = self.in_flight.get().saturating_sub(1);
        self.in_flight.set(left);
        if left == 0 {
            for waker in self.waiters.borrow_mut().drain(..) {
                waker.wake();
            }
        }
    }

    /// Whether no operation is in flight
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.in_flight.get() == 0
    }

    /// Wait until no operation is in flight
    pub fn idle(&self) -> Idle<'_> {
        Idle { gauge: self }
    }
}

/// A wait for a gauge to read zero
pub struct Idle<'a> {
    /// The gauge
    gauge: &'a Gauge,
}

impl Future for Idle<'_> {
    type Output = ();

    /// Ready once nothing is in flight, else wait to be woken by the last to leave
    ///
    /// # Arguments
    ///
    /// * `cx` - The task's context
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.gauge.is_idle() {
            return Poll::Ready(());
        }
        self.gauge.waiters.borrow_mut().push(cx.waker().clone());
        Poll::Pending
    }
}

/// How a background's bytes are paced
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Pace {
    /// No background: the foreground alone
    None,
    /// A fixed budget, MiB a second
    Fixed(f64),
    /// A piece issued only while the foreground has nothing in flight, under a ceiling in MiB a
    /// second if one is given
    Idle(Option<f64>),
    /// As fast as it goes
    Unbounded,
}

impl Pace {
    /// Its name as a side is named
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Pace::None => "none".to_string(),
            Pace::Fixed(mib) => format!("fixed-{mib:.0}"),
            Pace::Idle(None) => "idle".to_string(),
            Pace::Idle(Some(mib)) => format!("idle-ceil-{mib:.0}"),
            Pace::Unbounded => "unbounded".to_string(),
        }
    }

    /// Its budget as a figure: MiB a second, zero for none, and -1 for no bound
    #[must_use]
    pub fn budget(&self) -> f64 {
        match self {
            Pace::None => 0.0,
            Pace::Fixed(mib) | Pace::Idle(Some(mib)) => *mib,
            Pace::Idle(None) | Pace::Unbounded => -1.0,
        }
    }
}

/// A pace applied to one background's pieces
pub struct Pacer {
    /// The pace
    pace: Pace,
    /// The allowance, for a fixed budget or a ceiling
    tokens: Option<Tokens>,
    /// The foreground's gauge, for pacing by idle time
    gauge: Rc<Gauge>,
}

impl Pacer {
    /// A pacer from now
    ///
    /// # Arguments
    ///
    /// * `pace` - The pace
    /// * `piece` - The largest piece, which caps the allowance
    /// * `gauge` - The foreground's gauge
    #[must_use]
    pub fn new(pace: Pace, piece: u64, gauge: Rc<Gauge>) -> Self {
        let tokens = match pace {
            Pace::Fixed(mib) | Pace::Idle(Some(mib)) => Some(Tokens::new(mib, piece, Instant::now())),
            _ => None,
        };
        Pacer { pace, tokens, gauge }
    }

    /// Wait until a piece of this many bytes may be issued
    ///
    /// # Arguments
    ///
    /// * `bytes` - The piece's length
    pub async fn admit(&mut self, bytes: u64) {
        // the budget or the ceiling first, charged once
        if let Some(tokens) = &mut self.tokens {
            tokens.take(bytes).await;
        }
        // then, pacing by idle time, the foreground's last operation out
        if matches!(self.pace, Pace::Idle(_)) {
            self.gauge.idle().await;
        }
    }
}

/// When an open loop's operations are due
#[derive(Debug, Clone, Copy)]
pub enum Arrivals {
    /// At a fixed interval, as X7's loops issue them
    #[cfg_attr(not(test), allow(dead_code))]
    Periodic,
    /// At the times of a Poisson process of the same rate, drawn from a seed
    Poisson(u64),
}

/// The slots of an open loop through a window
///
/// # Arguments
///
/// * `rate` - Operations a second
/// * `window` - The window; slots run from its start to its end
/// * `arrivals` - How the slots are spaced
#[must_use]
pub fn slots(rate: f64, window: Window, arrivals: Arrivals) -> Vec<Instant> {
    let mut slots = Vec::new();
    let mut rng = match arrivals {
        Arrivals::Poisson(seed) => Some(Rng::new(seed)),
        Arrivals::Periodic => None,
    };
    let mut offset = 0.0_f64;
    let mut nth = 0_u64;
    loop {
        // the next slot, from the last by the rate's interval or an exponential gap
        let slot = window.start + Duration::from_secs_f64(offset);
        if slot >= window.end {
            break;
        }
        slots.push(slot);
        nth += 1;
        offset = match &mut rng {
            Some(rng) => {
                // a uniform draw in (0, 1], so the logarithm is finite
                let uniform = (rng.next() >> 11) as f64 / (1_u64 << 53) as f64;
                offset - (1.0 - uniform).max(f64::MIN_POSITIVE).ln() / rate
            }
            None => nth as f64 / rate,
        };
    }
    slots
}

/// Issue an operation at every slot, each on a task of its own, and time each from its slot,
/// entering a gauge for as long as each is in flight
///
/// The operation is handed its number and its slot, so a part of it can be timed from the slot
/// too. Its lag, the time from its slot to its first poll, is the time the executor was held.
///
/// # Arguments
///
/// * `rate` - Operations a second
/// * `window` - The cell's window
/// * `arrivals` - How the slots are spaced
/// * `gauge` - The gauge every operation enters, if a background paces by it
/// * `op` - The operation, given its number and its slot
pub async fn open_loop_with<F, Fut>(rate: f64, window: Window, arrivals: Arrivals, gauge: Option<Rc<Gauge>>, op: F) -> Paced
where
    F: Fn(u64, Instant) -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let op = Rc::new(op);
    let samples = Rc::new(RefCell::new(Samples::default()));
    let lag = Rc::new(RefCell::new(Samples::default()));
    let in_flight = Rc::new(Cell::new(0_usize));
    let most = Rc::new(Cell::new(0_usize));
    let mut due = 0;
    for (nth, slot) in slots(rate, window, arrivals).into_iter().enumerate() {
        until(slot).await;
        if slot >= window.warm {
            due += 1;
        }
        // in flight from its slot, so a background waiting for idle sees it before it is polled
        in_flight.set(in_flight.get() + 1);
        most.set(most.get().max(in_flight.get()));
        if let Some(gauge) = &gauge {
            gauge.enter();
        }
        let (op, samples, lag, in_flight, gauge) = (op.clone(), samples.clone(), lag.clone(), in_flight.clone(), gauge.clone());
        glommio::spawn_local(async move {
            if slot >= window.warm {
                lag.borrow_mut().push(slot.elapsed());
            }
            op(nth as u64, slot).await;
            // counted from the slot, so a wait to be issued is part of the latency
            if slot >= window.warm {
                samples.borrow_mut().push(slot.elapsed());
            }
            in_flight.set(in_flight.get() - 1);
            if let Some(gauge) = &gauge {
                gauge.leave();
            }
        })
        .detach();
    }
    // every operation issued is waited for, the slow ones included
    while in_flight.get() > 0 {
        glommio::timer::sleep(Duration::from_millis(1)).await;
    }
    let (samples, lag) = (samples.take(), lag.take());
    Paced { samples, due, most_in_flight: most.get(), lag }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// After a stall the allowance holds one piece, so one goes at once and the next waits
    #[test]
    fn tokens_cap_no_catchup() {
        let start = Instant::now();
        let mut tokens = Tokens::new(10.0, 1 << 20, start);
        // the first piece goes at once, from the full allowance
        assert_eq!(tokens.delay(start, 1 << 20), Duration::ZERO);
        // a second of nothing refills one piece, not ten
        let later = start + Duration::from_secs(1);
        assert_eq!(tokens.delay(later, 1 << 20), Duration::ZERO);
        // so the next piece at the same instant waits a tenth of a second
        let wait = tokens.delay(later, 1 << 20);
        assert!((wait.as_secs_f64() - 0.1).abs() < 1e-6, "waited {wait:?}");
    }

    /// Taken back to back, an allowance runs at its rate
    #[test]
    fn tokens_rate() {
        let start = Instant::now();
        let mut tokens = Tokens::new(100.0, 1 << 20, start);
        let mut now = start;
        // a hundred pieces of a mebibyte, each issued when its wait ends
        for _ in 0..100 {
            now += tokens.delay(now, 1 << 20);
        }
        // the first was free, so ninety-nine pieces at 100 MiB/s
        let took = now.duration_since(start).as_secs_f64();
        assert!((took - 0.99).abs() < 1e-6, "took {took}");
    }

    /// A waiter on the gauge is woken by the last operation to leave, not the first
    #[test]
    fn gauge_wakes_on_idle() {
        let executor = glommio::LocalExecutorBuilder::default().make().expect("an executor");
        executor.run(async {
            let gauge = Rc::new(Gauge::default());
            gauge.enter();
            gauge.enter();
            let woke = Rc::new(Cell::new(false));
            let waiter = {
                let (gauge, woke) = (gauge.clone(), woke.clone());
                glommio::spawn_local(async move {
                    gauge.idle().await;
                    woke.set(true);
                })
            };
            // one leaves: still busy
            glommio::timer::sleep(Duration::from_millis(5)).await;
            gauge.leave();
            glommio::timer::sleep(Duration::from_millis(5)).await;
            assert!(!woke.get(), "woken while an operation was in flight");
            // the last leaves: the waiter runs
            gauge.leave();
            waiter.await;
            assert!(woke.get());
        });
    }

    /// The same seed draws the same slots, at about the rate asked for
    #[test]
    fn poisson_slots_seeded() {
        let window = Window::new(Duration::ZERO, Duration::from_secs(100));
        let one = slots(50.0, window, Arrivals::Poisson(7));
        let two = slots(50.0, window, Arrivals::Poisson(7));
        assert_eq!(one, two);
        // five thousand expected over the window; a Poisson count's deviation is about seventy
        assert!((4700..5300).contains(&one.len()), "{} slots", one.len());
        // and a periodic loop has exactly the rate's
        assert_eq!(slots(50.0, window, Arrivals::Periodic).len(), 5000);
    }
}
