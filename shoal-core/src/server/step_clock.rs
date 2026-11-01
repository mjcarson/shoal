//! A step's clock: how long a step held its shard's loop by the wall, and how much of that its
//! thread spent on its own cpu ([F130](../../../docs/src/features/loop-steps-by-kind.md))
//!
//! A walk page holds its shard's loop in steps, a capture and its encode steps, and W12 fails a
//! scan whose step passes 100 µs. A step can pass it by its own work, or because its thread was
//! off its cpu for part of it: preempted by the kernel's io_uring workers bound to the same cpu
//! ([O147](../../../docs/src/appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu))
//! or any other thread. The wall cannot tell them apart; the thread's own cpu clock can.
//!
//! The thread's clock is `CLOCK_THREAD_CPUTIME_ID`, which the kernel brings up to date as it is
//! read, to the nanosecond. It has no vDSO path, so each read is a system call: 0.45 µs on europa
//! and 0.62 µs on a Zen1 host, measured 2026-11-01, two a step.
//!
//! # Invariants
//!
//! **A clock is started and stopped on one thread with nothing awaited between**: a thread's cpu
//! clock is its own, and a step that awaited would count its wait as its wall. Both steps it times
//! are synchronous on their loop.
//!
//! **A step's own cpu is never more than its wall**: the cpu is read after the wall at the start
//! and before it at the stop, so its interval lies inside the wall's, and it is clamped besides. A
//! clock that cannot read the thread's cpu counts a step's cpu as its wall, so no step is ever
//! taken for one its thread was preempted in without the evidence of it.

use std::time::{Duration, Instant};

use crate::shared::protocol::stats::StepSample;

/// The cpu time the calling thread has spent since it began, in nanoseconds, or none if the kernel
/// would not say
#[must_use]
pub fn thread_cpu_ns() -> Option<u64> {
    // SAFETY: a zeroed timespec is a valid value of a plain C struct, and only an out parameter
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: the clock is one every Linux has, and the pointer is to a timespec that lives across
    // the call; the call writes nothing else
    let read = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };
    if read != 0 {
        return None;
    }
    // a time before the thread began cannot be, but say none rather than wrap
    let secs = u64::try_from(now.tv_sec).ok()?;
    let nanos = u64::try_from(now.tv_nsec).ok()?;
    Some(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
}

/// When a step began, by the wall and by its thread's own cpu
#[derive(Debug, Clone, Copy)]
pub struct StepClock {
    /// When it began by the wall
    wall: Instant,
    /// The thread's cpu time when it began, if the kernel said
    cpu: Option<u64>,
}

/// How long a step held its loop, by the wall and on its thread's own cpu
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepTime {
    /// How long it held the loop by the wall
    pub wall: Duration,
    /// How much of that its thread spent on its own cpu, in nanoseconds, never more than its wall
    pub cpu_ns: u64,
}

impl StepClock {
    /// Start a step's clock: the wall first, then the thread's cpu
    #[must_use]
    pub fn start() -> Self {
        // the wall first, so the cpu's interval lies inside the wall's
        let wall = Instant::now();
        let cpu = thread_cpu_ns();
        StepClock { wall, cpu }
    }

    /// When the step began by the wall, which a step's own deadlines are set from
    /// ([item 350](../../../docs/src/appendix/resolved/encode-copy-deadline.md))
    #[must_use]
    pub fn began(&self) -> Instant {
        self.wall
    }

    /// Stop a step's clock: the thread's cpu first, then the wall
    #[must_use]
    pub fn stop(self) -> StepTime {
        // the cpu first, so its interval ends inside the wall's
        let cpu = thread_cpu_ns();
        let wall = self.wall.elapsed();
        let wall_ns = u64::try_from(wall.as_nanos()).unwrap_or(u64::MAX);
        // the thread's own share of it, never past the wall; the wall itself if either read failed
        let cpu_ns = match (self.cpu, cpu) {
            (Some(began), Some(ended)) => ended.saturating_sub(began).min(wall_ns),
            _ => wall_ns,
        };
        StepTime { wall, cpu_ns }
    }
}

impl StepTime {
    /// The step as its figures count it, with what it looked at and copied
    ///
    /// # Arguments
    ///
    /// * `keys` - The resident keys it looked at
    /// * `rows` - The rows it copied off the loop
    /// * `bytes` - The bytes it copied off the loop
    #[must_use]
    pub fn sample(&self, keys: u64, rows: u64, bytes: u64) -> StepSample {
        StepSample {
            wall_ns: u64::try_from(self.wall.as_nanos()).unwrap_or(u64::MAX),
            cpu_ns: self.cpu_ns,
            keys,
            rows,
            bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{thread_cpu_ns, StepClock};
    use std::time::Duration;

    /// A step that spins is on its own cpu for as long as it holds its loop, and one whose thread
    /// sleeps holds it by the wall with almost none of it on its cpu: the wall cannot tell a
    /// step's own work from a preemption, and the thread's clock can
    /// ([F130](../../../docs/src/features/loop-steps-by-kind.md))
    #[test]
    fn a_threads_own_cpu_leaves_out_its_sleep() {
        // the thread's clock reads, and moves only forward
        let first = thread_cpu_ns().expect("the thread's cpu clock reads");
        // a step that spins until its own cpu has moved 3 ms
        let clock = StepClock::start();
        let began = thread_cpu_ns().expect("the thread's cpu clock reads");
        while thread_cpu_ns().expect("the thread's cpu clock reads") < began + 3_000_000 {
            std::hint::spin_loop();
        }
        let spun = clock.stop();
        assert!(began >= first);
        let wall_ns = u64::try_from(spun.wall.as_nanos()).expect("a short wall");
        assert!(spun.cpu_ns >= 3_000_000, "a spin of 3 ms counted {} ns on its cpu", spun.cpu_ns);
        assert!(spun.cpu_ns <= wall_ns, "{} ns on its cpu past a wall of {wall_ns}", spun.cpu_ns);
        // a step whose thread sleeps 20 ms holds its loop by the wall, and nearly none on its cpu
        let clock = StepClock::start();
        std::thread::sleep(Duration::from_millis(20));
        let slept = clock.stop();
        assert!(slept.wall >= Duration::from_millis(20), "a sleep of 20 ms held {:?}", slept.wall);
        assert!(slept.cpu_ns < 2_000_000, "a sleep of 20 ms counted {} ns on its cpu", slept.cpu_ns);
        // and a sample carries both, with what the step held
        let sample = slept.sample(1, 40, 60_000);
        assert_eq!((sample.cpu_ns, sample.keys, sample.rows, sample.bytes), (slept.cpu_ns, 1, 40, 60_000));
        assert_eq!(u128::from(sample.wall_ns), slept.wall.as_nanos());
    }
}
