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
//!
//! **A long step says what the kernel did to its thread inside it**
//! ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md)):
//! the page faults it took and the times it was switched off its cpu, from the thread's own
//! counters (`getrusage(RUSAGE_THREAD)`), which the kernel bumps as each happens. A fault's time is
//! the thread's own cpu on any kernel; an interrupt's is too on a kernel without irq time
//! accounting, as the lab's are, and nothing counts it a step, so a step past its bound on its own
//! cpu with no fault was held by its work or by the interrupts its cpu took. The counters are read
//! before the clock starts and, only for a step past 100 µs by the wall, after it stops, so neither
//! read is inside the time a step is judged on: one more system call a step, 0.40 µs on a Zen1 host
//! and 0.34 µs on europa.

use std::time::{Duration, Instant};

use crate::shared::protocol::stats::{CapturePart, CaptureParts, KernelEvents, StepSample, STEP_BOUNDS_US};

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

/// The page faults the calling thread has taken and the times it has been switched off its cpu
/// since it began, or none if the kernel would not say
/// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
#[must_use]
pub fn thread_kernel_events() -> Option<KernelEvents> {
    // SAFETY: a zeroed rusage is a valid value of a plain C struct, and only an out parameter
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: RUSAGE_THREAD names the calling thread on every Linux since 2.6.26, and the pointer
    // is to a rusage that lives across the call; the call writes nothing else
    let read = unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut usage) };
    if read != 0 {
        return None;
    }
    // each counter only grows; a negative one cannot be, so say none rather than wrap
    Some(KernelEvents {
        minor_faults: u64::try_from(usage.ru_minflt).ok()?,
        major_faults: u64::try_from(usage.ru_majflt).ok()?,
        voluntary_switches: u64::try_from(usage.ru_nvcsw).ok()?,
        involuntary_switches: u64::try_from(usage.ru_nivcsw).ok()?,
    })
}

/// The cpu the calling thread is pinned to, or none if it may run on more than one
/// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
///
/// A shard's executor is pinned to one cpu when it starts, so the cpu its steps ran on, and so the
/// interrupts they could take, is read once from its own affinity rather than at each step.
#[must_use]
pub fn pinned_cpu() -> Option<u32> {
    // SAFETY: a zeroed cpu_set_t is the empty set, a valid value of a plain C struct
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: pid 0 names the calling thread, and the size and the pointer are of a set that lives
    // across the call; the call writes nothing else
    let read = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if read != 0 {
        return None;
    }
    // the one cpu the set holds, or none if it holds more or none
    let mut pinned = None;
    for cpu in 0..libc::CPU_SETSIZE as usize {
        // SAFETY: the index is below the set's size, and the set was filled by the kernel
        if unsafe { libc::CPU_ISSET(cpu, &set) } {
            if pinned.is_some() {
                return None;
            }
            pinned = u32::try_from(cpu).ok();
        }
    }
    pinned
}

/// When a step began, by the wall and by its thread's own cpu, and what the kernel had done to its
/// thread by then
#[derive(Debug, Clone, Copy)]
pub struct StepClock {
    /// When it began by the wall
    wall: Instant,
    /// The thread's cpu time when it began, if the kernel said
    cpu: Option<u64>,
    /// The thread's faults and switches when it began, if the kernel said
    kernel: Option<KernelEvents>,
}

/// How long a step held its loop, by the wall and on its thread's own cpu, and what the kernel did
/// to its thread inside it if it was long
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepTime {
    /// How long it held the loop by the wall
    pub wall: Duration,
    /// How much of that its thread spent on its own cpu, in nanoseconds, never more than its wall
    pub cpu_ns: u64,
    /// The faults its thread took and the times it was switched off its cpu inside it: read only
    /// for a step past the first of [`STEP_BOUNDS_US`] by the wall, and zero for every other or
    /// when the kernel would not say
    pub kernel: KernelEvents,
}

impl StepClock {
    /// Start a step's clock: the thread's counters, outside the step, then the wall, then the
    /// thread's cpu
    #[must_use]
    pub fn start() -> Self {
        // the counters before the clock, so their read is never part of the step's time
        let kernel = thread_kernel_events();
        // the wall first, so the cpu's interval lies inside the wall's
        let wall = Instant::now();
        let cpu = thread_cpu_ns();
        StepClock { wall, cpu, kernel }
    }

    /// When the step began by the wall, which a step's own deadlines are set from
    /// ([item 350](../../../docs/src/appendix/resolved/encode-copy-deadline.md))
    #[must_use]
    pub fn began(&self) -> Instant {
        self.wall
    }

    /// Stop a step's clock: the thread's cpu first, then the wall, then, for a step past the first
    /// bound by the wall alone, the thread's counters outside it
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
        // what the kernel did inside it, read only for a long step, as every bound is judged
        let kernel = if wall_ns / 1_000 > STEP_BOUNDS_US[0] {
            match (self.kernel, thread_kernel_events()) {
                (Some(began), Some(ended)) => events_between(&began, &ended),
                _ => KernelEvents::default(),
            }
        } else {
            KernelEvents::default()
        };
        StepTime { wall, cpu_ns, kernel }
    }
}

/// What the kernel did to a thread between two reads of its counters
///
/// # Arguments
///
/// * `began` - The counters at the first read
/// * `ended` - The counters at the second
fn events_between(began: &KernelEvents, ended: &KernelEvents) -> KernelEvents {
    // each counter only grows, so the later less the earlier, never below zero
    KernelEvents {
        minor_faults: ended.minor_faults.saturating_sub(began.minor_faults),
        major_faults: ended.major_faults.saturating_sub(began.major_faults),
        voluntary_switches: ended.voluntary_switches.saturating_sub(began.voluntary_switches),
        involuntary_switches: ended.involuntary_switches.saturating_sub(began.involuntary_switches),
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
            kernel: self.kernel,
        }
    }
}

/// A capture's clock of its parts, by the wall alone: when its last part ended, and what each part
/// has taken so far
/// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
///
/// The wall is read through the vDSO, a few tens of nanoseconds a read; the thread's own cpu would
/// be a system call a part. The capture's step clock, a [`StepClock`] around it, still says its
/// own cpu, which is what the step clause judges; a part's wall is its own cpu for a capture its
/// thread was not switched out of.
#[derive(Debug, Clone, Copy)]
pub struct CaptureClock {
    /// When the last part ended, or the capture began
    last: Instant,
    /// What each part has taken
    parts: CaptureParts,
}

impl CaptureClock {
    /// Start a capture's clock of its parts, at the start of its first
    #[must_use]
    pub fn start() -> Self {
        CaptureClock {
            last: Instant::now(),
            parts: CaptureParts {
                captures: 1,
                ..CaptureParts::default()
            },
        }
    }

    /// End a part: what a test asked to lengthen it by, then its time since the last part ended
    ///
    /// # Arguments
    ///
    /// * `part` - The part that ended
    pub fn ended(&mut self, part: CapturePart) {
        // a test's stretch inside the part, so its time counts in it
        crate::server::faults::stretch_capture(part);
        // the part's time since the last one ended
        let now = Instant::now();
        let nanos = u64::try_from(now.saturating_duration_since(self.last).as_nanos()).unwrap_or(u64::MAX);
        self.parts.add_ns(part, nanos);
        self.last = now;
    }

    /// The capture's parts, with what its plan and its view took and how long its view's delta and
    /// runs took of it; its rest is its step's to say
    ///
    /// # Arguments
    ///
    /// * `planned` - The keys it planned or captured
    /// * `delta_entries` - The archive map's delta entries its view copied
    /// * `runs` - The archive map's runs its view made a cursor for
    /// * `view_ns` - How long its view's scan took to copy the delta and to make its run cursors
    #[must_use]
    pub fn parts(self, planned: usize, delta_entries: usize, runs: usize, view_ns: (u64, u64)) -> CaptureParts {
        let (view_delta_ns, view_runs_ns) = view_ns;
        CaptureParts {
            planned: planned as u64,
            delta_entries: delta_entries as u64,
            runs: runs as u64,
            view_delta_ns,
            view_runs_ns,
            ..self.parts
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{pinned_cpu, thread_cpu_ns, CaptureClock, StepClock};
    use crate::shared::protocol::stats::CapturePart;
    use crate::shared::protocol::stats::KernelEvents;
    use std::time::{Duration, Instant};

    /// The cpus the calling thread may run on
    fn allowed_cpus() -> Vec<usize> {
        // SAFETY: a zeroed cpu_set_t is the empty set, and the call fills it for this thread
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        // SAFETY: pid 0 names the calling thread, and the set lives across the call
        let read = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
        assert_eq!(read, 0, "the thread's affinity reads");
        // every cpu the set holds
        // SAFETY: each index is below the set's size, and the set was filled by the kernel
        (0..libc::CPU_SETSIZE as usize).filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) }).collect()
    }

    /// A step that touches fresh pages counts every page fault it took, read only because it held
    /// its loop past 100 µs; a short step reads no events at all, and a step whose thread sleeps
    /// counts the switch it gave its cpu up in
    /// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
    #[test]
    fn a_steps_page_faults_are_counted_past_its_bound() {
        // 8 MiB of fresh anonymous memory, in small pages so each touch is a fault of its own
        const BYTES: usize = 8 << 20;
        const PAGE: usize = 4096;
        // SAFETY: an anonymous private mapping of a length the call is given, nothing else named
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert_ne!(map, libc::MAP_FAILED, "an anonymous mapping");
        // SAFETY: the range is the mapping just made; a host that takes no advice only faults less
        unsafe { libc::madvise(map, BYTES, libc::MADV_NOHUGEPAGE) };
        // a step that writes one byte of every page, each write a minor fault
        let clock = StepClock::start();
        for offset in (0..BYTES).step_by(PAGE) {
            // SAFETY: the offset is inside the mapping, which is writable and lives until unmapped
            unsafe { std::ptr::write_volatile(map.cast::<u8>().add(offset), 1) };
        }
        let touched = clock.stop();
        // SAFETY: the mapping made above, unmapped once, with nothing pointing into it after
        unsafe { libc::munmap(map, BYTES) };
        assert!(touched.wall > Duration::from_micros(100), "2,048 faults held only {:?}", touched.wall);
        assert!(
            touched.kernel.minor_faults >= (BYTES / PAGE) as u64,
            "a step that touched {} fresh pages counted {:?}",
            BYTES / PAGE,
            touched.kernel
        );
        assert_eq!(touched.sample(0, 0, 0).kernel, touched.kernel);
        // a step under the bound reads no events: tried until one held its loop under it
        let short = (0..100)
            .map(|_| StepClock::start().stop())
            .find(|time| time.wall <= Duration::from_micros(100))
            .expect("a step that does nothing held its loop under 100 µs");
        assert_eq!(short.kernel, KernelEvents::default());
        // a step whose thread sleeps gives its cpu up at least once, and faults on nothing it touched
        let clock = StepClock::start();
        std::thread::sleep(Duration::from_millis(1));
        let slept = clock.stop();
        assert!(slept.kernel.voluntary_switches >= 1, "a sleep counted {:?}", slept.kernel);
    }

    /// A thread pinned to one cpu names it, as a shard's executor is pinned when it starts, and a
    /// thread that may run on several names none
    /// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
    #[test]
    fn a_pinned_thread_names_its_cpu() {
        // a cpu this test may run on
        let allowed = allowed_cpus();
        let cpu = *allowed.last().expect("a cpu to run on");
        // an executor pinned there the way a shard's is
        let named = glommio::LocalExecutorBuilder::new(glommio::Placement::Fixed(cpu))
            .name("pinned-cpu")
            .spawn(|| async { pinned_cpu() })
            .expect("an executor pinned to one cpu")
            .join()
            .expect("the executor ran");
        assert_eq!(named, u32::try_from(cpu).ok());
        // and this thread, which names one only if it is pinned to one
        let expected = if allowed.len() == 1 { u32::try_from(allowed[0]).ok() } else { None };
        assert_eq!(pinned_cpu(), expected);
    }

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

    /// A capture's parts are each timed from where the last one ended, so the time a part spends is
    /// counted in it and no other, and the capture says what its plan and its view took
    /// ([item 353](../../../docs/src/appendix/resolved/capture-allocations.md))
    #[test]
    fn a_captures_parts_are_timed_where_they_end() {
        // a capture whose plan spins 2 ms by the wall and whose keys and view do nothing
        let mut clock = CaptureClock::start();
        clock.ended(CapturePart::Keys);
        let began = Instant::now();
        while began.elapsed() < Duration::from_millis(2) {
            std::hint::spin_loop();
        }
        clock.ended(CapturePart::Plan);
        clock.ended(CapturePart::View);
        let parts = clock.parts(3, 40, 2, (1_000, 2_000));
        // its plan held it, and the parts that did nothing took far less
        assert!(parts.plan_ns >= 2_000_000, "a plan of 2 ms counted {} ns", parts.plan_ns);
        assert!(parts.keys_ns < 1_000_000 && parts.view_ns < 1_000_000, "{parts:?}");
        assert_eq!(parts.held_by(), CapturePart::Plan);
        // one capture, with what it planned and its view took, its rest its step's to say
        assert_eq!((parts.captures, parts.planned, parts.delta_entries, parts.runs), (1, 3, 40, 2));
        assert_eq!((parts.view_delta_ns, parts.view_runs_ns), (1_000, 2_000));
        assert_eq!(parts.rest_ns, 0);
        assert_eq!(parts.with_rest(parts.timed_ns() + 500).rest_ns, 500);
    }
}
