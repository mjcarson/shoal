//! Background work done in steps, giving its core up between them
//! ([F117](../../../docs/src/features/scan-class.md))
//!
//! A table shard's background reads - a scan's page today, M23's rebuilt walk next - run in the
//! shard's scan queue, whose latency goal is what makes a yield real: glommio gives a queue's core
//! up at a yield only once a preemption is due, and only a queue with a goal makes one come
//! ([S13](../../../docs/src/object-storage/isolation.md#shared-executors-or-dedicated-ones)). Each
//! step is at most [`STEP`] of work, the shape [X9](../../../docs/src/object-storage/table-latency.md)
//! measured holding the reference cell under its line at 100 MiB/s.
//!
//! # Invariants
//!
//! **A step is counted by the bytes it handled, never the rows it kept**, so work that keeps
//! little still yields as often as work that keeps much; and every handling counts at least
//! [`STEP_FLOOR`], so a run of empty partitions yields too.

/// The most work one step does before it offers its core up: 64 KiB, the object store's step
pub const STEP: usize = 64 << 10;

/// The least one handling counts toward a step, so many small or empty ones still end a step
pub const STEP_FLOOR: usize = 256;

/// The work done since a step last offered its core up
#[derive(Debug, Default)]
pub struct Steps {
    /// Whether these steps yield at all; a driver's page does not, as before F117
    yields: bool,
    /// The bytes handled since the last yield
    since: usize,
    /// How many times a step offered its core up
    offered: u64,
}

impl Steps {
    /// Steps that offer the core up every [`STEP`] of work, for work in a queue with a goal
    #[must_use]
    pub fn yielding() -> Self {
        Steps {
            yields: true,
            since: 0,
            offered: 0,
        }
    }

    /// Steps that never yield: a driver's page, scheduled exactly as it was before F117
    #[must_use]
    pub fn unyielding() -> Self {
        Steps::default()
    }

    /// Whether these steps yield
    #[must_use]
    pub fn yields(&self) -> bool {
        self.yields
    }

    /// How many times these steps offered their core up
    #[must_use]
    pub fn offered(&self) -> u64 {
        self.offered
    }

    /// Count some work done, and offer the core up once a step's worth is done since the last time
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes the work handled
    pub async fn take(&mut self, bytes: usize) {
        // steps that never yield count nothing
        if !self.yields {
            return;
        }
        self.since = self.since.saturating_add(bytes.max(STEP_FLOOR));
        if self.since >= STEP {
            self.since = 0;
            self.offered += 1;
            glommio::yield_if_needed().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Steps, STEP, STEP_FLOOR};
    use glommio::{Latency, LocalExecutorBuilder, Shares};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    /// A step offers its core up every 64 KiB, at least every few hundred empty handlings, and a
    /// driver's steps never do
    #[test]
    fn a_step_is_offered_up_every_64_kib() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let mut steps = Steps::yielding();
                for _ in 0..16 {
                    steps.take(STEP / 4).await;
                }
                assert_eq!(steps.offered(), 4);
                // nothing handled still counts the floor
                let mut empty = Steps::yielding();
                for _ in 0..(STEP / STEP_FLOOR) {
                    empty.take(0).await;
                }
                assert_eq!(empty.offered(), 1);
                // and a driver's never yield
                let mut driver = Steps::unyielding();
                driver.take(STEP * 8).await;
                assert_eq!(driver.offered(), 0);
                assert!(!driver.yields());
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }

    /// Work in steps in a queue of 100 shares at a 100 µs goal leaves a foreground task in a
    /// queue with a goal waiting for a step, never for the whole of the work
    ///
    /// glommio arms its preempt timer at an I/O poll for the queues active then, so work that
    /// starts just after a poll made while no queue with a goal was active runs under that poll's
    /// timer, 100 ms, until it fires once; every poll after it arms the scan queue's 100 µs. The
    /// foreground is asked after that first preemption, as it is on a shard whose scan has run.
    #[test]
    fn a_scan_step_yields_to_the_foreground() {
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let scan = glommio::executor().create_task_queue(
                    Shares::Static(100),
                    Latency::Matters(Duration::from_micros(100)),
                    "test-scan",
                );
                let high = glommio::executor().create_task_queue(
                    Shares::Static(1000),
                    Latency::Matters(Duration::from_micros(500)),
                    "test-high",
                );
                // 400 ms of work in steps, yielding as a scan's page does
                let done = Rc::new(Cell::new(false));
                let worker = {
                    let done = done.clone();
                    glommio::spawn_local_into(
                        async move {
                            let mut steps = Steps::yielding();
                            let until = Instant::now() + Duration::from_millis(400);
                            while Instant::now() < until {
                                // a step's worth of work that never awaits
                                let started = Instant::now();
                                while started.elapsed() < Duration::from_micros(20) {}
                                steps.take(STEP).await;
                            }
                            done.set(true);
                        },
                        scan,
                    )
                    .expect("the scan's task")
                };
                // a foreground task asked for past the first preemption, while the work runs
                glommio::timer::sleep(Duration::from_millis(150)).await;
                let asked = Instant::now();
                let ran = glommio::spawn_local_into(async move { asked.elapsed() }, high)
                    .expect("the foreground's task")
                    .await;
                assert!(!done.get(), "the work ended before the foreground was asked");
                // a step and a preemption at most, with room for a loaded host's scheduler, never
                // the 250 ms of work left
                assert!(ran < Duration::from_millis(20), "the foreground waited {ran:?}");
                worker.await;
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }
}
