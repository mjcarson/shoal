//! One device flush for every shard's WAL on a device
//!
//! A shard's WAL writes each batch with a direct write into a segment whose blocks were written
//! before it was opened, so the batch's bytes need no filesystem metadata to be found again, only
//! the device's volatile cache flushed ([F60](../../../../docs/src/features/shared-wal-flush.md)).
//! A device flush is device wide: one `fdatasync` on any file of the filesystem, issued after a
//! direct write completed, makes that write durable whatever file it went to. So the shards of a
//! node whose WALs share a device share their flushes here, rather than each issuing its own.
//!
//! On the lab's Zen1 hosts a flush is the 970 EVO emptying its cache, 7 to 11 ms whether it
//! carries 16 KiB or 256 KiB, and six shards' writers issuing one each saturated the device at
//! 450 to 630 flushes a second ([O64](../../../../docs/src/appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)).
//!
//! # Invariants
//!
//! **A write is durable only through a flush that started after the write completed.** A caller
//! takes the number of the next flush to start, and waits for a flush of at least that number
//! to end; the flush in flight when it asked started before its write and never counts.
//!
//! **One flush is in flight on a device at a time**, issued by whichever waiting shard finds none
//! in flight, on its own file. Every waiter is woken when one ends, and the ones it did not cover
//! elect the next flusher among themselves.
//!
//! **A failed flush poisons the device's group for good.** Linux reports a writeback error once,
//! so a later flush that succeeds says nothing about the writes the failed one covered; every
//! caller from then on is refused, which stops the WAL the way a failed sync of its own does
//! ([Resolved #156](../../../../docs/src/appendix/resolved/wal-failure-stops-the-node.md)).
//!
//! **No lock is held across an `.await`.** The state is behind a `std` mutex shared by every
//! executor on the node, taken only to read and change counters and wakers.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use glommio::io::DmaFile;

/// A device, as the major and minor ids of the filesystem's device a file is on
pub type DeviceId = (u32, u32);

/// Every device's flush group on this node, created on first use
static GROUPS: OnceLock<Mutex<HashMap<DeviceId, Arc<FlushGroup>>>> = OnceLock::new();

/// What a device's flushes have done so far
#[derive(Debug, Default)]
struct FlushState {
    /// The number of the last flush started
    started: u64,
    /// The number of the last flush that ended
    done: u64,
    /// Whether a flush is in flight
    in_flight: bool,
    /// Bumped whenever a flush ends, which is what a waiter waits to see move
    epoch: u64,
    /// The error a failed flush ended with, which refuses every caller from then on
    poisoned: Option<String>,
    /// The waiters to wake when a flush ends
    waiters: Vec<Waker>,
    /// Flushes issued, for the WAL's figures
    flushes: u64,
    /// Syncs asked of the group, for the WAL's figures
    asked: u64,
}

/// The shared flushes of one device's WAL writers
#[derive(Debug, Default)]
pub struct FlushGroup {
    /// The state every executor on the node reads and changes
    state: Mutex<FlushState>,
}

/// What a caller does next, decided under the lock
enum Step {
    /// A flush it can count has ended: durable, or refused
    Done(io::Result<()>),
    /// No flush is in flight: issue one, numbered this
    Lead(u64),
    /// A flush is in flight: wait for the epoch to move past this
    Wait(u64),
}

impl FlushGroup {
    /// The flush group of a device, created on first use
    ///
    /// # Arguments
    ///
    /// * `device` - The device
    #[must_use]
    pub fn of(device: DeviceId) -> Arc<FlushGroup> {
        // one map for the node, whatever executor asks first
        let groups = GROUPS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut groups = groups.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        groups.entry(device).or_default().clone()
    }

    /// The device a file is on
    ///
    /// # Arguments
    ///
    /// * `file` - The file
    #[must_use]
    pub fn device_of(file: &DmaFile) -> DeviceId {
        (file.dev_major(), file.dev_minor())
    }

    /// Lock the state, whatever a panicking holder left it as
    fn lock(&self) -> std::sync::MutexGuard<'_, FlushState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The flushes issued and the syncs asked of this group so far
    #[must_use]
    pub fn counts(&self) -> (u64, u64) {
        let state = self.lock();
        (state.flushes, state.asked)
    }

    /// Make every direct write this caller completed before now durable
    ///
    /// Waits for a flush of the device that started after the call, issuing one on `file` when
    /// none is in flight.
    ///
    /// # Arguments
    ///
    /// * `file` - A file on the device, the caller's own, to flush through if it leads
    pub async fn sync(&self, file: &DmaFile) -> io::Result<()> {
        self.sync_with(|| file.fdatasync()).await
    }

    /// [`Self::sync`] with the flush itself given, which is what a test counts
    ///
    /// # Arguments
    ///
    /// * `flush` - Issues one flush of the device
    pub async fn sync_with<F, Fut>(&self, flush: F) -> io::Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = glommio::Result<(), ()>>,
    {
        // the first flush to start from here on is the one this caller needs
        let need = {
            let mut state = self.lock();
            state.asked += 1;
            state.started + 1
        };
        loop {
            // decide under the lock and act outside it
            let step = {
                let mut state = self.lock();
                if let Some(error) = &state.poisoned {
                    Step::Done(Err(io::Error::other(format!(
                        "a flush of this device failed: {error}"
                    ))))
                } else if state.done >= need {
                    Step::Done(Ok(()))
                } else if state.in_flight {
                    Step::Wait(state.epoch)
                } else {
                    state.started += 1;
                    state.in_flight = true;
                    state.flushes += 1;
                    Step::Lead(state.started)
                }
            };
            match step {
                Step::Done(result) => return result,
                Step::Lead(number) => {
                    // the flush, and then everyone waiting told it ended
                    let flushed = flush().await;
                    let waiters = {
                        let mut state = self.lock();
                        state.done = number;
                        state.in_flight = false;
                        state.epoch += 1;
                        if let Err(error) = flushed {
                            state.poisoned = Some(error.to_string());
                        }
                        std::mem::take(&mut state.waiters)
                    };
                    for waker in waiters {
                        waker.wake();
                    }
                }
                Step::Wait(epoch) => {
                    EpochMoved {
                        group: self,
                        epoch,
                    }
                    .await;
                }
            }
        }
    }
}

/// A future that resolves once a flush ends after the epoch it was made at
struct EpochMoved<'a> {
    /// The group
    group: &'a FlushGroup,
    /// The epoch seen when the caller chose to wait
    epoch: u64,
}

impl Future for EpochMoved<'_> {
    type Output = ();

    /// Ready once the epoch moved, else registered to be woken when it does
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.group.lock();
        if state.epoch != self.epoch {
            return Poll::Ready(());
        }
        state.waiters.push(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::FlushGroup;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    /// Callers that ask while a flush is in flight share the next one, and nobody counts the
    /// flush that was in flight when it asked
    #[test]
    fn waiters_share_the_next_flush_and_never_the_one_in_flight() {
        glommio::LocalExecutorBuilder::default()
            .spawn(|| async move {
                let group = Arc::new(FlushGroup::default());
                let flushes = Rc::new(Cell::new(0u32));
                // a flush that takes a while, so the others arrive during it
                let flush = {
                    let flushes = flushes.clone();
                    move || {
                        let flushes = flushes.clone();
                        async move {
                            flushes.set(flushes.get() + 1);
                            glommio::timer::sleep(Duration::from_millis(20)).await;
                            Ok(())
                        }
                    }
                };
                // the first caller leads the first flush
                let first = {
                    let group = group.clone();
                    let flush = flush.clone();
                    glommio::spawn_local(async move { group.sync_with(flush).await })
                };
                glommio::timer::sleep(Duration::from_millis(5)).await;
                // five more arrive while it is in flight
                let mut rest = Vec::new();
                for _ in 0..5 {
                    let group = group.clone();
                    let flush = flush.clone();
                    rest.push(glommio::spawn_local(async move { group.sync_with(flush).await }));
                }
                first.await.expect("the first flush");
                for task in rest {
                    task.await.expect("the shared flush");
                }
                // one flush for the first, one after it for the five, never six
                assert_eq!(flushes.get(), 2);
                assert_eq!(group.counts(), (2, 6));
            })
            .expect("an executor")
            .join()
            .expect("the test");
    }

    /// A failed flush refuses every caller from then on, including ones a later flush would cover
    #[test]
    fn a_failed_flush_poisons_the_group() {
        glommio::LocalExecutorBuilder::default()
            .spawn(|| async move {
                let group = FlushGroup::default();
                let failed = group
                    .sync_with(|| async {
                        Err(glommio::GlommioError::IoError(std::io::Error::other("eio")))
                    })
                    .await;
                assert!(failed.is_err());
                // a flush that would work is never issued again
                let after = group.sync_with(|| async { Ok(()) }).await;
                assert!(after.is_err());
                assert_eq!(group.counts(), (1, 2));
            })
            .expect("an executor")
            .join()
            .expect("the test");
    }

    /// Two executors' writers share one device's flushes across threads
    #[test]
    fn executors_on_other_threads_share_flushes() {
        let group = Arc::new(FlushGroup::default());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let group = group.clone();
                glommio::LocalExecutorBuilder::default()
                    .spawn(move || async move {
                        for _ in 0..50 {
                            group
                                .sync_with(|| async {
                                    glommio::timer::sleep(Duration::from_millis(1)).await;
                                    Ok(())
                                })
                                .await
                                .expect("a flush");
                        }
                    })
                    .expect("an executor")
            })
            .collect();
        for handle in handles {
            handle.join().expect("a writer");
        }
        let (flushes, asked) = group.counts();
        assert_eq!(asked, 200);
        // shared, so fewer than one a sync, and at least one per sync's worth of rounds
        assert!(flushes < asked, "{flushes} flushes for {asked} syncs");
        assert!(flushes >= 50, "{flushes}");
    }
}
