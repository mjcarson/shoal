//! An async mutex for one thread
//!
//! On a single thread a mutex cannot be contended by another thread, only by another task that
//! holds the guard across an await. So this is a flag, the value, and the wakers of the tasks
//! waiting for the flag to clear. A lock attempt checks the flag on every poll rather than only
//! when woken, which is what lets a task polled by hand - the way the conformance suite polls
//! one - see a guard that was dropped between polls.

use std::cell::{Cell, RefCell, RefMut, UnsafeCell};
use std::collections::VecDeque;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use openraft_rt::{Mutex, OptionalSend};

/// An async mutex
pub struct GlommioMutex<T> {
    /// Whether a guard exists
    locked: Cell<bool>,
    /// The value
    value: UnsafeCell<T>,
    /// Every lock attempt waiting for the guard to be dropped, oldest first, one waker each
    ///
    /// A slot per attempt rather than a waker per poll, so an attempt polled again replaces its
    /// waker and one abandoned takes its slot away. With a waker per poll, an abandoned
    /// attempt's waker stayed behind and the next drop woke it instead of a task still waiting,
    /// which then slept with the mutex free ([Resolved #158](../../../../../docs/src/appendix/resolved/runtime-waker-lists.md)).
    waiters: RefCell<VecDeque<(u64, Waker)>>,
    /// The id the next waiting attempt is given
    next_waiter: Cell<u64>,
}

impl<T: OptionalSend + 'static> Mutex<T> for GlommioMutex<T> {
    type Guard<'a>
        = Guard<'a, T>
    where
        Self: 'a;

    /// A mutex around a value
    fn new(value: T) -> Self {
        GlommioMutex {
            locked: Cell::new(false),
            value: UnsafeCell::new(value),
            waiters: RefCell::new(VecDeque::new()),
            next_waiter: Cell::new(0),
        }
    }

    /// Take the guard, waiting for whoever holds it
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + OptionalSend {
        Lock {
            mutex: self,
            slot: None,
            done: false,
        }
    }
}

impl<T> GlommioMutex<T> {
    /// Wake the attempt that has waited longest, taking its slot
    ///
    /// It takes the guard when it is polled, or, if it is abandoned first, hands the wake on.
    fn wake_next(&self) {
        let next = self.waiters.borrow_mut().pop_front();
        if let Some((_, waker)) = next {
            waker.wake();
        }
    }
}

/// A lock attempt in progress
struct Lock<'a, T> {
    /// The mutex being locked
    mutex: &'a GlommioMutex<T>,
    /// This attempt's slot among the waiters, once it has waited
    slot: Option<u64>,
    /// Whether this attempt took the guard
    done: bool,
}

impl<T> Unpin for Lock<'_, T> {}

impl<'a, T> Future for Lock<'a, T> {
    type Output = Guard<'a, T>;

    /// Take the guard if nobody holds it, or wait
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mutex = self.mutex;
        // free, so take it, and give up any slot this attempt was still holding
        if !mutex.locked.get() {
            mutex.locked.set(true);
            if let Some(slot) = self.slot.take() {
                mutex.waiters.borrow_mut().retain(|(id, _)| *id != slot);
            }
            self.done = true;
            return Poll::Ready(Guard { mutex });
        }
        // held, so wait for the guard to drop: in this attempt's slot if it still has one
        let mut waiters = mutex.waiters.borrow_mut();
        if let Some(slot) = self.slot {
            if let Some((_, waker)) = waiters.iter_mut().find(|(id, _)| *id == slot) {
                waker.clone_from(cx.waker());
                return Poll::Pending;
            }
        }
        // a first wait, or one whose slot a drop already took and woke, queues at the back
        let slot = mutex.next_waiter.get();
        mutex.next_waiter.set(slot.wrapping_add(1));
        waiters.push_back((slot, cx.waker().clone()));
        self.slot = Some(slot);
        Poll::Pending
    }
}

impl<T> Drop for Lock<'_, T> {
    /// Give up this attempt's place, and pass on a wake it was given and will not use
    fn drop(&mut self) {
        // an attempt that took the guard holds no place
        if self.done {
            return;
        }
        // one that never waited holds none either
        let Some(slot) = self.slot else {
            return;
        };
        // still queued: leave the queue
        let mut waiters = self.mutex.waiters.borrow_mut();
        let before = waiters.len();
        waiters.retain(|(id, _)| *id != slot);
        let queued = waiters.len() != before;
        drop(waiters);
        // not queued means a drop took this slot to wake it; the mutex may be free with others
        // waiting, and this attempt will never take it, so the next one is woken instead
        if !queued && !self.mutex.locked.get() {
            self.mutex.wake_next();
        }
    }
}

/// The guard, which is the lock
pub struct Guard<'a, T> {
    /// The mutex this guard holds
    mutex: &'a GlommioMutex<T>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    /// The value
    fn deref(&self) -> &T {
        // SAFETY: the guard exists only while `locked` is set, which this thread set and no
        // other thread can see, so this is the one reference to the value
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T> DerefMut for Guard<'_, T> {
    /// The value, to change
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as above, and `&mut self` makes this the one mutable path through the guard
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T> Drop for Guard<'_, T> {
    /// Release the lock and wake the waiter that has waited longest
    ///
    /// One rather than all: a mutex hands itself to one task at a time, and waking the rest
    /// would have them find it held and queue again. If the one woken is abandoned before it
    /// is polled, its drop wakes the next.
    fn drop(&mut self) {
        self.mutex.locked.set(false);
        self.mutex.wake_next();
    }
}

/// Unused, but keeps `RefMut` in scope for the reader wondering why the guard is not one
///
/// A `RefMut` guard would work on one thread too, but `lock_owned` in the trait's default
/// implementation transmutes the guard to `'static`, and a guard that is a plain reference into
/// the mutex is the shape that transmute was written for.
#[allow(dead_code)]
type NotUsed<'a, T> = RefMut<'a, T>;

#[cfg(test)]
mod tests {
    use super::GlommioMutex;
    use futures::FutureExt;
    use glommio::{LocalExecutorBuilder, Placement};
    use openraft_rt::Mutex;
    use std::rc::Rc;
    use std::time::Duration;

    /// A waiter is woken when the guard drops, even if a waiter behind it gave up
    ///
    /// openraft takes this mutex inside a replication stream it drops whenever a session ends,
    /// so a lock attempt that is abandoned while it waits is ordinary. Its waker was left in
    /// the list, the next drop woke it instead of the waiter still there, and that waiter slept
    /// with the mutex free.
    #[test]
    fn a_waiter_is_woken_past_an_abandoned_one() {
        let executor = LocalExecutorBuilder::new(Placement::Unbound)
            .make()
            .expect("failed to build a glommio executor");
        executor.run(async {
            let mutex = Rc::new(GlommioMutex::new(0u32));
            // hold the guard, so the next two attempts wait
            let guard = mutex.lock().await;
            // one task waits for it
            let waiting = mutex.clone();
            let waiter = glommio::spawn_local(async move {
                *waiting.lock().await += 1;
            })
            .detach();
            glommio::timer::sleep(Duration::from_millis(10)).await;
            // a second attempt waits once and is abandoned
            assert!(mutex.lock().now_or_never().is_none());
            // the guard drops, which has to reach the task still waiting
            drop(guard);
            let finished = glommio::timer::timeout(Duration::from_secs(1), async {
                Ok(waiter.await)
            })
            .await;
            assert!(
                finished.is_ok(),
                "the waiting task was never woken with the mutex free"
            );
            assert_eq!(*mutex.lock().await, 1);
        });
    }

    /// A waiter woken by a drop and abandoned before it runs hands the wake on
    #[test]
    fn an_abandoned_wake_is_handed_on() {
        let executor = LocalExecutorBuilder::new(Placement::Unbound)
            .make()
            .expect("failed to build a glommio executor");
        executor.run(async {
            let mutex = Rc::new(GlommioMutex::new(0u32));
            let guard = mutex.lock().await;
            // the first attempt waits, by hand, so it can be dropped after it is woken
            let mut first = Box::pin(mutex.lock());
            assert!((&mut first).now_or_never().is_none());
            // a task waits behind it
            let waiting = mutex.clone();
            let second = glommio::spawn_local(async move {
                *waiting.lock().await += 1;
            })
            .detach();
            glommio::timer::sleep(Duration::from_millis(10)).await;
            // the drop wakes the first, which is abandoned without being polled
            drop(guard);
            drop(first);
            let finished = glommio::timer::timeout(Duration::from_secs(1), async {
                Ok(second.await)
            })
            .await;
            assert!(finished.is_ok(), "the wake died with the abandoned attempt");
        });
    }

    /// A waiter polled many times while it waits leaves one waker
    #[test]
    fn a_waiter_polled_again_keeps_one_waker() {
        let mutex = GlommioMutex::new(0u32);
        let guard = mutex.lock().now_or_never().expect("the mutex is free");
        // one attempt, polled a thousand times while the guard is held
        let mut attempt = Box::pin(mutex.lock());
        for _ in 0..1000 {
            assert!((&mut attempt).now_or_never().is_none());
        }
        let waiters = mutex.waiters.borrow().len();
        assert!(waiters <= 1, "one attempt left {waiters} wakers");
        drop(attempt);
        drop(guard);
    }
}
