//! An async mutex for one thread
//!
//! On a single thread a mutex cannot be contended by another thread, only by another task that
//! holds the guard across an await. So this is a flag, the value, and the wakers of the tasks
//! waiting for the flag to clear. A lock attempt checks the flag on every poll rather than only
//! when woken, which is what lets a task polled by hand - the way the conformance suite polls
//! one - see a guard that was dropped between polls.

use std::cell::{Cell, RefCell, RefMut, UnsafeCell};
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
    /// Every task waiting for the guard to be dropped
    waiters: RefCell<Vec<Waker>>,
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
            waiters: RefCell::new(Vec::new()),
        }
    }

    /// Take the guard, waiting for whoever holds it
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + OptionalSend {
        Lock { mutex: self }
    }
}

/// A lock attempt in progress
struct Lock<'a, T> {
    /// The mutex being locked
    mutex: &'a GlommioMutex<T>,
}

impl<T> Unpin for Lock<'_, T> {}

impl<'a, T> Future for Lock<'a, T> {
    type Output = Guard<'a, T>;

    /// Take the guard if nobody holds it, or wait
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // free, so take it
        if !self.mutex.locked.get() {
            self.mutex.locked.set(true);
            return Poll::Ready(Guard { mutex: self.mutex });
        }
        // held, so wait for the guard to drop
        self.mutex.waiters.borrow_mut().push(cx.waker().clone());
        Poll::Pending
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
    /// Release the lock and wake one waiter
    ///
    /// One rather than all: a mutex hands itself to one task at a time, and waking the rest
    /// would have them find it held and queue again. If the one woken has gone away the next
    /// drop wakes the next, since every waiter registers on every poll.
    fn drop(&mut self) {
        self.mutex.locked.set(false);
        let waiter = self.mutex.waiters.borrow_mut().pop();
        if let Some(waker) = waiter {
            waker.wake();
        }
    }
}

/// Unused, but keeps `RefMut` in scope for the reader wondering why the guard is not one
///
/// A `RefMut` guard would work on one thread too, but `lock_owned` in the trait's default
/// implementation transmutes the guard to `'static`, and a guard that is a plain reference into
/// the mutex is the shape that transmute was written for.
#[allow(dead_code)]
type NotUsed<'a, T> = RefMut<'a, T>;
