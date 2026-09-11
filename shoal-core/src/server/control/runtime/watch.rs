//! A watch channel for one thread
//!
//! A single value that any number of receivers can read and wait for changes to. openraft
//! publishes its metrics through one, and its storage callbacks signal completion through
//! `send_if_modified` on one, so the semantics matter beyond the conformance suite:
//!
//! - a receiver remembers the last version it *saw*, and `changed` returns at once if the value
//!   has moved since, or waits until it does, or fails once the last sender is gone
//! - `borrow_watched` reads without marking anything seen; `borrow_and_update` reads and marks
//! - `send` fails when there is no receiver; `send_if_modified` never does, since it is how a
//!   callback completes whether or not anyone is waiting
//! - a receiver made by `subscribe` or `clone` has seen the value as it is now

use std::cell::{Ref, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use openraft_rt::watch::{RecvError, SendError};
use openraft_rt::{OptionalSend, OptionalSync, Watch, WatchReceiver, WatchSender};

/// The channel's state, shared by every handle to it
struct Shared<T> {
    /// The value
    value: T,
    /// How many times the value has been set; a receiver compares against it
    version: u64,
    /// How many senders exist; zero means no change will ever come
    senders: usize,
    /// How many receivers exist; zero means a send has nobody to reach
    receivers: usize,
    /// Every receiver waiting for a change
    wakers: Vec<Waker>,
}

impl<T> Shared<T> {
    /// Record a change and wake everyone waiting for one
    fn changed(&mut self) {
        self.version += 1;
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// The channel type, which is only a namespace for the trait
pub struct GlommioWatch;

impl Watch for GlommioWatch {
    type Sender<T: OptionalSend + OptionalSync> = Sender<T>;
    type Receiver<T: OptionalSend + OptionalSync> = Receiver<T>;
    type Ref<'a, T: OptionalSend + 'a> = Ref<'a, T>;

    /// Open a channel holding an initial value, which its first receiver has seen
    ///
    /// # Arguments
    ///
    /// * `init` - The initial value
    fn channel<T: OptionalSend + OptionalSync>(init: T) -> (Self::Sender<T>, Self::Receiver<T>) {
        let shared = Rc::new(RefCell::new(Shared {
            value: init,
            version: 0,
            senders: 1,
            receivers: 1,
            wakers: Vec::new(),
        }));
        (
            Sender {
                shared: shared.clone(),
            },
            Receiver { shared, seen: 0 },
        )
    }
}

/// A sender
pub struct Sender<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
}

impl<T> Clone for Sender<T> {
    /// Another sender
    fn clone(&self) -> Self {
        self.shared.borrow_mut().senders += 1;
        Sender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    /// One fewer sender; the last one gone wakes every receiver so they can fail
    fn drop(&mut self) {
        let mut shared = self.shared.borrow_mut();
        shared.senders -= 1;
        if shared.senders == 0 {
            for waker in shared.wakers.drain(..) {
                waker.wake();
            }
        }
    }
}

impl<T: OptionalSend + OptionalSync> WatchSender<GlommioWatch, T> for Sender<T> {
    /// Replace the value, failing if nobody could ever read it
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut shared = self.shared.borrow_mut();
        if shared.receivers == 0 {
            return Err(SendError(value));
        }
        shared.value = value;
        shared.changed();
        Ok(())
    }

    /// Let a closure change the value, and publish the change if it says it made one
    fn send_if_modified<F>(&self, modify: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        let mut shared = self.shared.borrow_mut();
        let modified = modify(&mut shared.value);
        if modified {
            shared.changed();
        }
        modified
    }

    /// Read the value
    fn borrow_watched(&self) -> Ref<'_, T> {
        Ref::map(self.shared.borrow(), |shared| &shared.value)
    }

    /// A receiver that has seen the value as it is now
    fn subscribe(&self) -> Receiver<T> {
        let mut shared = self.shared.borrow_mut();
        shared.receivers += 1;
        Receiver {
            shared: self.shared.clone(),
            seen: shared.version,
        }
    }
}

/// A receiver, which remembers what it has seen
pub struct Receiver<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
    /// The version this receiver last marked as seen
    seen: u64,
}

impl<T> Clone for Receiver<T> {
    /// Another receiver, having seen what this one has
    fn clone(&self) -> Self {
        self.shared.borrow_mut().receivers += 1;
        Receiver {
            shared: self.shared.clone(),
            seen: self.seen,
        }
    }
}

impl<T> Drop for Receiver<T> {
    /// One fewer receiver
    fn drop(&mut self) {
        self.shared.borrow_mut().receivers -= 1;
    }
}

impl<T: OptionalSend + OptionalSync> WatchReceiver<GlommioWatch, T> for Receiver<T> {
    /// Wait for a change since the last one seen, and mark it seen
    fn changed(&mut self) -> impl Future<Output = Result<(), RecvError>> {
        Changed { receiver: self }
    }

    /// Read the value without marking it seen
    fn borrow_watched(&self) -> Ref<'_, T> {
        Ref::map(self.shared.borrow(), |shared| &shared.value)
    }

    /// Read the value and mark it seen
    fn borrow_and_update(&mut self) -> Ref<'_, T> {
        self.seen = self.shared.borrow().version;
        Ref::map(self.shared.borrow(), |shared| &shared.value)
    }
}

/// A wait for a change
struct Changed<'a, T> {
    /// The receiver waiting
    receiver: &'a mut Receiver<T>,
}

impl<T> Unpin for Changed<'_, T> {}

impl<T> Future for Changed<'_, T> {
    type Output = Result<(), RecvError>;

    /// Finish at once if the value has moved, fail if it never will, or wait
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.receiver.shared.borrow_mut();
        // a change this receiver has not seen yet
        if shared.version != self.receiver.seen {
            let version = shared.version;
            drop(shared);
            self.receiver.seen = version;
            return Poll::Ready(Ok(()));
        }
        // nothing will ever change it again
        if shared.senders == 0 {
            return Poll::Ready(Err(RecvError(())));
        }
        // wait for a send
        shared.wakers.push(cx.waker().clone());
        Poll::Pending
    }
}
