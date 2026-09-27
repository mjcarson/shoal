//! A kanal receiver that can be raced against a timer without losing a message
//!
//! A kanal [`ReceiveFuture`](kanal::ReceiveFuture) that a sender has already handed a value to
//! drops that value when the future itself is dropped. So a `select!` of a receive against a
//! timer, a `timeout` around one, or a `race` of one against anything loses a message whenever
//! the other side wins after the hand-off. That is how the compactor lost merges
//! ([Resolved #152](../../docs/src/appendix/resolved/kanal-receive-races.md)).
//!
//! **Never race a kanal receive directly.** Wrap the receiver in a [`KeptReceiver`] and race
//! [`KeptReceiver::next`] instead: the receive it starts is kept by the receiver, not by the
//! future `next` returns, so dropping that future loses nothing and the next call resumes the
//! same receive. `shoal-channel/tests/no_raced_receives.rs` fails on any race over a bare
//! `.recv()` in the workspace.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub use kanal::{AsyncReceiver, ReceiveError};

/// Define a kept receiver and its wait future, for values that are `Send` or for ones that stay
/// on one thread
///
/// The two are the same code: only the bound on the receive in progress differs, since a
/// boxed future is `Send` or not by its type.
macro_rules! kept_receiver {
    ($(#[$doc:meta])* $name:ident, $next:ident, ($($bound:tt)*)) => {
        $(#[$doc])*
        ///
        /// Dropping the receiver itself drops a receive in progress, and a value already handed
        /// to it with that. Drop one only when nothing more will ever be read from its channel.
        pub struct $name<T> {
            /// The channel's receiving half
            rx: AsyncReceiver<T>,
            /// The receive a wait started and nothing has finished yet
            pending: Option<Pin<Box<dyn Future<Output = Result<T, ReceiveError>> $($bound)*>>>,
        }

        impl<T: 'static $($bound)*> $name<T> {
            /// Keep the receives on a channel
            ///
            /// # Arguments
            ///
            /// * `rx` - The channel's receiving half
            #[must_use]
            pub fn new(rx: AsyncReceiver<T>) -> Self {
                $name { rx, pending: None }
            }

            /// Wait for the next value, safe to race against anything
            ///
            /// Dropping the returned future before it completes keeps the receive it started,
            /// and the next call to this resumes it, so a value handed over in between is
            /// returned then.
            pub fn next(&mut self) -> $next<'_, T> {
                $next { kept: self }
            }

            /// Take a value that is already there, without waiting
            ///
            /// While a receive started by `next` is still waiting, this answers `None` and
            /// takes nothing: a sender hands its value to that receive first, so taking a later
            /// one here would reorder the channel. The next call to `next` returns it.
            ///
            /// # Errors
            ///
            /// The channel is closed.
            pub fn try_next(&mut self) -> Result<Option<T>, ReceiveError> {
                // a receive in progress is first in line for whatever arrives
                if self.pending.is_some() {
                    return Ok(None);
                }
                // nothing is waiting, so the channel's queue is in order
                self.rx.try_recv()
            }

            /// Give the receiving half back, dropping any receive in progress
            ///
            /// A value already handed to a receive in progress is dropped with it, so call this
            /// only once nothing more is wanted from the channel's current use: after a
            /// stream's last answer, to recycle the channel for the next one.
            #[must_use]
            pub fn into_receiver(self) -> AsyncReceiver<T> {
                self.rx
            }

            /// The channel's receiving half, for what does not receive (its length, whether it
            /// closed)
            #[must_use]
            pub fn receiver(&self) -> &AsyncReceiver<T> {
                &self.rx
            }
        }

        /// The future a kept receiver's `next` returns
        ///
        /// It holds no receive of its own, so it can be dropped at any point without losing a
        /// value.
        pub struct $next<'a, T> {
            /// The receiver that owns the receive this waits on
            kept: &'a mut $name<T>,
        }

        impl<T: 'static $($bound)*> Future for $next<'_, T> {
            type Output = Result<T, ReceiveError>;

            /// Poll the receive the receiver keeps, starting one if none is in progress
            ///
            /// # Arguments
            ///
            /// * `cx` - The waker to call when a value arrives
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let kept = &mut self.get_mut().kept;
                // start a receive owned by the receiver, on a handle of its own so it borrows
                // nothing
                let pending = kept.pending.get_or_insert_with(|| {
                    let rx = kept.rx.clone();
                    Box::pin(async move { rx.recv().await })
                });
                // a finished receive is forgotten, so the next call starts a new one
                match pending.as_mut().poll(cx) {
                    Poll::Ready(received) => {
                        kept.pending = None;
                        Poll::Ready(received)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    };
}

kept_receiver!(
    /// A kanal receiver whose receive in progress survives the future that waited on it
    ///
    /// For values that are `Send`, so the receiver can move between threads with a tokio task.
    ///
    /// ```
    /// use shoal_channel::KeptReceiver;
    ///
    /// let (_tx, rx) = kanal::unbounded_async::<u64>();
    /// let mut kept = KeptReceiver::new(rx);
    /// assert!(kept.try_next().unwrap().is_none());
    /// ```
    KeptReceiver,
    Next,
    (+ Send)
);

kept_receiver!(
    /// A [`KeptReceiver`] for values that are not `Send`, on a thread-per-core executor
    ///
    /// A shard's messages are `Send` only for some schemas, and the receiver never leaves the
    /// executor it was made on, so it needs no bound.
    LocalKeptReceiver,
    LocalNext,
    ()
);

#[cfg(test)]
mod tests {
    use super::{KeptReceiver, LocalKeptReceiver};
    use std::rc::Rc;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    /// A bare kanal receive dropped after its value was handed over loses the value
    ///
    /// This is the defect, reproduced with no runtime: what a `select!` does when its timer
    /// wins after a sender handed the waiting receive its value. It pins kanal's behaviour, so
    /// a kanal that stops losing the value fails here, and this crate can be reconsidered.
    #[test]
    fn a_dropped_kanal_receive_loses_a_value_handed_to_it() {
        let (tx, rx) = kanal::unbounded_async::<u64>();
        let mut cx = Context::from_waker(Waker::noop());
        {
            // the receive waits, as the one in a select! does before its timer fires
            let mut recv = pin!(rx.recv());
            assert!(recv.as_mut().poll(&mut cx).is_pending());
            // the sender hands its value straight to the waiting receive
            tx.try_send(7).expect("the channel is open");
            // and the timer wins: the receive is dropped with the value in it
        }
        // the value is gone
        assert_eq!(rx.try_recv().expect("the channel is open"), None);
    }

    /// The same race through a [`KeptReceiver`] loses nothing
    #[test]
    fn a_dropped_next_keeps_the_value_for_the_next_call() {
        let (tx, rx) = kanal::unbounded_async::<u64>();
        let mut kept = KeptReceiver::new(rx);
        let mut cx = Context::from_waker(Waker::noop());
        {
            // the wait starts, and the sender hands its value to it
            let mut next = pin!(kept.next());
            assert!(next.as_mut().poll(&mut cx).is_pending());
            tx.try_send(7).expect("the channel is open");
            // and the timer wins
        }
        // a value that came after is queued behind the one handed over, and not taken first
        tx.try_send(8).expect("the channel is open");
        assert_eq!(kept.try_next().expect("the channel is open"), None);
        // the next wait returns the handed value, then the queued one, in order
        let first = {
            let mut next = pin!(kept.next());
            next.as_mut().poll(&mut cx)
        };
        assert!(matches!(first, Poll::Ready(Ok(7))));
        assert_eq!(kept.try_next().expect("the channel is open"), Some(8));
    }

    /// A value already queued is taken without waiting, and a closed channel says so
    #[test]
    fn try_next_takes_what_is_queued_and_reports_a_close() {
        let (tx, rx) = kanal::unbounded_async::<u64>();
        let mut kept = KeptReceiver::new(rx);
        tx.try_send(1).expect("the channel is open");
        assert_eq!(kept.try_next().expect("the channel is open"), Some(1));
        assert_eq!(kept.try_next().expect("the channel is open"), None);
        drop(tx);
        assert!(kept.try_next().is_err());
    }

    /// The local receiver keeps a value that is not `Send` through the same race
    #[test]
    fn a_local_receiver_keeps_a_value_that_is_not_send() {
        let (tx, rx) = kanal::unbounded_async::<Rc<u64>>();
        let mut kept = LocalKeptReceiver::new(rx);
        let mut cx = Context::from_waker(Waker::noop());
        {
            // the wait starts, the value is handed to it, and the timer wins
            let mut next = pin!(kept.next());
            assert!(next.as_mut().poll(&mut cx).is_pending());
            tx.try_send(Rc::new(7)).expect("the channel is open");
        }
        // the next wait returns it
        let mut next = pin!(kept.next());
        match next.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(value)) => assert_eq!(*value, 7),
            other => panic!("the handed value was lost: {other:?}"),
        }
    }
}
