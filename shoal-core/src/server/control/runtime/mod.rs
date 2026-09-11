//! A glommio [`AsyncRuntime`] for openraft
//!
//! openraft is written against a runtime abstraction rather than against tokio, and its
//! `single-threaded` feature empties every `Send` and `Sync` bound in that abstraction. Those two
//! things together are what let the control group run on the same kind of executor the shards
//! do: a pinned glommio `LocalExecutor`, whose files, timers and tasks are all `!Send`, and
//! which never has to hand anything to another thread. The alternative was a current-thread
//! tokio runtime on the control core, which is what
//! [C1](../../../../../docs/src/distributed/node-identity.md) first proposed - a second reactor,
//! a second timer wheel and a second set of channel types in a process that already has one of
//! each, for no property glommio lacks.
//!
//! What openraft needs of a runtime is small and this module is all of it:
//!
//! - [`task`]: spawn and join, over `glommio::spawn_local`
//! - [`timer`]: sleep and timeout, over `glommio::timer::Timer`
//! - [`channel`]: a bounded mpsc with weak senders, written here because glommio's local
//!   channel has neither a cloneable sender nor a weak one
//! - [`watch`]: a watch channel with the seen/unseen semantics openraft's metrics rely on
//! - [`mutex`]: an async mutex, which on one thread is a flag and a waker list
//!
//! Every one of them is `Rc` and `RefCell` rather than `Arc` and atomics, which is the point:
//! nothing here is ever shared across threads, and the single-threaded feature is what makes
//! that a type error rather than a promise.
//!
//! `openraft_rt::testing::Suite` is the contract, and `glommio_runtime_passes_the_openraft_suite`
//! runs the whole of it against this runtime.

pub mod channel;
pub mod mutex;
pub mod task;
pub mod timer;
pub mod watch;

use std::future::Future;
use std::time::Duration;

use glommio::{LocalExecutor, LocalExecutorBuilder, Placement};
use openraft_rt::{AsyncRuntime, OptionalSend};

pub use channel::GlommioMpsc;
pub use mutex::GlommioMutex;
pub use task::{JoinError, JoinHandle};
pub use timer::{Sleep, Timeout, TimeoutError};
pub use watch::GlommioWatch;

/// The oneshot the runtime hands openraft
///
/// `futures_channel`'s, wrapped only as far as the trait needs: the receiver is used as it is,
/// and the sender is wrapped so the `OneshotSender` impl can live in this crate.
pub struct GlommioOneshot;

/// A oneshot sender, wrapped for the trait
pub struct OneshotSender<T>(futures_channel::oneshot::Sender<T>);

impl openraft_rt::Oneshot for GlommioOneshot {
    type Sender<T: OptionalSend> = OneshotSender<T>;
    type Receiver<T: OptionalSend> = futures_channel::oneshot::Receiver<T>;
    type ReceiverError = futures_channel::oneshot::Canceled;

    /// Open a oneshot channel
    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where
        T: OptionalSend,
    {
        let (tx, rx) = futures_channel::oneshot::channel();
        (OneshotSender(tx), rx)
    }
}

impl<T> openraft_rt::OneshotSender<T> for OneshotSender<T>
where
    T: OptionalSend,
{
    /// Send the one value, handing it back if the receiver is gone
    fn send(self, value: T) -> Result<(), T> {
        self.0.send(value)
    }
}

/// A glommio executor, driving openraft
///
/// An instance of this exists only for `new` and `block_on`, which the conformance suite uses
/// and Shoal does not: Shoal builds its own pinned executor in
/// [`plane`](super::plane) and runs the group inside it. Every other method is an associated
/// function that reaches the executor of the calling thread, which is how openraft calls them.
pub struct GlommioRuntime {
    /// The executor `block_on` runs on
    executor: LocalExecutor,
}

impl std::fmt::Debug for GlommioRuntime {
    /// Name the runtime; the executor has nothing to print
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlommioRuntime").finish()
    }
}

impl AsyncRuntime for GlommioRuntime {
    type JoinError = JoinError;
    type JoinHandle<T: OptionalSend + 'static> = JoinHandle<T>;
    type Sleep = Sleep;
    type Instant = std::time::Instant;
    type TimeoutError = TimeoutError;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = Timeout<T>;
    type ThreadLocalRng = rand::rngs::ThreadRng;
    type Mpsc = GlommioMpsc;
    type Watch = GlommioWatch;
    type Oneshot = GlommioOneshot;
    type Mutex<T: OptionalSend + 'static> = GlommioMutex<T>;

    /// Spawn a task on the executor of the calling thread
    #[inline]
    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        JoinHandle::new(glommio::spawn_local(future).detach())
    }

    /// Wait for a duration
    #[inline]
    fn sleep(duration: Duration) -> Self::Sleep {
        Sleep::after(duration)
    }

    /// Wait until an instant
    #[inline]
    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        Sleep::until(deadline)
    }

    /// Bound a future by a duration
    #[inline]
    fn timeout<R, F: Future<Output = R> + OptionalSend>(
        duration: Duration,
        future: F,
    ) -> Self::Timeout<R, F> {
        Timeout::after(duration, future)
    }

    /// Bound a future by an instant
    #[inline]
    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: Self::Instant,
        future: F,
    ) -> Self::Timeout<R, F> {
        Timeout::until(deadline, future)
    }

    /// Whether a join error was a panic
    ///
    /// Never: a glommio task that panics unwinds the executor thread, which the pool reports as
    /// the control plane having failed. A `JoinError` here is always a cancellation.
    #[inline]
    fn is_panic(_join_error: &Self::JoinError) -> bool {
        false
    }

    /// The thread's random number generator
    #[inline]
    fn thread_rng() -> Self::ThreadLocalRng {
        rand::rng()
    }

    /// Build an unpinned executor, for the conformance suite
    ///
    /// `threads` is ignored: a glommio executor is one thread by construction.
    fn new(_threads: usize) -> Self {
        let executor = LocalExecutorBuilder::new(Placement::Unbound)
            .name("openraft-rt")
            .make()
            .expect("failed to build a glommio executor");
        GlommioRuntime { executor }
    }

    /// Run a future to completion on this executor
    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
        T: OptionalSend,
    {
        self.executor.run(future)
    }
}

#[cfg(test)]
mod tests {
    use super::GlommioRuntime;

    /// The whole openraft runtime conformance suite passes on the glommio runtime
    ///
    /// This is the test [M1](../../../../../docs/src/distributed/milestones.md) names for the
    /// runtime seam. It covers spawn and join, sleeps and timeouts, the mpsc's backpressure and
    /// weak senders, the watch's seen and unseen semantics, the oneshot, the mutex, task locals,
    /// and the deterministic rng wrapper openraft's simulation uses.
    #[test]
    fn glommio_runtime_passes_the_openraft_suite() {
        openraft_rt::testing::Suite::<GlommioRuntime>::test_all();
    }
}
