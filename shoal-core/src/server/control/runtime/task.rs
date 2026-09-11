//! Spawning and joining tasks on a glommio executor

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Why a join did not produce a value
///
/// One reason: the task was cancelled. A task that panics takes the executor thread with it,
/// so a panic never reaches a join handle as an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinError;

impl fmt::Display for JoinError {
    /// Say what happened
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the task was cancelled")
    }
}

impl std::error::Error for JoinError {}

/// A handle to a spawned task's result
///
/// Over glommio's, whose output is an `Option`: `None` is a task that was cancelled or whose
/// executor is gone, which the trait wants spelled as an error.
pub struct JoinHandle<T> {
    /// The task's own handle
    inner: glommio::task::JoinHandle<T>,
}

impl<T> JoinHandle<T> {
    /// Wrap a detached task's handle
    ///
    /// # Arguments
    ///
    /// * `inner` - The handle `Task::detach` gave back
    pub fn new(inner: glommio::task::JoinHandle<T>) -> Self {
        JoinHandle { inner }
    }
}

impl<T> Unpin for JoinHandle<T> {}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    /// Wait for the task, turning a cancellation into an error
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // glommio's handle is `Unpin`, so it can be polled through a plain pin
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(Some(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(None) => Poll::Ready(Err(JoinError)),
            Poll::Pending => Poll::Pending,
        }
    }
}
