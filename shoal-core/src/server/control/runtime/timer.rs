//! Sleeps and timeouts over glommio's timer

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use glommio::timer::Timer;
use pin_project_lite::pin_project;

/// A wait that completes once a timer fires
///
/// glommio's timer resolves to the instant it fired at; the trait wants `()`.
pub struct Sleep {
    /// The timer, which has to be built on the executor thread that will poll it
    timer: Timer,
}

impl Sleep {
    /// A wait for a duration
    ///
    /// # Arguments
    ///
    /// * `duration` - How long to wait
    pub fn after(duration: Duration) -> Self {
        Sleep {
            timer: Timer::new(duration),
        }
    }

    /// A wait until an instant, or no wait at all if it has passed
    ///
    /// # Arguments
    ///
    /// * `deadline` - When to wake
    pub fn until(deadline: Instant) -> Self {
        Sleep::after(deadline.saturating_duration_since(Instant::now()))
    }
}

impl Future for Sleep {
    type Output = ();

    /// Wait for the timer
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // the timer is `Unpin`, so a plain pin reaches it
        Pin::new(&mut self.timer).poll(cx).map(|_fired_at| ())
    }
}

/// A future that did not complete in time
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutError;

impl fmt::Display for TimeoutError {
    /// Say what happened
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the deadline passed")
    }
}

impl std::error::Error for TimeoutError {}

pin_project! {
    /// A future bounded by a timer
    ///
    /// The future is polled first, so a future that is ready at the deadline wins rather than
    /// losing to it, which is what tokio does and what openraft's tests assume. The fields are
    /// the future, pinned, and the timer that bounds it.
    pub struct Timeout<F> {
        #[pin]
        future: F,
        timer: Timer,
    }
}

impl<F> Timeout<F> {
    /// Bound a future by a duration
    ///
    /// # Arguments
    ///
    /// * `duration` - How long the future has
    /// * `future` - The future
    pub fn after(duration: Duration, future: F) -> Self {
        Timeout {
            future,
            timer: Timer::new(duration),
        }
    }

    /// Bound a future by an instant
    ///
    /// # Arguments
    ///
    /// * `deadline` - When the future has to have completed by
    /// * `future` - The future
    pub fn until(deadline: Instant, future: F) -> Self {
        Timeout::after(deadline.saturating_duration_since(Instant::now()), future)
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, TimeoutError>;

    /// Poll the future, then the timer
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // the future first, so a ready value beats an expired timer
        if let Poll::Ready(value) = this.future.poll(cx) {
            return Poll::Ready(Ok(value));
        }
        // then the timer, whose firing is the timeout
        match Pin::new(this.timer).poll(cx) {
            Poll::Ready(_) => Poll::Ready(Err(TimeoutError)),
            Poll::Pending => Poll::Pending,
        }
    }
}
