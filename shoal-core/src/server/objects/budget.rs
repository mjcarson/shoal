//! The memory object work holds, which every object buffer is drawn from
//!
//! Object work holds memory that is not a row - a stage in flight, the units of a partial write,
//! what a read returns, a stream's window and the kernel's socket buffers under it - and without a
//! budget it would grow the resident set past `node_memory` and have every shard evict rows for
//! bytes that are not theirs, item 191's failure with a different guest
//! ([S13](../../../../docs/src/object-storage/isolation.md#memory)). So each object executor has a
//! budget, carved out of the node's before the rows' is, and every buffer is drawn from it.
//!
//! # Invariants
//!
//! **Nothing grows past the budget.** A draw that does not fit waits a bounded time, then is shed
//! by name with a code a caller retries on, and the bytes it would have held are never allocated.
//!
//! **A draw is returned when what it held is dropped**: a [`Permit`] gives its bytes back.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::store::StoreError;

/// What the kernel holds under a stream's window, a side, counted beside the window itself
///
/// X11 found a stream holds up to about 4 MiB a side in its socket's buffers whatever its window
/// ([X11](../../../../docs/src/object-storage/streamed-bodies.md#2-the-window-and-what-a-stream-holds)).
pub const SOCKET_BUFFERS: u64 = 4 << 20;

/// How long a draw that does not fit waits for room before it is shed
pub const DRAW_WAIT: Duration = Duration::from_millis(250);

/// One object executor's memory budget
#[derive(Debug)]
pub struct Budget {
    /// The bytes it may hold
    limit: u64,
    /// The bytes drawn now
    used: AtomicU64,
    /// The draws shed since it was made
    shed: AtomicU64,
}

/// Bytes drawn from a budget, given back when dropped
#[derive(Debug)]
pub struct Permit {
    /// The budget
    budget: Arc<Budget>,
    /// The bytes
    bytes: u64,
}

impl Drop for Permit {
    /// Give the bytes back
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl Budget {
    /// A budget of so many bytes
    ///
    /// # Arguments
    ///
    /// * `limit` - The bytes it may hold
    #[must_use]
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Budget {
            limit,
            used: AtomicU64::new(0),
            shed: AtomicU64::new(0),
        })
    }

    /// Draw bytes if they fit now
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    #[must_use]
    pub fn try_draw(self: &Arc<Self>, bytes: u64) -> Option<Permit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let after = used.checked_add(bytes)?;
            if after > self.limit {
                return None;
            }
            match self
                .used
                .compare_exchange_weak(used, after, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(Permit {
                        budget: self.clone(),
                        bytes,
                    })
                }
                Err(now) => used = now,
            }
        }
    }

    /// Draw bytes, waiting up to a bound for room, then shed by name
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    /// * `wait` - How long to wait for room
    ///
    /// # Errors
    ///
    /// [`StoreError::Budget`] once the wait runs out, or at once for a draw past the whole budget.
    pub async fn draw(self: &Arc<Self>, bytes: u64, wait: Duration) -> Result<Permit, StoreError> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(permit) = self.try_draw(bytes) {
                return Ok(permit);
            }
            // a draw no wait can ever satisfy is shed at once
            if bytes > self.limit || Instant::now() >= deadline {
                self.shed.fetch_add(1, Ordering::Relaxed);
                return Err(StoreError::Budget {
                    needed: bytes,
                    limit: self.limit,
                });
            }
            glommio::timer::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Draw a stream's window and the socket buffers the kernel holds under it
    ///
    /// # Arguments
    ///
    /// * `window` - The stream's window
    #[must_use]
    pub fn try_draw_stream(self: &Arc<Self>, window: u64) -> Option<Permit> {
        self.try_draw(window.saturating_add(SOCKET_BUFFERS))
    }

    /// The bytes drawn now
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// The bytes it may hold
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// The draws shed since it was made
    #[must_use]
    pub fn shed(&self) -> u64 {
        self.shed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{Budget, SOCKET_BUFFERS};

    /// A draw fits or it does not, and a permit gives its bytes back
    #[test]
    fn a_budget_holds_what_is_drawn_and_no_more() {
        let budget = Budget::new(10 << 20);
        let first = budget.try_draw(6 << 20).expect("fits");
        assert!(budget.try_draw(6 << 20).is_none());
        assert_eq!(budget.used(), 6 << 20);
        drop(first);
        assert_eq!(budget.used(), 0);
        // a stream counts its socket's buffers beside its window
        let stream = budget.try_draw_stream(1 << 20).expect("fits");
        assert_eq!(budget.used(), (1 << 20) + SOCKET_BUFFERS);
        drop(stream);
    }
}
