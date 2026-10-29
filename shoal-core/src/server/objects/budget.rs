//! The memory object work holds, which every object buffer is drawn from
//!
//! Object work holds memory that is not a row - a stage in flight, the units of a partial write,
//! what a read returns, a stream's window and the kernel's socket buffers under it - and without a
//! budget it would grow the resident set past `node_memory` and have every shard evict rows for
//! bytes that are not theirs, item 191's failure with a different guest
//! ([S13](../../../../docs/src/object-storage/isolation.md#memory)). So each object executor has a
//! budget, carved out of the node's before the rows' is, and every buffer is drawn from it.
//!
//! Since [F118](../../../../docs/src/features/scan-budget.md) the accounting is the node's one kind
//! of budget ([`crate::server::budget`]), which a scan's buffers draw on too; this keeps the object
//! store's own face of it: its sheds answered as a [`StoreError`], its streams' socket buffers
//! counted beside their windows, and its warnings its own executor's.
//!
//! # Invariants
//!
//! **Nothing grows past the budget.** A draw that does not fit waits a bounded time, then is shed
//! by name with a code a caller retries on, and the bytes it would have held are never allocated.
//!
//! **A draw is returned when what it held is dropped**: a [`Permit`] gives its bytes back.

use std::sync::Arc;
use std::time::Duration;

use super::store::StoreError;
use crate::server::budget as shared;

pub use shared::Permit;

/// What the kernel holds under a stream's window, a side, counted beside the window itself
///
/// X11 found a stream holds up to about 4 MiB a side in its socket's buffers whatever its window
/// ([X11](../../../../docs/src/object-storage/streamed-bodies.md#2-the-window-and-what-a-stream-holds)).
pub const SOCKET_BUFFERS: u64 = 4 << 20;

/// How long a draw that does not fit waits for room before it is shed
pub const DRAW_WAIT: Duration = shared::DRAW_WAIT;

/// One object executor's memory budget
#[derive(Debug)]
pub struct Budget {
    /// The node's kind of budget, named as an object executor's in its warnings
    inner: Arc<shared::Budget>,
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
            inner: shared::Budget::new("an object executor's budget", limit),
        })
    }

    /// Draw bytes if they fit now
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    #[must_use]
    pub fn try_draw(self: &Arc<Self>, bytes: u64) -> Option<Permit> {
        self.inner.try_draw(bytes)
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
        // a shed as the object store has always answered one, a code a caller retries on
        self.inner
            .draw(bytes, wait)
            .await
            .map_err(|shed| StoreError::Budget {
                needed: shed.needed,
                limit: shed.limit,
            })
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
        self.inner.used()
    }

    /// The bytes it may hold
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.inner.limit()
    }

    /// The draws shed since it was made
    #[must_use]
    pub fn shed(&self) -> u64 {
        self.inner.shed()
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
