//! The bundles a node's clients cancelled, which every shard checks before it runs a query
//! ([F75](../../../docs/src/features/client-cancel.md))
//!
//! A client's `Cancel` is read by the relay of the connection it arrived on and handed to the
//! shard coordinating that connection, behind the bundle it names. That shard records it here,
//! and every shard of the node reads it before it runs a query of that bundle, and answers the
//! query `Cancelled` instead.
//!
//! # Invariants
//!
//! **The board is read across threads, not told by message.** The mesh between shards is a set
//! of FIFO queues: a cancel broadcast to every shard lands behind the very `Query` it means to
//! stop, so a shard would always run the query first. Only a table every shard reads when it
//! dequeues a query lets a cancel overtake work still waiting in a queue, which is the work a
//! cancel exists to save - a client that gave up on a slow server, while the server grinds on.
//!
//! **A cancel covers the attempts before it and none after.** A shard coordinates every arrival
//! on a connection and mints each one's attempt from one counter that only rises, so the
//! counter's value when the cancel is handled is a bound: every arrival of the bundle the cancel
//! came after has a lower attempt, and a retry under the same id sent after it has a higher one.
//! Attempt zero is an answer whose attempt was not carried, and is never covered, so a path that
//! forgets to carry one weakens a cancel and never stops a retry.
//!
//! **Nothing is checked while nothing is recorded.** A shard's look at the board is one atomic
//! load until a cancel is live, so the query path of a node whose clients never cancel pays that
//! and nothing else.
//!
//! **An entry lives no longer than the work it can stop.** Every query of a bundle is answered
//! by the bundle's deadline, which is at most the server's, so an entry expires at the deadline
//! after it was recorded and is swept on the shards' ticks; a client that leaves takes its
//! entries with it. The board holds at most [`MAX_CANCELLED`], and a cancel past that is not
//! recorded: its answers are still dropped at the relay, and its work runs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use uuid::Uuid;

use crate::server::stage_profile::Stamp;

/// The most cancels one node holds at once
///
/// About sixty-four bytes an entry with the map's overhead, so a full board is a few megabytes.
/// A node meets it only when its clients cancel this many bundles within one deadline.
pub const MAX_CANCELLED: usize = 1 << 16;

/// One cancelled bundle on one connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cancelled {
    /// Every arrival of the bundle at an attempt below this is cancelled
    before: u64,
    /// When the entry can no longer stop anything, and is swept
    until: Stamp,
}

/// The bundles a node's clients cancelled, shared by every shard of the node
#[derive(Debug)]
pub struct CancelBoard {
    /// How many cancels are recorded, which is all a shard reads while there are none
    live: AtomicUsize,
    /// Every recorded cancel, by the connection it arrived on and the bundle it names
    entries: RwLock<HashMap<(Uuid, Uuid), Cancelled>>,
    /// The most cancels this board holds
    capacity: usize,
}

impl Default for CancelBoard {
    /// A board holding nothing, bounded at [`MAX_CANCELLED`]
    fn default() -> Self {
        CancelBoard::with_capacity(MAX_CANCELLED)
    }
}

impl CancelBoard {
    /// A board holding nothing, bounded at a capacity of its own
    ///
    /// # Arguments
    ///
    /// * `capacity` - The most cancels it holds at once
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        CancelBoard {
            live: AtomicUsize::new(0),
            entries: RwLock::new(HashMap::new()),
            capacity,
        }
    }

    /// Take the entries to read, whatever a panicking holder left
    fn read(&self) -> RwLockReadGuard<'_, HashMap<(Uuid, Uuid), Cancelled>> {
        self.entries.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take the entries to change, whatever a panicking holder left
    fn write(&self) -> RwLockWriteGuard<'_, HashMap<(Uuid, Uuid), Cancelled>> {
        self.entries.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a cancel, or raise one already recorded for the same bundle
    ///
    /// Returns whether it was recorded: a full board records no new bundle.
    ///
    /// # Arguments
    ///
    /// * `client` - The connection the cancelled bundle arrived on
    /// * `bundle` - The bundle
    /// * `before` - Every arrival of the bundle at an attempt below this is cancelled
    /// * `until` - When the entry can no longer stop anything
    pub fn record(&self, client: Uuid, bundle: Uuid, before: u64, until: Stamp) -> bool {
        let mut entries = self.write();
        // a second cancel of the same bundle covers everything the first did and more
        if let Some(entry) = entries.get_mut(&(client, bundle)) {
            entry.before = entry.before.max(before);
            if until.since(entry.until) > 0 {
                entry.until = until;
            }
            return true;
        }
        // a full board records nothing more, so a flood of cancels is bounded
        if entries.len() >= self.capacity {
            return false;
        }
        entries.insert((client, bundle), Cancelled { before, until });
        self.live.store(entries.len(), Ordering::Release);
        true
    }

    /// Whether an arrival of a bundle was cancelled
    ///
    /// One atomic load while nothing is recorded, which is what every query a node runs pays.
    ///
    /// # Arguments
    ///
    /// * `client` - The connection the bundle arrived on
    /// * `bundle` - The bundle
    /// * `attempt` - The arrival's attempt, zero when it was not carried
    #[inline]
    #[must_use]
    pub fn covers(&self, client: Uuid, bundle: Uuid, attempt: u64) -> bool {
        // an attempt nobody carried is never covered, and an empty board covers nothing
        if attempt == 0 || self.live.load(Ordering::Acquire) == 0 {
            return false;
        }
        self.read()
            .get(&(client, bundle))
            .is_some_and(|entry| attempt < entry.before)
    }

    /// Whether any cancel is recorded, so a caller can skip a sweep
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.live.load(Ordering::Acquire) == 0
    }

    /// How many cancels are recorded
    #[must_use]
    pub fn len(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// Remove every entry that can no longer stop anything
    ///
    /// Returns how many were removed.
    ///
    /// # Arguments
    ///
    /// * `now` - The time to judge them by
    pub fn sweep(&self, now: Stamp) -> usize {
        // nothing recorded is nothing to take the lock for
        if self.is_empty() {
            return 0;
        }
        let mut entries = self.write();
        let before = entries.len();
        // an entry is kept until its deadline has passed
        entries.retain(|_, entry| now.since(entry.until) == 0);
        self.live.store(entries.len(), Ordering::Release);
        before - entries.len()
    }

    /// Remove every entry of a connection that ended, whose work can never be answered
    ///
    /// # Arguments
    ///
    /// * `client` - The connection
    pub fn forget_client(&self, client: Uuid) {
        // nothing recorded is nothing to take the lock for
        if self.is_empty() {
            return;
        }
        let mut entries = self.write();
        entries.retain(|(owner, _), _| *owner != client);
        self.live.store(entries.len(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deadline a few seconds out
    fn soon() -> Stamp {
        Stamp::now().plus_nanos(5_000_000_000)
    }

    /// A board with nothing on it covers nothing, without reading its table
    #[test]
    fn an_empty_board_takes_the_fast_path() {
        let board = CancelBoard::default();
        assert!(board.is_empty());
        assert_eq!(board.len(), 0);
        assert!(!board.covers(Uuid::new_v4(), Uuid::new_v4(), 1));
        // a sweep and a forget of an empty board change nothing
        assert_eq!(board.sweep(Stamp::now()), 0);
        board.forget_client(Uuid::new_v4());
        assert!(board.is_empty());
    }

    /// A cancel covers the arrivals before it on its own connection, and nothing else
    #[test]
    fn only_attempts_before_it_are_covered() {
        let board = CancelBoard::default();
        let (client, bundle) = (Uuid::new_v4(), Uuid::now_v7());
        assert!(board.record(client, bundle, 10, soon()));
        // every attempt below the bound is covered
        assert!(board.covers(client, bundle, 1));
        assert!(board.covers(client, bundle, 9));
        // the bound and above were sent after the cancel
        assert!(!board.covers(client, bundle, 10));
        assert!(!board.covers(client, bundle, 11));
        // another bundle, or the same bundle on another connection, is untouched
        assert!(!board.covers(client, Uuid::now_v7(), 1));
        assert!(!board.covers(Uuid::new_v4(), bundle, 1));
    }

    /// An answer whose attempt was not carried is never covered
    #[test]
    fn attempt_zero_is_never_covered() {
        let board = CancelBoard::default();
        let (client, bundle) = (Uuid::new_v4(), Uuid::now_v7());
        board.record(client, bundle, u64::MAX, soon());
        assert!(!board.covers(client, bundle, 0));
        assert!(board.covers(client, bundle, 1));
    }

    /// A second cancel of a bundle raises the first's bound rather than adding an entry
    #[test]
    fn a_later_cancel_raises_before() {
        let board = CancelBoard::default();
        let (client, bundle) = (Uuid::new_v4(), Uuid::now_v7());
        board.record(client, bundle, 5, soon());
        board.record(client, bundle, 8, soon());
        assert_eq!(board.len(), 1);
        assert!(board.covers(client, bundle, 7));
        // and a lower one never lowers it
        board.record(client, bundle, 2, soon());
        assert!(board.covers(client, bundle, 7));
    }

    /// An entry past its deadline is swept, and one before it is kept
    #[test]
    fn entries_expire() {
        let board = CancelBoard::default();
        let (client, bundle) = (Uuid::new_v4(), Uuid::now_v7());
        let now = Stamp::now();
        board.record(client, bundle, 5, now.plus_nanos(1_000));
        board.record(client, Uuid::now_v7(), 5, now.plus_nanos(10_000_000_000));
        // before either deadline nothing goes
        assert_eq!(board.sweep(now), 0);
        // past the first only it goes
        assert_eq!(board.sweep(now.plus_nanos(2_000)), 1);
        assert_eq!(board.len(), 1);
        assert!(!board.covers(client, bundle, 1));
    }

    /// A full board records no new bundle, but still raises one it holds
    #[test]
    fn a_full_board_records_nothing() {
        let board = CancelBoard::with_capacity(2);
        let client = Uuid::new_v4();
        let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
        assert!(board.record(client, first, 3, soon()));
        assert!(board.record(client, second, 3, soon()));
        // a third bundle does not fit
        let third = Uuid::now_v7();
        assert!(!board.record(client, third, 3, soon()));
        assert!(!board.covers(client, third, 1));
        // one it holds is still raised
        assert!(board.record(client, first, 9, soon()));
        assert!(board.covers(client, first, 8));
        assert_eq!(board.len(), 2);
    }

    /// A connection that ends takes its own entries and nobody else's
    #[test]
    fn forget_client_is_scoped() {
        let board = CancelBoard::default();
        let (gone, stays) = (Uuid::new_v4(), Uuid::new_v4());
        let bundle = Uuid::now_v7();
        board.record(gone, bundle, 4, soon());
        board.record(stays, bundle, 4, soon());
        board.forget_client(gone);
        assert!(!board.covers(gone, bundle, 1));
        assert!(board.covers(stays, bundle, 1));
        assert_eq!(board.len(), 1);
    }
}
