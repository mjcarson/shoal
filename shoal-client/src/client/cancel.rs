//! Telling a server about a bundle this client will never read
//! ([F75](../../../../docs/src/features/client-cancel.md))
//!
//! A result stream that ends before its answers are all in - dropped, timed out at its deadline,
//! or ended by an error - cancels what it is still owed: one `Cancel` frame on every connection
//! that owes it answers, for a server that granted cancels at the hello. The server stops writing
//! the bundle's answers, answers `Cancelled` instead of running any of its queries not yet run, and
//! acknowledges with one error frame, which this client reads and never hands to a caller.
//!
//! # Invariants
//!
//! **A connection's write half is shared, because a bundle's connection is not held.** A send
//! hands its connection back to the pool the moment its frames are written, so by the time a
//! caller abandons the stream another send may be writing on that connection, or nobody is. The
//! write half sits behind a lock that every frame written on the connection takes, and a cancel
//! takes it too, so a cancel is never written into the middle of another frame.
//!
//! **A cancel goes ahead of whatever is written next on its connection.** It is queued on the
//! connection synchronously, where it was decided, and whoever next takes the connection's lock
//! writes every queued cancel before its own frame: the next bundle, an admin request, or the task
//! spawned to write it. A retry sent under the same id after a cancel therefore always follows the
//! cancel on that connection, and the server, which applies a cancel only to what arrived before
//! it, answers the retry in full.
//!
//! **A cancel's acknowledgement is never an answer.** A retry may hold the bundle's id by the time
//! the acknowledgement arrives, so it is only ever bookkeeping: it ends a stream of the bundle being
//! assembled, and frames that arrive for a cancelled bundle with nobody waiting are expected and
//! logged quietly rather than as orphans.

use papaya::HashMap;
use std::collections::{HashSet, VecDeque};
use std::io::{ErrorKind, IoSlice};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{event, Level};
use uuid::Uuid;

use super::Errors;
use shoal_proto::shared::protocol::cancel::{cancel_frame, CLIENT_CAP_CANCEL};

/// How many cancelled bundles are remembered, so their late frames are expected
const RECENT_CANCELS: usize = 4096;

/// Every open connection's shared half, by connection id
pub(crate) type Writers = Arc<HashMap<u64, Weak<ConnShared>>>;

/// What a pooled connection shares with whoever cancels on it: its write half, and the cancels
/// waiting to go ahead of what is written next
#[derive(Debug)]
pub(crate) struct ConnShared {
    /// Which connection this is
    id: u64,
    /// The optional sections the server granted this connection in its hello ack
    caps: u8,
    /// The write half, held by whoever writes a frame on the connection
    writer: tokio::sync::Mutex<OwnedWriteHalf>,
    /// Bundles to cancel on this connection, written ahead of its next frame
    queued: Mutex<Vec<Uuid>>,
    /// Whether anything is queued, so a frame written with nothing queued takes no second lock
    any_queued: AtomicBool,
    /// Where the connection is found by its id, which it leaves when it is dropped
    writers: Writers,
}

impl ConnShared {
    /// Share a connection's write half, entering it where a cancel can find it
    ///
    /// # Arguments
    ///
    /// * `id` - The connection's id
    /// * `caps` - What its server granted in its hello ack
    /// * `writer` - Its write half
    /// * `writers` - Where every open connection is found by its id
    pub(crate) fn enter(id: u64, caps: u8, writer: OwnedWriteHalf, writers: &Writers) -> Arc<Self> {
        let shared = Arc::new(ConnShared {
            id,
            caps,
            writer: tokio::sync::Mutex::new(writer),
            queued: Mutex::new(Vec::new()),
            any_queued: AtomicBool::new(false),
            writers: writers.clone(),
        });
        writers.pin().insert(id, Arc::downgrade(&shared));
        shared
    }

    /// Queue a cancel of a bundle, to go ahead of the next frame written on this connection
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle to cancel
    pub(crate) fn queue(&self, bundle: Uuid) {
        self.queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(bundle);
        // set after the push, so a writer that sees it finds the cancel under the lock
        self.any_queued.store(true, Ordering::Release);
    }

    /// Take every queued cancel, as the frames that cancel them
    fn take_queued(&self) -> Result<Vec<u8>, Errors> {
        // nothing queued is nothing to lock for, which is every frame of a client that never
        // cancels; the flag is cleared before the take, so a cancel queued after it is seen by
        // the next frame
        if !self.any_queued.swap(false, Ordering::AcqRel) {
            return Ok(Vec::new());
        }
        // taken in one step, so a cancel queued while these are written goes with the next frame
        let queued =
            std::mem::take(&mut *self.queued.lock().unwrap_or_else(PoisonError::into_inner));
        let mut frames = Vec::with_capacity(
            queued.len() * shoal_proto::shared::protocol::cancel::CANCEL_FRAME_LEN,
        );
        for bundle in queued {
            // a cancel is far smaller than any frame bound a server can name
            frames.extend_from_slice(&cancel_frame(&bundle, u32::MAX)?);
        }
        Ok(frames)
    }

    /// Write a frame's buffers on this connection, every queued cancel ahead of them
    ///
    /// # Arguments
    ///
    /// * `bufs` - The frame's buffers, in order
    ///
    /// # Errors
    ///
    /// A write that failed or wrote nothing, which ends what the connection is good for.
    pub(crate) async fn write(&self, bufs: &mut [IoSlice<'_>]) -> Result<(), Errors> {
        // the lock is almost always free, and taken without spending the task's budget when it is
        let mut writer = match self.writer.try_lock() {
            Ok(writer) => writer,
            Err(_) => self.writer.lock().await,
        };
        // the cancels go first, so a retry under a cancelled id always follows its cancel
        let cancels = self.take_queued()?;
        if !cancels.is_empty() {
            writer.write_all(&cancels).await?;
        }
        write_all_slices(&mut writer, bufs).await
    }

    /// Write every queued cancel now, if any are queued
    ///
    /// # Errors
    ///
    /// A write that failed.
    pub(crate) async fn flush(&self) -> Result<(), Errors> {
        let mut writer = self.writer.lock().await;
        let cancels = self.take_queued()?;
        if !cancels.is_empty() {
            writer.write_all(&cancels).await?;
        }
        Ok(())
    }

    /// Whether the connection's socket has gone, when nobody is writing on it to say otherwise
    ///
    /// A connection being written on is not broken by this measure: the write will say.
    pub(crate) fn broken(&self) -> bool {
        self.writer
            .try_lock()
            .is_ok_and(|writer| writer.peer_addr().is_err())
    }
}

impl Drop for ConnShared {
    /// Leave the registry, so a connection the pool let go is never asked to cancel anything
    fn drop(&mut self) {
        // ids are never reused, so the entry under this id is this connection's
        self.writers.pin().remove(&self.id);
    }
}

/// Write every byte of some buffers to a write half
///
/// # Arguments
///
/// * `writer` - The write half
/// * `bufs` - The buffers, in order
///
/// # Errors
///
/// A write that failed or wrote nothing.
async fn write_all_slices(
    writer: &mut OwnedWriteHalf,
    mut bufs: &mut [IoSlice<'_>],
) -> Result<(), Errors> {
    // keep sending until every byte has been sent
    while !bufs.is_empty() {
        match writer.write_vectored(bufs).await? {
            // if n is zero then no bytes were written
            0 => {
                return Err(Errors::IO(std::io::Error::new(
                    ErrorKind::WriteZero,
                    "no bytes were written",
                )))
            }
            // consume the data thats already been sent
            n => IoSlice::advance_slices(&mut bufs, n),
        }
    }
    Ok(())
}

/// The bundles this client cancelled lately, so a frame for one with nobody waiting is expected
#[derive(Debug, Default)]
pub(crate) struct RecentCancels {
    /// The bundles, oldest first, and the same as a set
    held: Mutex<(VecDeque<Uuid>, HashSet<Uuid>)>,
}

impl RecentCancels {
    /// Remember a bundle as cancelled, forgetting the oldest past the bound
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle
    fn note(&self, bundle: Uuid) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let (order, set) = &mut *held;
        if set.insert(bundle) {
            order.push_back(bundle);
        }
        // the oldest goes once there are too many
        while order.len() > RECENT_CANCELS {
            if let Some(oldest) = order.pop_front() {
                set.remove(&oldest);
            }
        }
    }

    /// Whether a bundle was cancelled lately
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle
    pub(crate) fn contains(&self, bundle: &Uuid) -> bool {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1
            .contains(bundle)
    }
}

/// What a result stream needs to cancel what it is owed
#[derive(Debug, Clone)]
pub(crate) struct Canceller {
    /// Every open connection's shared half, by connection id
    writers: Writers,
    /// The bundles cancelled lately
    recent: Arc<RecentCancels>,
}

impl Canceller {
    /// A canceller over a client's connections
    ///
    /// # Arguments
    ///
    /// * `writers` - Every open connection's shared half, by connection id
    /// * `recent` - The bundles cancelled lately
    pub(crate) fn new(writers: &Writers, recent: &Arc<RecentCancels>) -> Self {
        Canceller {
            writers: writers.clone(),
            recent: recent.clone(),
        }
    }

    /// Cancel a bundle on every connection that still owes it answers
    ///
    /// Queued on each connection where it is decided, so it goes ahead of anything written on the
    /// connection afterwards, and written by a task of its own if nothing else writes first. A
    /// connection whose server did not grant cancels, or that the pool already let go, is sent
    /// nothing.
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle
    /// * `conns` - The connections that owe it answers
    pub(crate) fn cancel(&self, bundle: Uuid, conns: &[u64]) {
        // nothing owed is nothing to cancel
        if conns.is_empty() {
            return;
        }
        // whatever still arrives for it is expected from here on
        self.recent.note(bundle);
        let writers = self.writers.pin();
        for conn in conns {
            // a connection the pool let go has nobody to tell
            let Some(shared) = writers.get(conn).and_then(Weak::upgrade) else {
                continue;
            };
            // a server that granted no cancels ends the connection on one
            if !granted(shared.caps) {
                continue;
            }
            shared.queue(bundle);
            // written now if nothing else writes on the connection first; a client with no
            // runtime left writes it with its next frame, if there is one
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    runtime.spawn(async move {
                        if let Err(error) = shared.flush().await {
                            event!(Level::DEBUG, msg = "could not write a cancel", %bundle, ?error);
                        }
                    });
                }
                Err(_) => {
                    event!(Level::DEBUG, msg = "a cancel waits for its connection's next frame", %bundle);
                }
            }
        }
    }
}

/// Whether a server granted cancels on a connection, from the capabilities its ack named
///
/// # Arguments
///
/// * `caps` - The capabilities the server granted
pub(crate) const fn granted(caps: u8) -> bool {
    caps & CLIENT_CAP_CANCEL != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bundle noted is recalled until enough newer ones push it out
    #[test]
    fn recent_cancels_are_bounded() {
        let recent = RecentCancels::default();
        let first = Uuid::now_v7();
        recent.note(first);
        assert!(recent.contains(&first));
        // noting the same one twice keeps one entry
        recent.note(first);
        for _ in 0..RECENT_CANCELS - 1 {
            recent.note(Uuid::now_v7());
        }
        assert!(recent.contains(&first), "still within the bound");
        // one more pushes the oldest out
        recent.note(Uuid::now_v7());
        assert!(!recent.contains(&first));
    }
}
