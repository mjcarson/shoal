//! One sync for every write that completed before it began: a slice's group commit
//!
//! A stager waits on its record being durable, and an apply under a shared flush on its write being
//! durable; one `fdatasync` covers every one of them whose write had completed when it began, as the
//! WAL's batch does ([F60](../../../../../docs/src/features/shared-wal-flush.md)). X6 measured a
//! journal written ahead and committed this way at 2.3 to sixteen times an appending journal's
//! records ([S6](../../../../../docs/src/object-storage/device-store.md#staging-two-cases)).
//!
//! # Invariants
//!
//! **A write is durable only through a sync that began after it completed.** A waiter asks only
//! once its write has completed, and a sync answers only the waiters that asked before it began.
//!
//! **A committer never syncs with nothing new**: it waits for a waiter before every sync, since on
//! ext4 and XFS a sync with nothing to write still flushes the device.
//!
//! **A sync that fails fails every waiter it would have answered**, and every later one: the device
//! is failed, never retried in place, and never answered as durable.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use futures::StreamExt;
use futures_channel::{mpsc, oneshot};
use glommio::io::DmaFile;
use glommio::task::JoinHandle;

/// What a committer has done: its syncs and the waiters they answered
#[derive(Debug, Default)]
pub struct Syncs {
    /// Syncs issued
    pub syncs: Cell<u64>,
    /// Waiters those syncs answered, together
    pub waiters: Cell<u64>,
}

/// Where a write that completed is told it is durable, or why it never will be
type Waiter = oneshot::Sender<Result<(), String>>;

/// A committer of one file, whose sync flushes what it covers
pub struct Committer {
    /// Where a write that completed asks to be made durable
    tx: RefCell<Option<mpsc::UnboundedSender<Waiter>>>,
    /// The file it syncs, closed when it stops
    file: RefCell<Option<Rc<DmaFile>>>,
    /// The committer's task
    task: RefCell<Option<JoinHandle<()>>>,
    /// Its syncs, counted as it makes them
    pub counts: Rc<Syncs>,
}

impl Committer {
    /// Start a committer for a file, on the queue it is started from
    ///
    /// # Arguments
    ///
    /// * `file` - The file it syncs
    #[must_use]
    pub fn start(file: Rc<DmaFile>) -> Rc<Committer> {
        let (tx, mut rx) = mpsc::unbounded::<Waiter>();
        let counts = Rc::new(Syncs::default());
        let counted = counts.clone();
        // kept beside the task's, so a stop closes it
        let kept = file.clone();
        let task = glommio::spawn_local(async move {
            // the first failure is every later answer too: a failed device is never durable again
            let mut failed: Option<String> = None;
            // one sync for everything that has completed, then the next batch; it waits on the
            // channel, so it never syncs with nothing new to make durable
            while let Some(first) = rx.next().await {
                let mut batch = vec![first];
                while let Ok(Some(more)) = rx.try_next() {
                    batch.push(more);
                }
                if failed.is_none() {
                    if let Err(error) = file.fdatasync().await {
                        failed = Some(format!("fdatasync of {:?} failed: {error}", file.path()));
                    }
                    counted.syncs.set(counted.syncs.get() + 1);
                }
                counted.waiters.set(counted.waiters.get() + batch.len() as u64);
                for waiter in batch {
                    let _ = waiter.send(failed.clone().map_or(Ok(()), Err));
                }
            }
        })
        .detach();
        Rc::new(Committer {
            tx: RefCell::new(Some(tx)),
            file: RefCell::new(Some(kept)),
            task: RefCell::new(Some(task)),
            counts,
        })
    }

    /// Wait until a write that has completed is durable
    ///
    /// # Errors
    ///
    /// The sync failed, or the committer has stopped.
    pub async fn durable(&self) -> Result<(), String> {
        let (waiter, done) = oneshot::channel();
        // the borrow ends before the await
        let sent = self
            .tx
            .borrow()
            .as_ref()
            .map(|tx| tx.unbounded_send(waiter).is_ok())
            .unwrap_or(false);
        if !sent {
            return Err("the slice's committer has stopped".to_string());
        }
        done.await
            .map_err(|_| "the slice's committer stopped before it answered".to_string())?
    }

    /// Stop the committer once every write asked of it is durable
    pub async fn stop(&self) {
        self.tx.borrow_mut().take();
        let task = self.task.borrow_mut().take();
        if let Some(task) = task {
            let _ = task.await;
        }
        // and its file closed now, or by whoever holds it last
        let file = self.file.borrow_mut().take();
        if let Some(file) = file {
            super::close(file).await;
        }
    }
}
