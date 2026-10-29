//! The holder's journal: a ring written ahead with zeros and overwritten, one sync a batch
//!
//! X6's journal (`shoal-spike/src/device/journal.rs`), the way it found best
//! ([X6](../../../docs/src/object-storage/device-store-ssd.md#2-the-journal)): a file written
//! ahead to its length once, so a stage overwrites blocks already allocated and its sync commits
//! no metadata; each stager writing its own record; and one committer syncing whatever writes have
//! completed, so a sync covers every record staged since the last. It keeps F60's rule: a record
//! is durable only through a flush that began after its write completed. Copied with one change:
//! the committer counts its syncs and the records they covered, where X6's kept a vector of every
//! sync, which a holder serving for hours would grow without end.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use glommio::io::DmaFile;
use glommio::Task;

/// Where a journal's offsets come from: a ring a record never straddles the end of
pub struct Ring {
    /// The next offset
    next: Cell<u64>,
    /// The length it wraps at
    wrap: u64,
}

impl Ring {
    /// A journal that wraps at a length
    ///
    /// # Arguments
    ///
    /// * `len` - The length
    #[must_use]
    pub fn wrapping(len: u64) -> Self {
        Ring {
            next: Cell::new(0),
            wrap: len,
        }
    }

    /// Take the next run of bytes
    ///
    /// # Arguments
    ///
    /// * `len` - The run's length, which must not exceed the ring's
    #[must_use]
    pub fn take(&self, len: u64) -> u64 {
        let mut at = self.next.get();
        // a record never straddles the end; it starts again at the front
        if at + len > self.wrap {
            at = 0;
        }
        self.next.set(at + len);
        at
    }
}

/// What a committer has done: its syncs and the records they covered
#[derive(Debug, Default)]
pub struct Syncs {
    /// Syncs issued
    pub syncs: Cell<u64>,
    /// Records those syncs covered, together
    pub records: Cell<u64>,
}

/// A committer that syncs a file for every batch of writes that completed before it began
pub struct Committer {
    /// Where a write that completed asks to be made durable
    tx: RefCell<Option<mpsc::UnboundedSender<oneshot::Sender<()>>>>,
    /// The committer's task
    task: RefCell<Option<Task<()>>>,
    /// Its syncs, counted as it makes them
    pub counts: Rc<Syncs>,
}

impl Committer {
    /// Start a committer for a file
    ///
    /// # Arguments
    ///
    /// * `file` - The file it syncs
    #[must_use]
    pub fn start(file: Rc<DmaFile>) -> Rc<Committer> {
        let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<()>>();
        let counts = Rc::new(Syncs::default());
        let counted = counts.clone();
        let task = glommio::spawn_local(async move {
            // one sync for everything that has completed, then the next batch; it waits on the
            // channel, so it never syncs with nothing new to make durable
            while let Some(first) = rx.next().await {
                let mut batch = vec![first];
                while let Ok(Some(more)) = rx.try_next() {
                    batch.push(more);
                }
                file.fdatasync().await.expect("the journal is synced");
                counted.syncs.set(counted.syncs.get() + 1);
                counted.records.set(counted.records.get() + batch.len() as u64);
                for waiter in batch {
                    let _ = waiter.send(());
                }
            }
        });
        Rc::new(Committer {
            tx: RefCell::new(Some(tx)),
            task: RefCell::new(Some(task)),
            counts,
        })
    }

    /// Wait until a write that has completed is durable
    pub async fn durable(&self) {
        let (waiter, done) = oneshot::channel();
        self.tx
            .borrow()
            .as_ref()
            .expect("the committer runs")
            .unbounded_send(waiter)
            .expect("the committer listens");
        done.await.expect("the committer answers");
    }

    /// Stop the committer once every write asked of it is durable
    pub async fn stop(&self) {
        self.tx.borrow_mut().take();
        let task = self.task.borrow_mut().take();
        if let Some(task) = task {
            task.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record never straddles the ring's end, and every offset is where the last record ended
    /// or the front
    #[test]
    fn a_ring_wraps_whole_records() {
        let ring = Ring::wrapping(1 << 20);
        let mut expected = 0;
        for len in [4096u64, 8192, 4096 + (256 << 10), 4096 + (64 << 10)].iter().cycle().take(200) {
            let at = ring.take(*len);
            if expected + len > 1 << 20 {
                assert_eq!(at, 0, "a record that would cross the end starts at the front");
            } else {
                assert_eq!(at, expected);
            }
            assert!(at + len <= 1 << 20);
            assert_eq!(at % 4096, 0, "every record starts on a block");
            expected = at + len;
        }
    }
}
