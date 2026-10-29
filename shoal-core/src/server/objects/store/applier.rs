//! A rotational slice's applies, issued a whole batch at a time, each batch bounded by its time
//!
//! A disk's arm does one thing at a time, and a foreground read beside applies waits for whatever
//! is in flight when it arrives. X7 put the order to the lab's disks: applies one at a time in
//! offset order finished no sooner than as they arrived, while a whole batch in flight was ordered
//! by the block layer and the disk's own queue and kept its offer, and a read's p99 rose with the
//! batch ([X7](../../../../../docs/src/object-storage/device-store-hdd.md#10-reads-and-stages-beside-applies)).
//! So a disk's applies are deferred here, issued a batch at a time with every apply of it in flight
//! at once, in no order of their own, and the batch is bounded by the time a disk takes to apply it
//! ([F91](../../../../../docs/src/features/rotational-devices.md)).
//!
//! # Invariants
//!
//! **A read waits for at most one batch.** A batch is issued only once the slice has no foreground
//! request in flight, or once one bound has passed waiting for that, so a read that arrives while a
//! batch is in flight is not followed by another until it is answered, and applies are never held
//! back past one bound a batch.
//!
//! **An apply is the same apply.** A batch runs the slice's own apply for each of its chunks, each
//! under its chunk's lock, so everything S6 says of an apply holds of one in a batch; a batch only
//! decides when.
//!
//! **Nothing waits to fill a batch.** Whatever is pending when the slice is free is issued, so an
//! idle disk applies at once and a busy one gathers its batches while the last is in flight.

use std::collections::VecDeque;
use std::rc::{Rc, Weak};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use glommio::TaskQueueHandle;

use super::chunk::ChunkId;
use super::slice::Slice;
use super::StoreError;
use crate::shared::objects::Label;

/// The most applies one batch takes, X7's largest batch measured
pub const MAX_BATCH: usize = 128;

/// What one apply is reckoned to take before a batch has been timed: a disk's seek and revolution
pub const SEED_ESTIMATE: Duration = Duration::from_millis(10);

/// How often the applier looks whether the slice's foreground is done
const IDLE_POLL: Duration = Duration::from_millis(1);

/// One apply waiting for its batch, and where its answer goes
pub struct Pending {
    /// The chunk
    pub id: ChunkId,
    /// The label its committed write makes
    pub label: Label,
    /// Where its answer goes
    pub reply: oneshot::Sender<Result<(), StoreError>>,
}

/// A rotational slice's applier: where its applies are sent to wait for their batch
#[derive(Clone)]
pub struct Applier {
    /// Where an apply is sent
    sender: mpsc::UnboundedSender<Pending>,
}

impl Applier {
    /// Start the applier of a slice, on the queue applies run on
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice, held weakly so a closed slice ends its applier
    /// * `queue` - The queue its applies run on, or the current one
    #[must_use]
    pub fn start(slice: &Rc<Slice>, queue: Option<TaskQueueHandle>) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        let task = run(Rc::downgrade(slice), receiver, queue);
        // on the apply queue where there is one, so a batch runs below the foreground
        let spawned = match queue {
            Some(queue) => glommio::spawn_local_into(task, queue).map(glommio::Task::detach),
            None => Ok(glommio::spawn_local(task).detach()),
        };
        if let Err(error) = spawned {
            tracing::event!(tracing::Level::ERROR, msg = "a rotational slice's applier did not start", %error);
        }
        Applier { sender }
    }

    /// Send an apply to wait for its batch, answered once it has run
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `label` - The label its committed write makes
    ///
    /// # Errors
    ///
    /// The apply's own refusal, or one naming a slice that closed before its batch ran; its record
    /// stays, for the next start's decide poll.
    pub async fn apply(&self, id: ChunkId, label: Label) -> Result<(), StoreError> {
        let (reply, answer) = oneshot::channel();
        self.sender
            .unbounded_send(Pending { id, label, reply })
            .map_err(|_| closed())?;
        answer.await.unwrap_or_else(|_| Err(closed()))
    }
}

/// The refusal of an apply whose slice closed before its batch ran
fn closed() -> StoreError {
    StoreError::Invalid("the slice closed before its apply's batch ran; its record stays".to_string())
}

/// How many applies one batch takes: as many as the bound holds at what an apply was reckoned to
/// take, one at least and [`MAX_BATCH`] at most
///
/// # Arguments
///
/// * `bound` - The longest a batch takes
/// * `estimate` - What one apply in a batch is reckoned to take
#[must_use]
pub fn batch_size(bound: Duration, estimate: Duration) -> usize {
    let per = estimate.as_micros().max(1);
    usize::try_from(bound.as_micros() / per)
        .unwrap_or(MAX_BATCH)
        .clamp(1, MAX_BATCH)
}

/// The applier's loop: pending applies gathered, the foreground waited out for at most one bound,
/// and a batch issued whole, until the slice closes
///
/// # Arguments
///
/// * `slice` - The slice, held weakly
/// * `receiver` - Where its applies arrive
/// * `queue` - The queue its applies run on
async fn run(slice: Weak<Slice>, mut receiver: mpsc::UnboundedReceiver<Pending>, queue: Option<TaskQueueHandle>) {
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut estimate = SEED_ESTIMATE;
    loop {
        // the first apply waited for, then everything already sent
        if pending.is_empty() {
            match receiver.next().await {
                Some(first) => pending.push_back(first),
                None => break,
            }
        }
        while let Ok(Some(more)) = receiver.try_next() {
            pending.push_back(more);
        }
        let Some(open) = slice.upgrade() else {
            break;
        };
        let state = open.state().clone();
        let bound = open.device().spec.apply_batch_bound();
        // the foreground first, for at most one bound, so a read waits for one batch and applies
        // for one bound
        let waiting = Instant::now();
        while !state.idle() && waiting.elapsed() < bound {
            glommio::timer::sleep(IDLE_POLL).await;
        }
        while let Ok(Some(more)) = receiver.try_next() {
            pending.push_back(more);
        }
        // as many as the bound holds and the budget can draw for, every one in flight at once
        let take = batch_size(bound, estimate);
        let mut batch: Vec<Pending> = Vec::with_capacity(take);
        let mut bytes = 0u64;
        let room = open.batch_room();
        while batch.len() < take {
            let Some(next) = pending.front() else {
                break;
            };
            let needs = open.apply_bytes(&next.id, next.label);
            if !batch.is_empty() && bytes.saturating_add(needs) > room {
                break;
            }
            bytes = bytes.saturating_add(needs);
            if let Some(next) = pending.pop_front() {
                batch.push(next);
            }
        }
        state.apply_batch_in_flight.store(1, Ordering::Release);
        state.apply_batches.fetch_add(1, Ordering::Relaxed);
        state.apply_batch_peak.fetch_max(batch.len() as u64, Ordering::Relaxed);
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(batch.len());
        let mut replies = Vec::with_capacity(batch.len());
        for Pending { id, label, reply } in batch {
            let open = open.clone();
            let task = async move { open.apply_now(id, label).await };
            let spawned = match queue {
                Some(queue) => glommio::spawn_local_into(task, queue),
                None => Ok(glommio::spawn_local(task)),
            };
            match spawned {
                Ok(task) => {
                    tasks.push(task);
                    replies.push(reply);
                }
                Err(error) => {
                    let _ = reply.send(Err(StoreError::Invalid(format!("an apply could not be spawned: {error}"))));
                }
            }
        }
        let issued = tasks.len();
        let answers = futures::future::join_all(tasks).await;
        for (reply, answer) in replies.into_iter().zip(answers) {
            let _ = reply.send(answer);
        }
        // what a batch took, for the bound, the figures and the next batch's size
        let took = started.elapsed();
        state.apply_batch_in_flight.store(0, Ordering::Release);
        state
            .apply_batch_longest_us
            .fetch_max(u64::try_from(took.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        if issued > 0 {
            let per = took / u32::try_from(issued).unwrap_or(u32::MAX);
            estimate = (estimate * 3 + per) / 4;
        }
        drop(open);
    }
    // a slice that closed answers what it never ran, whose records stay for the next start
    for left in pending {
        let _ = left.reply.send(Err(closed()));
    }
    while let Ok(Some(left)) = receiver.try_next() {
        let _ = left.reply.send(Err(closed()));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{batch_size, MAX_BATCH};

    /// A batch holds what its bound holds at what an apply is reckoned to take, one at least and
    /// the most X7 measured at most
    #[test]
    fn a_batch_is_what_its_bound_holds() {
        assert_eq!(batch_size(Duration::from_millis(100), Duration::from_millis(10)), 10);
        assert_eq!(batch_size(Duration::from_millis(100), Duration::from_millis(40)), 2);
        assert_eq!(batch_size(Duration::from_millis(5), Duration::from_millis(10)), 1);
        assert_eq!(batch_size(Duration::from_secs(10), Duration::from_micros(1)), MAX_BATCH);
        assert_eq!(batch_size(Duration::from_millis(100), Duration::ZERO), MAX_BATCH);
    }
}
