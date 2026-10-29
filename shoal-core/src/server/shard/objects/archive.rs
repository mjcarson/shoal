//! A bucket's objects backed up and put again, each at one state
//! ([F93](../../../../../docs/src/features/operations-and-the-real-cluster.md))
//!
//! A backup reads every live object a shard's `ObjectMeta` groups lead, its entry and its bytes,
//! into an archive ([`crate::server::objects::archive`]); a restore puts each again through this
//! shard's coordinator, into the pool the destination binds the bucket to. Both are object tasks
//! with no connection behind them: a task is made here as `handle_object` makes one, and its
//! outcome acted on rather than answered.
//!
//! # Invariants
//!
//! **An archived object is one state it held.** Its entry and every stripe's row are read
//! strongly before its bytes and again after them, and the object is archived only when both
//! reads agree: a write in place moves a stripe's row, a truncate or an append the entry, and a
//! replace the entry's object, so bytes read between two equal reads are of one state. An object
//! that keeps moving is read again a bounded number of times, then named as the failure.
//!
//! **A put again is the same put however often it is asked.** Its identity is derived from the
//! restore, the archive, the record and the try, so a driver that resumes past an object it put
//! is answered as the first put was, and an identity a put gave up on is never asked again.
//!
//! **A put again goes over no live object it did not make.** A path a client wrote since the restore
//! began is left as the client left it, which a restore driven again must not undo
//! ([Resolved #261](../../../../../docs/src/appendix/resolved/archive-driven-once.md)).

use bytes::Bytes;
use tracing::{event, Level};

use super::read::RangeFailure;
use super::{Meta, MetaClient, ObjectTask, Outcome, DEFAULT_LEVEL};
use crate::server::conf::ObjectTimings;
use crate::server::database::ShoalDatabase;
use crate::server::messages::ServerMsg;
use crate::server::objects::archive::{ArchiveWriter, ArchivedObject};
use crate::server::objects::faults::object_id;
use crate::server::objects::ObjectStore;
use crate::shared::checksum;
use crate::shared::identity::BucketId;
use crate::shared::objects::{ObjectEntry, ObjectOps};
use crate::shared::pools::PoolSpec;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::{ObjectOp, ObjectOutcome};
use crate::shared::stripes::StripeRow;
use crate::shared::traits::QuerySupport;

/// How many times an object that moved under its backup is read again before it is the failure
const MOVED_TRIES: u32 = 8;

/// How many stripes' rows one strong read asks for
const ROWS_A_READ: u64 = 256;

/// How many times a put again is asked before its archive fails
const PUT_TRIES: u32 = 32;

/// The bytes a striped put again is fed in, a frame's worth at a time as a client's would be
const PIECE: usize = 1 << 20;

/// What a driver with no connection makes its object tasks from: one bucket on one shard
pub(crate) struct ObjectsBase<D: ShoalDatabase> {
    /// The bucket's queries
    pub ops: &'static dyn ObjectOps<D::ClientType>,
    /// The bucket's identity
    pub bucket: BucketId,
    /// The shard's metadata client
    pub client: MetaClient,
    /// The shard's own channel
    pub shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    /// The pool the bucket is bound to, by name
    pub pool_name: String,
    /// The pool
    pub pool: PoolSpec,
    /// The consumer the bucket is bound as
    pub consumer: u64,
    /// The shard's map, which every task places by as it is when the task starts
    pub map: crate::server::map::MapCell,
    /// The node's object store
    pub store: Option<ObjectStore>,
    /// Whether this node starts one
    pub store_starts: bool,
    /// The object timings
    pub timings: ObjectTimings,
    /// How long one internal query may take
    pub wait: std::time::Duration,
}

impl<D: ShoalDatabase> Clone for ObjectsBase<D> {
    /// The same bucket on the same shard, for another driver
    fn clone(&self) -> Self {
        ObjectsBase {
            ops: self.ops,
            bucket: self.bucket,
            client: self.client.clone(),
            shard_tx: self.shard_tx.clone(),
            pool_name: self.pool_name.clone(),
            pool: self.pool.clone(),
            consumer: self.consumer,
            map: self.map.clone(),
            store: self.store.clone(),
            store_starts: self.store_starts,
            timings: self.timings.clone(),
            wait: self.wait,
        }
    }
}

impl<D: ShoalDatabase> ObjectsBase<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// The object task an operation would be, with nobody to answer
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `data` - An inline put's bytes
    /// * `stream` - A striped put's bytes, as they come
    pub fn task(
        &self,
        op: ObjectOp,
        data: Option<Bytes>,
        stream: Option<kanal::AsyncReceiver<Vec<u8>>>,
    ) -> ObjectTask<D> {
        // a reply nobody reads: the outcome is the driver's
        let (reply_tx, _replies) = kanal::bounded_async(1);
        ObjectTask {
            meta: Meta {
                ops: self.ops,
                client: self.client.clone(),
                shard_tx: self.shard_tx.clone(),
                span: tracing::info_span!("Object::archive", path = %op.path),
                wait: self.wait,
            },
            op,
            data,
            stream,
            _hold: None,
            pool_name: self.pool_name.clone(),
            pool: self.pool.clone(),
            consumer: self.consumer,
            map: self.map.get(),
            store: self.store.clone(),
            store_starts: self.store_starts,
            timings: self.timings.clone(),
            reply_tx,
            attempt: 0,
        }
    }

    /// Write one object to an archive at one state it held
    ///
    /// Answers whether it was written: an object no longer live at its path, replaced or deleted
    /// since the walk read it, is nothing to back up.
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry, as a walk read it
    /// * `writer` - The archive
    ///
    /// # Errors
    ///
    /// The object kept moving, its row is behind its chunks or declared lost, or the archive
    /// cannot be written.
    pub async fn archive(&self, entry: &ObjectEntry, writer: &mut ArchiveWriter) -> Result<bool, String> {
        // a test refuses it as a group that cannot confirm its leader refuses a strong read (item 322)
        if crate::server::objects::faults::archive_read_refused() {
            return Err(refusal(
                ErrorCode::QuorumUnavailable,
                format!("group of {} could not confirm its leader: a test refused the archive's read", entry.path),
            ));
        }
        let op = ObjectOp::read(uuid::Uuid::now_v7(), self.bucket, &entry.path, 0, u64::MAX);
        let task = self.task(op, None, None);
        for _ in 0..MOVED_TRIES {
            // the entry, strongly, which the walk's copy may be behind
            let Some(before) = task.live_entry().await.map_err(|(code, why)| refusal(code, why))? else {
                return Ok(false);
            };
            // an inline object is its entry
            if let Some(inline) = &before.inline {
                writer.begin(&before, inline.len() as u64).await?;
                writer.push(inline.clone()).await?;
                writer.end().await?;
                return Ok(true);
            }
            // every stripe's row, strongly, before the bytes
            let rows = task.rows_of(&before).await.map_err(|(code, why)| refusal(code, why))?;
            writer.begin(&before, before.size).await?;
            let mut offset = 0u64;
            let mut moved = false;
            while offset < before.size {
                let end = before
                    .size
                    .min(offset.saturating_add(self.timings.max_read as u64));
                match task.read_range(before.clone(), offset, end).await {
                    // the range as its stripes were read, written from where they are (F109)
                    Ok(range) => {
                        offset += range.len();
                        writer.push(range).await?;
                    }
                    // a row behind its chunks or declared lost is never archived as anything
                    Err(RangeFailure::Behind(why) | RangeFailure::Lost(why)) => {
                        writer.abort().await?;
                        return Err(why);
                    }
                    // a read that met the object moving, or a holder away, is read again
                    Err(RangeFailure::Code(ErrorCode::Shedding | ErrorCode::Unavailable, _)) => {
                        moved = true;
                        break;
                    }
                    Err(RangeFailure::Code(code, why)) => {
                        writer.abort().await?;
                        return Err(refusal(code, why));
                    }
                }
            }
            // the same entry and the same rows after the bytes: one state
            if !moved {
                let after = task.live_entry().await.map_err(|(code, why)| refusal(code, why))?;
                let same_entry = after.as_ref().is_some_and(|after| {
                    after.object == before.object
                        && after.epoch == before.epoch
                        && after.content_version == before.content_version
                        && after.size == before.size
                });
                if same_entry && task.rows_of(&before).await.map_err(|(code, why)| refusal(code, why))? == rows {
                    writer.end().await?;
                    return Ok(true);
                }
            }
            writer.abort().await?;
            glommio::timer::sleep(std::time::Duration::from_millis(50)).await;
        }
        // an object that kept moving, or a holder that stayed away, may not on a later try (item 322)
        Err(format!(
            "{}{} moved under its backup {MOVED_TRIES} times",
            crate::server::shard::backup::RETRIABLE,
            entry.path
        ))
    }

    /// Put one archived object again through this shard's coordinator
    ///
    /// A path with an object live at it is never put over: one of this record's own puts is the
    /// record done, and anyone else's is a write a client made since the restore began, which a
    /// restore driven again must not undo
    /// ([Resolved #261](../../../../../docs/src/appendix/resolved/archive-driven-once.md)).
    ///
    /// # Arguments
    ///
    /// * `object` - The object as the archive holds it
    /// * `identity` - What its put's identity is derived from: the restore, the archive, the record
    ///
    /// # Errors
    ///
    /// The put was refused for good, or kept failing.
    pub async fn put_again(&self, object: &ArchivedObject, identity: &str) -> Result<(), String> {
        let length = object.bytes.len() as u64;
        let crc = checksum::crc64(&object.bytes);
        let path = &object.entry.path;
        // every object a try of this record's put can make, whichever attempt it was
        let ours: Vec<u128> = (0..PUT_TRIES)
            .map(|attempt| object_id(&derived_id(&format!("{identity}/{attempt}")), path))
            .collect();
        let mut attempt = 0u32;
        let mut tries = 0u32;
        loop {
            tries += 1;
            // the identity of this try, the same however often it is asked
            let id = derived_id(&format!("{identity}/{attempt}"));
            let mut op = ObjectOp::put(id, self.bucket, path, length, crc);
            op.attributes = Some(object.entry.attributes.clone());
            // what is live at the path before this try writes anything: ours is the record
            // done, and another's is left as it is (item 261)
            match self.task(op.clone(), None, None).live_entry().await {
                Ok(Some(live)) if ours.contains(&live.object) => {
                    // said, so a node's log tells a record done by an earlier try from one put now
                    // (item 315)
                    event!(Level::DEBUG, msg = "a restored object's own put is live at its path", %path, %identity, object = %format!("{:032x}", live.object), tries);
                    return Ok(());
                }
                Ok(Some(live)) => {
                    event!(Level::INFO, msg = "a restored object's path was written since the restore began; left as it is", %path, object = %format!("{:032x}", live.object));
                    return Ok(());
                }
                Ok(None) => {}
                Err((code, why)) => {
                    event!(Level::DEBUG, msg = "a restored object's path could not be read before its put", %path, %identity, tries, %code, why);
                    if tries >= PUT_TRIES {
                        return Err(format!("{path} could not be read before it was put again: {code}: {why}"));
                    }
                    glommio::timer::sleep(std::time::Duration::from_millis(200 * u64::from(tries.min(10)))).await;
                    continue;
                }
            }
            // an object at or under the pool's threshold rides its entry, a larger one is fed
            // to its stripes a piece at a time, as a client's put is
            let task = if length <= self.pool.inline_threshold() {
                self.task(op, Some(Bytes::from(object.bytes.clone())), None)
            } else {
                let (tx, rx) = kanal::bounded_async::<Vec<u8>>(4);
                let pieces: Vec<Vec<u8>> = object.bytes.chunks(PIECE).map(<[u8]>::to_vec).collect();
                glommio::spawn_local(async move {
                    for piece in pieces {
                        if tx.send(piece).await.is_err() {
                            return;
                        }
                    }
                })
                .detach();
                self.task(op, None, Some(rx))
            };
            let outcome: Outcome = task.execute().await;
            let why = match outcome {
                Ok((answer, _)) => match answer.outcome {
                    ObjectOutcome::Done => {
                        // said with the object it made, which a get that cannot find it is read
                        // against (item 315)
                        event!(Level::DEBUG, msg = "a restored object was put again", %path, %identity, object = %format!("{:032x}", object_id(&id, path)), tries, inline = length <= self.pool.inline_threshold());
                        return Ok(());
                    }
                    // asked again under the same identity, which a pool short for now may take
                    ObjectOutcome::NotReady | ObjectOutcome::Busy => answer.message,
                    // an identity given up after writing is never asked again: a new one is
                    ObjectOutcome::Abandoned | ObjectOutcome::Gone => {
                        attempt += 1;
                        answer.message
                    }
                    other => {
                        return Err(format!(
                            "{} was refused {other}: {}",
                            object.entry.path, answer.message
                        ))
                    }
                },
                Err((code, why)) => format!("{code}: {why}"),
            };
            event!(Level::DEBUG, msg = "a restored object's put is tried again", %path, %identity, tries, attempt, why);
            if tries >= PUT_TRIES {
                return Err(format!("{} could not be put again: {why}", object.entry.path));
            }
            glommio::timer::sleep(std::time::Duration::from_millis(200 * u64::from(tries.min(10)))).await;
        }
    }
}

/// An archive's error for a read refused with a code, marked a refusal for now when a later try may
/// not meet it, so its group's archive is driven again after a pause rather than failed (item 322)
///
/// The codes are the ones a client tries again under the same identity: a leader not one, a member
/// away, a quorum not there, a lost connection, an unknown outcome, a deadline, a shed and a route
/// by an older map. A read changes nothing, so a try again is always safe.
///
/// # Arguments
///
/// * `code` - What the read was refused with
/// * `why` - What it said
#[must_use]
pub fn refusal(code: ErrorCode, why: String) -> String {
    let for_now = matches!(
        code,
        ErrorCode::NotLeader
            | ErrorCode::Unavailable
            | ErrorCode::QuorumUnavailable
            | ErrorCode::ConnectionLost
            | ErrorCode::OutcomeUnknown
            | ErrorCode::Timeout
            | ErrorCode::Shedding
            | ErrorCode::StaleTopology
    );
    if for_now {
        format!("{}{code}: {why}", crate::server::shard::backup::RETRIABLE)
    } else {
        format!("{code}: {why}")
    }
}

/// A put's identity derived from a name, the same on every node and every try that names it
///
/// # Arguments
///
/// * `name` - What the identity stands for
#[must_use]
pub(crate) fn derived_id(name: &str) -> uuid::Uuid {
    // two checksums of the name under different prefixes, which no two names share both of
    let high = checksum::crc64(format!("archive/{name}").as_bytes());
    let low = checksum::crc64(format!("put/{name}").as_bytes());
    uuid::Uuid::from_u128((u128::from(high) << 64) | u128::from(low))
}

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// The operation's object's entry, strongly, if one is live at its path
    pub(super) async fn live_entry(&self) -> Result<Option<ObjectEntry>, (ErrorCode, String)> {
        let row = self.meta.row(self.key(), Some(DEFAULT_LEVEL)).await?;
        Ok(row
            .and_then(|row| row.entry(&self.op.path).cloned())
            .filter(|entry| entry.state.live))
    }

    /// Every stripe's row of an object, strongly, a bounded number at a time
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry
    pub(super) async fn rows_of(
        &self,
        entry: &ObjectEntry,
    ) -> Result<std::collections::BTreeMap<u64, StripeRow>, (ErrorCode, String)> {
        let stripe_bytes = u64::from(entry.geometry.stripe_bytes.max(1));
        let stripes = entry.extent.max(entry.size).div_ceil(stripe_bytes);
        let mut rows = std::collections::BTreeMap::new();
        let mut first = 0;
        while first < stripes {
            let these: Vec<u64> = (first..stripes.min(first + ROWS_A_READ)).collect();
            rows.extend(self.meta.stripe_rows(self.consumer, entry.object, &these).await?);
            first += ROWS_A_READ;
        }
        Ok(rows)
    }

}

#[cfg(test)]
mod tests {
    use super::{derived_id, refusal};
    use crate::server::shard::backup::RETRIABLE;
    use crate::shared::protocol::error::ErrorCode;

    /// A read refused with a code a later try may not meet is marked a refusal for now, which drives
    /// its group's archive again; one a try again would meet the same is not (item 322)
    #[test]
    fn a_refusal_for_now_is_marked_so() {
        for code in [ErrorCode::QuorumUnavailable, ErrorCode::NotLeader, ErrorCode::Unavailable, ErrorCode::Timeout] {
            let error = refusal(code, "a read".to_string());
            assert!(error.starts_with(RETRIABLE), "{code} was not marked a refusal for now: {error}");
            assert!(error.contains(&code.to_string()), "{error}");
        }
        for code in [ErrorCode::CorruptArchive, ErrorCode::Internal, ErrorCode::Unauthorized] {
            let error = refusal(code, "a read".to_string());
            assert!(!error.starts_with(RETRIABLE), "{code} was marked a refusal for now: {error}");
        }
    }

    /// A derived identity is the same for one name and another for another
    #[test]
    fn a_derived_identity_is_the_names_own() {
        assert_eq!(derived_id("r/a/1/0"), derived_id("r/a/1/0"));
        assert_ne!(derived_id("r/a/1/0"), derived_id("r/a/1/1"));
        assert_ne!(derived_id("r/a/1/0"), derived_id("r/a/2/0"));
    }
}
