//! A put: the whole object, held in its entry or written as stripes in two commits
//!
//! At or under its pool's inline threshold an object is held in its path's entry, one commit
//! ([F80](../../../../../docs/src/features/object-wire.md)). Past it, a put is S7's whole object
//! in two commits ([S7](../../../../../docs/src/object-storage/write-path.md#a-whole-object-in-one-commit)):
//!
//! 1. its new object's id is recorded in the entry as a replacement in flight - registration
//!    comes first, so no chunk is written for an id no row names;
//! 2. its stripe chunks are written straight into place on every holder, labelled with the put's
//!    label, through a pipeline on one of the node's object executors, and a stripe a holder
//!    missed is given a row naming that holder missed;
//! 3. once every stripe is held by `k + f` holders, one conditional commit of the entry makes the
//!    id current and retires the one it replaces.
//!
//! A reader sees the old object or the new one ([P19](../../../../../docs/src/object-storage/contract.md)).
//! A put that fails after its registration commits its id away, retired, for reclamation (M20)
//! to find.

use super::place::Placement;
use super::{now_ms, MetaAnswer, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::server::objects::put::{self as pipeline, PutPlan};
use crate::shared::checksum;
use crate::server::objects::faults::object_id;
use crate::shared::objects::{ObjectEntry, ObjectRow, ObjectState};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::ObjectOutcome;
use crate::shared::stripes::StripeRow;
use crate::shared::traits::QuerySupport;

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Put the whole object, inline or as stripes
    pub(super) async fn put(&self) -> Outcome {
        match &self.stream {
            Some(stream) => self.put_stripes(stream.clone()).await,
            None => self.put_inline().await,
        }
    }

    /// Put an object held in its entry: check its bytes, then commit its entry
    async fn put_inline(&self) -> Outcome {
        let op = &self.op;
        // the bytes, held to the checksum the head named before anything is sent
        let bytes = self.data.as_deref().unwrap_or(&[]);
        let crc = checksum::crc64(bytes);
        if crc != op.checksum {
            return Ok((self.mismatch(bytes.len() as u64, crc), None));
        }
        // the row its entry lives in, and the object this put makes, minted from the put itself
        let key = self.key();
        let object = object_id(&op.id, &op.path);
        for _ in 0..MAX_TRIES {
            // what the row holds now
            let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
            let current = row.as_ref().and_then(|row| row.entry(&op.path));
            // a retry of a put that committed finds its own object and commits nothing
            if let Some(entry) = current {
                if entry.object == object && entry.state.live {
                    return Ok((self.done(entry, crc), None));
                }
            }
            // the entry this put makes, the row's other entries untouched
            let entry = self.made(current, object, bytes.len() as u64, Some(bytes.to_vec()));
            match self.meta.ask(self.with_entry(key, row.as_ref(), entry.clone()), None).await {
                MetaAnswer::Written => return Ok((self.done(&entry, crc), None)),
                // the row moved under this put: read it again
                MetaAnswer::Refused(_) => continue,
                // a commit whose outcome is unknown may have applied: the row says whether
                MetaAnswer::Failed(ErrorCode::OutcomeUnknown | ErrorCode::Timeout, why) => {
                    if let Some(entry) = self.committed(key, object).await? {
                        return Ok((self.done(&entry, crc), None));
                    }
                    return Err((ErrorCode::OutcomeUnknown, why));
                }
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((ErrorCode::Internal, "a write answered as a read".to_string()))
                }
            }
        }
        Err(super::contended(&op.path))
    }

    /// Put an object as stripes: register its id, write its chunks, then make it current
    ///
    /// # Arguments
    ///
    /// * `stream` - The put's bytes, as the client sends them
    async fn put_stripes(&self, stream: kanal::AsyncReceiver<Vec<u8>>) -> Outcome {
        let op = &self.op;
        // a node with no object executor writes no stripes
        let store = match self.store() {
            Ok(store) => store.clone(),
            Err(refused) => return Ok((refused, None)),
        };
        // a pool ready at its pin, of a kind M15 writes
        let placement = match Placement::new(self.map.clone(), &self.pool_name, &self.pool, self.consumer, true) {
            Ok(placement) => placement,
            Err((outcome, why)) => return Ok((self.refused(outcome, why), None)),
        };
        let key = self.key();
        let object = object_id(&op.id, &op.path);
        // 1. registration: the entry names the object in flight before any chunk is written
        match self.register(key, object).await? {
            Registered::Done(entry) => return Ok((self.done(&entry, op.checksum), None)),
            Registered::InFlight => {}
        }
        // 2. the chunks, through a pipeline on an object executor
        let geometry = self.pool.geometry();
        let stripes = op.length.div_ceil(u64::from(geometry.stripe_bytes));
        let mut placed = Vec::with_capacity(usize::try_from(stripes).unwrap_or(0));
        for stripe in 0..stripes {
            match placement.stripe(object, stripe) {
                Ok(holders) => placed.push(holders),
                Err(why) => {
                    self.abandon(key, object).await;
                    return Ok((self.refused(ObjectOutcome::NotReady, why), None));
                }
            }
        }
        let plan = PutPlan {
            op: op.id,
            stripe_bytes: u64::from(geometry.stripe_bytes),
            unit_bytes: geometry.unit_bytes,
            length: op.length,
            stripes: placed,
            needed: placement.needed(),
            straggler_wait: self.timings.straggler_wait.duration(),
            deadline: self.meta.wait,
            inflight: self.timings.inflight_stripes,
        };
        let written = pipeline::start(&store, plan, stream).recv().await;
        let done = match written {
            Ok(Ok(done)) => done,
            Ok(Err(failure)) => {
                self.abandon(key, object).await;
                return Ok((self.refused(ObjectOutcome::NotReady, failure.to_string()), None));
            }
            Err(_) => {
                self.abandon(key, object).await;
                return Err((ErrorCode::Unavailable, "the put's pipeline stopped".to_string()));
            }
        };
        // the bytes are the ones the client named, or nothing is made current
        if done.crc != op.checksum {
            self.abandon(key, object).await;
            return Ok((self.mismatch(done.bytes, done.crc), None));
        }
        // a stripe some holders missed gets a row that says so, before the object is current
        for (stripe, positions) in &done.missed {
            let start = stripe * u64::from(geometry.stripe_bytes);
            let length = op.length.saturating_sub(start).min(u64::from(geometry.stripe_bytes));
            let mut row = StripeRow::put(self.consumer, object, *stripe, placement.width(), length);
            for position in positions {
                if let Some(label) = row.labels.get_mut(*position) {
                    *label = label.marked();
                }
            }
            match self.meta.commit_stripe(row, None, None).await {
                // written, or a retry of this put wrote it already
                crate::shared::objects::StripeAnswer::Written
                | crate::shared::objects::StripeAnswer::Refused(_) => {}
                crate::shared::objects::StripeAnswer::Failed(code, why) => {
                    self.abandon(key, object).await;
                    return Err((code, why));
                }
                _ => {
                    self.abandon(key, object).await;
                    return Err((ErrorCode::Internal, "a stripe's row answered as a read".to_string()));
                }
            }
        }
        // 3. the entry, making the object current and retiring the one it replaces
        self.make_current(key, object, done.crc).await
    }

    /// Record a put's object in its entry as a replacement in flight
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `object` - The put's object
    async fn register(&self, key: u64, object: u128) -> Result<Registered, (ErrorCode, String)> {
        let op = &self.op;
        for _ in 0..MAX_TRIES {
            let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
            let current = row.as_ref().and_then(|row| row.entry(&op.path));
            // a retry finds its own object current, or still in flight
            if let Some(entry) = current {
                if entry.object == object && entry.state.live {
                    return Ok(Registered::Done(entry.clone()));
                }
                if entry.state.replacing == Some(object) {
                    return Ok(Registered::InFlight);
                }
            }
            // the entry as it is, or a new path's entry that is not live, naming the put in flight
            let mut entry = match current {
                Some(entry) => entry.clone(),
                None => ObjectEntry {
                    path: op.path.clone(),
                    geometry: self.pool.geometry(),
                    created_ms: now_ms(),
                    modified_ms: now_ms(),
                    ..ObjectEntry::default()
                },
            };
            // a put already in flight there is abandoned, retired for reclamation to find
            if let Some(earlier) = entry.state.replacing.replace(object) {
                entry.state.retired.push(earlier);
            }
            match self.meta.ask(self.with_entry(key, row.as_ref(), entry), None).await {
                MetaAnswer::Written => return Ok(Registered::InFlight),
                MetaAnswer::Refused(_) => continue,
                // unknown: the next read says whether it applied
                MetaAnswer::Failed(ErrorCode::OutcomeUnknown | ErrorCode::Timeout, _) => continue,
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((ErrorCode::Internal, "a write answered as a read".to_string()))
                }
            }
        }
        Err(super::contended(&op.path))
    }

    /// Make a put's object current: one conditional commit of its entry
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `object` - The put's object
    /// * `crc` - The checksum of its bytes
    async fn make_current(&self, key: u64, object: u128, crc: u64) -> Outcome {
        let op = &self.op;
        for _ in 0..MAX_TRIES {
            let row = self.meta.row(key, Some(DEFAULT_LEVEL)).await?;
            let current = row.as_ref().and_then(|row| row.entry(&op.path));
            match current {
                // a retry that committed already
                Some(entry) if entry.object == object && entry.state.live => {
                    return Ok((self.done(entry, crc), None))
                }
                // still in flight here
                Some(entry) if entry.state.replacing == Some(object) => {}
                // another put or a delete took the path from under it: this put is not current
                _ => {
                    return Ok((
                        self.refused(
                            ObjectOutcome::Busy,
                            format!("another write to {} replaced this put before it committed", op.path),
                        ),
                        None,
                    ))
                }
            }
            let entry = self.made(current, object, op.length, None);
            match self.meta.ask(self.with_entry(key, row.as_ref(), entry.clone()), None).await {
                MetaAnswer::Written => return Ok((self.done(&entry, crc), None)),
                MetaAnswer::Refused(_) => continue,
                MetaAnswer::Failed(ErrorCode::OutcomeUnknown | ErrorCode::Timeout, why) => {
                    if let Some(entry) = self.committed(key, object).await? {
                        return Ok((self.done(&entry, crc), None));
                    }
                    return Err((ErrorCode::OutcomeUnknown, why));
                }
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((ErrorCode::Internal, "a write answered as a read".to_string()))
                }
            }
        }
        Err(super::contended(&op.path))
    }

    /// Give up a put after its registration: its object retired, for reclamation to find
    ///
    /// Best effort: a put whose abandon does not commit leaves its id in flight, which the next
    /// put to the path retires.
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `object` - The put's object
    pub(super) async fn abandon(&self, key: u64, object: u128) {
        for _ in 0..MAX_TRIES {
            let Ok(Some(row)) = self.meta.row(key, Some(DEFAULT_LEVEL)).await else {
                return;
            };
            let Some(mut entry) = row.entry(&self.op.path).cloned() else {
                return;
            };
            if entry.state.replacing != Some(object) {
                return;
            }
            entry.state.replacing = None;
            entry.state.retired.push(object);
            match self.meta.ask(self.with_entry(key, Some(&row), entry), None).await {
                MetaAnswer::Refused(_) => continue,
                _ => return,
            }
        }
    }

    /// The entry a write leaves for its object, over the one at the path if there is one
    ///
    /// # Arguments
    ///
    /// * `current` - The entry at the path
    /// * `object` - The object made current
    /// * `size` - Its size
    /// * `inline` - Its bytes, when it is held inline
    pub(super) fn made(
        &self,
        current: Option<&ObjectEntry>,
        object: u128,
        size: u64,
        inline: Option<Vec<u8>>,
    ) -> ObjectEntry {
        let now = now_ms();
        // the object it replaces is retired, with any put in flight that is not this one
        let mut retired = current.map(|entry| entry.state.retired.clone()).unwrap_or_default();
        if let Some(entry) = current {
            if entry.state.live && entry.object != 0 && entry.object != object {
                retired.push(entry.object);
            }
            if let Some(replacing) = entry.state.replacing.filter(|replacing| *replacing != object) {
                retired.push(replacing);
            }
        }
        ObjectEntry {
            path: self.op.path.clone(),
            object,
            content_version: current.map_or(0, |entry| entry.content_version) + 1,
            size,
            put_length: size,
            geometry: self.pool.geometry(),
            epoch: 0,
            floors: Vec::new(),
            state: ObjectState {
                live: true,
                replacing: None,
                retired,
            },
            created_ms: now,
            modified_ms: now,
            attributes: self.op.attributes.clone().unwrap_or_default(),
            inline,
        }
    }

    /// The query committing an entry into its row, conditional on the row read
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `row` - The row read, if there was one
    /// * `entry` - The entry
    pub(super) fn with_entry(
        &self,
        key: u64,
        row: Option<&ObjectRow>,
        entry: ObjectEntry,
    ) -> <D::ClientType as QuerySupport>::QueryKinds {
        match row {
            Some(row) => {
                let mut next = ObjectRow {
                    key,
                    version: row.version + 1,
                    entries: row.entries.clone(),
                };
                match next.position(&entry.path) {
                    Some(at) => next.entries[at] = entry,
                    None => next.entries.push(entry),
                }
                self.meta.ops.replace(next, row.version)
            }
            None => self.meta.ops.create(ObjectRow {
                key,
                version: 1,
                entries: vec![entry],
            }),
        }
    }

    /// The entry of a put's object, if a commit whose outcome was unknown made it current
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `object` - The put's object
    async fn committed(&self, key: u64, object: u128) -> Result<Option<ObjectEntry>, (ErrorCode, String)> {
        Ok(self
            .meta
            .row(key, Some(DEFAULT_LEVEL))
            .await?
            .and_then(|row| row.entry(&self.op.path).cloned())
            .filter(|entry| entry.object == object && entry.state.live))
    }

    /// The refusal of a put or a write whose bytes are not the ones its head named
    ///
    /// # Arguments
    ///
    /// * `bytes` - How many bytes arrived
    /// * `crc` - Their checksum
    pub(super) fn mismatch(&self, bytes: u64, crc: u64) -> crate::shared::protocol::object::ObjectAnswer {
        self.refused(
            ObjectOutcome::ChecksumMismatch,
            format!(
                "the {bytes} bytes have CRC-64/NVME {crc:#018x}, not the {:#018x} the head named; nothing was committed",
                self.op.checksum
            ),
        )
    }
}

/// What registering a put found
enum Registered {
    /// The put is in flight and its chunks may be written
    InFlight,
    /// A retry of a put that committed: this is its entry
    Done(ObjectEntry),
}
