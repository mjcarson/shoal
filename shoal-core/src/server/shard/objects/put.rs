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

use super::place::{Placement, StripeSet};
use super::{now_ms, MetaAnswer, ObjectTask, Outcome, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::server::objects::put::{self as pipeline, PutDone, PutPlan};
use crate::shared::checksum;
use crate::server::objects::faults::object_id;
use crate::server::objects::Holder;
use crate::shared::objects::{ObjectEntry, ObjectRow, ObjectState, Retired, StripeAnswer};
use crate::shared::placement_state::PgKey;
use crate::shared::queries::ConditionRefusal;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::ObjectOutcome;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, SealScope};
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
            Registered::Retired => {
                return Ok((
                    self.refused(
                        ObjectOutcome::Gone,
                        format!(
                            "this put to {} was given up or replaced before it committed; put it again under a new identity",
                            op.path
                        ),
                    ),
                    None,
                ))
            }
        }
        // 2. the chunks, through a pipeline on an object executor, where each stripe's placement
        // group's state as this shard last read it names them
        let geometry = self.pool.geometry();
        let stripes = op.length.div_ceil(u64::from(geometry.stripe_bytes));
        let sets = match self.place_stripes(&placement, object, stripes).await {
            Ok(sets) => sets,
            // nothing was written, so the id is not retired and a retry under it goes on
            Err(why) => {
                self.abandon(key, object, false).await;
                return Ok((self.refused(ObjectOutcome::NotReady, why), None));
            }
        };
        let plan = PutPlan {
            op: op.id,
            stripe_bytes: u64::from(geometry.stripe_bytes),
            unit_bytes: geometry.unit_bytes,
            data: geometry.data,
            parity: geometry.parity,
            length: op.length,
            stripes: sets.iter().map(|set| set.targets.clone()).collect(),
            needed: placement.needed(),
            straggler_wait: self.timings.straggler_wait.duration(),
            deadline: self.meta.wait,
            inflight: self.timings.inflight_stripes,
            owner: key,
        };
        // a test can hold a put's bytes unread before its pipeline starts, as a put whose holders
        // take none of them would (item 309)
        if let Some(hold) = crate::server::objects::faults::feed_hold(&self.op.path) {
            crate::server::objects::faults::hold_this_feed(hold).await;
        }
        let written = pipeline::start(&store, plan, stream).recv().await;
        // from here a chunk may be on a holder: a try given up retires its id
        let done = match written {
            Ok(Ok(done)) => done,
            Ok(Err(failure)) => return self.given_up(key, object, ObjectOutcome::NotReady, failure.to_string()).await,
            Err(_) => {
                return self
                    .given_up(key, object, ObjectOutcome::NotReady, "the put's pipeline stopped".to_string())
                    .await
            }
        };
        // the bytes are the ones the client named, or nothing is made current
        if done.crc != op.checksum {
            self.abandon(key, object, true).await;
            return Ok((self.mismatch(done.bytes, done.crc), None));
        }
        // a stripe a position missed under its placement group's state now gets a row that says
        // so, before the object is current
        match self.check_put(&placement, object, &sets, &done, op.length).await {
            Ok(None) => {}
            Ok(Some(refused)) => {
                let why = refused.message.clone();
                return self.given_up(key, object, refused.outcome, why).await;
            }
            Err((_, why)) => return self.given_up(key, object, ObjectOutcome::NotReady, why).await,
        }
        // a test can hold a put here, its chunks placed and its object in flight, as it holds a
        // write's stager before its commit (M20)
        if let Some(hold) = crate::server::objects::faults::stager_hold() {
            crate::server::objects::faults::hold_this_stager(hold).await;
        }
        // 3. the entry, making the object current and retiring the one it replaces
        self.make_current(key, object, done.crc).await
    }

    /// Each stripe of a put's chunks, where its placement group's state as this shard last read
    /// it names them
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `object` - The object
    /// * `stripes` - How many stripes it has
    pub(super) async fn place_stripes(
        &self,
        placement: &Placement,
        object: u128,
        stripes: u64,
    ) -> Result<Vec<StripeSet>, String> {
        let mut sets = Vec::with_capacity(stripes as usize);
        for stripe in 0..stripes {
            // where its group is as this shard knows it, read first if the group may have moved
            let view = self.meta.placement_for(placement, placement.pg_of(object, stripe)).await;
            sets.push(placement.stripe_set(object, stripe, &view)?);
        }
        Ok(sets)
    }

    /// Check a put's stripes once its pieces are durable, against their placement groups' state
    /// read now, linearizably
    ///
    /// A stripe's positions are counted under the state now: each held only if every holder it
    /// names there got the put's piece. A group that began moving, or moved, while the pieces were
    /// written names an arrival no piece reached, which is marked missed in a row that claims the
    /// epoch read; one the pieces reached fewer than `k + f` positions of fails the put. Without
    /// this a put that wrote no row is lost from the arrival at the switch (X1's
    /// `put_unchecked_after_its_stripes`, [F88](../../../../../docs/src/features/recovery-and-moves.md)).
    ///
    /// Returns the refusal to answer with, if the put cannot be made current.
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `object` - The object
    /// * `sets` - Where each stripe's pieces were written
    /// * `done` - What the pipeline found
    /// * `length` - The object's length
    pub(super) async fn check_put(
        &self,
        placement: &Placement,
        object: u128,
        sets: &[StripeSet],
        done: &PutDone,
        length: u64,
    ) -> Result<Option<crate::shared::protocol::object::ObjectAnswer>, (ErrorCode, String)> {
        let geometry = self.pool.geometry();
        let stripe_bytes = u64::from(geometry.stripe_bytes);
        // every group the put's stripes fall in, once
        let mut keys: Vec<PgKey> = sets.iter().map(|set| set.key).collect();
        keys.sort_unstable();
        keys.dedup();
        // what the last try found the put left where its groups no longer place, for a put whose
        // groups moved under it too often
        let mut last_stale: Vec<(Holder, PgKey, u64)> = Vec::new();
        for _ in 0..MAX_TRIES {
            // a read that fails leaves what an earlier try found stale to be removed, on this way
            // out as on every other (item 269)
            let now = match self.meta.read_placements(&keys).await {
                Ok(now) => now,
                Err(error) => {
                    self.remove_stale(placement, object, last_stale).await;
                    return Err(error);
                }
            };
            let mut moved = false;
            // every holder a piece reached that its group no longer places on: what a placement
            // read before a move left there, which nothing else will remove (item 253). Found for
            // every stripe before any is judged, so a put refused below removes them too: a group
            // that left two of its slices at once leaves a stale put holding fewer than it needs
            let mut stale: Vec<(Holder, PgKey, u64)> = Vec::new();
            let mut currents: Vec<StripeSet> = Vec::with_capacity(sets.len());
            for (stripe, set) in sets.iter().enumerate() {
                let state = now.get(&set.key).cloned().unwrap_or_default();
                let current = match placement.stripe_set(object, stripe as u64, &state) {
                    Ok(current) => current,
                    Err(why) => {
                        // what this try found so far, and what the last try found of the stripes
                        // this one has not reached (item 269)
                        for left in last_stale.drain(..) {
                            if !stale.contains(&left) {
                                stale.push(left);
                            }
                        }
                        self.remove_stale(placement, object, stale).await;
                        return Ok(Some(self.refused(ObjectOutcome::NotReady, why)));
                    }
                };
                // whether its piece was counted or still on its way: a straggler lands late, and a
                // slice that refuses what it just removed turns it away
                for (_, written) in &set.targets {
                    if !current.targets.iter().any(|(_, now)| now == written) {
                        stale.push((*written, set.key, stripe as u64));
                    }
                }
                currents.push(current);
            }
            for (stripe, (set, current)) in sets.iter().zip(&currents).enumerate() {
                let stripe = stripe as u64;
                // each holder named now is held if the put's piece reached that very holder
                let reached = done.held.get(&stripe).cloned().unwrap_or_default();
                let held: Vec<bool> = current
                    .targets
                    .iter()
                    .map(|target| {
                        set.targets
                            .iter()
                            .zip(&reached)
                            .any(|(written, held)| *held && written == target)
                    })
                    .collect();
                // a position whose chunk holds no bytes holds it with no piece written (F90)
                let start = stripe * stripe_bytes;
                let held_bytes = length.saturating_sub(start).min(stripe_bytes);
                let positions: Vec<bool> = current
                    .positions(&held)
                    .into_iter()
                    .enumerate()
                    .map(|(position, held)| held || geometry.chunk_length(held_bytes, position) == 0)
                    .collect();
                let count = positions.iter().filter(|held| **held).count();
                if count < placement.needed() {
                    self.remove_stale(placement, object, stale).await;
                    return Ok(Some(self.refused(
                        ObjectOutcome::NotReady,
                        format!(
                            "stripe {stripe} of {} is held by {count} positions under its placement group's state now, and a put needs {}",
                            self.op.path,
                            placement.needed()
                        ),
                    )));
                }
                let missing: Vec<usize> = positions
                    .iter()
                    .enumerate()
                    .filter(|(_, held)| !**held)
                    .map(|(position, _)| position)
                    .collect();
                if missing.is_empty() {
                    continue;
                }
                // a row that says so, claiming the epoch read
                let marked = match self.mark_put(object, stripe, current.width, held_bytes, &missing, current.epoch).await {
                    Ok(marked) => marked,
                    Err(error) => {
                        self.remove_stale(placement, object, stale).await;
                        return Err(error);
                    }
                };
                match marked {
                    Marked::Done => {}
                    Marked::Moved => {
                        self.meta.refresh_placement(set.key).await;
                        moved = true;
                        break;
                    }
                }
            }
            if !moved {
                self.remove_stale(placement, object, stale).await;
                return Ok(None);
            }
            last_stale = stale;
        }
        // moved too often to be judged: what it left where its groups no longer place goes still
        self.remove_stale(placement, object, last_stale).await;
        Ok(Some(self.refused(
            ObjectOutcome::Busy,
            format!("the placement groups of {} moved under this put too often; try it again", self.op.path),
        )))
    }

    /// Remove what a put left on holders its placement groups no longer place on: each sealed,
    /// its group's state read again while sealed, and the object's chunks there removed under the
    /// seal unless the group places there again (item 253)
    ///
    /// Best effort: a holder that does not answer keeps its copy, which a drained device's removal
    /// then waits for. The object is not current yet, so no reader is owed a grace.
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `object` - The put's object
    /// * `stale` - Each holder, the placement group and a stripe of the object it holds there
    async fn remove_stale(&self, placement: &Placement, object: u128, mut stale: Vec<(Holder, PgKey, u64)>) {
        // one removal a holder and group, whichever stripes it holds there
        stale.sort_unstable_by_key(|(holder, key, _)| (holder.node, holder.slice, *key));
        stale.dedup_by_key(|(holder, key, _)| (holder.node, holder.slice, *key));
        let Ok(store) = self.store() else {
            return;
        };
        for (holder, key, stripe) in stale {
            let scope = SealScope::Object {
                consumer: self.consumer,
                pg: key.1,
                object,
            };
            let seal: u64 = rand::random();
            if store.ask_holder(holder, ChunkRequest::Seal { scope, seal }).await != ChunkReply::Done {
                continue;
            }
            // the group's state again, at its leader, while the holder is sealed
            let places = match self.meta.read_placements(&[key]).await {
                Ok(mut views) => {
                    let view = views.remove(&key).unwrap_or_default();
                    placement
                        .stripe_set_in(key.1, object, stripe, &view)
                        .map_or(true, |set| set.targets.iter().any(|(_, now)| *now == holder))
                }
                Err(_) => true,
            };
            if places {
                let _ = store.ask_holder(holder, ChunkRequest::Unseal { scope, seal }).await;
                continue;
            }
            match store.ask_holder(holder, ChunkRequest::Reclaim { scope, seal }).await {
                ChunkReply::Done => {
                    self.meta.client.count_reclaim(&placement.pool_name, |figures| figures.chunks += 1);
                    tracing::event!(tracing::Level::DEBUG, msg = "a put removed what it left on a holder its group moved off", object = %format!("{object:032x}"), ?key, ?holder);
                }
                other => {
                    let _ = store.ask_holder(holder, ChunkRequest::Unseal { scope, seal }).await;
                    tracing::event!(tracing::Level::DEBUG, msg = "a put's copy on a holder its group moved off was not removed", ?key, ?holder, ?other);
                }
            }
        }
    }

    /// Mark a put's stripe missed at some positions, in a row that claims a placement epoch
    ///
    /// A stripe with no row gets one; one whose row a retry of this put already made has the
    /// positions it does not mark yet marked over it.
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `stripe` - The stripe
    /// * `width` - Its positions
    /// * `length` - How much of it holds bytes
    /// * `missing` - The positions to mark
    /// * `epoch` - The placement epoch the marks were judged under
    async fn mark_put(
        &self,
        object: u128,
        stripe: u64,
        width: usize,
        length: u64,
        missing: &[usize],
        epoch: u64,
    ) -> Result<Marked, (ErrorCode, String)> {
        for _ in 0..MAX_TRIES {
            let rows = self.meta.stripe_rows(self.consumer, object, &[stripe]).await?;
            // the row there, or the put's as it would be
            let (read, row) = match rows.get(&stripe) {
                Some(row) => (Some(row.sequence), row.clone()),
                None => (None, StripeRow::put(self.consumer, object, stripe, width, length, self.key())),
            };
            // a row a build before M20 made names no owner; this one does from now on
            let mut row = row.owned_by(self.key());
            if missing.iter().all(|position| row.labels.get(*position).is_some_and(|label| label.is_missed())) {
                return Ok(Marked::Done);
            }
            // over a row, each mark a rebuild's that found nothing; where none is, the put's own
            for position in missing {
                if read.is_some() {
                    row = row.rebuilt(*position, false);
                } else if let Some(label) = row.labels.get_mut(*position) {
                    *label = label.marked();
                }
            }
            match self.meta.commit_stripe(row.claiming(epoch), read, None).await {
                StripeAnswer::Written => return Ok(Marked::Done),
                StripeAnswer::Refused(ConditionRefusal::Placement) => return Ok(Marked::Moved),
                // another commit landed first: read it again
                StripeAnswer::Refused(_) => continue,
                StripeAnswer::Failed(code, why) => return Err((code, why)),
                StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                    return Err((ErrorCode::Internal, "a stripe's row answered as a read".to_string()))
                }
            }
        }
        Err((
            ErrorCode::Timeout,
            format!("stripe {stripe} of {} moved too often to be marked", self.op.path),
        ))
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
                if entry.state.replacing_id() == Some(object) {
                    return Ok(Registered::InFlight);
                }
                // and one whose try was given up or replaced is never registered again: its id is
                // retired, and reclamation would remove what it wrote (M20)
                if entry.state.retires(object) {
                    return Ok(Registered::Retired);
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
            // a put already in flight there is abandoned, retired for reclamation to find, and this
            // one names how far its stripes will reach
            let registered = Retired {
                object,
                extent: op.length,
            };
            if let Some(earlier) = entry.state.replacing.replace(registered) {
                entry.state.retire(earlier);
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
                Some(entry) if entry.state.replacing_id() == Some(object) => {}
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

    /// Give a put up once its chunks may be on its holders: its id retired, and the try answered
    /// `Abandoned` where its own outcome would be tried again under the same identity, which would
    /// only meet its retired id (M20)
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `object` - The put's object
    /// * `outcome` - What the try came to
    /// * `why` - Why, for the client
    async fn given_up(&self, key: u64, object: u128, outcome: ObjectOutcome, why: String) -> Outcome {
        /// The warnings that a put was given up, one logged every `WARN_EVERY` with the count of the
        /// rest (F108)
        static GIVEN_UP: crate::server::objects::inflight::Throttle =
            crate::server::objects::inflight::Throttle::new();
        // counted in the node's figures, and said once a while (F108)
        self.meta.client.count_given_up();
        if let Some(besides) = GIVEN_UP.pass(crate::server::objects::inflight::WARN_EVERY) {
            tracing::event!(
                tracing::Level::WARN,
                msg = "a put was given up after its stripes failed",
                path = %self.op.path,
                why = %why,
                given_up_besides = besides,
            );
        }
        self.abandon(key, object, true).await;
        let outcome = if outcome.retryable() { ObjectOutcome::Abandoned } else { outcome };
        Ok((
            self.refused(
                outcome,
                format!("{why}; this try of {} was given up and its identity retired, so put it again under a new one", self.op.path),
            ),
            None,
        ))
    }

    /// Give up a put after its registration: its object retired for reclamation to find when a
    /// chunk of it may have been written, or only no longer in flight when none was
    ///
    /// Best effort: a put whose abandon does not commit leaves its id in flight, which the next
    /// put to the path retires.
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `object` - The put's object
    /// * `written` - Whether a chunk of it may be on a holder
    pub(super) async fn abandon(&self, key: u64, object: u128, written: bool) {
        for _ in 0..MAX_TRIES {
            let Ok(Some(row)) = self.meta.row(key, Some(DEFAULT_LEVEL)).await else {
                return;
            };
            let Some(mut entry) = row.entry(&self.op.path).cloned() else {
                return;
            };
            let Some(registered) = entry.state.replacing.filter(|replacing| replacing.object == object) else {
                return;
            };
            entry.state.replacing = None;
            // nothing written is nothing to reclaim, and its id may be put again
            if written {
                entry.state.retire(registered);
            }
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
        // the object it replaces is retired, with any put in flight that is not this one, each
        // with how far its stripes reach
        let mut state = ObjectState {
            live: true,
            replacing: None,
            retired: current.map(|entry| entry.state.retired.clone()).unwrap_or_default(),
        };
        if let Some(entry) = current {
            if entry.state.live && entry.object != 0 && entry.object != object {
                state.retire(entry.as_retired());
            }
            if let Some(replacing) = entry.state.replacing.filter(|replacing| replacing.object != object) {
                state.retire(replacing);
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
            state,
            created_ms: now,
            modified_ms: now,
            attributes: self.op.attributes.clone().unwrap_or_default(),
            // an object held inline has no stripes to reach
            extent: if inline.is_some() { 0 } else { size },
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

/// What marking a put's stripe came to
enum Marked {
    /// The row marks every position it had to
    Done,
    /// Its placement group moved since the marks were judged: they are judged again
    Moved,
}

/// What registering a put found
enum Registered {
    /// The put is in flight and its chunks may be written
    InFlight,
    /// A retry of a put that committed: this is its entry
    Done(ObjectEntry),
    /// A retry of a put that was given up or replaced: its id is retired, never registered again
    Retired,
}
