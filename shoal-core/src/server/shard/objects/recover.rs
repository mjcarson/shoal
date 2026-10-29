//! Recovery: a stale position made current again, and a group whose record overflowed walked
//!
//! Every shard runs one task that reads the missed record of each stripe group it leads and works
//! through it ([S10](../../../../../docs/src/object-storage/recovery.md),
//! [F88](../../../../../docs/src/features/recovery-and-moves.md)):
//!
//! 1. a stale position's row is read at `Quorum`, and its placement group's state linearizably;
//!    a group that is moving is left until it switches or aborts;
//! 2. its holder is asked which label it holds. One at the row's label already is only committed
//!    current; otherwise the bytes are read from a holder the row calls current and written to it:
//!    the units of every write the record says it missed since the label it holds, when the record
//!    knows them and they are fewer than half the chunk, as a stage over that label and its apply,
//!    and otherwise the whole chunk, in place at the row's label;
//! 3. the row is committed with the position current, on the sequence read and claiming the epoch
//!    read. A refusal leaves the position marked, and the next poll tries again.
//!
//! Every request a rebuild makes of a slice is a background one, served only while the slice has
//! no foreground request in flight and drawn from its device's recovery ceiling. A position whose
//! record overflowed is walked instead: every row of its group that marks it is rebuilt the same
//! way, and once every one was, the group commits that the record is whole again, if nothing was
//! dropped while it walked.
//!
//! # Invariants
//!
//! **A rebuild is never staged and discarded.** A whole chunk is written in place at a committed
//! label, never over a newer one; a stage of units is applied at once and never discarded
//! (X1's `rebuild_drops_its_stage_when_refused`).
//!
//! **Units are rebuilt only over a label at or past where the record's entry began**: a holder
//! below it may have missed a write no range names (X1's rule on `since`).
//!
//! **A corrupt chunk is never a source** (P15, [F89](../../../../../docs/src/features/scrub-and-repair.md)).
//! A source whose unit fails its checksum answers so and sets its chunk aside; the rebuild marks its
//! position missed in the same commit that makes the target current, and tries the next. A stripe
//! no source verifies for is lost: reported by name, counted, and nothing written.
//!
//! **A commit is made over the row as it is now.** The row is held by this shard from the read to
//! the commit and read again before it, so no mark a scrub or a read made since is committed over.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tracing::{event, Level};

use super::place::Placement;
use super::{Meta, MetaClient};
use crate::server::database::ShoalDatabase;
use crate::server::map::{MapCell, TabletMap};
use crate::server::messages::ServerMsg;
use crate::server::objects::put::PIECE_BYTES;
use crate::server::objects::{Holder, ObjectStore};
use crate::server::shard::walk::{TabletWalk, WalkAsk};
use crate::server::shard::RecoveryWork;
use crate::shared::checksum::crc64;
use crate::shared::identity::{BucketId, TableId};
use crate::shared::objects::{Label, StripeAnswer};
use crate::shared::placement_state::{Missed, PgKey, PlacementCommand, StripeKey};
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece, StoreCode, UnitBytes};
use crate::shared::stripes::{ChunkId, StripeRow};
use crate::shared::traits::QuerySupport;

/// What one rebuild came to
#[derive(Debug, PartialEq, Eq)]
enum Rebuilt {
    /// The position is current, committed by this rebuild
    Done,
    /// Nothing was to be done: the position is current already, or the stripe has no row
    Current,
    /// Nothing could be done now: its holder is not serving, its group moves, or its row moved
    /// under the commit, so a later poll tries again
    Later,
    /// Its holder holds a label the row does not name and not an older one, which read again
    /// is a row behind its chunks: nothing is rebuilt over them, kept for an operator's
    /// `ResolveStripe` (F93)
    Ahead,
}

/// Why a copy from a source did not finish
#[derive(Debug)]
pub(super) enum CopyError {
    /// The source's unit failed its checksum, and its holder set the chunk aside (F89)
    SourceCorrupt(String),
    /// The target refused the units as a request no slice serves: the chunk it holds is in a
    /// file of a smaller class than the row's length needs, so the copy is made whole (F96)
    Outgrown(String),
    /// Anything else, and why
    Failed(String),
}

impl std::fmt::Display for CopyError {
    /// Write why
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyError::SourceCorrupt(why) => write!(f, "the source is corrupt: {why}"),
            CopyError::Outgrown(why) => write!(f, "the target's chunk is past its file: {why}"),
            CopyError::Failed(why) => f.write_str(why),
        }
    }
}

/// Whether a holder can take a rebuild or a scrub's request now: its device serves, neither failed
/// nor one its node refuses to serve (item 333), and its node is up
///
/// # Arguments
///
/// * `installed` - The map
/// * `holder` - The holder
pub(super) fn holder_serving(installed: &TabletMap, holder: Holder) -> bool {
    let failed = installed.pool_map.devices.values().any(|device| {
        device.serves_nothing()
            && device
                .eras
                .iter()
                .any(|era| era.slices.contains(&holder.slice))
    });
    let up = installed.members.is_empty() || installed.is_up(holder.node);
    !failed && up
}

/// What every rebuild of one shard's task holds
struct Recovery<D: ShoalDatabase> {
    /// The shard's metadata client
    client: MetaClient,
    /// The shard's own channel
    shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    /// The shard's map
    map: MapCell,
    /// The node's object store
    store: ObjectStore,
    /// How long one metadata query may take
    wait: Duration,
    /// Each stripe found lost, by the sequence of its row then and when: tried again once its row
    /// moves, or [`LOST_RETRY`] later, when a holder that was away may have come back (F89)
    lost: RefCell<std::collections::HashMap<(u64, u128, u64), (u64, std::time::Instant)>>,
}

/// How long a stripe found lost waits before a rebuild tries it again on an unchanged row
const LOST_RETRY: Duration = Duration::from_secs(600);

impl<D: ShoalDatabase> Recovery<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// A bucket's queries and its pool's placement, for the consumer it is bound as
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `consumer` - The consumer
    fn bucket(&self, installed: &Arc<TabletMap>, consumer: u64) -> Result<(Meta<D>, Placement), String> {
        let (name, binding) = installed
            .objects
            .bindings
            .iter()
            .find(|(_, binding)| binding.consumer == consumer)
            .ok_or_else(|| format!("no bucket is bound as consumer {consumer}"))?;
        let ops = <D::ClientType as QuerySupport>::object_queries(BucketId::of(name))
            .ok_or_else(|| format!("this schema serves no bucket {name}"))?;
        let pool = installed
            .objects
            .pools
            .get(&binding.pool)
            .ok_or_else(|| format!("bucket {name}'s pool {} is not committed", binding.pool))?;
        let placement = Placement::new(installed.clone(), &binding.pool, pool, consumer, false)
            .map_err(|(_, why)| why)?;
        let meta = Meta {
            ops,
            client: self.client.clone(),
            shard_tx: self.shard_tx.clone(),
            span: tracing::info_span!("Object::recover", consumer),
            wait: self.wait,
        };
        Ok((meta, placement))
    }

    /// Whether a holder can take a rebuild now: its device has not failed and its node is up
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `holder` - The holder
    fn serving(installed: &TabletMap, holder: Holder) -> bool {
        holder_serving(installed, holder)
    }

    /// Make one stripe's stale position current
    ///
    /// # Arguments
    ///
    /// * `key` - The stripe's placement group
    /// * `position` - The position
    /// * `stripe` - The stripe
    /// * `missed` - What the record says it missed
    async fn rebuild(&self, key: PgKey, position: usize, stripe: StripeKey, missed: &Missed) -> Result<Rebuilt, String> {
        let installed = self.map.get();
        let (consumer, _) = key;
        let (object, index) = stripe;
        let (meta, placement) = self.bucket(&installed, consumer)?;
        // the row held by this shard until the commit, so no mark a scrub or a read of it makes
        // meanwhile is committed over (F89)
        let _held = self.client.hold_row((consumer, object, index)).await;
        // the row, strongly; a position it calls current needs nothing
        let rows = meta
            .stripe_rows(consumer, object, &[index])
            .await
            .map_err(|(code, why)| format!("{code}: {why}"))?;
        let Some(row) = rows.get(&index).cloned() else {
            return Ok(Rebuilt::Current);
        };
        if row.tombstone || row.lost || row.current(position) {
            return Ok(Rebuilt::Current);
        }
        // a stripe found lost is tried again once its row moves, or a while later
        if self
            .lost
            .borrow()
            .get(&(consumer, object, index))
            .is_some_and(|(sequence, at)| *sequence == row.sequence && at.elapsed() < LOST_RETRY)
        {
            return Ok(Rebuilt::Later);
        }
        // the group's state as of now; one that is moving is left to its move
        let view = meta
            .read_placements(&[key])
            .await
            .map_err(|(code, why)| format!("{code}: {why}"))?
            .remove(&key)
            .unwrap_or_default();
        if view.moving.is_some() {
            return Ok(Rebuilt::Later);
        }
        let set = placement.stripe_set(object, index, &view)?;
        let Some((chunk, holder)) = set.current().get(position).copied() else {
            return Err(format!("stripe {index} has no position {position}"));
        };
        // a holder whose device failed, or whose node is down, is rebuilt once it serves again
        if !Self::serving(&installed, holder) {
            return Ok(Rebuilt::Later);
        }
        let target = row.named(position);
        // what the holder holds: the row's label already is only committed current
        let have = match self
            .store
            .ask_holder(holder, background(ChunkRequest::Confirm { chunk, labels: vec![target] }))
            .await
        {
            ChunkReply::Held { label: Some(label) } if label == target => {
                return self.commit(&meta, &row, position, &[], view.epoch).await;
            }
            ChunkReply::Held { label } => label,
            other => return Err(format!("the holder of position {position} answered {other:?}")),
        };
        // a holder past the row it was read under, the row read strongly and held: the row is
        // behind its chunks, and a rebuild over them would destroy a write its holders applied,
        // so they are kept for an operator's word (F93, item 256)
        if have.is_some_and(|have| have.ahead_of(target)) {
            event!(Level::WARN, msg = "a rebuild found a holder ahead of its stripe's row and left it for ResolveStripe", object = %format!("{object:032x}"), stripe = index, position);
            return Ok(Rebuilt::Ahead);
        }
        let geometry = placement.pool.geometry();
        // an erasure coded position is decoded whole from k chunks that verify, the model's rule:
        // its record is whole, since no other position holds its bytes to copy (F90)
        if geometry.is_erasure() {
            return self.rebuild_erasure(&meta, &row, &set, position, (chunk, holder), target, view.epoch).await;
        }
        // the units it lacks, where the record knows them and they are fewer than half the chunk
        let unit = u64::from(geometry.unit_bytes);
        let chunk_bytes = u64::from(geometry.stripe_bytes);
        let units = have
            .and_then(|have| missed.units_after(have).map(|run| (have, run)))
            .filter(|(_, (_, count))| u64::from(*count) * unit * 2 <= chunk_bytes);
        // from any position the row calls current, one on this node first
        let mut sources: Vec<usize> = (0..set.width)
            .filter(|source| *source != position && row.current(*source))
            .collect();
        sources.sort_by_key(|source| !self.store.is_local(&set.current()[*source].1));
        let mut why = Vec::new();
        // the sources that answered their chunk corrupt, which are marked and never used (P15)
        let mut corrupt: Vec<usize> = Vec::new();
        let tried = sources.len();
        for source in sources {
            let (from_chunk, from) = set.current()[source];
            let label = row.named(source);
            let copied = match units {
                Some((have, (first, count))) => {
                    match self
                        .copy_units(&row, (from_chunk, from, label), (chunk, holder), (have, first, count), missed, geometry.unit_bytes)
                        .await
                    {
                        // the target's file is of a smaller class than the row's length: whole (F96)
                        Err(CopyError::Outgrown(_)) => {
                            copy_whole(&self.store, row.length.min(chunk_bytes), (from_chunk, from, label), (chunk, holder), target, chunk_bytes, geometry.unit_bytes, row.length, row.owner)
                                .await
                        }
                        other => other,
                    }
                }
                None => {
                    copy_whole(&self.store, row.length.min(chunk_bytes), (from_chunk, from, label), (chunk, holder), target, chunk_bytes, geometry.unit_bytes, row.length, row.owner)
                        .await
                }
            };
            match copied {
                Ok(()) => {
                    // a test can hold a rebuild here, between its copy and its commit
                    if let Some(hold) = crate::server::objects::faults::rebuild_hold() {
                        glommio::timer::sleep(hold).await;
                    }
                    return self.commit(&meta, &row, position, &corrupt, view.epoch).await;
                }
                Err(CopyError::SourceCorrupt(error)) => {
                    corrupt.push(source);
                    why.push(format!("from position {source}: corrupt: {error}"));
                }
                Err(error @ (CopyError::Outgrown(_) | CopyError::Failed(_))) => {
                    why.push(format!("from position {source}: {error}"));
                }
            }
        }
        // the corrupt sources marked, so nothing reads them and each is rebuilt in turn
        if !corrupt.is_empty() {
            let length = row.length.min(chunk_bytes);
            let _ = meta
                .commit_marks(consumer, object, index, set.width, length, row.owner, &corrupt, view.epoch)
                .await;
            if let Some((pool, _)) = installed.objects.pool_of_consumer(consumer) {
                self.client
                    .count_scrub(pool, |figures| figures.corrupt += corrupt.len() as u64);
            }
        }
        // no current copy that verifies: the stripe is lost, said by name, and nothing written
        if corrupt.len() == tried {
            self.lost
                .borrow_mut()
                .insert((consumer, object, index), (row.sequence, std::time::Instant::now()));
            if let Some((pool, _)) = installed.objects.pool_of_consumer(consumer) {
                self.client.count_scrub(pool, |figures| figures.lost += 1);
            }
            event!(
                Level::ERROR,
                msg = "a stripe has no current copy that verifies; it is lost and nothing is rebuilt",
                object = %format!("{object:032x}"),
                stripe = index,
                position,
                sequence = row.sequence,
                corrupt = ?corrupt,
            );
        }
        Err(format!(
            "stripe {index} of object {object:032x} position {position}: no source gave its bytes: {}",
            why.join("; ")
        ))
    }

    /// Rebuild an erasure coded position by decoding its chunk from `k` others, and commit it
    /// current (F90)
    ///
    /// A position whose chunk holds no bytes is current with nothing written. Any other is decoded
    /// a piece at a time, each from `k` current chunks that verify over the same range, and written
    /// whole in place at the row's label; a source that fails its checksum is marked in the commit
    /// that makes the target current, and a stripe fewer than `k` of whose chunks verify is lost.
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `row` - The row read
    /// * `set` - The stripe's chunks and holders
    /// * `position` - The position
    /// * `to` - Its chunk and its holder
    /// * `target` - The label the row names for it
    /// * `epoch` - The placement group's epoch read
    #[allow(clippy::too_many_arguments)]
    async fn rebuild_erasure(
        &self,
        meta: &Meta<D>,
        row: &StripeRow,
        set: &super::place::StripeSet,
        position: usize,
        to: (ChunkId, Holder),
        target: Label,
        epoch: u64,
    ) -> Result<Rebuilt, String> {
        let installed = self.map.get();
        let (_, placement) = self.bucket(&installed, row.consumer)?;
        let geometry = placement.pool.geometry();
        // a position holding no bytes is current by holding none
        if geometry.chunk_length(row.length, position) == 0 {
            return self.commit(meta, row, position, &[], epoch).await;
        }
        match decode_whole(&self.store, set.current(), row, &geometry, position, to, target).await {
            Ok(corrupt) => {
                // a test can hold a rebuild here, between its copy and its commit
                if let Some(hold) = crate::server::objects::faults::rebuild_hold() {
                    glommio::timer::sleep(hold).await;
                }
                self.commit(meta, row, position, &corrupt, epoch).await
            }
            Err(failed) => {
                let (consumer, object, index) = (row.consumer, row.object, row.stripe);
                // the corrupt sources marked, so nothing reads them and each is rebuilt in turn
                if !failed.corrupt.is_empty() {
                    let _ = meta
                        .commit_marks(consumer, object, index, set.width, row.length, row.owner, &failed.corrupt, epoch)
                        .await;
                    if let Some((pool, _)) = installed.objects.pool_of_consumer(consumer) {
                        self.client
                            .count_scrub(pool, |figures| figures.corrupt += failed.corrupt.len() as u64);
                    }
                }
                // fewer than k current chunks that verify: the stripe is lost, by name
                let current = (0..set.width)
                    .filter(|source| *source != position && row.current(*source))
                    .count();
                if failed.lost && current.saturating_sub(failed.corrupt.len()) < usize::from(geometry.data) {
                    self.lost
                        .borrow_mut()
                        .insert((consumer, object, index), (row.sequence, std::time::Instant::now()));
                    if let Some((pool, _)) = installed.objects.pool_of_consumer(consumer) {
                        self.client.count_scrub(pool, |figures| figures.lost += 1);
                    }
                    event!(
                        Level::ERROR,
                        msg = "a stripe has fewer chunks that verify than its code needs; it is lost and nothing is rebuilt",
                        object = %format!("{object:032x}"),
                        stripe = index,
                        position,
                        sequence = row.sequence,
                        corrupt = ?failed.corrupt,
                    );
                }
                Err(format!(
                    "stripe {index} of object {object:032x} position {position}: {}",
                    failed.why
                ))
            }
        }
    }

    /// Commit a position current, and any sources found corrupt marked, over the row as it is now,
    /// claiming the epoch read
    ///
    /// The row is read again first: one that moved since the rebuild read it, at its sequence or
    /// at the position, is left for the next poll, and a mark made since is kept (F89).
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `row` - The row read
    /// * `position` - The position
    /// * `corrupt` - The sources found corrupt
    /// * `epoch` - The placement group's epoch read
    async fn commit(&self, meta: &Meta<D>, row: &StripeRow, position: usize, corrupt: &[usize], epoch: u64) -> Result<Rebuilt, String> {
        let rows = meta
            .stripe_rows(row.consumer, row.object, &[row.stripe])
            .await
            .map_err(|(code, why)| format!("{code}: {why}"))?;
        let Some(now) = rows.get(&row.stripe).cloned() else {
            return Ok(Rebuilt::Later);
        };
        if now.tombstone || now.sequence != row.sequence || now.named(position) != row.named(position) {
            return Ok(Rebuilt::Later);
        }
        // the position current, and each corrupt source marked whole, in one commit
        let next = if corrupt.is_empty() {
            now.rebuilt(position, true)
        } else {
            let mut next = now.marked(corrupt);
            next.labels[position] = now.named(position);
            next
        }
        .claiming(epoch);
        match meta.commit_stripe(next, Some(now.sequence), None).await {
            StripeAnswer::Written => {
                // counted under the pool the stripe's consumer is bound to
                if let Some((pool, _)) = self.map.get().objects.pool_of_consumer(row.consumer) {
                    self.client.count_recovery(pool, |figures| figures.rebuilt += 1);
                }
                Ok(Rebuilt::Done)
            }
            // the row or its group moved since: the next poll reads them again
            StripeAnswer::Refused(_) => Ok(Rebuilt::Later),
            StripeAnswer::Failed(code, why) => Err(format!("{code}: {why}")),
            StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                Err("a rebuild's commit answered as a read".to_string())
            }
        }
    }

    /// Write the units a holder lacks, over the label it holds, and apply them at once
    ///
    /// # Arguments
    ///
    /// * `row` - The row
    /// * `from` - The source's chunk, its holder and the label it holds
    /// * `to` - The chunk and the holder to write
    /// * `run` - The label the holder holds, and the first unit and how many to write
    /// * `missed` - What the record says it missed, for the labels the stage stands on
    /// * `unit_bytes` - The bytes of a unit
    async fn copy_units(
        &self,
        row: &StripeRow,
        from: (ChunkId, Holder, Label),
        to: (ChunkId, Holder),
        run: (Label, u32, u32),
        missed: &Missed,
        unit_bytes: u32,
    ) -> Result<(), CopyError> {
        let (have, first, count) = run;
        let unit = u64::from(unit_bytes);
        let target = row.named(usize::from(to.0.position));
        // the run's bytes, the part past the chunk's length zeros
        let offset = u64::from(first) * unit;
        let wanted = u64::from(count) * unit;
        let len = wanted.min(row.length.saturating_sub(offset));
        let mut bytes = if len > 0 {
            read_from(&self.store, from, offset, len).await?
        } else {
            Vec::new()
        };
        bytes.resize(wanted as usize, 0);
        let units: Vec<UnitBytes> = bytes
            .chunks(unit as usize)
            .enumerate()
            .map(|(at, value)| UnitBytes {
                index: first + at as u32,
                crc: crc64(value),
                bytes: bytes::Bytes::copy_from_slice(value),
            })
            .collect();
        // over the label it holds, or any write the record names between it and the row's
        let mut expects = vec![have];
        expects.extend(missed.labels_between(have, target));
        let stage = ChunkRequest::StageUnits {
            chunk: to.0,
            makes: target,
            expects,
            length: row.length,
            unit_bytes,
            units,
        };
        match self.store.ask_holder(to.1, background(stage)).await {
            ChunkReply::Done => {}
            // a chunk is never extended in place, so units past its file are copied whole (F96)
            ChunkReply::Refused {
                code: StoreCode::Invalid,
                message,
            } => return Err(CopyError::Outgrown(message)),
            other => return Err(CopyError::Failed(format!("the holder refused the units: {other:?}"))),
        }
        // and applied at once: a rebuild's stage is never left to a decide, nor discarded
        match self
            .store
            .ask_holder(to.1, background(ChunkRequest::Apply { chunk: to.0, label: target }))
            .await
        {
            ChunkReply::Done => Ok(()),
            other => Err(CopyError::Failed(format!("the holder did not apply the units: {other:?}"))),
        }
    }

    /// Walk a position's placement group and rebuild every row that marks it, then say the record
    /// is whole again
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `position` - The position
    /// * `drops` - The drops its record had counted when the work was read
    async fn backfill(&self, table: TableId, key: PgKey, position: usize, drops: u64) -> Result<Rebuilt, String> {
        let installed = self.map.get();
        let (consumer, pg) = key;
        let (_, placement) = self.bucket(&installed, consumer)?;
        let range = pg.range(placement.pool.groups_per_tablet());
        let mut walk = TabletWalk::<D>::new(self.shard_tx.clone(), WalkAsk::range(table, pg.tablet, range));
        let mut whole = true;
        while let Some(page) = walk.next().await.map_err(|refusal| format!("{refusal:?}"))? {
            let rows = page.stripes::<D>().map_err(|error| format!("{error:?}"))?;
            for (_, row) in rows {
                // only this consumer's rows, and only those that mark the position
                if row.consumer != consumer || row.tombstone || row.current(position) {
                    continue;
                }
                // whole only if every stripe it found stale is current now, never one put off
                match self.rebuild(key, position, (row.object, row.stripe), &Missed::Whole).await {
                    Ok(Rebuilt::Done | Rebuilt::Current) => {}
                    // a stripe ahead of its row is left for an operator's word, never done
                    Ok(Rebuilt::Later | Rebuilt::Ahead) => whole = false,
                    Err(error) => {
                        event!(Level::DEBUG, msg = "a backfill could not rebuild a stripe", error);
                        whole = false;
                    }
                }
            }
        }
        if !whole {
            return Ok(Rebuilt::Later);
        }
        // the record holds every stale stripe again, if nothing was dropped while it walked
        let (reply, answer) = futures_channel::oneshot::channel();
        let command = PlacementCommand::BackfillDone {
            consumer,
            pg,
            position: u8::try_from(position).unwrap_or(u8::MAX),
            drops,
        };
        self.shard_tx
            .send(ServerMsg::PlacementPropose { table, command, reply })
            .await
            .map_err(|_| "the shard stopped".to_string())?;
        match answer.await {
            Ok(Ok(())) => {
                if let Some((pool, _)) = installed.objects.pool_of_consumer(consumer) {
                    self.client.count_recovery(pool, |figures| figures.backfilled += 1);
                }
                Ok(Rebuilt::Done)
            }
            Ok(Err(why)) => Err(why),
            Err(_) => Err("the backfill's commit was dropped".to_string()),
        }
    }
}

/// Write a whole chunk to a holder in place at a label, read from a source a piece at a time
///
/// The pieces go one at a time, so the destination's stream writes them in order; a destination
/// already past the label refuses the chunk, and keeps what it has.
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `length` - How much of the chunk holds bytes
/// * `from` - The source's chunk, its holder and the label it holds
/// * `to` - The chunk and the holder to write
/// * `label` - The label the chunk is written at
/// * `chunk_bytes` - The bytes of a whole chunk
/// * `unit_bytes` - The bytes of a unit
/// * `stripe_length` - How much of the chunk's stripe holds bytes, which its header records (F90)
/// * `owner` - The key the bucket records the object under, which its header records (M20)
#[allow(clippy::too_many_arguments)]
pub(super) async fn copy_whole(
    store: &ObjectStore,
    length: u64,
    from: (ChunkId, Holder, Label),
    to: (ChunkId, Holder),
    label: Label,
    chunk_bytes: u64,
    unit_bytes: u32,
    stripe_length: u64,
    owner: u64,
) -> Result<(), CopyError> {
    let unit = u64::from(unit_bytes);
    // whole units a piece, at most a frame's worth, one at a time so the stream keeps order
    let per = ((PIECE_BYTES / unit).max(1) * unit).max(unit);
    let stream: u64 = rand::random();
    let mut offset = 0u64;
    loop {
        let len = per.min(length - offset);
        let last = offset + len >= length;
        let mut bytes = read_from(store, from, offset, len).await?;
        // the last unit padded with zeros to a whole one
        bytes.resize(bytes.len().div_ceil(unit as usize) * unit as usize, 0);
        let crcs: Vec<u64> = bytes.chunks(unit as usize).map(crc64).collect();
        let piece = Piece {
            stream,
            chunk: to.0,
            label,
            chunk_bytes,
            unit_bytes,
            length,
            offset,
            last,
            stripe_length,
            owner,
            crcs,
            bytes: bytes.into(),
        };
        match store.ask_holder(to.1, background(ChunkRequest::Write(piece))).await {
            ChunkReply::Done => {}
            other => return Err(CopyError::Failed(format!("the holder refused a piece at {offset}: {other:?}"))),
        }
        if last {
            return Ok(());
        }
        offset += len;
    }
}

/// Why a decode of a whole chunk did not finish
#[derive(Debug)]
pub(super) struct DecodeFailed {
    /// The sources found corrupt on the way
    pub corrupt: Vec<usize>,
    /// Whether it failed for want of `k` sources, rather than at its destination
    pub lost: bool,
    /// Why
    pub why: String,
}

/// Decode a position's whole chunk from `k` others and write it in place at a label, a piece at a
/// time (F90)
///
/// Each piece is decoded on an object executor from `k` current chunks over the same range, each
/// at the label the row names and verified as it is read, and written to the destination's stream
/// in order. Returns the sources a piece found corrupt, which the commit marks.
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holders` - Each position's chunk and holder
/// * `row` - The stripe's row
/// * `geometry` - Its object's geometry
/// * `position` - The position to make
/// * `to` - The chunk and the holder to write
/// * `label` - The label it is written at
pub(super) async fn decode_whole(
    store: &ObjectStore,
    holders: &[(ChunkId, Holder)],
    row: &StripeRow,
    geometry: &crate::shared::objects::Geometry,
    position: usize,
    to: (ChunkId, Holder),
    label: Label,
) -> Result<Vec<usize>, DecodeFailed> {
    let unit = u64::from(geometry.unit_bytes.max(1));
    let length = geometry.chunk_length(row.length, position);
    let padded = length.div_ceil(unit) * unit;
    // whole units a piece, at most a frame's worth, one at a time so the stream keeps order
    let per = (PIECE_BYTES / unit).max(1) * unit;
    let stream: u64 = rand::random();
    let mut corrupt: Vec<usize> = Vec::new();
    let mut offset = 0u64;
    while offset < padded {
        let len = per.min(padded - offset);
        let last = offset + len >= padded;
        let mut gathered = match super::erasure::gather(store, holders, row, geometry, &[position], offset, len, true).await {
            Ok(gathered) => gathered,
            Err(super::read::StripeRead::Ahead) => {
                return Err(DecodeFailed {
                    corrupt,
                    lost: false,
                    why: "a source holds a label the row does not name and not an older one: \
                          the row moved, or is behind its chunks"
                        .to_string(),
                })
            }
            Err(super::read::StripeRead::Failed(why)) => {
                return Err(DecodeFailed { corrupt, lost: true, why })
            }
        };
        for found in gathered.corrupt.drain(..) {
            if !corrupt.contains(&found) {
                corrupt.push(found);
            }
        }
        let bytes = gathered.bytes.remove(&position).unwrap_or_default().into_vec();
        let crcs: Vec<u64> = bytes.chunks(unit as usize).map(crc64).collect();
        let piece = Piece {
            stream,
            chunk: to.0,
            label,
            chunk_bytes: geometry.chunk_bytes(),
            unit_bytes: geometry.unit_bytes,
            length,
            offset,
            last,
            stripe_length: row.length,
            owner: row.owner,
            crcs,
            bytes: bytes.into(),
        };
        match store.ask_holder(to.1, background(ChunkRequest::Write(piece))).await {
            ChunkReply::Done => {}
            other => {
                return Err(DecodeFailed {
                    corrupt,
                    lost: false,
                    why: format!("the holder refused a piece at {offset}: {other:?}"),
                })
            }
        }
        offset += len;
    }
    Ok(corrupt)
}

/// A range of a source's chunk at the label it holds
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `from` - The source's chunk, its holder and the label
/// * `offset` - Where the range starts
/// * `len` - How long it is
pub(super) async fn read_from(
    store: &ObjectStore,
    from: (ChunkId, Holder, Label),
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, CopyError> {
    let (chunk, holder, label) = from;
    if len == 0 {
        return Ok(Vec::new());
    }
    let read = ChunkRequest::Read {
        chunk,
        offset,
        len,
        labels: vec![label],
    };
    match store.ask_holder(holder, background(read)).await {
        ChunkReply::Bytes { label: read, bytes } if read == label => Ok(bytes.into_vec()),
        // a unit that fails its checksum: the source is never used (P15)
        ChunkReply::Refused {
            code: StoreCode::Corrupt,
            message,
        } => Err(CopyError::SourceCorrupt(message)),
        other => Err(CopyError::Failed(format!("the source answered {other:?}"))),
    }
}

/// A request made a background one: served only while its slice is idle, under its ceiling
///
/// # Arguments
///
/// * `request` - The request
pub(super) fn background(request: ChunkRequest) -> ChunkRequest {
    ChunkRequest::Background(Box::new(request))
}

/// Read the missed records of the groups this shard leads and rebuild what they name, for as long
/// as the shard runs
///
/// # Arguments
///
/// * `client` - The shard's metadata client
/// * `shard_tx` - The shard's own channel
/// * `map` - The shard's map
/// * `store` - The node's object store, once started
/// * `every` - How often the records are read
/// * `inflight` - How many rebuilds run at once
/// * `wait` - How long one metadata query may take
pub(crate) async fn poll<D: ShoalDatabase>(
    client: MetaClient,
    shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    map: MapCell,
    store: Arc<OnceLock<ObjectStore>>,
    every: Duration,
    inflight: usize,
    wait: Duration,
) where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    // the stripes being rebuilt and the groups being walked, so a poll starts none twice
    let busy: Rc<RefCell<HashSet<(PgKey, usize, StripeKey)>>> = Rc::new(RefCell::new(HashSet::new()));
    let running = Rc::new(Cell::new(0usize));
    let mut recovery: Option<Rc<Recovery<D>>> = None;
    loop {
        glommio::timer::sleep(every).await;
        // a test may pause the drivers to do their work by hand
        if crate::server::objects::faults::recovery_paused() {
            continue;
        }
        // the store starts once the shards answer
        let recovery = match &recovery {
            Some(recovery) => recovery.clone(),
            None => {
                let Some(store) = store.get().cloned() else {
                    continue;
                };
                let made = Rc::new(Recovery {
                    client: client.clone(),
                    shard_tx: shard_tx.clone(),
                    map: map.clone(),
                    store,
                    wait,
                    lost: RefCell::new(std::collections::HashMap::new()),
                });
                recovery = Some(made.clone());
                made
            }
        };
        // the work the records of the groups this shard leads hold
        let (reply, answer) = futures_channel::oneshot::channel();
        if shard_tx.send(ServerMsg::RecoveryWork { reply }).await.is_err() {
            return;
        }
        let Ok(work) = answer.await else {
            continue;
        };
        for item in work {
            if running.get() >= inflight.max(1) {
                break;
            }
            // a backfill is known by its group and position, and a rebuild by its stripe too
            let busy_key = match &item {
                RecoveryWork::Rebuild { key, position, stripe, .. } => (*key, *position, *stripe),
                RecoveryWork::Backfill { key, position, .. } => (*key, *position, (u128::MAX, u64::MAX)),
            };
            if !busy.borrow_mut().insert(busy_key) {
                continue;
            }
            running.set(running.get() + 1);
            let recovery = recovery.clone();
            let busy = busy.clone();
            let running = running.clone();
            glommio::spawn_local(async move {
                let outcome = match item {
                    RecoveryWork::Rebuild { key, position, stripe, missed } => {
                        recovery.rebuild(key, position, stripe, &missed).await
                    }
                    RecoveryWork::Backfill { table, key, position, drops } => {
                        recovery.backfill(table, key, position, drops).await
                    }
                };
                match outcome {
                    Ok(Rebuilt::Done) => {
                        event!(Level::DEBUG, msg = "rebuilt a stale position", key = ?busy_key)
                    }
                    Ok(Rebuilt::Current | Rebuilt::Later | Rebuilt::Ahead) => {}
                    Err(error) => {
                        event!(Level::DEBUG, msg = "a rebuild did not finish; the next poll tries again", key = ?busy_key, error)
                    }
                }
                busy.borrow_mut().remove(&busy_key);
                running.set(running.get() - 1);
            })
            .detach();
        }
    }
}
