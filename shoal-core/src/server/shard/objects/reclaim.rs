//! Reclamation: what nothing refers to any more, removed on the committed fact that says so
//!
//! Every shard runs one task that walks the `ObjectMeta` entries of the object groups it leads, or
//! on a standalone node the tablets it hosts, every `reclaim_poll`
//! ([S10](../../../../../docs/src/object-storage/recovery.md#reclamation),
//! [F92](../../../../../docs/src/features/reclamation-and-device-lifecycle.md)):
//!
//! 1. **a retired object** - replaced, deleted or given up - once this driver has seen it retired
//!    for `reclaim_grace` by its own clock: every holder of every stripe its extent reaches is
//!    sealed, the entry is read strongly to see it still retired or named nowhere, every holder
//!    removes the object's chunks under its seal, each stripe's row is tombstoned so the missed
//!    record forgets it and then deleted, and the id leaves the entry;
//! 2. **a floor** - the oldest of a live object's - by X1's `Reclaim`: every stripe wholly past its
//!    cut and stamped below it is tombstoned a sequence past what was read and its holders told to
//!    discard through that sequence, and the floor goes once the stripe its cut falls inside has
//!    been written past it, or the cut is a stripe's edge;
//! 3. **a deleted entry** that names nothing retired or in flight is removed, and its row with it
//!    when it was the last;
//! 4. **a stray group** - on one shard of a node, every slice it serves swept of each placement
//!    group whose committed state neither places on the slice nor names it left: a late piece of a
//!    rebuild, a move or a put that reached the slice after the group's release there and the
//!    release's window, which nothing else would ever ask about (item 253).
//!
//! # Invariants
//!
//! **A holder removes nothing a committed fact does not free** (P16). A retired object's chunks
//! go only under a seal set before the strong read that found it retired, so a put registered
//! again under its id after that read, or a reclaimer's word that arrives late, removes nothing; a
//! floor's stripes go only through the sequence a committed tombstone moved past.
//!
//! **The grace is the driver's own clock, and never a permission.** It only puts a removal off
//! for readers that began before the retirement; the committed retirement is what allows it, and a
//! new leader starts its grace again.
//!
//! **A holder that cannot be reached is not waited for.** What it holds of a reclaimed object is
//! found by a light scrub once it serves again, whose consumer names the object nowhere.
//!
//! **A stray group is removed only under a seal set before the read that found it stray**, and
//! never while it moves: a move, a rebuild or a put that begins after that read and would place
//! there waits on the seal and finds nothing of what was removed, and a group placed there again
//! before the read is left alone.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tracing::{event, Level};

use super::place::Placement;
use super::recover::holder_serving;
use super::{Meta, MetaClient, DEFAULT_LEVEL, MAX_TRIES};
use crate::server::database::ShoalDatabase;
use crate::server::map::{MapCell, TabletMap};
use crate::server::messages::ServerMsg;
use crate::server::objects::{Holder, ObjectStore};
use crate::server::shard::walk::{TabletWalk, WalkAsk};
use crate::server::shard::ReclaimTarget;
use crate::shared::devices::SliceId;
use crate::shared::identity::BucketId;
use crate::shared::objects::{Floor, MetaAnswer, ObjectEntry, ObjectRow, Retired, StripeAnswer};
use crate::shared::placement_state::{PgKey, PgView};
use crate::shared::poolmap::{PlacementGroup, SliceRef};
use crate::shared::pools::PoolSpec;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, SealScope};
use crate::shared::protocol::RECLAIM_WIRE_VERSION;
use crate::shared::stripes::StripeRow;
use crate::shared::traits::QuerySupport;

/// How many placement groups of one object are sealed, read and reclaimed in one round
const GROUPS_A_ROUND: usize = 32;

/// How many stripe rows one read of a retired object's rows asks for
const ROWS_A_READ: usize = 64;

/// How often a driver looks again whether a test asked for a pass, between its passes
const ASK_POLL: Duration = Duration::from_millis(100);

/// How many placement groups of one slice a sweep reads the states of in one pass
///
/// A drained device's slices hold every group not yet moved off, each a candidate until it moves,
/// so a pass reads a bounded number and leaves the rest to the next; a stray group is read once the
/// groups still moving have gone.
const SWEEP_READS: usize = 256;

/// One bucket a pass reclaims in: its queries, its pool, and its placement where it has one
struct Bucket<D: ShoalDatabase> {
    /// How its metadata is read and committed
    meta: Meta<D>,
    /// The consumer it is bound as
    consumer: u64,
    /// Its pool, by name
    pool_name: String,
    /// Its pool
    pool: PoolSpec,
    /// Its pool's placement, none for a pool not yet placed, whose objects are all inline
    placement: Option<Placement>,
}

/// What every pass of one shard's reclamation holds
struct Reclaimer<D: ShoalDatabase> {
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
    /// How long a retired object is kept for its readers
    grace: Duration,
    /// When this driver first saw each retired object, by its row's key and its id
    first_seen: RefCell<HashMap<(u64, u128), Instant>>,
    /// Whether this shard sweeps its node's slices of stray groups, which one shard a node does
    sweeps: bool,
    /// When this driver first found each group on a local slice its pool's target does not place
    /// there, by the slice and the group
    strays: RefCell<HashMap<(SliceId, PgKey), Instant>>,
}

/// Run one shard's reclamation, a pass every `every`, until the shard stops
///
/// # Arguments
///
/// * `client` - The shard's metadata client
/// * `shard_tx` - The shard's own channel
/// * `map` - The shard's map
/// * `store` - The node's object store, once it starts
/// * `sweeps` - Whether this shard sweeps its node's slices of stray groups
/// * `every` - How often a pass runs
/// * `grace` - How long a retired object is kept for its readers
/// * `wait` - How long one metadata query may take
#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll<D: ShoalDatabase>(
    client: MetaClient,
    shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    map: MapCell,
    store: Arc<OnceLock<ObjectStore>>,
    sweeps: bool,
    every: Duration,
    grace: Duration,
    wait: Duration,
) where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    let mut reclaimer: Option<Rc<Reclaimer<D>>> = None;
    let asked = Cell::new(crate::server::objects::faults::reclaim_asked());
    loop {
        // a pass every interval, or at once when a test asks for one
        let started = Instant::now();
        while started.elapsed() < every && crate::server::objects::faults::reclaim_asked() == asked.get() {
            glommio::timer::sleep(ASK_POLL.min(every)).await;
        }
        let now_asked = crate::server::objects::faults::reclaim_asked();
        let forced = now_asked != asked.get();
        asked.set(now_asked);
        // a test may pause the drivers to look at what is left first
        if crate::server::objects::faults::reclaim_paused() {
            continue;
        }
        // nothing is reclaimed before the cluster speaks the reclamation wire
        let installed = map.get();
        if !installed.members.is_empty() && installed.activated_wire < RECLAIM_WIRE_VERSION {
            continue;
        }
        // the store starts once the shards answer
        let reclaimer = match &reclaimer {
            Some(reclaimer) => reclaimer.clone(),
            None => {
                let Some(store) = store.get().cloned() else {
                    continue;
                };
                let made = Rc::new(Reclaimer {
                    client: client.clone(),
                    shard_tx: shard_tx.clone(),
                    map: map.clone(),
                    store,
                    wait,
                    grace,
                    first_seen: RefCell::new(HashMap::new()),
                    sweeps,
                    strays: RefCell::new(HashMap::new()),
                });
                reclaimer = Some(made.clone());
                made
            }
        };
        reclaimer.pass(forced).await;
    }
}

impl<D: ShoalDatabase> Reclaimer<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// One pass over every object tablet this shard leads or hosts
    ///
    /// # Arguments
    ///
    /// * `forced` - Whether a test asked for it, which takes every retired object past its grace
    async fn pass(&self, forced: bool) {
        let started = Instant::now();
        let (reply, answer) = futures_channel::oneshot::channel();
        if self.shard_tx.send(ServerMsg::ReclaimTargets { reply }).await.is_err() {
            return;
        }
        let Ok(targets) = answer.await else {
            return;
        };
        let installed = self.map.get();
        let mut seen: HashSet<(u64, u128)> = HashSet::new();
        let mut pending: BTreeMap<String, u64> = BTreeMap::new();
        // the live objects and bytes of every bucket the walked groups hold, for its figures (F93)
        let mut held: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for target in targets {
            let Some(bucket) = self.bucket(&installed, &target) else {
                continue;
            };
            let holding = held.entry(target.bucket.clone()).or_default();
            for tablet in &target.tablets {
                self.walk_tablet(&bucket, &target, *tablet, forced, &mut seen, &mut pending, holding)
                    .await;
            }
        }
        self.client.set_holdings(held);
        // a retired object no longer seen was reclaimed, or reclaimed by another leader
        self.first_seen.borrow_mut().retain(|key, _| seen.contains(key));
        for (pool, pending) in pending {
            self.client.set_reclaim_pending(&pool, pending);
        }
        // and one shard a node sweeps the node's slices of what their groups left there late
        let walked = started.elapsed();
        if self.sweeps {
            self.sweep(forced).await;
        }
        event!(
            Level::DEBUG,
            msg = "a reclamation pass ended",
            walked_ms = walked.as_millis() as u64,
            ms = started.elapsed().as_millis() as u64,
            swept = self.sweeps,
        );
    }

    /// What a pass needs of a bucket: its queries, its pool and its placement
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `target` - The bucket's object table and tablets
    fn bucket(&self, installed: &Arc<TabletMap>, target: &ReclaimTarget) -> Option<Bucket<D>> {
        self.bucket_named(installed, &target.bucket)
    }

    /// What a pass needs of a bucket named: its queries, its pool and its placement
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `name` - The bucket's name
    fn bucket_named(&self, installed: &Arc<TabletMap>, name: &str) -> Option<Bucket<D>> {
        let binding = installed.objects.bindings.get(name)?;
        let ops = <D::ClientType as QuerySupport>::object_queries(BucketId::of(name))?;
        let pool = installed.objects.pools.get(&binding.pool)?.clone();
        // a pool that was never placed holds every object inline, which has no stripes
        let placement = Placement::new(installed.clone(), &binding.pool, &pool, binding.consumer, false).ok();
        Some(Bucket {
            meta: Meta {
                ops,
                client: self.client.clone(),
                shard_tx: self.shard_tx.clone(),
                span: tracing::info_span!("Object::reclaim", bucket = %name),
                wait: self.wait,
            },
            consumer: binding.consumer,
            pool_name: binding.pool.clone(),
            pool,
            placement,
        })
    }

    /// Walk one tablet of a bucket's object table, reclaiming what each entry frees
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `target` - Its object table
    /// * `tablet` - The tablet
    /// * `forced` - Whether every retired object is taken past its grace
    /// * `seen` - Every retired object the pass saw, filled in
    /// * `pending` - The retired objects the pass left, by pool, filled in
    /// * `held` - The bucket's live objects and their bytes, counted up (F93)
    #[allow(clippy::too_many_arguments)]
    async fn walk_tablet(
        &self,
        bucket: &Bucket<D>,
        target: &ReclaimTarget,
        tablet: u16,
        forced: bool,
        seen: &mut HashSet<(u64, u128)>,
        pending: &mut BTreeMap<String, u64>,
        held: &mut (u64, u64),
    ) {
        let mut walk = TabletWalk::<D>::new(self.shard_tx.clone(), WalkAsk::tablet(target.table, tablet));
        loop {
            // a shed page is asked again by the walk itself (Resolved #345)
            let page = match walk.next().await {
                Ok(Some(page)) => page,
                Ok(None) => return,
                Err(refusal) => {
                    event!(Level::DEBUG, msg = "a reclamation's walk was refused; the next pass walks it again", tablet, why = %refusal.msg);
                    return;
                }
            };
            let rows = match page.objects::<D>() {
                Ok(rows) => rows,
                Err(error) => {
                    event!(Level::WARN, msg = "a reclamation's page did not read", tablet, error = ?error);
                    return;
                }
            };
            for (_, row) in rows {
                for entry in &row.entries {
                    // the bucket's live objects, counted once on the leader of their group (F93)
                    if entry.state.live {
                        held.0 += 1;
                        held.1 += entry.size;
                    }
                    let waiting = self.reclaim_entry(bucket, row.key, entry, forced, seen).await;
                    *pending.entry(bucket.pool_name.clone()).or_default() += waiting;
                }
                glommio::yield_if_needed().await;
            }
        }
    }

    /// Reclaim what one entry frees: its retired objects past their grace, its oldest floor, and
    /// the entry itself once it is deleted and names nothing
    ///
    /// Answers how many retired objects it left for a later pass.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The entry's row's key
    /// * `entry` - The entry, as the walk read it
    /// * `forced` - Whether every retired object is taken past its grace
    /// * `seen` - Every retired object the pass saw, filled in
    async fn reclaim_entry(
        &self,
        bucket: &Bucket<D>,
        key: u64,
        entry: &ObjectEntry,
        forced: bool,
        seen: &mut HashSet<(u64, u128)>,
    ) -> u64 {
        let mut waiting = 0;
        // 1. every retired object past its grace
        for retired in &entry.state.retired {
            seen.insert((key, retired.object));
            if !self.past_grace(key, retired.object, forced) {
                waiting += 1;
                continue;
            }
            match self.reclaim_object(bucket, key, &entry.path, *retired).await {
                Ok(true) => {}
                Ok(false) => {
                    waiting += 1;
                    bucket.meta.client.count_reclaim(&bucket.pool_name, |figures| figures.deferred += 1);
                }
                Err(why) => {
                    waiting += 1;
                    event!(Level::DEBUG, msg = "a retired object was not reclaimed; the next pass tries again", object = %format!("{:032x}", retired.object), why);
                }
            }
        }
        // 2. the oldest floor of a live object
        if entry.state.live && !entry.floors.is_empty() {
            if let Err(why) = self.reclaim_floor(bucket, key, entry).await {
                event!(Level::DEBUG, msg = "a floor was not reclaimed; the next pass tries again", path = %entry.path, why);
            }
        }
        // 3. a deleted entry that names nothing, read again since step 1 may have emptied it
        if !entry.state.live {
            if let Err(why) = self.remove_deleted(bucket, key, &entry.path).await {
                event!(Level::DEBUG, msg = "a deleted entry was not removed; the next pass tries again", path = %entry.path, why);
            }
        }
        waiting
    }

    /// Sweep this node's slices of every stray placement group: one whose committed state neither
    /// places on the slice nor names it left, and that is not moving (item 253)
    ///
    /// A release removes what a group left on a slice and refuses a late write of it for a seal's
    /// lapse, but a background copy waiting on its device's ceiling can arrive later still, and no
    /// release or light scrub ever asks that slice about the group again. A group on a slice its
    /// pool's target does not place there is read once this driver has found it so for the grace,
    /// or at once when a test asked; one whose state finds it stray is sealed on the slice, read
    /// again at its leader while sealed, and removed under the seal only if that state still finds
    /// it stray.
    ///
    /// # Arguments
    ///
    /// * `forced` - Whether a test asked for a pass, which takes every stray group past its grace
    async fn sweep(&self, forced: bool) {
        let installed = self.map.get();
        let mut found: HashSet<(SliceId, PgKey)> = HashSet::new();
        for slice in self.store.slices().into_keys() {
            // a slice the map does not place, or whose device serves nothing, is never asked
            let Some((slice_ref, holder)) = Self::slice_ref(&installed, slice) else {
                event!(Level::DEBUG, msg = "a sweep found a slice the map does not place", ?slice);
                continue;
            };
            let serving = installed
                .pool_map
                .devices
                .get(&slice_ref.device)
                .is_some_and(|device| device.failed.is_none() && device.tombstone.is_none());
            if !serving {
                continue;
            }
            let groups = match self.store.groups(slice).await {
                Ok(groups) => groups,
                Err(error) => {
                    event!(Level::DEBUG, msg = "a slice's groups did not list; the next pass asks again", slice = %slice_ref, error = ?error);
                    continue;
                }
            };
            // by consumer, since each is its bucket's
            let mut by_consumer: BTreeMap<u64, Vec<PlacementGroup>> = BTreeMap::new();
            for (consumer, pg) in groups {
                by_consumer.entry(consumer).or_default().push(pg);
            }
            for (consumer, groups) in by_consumer {
                let Some(name) = installed
                    .objects
                    .bindings
                    .iter()
                    .find(|(_, binding)| binding.consumer == consumer)
                    .map(|(name, _)| name.clone())
                else {
                    continue;
                };
                let Some(bucket) = self.bucket_named(&installed, &name) else {
                    continue;
                };
                let Some(placement) = bucket.placement.as_ref() else {
                    continue;
                };
                // a group its pool's target places here is where it belongs, and is never read
                let Some(target) = installed.pool_map.target_of(&bucket.pool_name) else {
                    continue;
                };
                // and the rest read once found for the grace, which a reader of an old view and a
                // release of what a move left both outlive, so many a pass
                let candidates: Vec<PgKey> = groups
                    .into_iter()
                    .filter(|pg| placement.answer_at(*pg, target).is_ok_and(|answer| !answer.contains(&slice_ref)))
                    .map(|pg| (consumer, pg))
                    .collect();
                let mut due = Vec::new();
                for key in candidates {
                    found.insert((slice, key));
                    let first = *self.strays.borrow_mut().entry((slice, key)).or_insert_with(Instant::now);
                    if (forced || first.elapsed() >= self.grace) && due.len() < SWEEP_READS {
                        due.push(key);
                    }
                }
                event!(Level::DEBUG, msg = "a sweep read a slice's groups", slice = %slice_ref, consumer, candidates = found.len(), due = due.len());
                if due.is_empty() {
                    continue;
                }
                // their states, read at their leaders
                let views = match bucket.meta.read_placements(&due).await {
                    Ok(views) => views,
                    Err((code, why)) => {
                        event!(Level::DEBUG, msg = "a sweep's placement read failed; the next pass reads again", %code, why);
                        continue;
                    }
                };
                for key in due {
                    let view = views.get(&key).cloned().unwrap_or_default();
                    if !Self::stray(placement, key.1, &view, slice_ref) {
                        event!(Level::DEBUG, msg = "a sweep found a group where it belongs", ?key, slice = %slice_ref, moving = view.moving.is_some(), left = ?view.left, generation = ?view.generation);
                        continue;
                    }
                    match self.sweep_group(&bucket, placement, holder, slice_ref, key).await {
                        Ok(true) => {
                            self.strays.borrow_mut().remove(&(slice, key));
                            bucket.meta.client.count_reclaim(&bucket.pool_name, |figures| figures.released += 1);
                            event!(Level::INFO, msg = "a stray placement group was swept from a slice", ?key, slice = %slice_ref);
                        }
                        Ok(false) => {}
                        Err(why) => {
                            bucket.meta.client.count_reclaim(&bucket.pool_name, |figures| figures.deferred += 1);
                            event!(Level::DEBUG, msg = "a stray placement group was not swept; the next pass tries again", ?key, why);
                        }
                    }
                }
            }
        }
        // a group no longer found was swept, or its pool's target places it there again
        self.strays.borrow_mut().retain(|key, _| found.contains(key));
    }

    /// A local slice's place on the pool map, its device and index, and the holder it is reached at
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `slice` - The slice
    fn slice_ref(installed: &TabletMap, slice: SliceId) -> Option<(SliceRef, Holder)> {
        installed.pool_map.devices.iter().find_map(|(device, record)| {
            let index = record
                .eras
                .iter()
                .rev()
                .find_map(|era| era.slices.iter().position(|id| *id == slice))?;
            let slice_ref = SliceRef {
                device: *device,
                index: u16::try_from(index).ok()?,
            };
            Some((slice_ref, Holder { node: record.node, slice }))
        })
    }

    /// Whether a placement group is stray on a slice under its state: not moving, not naming the
    /// slice left, and placing nothing on it
    ///
    /// A state a generation's view cannot place is never stray.
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `pg` - The placement group
    /// * `view` - Its state
    /// * `slice` - The slice
    fn stray(placement: &Placement, pg: PlacementGroup, view: &PgView, slice: SliceRef) -> bool {
        view.moving.is_none()
            && !view.left.contains(&slice)
            && placement.places_on(pg, view, slice).is_ok_and(|places| !places)
    }

    /// Sweep one stray placement group from a slice: seal it there, read its state at its leader
    /// while sealed, and remove it under the seal if the state still finds it stray
    ///
    /// Answers whether it was removed; false when the read found it placed there again.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `placement` - The pool's placement
    /// * `holder` - The slice's holder
    /// * `slice` - The slice
    /// * `key` - The placement group
    async fn sweep_group(&self, bucket: &Bucket<D>, placement: &Placement, holder: Holder, slice: SliceRef, key: PgKey) -> Result<bool, String> {
        let (consumer, pg) = key;
        // sealed first, so nothing the read below does not see lands there after it
        let seal: u64 = rand::random();
        let scope = SealScope::Group { consumer, pg };
        match self.store.ask_holder(holder, ChunkRequest::Seal { scope, seal }).await {
            ChunkReply::Done => {}
            other => return Err(format!("slice {slice} did not take its seal: {other:?}")),
        }
        let sealed = [(holder, scope)];
        // a test can hold a sweep here, between its seal and its read
        if let Some(hold) = crate::server::objects::faults::reclaim_hold() {
            glommio::timer::sleep(hold).await;
        }
        // the group's state while sealed
        let view = match bucket.meta.read_placements(&[key]).await {
            Ok(mut views) => views.remove(&key).unwrap_or_default(),
            Err((code, why)) => {
                self.unseal(&sealed, seal).await;
                return Err(format!("{code}: {why}"));
            }
        };
        if !Self::stray(placement, pg, &view, slice) {
            self.unseal(&sealed, seal).await;
            return Ok(false);
        }
        // removed under the seal, which the removal ends
        match self.store.ask_holder(holder, ChunkRequest::Reclaim { scope, seal }).await {
            ChunkReply::Done => Ok(true),
            other => {
                self.unseal(&sealed, seal).await;
                Err(format!("slice {slice} did not remove the group: {other:?}"))
            }
        }
    }

    /// Whether this driver has seen a retired object for its grace, noting it the first time
    ///
    /// # Arguments
    ///
    /// * `key` - The entry's row's key
    /// * `object` - The object
    /// * `forced` - Whether a test asked for every one now
    fn past_grace(&self, key: u64, object: u128, forced: bool) -> bool {
        let mut seen = self.first_seen.borrow_mut();
        let first = *seen.entry((key, object)).or_insert_with(Instant::now);
        forced || first.elapsed() >= self.grace
    }

    /// Every holder that may hold a placement group's chunks: each position's at its generation,
    /// each moving position's arrival, and every slice it left and has not released
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `key` - The placement group
    /// * `object` - An object whose stripe falls in it
    /// * `stripe` - The stripe
    /// * `view` - The group's state
    fn holders_of(placement: &Placement, key: PgKey, object: u128, stripe: u64, view: &PgView) -> Result<Vec<Holder>, String> {
        let set = placement.stripe_set_in(key.1, object, stripe, view)?;
        let mut holders: Vec<Holder> = set.targets.iter().map(|(_, holder)| *holder).collect();
        holders.extend(view.left.iter().filter_map(|slice| placement.holder_of(*slice)));
        holders.sort_unstable_by_key(|holder| (holder.node, holder.slice));
        holders.dedup();
        Ok(holders)
    }

    /// Reclaim a retired object: seal its holders, read it still retired, remove its chunks under
    /// the seals, then its rows, then its id from the entry
    ///
    /// Answers whether it is gone; false when a holder could not be sealed or its seal lapsed, for
    /// a later pass.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The entry's row's key
    /// * `path` - The entry's path
    /// * `retired` - The object and how far its stripes reach
    async fn reclaim_object(&self, bucket: &Bucket<D>, key: u64, path: &str, retired: Retired) -> Result<bool, String> {
        let object = retired.object;
        let geometry = bucket.pool.geometry();
        let stripe_bytes = u64::from(geometry.stripe_bytes).max(1);
        let stripes = retired.extent.div_ceil(stripe_bytes);
        let mut chunks = 0u64;
        let mut rows = 0u64;
        if stripes > 0 {
            let placement = bucket
                .placement
                .as_ref()
                .ok_or_else(|| format!("pool {} is not placed, so its stripes cannot be found", bucket.pool_name))?;
            // the stripes by placement group
            let mut groups: BTreeMap<PgKey, Vec<u64>> = BTreeMap::new();
            for stripe in 0..stripes {
                groups.entry(placement.pg_of(object, stripe)).or_default().push(stripe);
            }
            let groups: Vec<(PgKey, Vec<u64>)> = groups.into_iter().collect();
            for round in groups.chunks(GROUPS_A_ROUND) {
                match self.reclaim_round(bucket, placement, key, path, object, round).await? {
                    Some(removed) => chunks += removed,
                    None => return Ok(false),
                }
            }
            // its rows: each tombstoned, so the missed record forgets it, then deleted
            let all: Vec<u64> = (0..stripes).collect();
            for batch in all.chunks(ROWS_A_READ) {
                rows += self.remove_rows(bucket, object, batch).await?;
            }
        }
        // the id leaves the entry, and a deleted entry that names nothing goes with it
        self.forget(bucket, key, path, object).await?;
        bucket.meta.client.count_reclaim(&bucket.pool_name, |figures| {
            figures.objects += 1;
            figures.stripes += rows;
            figures.chunks += chunks;
        });
        event!(Level::DEBUG, msg = "a retired object was reclaimed", object = %format!("{object:032x}"), stripes, chunks, rows);
        Ok(true)
    }

    /// Seal, read and reclaim one round of a retired object's placement groups
    ///
    /// Answers the chunks removed, or none when a holder could not be sealed or its seal lapsed.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `placement` - The pool's placement
    /// * `key` - The entry's row's key
    /// * `path` - The entry's path
    /// * `object` - The object
    /// * `round` - The placement groups, each with the object's stripes in it
    async fn reclaim_round(
        &self,
        bucket: &Bucket<D>,
        placement: &Placement,
        key: u64,
        path: &str,
        object: u128,
        round: &[(PgKey, Vec<u64>)],
    ) -> Result<Option<u64>, String> {
        let installed = self.map.get();
        // each group's state, read linearizably at its leader
        let keys: Vec<PgKey> = round.iter().map(|(key, _)| *key).collect();
        let views = bucket
            .meta
            .read_placements(&keys)
            .await
            .map_err(|(code, why)| format!("{code}: {why}"))?;
        // every serving holder of every group, sealed under one seal
        let seal: u64 = rand::random();
        let mut sealed: Vec<(Holder, SealScope)> = Vec::new();
        for (pg_key, stripes) in round {
            let view = views.get(pg_key).cloned().unwrap_or_default();
            let first = stripes.first().copied().unwrap_or(0);
            let scope = SealScope::Object {
                consumer: bucket.consumer,
                pg: pg_key.1,
                object,
            };
            for holder in Self::holders_of(placement, *pg_key, object, first, &view)? {
                // a holder that cannot be reached is left to a light scrub when it serves again
                if !holder_serving(&installed, holder) {
                    continue;
                }
                sealed.push((holder, scope));
            }
        }
        let asks = sealed
            .iter()
            .map(|(holder, scope)| self.store.ask_holder(*holder, ChunkRequest::Seal { scope: *scope, seal }));
        let answers = futures::future::join_all(asks).await;
        if answers.iter().any(|answer| *answer != ChunkReply::Done) {
            self.unseal(&sealed, seal).await;
            return Ok(None);
        }
        // a test can hold a reclamation here, between its seals and its read
        if let Some(hold) = crate::server::objects::faults::reclaim_hold() {
            glommio::timer::sleep(hold).await;
        }
        // the fact, read strongly while sealed: retired, or named nowhere
        let row = bucket
            .meta
            .row(key, Some(DEFAULT_LEVEL))
            .await
            .map_err(|(code, why)| format!("{code}: {why}"))?;
        let named = row.as_ref().and_then(|row| row.entry(path)).is_some_and(|entry| {
            (entry.state.live && entry.object == object) || entry.state.replacing_id() == Some(object)
        });
        if named {
            self.unseal(&sealed, seal).await;
            return Err(format!("{object:032x} is named again at {path}"));
        }
        // removed under the seals
        let reclaims = sealed
            .iter()
            .map(|(holder, scope)| self.store.ask_holder(*holder, ChunkRequest::Reclaim { scope: *scope, seal }));
        let answers = futures::future::join_all(reclaims).await;
        if answers.iter().any(|answer| *answer != ChunkReply::Done) {
            // a seal that lapsed under a slow holder: what is left goes next pass
            self.unseal(&sealed, seal).await;
            return Ok(None);
        }
        Ok(Some(sealed.len() as u64))
    }

    /// End every seal of a round that is not to be reclaimed under
    ///
    /// # Arguments
    ///
    /// * `sealed` - Each holder and the scope sealed on it
    /// * `seal` - The seal
    async fn unseal(&self, sealed: &[(Holder, SealScope)], seal: u64) {
        let asks = sealed
            .iter()
            .map(|(holder, scope)| self.store.ask_holder(*holder, ChunkRequest::Unseal { scope: *scope, seal }));
        let _ = futures::future::join_all(asks).await;
    }

    /// Remove some of a retired object's stripe rows: each tombstoned first, which the missed record
    /// forgets it by, then deleted
    ///
    /// Answers how many rows it removed.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `object` - The object
    /// * `stripes` - The stripes
    async fn remove_rows(&self, bucket: &Bucket<D>, object: u128, stripes: &[u64]) -> Result<u64, String> {
        let rows = bucket
            .meta
            .stripe_rows(bucket.consumer, object, stripes)
            .await
            .map_err(|(code, why)| format!("{code}: {why}"))?;
        let mut removed = 0;
        for (stripe, row) in rows {
            // tombstoned, unless a pass before this one did
            let sequence = if row.tombstone {
                row.sequence
            } else {
                let tombstone = row.reclaimed(row.stamp);
                let sequence = tombstone.sequence;
                match bucket.meta.commit_stripe(tombstone, Some(row.sequence), None).await {
                    StripeAnswer::Written => sequence,
                    // a stale writer committed over it: the next pass removes what it made
                    StripeAnswer::Refused(_) => continue,
                    StripeAnswer::Failed(code, why) => return Err(format!("{code}: {why}")),
                    StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                        return Err("a tombstone answered as a read".to_string())
                    }
                }
            };
            match bucket
                .meta
                .ask_stripes(bucket.meta.ops.stripe_remove((bucket.consumer, object, stripe), sequence), None, None)
                .await
            {
                StripeAnswer::Written => removed += 1,
                StripeAnswer::Refused(_) => {}
                StripeAnswer::Failed(code, why) => return Err(format!("{code}: {why}")),
                StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => {
                    return Err("a row's delete answered as a read".to_string())
                }
            }
        }
        Ok(removed)
    }

    /// Take a reclaimed object's id out of its entry, and the entry out of its row once it is
    /// deleted and names nothing
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The row's key
    /// * `path` - The entry's path
    /// * `object` - The object
    async fn forget(&self, bucket: &Bucket<D>, key: u64, path: &str, object: u128) -> Result<(), String> {
        for _ in 0..MAX_TRIES {
            let Some(row) = bucket
                .meta
                .row(key, Some(DEFAULT_LEVEL))
                .await
                .map_err(|(code, why)| format!("{code}: {why}"))?
            else {
                return Ok(());
            };
            let Some(at) = row.position(path) else {
                return Ok(());
            };
            let mut entries = row.entries.clone();
            if !entries[at].state.retires(object) {
                return Ok(());
            }
            entries[at].state.retired.retain(|retired| retired.object != object);
            match self.commit_entries(bucket, &row, entries).await? {
                true => return Ok(()),
                false => continue,
            }
        }
        Err(format!("the row of {path} moved under its reclamation {MAX_TRIES} times"))
    }

    /// Remove a deleted entry that names nothing retired or in flight
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The row's key
    /// * `path` - The entry's path
    async fn remove_deleted(&self, bucket: &Bucket<D>, key: u64, path: &str) -> Result<(), String> {
        for _ in 0..MAX_TRIES {
            let Some(row) = bucket
                .meta
                .row(key, Some(DEFAULT_LEVEL))
                .await
                .map_err(|(code, why)| format!("{code}: {why}"))?
            else {
                return Ok(());
            };
            let Some(at) = row.position(path) else {
                return Ok(());
            };
            let state = &row.entries[at].state;
            if state.live || !state.retired.is_empty() || state.replacing.is_some() {
                return Ok(());
            }
            let mut entries = row.entries.clone();
            entries.remove(at);
            match self.commit_entries(bucket, &row, entries).await? {
                true => return Ok(()),
                false => continue,
            }
        }
        Err(format!("the row of {path} moved under its removal {MAX_TRIES} times"))
    }

    /// Commit a row's entries over the row read, or remove the row when none is left
    ///
    /// Answers whether it committed; false when the row moved since it was read.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `row` - The row read
    /// * `entries` - Its entries after the change
    async fn commit_entries(&self, bucket: &Bucket<D>, row: &ObjectRow, entries: Vec<ObjectEntry>) -> Result<bool, String> {
        let query = if entries.is_empty() {
            bucket.meta.ops.remove(row.key, row.version)
        } else {
            bucket.meta.ops.replace(
                ObjectRow {
                    key: row.key,
                    version: row.version + 1,
                    entries,
                },
                row.version,
            )
        };
        match bucket.meta.ask(query, None).await {
            MetaAnswer::Written => Ok(true),
            MetaAnswer::Refused(_) => Ok(false),
            MetaAnswer::Failed(code, why) => Err(format!("{code}: {why}")),
            MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => Err("an entry's commit answered as a read".to_string()),
        }
    }

    /// Reclaim the stripes a live object's oldest floor hides, and the floor once nothing it hides
    /// is left: X1's `Reclaim`, the floor dropped by M20's rule
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The row's key
    /// * `entry` - The entry, as the walk read it
    async fn reclaim_floor(&self, bucket: &Bucket<D>, key: u64, entry: &ObjectEntry) -> Result<(), String> {
        let Some(floor) = entry.floors.iter().min_by_key(|floor| floor.epoch).cloned() else {
            return Ok(());
        };
        let geometry = entry.geometry;
        let stripe_bytes = u64::from(geometry.stripe_bytes).max(1);
        let width = geometry.width();
        let reach = entry.extent.max(entry.size).max(entry.put_length);
        // every stripe wholly past the cut, up to the most the object's stripes ever reached
        let first = floor.length.div_ceil(stripe_bytes);
        let last = reach.div_ceil(stripe_bytes);
        let mut all = true;
        let stripes: Vec<u64> = (first..last).collect();
        for batch in stripes.chunks(ROWS_A_READ) {
            let rows = bucket
                .meta
                .stripe_rows(bucket.consumer, entry.object, batch)
                .await
                .map_err(|(code, why)| format!("{code}: {why}"))?;
            for stripe in batch {
                let row = rows.get(stripe);
                // written since the truncate, or reclaimed under it already: nothing it hides
                if row.is_some_and(|row| row.stamp >= floor.epoch) {
                    continue;
                }
                if !self.tombstone_stripe(bucket, key, entry, row, *stripe, &floor, width).await? {
                    all = false;
                }
            }
        }
        // the stripe the cut falls inside keeps the floor until a write since stamped it past
        if !floor.length.is_multiple_of(stripe_bytes) && floor.length < reach {
            let cut = floor.length / stripe_bytes;
            let rows = bucket
                .meta
                .stripe_rows(bucket.consumer, entry.object, &[cut])
                .await
                .map_err(|(code, why)| format!("{code}: {why}"))?;
            if !rows.get(&cut).is_some_and(|row| row.stamp >= floor.epoch) {
                all = false;
            }
        }
        if all {
            self.drop_floor(bucket, key, &entry.path, entry.object, floor).await?;
        }
        Ok(())
    }

    /// Tombstone one stripe a floor hides a sequence past what was read, and tell its holders to
    /// discard through that sequence; whether it committed
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The row's key
    /// * `entry` - The entry
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe
    /// * `floor` - The floor
    /// * `width` - The stripe's positions
    #[allow(clippy::too_many_arguments)]
    async fn tombstone_stripe(
        &self,
        bucket: &Bucket<D>,
        key: u64,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        stripe: u64,
        floor: &Floor,
        width: usize,
    ) -> Result<bool, String> {
        let base = row
            .cloned()
            .unwrap_or_else(|| StripeRow::put(bucket.consumer, entry.object, stripe, width, 0, key));
        let through = base.sequence;
        match bucket
            .meta
            .commit_stripe(base.reclaimed(floor.epoch), row.map(|row| row.sequence), None)
            .await
        {
            StripeAnswer::Written => {}
            // the row moved: judged again next pass
            StripeAnswer::Refused(_) => return Ok(false),
            StripeAnswer::Failed(code, why) => return Err(format!("{code}: {why}")),
            StripeAnswer::Rows(_) | StripeAnswer::Unexpected(_) => return Err("a tombstone answered as a read".to_string()),
        }
        // the committed tombstone is the fact: every holder discards through the sequence read
        if let Some(placement) = &bucket.placement {
            let pg = placement.pg_of(entry.object, stripe);
            let views = bucket
                .meta
                .read_placements(&[pg])
                .await
                .map_err(|(code, why)| format!("{code}: {why}"))?;
            let view = views.get(&pg).cloned().unwrap_or_default();
            let set = placement.stripe_set_in(pg.1, entry.object, stripe, &view)?;
            let discards = set.targets.iter().map(|(chunk, holder)| {
                self.store
                    .ask_holder(*holder, ChunkRequest::DiscardThrough { chunk: *chunk, through })
            });
            let answers = futures::future::join_all(discards).await;
            let done = answers.iter().filter(|answer| **answer == ChunkReply::Done).count() as u64;
            bucket.meta.client.count_reclaim(&bucket.pool_name, |figures| {
                figures.stripes += 1;
                figures.chunks += done;
            });
        }
        Ok(true)
    }

    /// Drop a floor from a live object's entry, if it is still the one read
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket
    /// * `key` - The row's key
    /// * `path` - The entry's path
    /// * `object` - The object the floor was read under
    /// * `floor` - The floor
    async fn drop_floor(&self, bucket: &Bucket<D>, key: u64, path: &str, object: u128, floor: Floor) -> Result<(), String> {
        for _ in 0..MAX_TRIES {
            let Some(row) = bucket
                .meta
                .row(key, Some(DEFAULT_LEVEL))
                .await
                .map_err(|(code, why)| format!("{code}: {why}"))?
            else {
                return Ok(());
            };
            let Some(at) = row.position(path) else {
                return Ok(());
            };
            let mut entries = row.entries.clone();
            let entry = &mut entries[at];
            // the same object, still holding the same floor
            if entry.object != object || !entry.floors.iter().any(|held| held.epoch == floor.epoch) {
                return Ok(());
            }
            entry.floors.retain(|held| held.epoch != floor.epoch);
            match self.commit_entries(bucket, &row, entries).await? {
                true => {
                    bucket.meta.client.count_reclaim(&bucket.pool_name, |figures| figures.floors += 1);
                    return Ok(());
                }
                false => continue,
            }
        }
        Err(format!("the row of {path} moved under a floor's removal {MAX_TRIES} times"))
    }
}
