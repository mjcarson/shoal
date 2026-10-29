//! A placement group's move: from the generation its chunks sit under to its pool's target
//!
//! Every shard runs one task that looks at each bucket stripe group it leads
//! ([S10](../../../../../docs/src/object-storage/recovery.md#moves),
//! [F88](../../../../../docs/src/features/recovery-and-moves.md)). A placement group whose holders
//! at its own generation differ from its pool's target's is moved:
//!
//! 1. the control group admits it, one move arriving on a device at once, unless its old holders
//!    list nothing of it: a group that holds nothing begins with no slot, since nothing arrives, and
//!    its copier takes one before its first byte moves if a put landed since (item 250);
//! 2. `MoveBegin` puts the group in both generations: every write, put and fold from then on
//!    stages on its old holders and its arrivals, and a moving position counts held only when both
//!    hold it;
//! 3. the copier writes every stripe onto its arrivals: each row the group's walk finds, at the
//!    label it names, and each stripe a put wrote whole and no row names, which the old holders'
//!    listings find, at the put's label. A stripe it cannot copy is marked missed if the old
//!    slice is gone, and otherwise the move is given up for a later one;
//! 4. once every arrival is up, `MoveSwitch` records the new generation and positions in one
//!    commit, and the slot is freed.
//!
//! A group found moving is one a driver began: its copy and switch are run again if the control
//! group still holds its slot, and it is aborted if not. The old holders' chunks stay where they
//! are; reclaiming them is M20's.
//!
//! # Invariants
//!
//! **The switch comes after the copier, never before** (X1's `switch_before_the_copier_finishes`).
//!
//! **A stripe no row names is found by listing an old holder other than its moving position**,
//! and a move none of whose listings were reached is given up rather than switched without them.
//!
//! **A slot bounds bytes, never safety**: the copier's walk and listings after the begin decide what
//! a move copies, with a slot or without one, and a move with none takes one before a byte arrives
//! (item 250).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tracing::{event, Level};

use super::place::Placement;
use super::recover::{background, copy_whole, holder_serving};
use super::{Meta, MetaClient};
use crate::server::control::plane::ControlRequest;
use crate::server::control::types::{ControlCommand, ControlResponse};
use crate::server::database::ShoalDatabase;
use crate::server::map::{MapCell, TabletMap};
use crate::server::messages::ServerMsg;
use crate::server::objects::{Holder, ObjectStore};
use crate::server::shard::walk::{TabletWalk, WalkAsk};
use crate::server::shard::LedStripes;
use crate::shared::identity::{BucketId, TableId};
use crate::shared::objects::{Label, StripeAnswer};
use crate::shared::placement_state::{Moving, PgKey, PgView, PlacementCommand};
use crate::shared::pool_placement::positions_after;
use crate::shared::poolmap::{MoveSlot, PlacementGroup, SliceRef, MOVES_A_DEVICE};
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, SealScope, MAX_LISTED};
use crate::shared::protocol::{RECLAIM_WIRE_VERSION, RECOVERY_WIRE_VERSION};
use crate::shared::queries::ConditionRefusal;
use crate::shared::stripes::{ChunkId, StripeRow};
use crate::shared::traits::QuerySupport;

/// How many times a move's copy is run again before the move is given up
const COPY_TRIES: u32 = 3;

/// How long a move whose copy is done waits for an arrival that is not serving before it is given
/// up: a node the leader's detector calls down for a moment under load is up again well inside it
/// (item 281)
const SWITCH_WAIT: Duration = Duration::from_secs(30);

/// How often a move waiting for its arrival looks at the map again
const SWITCH_POLL: Duration = Duration::from_millis(500);

/// Why a move's copy stopped short
enum Short {
    /// It cannot finish: the move is given up, for a later one to try again
    Abort(String),
    /// It may finish on another try
    Retry(String),
}

/// Whether a move runs under an arrival's slot the control group holds for it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// The control group holds its slot, which is freed once the move switches or is given up
    Held,
    /// It began with none, since its group held nothing, and takes one before a byte arrives
    /// (item 250)
    Unneeded,
    /// It was found moving with no slot the control group holds, and is given up
    Gone,
}

/// What one placement group needs of the driver
enum Work {
    /// A move to begin, from its holders now to the target's
    Begin {
        /// The bucket's stripe table
        table: TableId,
        /// The placement group
        key: PgKey,
        /// Its state
        state: PgView,
        /// Each position's slice now
        old: Vec<SliceRef>,
        /// The target's answer, in the rule's order
        new: Vec<SliceRef>,
        /// The generation it moves to
        target: u64,
    },
    /// A move begun before, to copy and switch again or to abort
    Resume {
        /// The bucket's stripe table
        table: TableId,
        /// The placement group
        key: PgKey,
        /// Its state, with the move in it
        state: PgView,
        /// Whether the control group holds its slot, or this driver began it needing none
        slot: Slot,
    },
    /// A slot whose group is not moving: its switch or abort committed and its `MoveDone` did not,
    /// so nothing else would ever free it
    Free {
        /// The pool
        pool: String,
        /// The placement group
        key: PgKey,
    },
    /// A slice the group left, past its grace, to release of what it holds of the group (M20)
    Release {
        /// The bucket's stripe table
        table: TableId,
        /// The placement group
        key: PgKey,
        /// The slice
        slice: SliceRef,
    },
}

/// The devices a move from one set of holders to another arrives on
///
/// # Arguments
///
/// * `old` - Each position's slice now
/// * `new` - The target's answer, in the rule's order
fn arrivals(old: &[SliceRef], new: &[SliceRef]) -> Vec<crate::shared::devices::DeviceId> {
    positions_after(old, new)
        .iter()
        .enumerate()
        .filter_map(|(position, index)| {
            let arrival = new.get(usize::from(*index))?;
            (old.get(position) != Some(arrival)).then_some(arrival.device)
        })
        .collect()
}

/// What every move of one shard's task holds
struct Mover<D: ShoalDatabase> {
    /// The shard's metadata client
    client: MetaClient,
    /// The shard's own channel
    shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    /// The shard's map
    map: MapCell,
    /// The node's object store
    store: ObjectStore,
    /// The node's control thread
    control: kanal::Sender<ControlRequest>,
    /// How long one metadata query may take
    wait: Duration,
    /// How many times each group's copy failed, so one that keeps failing is given up
    failures: RefCell<HashMap<PgKey, u32>>,
    /// Each device an admission was refused for as full, and when, so no begin needing it is
    /// proposed again for `FULL_FOR`: every shard reads the slots empty at a move storm's start
    full: RefCell<HashMap<crate::shared::devices::DeviceId, std::time::Instant>>,
    /// How long a slice a group left keeps what it held for the readers that read the group
    /// before, none when this node does not reclaim (M20)
    release: Option<Duration>,
    /// When this driver first saw each slice a group left, and the reclamation passes a test had
    /// asked for by then, so the grace is this driver's own clock and a test can end it
    left_seen: RefCell<HashMap<(PgKey, SliceRef), (Instant, u64)>>,
    /// Whether each placement group held anything when its old holders listed it, by group and the
    /// target it moves to: one that held nothing moves with no arrival's slot (item 250)
    holds: RefCell<HashMap<(PgKey, u64), bool>>,
    /// The moves this driver began with no slot, by group and move, which a later poll resumes; a
    /// driver that did not begin one finds it moving with no slot and gives it up
    slotless: RefCell<HashSet<(PgKey, u64)>>,
}

/// How long a control proposal of a move's may take before it is logged
const SLOW_PROPOSAL: Duration = Duration::from_millis(250);

/// How long a device an admission was refused for is taken as full
const FULL_FOR: Duration = Duration::from_millis(500);

impl<D: ShoalDatabase> Mover<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// A bucket's queries, its pool's name and its placement, for the consumer it is bound as
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `consumer` - The consumer
    fn bucket(&self, installed: &Arc<TabletMap>, consumer: u64) -> Result<(Meta<D>, String, Placement), String> {
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
            span: tracing::info_span!("Object::move", consumer),
            wait: self.wait,
        };
        Ok((meta, binding.pool.clone(), placement))
    }

    /// Every placement group of the groups this shard leads that has a move to begin or resume
    ///
    /// # Arguments
    ///
    /// * `led` - The bucket stripe groups this shard leads
    /// * `slots` - Every move the control group holds a slot for
    fn work(&self, led: &[LedStripes], slots: &[MoveSlot]) -> Vec<Work> {
        let installed = self.map.get();
        let mut work = Vec::new();
        // how many groups each pool has still to move, which `pool_status` sums
        let mut behind: BTreeMap<String, u64> = BTreeMap::new();
        // what a group left is released once the cluster speaks the reclamation wire, unless a
        // test paused reclamation to look at it first (M20)
        let releasing = self.release.is_some()
            && (installed.members.is_empty() || installed.activated_wire >= RECLAIM_WIRE_VERSION);
        let paused = crate::server::objects::faults::reclaim_paused();
        let mut seen_left: HashSet<(PgKey, SliceRef)> = HashSet::new();
        // each consumer's target now, which the listings kept for groups' moves are judged against
        let mut targets: HashMap<u64, u64> = HashMap::new();
        for (name, binding) in &installed.objects.bindings {
            let consumer = binding.consumer;
            // a pool with no target has nowhere to move to
            let Some(target) = installed.pool_map.target_of(&binding.pool) else {
                continue;
            };
            targets.insert(consumer, target);
            let Some(ops) = <D::ClientType as QuerySupport>::object_queries(BucketId::of(name)) else {
                continue;
            };
            let Ok((_, _, placement)) = self.bucket(&installed, consumer) else {
                continue;
            };
            let table = TableId::of(ops.stripe_table());
            let groups = placement.pool.groups_per_tablet().max(1);
            for group in led.iter().filter(|group| group.table == table) {
                let moved: HashMap<PgKey, PgView> = group.moved.iter().cloned().collect();
                // a slot held for a group of these that is not moving was left by a lost
                // `MoveDone`, and is freed
                for slot in slots
                    .iter()
                    .filter(|slot| slot.pool == binding.pool && slot.consumer == consumer)
                    .filter(|slot| group.tablets.contains(&slot.pg.tablet))
                {
                    let key = (consumer, slot.pg);
                    if moved.get(&key).is_none_or(|state| state.moving.is_none()) {
                        work.push(Work::Free {
                            pool: binding.pool.clone(),
                            key,
                        });
                    }
                }
                for tablet in &group.tablets {
                    for sub in 0..groups {
                        let key = (
                            consumer,
                            PlacementGroup {
                                tablet: *tablet,
                                sub: u16::try_from(sub).unwrap_or(u16::MAX),
                            },
                        );
                        let state = moved.get(&key).cloned().unwrap_or_default();
                        // each slice the group left is released once its grace passed, never while
                        // the group moves, whose switch or abort may name it again
                        if releasing {
                            for slice in &state.left {
                                seen_left.insert((key, *slice));
                                if !paused && state.moving.is_none() && self.release_due(&installed, &placement, key, *slice) {
                                    work.push(Work::Release {
                                        table,
                                        key,
                                        slice: *slice,
                                    });
                                }
                            }
                        }
                        // a move begun before is resumed or given up: one this driver began with no
                        // slot is its own to resume (item 250)
                        if let Some(moving) = &state.moving {
                            *behind.entry(binding.pool.clone()).or_default() += 1;
                            let held = slots
                                .iter()
                                .any(|slot| slot.pool == binding.pool && (slot.consumer, slot.pg) == key);
                            let slot = if held {
                                Slot::Held
                            } else if self.slotless.borrow().contains(&(key, moving.id)) {
                                Slot::Unneeded
                            } else {
                                Slot::Gone
                            };
                            work.push(Work::Resume { table, key, state, slot });
                            continue;
                        }
                        if placement.generation_of(&state) >= target {
                            continue;
                        }
                        // a group whose holders there are its holders now stays where it is
                        let (Ok(old), Ok(new)) = (placement.slices_of(key.1, &state), placement.answer_at(key.1, target)) else {
                            continue;
                        };
                        let mut was: Vec<SliceRef> = old.clone();
                        let mut will: Vec<SliceRef> = new.clone();
                        was.sort_unstable();
                        will.sort_unstable();
                        if was == will {
                            continue;
                        }
                        *behind.entry(binding.pool.clone()).or_default() += 1;
                        // an arrival the group left before is released first, so nothing it still
                        // holds of the group is taken for what a copy put there (X1's release)
                        if releasing && new.iter().any(|slice| !old.contains(slice) && state.left.contains(slice)) {
                            continue;
                        }
                        work.push(Work::Begin {
                            table,
                            key,
                            state,
                            old,
                            new,
                            target,
                        });
                    }
                }
            }
        }
        self.client.set_behind(behind);
        // a slice no group names as left any more was released, here or by another leader
        self.left_seen.borrow_mut().retain(|key, _| seen_left.contains(key));
        // a group's listing is kept only for the target its pool moves to now
        self.holds
            .borrow_mut()
            .retain(|((consumer, _), target), _| targets.get(consumer) == Some(target));
        work
    }

    /// Whether a move to begin is of a group not known to hold anything, which moves with no slot
    /// and so waits for no device's arrivals (item 250)
    ///
    /// # Arguments
    ///
    /// * `work` - The work
    fn light(&self, work: &Work) -> bool {
        match work {
            Work::Begin { key, target, .. } => self.holds.borrow().get(&(*key, *target)) != Some(&true),
            _ => false,
        }
    }

    /// Where a piece of work goes in a poll's order: the moves under way and the slots to free
    /// first, then the moves of groups not known to hold anything, those still placing on a slice
    /// that serves nothing ahead, then every other move, and the releases of slices groups left
    /// last, since a release that keeps failing must never hold a move back (item 250)
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `work` - The work
    fn order(&self, installed: &TabletMap, work: &Work) -> u8 {
        match work {
            Work::Resume { .. } | Work::Free { .. } => 0,
            Work::Begin { old, .. } if self.light(work) => {
                if old.iter().any(|slice| Self::gone(installed, *slice)) {
                    1
                } else {
                    2
                }
            }
            Work::Begin { .. } => 3,
            Work::Release { .. } => 4,
        }
    }

    /// Whether a slice a group left is due its release: this driver has seen it left for the
    /// grace, or a test asked for a reclamation since, and it can be reached or never serves again
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `placement` - The pool's placement
    /// * `key` - The placement group
    /// * `slice` - The slice
    fn release_due(&self, installed: &TabletMap, placement: &Placement, key: PgKey, slice: SliceRef) -> bool {
        let Some(grace) = self.release else {
            return false;
        };
        // a slice on a node that is down waits for it; one that never serves again does not
        if let Some(holder) = placement.holder_of(slice) {
            if !Self::gone(installed, slice) && !holder_serving(installed, holder) {
                return false;
            }
        }
        let asked = crate::server::objects::faults::reclaim_asked();
        let mut seen = self.left_seen.borrow_mut();
        let (first, asked_then) = *seen.entry((key, slice)).or_insert((Instant::now(), asked));
        first.elapsed() >= grace || asked != asked_then
    }

    /// Whether a slice is never served again: its device left the map, failed, or is one its node
    /// refuses to serve, removed or retired, so what it holds is never read, and a light scrub
    /// finds it if it serves again (M20, item 333)
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `slice` - The slice
    fn gone(installed: &TabletMap, slice: SliceRef) -> bool {
        installed
            .pool_map
            .devices
            .get(&slice.device)
            .is_none_or(crate::shared::poolmap::DeviceRecord::serves_nothing)
    }

    /// Release a slice a placement group left of what it holds of the group: seal the group on it,
    /// read the group's state at its leader, remove the group there under the seal if the group
    /// still names it left and places nothing on it, then commit the release (M20)
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `slice` - The slice
    async fn release(&self, table: TableId, key: PgKey, slice: SliceRef) -> Result<(), String> {
        let installed = self.map.get();
        let (consumer, pg) = key;
        let (meta, pool, placement) = self.bucket(&installed, consumer)?;
        // a slice that never serves again is released without a word to it
        let holder = match placement.holder_of(slice) {
            Some(holder) if !Self::gone(&installed, slice) => holder,
            _ => return self.release_done(table, key, &pool, slice).await,
        };
        // sealed first, so nothing the read below does not see lands on it after
        let seal: u64 = rand::random();
        let scope = SealScope::Group { consumer, pg };
        match self.store.ask_holder(holder, ChunkRequest::Seal { scope, seal }).await {
            ChunkReply::Done => {}
            other => return Err(format!("slice {slice:?} did not take its seal: {other:?}")),
        }
        let unseal = || self.store.ask_holder(holder, ChunkRequest::Unseal { scope, seal });
        // a test can hold a release here, between its seal and its read
        if let Some(hold) = crate::server::objects::faults::reclaim_hold() {
            glommio::timer::sleep(hold).await;
        }
        // the group's state while sealed, read at its leader
        let view = match meta.read_placements(&[key]).await {
            Ok(mut views) => views.remove(&key).unwrap_or_default(),
            Err((code, why)) => {
                unseal().await;
                return Err(format!("{code}: {why}"));
            }
        };
        let places = match placement.places_on(pg, &view, slice) {
            Ok(places) => places,
            Err(why) => {
                unseal().await;
                return Err(why);
            }
        };
        // released by another leader since, or placing there again: nothing there is removed
        if !view.left.contains(&slice) || places {
            unseal().await;
            if view.left.contains(&slice) {
                return self.release_done(table, key, &pool, slice).await;
            }
            return Ok(());
        }
        // removed under the seal, which a removal ends
        match self.store.ask_holder(holder, ChunkRequest::Reclaim { scope, seal }).await {
            ChunkReply::Done => {}
            other => {
                unseal().await;
                return Err(format!("slice {slice:?} did not release the group: {other:?}"));
            }
        }
        self.release_done(table, key, &pool, slice).await
    }

    /// Commit that a placement group no longer names a slice as left
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `pool` - The pool
    /// * `slice` - The slice
    async fn release_done(&self, table: TableId, key: PgKey, pool: &str, slice: SliceRef) -> Result<(), String> {
        let (consumer, pg) = key;
        self.placement(table, PlacementCommand::ReleaseDone { consumer, pg, slice })
            .await?;
        self.left_seen.borrow_mut().remove(&(key, slice));
        self.client.count_reclaim(pool, |figures| figures.released += 1);
        event!(Level::INFO, msg = "a slice a placement group left was released", ?key, ?slice);
        Ok(())
    }

    /// Every move the control group holds a slot for, as this node's control thread has applied
    async fn slots(&self) -> Result<Vec<MoveSlot>, String> {
        let (reply, answer) = kanal::bounded(1);
        self.control
            .try_send(ControlRequest::MoveSlots(reply))
            .map_err(|_| "the control thread is not taking requests".to_string())?;
        let mut answer = shoal_channel::KeptReceiver::new(answer.to_async());
        match glommio::timer::timeout(Duration::from_secs(10), async { Ok(answer.next().await) }).await {
            Ok(Ok(slots)) => Ok(slots),
            Ok(Err(_)) => Err("the control thread dropped the request".to_string()),
            Err(_) => Err("the control thread did not answer within 10s".to_string()),
        }
    }

    /// Propose a command through the control group and wait for what it came to
    ///
    /// # Arguments
    ///
    /// * `command` - The command
    async fn control(&self, command: ControlCommand) -> Result<ControlResponse, String> {
        let started = std::time::Instant::now();
        let kind = command.to_string();
        let (reply, answer) = kanal::bounded(1);
        self.control
            .try_send(ControlRequest::Propose { command, reply })
            .map_err(|_| "the control thread is not taking proposals".to_string())?;
        let mut answer = shoal_channel::KeptReceiver::new(answer.to_async());
        let outcome = match glommio::timer::timeout(Duration::from_secs(10), async { Ok(answer.next().await) }).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err("the control thread dropped the proposal".to_string()),
            Err(_) => Err("the control group did not answer within 10s".to_string()),
        };
        // a move waits on two of these, so a slow one is said
        let took = started.elapsed();
        if took > SLOW_PROPOSAL {
            event!(Level::INFO, msg = "a move's control proposal was slow", kind = %kind, ms = took.as_millis());
        }
        outcome
    }

    /// Propose a placement command through the group that holds its placement group
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `command` - The command
    async fn placement(&self, table: TableId, command: PlacementCommand) -> Result<(), String> {
        let (reply, answer) = futures_channel::oneshot::channel();
        self.shard_tx
            .send(ServerMsg::PlacementPropose { table, command, reply })
            .await
            .map_err(|_| "the shard stopped".to_string())?;
        answer.await.map_err(|_| "the placement command was dropped".to_string())?
    }

    /// Whether a holder can take an arrival now: its device serves, neither failed nor one its node
    /// refuses to serve (item 333), and its node is up
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `holder` - The holder
    fn serving(installed: &TabletMap, holder: Holder) -> bool {
        let failed = installed.pool_map.devices.values().any(|device| {
            device.serves_nothing() && device.eras.iter().any(|era| era.slices.contains(&holder.slice))
        });
        !failed && (installed.members.is_empty() || installed.is_up(holder.node))
    }

    /// Ask the control group for an arrival's slot for a move: one move arriving on a device at once
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `key` - The placement group
    /// * `target` - The generation it moves to
    /// * `devices` - The devices it arrives on
    async fn admit(&self, pool: &str, key: PgKey, target: u64, devices: Vec<crate::shared::devices::DeviceId>) -> Result<(), String> {
        let (consumer, pg) = key;
        let slot = MoveSlot {
            pool: pool.to_string(),
            consumer,
            pg,
            generation: target,
            devices,
        };
        let devices_asked = slot.devices.clone();
        // a test can keep this node's moves from being admitted, as a cluster whose arrivals' slots
        // are all taken keeps them (item 250)
        if crate::server::objects::faults::admission_held() {
            return Err("a test holds this node's admissions".to_string());
        }
        match self.control(ControlCommand::AdmitMove { slot }).await? {
            ControlResponse::Applied { .. } => Ok(()),
            // its devices take as many moves as they may: none of them is asked for again for a
            // while, and the next poll after that asks again
            other => {
                let now = std::time::Instant::now();
                let mut full = self.full.borrow_mut();
                for device in devices_asked {
                    full.insert(device, now);
                }
                Err(format!("the move was not admitted yet: {other:?}"))
            }
        }
    }

    /// Whether a placement group holds nothing a move would copy, as every old holder that serves
    /// lists it, kept by group and target so a group is listed once a move (item 250)
    ///
    /// A listing commits nothing and waits for no idle time. A holder that does not answer, or no
    /// holder serving, leaves the group taken as holding something, and so does any chunk listed.
    /// The verdict decides only whether the move waits for a slot: its copier lists the old
    /// holders again after the begin, and a stripe it finds there asks for one then.
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `key` - The placement group
    /// * `old` - Each position's slice now
    /// * `target` - The generation it moves to
    async fn holds_nothing(&self, placement: &Placement, key: PgKey, old: &[SliceRef], target: u64) -> bool {
        // listed already for this move
        if let Some(holds) = self.holds.borrow().get(&(key, target)) {
            return !*holds;
        }
        let installed = self.map.get();
        let (consumer, pg) = key;
        let mut answered = 0usize;
        let mut holds = false;
        for slice in old {
            // a holder that serves nothing now holds nothing a copy could read
            let Some(holder) = placement.holder_of(*slice) else {
                continue;
            };
            if !Self::serving(&installed, holder) {
                continue;
            }
            match self.list(holder, consumer, pg).await {
                Ok(chunks) if chunks.is_empty() => answered += 1,
                // a chunk listed, or a holder that may hold what the others do not
                Ok(_) | Err(_) => {
                    holds = true;
                    break;
                }
            }
        }
        let holds = holds || answered == 0;
        self.holds.borrow_mut().insert((key, target), holds);
        !holds
    }

    /// Begin a placement group's move, then copy and switch it
    ///
    /// A group that holds nothing begins with no arrival's slot, since nothing arrives: a slot
    /// bounds the bytes a device takes at once, never what a move may do. One found holding
    /// something is admitted first, here when the poll counted the moves arriving on its devices,
    /// and at the next poll when it did not (item 250).
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `state` - Its state
    /// * `old` - Each position's slice now
    /// * `new` - The target's answer
    /// * `target` - The generation it moves to
    /// * `gated` - Whether the poll counted the moves arriving on its devices, so it may be admitted
    #[allow(clippy::too_many_arguments)]
    async fn begin(
        &self,
        table: TableId,
        key: PgKey,
        state: PgView,
        old: Vec<SliceRef>,
        new: Vec<SliceRef>,
        target: u64,
        gated: bool,
    ) -> Result<(), String> {
        let installed = self.map.get();
        let (consumer, pg) = key;
        let (_, pool, placement) = self.bucket(&installed, consumer)?;
        // each position's slice there, a slice that stays keeping its position
        let positions = positions_after(&old, &new);
        let devices = arrivals(&old, &new);
        // a group that holds nothing moves with no slot; one that holds something is admitted, one
        // move arriving on a device at once, once the poll has counted its devices' arrivals: a
        // refused admission is still a commit of the control group (Resolved #243)
        let slot = if self.holds_nothing(&placement, key, &old, target).await {
            Slot::Unneeded
        } else if gated {
            self.admit(&pool, key, target, devices).await?;
            Slot::Held
        } else {
            return Err("the group holds stripes, so its slot is asked for at the next poll".to_string());
        };
        // the group in both generations, under the epoch the begin makes
        let id = state.epoch + 1;
        let moving = Moving {
            id,
            generation: target,
            positions: positions.clone(),
        };
        let begun = self
            .placement(
                table,
                PlacementCommand::MoveBegin {
                    consumer,
                    pg,
                    id,
                    expect: state.epoch,
                    generation: target,
                    positions,
                },
            )
            .await;
        // a begin refused leaves its slot to nobody: freed, and the next poll reads the group again
        if let Err(why) = begun {
            if slot == Slot::Held {
                self.done(&pool, consumer, pg).await?;
            }
            return Err(why);
        }
        event!(Level::INFO, msg = "a placement group began its move", ?key, target, id, ?slot);
        // a move with no slot is this driver's own while it runs, so a later poll resumes it rather
        // than taking it for one whose slot is gone; a new leader finds no such word, and gives it up
        if slot == Slot::Unneeded {
            self.slotless.borrow_mut().insert((key, id));
        }
        let state = PgView {
            epoch: id,
            moving: Some(moving),
            ..state
        };
        // the slot is this driver's own, just admitted, or none was needed
        self.resume(table, key, state, slot).await
    }

    /// Copy a moving placement group onto its arrivals and switch it, or give the move up
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `state` - Its state, with the move in it
    /// * `slot` - Whether the control group holds the move's slot, as it answered this driver, or
    ///   the move needed none
    async fn resume(&self, table: TableId, key: PgKey, state: PgView, slot: Slot) -> Result<(), String> {
        let installed = self.map.get();
        let (consumer, pg) = key;
        let (meta, pool, placement) = self.bucket(&installed, consumer)?;
        let Some(moving) = state.moving.clone() else {
            return Ok(());
        };
        // a move found moving whose slot is gone was given up, or another leader finished its
        // slot. Judged by the control group's own answer, never by a pushed map, which a slot
        // does not move: a move judged so gave itself up half the time on the lab (Resolved #243)
        if slot == Slot::Gone {
            return self.abort(table, key, &pool, (&placement, &state), moving.id, "its slot is gone", false).await;
        }
        // a move with no slot takes one if its copier finds a byte to copy (item 250)
        let slot = Cell::new(slot);
        match self.copy(&meta, &placement, (table, &pool), key, &state, &slot).await {
            Ok(()) => {}
            Err(Short::Abort(why)) => {
                return self.abort(table, key, &pool, (&placement, &state), moving.id, &why, slot.get() == Slot::Held).await
            }
            Err(Short::Retry(why)) => {
                // one that keeps failing is given up, for a later move to try again
                let tries = {
                    let mut failures = self.failures.borrow_mut();
                    let tries = failures.entry(key).or_insert(0);
                    *tries += 1;
                    *tries
                };
                if tries >= COPY_TRIES {
                    self.failures.borrow_mut().remove(&key);
                    return self.abort(table, key, &pool, (&placement, &state), moving.id, &why, slot.get() == Slot::Held).await;
                }
                return Err(why);
            }
        }
        // every arrival up before the switch, waited for a while from the map as it is installed:
        // a node the leader's detector calls down for a moment under load is up again in seconds,
        // and a move given up for it costs its copy again (item 281)
        let probe = placement.stripe_set_in(pg, 0, 0, &state)?;
        let waited = Instant::now();
        // a test's arrivals found down at the first look, as a node called down for a moment is
        let mut found_down = crate::server::objects::faults::arrivals_found_down();
        loop {
            let installed = self.map.get();
            let down = probe.targets[probe.width..]
                .iter()
                .find(|(_, holder)| found_down || !Self::serving(&installed, *holder))
                .map(|(_, holder)| *holder);
            let Some(down) = down else {
                break;
            };
            // still down past the wait is a node that is down, not one called so for a moment
            if waited.elapsed() >= SWITCH_WAIT {
                let why = format!("arrival {down:?} is not serving");
                return self.abort(table, key, &pool, (&placement, &state), moving.id, &why, slot.get() == Slot::Held).await;
            }
            event!(Level::DEBUG, msg = "a move waits for its arrival before its switch", ?key, arrival = ?down);
            glommio::timer::sleep(SWITCH_POLL).await;
            found_down = false;
        }
        // the new generation and positions in one commit, naming the old holders it leaves for their
        // release (M20), then the slot freed
        let (left, _) = placement.moving_slices(pg, &state)?;
        self.placement(
            table,
            PlacementCommand::MoveSwitch {
                consumer,
                pg,
                id: moving.id,
                left,
            },
        )
        .await?;
        self.failures.borrow_mut().remove(&key);
        self.slotless.borrow_mut().remove(&(key, moving.id));
        self.holds.borrow_mut().remove(&(key, moving.generation));
        self.client.count_recovery(&pool, |figures| figures.moves_switched += 1);
        event!(Level::INFO, msg = "a placement group switched to its new generation", ?key, generation = moving.generation);
        // a slot freed, where the move held one
        if slot.get() == Slot::Held {
            self.done(&pool, consumer, pg).await
        } else {
            Ok(())
        }
    }

    /// Give a move up: the group back in its one generation, and its slot freed if it held one
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `pool` - The pool
    /// * `moving` - The pool's placement and the group's state, with the move in it
    /// * `id` - The move
    /// * `why` - Why, for the log
    /// * `held` - Whether the control group holds the move's slot
    #[allow(clippy::too_many_arguments)]
    async fn abort(
        &self,
        table: TableId,
        key: PgKey,
        pool: &str,
        (placement, state): (&Placement, &PgView),
        id: u64,
        why: &str,
        held: bool,
    ) -> Result<(), String> {
        let (consumer, pg) = key;
        event!(Level::WARN, msg = "a placement group's move was given up", ?key, why);
        // the arrivals it may have copied to, named for their release (M20); a group whose
        // placement cannot be read here names none, and a light scrub finds what they hold
        let left = placement
            .moving_slices(pg, state)
            .map(|(_, arrived)| arrived)
            .unwrap_or_default();
        self.placement(table, PlacementCommand::MoveAbort { consumer, pg, id, left })
            .await?;
        self.slotless.borrow_mut().remove(&(key, id));
        self.client.count_recovery(pool, |figures| figures.moves_aborted += 1);
        // a slot freed, where the move held one: a move with none frees nothing
        if held {
            self.done(pool, consumer, pg).await
        } else {
            Ok(())
        }
    }

    /// Free a placement group's slot
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    async fn done(&self, pool: &str, consumer: u64, pg: PlacementGroup) -> Result<(), String> {
        match self
            .control(ControlCommand::MoveDone {
                pool: pool.to_string(),
                consumer,
                pg,
            })
            .await?
        {
            ControlResponse::Applied { .. } => Ok(()),
            other => Err(format!("the move's slot was not freed: {other:?}")),
        }
    }

    /// Copy every stripe of a moving group onto its arrivals
    ///
    /// A move begun with no slot asks for one before its first byte moves: its group held nothing
    /// when it was listed, and what the walk and the listings after the begin find is what decides.
    /// One refused its slot is given up, for a later move to try again (item 250).
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `placement` - The pool's placement
    /// * `table` - The bucket's stripe table, and its pool
    /// * `key` - The placement group
    /// * `state` - Its state, with the move in it
    /// * `slot` - Whether the move holds a slot, which this sets once it takes one
    async fn copy(
        &self,
        meta: &Meta<D>,
        placement: &Placement,
        (table, pool): (TableId, &str),
        key: PgKey,
        state: &PgView,
        slot: &Cell<Slot>,
    ) -> Result<(), Short> {
        let installed = self.map.get();
        let (consumer, pg) = key;
        let geometry = placement.pool.geometry();
        // a chunk is its stripe over its data chunks: the stripe for copies (F90)
        let chunk_bytes = geometry.chunk_bytes();
        // the group's holders and arrivals, the same for every stripe of it
        let probe = placement.stripe_set_in(pg, 0, 0, state).map_err(Short::Retry)?;
        let width = probe.width;
        let moving: Vec<usize> = probe.targets[width..]
            .iter()
            .map(|(chunk, _)| usize::from(chunk.position))
            .collect();
        // every row of the group, by a walk of its range, applied past the begin
        let range = pg.range(placement.pool.groups_per_tablet());
        let mut walk = TabletWalk::<D>::new(self.shard_tx.clone(), WalkAsk::range(table, pg.tablet, range));
        let mut rows: BTreeMap<(u128, u64), StripeRow> = BTreeMap::new();
        let mut shed = 0u32;
        loop {
            match walk.next().await {
                Ok(Some(page)) => {
                    let found = page
                        .stripes::<D>()
                        .map_err(|error| Short::Retry(format!("{error:?}")))?;
                    for (_, row) in found.into_iter().filter(|(_, row)| row.consumer == consumer) {
                        rows.insert((row.object, row.stripe), row);
                    }
                }
                Ok(None) => break,
                // a shard reading as many pages as it reads at once is asked again shortly
                Err(refusal) if refusal.code == crate::shared::protocol::error::ErrorCode::Shedding && shed < 100 => {
                    shed += 1;
                    glommio::timer::sleep(Duration::from_millis(20)).await;
                }
                Err(refusal) => return Err(Short::Retry(format!("{refusal:?}"))),
            }
        }
        // every stripe a put wrote whole, by the old holders' listings
        let mut listed: BTreeMap<(u128, u64), BTreeMap<usize, (u64, u64)>> = BTreeMap::new();
        // whose stripe each is, as a chunk's header recorded it (M20)
        let mut owners: BTreeMap<(u128, u64), u64> = BTreeMap::new();
        let mut reached: BTreeSet<usize> = BTreeSet::new();
        for (position, (_, holder)) in probe.current().iter().enumerate() {
            if let Ok(chunks) = self.list(*holder, consumer, pg).await {
                reached.insert(position);
                for (chunk, label, length, stripe_length, owner) in chunks {
                    if owner != 0 {
                        owners.insert((chunk.object, chunk.stripe), owner);
                    }
                    // only a put's chunk: a staged one is a write the walk's rows decide
                    if label == Label::PUT {
                        listed
                            .entry((chunk.object, chunk.stripe))
                            .or_default()
                            .insert(usize::from(chunk.position), (length, stripe_length));
                    }
                }
            }
        }
        // a moving position needs another old holder's listing reached, or a stripe only it
        // listed could be missed
        for position in &moving {
            if !reached.iter().any(|other| other != position) {
                return Err(Short::Abort(format!(
                    "no old holder but position {position}'s was listed"
                )));
            }
        }
        // each stripe onto each arrival
        let stripes: BTreeSet<(u128, u64)> = rows.keys().chain(listed.keys()).copied().collect();
        for (object, stripe) in stripes {
            let set = placement
                .stripe_set_in(pg, object, stripe, state)
                .map_err(Short::Retry)?;
            let row = rows.get(&(object, stripe));
            // whose stripe this is: its row's word, or a chunk's header's
            let owner = row
                .map(|row| row.owner)
                .filter(|owner| *owner != 0)
                .or_else(|| owners.get(&(object, stripe)).copied())
                .unwrap_or(0);
            for (arrival_chunk, arrival) in &set.targets[set.width..] {
                let position = usize::from(arrival_chunk.position);
                // what the arrival takes, its stripe's length, and where from: any copy, or for an
                // erasure coded stripe its own position's chunk alone (F90)
                let (label, stripe_length, sources): (Label, u64, Vec<usize>) = match row {
                    Some(row) => {
                        // a reclaimed stripe and a stale position need nothing on the arrival
                        if row.tombstone || row.lost || !row.current(position) {
                            continue;
                        }
                        let sources = (0..width)
                            .filter(|source| row.current(*source))
                            .filter(|source| !geometry.is_erasure() || *source == position)
                            .collect();
                        (row.named(position), row.length, sources)
                    }
                    None => {
                        let found = &listed[&(object, stripe)];
                        // a copy's length is its stripe's; an erasure coded chunk records its
                        // stripe's, since its own is its share
                        let stripe_length = if geometry.is_erasure() {
                            found.values().map(|(_, stripe_length)| *stripe_length).max().unwrap_or(0)
                        } else {
                            found.values().map(|(length, _)| *length).max().unwrap_or(0)
                        };
                        let sources = found
                            .keys()
                            .copied()
                            .filter(|source| !geometry.is_erasure() || *source == position)
                            .collect();
                        (Label::PUT, stripe_length, sources)
                    }
                };
                // a position holding no bytes takes nothing
                let length = geometry.chunk_length(stripe_length, position).min(chunk_bytes);
                if length == 0 {
                    continue;
                }
                // a move with no slot takes one before a byte arrives, since its group held nothing
                // when it was listed and a stripe has landed since (item 250)
                if slot.get() == Slot::Unneeded {
                    let Some(moving) = &state.moving else {
                        return Err(Short::Retry("the group is not moving".to_string()));
                    };
                    self.holds.borrow_mut().insert((key, moving.generation), true);
                    let (_, arrived) = placement.moving_slices(pg, state).map_err(Short::Retry)?;
                    let devices = arrived.iter().map(|slice| slice.device).collect();
                    if let Err(why) = self.admit(pool, key, moving.generation, devices).await {
                        return Err(Short::Abort(format!("it found a stripe to copy and has no slot to copy it under: {why}")));
                    }
                    slot.set(Slot::Held);
                    self.slotless.borrow_mut().remove(&(key, moving.id));
                    event!(Level::INFO, msg = "a move begun with no slot found a stripe to copy, and took a slot", ?key, object = %format!("{object:032x}"), stripe);
                }
                let mut why = Vec::new();
                let mut copied = false;
                for source in sources {
                    let (from_chunk, from) = set.current()[source];
                    let from_label = row.map_or(Label::PUT, |row| row.named(source));
                    match copy_whole(
                        &self.store,
                        length,
                        (from_chunk, from, from_label),
                        (*arrival_chunk, *arrival),
                        label,
                        chunk_bytes,
                        geometry.unit_bytes,
                        stripe_length,
                        owner,
                    )
                    .await
                    {
                        Ok(()) => {
                            copied = true;
                            break;
                        }
                        Err(error) => why.push(error.to_string()),
                    }
                }
                // an erasure coded chunk whose old holder gave none is decoded from k others
                if !copied && geometry.is_erasure() && stripe_length > 0 {
                    let decoded_row = row.cloned().unwrap_or_else(|| {
                        StripeRow::put(consumer, object, stripe, width, stripe_length, owner)
                    });
                    match super::recover::decode_whole(
                        &self.store,
                        set.current(),
                        &decoded_row,
                        &geometry,
                        position,
                        (*arrival_chunk, *arrival),
                        label,
                    )
                    .await
                    {
                        Ok(_) => copied = true,
                        Err(failed) => why.push(failed.why),
                    }
                }
                if copied {
                    continue;
                }
                // an arrival already past the label holds a write the move staged on it
                let held = self
                    .store
                    .ask_holder(*arrival, background(ChunkRequest::Confirm { chunk: *arrival_chunk, labels: vec![label] }))
                    .await;
                if matches!(held, ChunkReply::Held { label: Some(have) } if have.sequence >= label.sequence) {
                    continue;
                }
                // nothing gave its bytes: marked missed if its old slice is gone, else given up
                let old = set.current()[position].1;
                if Self::serving(&installed, old) {
                    return Err(Short::Abort(format!(
                        "stripe {stripe} of {object:032x} could not be copied to position {position}: {}",
                        why.join("; ")
                    )));
                }
                self.mark(meta, row, (consumer, object, stripe), width, (stripe_length, owner), position, state.epoch)
                    .await?;
            }
        }
        Ok(())
    }

    /// Every chunk of a placement group one old holder lists, in pages
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    async fn list(&self, holder: Holder, consumer: u64, pg: PlacementGroup) -> Result<Vec<(ChunkId, Label, u64, u64, u64)>, String> {
        // a chunk set aside for failing its checksum is evidence, never a chunk a move copies
        // (F89)
        let mut after = ChunkId {
            consumer,
            pg,
            object: 0,
            stripe: 0,
            position: 0,
        };
        let mut start = true;
        let mut found = Vec::new();
        loop {
            let request = ChunkRequest::List {
                after,
                start,
                limit: MAX_LISTED,
            };
            match self.store.ask_holder(holder, background(request)).await {
                ChunkReply::Listed { chunks, more } => {
                    if let Some(last) = chunks.last() {
                        after = last.chunk;
                        start = false;
                    }
                    found.extend(
                        chunks
                            .into_iter()
                            .filter(|listed| !listed.quarantined)
                            .map(|listed| (listed.chunk, listed.label, listed.length, listed.stripe_length, listed.owner)),
                    );
                    if !more {
                        return Ok(found);
                    }
                }
                other => return Err(format!("{holder:?} did not list {pg:?}: {other:?}")),
            }
        }
    }

    /// Mark a stripe missed at a moving position whose old slice is gone, claiming the epoch read
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe's consumer, object and index
    /// * `width` - Its positions
    /// * `length` - How much of it holds bytes, and the key the bucket records the object under
    /// * `position` - The position
    /// * `epoch` - The group's epoch
    #[allow(clippy::too_many_arguments)]
    async fn mark(
        &self,
        meta: &Meta<D>,
        row: Option<&StripeRow>,
        stripe: (u64, u128, u64),
        width: usize,
        (length, owner): (u64, u64),
        position: usize,
        epoch: u64,
    ) -> Result<(), Short> {
        let (consumer, object, index) = stripe;
        let (next, read) = match row {
            Some(row) => (row.rebuilt(position, false).owned_by(owner), Some(row.sequence)),
            None => {
                let mut made = StripeRow::put(consumer, object, index, width, length, owner);
                if let Some(label) = made.labels.get_mut(position) {
                    *label = label.marked();
                }
                (made, None)
            }
        };
        match meta.commit_stripe(next.claiming(epoch), read, None).await {
            StripeAnswer::Written => Ok(()),
            StripeAnswer::Refused(ConditionRefusal::Placement) => {
                Err(Short::Retry("the group moved while a stripe was marked".to_string()))
            }
            StripeAnswer::Refused(_) => Err(Short::Retry("the stripe's row moved while it was marked".to_string())),
            other => Err(Short::Retry(format!("marking the stripe: {other:?}"))),
        }
    }
}

/// Move the placement groups of the stripe groups this shard leads to their pools' targets, for as
/// long as the shard runs
///
/// # Arguments
///
/// * `client` - The shard's metadata client
/// * `shard_tx` - The shard's own channel
/// * `map` - The shard's map
/// * `store` - The node's object store, once started
/// * `control` - The node's control thread
/// * `every` - How often the groups are looked at
/// * `inflight` - How many moves run at once
/// * `wait` - How long one metadata query may take
/// * `release` - How long a slice a group left keeps what it held, none when the node does not
///   reclaim
#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll<D: ShoalDatabase>(
    client: MetaClient,
    shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    map: MapCell,
    store: Arc<OnceLock<ObjectStore>>,
    control: kanal::Sender<ControlRequest>,
    every: Duration,
    inflight: usize,
    wait: Duration,
    release: Option<Duration>,
) where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    // the groups being moved, so a poll starts none twice
    let busy: Rc<RefCell<HashSet<PgKey>>> = Rc::new(RefCell::new(HashSet::new()));
    // what runs at once, apart for the moves of groups not known to hold anything, which move no
    // byte and so are held back by no copy (item 250)
    let running = Rc::new(Cell::new(0usize));
    let running_light = Rc::new(Cell::new(0usize));
    let mut mover: Option<Rc<Mover<D>>> = None;
    // a poll that found work looks again soon, so a move follows the one before at once
    let mut pause = every;
    loop {
        glommio::timer::sleep(pause).await;
        pause = every;
        // a test may pause the drivers to do their work by hand
        if crate::server::objects::faults::recovery_paused() {
            continue;
        }
        // nothing moves before the cluster speaks the recovery wire
        if map.get().activated_wire < RECOVERY_WIRE_VERSION {
            continue;
        }
        let mover = match &mover {
            Some(mover) => mover.clone(),
            None => {
                let Some(store) = store.get().cloned() else {
                    continue;
                };
                let made = Rc::new(Mover {
                    client: client.clone(),
                    shard_tx: shard_tx.clone(),
                    map: map.clone(),
                    store,
                    control: control.clone(),
                    wait,
                    failures: RefCell::new(HashMap::new()),
                    full: RefCell::new(HashMap::new()),
                    release,
                    left_seen: RefCell::new(HashMap::new()),
                    holds: RefCell::new(HashMap::new()),
                    slotless: RefCell::new(HashSet::new()),
                });
                mover = Some(made.clone());
                made
            }
        };
        // the stripe groups this shard leads
        let (reply, answer) = futures_channel::oneshot::channel();
        if shard_tx.send(ServerMsg::LedStripes { reply }).await.is_err() {
            return;
        }
        let Ok(led) = answer.await else {
            continue;
        };
        // the slots as the control thread holds them, which no pushed map carries
        let Ok(slots) = mover.slots().await else {
            continue;
        };
        let mut work = mover.work(&led, &slots);
        if !work.is_empty() {
            pause = every.min(Duration::from_millis(20));
        }
        // what is under way first, then the moves of groups not known to hold anything, those still
        // placing on a slice that serves nothing ahead, since a put into one stages there until it
        // moves, and releases last (item 250); the sort keeps each kind's order
        let installed = map.get();
        work.sort_by_key(|work| mover.order(&installed, work));
        // the moves arriving on each device now, so a begin a full device would refuse is never
        // proposed: a refused admission is still a commit of the control group, and every shard
        // asking every poll for a full device's slots kept its detector from hearing members
        // (Resolved #243)
        let mut arriving: HashMap<crate::shared::devices::DeviceId, usize> = HashMap::new();
        for slot in &slots {
            for device in &slot.devices {
                *arriving.entry(*device).or_default() += 1;
            }
        }
        for work in work {
            // a move of a group not known to hold anything waits for no device's arrivals, and runs
            // within a budget of its own, so the copies under way never hold it back (item 250)
            let light = mover.light(&work);
            let budget = if light { &running_light } else { &running };
            if budget.get() >= inflight.max(1) {
                continue;
            }
            let key = match &work {
                Work::Begin { key, .. }
                | Work::Resume { key, .. }
                | Work::Free { key, .. }
                | Work::Release { key, .. } => *key,
            };
            if busy.borrow().contains(&key) {
                continue;
            }
            if let (false, Work::Begin { old, new, .. }) = (light, &work) {
                let devices = arrivals(old, new);
                let refused_lately = |device: &crate::shared::devices::DeviceId| {
                    mover.full.borrow().get(device).is_some_and(|at| at.elapsed() < FULL_FOR)
                };
                if devices.iter().any(|device| {
                    arriving.get(device).copied().unwrap_or(0) >= MOVES_A_DEVICE || refused_lately(device)
                }) {
                    continue;
                }
                for device in devices {
                    *arriving.entry(device).or_default() += 1;
                }
            }
            if !busy.borrow_mut().insert(key) {
                continue;
            }
            budget.set(budget.get() + 1);
            let mover = mover.clone();
            let busy = busy.clone();
            let budget = budget.clone();
            glommio::spawn_local(async move {
                let outcome = match work {
                    // a move counted against its devices' arrivals may be admitted at once; one that
                    // was not, found holding something, is admitted at the next poll
                    Work::Begin {
                        table,
                        key,
                        state,
                        old,
                        new,
                        target,
                    } => mover.begin(table, key, state, old, new, target, !light).await,
                    Work::Resume { table, key, state, slot } => mover.resume(table, key, state, slot).await,
                    Work::Free { pool, key } => mover.done(&pool, key.0, key.1).await,
                    Work::Release { table, key, slice } => mover.release(table, key, slice).await,
                };
                if let Err(error) = outcome {
                    event!(Level::DEBUG, msg = "a move did not finish; the next poll tries again", ?key, error);
                }
                busy.borrow_mut().remove(&key);
                budget.set(budget.get() - 1);
            })
            .detach();
        }
    }
}
