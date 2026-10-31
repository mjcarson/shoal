//! Scrubs: every placement group of the stripe groups a shard leads, read on purpose
//!
//! Every shard runs one task that reads the scrub cursors of each bucket stripe group it leads and
//! drives a pass when one is due ([S11](../../../../../docs/src/object-storage/scrub.md),
//! [F89](../../../../../docs/src/features/scrub-and-repair.md)):
//!
//! 1. a pass begins with a committed `ScrubBegin`, one past the group's cursor, when its pool's
//!    schedule says so, or at once for an operator's request;
//! 2. each placement group, in key order from the cursor: its rows are walked, and each position's
//!    holder lists what it holds of the group. A **light** pass compares the two: a holder behind
//!    the row or without its chunk is stale, a chunk set aside is corrupt, and a chunk at a label
//!    no row state explains is reported. A **deep** pass then verifies every unit of every chunk
//!    judged present where it lies, a piece at a time, and compares the copies' checksum tables;
//! 3. what is stale or corrupt is marked missed in its row, claiming the epoch read, and recovery
//!    rebuilds it from copies that verify. Copies that each verify and disagree stop with their
//!    evidence, and nothing rebuilds them by a rule;
//! 4. each placement group finished is committed as a `ScrubStep` from where the cursor was read,
//!    and the last as a `ScrubEnd`.
//!
//! Every request a scrub makes of a slice is a scrub's: served only while the slice has no
//! foreground request and no rebuild's or move's in flight, one at a time, under its device's
//! ceiling, which recovery draws on too.
//!
//! # Invariants
//!
//! **A scrub never mends a chunk itself.** It marks a position missed, and the rebuild path makes
//! it current again from copies that verify under the row's labels.
//!
//! **A stale chunk is never counted corrupt.** A holder behind the row, or without the chunk, is
//! stale; only a chunk set aside for failing its own checksum is corrupt.
//!
//! **A pass's cursor moves only by a committed step from where it was read**, so no placement
//! group is skipped and a new leader resumes where the last one stopped.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::{event, Level};

use super::place::Placement;
use super::recover::holder_serving;
use super::{Marking, Meta, MetaClient, DEFAULT_LEVEL};
use crate::server::database::ShoalDatabase;
use crate::server::map::{MapCell, TabletMap};
use crate::server::messages::ServerMsg;
use crate::server::objects::put::PIECE_BYTES;
use crate::server::objects::{Holder, ObjectStore};
use crate::server::shard::walk::{TabletWalk, WalkAsk};
use crate::server::shard::ScrubGroup;
use crate::shared::identity::{BucketId, GroupId, TableId};
use crate::shared::objects::Label;
use crate::shared::placement_state::{PgKey, PlacementCommand};
use crate::shared::poolmap::PlacementGroup;
use crate::shared::pools::PoolSpec;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, ListedChunk, SealScope};
use crate::shared::protocol::SCRUB_WIRE_VERSION;
use crate::shared::scrub::{due, stagger_ms, Due, ScrubCursor, ScrubFigures, ScrubKind};
use crate::shared::stripes::{ChunkId, StripeRow};
use crate::shared::traits::QuerySupport;

/// The most chunks one scrub's listing asks a holder for: a megabyte of headers under its ceiling
const LIST_PAGE: u32 = 256;

/// The time now, in milliseconds since the Unix epoch, by this node's clock
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

/// One stripe of a placement group as a pass finds it: its row, and what each holder lists
#[derive(Debug, Default)]
struct Found {
    /// Its row, if it has one
    row: Option<StripeRow>,
    /// What each position's holder listed of it
    listed: BTreeMap<usize, ListedChunk>,
}

/// What one position of a stripe came to under a deep scrub
#[derive(Debug)]
enum Checked {
    /// Every unit verified, with each unit's checksum of its bytes, and their summary when the
    /// parity check asked for one (F90)
    Verified(Vec<u64>, Option<Vec<u8>>),
    /// A unit or its header failed, and its holder set the chunk aside
    Corrupt,
    /// Its holder is not at the label
    Stale,
    /// Records make the label and are not applied yet
    Pending,
    /// The holder could not be asked or refused, and why
    Failed(String),
}

/// What an object's consumer says of an object a stripe with no row names, read strongly (M20)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Named {
    /// Its entry names it current or in flight: its chunks are judged as F89 judged them
    Current,
    /// Its entry names it retired: the reclaimer's, after its grace, and nothing a scrub marks
    Retired,
    /// No entry names it: what a reclamation left on a holder it did not reach
    Nowhere,
}

/// What every pass of one shard's task holds
struct Scrubber<D: ShoalDatabase> {
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
    /// How long a put's stripe short of a copy is given to finish landing before it is marked
    put_grace: Duration,
    /// When this leader first saw each group, by its own clock, which a first pass staggers from
    first_seen: RefCell<HashMap<GroupId, u64>>,
    /// Each position of a put's stripe found short of its chunk, and when first: marked once a
    /// later pass finds it short past the grace
    short: RefCell<HashMap<(u64, u128, u64, usize), Instant>>,
    /// What each object's consumer said of it, by its owner key and id, for one placement group's
    /// judging, so an object is asked about once a group (M20)
    named: RefCell<HashMap<(u64, u128), Named>>,
}

impl<D: ShoalDatabase> Scrubber<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// The consumer, pool name and pool of the bucket whose stripe table a group serves
    ///
    /// # Arguments
    ///
    /// * `installed` - The map
    /// * `table` - The group's table
    fn binding_of(installed: &TabletMap, table: TableId) -> Option<(u64, String, PoolSpec)> {
        installed.objects.bindings.iter().find_map(|(name, binding)| {
            let ops = <D::ClientType as QuerySupport>::object_queries(BucketId::of(name))?;
            if TableId::of(ops.stripe_table()) != table {
                return None;
            }
            let pool = installed.objects.pools.get(&binding.pool)?;
            Some((binding.consumer, binding.pool.clone(), pool.clone()))
        })
    }

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
            span: tracing::info_span!("Object::scrub", consumer),
            wait: self.wait,
        };
        Ok((meta, placement))
    }

    /// Propose a scrub's command through the group this shard leads
    ///
    /// # Arguments
    ///
    /// * `table` - The bucket's stripe table
    /// * `command` - The command
    async fn propose(&self, table: TableId, command: PlacementCommand) -> Result<(), String> {
        let (reply, answer) = futures_channel::oneshot::channel();
        self.shard_tx
            .send(ServerMsg::PlacementPropose { table, command, reply })
            .await
            .map_err(|_| "the shard stopped".to_string())?;
        answer
            .await
            .map_err(|_| "the scrub's commit was dropped".to_string())?
    }

    /// Drive one kind of scrub's pass over a group, from where its cursor is, until it ends, its
    /// window shuts, or another driver moved its cursor
    ///
    /// # Arguments
    ///
    /// * `group` - The group, as its cursors were read
    /// * `consumer` - The consumer of the bucket it serves
    /// * `pool_name` - The bucket's pool
    /// * `kind` - The kind of scrub
    /// * `due` - What its schedule said: begin a pass, or go on with one
    async fn pass(
        &self,
        group: ScrubGroup,
        consumer: u64,
        pool_name: String,
        kind: ScrubKind,
        due: Due,
        target: Option<crate::shared::scrub::ScrubTarget>,
    ) -> Result<(), String> {
        let installed = self.map.get();
        let (meta, placement) = self.bucket(&installed, consumer)?;
        // an operator's scrub narrowed to another bucket is this group's to end at once (F93)
        let others = match target {
            Some(crate::shared::scrub::ScrubTarget::Bucket(bucket)) => installed
                .objects
                .bindings
                .iter()
                .find(|(name, _)| crate::shared::identity::BucketId::of(name) == bucket)
                .is_none_or(|(_, binding)| binding.consumer != consumer),
            _ => false,
        };
        let groups_per = placement.pool.groups_per_tablet().max(1);
        // every placement group of the tablet group, in key order
        let mut keys: Vec<PgKey> = group
            .tablets
            .iter()
            .flat_map(|tablet| {
                (0..groups_per).map(move |sub| {
                    (
                        consumer,
                        PlacementGroup {
                            tablet: *tablet,
                            sub: u16::try_from(sub).unwrap_or(u16::MAX),
                        },
                    )
                })
            })
            .collect();
        keys.sort_unstable();
        let Some(first) = keys.first().copied() else {
            return Ok(());
        };
        let cursor: ScrubCursor = *group.scrub.cursor(kind);
        // a pass narrowed to less than this group begins and ends at the time its last pass
        // finished, so it moves neither the group's next scheduled pass nor its last scrubbed
        // time: it scrubbed only what it names. One narrowed to the group's own bucket is all of
        // it (item 266)
        let partial = match target {
            None => false,
            Some(crate::shared::scrub::ScrubTarget::Bucket(_)) => others,
            Some(_) => true,
        };
        let clock = |now: u64| if partial { cursor.last_ms } else { now };
        // a pass begun now, or the one the cursor is in
        let (pass, mut after, asked) = match due {
            Due::Wait => return Ok(()),
            Due::Begin { request, asked } => {
                let pass = cursor.pass + 1;
                self.propose(
                    group.table,
                    PlacementCommand::ScrubBegin {
                        consumer,
                        pg: first.1,
                        kind,
                        pass,
                        request,
                        asked,
                        began_ms: clock(now_ms()),
                    },
                )
                .await?;
                event!(Level::INFO, msg = "a scrub pass began", group = %group.group, kind = kind.as_str(), pass, request, asked);
                (pass, None, asked)
            }
            Due::Continue => (cursor.pass, cursor.after, cursor.asked),
        };
        let policy = placement.pool.scrub_policy();
        // the placement groups past the cursor, in order, those the pass is narrowed to alone
        let mut remaining: Vec<PgKey> = Vec::with_capacity(keys.len());
        for key in keys.iter().copied().filter(|key| after.is_none_or(|after| *key > after)) {
            if others {
                continue;
            }
            // a pass narrowed to a device or a slice scrubs the groups with a position on it
            let narrowed = match target {
                Some(crate::shared::scrub::ScrubTarget::Device(device)) => Some((device, None)),
                Some(crate::shared::scrub::ScrubTarget::Slice(slice)) => Some((slice.device, Some(slice.index))),
                _ => None,
            };
            if let Some((device, index)) = narrowed {
                let view = meta.placement_for(&placement, key).await;
                let on = placement.slices_of(key.1, &view).is_ok_and(|slices| {
                    slices
                        .iter()
                        .any(|slice| slice.device == device && index.is_none_or(|index| slice.index == index))
                });
                if !on {
                    continue;
                }
            }
            remaining.push(key);
        }
        for key in remaining {
            // a scheduled pass waits for its window, and a paused one for its test
            if (!asked && !policy.in_window(now_ms())) || crate::server::objects::faults::scrub_paused() {
                return Ok(());
            }
            // a moving placement group is scrubbed once its move switches or aborts
            if group.moving.contains(&key) {
                return Ok(());
            }
            // an erasure coded pool's deep pass checks a rotating sample of its groups' parity (F90)
            let parity = kind == ScrubKind::Deep
                && placement.pool.geometry().is_erasure()
                && policy.checks_parity(pass, key.1.tablet, key.1.sub);
            let found = self
                .scrub_group(&meta, &placement, group.table, key, kind == ScrubKind::Deep, parity)
                .await?;
            // the cursor past it, from where it was read: refused if another driver moved it
            self.propose(
                group.table,
                PlacementCommand::ScrubStep {
                    consumer: key.0,
                    pg: key.1,
                    kind,
                    pass,
                    after,
                },
            )
            .await?;
            after = Some(key);
            self.client.count_scrub(&pool_name, |figures| {
                figures.scrubbed += 1;
                figures.absorb(&found);
            });
            // a test can hold a pass between its placement groups
            if let Some(hold) = crate::server::objects::faults::scrub_hold() {
                glommio::timer::sleep(hold).await;
            }
        }
        self.propose(
            group.table,
            PlacementCommand::ScrubEnd {
                consumer,
                pg: first.1,
                kind,
                pass,
                finished_ms: clock(now_ms()),
            },
        )
        .await?;
        self.client.count_scrub(&pool_name, |figures| match kind {
            ScrubKind::Light => figures.light_passes += 1,
            ScrubKind::Deep => figures.deep_passes += 1,
        });
        event!(Level::INFO, msg = "a scrub pass finished", group = %group.group, kind = kind.as_str(), pass, groups = keys.len());
        Ok(())
    }

    /// Scrub one placement group: its rows against what each holder lists, and for a deep scrub
    /// every chunk judged present verified where it lies; what is stale or corrupt is marked
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `placement` - The pool's placement
    /// * `table` - The bucket's stripe table
    /// * `key` - The placement group
    /// * `deep` - Whether every chunk's units are verified too
    /// * `parity` - Whether each erasure coded stripe's parity is checked by summaries (F90)
    #[allow(clippy::too_many_arguments)]
    async fn scrub_group(
        &self,
        meta: &Meta<D>,
        placement: &Placement,
        table: TableId,
        key: PgKey,
        deep: bool,
        parity: bool,
    ) -> Result<ScrubFigures, String> {
        let installed = self.map.get();
        let (consumer, pg) = key;
        // the group's state as of now; one that is moving is scrubbed once it is not
        let view = meta
            .read_placements(&[key])
            .await
            .map_err(meta_error)?
            .remove(&key)
            .unwrap_or_default();
        if view.moving.is_some() {
            return Err(format!("{pg:?} is moving"));
        }
        let set = placement.stripe_set_in(pg, 0, 0, &view)?;
        let width = set.width;
        let holders: Vec<Holder> = set.current().iter().map(|(_, holder)| *holder).collect();
        // every row of the group, by a walk of its range
        let mut found: BTreeMap<(u128, u64), Found> = BTreeMap::new();
        let range = pg.range(placement.pool.groups_per_tablet());
        let mut walk = TabletWalk::<D>::new(self.shard_tx.clone(), WalkAsk::range(table, pg.tablet, range));
        loop {
            // a shed page is asked again by the walk itself (Resolved #345)
            match walk.next().await {
                Ok(Some(page)) => {
                    let rows = page.stripes::<D>().map_err(|error| format!("{error:?}"))?;
                    for (_, row) in rows.into_iter().filter(|(_, row)| row.consumer == consumer) {
                        let key = (row.object, row.stripe);
                        found.entry(key).or_default().row = Some(row);
                    }
                }
                Ok(None) => break,
                Err(refusal) => return Err(format!("{refusal:?}")),
            }
        }
        // what each serving holder lists of the group
        let mut reached = vec![false; width];
        for (position, holder) in holders.iter().enumerate().take(width) {
            if !holder_serving(&installed, *holder) {
                continue;
            }
            match self.list(*holder, consumer, pg).await {
                Ok(chunks) => {
                    reached[position] = true;
                    for listed in chunks {
                        found
                            .entry((listed.chunk.object, listed.chunk.stripe))
                            .or_default()
                            .listed
                            .insert(position, listed);
                    }
                }
                Err(error) => {
                    event!(Level::DEBUG, msg = "a holder did not list a placement group for a scrub", ?pg, position, error);
                }
            }
        }
        // each stripe judged, and what it found counted; the consumer's answers last the group
        self.named.borrow_mut().clear();
        let mut figures = ScrubFigures::default();
        // a stripe a query was refused for now for: the group is not done, so its step is not
        // committed and the next poll scrubs it again from the cursor (item 330)
        let mut for_now = None;
        for ((object, stripe), entry) in found {
            if let Err(error) = self
                .judge(meta, placement, &set, (consumer, object, stripe), entry, &reached, view.epoch, (deep, parity), &mut figures)
                .await
            {
                event!(Level::DEBUG, msg = "a scrub could not judge a stripe", object = %format!("{object:032x}"), stripe, error);
                if error.starts_with(FOR_NOW) && for_now.is_none() {
                    for_now = Some(error);
                }
            }
        }
        if let Some(error) = for_now {
            return Err(format!("placement group {pg:?} is scrubbed again: {error}"));
        }
        Ok(figures)
    }

    /// Judge one stripe: each position its row calls current, or that holds a put's chunk, against
    /// what its holder listed and, for a deep scrub, what its units verify to; mark what is stale
    /// or corrupt, and report what nothing explains or what verified and disagreed
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `placement` - The pool's placement
    /// * `set` - The placement group's holders
    /// * `stripe` - The stripe's consumer, object and index
    /// * `entry` - Its row and what each holder listed
    /// * `reached` - Which positions' holders were listed
    /// * `epoch` - The placement group's epoch read
    /// * `deep` - Whether its chunks' units are verified, and whether an erasure coded stripe's
    ///   parity is checked by summaries (F90)
    /// * `figures` - What the scrub found, counted
    #[allow(clippy::too_many_arguments)]
    async fn judge(
        &self,
        meta: &Meta<D>,
        placement: &Placement,
        set: &super::place::StripeSet,
        stripe: (u64, u128, u64),
        entry: Found,
        reached: &[bool],
        epoch: u64,
        (deep, parity): (bool, bool),
        figures: &mut ScrubFigures,
    ) -> Result<(), String> {
        let (consumer, object, index) = stripe;
        let width = set.width;
        let geometry = placement.pool.geometry();
        // whose stripe this is, as its row or any chunk's header recorded it (M20)
        let owner = entry
            .row
            .as_ref()
            .map(|row| row.owner)
            .filter(|owner| *owner != 0)
            .or_else(|| entry.listed.values().map(|listed| listed.owner).find(|owner| *owner != 0))
            .unwrap_or(0);
        let erasure = geometry.is_erasure();
        // a stripe with no row: its length is any copy's, or what an erasure coded chunk's header
        // recorded of its stripe, since its own length is its share (F90)
        let rowless_length = if erasure {
            entry.listed.values().map(|listed| listed.stripe_length).max().unwrap_or(0)
        } else {
            entry.listed.values().map(|listed| listed.length).max().unwrap_or(0)
        };
        // a position holds a chunk unless its share of the stripe is no bytes
        let holds = |length: u64, position: usize| geometry.chunk_length(length, position) > 0;
        // a stripe an operator declared lost is nobody's to judge again: its chunks go with its
        // object, and nothing of it is marked or rebuilt (F93)
        if entry.row.as_ref().is_some_and(|row| row.lost) {
            return Ok(());
        }
        // a reclaimed stripe's chunks a holder kept, because the reclamation did not reach it, are
        // discarded through the tombstone's sequence: no label at or below it is ever written again
        // (M20, X1's `Reclaim`)
        if let Some(row) = entry.row.as_ref().filter(|row| row.tombstone) {
            let kept: Vec<(usize, &ListedChunk)> = entry
                .listed
                .iter()
                .filter(|(_, listed)| !listed.staged)
                .map(|(position, listed)| (*position, listed))
                .collect();
            figures.reclaimable += kept.len() as u64;
            let discards = kept.iter().map(|(position, listed)| {
                let (_, holder) = set.current()[*position];
                self.store.ask_holder(
                    holder,
                    ChunkRequest::DiscardThrough {
                        chunk: listed.chunk,
                        through: row.sequence,
                    },
                )
            });
            let done = futures::future::join_all(discards)
                .await
                .iter()
                .filter(|answer| **answer == ChunkReply::Done)
                .count() as u64;
            if done > 0 {
                self.client.count_reclaim(&placement.pool_name, |figures| {
                    figures.unexplained += done;
                    figures.chunks += done;
                });
            }
            return Ok(());
        }
        // a stripe with no row whose object its consumer names nowhere is what a reclamation left
        // on a holder it did not reach; one it names retired is the reclaimer's after its grace,
        // and neither is unexplained nor short (M20)
        if entry.row.is_none() && owner != 0 && entry.listed.values().any(|listed| !listed.staged) {
            match self.named(meta, owner, object).await? {
                Named::Current => {}
                Named::Retired => return Ok(()),
                Named::Nowhere => {
                    let removed = self.reclaim_orphan(meta, set, (consumer, object), owner, &entry).await?;
                    if removed > 0 {
                        self.client.count_reclaim(&placement.pool_name, |figures| {
                            figures.unexplained += removed;
                            figures.chunks += removed;
                        });
                        event!(Level::INFO, msg = "a scrub removed chunks of an object its consumer names nowhere", object = %format!("{object:032x}"), stripe = index, chunks = removed);
                    }
                    return Ok(());
                }
            }
        }
        // a stripe with no row is a put's, every position at its label, when some holder has it
        let row = match entry.row {
            Some(row) => Some(row),
            None => {
                // a placed chunk at another label with no row is explained by no row state
                let unexplained = entry
                    .listed
                    .values()
                    .filter(|listed| !listed.staged && !listed.quarantined && listed.label != Label::PUT)
                    .count() as u64;
                // a reached holder without a put's chunk in place, which a pass would mark; an
                // erasure coded position holding no bytes holds no chunk
                let short = (0..width).any(|position| {
                    reached.get(position).copied().unwrap_or(false)
                        && holds(rowless_length, position)
                        && !entry
                            .listed
                            .get(&position)
                            .is_some_and(|listed| listed.label == Label::PUT && !listed.quarantined && !listed.staged)
                });
                // a write in place may have made its row since the walk read the group: before
                // anything is reported or marked, the row as it is now, and a later pass if one is
                if unexplained > 0 || short {
                    let rows = meta
                        .stripe_rows(consumer, object, &[index])
                        .await
                        .map_err(meta_error)?;
                    if rows.contains_key(&index) {
                        return Ok(());
                    }
                }
                // a placed chunk past its put's label with still no row, read strongly, holds a
                // write the row is behind: only a `force_recover` that lost a write leaves one.
                // Nothing of the stripe is marked or rebuilt over it; it is kept for an
                // operator's `ResolveStripe` (F93, item 256)
                if unexplained > 0 {
                    figures.behind += 1;
                    event!(Level::WARN, msg = "a scrub found a stripe's chunks ahead of its row; they are kept for ResolveStripe", object = %format!("{object:032x}"), stripe = index, chunks = unexplained);
                    return Ok(());
                }
                if !entry.listed.values().any(|listed| listed.label == Label::PUT || listed.quarantined) {
                    return Ok(());
                }
                None
            }
        };
        let length = row.as_ref().map_or(rowless_length, |row| row.length);
        // an erasure coded chunk with no row and no stripe recorded cannot be judged
        if erasure && length == 0 {
            return Ok(());
        }
        // what each position should hold, and what its holder listed
        let mut corrupt: Vec<usize> = Vec::new();
        let mut suspects: Vec<usize> = Vec::new();
        // each present copy by the chunk its holder listed: the probe set names object zero
        let mut present: Vec<(usize, Label)> = Vec::new();
        for position in (0..width).filter(|position| reached.get(*position).copied().unwrap_or(false)) {
            // a position whose share of the stripe is no bytes expects nothing; what its holder
            // lists is a chunk a cut left, for reclamation (F90)
            if !holds(length, position) {
                if entry.listed.get(&position).is_some_and(|listed| !listed.staged && !listed.quarantined) {
                    figures.reclaimable += 1;
                }
                continue;
            }
            let expected = match &row {
                // a position the row marks is recovery's
                Some(row) if !row.current(position) => continue,
                Some(row) => row.named(position),
                None => Label::PUT,
            };
            match entry.listed.get(&position) {
                Some(listed) if listed.quarantined => corrupt.push(position),
                // a write still landing is the write's to decide
                Some(listed) if listed.staged => {}
                Some(listed) if listed.label == expected => present.push((position, expected)),
                // a holder at the base of the row's pending bytes holds its state with them, and is
                // verified at the label it is at
                Some(listed) if row.as_ref().is_some_and(|row| row.makes(position, listed.label)) => {
                    present.push((position, listed.label));
                }
                // behind the row, ahead of it, or without the chunk: judged on the row as it is now
                _ => suspects.push(position),
            }
        }
        // a deep scrub verifies every chunk present, every position at once, a piece at a time
        if deep && !present.is_empty() {
            // each position's units that hold bytes, its share of the stripe
            let unit = u64::from(geometry.unit_bytes);
            let end_of = |position: usize| geometry.chunk_length(length, position).div_ceil(unit) * unit;
            let checks = present.iter().map(|(position, label)| {
                let (_, holder) = set.current()[*position];
                let chunk = entry.listed[position].chunk;
                self.verify_chunk(chunk, holder, *label, end_of(*position), parity && erasure)
            });
            let checked = futures::future::join_all(checks).await;
            let mut tables: Vec<(usize, Label, Vec<u64>)> = Vec::new();
            let mut summaries: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
            for ((position, label), outcome) in present.iter().zip(checked) {
                match outcome {
                    Checked::Verified(crcs, summary) => {
                        figures.chunks += 1;
                        figures.bytes += end_of(*position);
                        tables.push((*position, *label, crcs));
                        if let Some(summary) = summary {
                            summaries.insert(*position, summary);
                        }
                    }
                    Checked::Corrupt => corrupt.push(*position),
                    Checked::Stale => suspects.push(*position),
                    Checked::Pending => {}
                    Checked::Failed(why) => {
                        event!(Level::DEBUG, msg = "a holder could not verify a chunk for a scrub", position, why);
                    }
                }
            }
            // an erasure coded stripe's parity against its data, by summaries, once every chunk
            // that holds bytes verified at the label its row names (F90)
            if parity && erasure {
                let holding: Vec<usize> = (0..width).filter(|position| holds(length, *position)).collect();
                if holding.iter().all(|position| summaries.contains_key(position)) {
                    self.check_parity(placement, (object, index), &geometry, &summaries, figures).await;
                }
            }
            // the copies' checksum tables, which every copy of a replicated stripe at one label
            // shares; an erasure coded stripe's chunks are each their own (F90)
            let mut labels: Vec<Label> = Vec::new();
            for (_, label, _) in tables.iter().filter(|_| !erasure) {
                if !labels.contains(label) {
                    labels.push(*label);
                }
            }
            for label in labels {
                let copies: Vec<&(usize, Label, Vec<u64>)> = tables.iter().filter(|(_, at, _)| *at == label).collect();
                let Some((first, _, reference)) = copies.first().copied() else {
                    continue;
                };
                let differing: Vec<usize> = copies
                    .iter()
                    .filter(|(_, _, table)| table != reference)
                    .map(|(position, _, _)| *position)
                    .collect();
                if !differing.is_empty() {
                    figures.disagreed += 1;
                    let units: Vec<usize> = copies
                        .iter()
                        .filter(|(position, _, _)| differing.contains(position))
                        .flat_map(|(_, _, table)| {
                            table
                                .iter()
                                .zip(reference)
                                .enumerate()
                                .filter(|(_, (a, b))| a != b)
                                .map(|(unit, _)| unit)
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    event!(
                        Level::ERROR,
                        msg = "a stripe's copies each verify and disagree; it is stopped with its evidence and nothing rebuilds it by a rule",
                        object = %format!("{object:032x}"),
                        stripe = index,
                        label = ?label,
                        reference = *first,
                        differing = ?differing,
                        units = ?units,
                    );
                }
            }
        }
        // a suspect is stale only if the row as it is now still names what its holder lacks
        let mut stale: Vec<usize> = Vec::new();
        if !suspects.is_empty() {
            match &row {
                Some(walked) => {
                    let rows = meta
                        .stripe_rows(consumer, object, &[index])
                        .await
                        .map_err(meta_error)?;
                    let Some(now) = rows.get(&index) else {
                        return Ok(());
                    };
                    for position in suspects {
                        let listed = entry.listed.get(&position);
                        // the row moved on at this position since it was walked: a later pass
                        if now.tombstone || !now.current(position) || now.named(position) != walked.named(position) {
                            continue;
                        }
                        // a placed chunk ahead of the row as it is now holds a write the row is
                        // behind: kept, never marked or rebuilt over, for an operator's word (F93)
                        if listed.is_some_and(|listed| !listed.quarantined && listed.label.ahead_of(now.named(position))) {
                            figures.behind += 1;
                            event!(Level::WARN, msg = "a scrub found a chunk ahead of its row; it is kept for ResolveStripe", object = %format!("{object:032x}"), stripe = index, position);
                            continue;
                        }
                        stale.push(position);
                    }
                }
                // a put's stripe a holder lacks may still be landing: marked once a later pass
                // finds it short past the grace a stager is given
                None => {
                    let mut short = self.short.borrow_mut();
                    for position in suspects {
                        let since = *short.entry((consumer, object, index, position)).or_insert_with(Instant::now);
                        if since.elapsed() >= self.put_grace {
                            short.remove(&(consumer, object, index, position));
                            stale.push(position);
                        }
                    }
                }
            }
        }
        figures.stale += stale.len() as u64;
        figures.corrupt += corrupt.len() as u64;
        // what is stale or corrupt is marked missed, for recovery to rebuild from copies that verify
        let mut marks: Vec<usize> = corrupt.iter().chain(stale.iter()).copied().collect();
        marks.sort_unstable();
        marks.dedup();
        if marks.is_empty() {
            return Ok(());
        }
        if !corrupt.is_empty() {
            event!(Level::WARN, msg = "a scrub found corrupt chunks; their positions are marked for a rebuild", object = %format!("{object:032x}"), stripe = index, positions = ?corrupt);
        }
        match meta
            .mark_missed(consumer, object, index, width, length, owner, &marks, epoch)
            .await
            .map_err(meta_error)?
        {
            Marking::Marked | Marking::Already => Ok(()),
            Marking::Moved => Err("the placement group moved since it was read".to_string()),
        }
    }

    /// What an object's consumer says of it, by the entry at its owner key read strongly, once a
    /// placement group (M20)
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `owner` - The key of the object's entry's row
    /// * `object` - The object
    async fn named(&self, meta: &Meta<D>, owner: u64, object: u128) -> Result<Named, String> {
        if let Some(named) = self.named.borrow().get(&(owner, object)) {
            return Ok(*named);
        }
        let named = Self::ask_owner(meta, owner, object).await?;
        self.named.borrow_mut().insert((owner, object), named);
        Ok(named)
    }

    /// Read an object's entry strongly, by its owner key, for what it says of the object
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `owner` - The key of the object's entry's row
    /// * `object` - The object
    async fn ask_owner(meta: &Meta<D>, owner: u64, object: u128) -> Result<Named, String> {
        // a test can refuse the read, as a loaded host's strong read is refused (item 330)
        if crate::server::objects::faults::scrub_owner_read_refused() {
            return Err(meta_error((
                crate::shared::protocol::error::ErrorCode::QuorumUnavailable,
                "a test refused the owner's read".to_string(),
            )));
        }
        let row = meta.row(owner, Some(DEFAULT_LEVEL)).await.map_err(meta_error)?;
        let Some(row) = row else {
            return Ok(Named::Nowhere);
        };
        // any entry of the row, since paths whose keys collide share one
        let mut named = Named::Nowhere;
        for entry in &row.entries {
            if (entry.state.live && entry.object == object) || entry.state.replacing_id() == Some(object) {
                return Ok(Named::Current);
            }
            if entry.state.retires(object) {
                named = Named::Retired;
            }
        }
        Ok(named)
    }

    /// Remove an object's chunks a holder listed of one placement group, under a seal set before
    /// its consumer is read again: only if the entry still names it nowhere (M20)
    ///
    /// Answers how many holders removed it.
    ///
    /// # Arguments
    ///
    /// * `meta` - The bucket's queries
    /// * `set` - The placement group's holders
    /// * `object` - The consumer and the object
    /// * `owner` - The key of the object's entry's row
    /// * `entry` - What each holder listed of the stripe
    async fn reclaim_orphan(
        &self,
        meta: &Meta<D>,
        set: &super::place::StripeSet,
        (consumer, object): (u64, u128),
        owner: u64,
        entry: &Found,
    ) -> Result<u64, String> {
        let scope = SealScope::Object {
            consumer,
            pg: set.key.1,
            object,
        };
        let seal: u64 = rand::random();
        // every holder that listed it, sealed first
        let mut holders: Vec<Holder> = entry
            .listed
            .iter()
            .filter(|(_, listed)| !listed.staged)
            .filter_map(|(position, _)| set.current().get(*position).map(|(_, holder)| *holder))
            .collect();
        holders.sort_unstable_by_key(|holder| (holder.node, holder.slice));
        holders.dedup();
        let sealed = futures::future::join_all(
            holders
                .iter()
                .map(|holder| self.store.ask_holder(*holder, ChunkRequest::Seal { scope, seal })),
        )
        .await;
        let unseal = || {
            futures::future::join_all(
                holders
                    .iter()
                    .map(|holder| self.store.ask_holder(*holder, ChunkRequest::Unseal { scope, seal })),
            )
        };
        if sealed.iter().any(|answer| *answer != ChunkReply::Done) {
            unseal().await;
            return Ok(0);
        }
        // its consumer again, while sealed: a put registered under the id since is current
        match Self::ask_owner(meta, owner, object).await {
            Ok(Named::Nowhere) => {}
            Ok(_) => {
                unseal().await;
                return Ok(0);
            }
            Err(why) => {
                unseal().await;
                return Err(why);
            }
        }
        // removed under the seal, which the removal ends
        let removed = futures::future::join_all(
            holders
                .iter()
                .map(|holder| self.store.ask_holder(*holder, ChunkRequest::Reclaim { scope, seal })),
        )
        .await;
        let done = removed.iter().filter(|answer| **answer == ChunkReply::Done).count();
        if done < holders.len() {
            unseal().await;
        }
        Ok(done as u64)
    }

    /// Verify every unit of a chunk under a label, a piece at a time, as a scrub's requests
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk
    /// * `holder` - Its holder
    /// * `label` - The label it should be at
    /// * `end` - How far into the chunk its units hold bytes
    /// * `summarise` - Whether its units' summary is folded too, for the parity check (F90)
    async fn verify_chunk(&self, chunk: ChunkId, holder: Holder, label: Label, end: u64, summarise: bool) -> Checked {
        let mut crcs = Vec::new();
        let mut summary = summarise.then(|| vec![0u8; crate::shared::protocol::peer::chunk::SUMMARY_LEN]);
        let mut offset = 0u64;
        while offset < end {
            let len = PIECE_BYTES.min(end - offset);
            let verify = if summarise {
                ChunkRequest::VerifySummary {
                    chunk,
                    label,
                    offset,
                    len,
                }
            } else {
                ChunkRequest::Verify {
                    chunk,
                    label,
                    offset,
                    len,
                }
            };
            let request = ChunkRequest::Scrub(Box::new(verify));
            match self.store.ask_holder(holder, request).await {
                ChunkReply::Verified { label: verified, crcs: piece } if verified == label => crcs.extend(piece),
                // each piece's summary folds into the chunk's: XOR is where the bytes lie
                ChunkReply::Summarised {
                    label: verified,
                    crcs: piece,
                    summary: folded,
                } if verified == label => {
                    crcs.extend(piece);
                    if let Some(summary) = summary.as_mut() {
                        for (at, byte) in summary.iter_mut().zip(folded) {
                            *at ^= byte;
                        }
                    }
                }
                ChunkReply::Corrupt { .. } => return Checked::Corrupt,
                ChunkReply::Held { label: Some(held) } if held == label => return Checked::Pending,
                ChunkReply::Held { .. } => return Checked::Stale,
                other => return Checked::Failed(format!("{other:?}")),
            }
            offset += len;
        }
        Checked::Verified(crcs, summary)
    }

    /// Check an erasure coded stripe's parity against its data by summaries (F90)
    ///
    /// The data chunks' summaries are encoded on an object executor and compared with the parity
    /// chunks'; a data chunk holding no bytes is zeros. A disagreement between chunks that each
    /// verify cannot say which chunk is wrong, so the stripe is reported with its evidence and
    /// nothing marks or rebuilds it by a rule; an operator's word does, at M21.
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `stripe` - The object and the stripe
    /// * `geometry` - The object's geometry
    /// * `summaries` - Each position's summary, for every position holding bytes
    /// * `figures` - What the scrub found, counted
    async fn check_parity(
        &self,
        placement: &Placement,
        stripe: (u128, u64),
        geometry: &crate::shared::objects::Geometry,
        summaries: &BTreeMap<usize, Vec<u8>>,
        figures: &mut ScrubFigures,
    ) {
        let (object, index) = stripe;
        let (data, parity) = (geometry.data, geometry.parity);
        let zeros = vec![0u8; crate::shared::protocol::peer::chunk::SUMMARY_LEN];
        let of = |position: usize| summaries.get(&position).cloned().unwrap_or_else(|| zeros.clone());
        let data_summaries: Vec<Vec<u8>> = (0..usize::from(data)).map(of).collect();
        let parity_summaries: Vec<Vec<u8>> = (usize::from(data)..geometry.width()).map(of).collect();
        let checked = self
            .store
            .compute(move || {
                Box::pin(async move {
                    let data_refs: Vec<&[u8]> = data_summaries.iter().map(Vec::as_slice).collect();
                    let parity_refs: Vec<&[u8]> = parity_summaries.iter().map(Vec::as_slice).collect();
                    crate::server::objects::codec::parity_disagrees(data, parity, &data_refs, &parity_refs)
                })
            })
            .await;
        match checked {
            Ok(Ok(rows)) => {
                figures.parity_checked += 1;
                if !rows.is_empty() {
                    figures.parity_mismatch += 1;
                    event!(
                        Level::ERROR,
                        msg = "an erasure coded stripe's chunks each verify and its parity disagrees with its data; it is stopped with its evidence and nothing rebuilds it by a rule",
                        pool = %placement.pool_name,
                        object = %format!("{object:032x}"),
                        stripe = index,
                        parity_rows = ?rows,
                    );
                }
            }
            Ok(Err(error)) => event!(Level::DEBUG, msg = "a parity check could not run", stripe = index, %error),
            Err(error) => event!(Level::DEBUG, msg = "a parity check found no object executor", stripe = index, %error),
        }
    }

    /// Every chunk of a placement group one holder lists, in pages, as a scrub's requests
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    async fn list(&self, holder: Holder, consumer: u64, pg: PlacementGroup) -> Result<Vec<ListedChunk>, String> {
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
            let request = ChunkRequest::Scrub(Box::new(ChunkRequest::List {
                after,
                start,
                limit: LIST_PAGE,
            }));
            match self.store.ask_holder(holder, request).await {
                ChunkReply::Listed { chunks, more } => {
                    if let Some(last) = chunks.last() {
                        after = last.chunk;
                        start = false;
                    }
                    found.extend(chunks);
                    if !more {
                        return Ok(found);
                    }
                }
                other => return Err(format!("{holder:?} did not list {pg:?}: {other:?}")),
            }
        }
    }
}

/// The words a scrub's error starts with when the metadata query behind it was refused for now: a
/// code a loaded or changing cluster answers, which the next poll's try rides out (item 330)
const FOR_NOW: &str = "refused for now: ";

/// A metadata query's refusal as a scrub's error, marked when it was refused for now
///
/// # Arguments
///
/// * `refusal` - The code and why
fn meta_error((code, why): (crate::shared::protocol::error::ErrorCode, String)) -> String {
    use crate::shared::protocol::error::ErrorCode;
    // the codes a client asks again on: between leaders, short of a quorum, too busy, or cut off
    let for_now = matches!(
        code,
        ErrorCode::NotLeader
            | ErrorCode::QuorumUnavailable
            | ErrorCode::Timeout
            | ErrorCode::OutcomeUnknown
            | ErrorCode::Unavailable
            | ErrorCode::Shedding
            | ErrorCode::StaleTopology
            | ErrorCode::NotInitialized
            | ErrorCode::ConnectionLost
            | ErrorCode::GoingAway
    );
    if for_now {
        format!("{FOR_NOW}{code}: {why}")
    } else {
        format!("{code}: {why}")
    }
}

/// Read the scrub cursors of the groups this shard leads and drive every pass that is due, for as
/// long as the shard runs
///
/// # Arguments
///
/// * `client` - The shard's metadata client
/// * `shard_tx` - The shard's own channel
/// * `map` - The shard's map
/// * `store` - The node's object store, once started
/// * `every` - How often the cursors are read
/// * `inflight` - How many passes run at once
/// * `put_grace` - How long a put's stripe short of a copy is given to finish landing
/// * `wait` - How long one metadata query may take
#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll<D: ShoalDatabase>(
    client: MetaClient,
    shard_tx: kanal::AsyncSender<ServerMsg<D>>,
    map: MapCell,
    store: Arc<OnceLock<ObjectStore>>,
    every: Duration,
    inflight: usize,
    put_grace: Duration,
    wait: Duration,
) where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    // the passes running, so a poll starts none twice
    let busy: Rc<RefCell<HashSet<(GroupId, ScrubKind)>>> = Rc::new(RefCell::new(HashSet::new()));
    let running = Rc::new(Cell::new(0usize));
    let mut scrubber: Option<Rc<Scrubber<D>>> = None;
    loop {
        glommio::timer::sleep(every).await;
        // a test may pause the drivers to arrange its damage first
        if crate::server::objects::faults::scrub_paused() {
            continue;
        }
        // nothing scrubs before the cluster speaks the scrub wire
        let installed = map.get();
        if installed.activated_wire < SCRUB_WIRE_VERSION {
            continue;
        }
        // the store starts once the shards answer
        let scrubber = match &scrubber {
            Some(scrubber) => scrubber.clone(),
            None => {
                let Some(store) = store.get().cloned() else {
                    continue;
                };
                let made = Rc::new(Scrubber {
                    client: client.clone(),
                    shard_tx: shard_tx.clone(),
                    map: map.clone(),
                    store,
                    wait,
                    put_grace,
                    first_seen: RefCell::new(HashMap::new()),
                    short: RefCell::new(HashMap::new()),
                    named: RefCell::new(HashMap::new()),
                });
                scrubber = Some(made.clone());
                made
            }
        };
        // the stripe groups this shard leads, with their cursors
        let (reply, answer) = futures_channel::oneshot::channel();
        if shard_tx.send(ServerMsg::ScrubGroups { reply }).await.is_err() {
            return;
        }
        let Ok(groups) = answer.await else {
            continue;
        };
        let now = now_ms();
        for group in groups {
            // the bucket whose stripes the group holds, and its pool's schedule
            let Some((consumer, pool_name, pool)) = Scrubber::<D>::binding_of(&installed, group.table) else {
                continue;
            };
            let first = *scrubber.first_seen.borrow_mut().entry(group.group).or_insert(now);
            let requests = installed.pool_map.scrub_requests_of(&pool_name);
            let policy = pool.scrub_policy();
            for kind in ScrubKind::ALL {
                if running.get() >= inflight.max(1) {
                    break;
                }
                let stagger = stagger_ms(group.group.0, policy.interval_ms(kind));
                let due = due(&policy, kind, group.scrub.cursor(kind), requests.of(kind), now, first, stagger);
                if due == Due::Wait || !busy.borrow_mut().insert((group.group, kind)) {
                    continue;
                }
                // what an operator's request narrows the pass to: the newest one's, while the pass
                // serves it and every request the group has not served names it; a scheduled pass
                // scrubs the whole pool (F93, item 266)
                let cursor = group.scrub.cursor(kind);
                let target = match due {
                    Due::Begin { asked: true, .. } => requests.target_for(kind, cursor.served),
                    Due::Continue if cursor.asked && cursor.request == requests.of(kind) => {
                        requests.target_for(kind, cursor.served)
                    }
                    _ => None,
                };
                running.set(running.get() + 1);
                let scrubber = scrubber.clone();
                let busy = busy.clone();
                let running = running.clone();
                let group = group.clone();
                let pool_name = pool_name.clone();
                glommio::spawn_local(async move {
                    let id = group.group;
                    if let Err(error) = scrubber.pass(group, consumer, pool_name, kind, due, target).await {
                        event!(Level::DEBUG, msg = "a scrub pass stopped; the next poll goes on from its cursor", group = %id, kind = kind.as_str(), error);
                    }
                    busy.borrow_mut().remove(&(id, kind));
                    running.set(running.get() - 1);
                })
                .detach();
            }
        }
    }
}
