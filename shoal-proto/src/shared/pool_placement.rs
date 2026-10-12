//! Where a placement group's stripe chunks belong: weighted rendezvous over a pool's slices
//!
//! A stripe's key puts its row in one tablet, and a **placement group** is a sub-range of that
//! tablet's keys, so the tablet's group orders every write to it
//! ([S5](../../../docs/src/object-storage/placement.md#a-placement-group-is-a-sub-range-of-a-tablet)).
//! A placement group's slices are a function of the pool map at a generation and nothing else:
//! every slice of the pool's class draws, each failure domain keeps its best slice, and the
//! `width` domains whose best slices score lowest are the answer, which X2 chose because every
//! change to the map moves exactly the least it could
//! ([X2](../../../docs/src/object-storage/placement-simulation.md#recommendation)). Which member
//! of the set holds which position is the tablet group's, not this function's.
//!
//! # Invariants
//!
//! **A placement group never spans two tablets**: [`pg_of`] takes the tablet's twelve bits, then
//! the next log2 of the pool's groups a tablet, of the stripe row's own partition key
//! ([`crate::shared::objects::stripe_key`]).
//!
//! **The answer is the same on every node and every build**: the score is
//! [`crate::shared::rendezvous`]'s integers, frozen table and one multiply, every weight is an
//! integer, and the slices are drawn in one order, by device id and then index, so even an exact
//! tie falls alike everywhere.
//!
//! **No two chunks share a failure domain of the pool's level, and none ever share a device**:
//! each domain gives one slice, and a device is in exactly one domain at any level.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::shared::devices::DeviceId;
use crate::shared::domains::{domain_at, HOST, RACK, ZONE};
use crate::shared::identity::NodeId;
use crate::shared::placement::{tablet_of, TABLET_BITS, TABLET_COUNT};
use crate::shared::poolmap::{PlacementGroup, PoolMap, SliceRef};
use crate::shared::pools::{FailureDomain, PoolSpec};
use crate::shared::rendezvous::{draw, mix64, score, GOLDEN};

/// The most placement groups a tablet can hold under one pool
///
/// A group's place in its tablet is sixteen bits of its key, so a tablet could split under a pool
/// sixteen times before a split splits placement groups.
pub const MAX_PLACEMENT_GROUPS: u32 = 1 << 16;

/// The placement group a stripe's key falls in, under a pool of so many groups a tablet
///
/// # Arguments
///
/// * `stripe_key` - The stripe row's partition key, [`crate::shared::objects::stripe_key`]
/// * `groups_per_tablet` - The pool's placement groups a tablet, a power of two
#[must_use]
pub fn pg_of(stripe_key: u64, groups_per_tablet: u32) -> PlacementGroup {
    // the tablet is the key's top twelve bits, as every table's is
    #[allow(clippy::cast_possible_truncation)]
    let tablet = tablet_of(stripe_key) as u16;
    // and the group is the next bits below them
    let bits = groups_per_tablet.max(1).trailing_zeros();
    #[allow(clippy::cast_possible_truncation)]
    let sub = if bits == 0 {
        0
    } else {
        ((stripe_key << TABLET_BITS) >> (u64::BITS - bits)) as u16
    };
    PlacementGroup { tablet, sub }
}

/// Every placement group of a pool, in order
///
/// # Arguments
///
/// * `groups_per_tablet` - The pool's placement groups a tablet
pub fn every_group(groups_per_tablet: u32) -> impl Iterator<Item = PlacementGroup> {
    let groups = groups_per_tablet.max(1);
    #[allow(clippy::cast_possible_truncation)]
    (0..TABLET_COUNT).flat_map(move |tablet| {
        (0..groups).map(move |sub| PlacementGroup {
            tablet: tablet as u16,
            sub: sub as u16,
        })
    })
}

/// The key one consumer's placement group draws by
///
/// The consumer is in it, so two consumers' groups at one place in one tablet draw apart.
///
/// # Arguments
///
/// * `consumer` - The consumer
/// * `pg` - The placement group
#[must_use]
pub const fn group_key(consumer: u64, pg: PlacementGroup) -> u64 {
    // the group's place, mixed with its consumer, then offset so a key of zero never mixes to zero
    let place = ((pg.tablet as u64) << 16) | pg.sub as u64;
    mix64(mix64(consumer ^ mix64(place)) ^ GOLDEN)
}

/// The key a slice draws by: its device's seat and its index, so a replacement in the seat draws
/// exactly as its predecessor's slices did
///
/// # Arguments
///
/// * `seat` - The device's seat
/// * `index` - The slice's index on the device
#[must_use]
pub const fn slice_key(seat: u64, index: u16) -> u64 {
    mix64(seat ^ mix64(index as u64 + 1))
}

/// The name of the level a pool's failure domain reads off a member, `device` for none
///
/// # Arguments
///
/// * `level` - The pool's failure domain
#[must_use]
pub const fn level_name(level: FailureDomain) -> &'static str {
    match level {
        FailureDomain::Device => "device",
        FailureDomain::Host => HOST,
        FailureDomain::Rack => RACK,
        FailureDomain::Zone => ZONE,
    }
}

/// Why a pool cannot place a stripe
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Infeasible {
    /// The pool
    pub pool: String,
    /// The level its chunks must not share
    pub level: String,
    /// The domains its devices span
    pub domains: usize,
    /// The domains a stripe needs
    pub needed: usize,
}

impl std::fmt::Display for Infeasible {
    /// Write the refusal for a person
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pool {} places a stripe on {} distinct {}s, and its devices span {}",
            self.pool, self.needed, self.level, self.domains
        )
    }
}

/// A slice as a view draws it
#[derive(Debug, Clone)]
struct Candidate {
    /// The slice
    slice: SliceRef,
    /// The key it draws by
    key: u64,
    /// One over its share of its device's placement weight
    inverse_weight: f64,
    /// Its domain's number in the view
    domain: u32,
    /// Its device's number in the view
    device: u32,
}

/// A device as a view holds it
#[derive(Debug, Clone)]
pub struct ViewDevice {
    /// Its id
    pub id: DeviceId,
    /// Its node
    pub node: NodeId,
    /// The weight placement draws it by, fitted or its capacity
    pub placement_weight: u64,
    /// Its capacity weight, which fill is judged against
    pub capacity: u64,
    /// Its domain's number in the view
    pub domain: u32,
    /// Its slices' numbers in the view
    pub slices: std::ops::Range<usize>,
}

/// A pool's slices at one generation, ready to place any placement group
#[derive(Debug, Clone)]
pub struct View {
    /// The pool
    pub pool: String,
    /// The generation it is a view of
    pub generation: u64,
    /// The level no two chunks share
    pub level: FailureDomain,
    /// The chunks a stripe has
    pub width: usize,
    /// Every slice that draws, by device id then index
    candidates: Vec<Candidate>,
    /// Every domain's name, by number
    pub domains: Vec<String>,
    /// Every device, by device id
    pub devices: Vec<ViewDevice>,
    /// Members whose name at the pool's level is a level below it, by the level it came from
    pub fallback: BTreeMap<&'static str, BTreeSet<NodeId>>,
    /// The exceptions in force, by consumer and placement group
    exceptions: HashMap<(u64, PlacementGroup), Vec<(SliceRef, SliceRef)>>,
}

impl View {
    /// A pool's view of the map at a generation, drawing by its fitted weights if it has some
    ///
    /// # Arguments
    ///
    /// * `map` - The pool map
    /// * `name` - The pool's name
    /// * `pool` - The pool
    /// * `generation` - The generation
    #[must_use]
    pub fn new(map: &PoolMap, name: &str, pool: &PoolSpec, generation: u64) -> Self {
        let fitted = map.fitted_at(name, generation).cloned().unwrap_or_default();
        View::with_weights(map, name, pool, generation, &fitted)
    }

    /// A pool's view of the map at a generation, drawing by the weights given where they name a
    /// device and by its capacity elsewhere
    ///
    /// # Arguments
    ///
    /// * `map` - The pool map
    /// * `name` - The pool's name
    /// * `pool` - The pool
    /// * `generation` - The generation
    /// * `weights` - Placement weights by device
    #[must_use]
    pub fn with_weights(
        map: &PoolMap,
        name: &str,
        pool: &PoolSpec,
        generation: u64,
        weights: &BTreeMap<DeviceId, u64>,
    ) -> Self {
        let level = pool.failure_domain;
        let wanted = level_name(level);
        // every device of the pool's class at the generation, in id order, and its domain's name
        let mut named = Vec::new();
        let mut fallback: BTreeMap<&'static str, BTreeSet<NodeId>> = BTreeMap::new();
        for device in map.devices.values() {
            let Some(era) = device.era_at(generation) else {
                continue;
            };
            if era.class != pool.class || era.slices.is_empty() {
                continue;
            }
            let domain = match level {
                FailureDomain::Device => format!("device:{}", device.id),
                _ => match domain_at(&era.domains, wanted) {
                    Some((used, value)) => {
                        // a member standing in the level below names it, and readiness says so
                        if used != wanted {
                            fallback.entry(used).or_default().insert(device.node);
                        }
                        format!("{used}:{value}")
                    }
                    // a member naming no domain at all stands in one of its own
                    None => {
                        fallback.entry("node").or_default().insert(device.node);
                        format!("node:{}", device.node)
                    }
                },
            };
            named.push((device, era, domain));
        }
        // domains numbered in name order, so the numbering is the same wherever it is built
        let names: BTreeSet<&String> = named.iter().map(|(_, _, domain)| domain).collect();
        let domains: Vec<String> = names.into_iter().cloned().collect();
        let number: HashMap<&String, u32> = domains
            .iter()
            .enumerate()
            .map(|(index, name)| (name, u32::try_from(index).unwrap_or(u32::MAX)))
            .collect();
        let mut candidates = Vec::new();
        let mut devices = Vec::new();
        for (device, era, domain) in &named {
            let domain = number[domain];
            let placement_weight = weights.get(&device.id).copied().unwrap_or(era.weight).max(1);
            // each slice an equal share of its device's weight, computed from integers
            #[allow(clippy::cast_precision_loss)]
            let inverse_weight = era.slices.len() as f64 / placement_weight as f64;
            let first = candidates.len();
            let number = u32::try_from(devices.len()).unwrap_or(u32::MAX);
            for index in 0..era.slices.len() {
                let index = u16::try_from(index).unwrap_or(u16::MAX);
                candidates.push(Candidate {
                    slice: SliceRef {
                        device: device.id,
                        index,
                    },
                    key: slice_key(device.seat, index),
                    inverse_weight,
                    domain,
                    device: number,
                });
            }
            devices.push(ViewDevice {
                id: device.id,
                node: device.node,
                placement_weight,
                capacity: era.weight.max(1),
                domain,
                slices: first..candidates.len(),
            });
        }
        // the exceptions in force, by consumer and group
        let mut exceptions: HashMap<(u64, PlacementGroup), Vec<(SliceRef, SliceRef)>> =
            HashMap::new();
        for exception in map
            .exceptions
            .iter()
            .filter(|exception| exception.pool == name && exception.holds_at(generation))
        {
            exceptions
                .entry((exception.consumer, exception.pg))
                .or_default()
                .push((exception.rule, exception.instead));
        }
        View {
            pool: name.to_string(),
            generation,
            level,
            width: usize::from(pool.redundancy.width()),
            candidates,
            domains,
            devices,
            fallback,
            exceptions,
        }
    }

    /// Whether a stripe can be placed at all: as many domains as a stripe has chunks
    ///
    /// # Errors
    ///
    /// A pool whose devices span fewer domains than its width.
    pub fn feasible(&self) -> Result<(), Infeasible> {
        if self.domains.len() < self.width || self.width == 0 {
            return Err(Infeasible {
                pool: self.pool.clone(),
                level: level_name(self.level).to_string(),
                domains: self.domains.len(),
                needed: self.width,
            });
        }
        Ok(())
    }

    /// The rule's answer for a placement group, before any exception: one slice in each of the
    /// `width` domains whose best slices score lowest, lowest first
    ///
    /// # Arguments
    ///
    /// * `consumer` - The consumer whose group it is
    /// * `pg` - The placement group
    ///
    /// # Errors
    ///
    /// A pool that cannot place a stripe.
    pub fn rule(&self, consumer: u64, pg: PlacementGroup) -> Result<Vec<SliceRef>, Infeasible> {
        self.feasible()?;
        let indices = self.rule_indices(group_key(consumer, pg));
        Ok(indices
            .into_iter()
            .map(|index| self.candidates[index].slice)
            .collect())
    }

    /// The rule's answer as candidate numbers, for a group's key
    ///
    /// # Arguments
    ///
    /// * `group` - The group's key, from [`group_key`]
    fn rule_indices(&self, group: u64) -> Vec<usize> {
        // each domain's best slice and its score, in one pass in a fixed order; a strict less
        // than leaves an exact tie with the earlier slice, alike everywhere
        let mut best: Vec<(f64, usize)> = vec![(f64::INFINITY, usize::MAX); self.domains.len()];
        for (index, candidate) in self.candidates.iter().enumerate() {
            let drawn = score(draw(group, candidate.key), candidate.inverse_weight);
            let domain = candidate.domain as usize;
            if drawn < best[domain].0 {
                best[domain] = (drawn, index);
            }
        }
        // the width lowest domains, lowest first, a tie to the lower numbered domain
        let mut order: Vec<usize> = (0..self.domains.len())
            .filter(|domain| best[*domain].1 != usize::MAX)
            .collect();
        order.sort_by(|a, b| best[*a].0.total_cmp(&best[*b].0).then(a.cmp(b)));
        order
            .into_iter()
            .take(self.width)
            .map(|domain| best[domain].1)
            .collect()
    }

    /// The slices a placement group's chunks belong on: the rule's answer with its exceptions
    ///
    /// # Arguments
    ///
    /// * `consumer` - The consumer whose group it is
    /// * `pg` - The placement group
    ///
    /// # Errors
    ///
    /// A pool that cannot place a stripe.
    pub fn place(&self, consumer: u64, pg: PlacementGroup) -> Result<Vec<SliceRef>, Infeasible> {
        let mut answer = self.rule(consumer, pg)?;
        // the planner's corrections, each a slice the rule names and the one used instead
        if let Some(exceptions) = self.exceptions.get(&(consumer, pg)) {
            for (rule, instead) in exceptions {
                if let Some(slot) = answer.iter_mut().find(|slice| *slice == rule) {
                    *slot = *instead;
                }
            }
        }
        Ok(answer)
    }

    /// The domain a slice stands in, by name
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    #[must_use]
    pub fn domain_of(&self, slice: SliceRef) -> Option<&str> {
        self.candidates
            .iter()
            .find(|candidate| candidate.slice == slice)
            .map(|candidate| self.domains[candidate.domain as usize].as_str())
    }
}

/// Each placement group's answer, kept by generation: an answer at a generation never changes
///
/// Computing one costs about ten nanoseconds a slice on a Zen1 core
/// ([O87](../../../docs/src/appendix/optimizations.md#o87-a-placement-answer-is-computed-again-on-every-lookup)),
/// so a node keeps what it computed.
#[derive(Debug, Default)]
pub struct PlacementCache {
    /// Each pool's view, by pool and generation
    views: HashMap<(String, u64), View>,
    /// Each answer, by pool, generation, consumer and group
    answers: HashMap<(String, u64, u64, PlacementGroup), Vec<SliceRef>>,
    /// The most answers kept before the cache is emptied
    limit: usize,
}

impl PlacementCache {
    /// A cache that keeps up to so many answers
    ///
    /// # Arguments
    ///
    /// * `limit` - The most answers it keeps
    #[must_use]
    pub fn new(limit: usize) -> Self {
        PlacementCache {
            views: HashMap::new(),
            answers: HashMap::new(),
            limit: limit.max(1),
        }
    }

    /// A placement group's slices at a generation, computed once
    ///
    /// # Arguments
    ///
    /// * `map` - The pool map, at or past the generation
    /// * `name` - The pool's name
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    /// * `generation` - The generation
    ///
    /// # Errors
    ///
    /// A pool that cannot place a stripe.
    pub fn answer(
        &mut self,
        map: &PoolMap,
        name: &str,
        pool: &PoolSpec,
        consumer: u64,
        pg: PlacementGroup,
        generation: u64,
    ) -> Result<Vec<SliceRef>, Infeasible> {
        let key = (name.to_string(), generation, consumer, pg);
        if let Some(answer) = self.answers.get(&key) {
            return Ok(answer.clone());
        }
        // a full cache starts again rather than tracking what was used last
        if self.answers.len() >= self.limit {
            self.answers.clear();
            self.views.clear();
        }
        let view = self
            .views
            .entry((name.to_string(), generation))
            .or_insert_with(|| View::new(map, name, pool, generation));
        let answer = view.place(consumer, pg)?;
        self.answers.insert(key, answer.clone());
        Ok(answer)
    }

    /// How many answers it holds
    #[must_use]
    pub fn len(&self) -> usize {
        self.answers.len()
    }

    /// Whether it holds none
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.answers.is_empty()
    }
}

/// What a node reports of one device, for readiness: never committed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeviceStatus {
    /// Whether its figures were heard at all: a node judges its own devices' space, and the
    /// control leader every member's, and nobody's space is judged from silence
    pub reported: bool,
    /// Whether it has failed
    pub failed: bool,
    /// Its free bytes
    pub free_bytes: u64,
    /// The free bytes it keeps
    pub reserve_bytes: u64,
}

/// Whether a pool can take writes, and if not, what it is short of
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PoolReadiness {
    /// The pool
    pub pool: String,
    /// The class its devices carry
    pub class: String,
    /// The level no two chunks share
    pub level: String,
    /// The chunks a stripe has
    pub width: usize,
    /// The chunks an acknowledged write needs current, `k + f`
    pub needed: usize,
    /// Devices of its class on the map now
    pub devices: usize,
    /// The ones serving: not failed, on a member that is up
    pub serving: usize,
    /// The ones failed
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<DeviceId>,
    /// The ones on a member that is down
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub down: Vec<DeviceId>,
    /// The domains its devices span
    pub domains: usize,
    /// The domains its serving devices with space span
    pub serving_domains: usize,
    /// Members that stand in the pool's level by a level below it, for an operator to see
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<String>,
    /// The free bytes above every serving device's reserve
    pub free_bytes: u64,
    /// The serving devices at or under their reserve
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub short_of_space: Vec<DeviceId>,
    /// What a pool exactly as wide as its domains can use: its smallest domain's free bytes
    /// times its width, since every placement group puts a chunk in every domain
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_bytes: Option<u64>,
    /// What it is short of, by name; empty when it is ready
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub short: Vec<String>,
}

impl PoolReadiness {
    /// Whether it is short of nothing
    #[must_use]
    pub fn ready(&self) -> bool {
        self.short.is_empty()
    }
}

/// A pool's readiness on a map now
///
/// # Arguments
///
/// * `map` - The pool map
/// * `name` - The pool's name
/// * `pool` - The pool
/// * `statuses` - What each device's node last reported of it
/// * `up` - Whether a member is up
#[must_use]
pub fn readiness(
    map: &PoolMap,
    name: &str,
    pool: &PoolSpec,
    statuses: &BTreeMap<DeviceId, DeviceStatus>,
    up: impl Fn(NodeId) -> bool,
) -> PoolReadiness {
    let view = View::new(map, name, pool, map.generation);
    let level = level_name(pool.failure_domain);
    let (data, _) = pool.redundancy.chunks();
    let needed = usize::from(data) + usize::from(pool.losses());
    let mut ready = PoolReadiness {
        pool: name.to_string(),
        class: pool.class.clone(),
        level: level.to_string(),
        width: view.width,
        needed,
        devices: view.devices.len(),
        domains: view.domains.len(),
        ..PoolReadiness::default()
    };
    // each serving device's space, by domain, and every domain serving with space or unheard
    let mut domain_free: BTreeMap<u32, u64> = BTreeMap::new();
    let mut serving: BTreeSet<u32> = BTreeSet::new();
    let mut all_reported = true;
    for device in &view.devices {
        let status = statuses.get(&device.id).copied().unwrap_or_default();
        let failed = status.failed
            || map
                .devices
                .get(&device.id)
                .is_some_and(|record| record.failed.is_some());
        if failed {
            ready.failed.push(device.id);
            continue;
        }
        if !up(device.node) {
            ready.down.push(device.id);
            continue;
        }
        ready.serving += 1;
        // a device whose figures were not heard serves, and its space is not judged
        if !status.reported {
            serving.insert(device.domain);
            all_reported = false;
            continue;
        }
        let above = status.free_bytes.saturating_sub(status.reserve_bytes);
        if above == 0 {
            ready.short_of_space.push(device.id);
            continue;
        }
        ready.free_bytes += above;
        serving.insert(device.domain);
        *domain_free.entry(device.domain).or_default() += above;
    }
    ready.serving_domains = serving.len();
    // a member standing in a level below the pool's is named, never silent
    if !view.fallback.is_empty() {
        let named: Vec<String> = view
            .fallback
            .iter()
            .map(|(used, members)| format!("{level} falls back to {used} on {} members", members.len()))
            .collect();
        ready.fallback = Some(named.join(", "));
    }
    // a pool as wide as its domains fills at the pace of its smallest
    if all_reported && view.domains.len() == view.width && domain_free.len() == view.domains.len() {
        ready.usable_bytes = domain_free
            .values()
            .min()
            .map(|least| least.saturating_mul(view.width as u64));
    }
    // and what it is short of, by name
    if view.devices.is_empty() {
        ready
            .short
            .push(format!("no device carries class {}", pool.class));
    } else if view.domains.len() < view.width {
        ready.short.push(format!(
            "it places a stripe on {} distinct {level}s and its devices span {}",
            view.width,
            view.domains.len()
        ));
    }
    if !view.devices.is_empty() && serving.len() < needed {
        let missing = if ready.short_of_space.is_empty() {
            "serving"
        } else {
            "serving with space above their reserve"
        };
        ready.short.push(format!(
            "an acknowledged write needs {needed} {level}s current and {} are {missing}",
            serving.len()
        ));
    }
    if !ready.short_of_space.is_empty() {
        ready.short.push(format!(
            "{} devices are at their reserve: {}",
            ready.short_of_space.len(),
            ready
                .short_of_space
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    ready
}

/// The rounds of fitting the planner runs
const FIT_ROUNDS: usize = 24;

/// The chunks each device of a view holds, over every group of the consumers given
///
/// # Arguments
///
/// * `view` - The view
/// * `consumers` - The consumers whose groups to place
/// * `groups_per_tablet` - The pool's groups a tablet
fn loads(view: &View, consumers: &[u64], groups_per_tablet: u32) -> Vec<u64> {
    let mut loads = vec![0u64; view.devices.len()];
    for consumer in consumers {
        for pg in every_group(groups_per_tablet) {
            for index in view.rule_indices(group_key(*consumer, pg)) {
                loads[view.candidates[index].device as usize] += 1;
            }
        }
    }
    loads
}

/// The fullest device's fill over the mean, by capacity
///
/// # Arguments
///
/// * `view` - The view
/// * `loads` - Each device's chunks
#[allow(clippy::cast_precision_loss)]
fn worst_fill(view: &View, loads: &[u64]) -> f64 {
    let total: u64 = loads.iter().sum();
    let capacity: u64 = view.devices.iter().map(|device| device.capacity).sum();
    let mean = total as f64 / capacity.max(1) as f64;
    view.devices
        .iter()
        .zip(loads)
        .map(|(device, load)| *load as f64 / device.capacity as f64)
        .fold(0.0, f64::max)
        / mean.max(f64::MIN_POSITIVE)
}

/// Placement weights that correct the bias of drawing devices of unequal weight
///
/// `None` for a pool whose devices are of one weight, which needs none, or that cannot place.
/// Each round places every group of a sample of consumers, then divides each device's weight by the
/// square root of its fill over the mean; the round whose fullest device was closest to the mean
/// wins ([X2](../../../docs/src/object-storage/placement-simulation.md#exceptions-and-fitted-weights)).
/// Run by the control leader alone, which commits the integers it returns.
///
/// # Arguments
///
/// * `map` - The pool map
/// * `name` - The pool's name
/// * `pool` - The pool
/// * `consumers` - The consumers to fit to
#[must_use]
pub fn fit_weights(
    map: &PoolMap,
    name: &str,
    pool: &PoolSpec,
    consumers: &[u64],
) -> Option<BTreeMap<DeviceId, u64>> {
    let groups = pool.groups_per_tablet();
    let capacity = View::with_weights(map, name, pool, map.generation, &BTreeMap::new());
    capacity.feasible().ok()?;
    // devices of one weight draw in proportion already
    let first = capacity.devices.first()?.capacity;
    if capacity.devices.iter().all(|device| device.capacity == first) {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    let mut weights: Vec<f64> = capacity
        .devices
        .iter()
        .map(|device| device.capacity as f64)
        .collect();
    let mut best = (f64::INFINITY, weights.clone());
    for _ in 0..FIT_ROUNDS {
        // place under these weights and read each device's fill against its capacity
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let integers: BTreeMap<DeviceId, u64> = capacity
            .devices
            .iter()
            .zip(&weights)
            .map(|(device, weight)| (device.id, (weight.round() as u64).max(1)))
            .collect();
        let placing = View::with_weights(map, name, pool, map.generation, &integers);
        let placed = loads(&placing, consumers, groups);
        let worst = worst_fill(&capacity, &placed);
        if worst < best.0 {
            best = (worst, weights.clone());
        }
        // and move each weight against its fill
        let total: u64 = placed.iter().sum();
        let all: u64 = capacity.devices.iter().map(|device| device.capacity).sum();
        #[allow(clippy::cast_precision_loss)]
        let mean = total as f64 / all.max(1) as f64;
        for ((device, load), weight) in capacity.devices.iter().zip(&placed).zip(weights.iter_mut()) {
            #[allow(clippy::cast_precision_loss)]
            let util = (*load as f64 / device.capacity as f64 / mean).max(1e-6);
            *weight /= util.sqrt();
        }
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Some(
        capacity
            .devices
            .iter()
            .zip(best.1)
            .map(|(device, weight)| (device.id, (weight.round() as u64).max(1)))
            .collect(),
    )
}

/// Exceptions that bring one consumer's fullest device within a margin of the mean
///
/// Starting from the rule's answer, a chunk is moved off the fullest device onto the emptiest one
/// the domain rule allows, one at a time, as Ceph's balancer writes `pg_upmap_items`; every chunk
/// that ends up away from the rule's slice is one exception. Stops at the margin, at `cap` moves,
/// or when no chunk of the fullest device can go anywhere emptier
/// ([X2](../../../docs/src/object-storage/placement-simulation.md#exceptions-and-fitted-weights)).
///
/// # Arguments
///
/// * `view` - The pool's view, with its exceptions or without
/// * `consumer` - The consumer
/// * `groups_per_tablet` - The pool's groups a tablet
/// * `margin` - How far over the mean the fullest device may be, such as 0.02
/// * `cap` - The most moves to try
#[must_use]
pub fn exceptions_for(
    view: &View,
    consumer: u64,
    groups_per_tablet: u32,
    margin: f64,
    cap: u64,
) -> Vec<(PlacementGroup, SliceRef, SliceRef)> {
    if view.feasible().is_err() {
        return Vec::new();
    }
    let groups: Vec<PlacementGroup> = every_group(groups_per_tablet).collect();
    let width = view.width;
    // every slot's candidate under the rule
    let mut slots: Vec<usize> = Vec::with_capacity(groups.len() * width);
    for pg in &groups {
        slots.extend(view.rule_indices(group_key(consumer, *pg)));
    }
    let rule = slots.clone();
    let mut device_load = vec![0u64; view.devices.len()];
    let mut slice_load = vec![0u64; view.candidates.len()];
    for candidate in &slots {
        device_load[view.candidates[*candidate].device as usize] += 1;
        slice_load[*candidate] += 1;
    }
    #[allow(clippy::cast_precision_loss)]
    let util = |load: u64, device: &ViewDevice| load as f64 / device.capacity as f64;
    let mut moves = 0u64;
    loop {
        let total: u64 = device_load.iter().sum();
        let capacity: u64 = view.devices.iter().map(|device| device.capacity).sum();
        #[allow(clippy::cast_precision_loss)]
        let mean = total as f64 / capacity.max(1) as f64;
        let Some(fullest) = (0..view.devices.len()).max_by(|a, b| {
            util(device_load[*a], &view.devices[*a]).total_cmp(&util(device_load[*b], &view.devices[*b]))
        }) else {
            break;
        };
        let full = util(device_load[fullest], &view.devices[fullest]);
        if full <= mean * (1.0 + margin) || moves >= cap {
            break;
        }
        // receivers emptiest first, each left no fuller than the donor
        let mut receivers: Vec<usize> = (0..view.devices.len())
            .filter(|device| {
                *device != fullest && util(device_load[*device] + 1, &view.devices[*device]) < full
            })
            .collect();
        receivers.sort_by(|a, b| {
            util(device_load[*a], &view.devices[*a]).total_cmp(&util(device_load[*b], &view.devices[*b]))
        });
        // the first slot of the fullest device whose group has nothing in the receiver's domain
        let found = receivers.into_iter().find_map(|receiver| {
            let domain = view.devices[receiver].domain;
            (0..slots.len())
                .find(|slot| {
                    view.candidates[slots[*slot]].device as usize == fullest && {
                        let pg = slot / width;
                        (0..width).all(|other| {
                            pg * width + other == *slot
                                || view.candidates[slots[pg * width + other]].domain != domain
                        })
                    }
                })
                .map(|slot| (slot, receiver))
        });
        let Some((slot, receiver)) = found else {
            break;
        };
        // onto the receiver's least loaded slice
        let target = view.devices[receiver]
            .slices
            .clone()
            .min_by_key(|candidate| slice_load[*candidate])
            .unwrap_or(view.devices[receiver].slices.start);
        let old = slots[slot];
        device_load[view.candidates[old].device as usize] -= 1;
        slice_load[old] -= 1;
        slots[slot] = target;
        device_load[receiver] += 1;
        slice_load[target] += 1;
        moves += 1;
    }
    // every slot away from the rule is one exception
    slots
        .iter()
        .zip(&rule)
        .enumerate()
        .filter(|(_, (now, was))| now != was)
        .map(|(slot, (now, was))| {
            (
                groups[slot / width],
                view.candidates[*was].slice,
                view.candidates[*now].slice,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests;
