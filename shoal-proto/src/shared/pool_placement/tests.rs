//! Placement over generated pool maps: the domain rule, movement, frozen answers and seats

use std::collections::{BTreeMap, BTreeSet};

use super::{
    every_group, exceptions_for, fit_weights, group_key, pg_of, readiness, DeviceStatus,
    PlacementCache, View, MAX_PLACEMENT_GROUPS,
};
use crate::shared::checksum::Crc64;
use crate::shared::devices::{DeviceFacts, DeviceId, SliceId};
use crate::shared::identity::NodeId;
use crate::shared::objects::stripe_key;
use crate::shared::placement::tablet_of;
use crate::shared::poolmap::{PlacementGroup, PoolMap, SliceRef};
use crate::shared::pools::{FailureDomain, PoolSpec, Redundancy};
use crate::shared::rendezvous::mix64;

/// A seeded stream of numbers, SplitMix64's, so every map here is the same on every host
struct Seeded(u64);

impl Seeded {
    /// The next number
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix64(self.0)
    }

    /// The next number below a bound
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

/// A device's facts, its id and slices' ids made from small numbers
fn facts(id: u128, class: &str, slices: u16, size: u64) -> DeviceFacts {
    DeviceFacts {
        id: DeviceId(uuid::Uuid::from_u128(id)),
        class: class.to_string(),
        slices: (0..slices)
            .map(|index| SliceId(uuid::Uuid::from_u128(id << 8 | u128::from(index))))
            .collect(),
        size_bytes: size,
        weight: None,
        write_back: false,
    }
}

/// A member's domains: a host, and a rack and a zone when named
fn domains(host: u64, rack: Option<u64>, zone: Option<u64>) -> BTreeMap<String, String> {
    let mut domains = BTreeMap::from([("host".to_string(), format!("h{host}"))]);
    if let Some(rack) = rack {
        domains.insert("rack".to_string(), format!("r{rack}"));
    }
    if let Some(zone) = zone {
        domains.insert("zone".to_string(), format!("z{zone}"));
    }
    domains
}

/// A map of hosts, each one node with some devices of one class, sizes and slices drawn by seed
fn generated(seed: u64, hosts: u64, per_host: u64, racks: Option<u64>) -> PoolMap {
    let mut rng = Seeded(seed);
    let mut map = PoolMap::default();
    let mut id = 1u128;
    for host in 0..hosts {
        let rack = racks.map(|racks| host % racks);
        let mut claimed = Vec::new();
        for _ in 0..per_host {
            // 1 to 16 TiB in whole TiB, one to four slices
            let size = (1 + rng.below(16)) << 40;
            let slices = 1 + u16::try_from(rng.below(4)).expect("small");
            claimed.push(facts(id, "ssd", slices, size));
            id += 1;
        }
        map.observe(NodeId::from(host + 1), &domains(host, rack, None), &claimed)
            .expect("observed");
    }
    map
}

/// Every placement group's answer for one consumer
fn answers(view: &View, consumer: u64, groups: u32) -> Vec<Vec<SliceRef>> {
    every_group(groups)
        .map(|pg| view.place(consumer, pg).expect("placed"))
        .collect()
}

/// The host a device stands on in a view
fn host_of(map: &PoolMap, device: DeviceId, generation: u64) -> String {
    map.devices[&device]
        .era_at(generation)
        .and_then(|era| era.domains.get("host").cloned())
        .expect("a host")
}

/// Over generated maps, no placement group's answer holds two slices of one host under a host
/// domain, or two slices of one device under either (S5)
#[test]
fn placement_never_shares_a_failure_domain() {
    for seed in 0..12u64 {
        let hosts = 3 + seed % 6;
        let map = generated(seed, hosts, 1 + seed % 3, None);
        for (level, redundancy) in [
            (FailureDomain::Host, Redundancy::Replicas(3)),
            (FailureDomain::Host, Redundancy::Erasure { data: 2, parity: 1 }),
            (FailureDomain::Device, Redundancy::Replicas(2)),
            (FailureDomain::Device, Redundancy::Erasure { data: 2, parity: 2 }),
        ] {
            let pool = PoolSpec::new("ssd", redundancy).failure_domain(level);
            let view = View::new(&map, "p", &pool, map.generation);
            if view.feasible().is_err() {
                continue;
            }
            for answer in answers(&view, seed + 1, 1) {
                assert_eq!(answer.len(), usize::from(redundancy.width()));
                let devices: BTreeSet<DeviceId> = answer.iter().map(|slice| slice.device).collect();
                assert_eq!(devices.len(), answer.len(), "two slices of one device: {answer:?}");
                if level == FailureDomain::Host {
                    let hosts: BTreeSet<String> = answer
                        .iter()
                        .map(|slice| host_of(&map, slice.device, map.generation))
                        .collect();
                    assert_eq!(hosts.len(), answer.len(), "two slices of one host: {answer:?}");
                }
            }
        }
    }
}

/// Adding, removing or reweighting one device changes only the sets its slices leave or join (S5)
#[test]
fn placement_moves_only_what_changed() {
    let pool = PoolSpec::new("ssd", Redundancy::Erasure { data: 4, parity: 2 });
    for seed in 0..4u64 {
        let mut map = generated(seed, 8, 2, None);
        let before = View::new(&map, "p", &pool, map.generation);
        let old = answers(&before, 7, 1);
        // a device added on a host of its own
        let added = facts(1000, "ssd", 2, 8 << 40);
        let added_id = added.id;
        map.observe(NodeId::from(99), &domains(99, None, None), &[added])
            .expect("observed");
        let after = View::new(&map, "p", &pool, map.generation);
        let new = answers(&after, 7, 1);
        for (was, now) in old.iter().zip(&new) {
            let was: BTreeSet<_> = was.iter().copied().collect();
            let now: BTreeSet<_> = now.iter().copied().collect();
            if was != now {
                // the new device joined the set, and only one member left for it
                let joined: Vec<_> = now.difference(&was).collect();
                assert_eq!(joined.len(), 1, "{was:?} -> {now:?}");
                assert_eq!(joined[0].device, added_id, "{was:?} -> {now:?}");
            }
        }
        // the device removed again: every set returns to what it was
        map.retire_node(NodeId::from(99));
        let removed = View::new(&map, "p", &pool, map.generation);
        assert_eq!(answers(&removed, 7, 1), old);
        // one device reweighted: only sets it leaves or joins change
        let device = *map.devices.keys().next().expect("a device");
        let era = map.devices[&device].current().expect("on the map").clone();
        let node = map.devices[&device].node;
        let mut heavier = facts(device.0.as_u128(), "ssd", u16::try_from(era.slices.len()).expect("small"), era.weight);
        heavier.slices = era.slices.clone();
        heavier.weight = Some(era.weight * 3);
        let generation = map.generation;
        map.observe(node, &era.domains, &[heavier]).expect("observed");
        assert_eq!(map.generation, generation + 1);
        let reweighted = answers(&View::new(&map, "p", &pool, map.generation), 7, 1);
        for (was, now) in old.iter().zip(&reweighted) {
            let was: BTreeSet<_> = was.iter().copied().collect();
            let now: BTreeSet<_> = now.iter().copied().collect();
            if was != now {
                let changed: BTreeSet<_> = was.symmetric_difference(&now).collect();
                assert!(
                    changed.iter().any(|slice| slice.device == device),
                    "a set changed without the reweighted device: {was:?} -> {now:?}"
                );
                assert_eq!(changed.len(), 2, "{was:?} -> {now:?}");
            }
        }
    }
}

/// The score's table and a set of answers are frozen, and every build gives them (S5)
#[test]
fn placement_is_the_same_on_every_build() {
    // the table under every score, as `rendezvous` freezes it
    let mut table = Crc64::new();
    for u in [1u64, 1 << 20, 1 << 40, 0x1234_5678_9abc_def1, u64::MAX] {
        table.update(&crate::shared::rendezvous::neglog2(u).to_le_bytes());
    }
    assert_eq!(table.finish(), FROZEN_NEGLOG2S, "{:#018x}", table.finish());
    // every answer of three pools over one generated map, in order
    let map = generated(42, 6, 2, Some(3));
    let mut digest = Crc64::new();
    for (level, redundancy) in [
        (FailureDomain::Host, Redundancy::Replicas(3)),
        (FailureDomain::Rack, Redundancy::Erasure { data: 2, parity: 1 }),
        (FailureDomain::Device, Redundancy::Erasure { data: 4, parity: 2 }),
    ] {
        let pool = PoolSpec::new("ssd", redundancy).failure_domain(level);
        let view = View::new(&map, "p", &pool, map.generation);
        for answer in answers(&view, 3, 2) {
            for slice in answer {
                digest.update(slice.device.0.as_bytes());
                digest.update(&slice.index.to_le_bytes());
            }
        }
    }
    assert_eq!(digest.finish(), FROZEN_ANSWERS, "{:#018x}", digest.finish());
}

/// `neglog2` at five draws, frozen
const FROZEN_NEGLOG2S: u64 = 0x14fe_5cdc_42d0_a410;

/// Every answer of the three pools in `placement_is_the_same_on_every_build`, frozen
const FROZEN_ANSWERS: u64 = 0xd033_29bb_16b6_ac88;

/// A device that takes over a seat is given exactly its predecessor's groups (S5)
#[test]
fn replacement_in_its_seat_moves_nothing_else() {
    let pool = PoolSpec::new("ssd", Redundancy::Erasure { data: 3, parity: 2 });
    let mut map = generated(7, 7, 2, None);
    let old = answers(&View::new(&map, "p", &pool, map.generation), 5, 1);
    // a device of two slices replaced by a new one of the same shape, in its seat
    let predecessor = *map
        .devices
        .iter()
        .find(|(_, device)| device.current().is_some_and(|era| era.slices.len() == 2))
        .expect("a device of two slices")
        .0;
    let record = map.devices[&predecessor].clone();
    let era = record.current().expect("on the map").clone();
    let mut replacement = facts(5000, "ssd", 2, era.weight);
    replacement.weight = Some(era.weight);
    let replacement_id = replacement.id;
    map.replace_in_seat(predecessor, record.node, &era.domains, &replacement)
        .expect("replaced");
    let new = answers(&View::new(&map, "p", &pool, map.generation), 5, 1);
    for (was, now) in old.iter().zip(&new) {
        // the replacement stands exactly where its predecessor stood, slice for slice
        let mapped: Vec<SliceRef> = was
            .iter()
            .map(|slice| {
                if slice.device == predecessor {
                    SliceRef {
                        device: replacement_id,
                        index: slice.index,
                    }
                } else {
                    *slice
                }
            })
            .collect();
        assert_eq!(&mapped, now);
    }
}

/// A placement group is a sub-range of its stripe row's tablet, at every count a tablet holds
#[test]
fn a_placement_group_is_a_sub_range_of_its_tablet() {
    let mut rng = Seeded(9);
    for _ in 0..10_000 {
        let key = stripe_key(rng.next(), u128::from(rng.next()) << 64 | u128::from(rng.next()), rng.below(1 << 20));
        for groups in [1u32, 2, 64, MAX_PLACEMENT_GROUPS] {
            let pg = pg_of(key, groups);
            assert_eq!(usize::from(pg.tablet), tablet_of(key));
            assert!(u32::from(pg.sub) < groups);
        }
    }
    // the sub-ranges are contiguous: the next bits below the tablet's
    assert_eq!(pg_of(0x0010_0000_0000_0000, 2).sub, 0);
    assert_eq!(pg_of(0x0018_0000_0000_0000, 2).sub, 1);
}

/// A stripe's key is frozen: it is every stripe row's partition key
#[test]
fn stripe_key_is_frozen() {
    let mut digest = Crc64::new();
    for (consumer, object, stripe) in [(1u64, 0u128, 0u64), (1, u128::MAX, 7), (42, 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210, 1 << 40)] {
        digest.update(&stripe_key(consumer, object, stripe).to_le_bytes());
    }
    assert_eq!(digest.finish(), FROZEN_STRIPE_KEYS, "{:#018x}", digest.finish());
}

/// Three stripes' keys, frozen
const FROZEN_STRIPE_KEYS: u64 = 0x15dc_4c57_279d_e2aa;

/// Two consumers' groups at one place draw apart
#[test]
fn consumers_draw_apart() {
    let pg = PlacementGroup { tablet: 7, sub: 0 };
    assert_ne!(group_key(1, pg), group_key(2, pg));
}

/// A pool short of domains, standing in a level by a level below, or at its reserve is named
#[test]
fn readiness_names_what_a_pool_is_short_of() {
    // three hosts, one device each, no racks
    let map = generated(3, 3, 1, None);
    let statuses: BTreeMap<DeviceId, DeviceStatus> = map
        .devices
        .keys()
        .map(|id| {
            (
                *id,
                DeviceStatus {
                    reported: true,
                    failed: false,
                    free_bytes: 10 << 30,
                    reserve_bytes: 1 << 30,
                },
            )
        })
        .collect();
    // three copies across hosts is ready, and as wide as its domains
    let ready = readiness(&map, "fast", &PoolSpec::new("ssd", Redundancy::Replicas(3)), &statuses, |_| true);
    assert!(ready.ready(), "{ready:?}");
    assert_eq!(ready.usable_bytes, Some(27 << 30));
    // four copies cannot be placed on three hosts
    let wide = readiness(&map, "wide", &PoolSpec::new("ssd", Redundancy::Replicas(4)), &statuses, |_| true);
    assert!(wide.short.iter().any(|why| why.contains("4 distinct hosts")), "{wide:?}");
    // a pool under racks places by host and says so
    let racked = PoolSpec::new("ssd", Redundancy::Replicas(3)).failure_domain(FailureDomain::Rack);
    let racked = readiness(&map, "racked", &racked, &statuses, |_| true);
    assert!(racked.ready(), "{racked:?}");
    assert_eq!(racked.fallback.as_deref(), Some("rack falls back to host on 3 members"));
    // a member down leaves too few hosts for an acknowledged write
    let down = NodeId::from(1);
    let degraded = readiness(&map, "fast", &PoolSpec::new("ssd", Redundancy::Replicas(2)), &statuses, |node| node != down);
    assert!(degraded.ready(), "{degraded:?}");
    let three = readiness(&map, "fast", &PoolSpec::new("ssd", Redundancy::Replicas(3)).placement_groups(1), &statuses, |node| node != down);
    assert_eq!(three.down.len(), 1);
    assert!(three.ready(), "two copies are an acknowledged write of three: {three:?}");
    // devices at their reserve are named
    let mut full = statuses.clone();
    for status in full.values_mut() {
        status.free_bytes = status.reserve_bytes;
    }
    let full = readiness(&map, "fast", &PoolSpec::new("ssd", Redundancy::Replicas(3)), &full, |_| true);
    assert_eq!(full.short_of_space.len(), 3);
    assert!(full.short.iter().any(|why| why.contains("at their reserve")), "{full:?}");
    // and no device of the class at all
    let none = readiness(&map, "cold", &PoolSpec::new("hdd", Redundancy::Replicas(1)), &statuses, |_| true);
    assert!(none.short.iter().any(|why| why.contains("no device carries class hdd")), "{none:?}");
}

/// Fitted weights bring a pool of unequal devices closer to even than its capacities do
#[test]
fn fitted_weights_even_out_unequal_devices() {
    // six hosts of one device: three of 4 TiB and three of 16 TiB, a 4+2 pool across them
    let mut map = PoolMap::default();
    for host in 0..6u64 {
        let size = if host < 3 { 4u64 << 40 } else { 16 << 40 };
        map.observe(NodeId::from(host + 1), &domains(host, None, None), &[facts(u128::from(host) + 1, "ssd", 1, size)])
            .expect("observed");
    }
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(3)).placement_groups(4);
    let consumers: Vec<u64> = (1..=4).collect();
    let fitted = fit_weights(&map, "p", &pool, &consumers).expect("unequal devices are fitted");
    // judged on a consumer it was not fitted to
    let by_capacity = View::with_weights(&map, "p", &pool, map.generation, &BTreeMap::new());
    let by_fit = View::with_weights(&map, "p", &pool, map.generation, &fitted);
    let worst_capacity = super::worst_fill(&by_capacity, &super::loads(&by_capacity, &[99], 4));
    let worst_fit = super::worst_fill(&by_capacity, &super::loads(&by_fit, &[99], 4));
    assert!(worst_fit < worst_capacity, "{worst_fit} against {worst_capacity}");
    // a pool of equal devices needs none
    let equal = generated(1, 4, 1, None);
    let mut equal_map = PoolMap::default();
    for (host, device) in equal.devices.values().enumerate() {
        let host = host as u64;
        equal_map
            .observe(device.node, &domains(host, None, None), &[facts(device.id.0.as_u128(), "ssd", 1, 1 << 40)])
            .expect("observed");
    }
    assert!(fit_weights(&equal_map, "p", &PoolSpec::new("ssd", Redundancy::Replicas(2)), &consumers).is_none());
}

/// Exceptions bring the fullest device within the margin, and keep the domain rule
#[test]
fn exceptions_keep_the_domain_rule_and_even_the_fill() {
    let map = generated(11, 6, 2, None);
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(2)).failure_domain(FailureDomain::Host);
    let view = View::new(&map, "p", &pool, map.generation);
    let found = exceptions_for(&view, 3, 1, 0.05, 100_000);
    let mut balanced = map.clone();
    balanced.set_exceptions("p", 3, &found);
    let after = View::new(&balanced, "p", &pool, balanced.generation);
    let mut loads: BTreeMap<DeviceId, u64> = BTreeMap::new();
    for answer in answers(&after, 3, 1) {
        // still never two slices of one host
        let hosts: BTreeSet<String> = answer
            .iter()
            .map(|slice| host_of(&balanced, slice.device, balanced.generation))
            .collect();
        assert_eq!(hosts.len(), answer.len(), "{answer:?}");
        for slice in answer {
            *loads.entry(slice.device).or_default() += 1;
        }
    }
    // the fullest device by capacity is within the margin, or the balancer said it could not be
    let capacity = |id: &DeviceId| map.devices[id].current().map_or(1, |era| era.weight);
    let total: u64 = loads.values().sum();
    let weight: u64 = loads.keys().map(capacity).sum();
    #[allow(clippy::cast_precision_loss)]
    let mean = total as f64 / weight as f64;
    #[allow(clippy::cast_precision_loss)]
    let worst = loads.iter().map(|(id, load)| *load as f64 / capacity(id) as f64).fold(0.0, f64::max);
    assert!(!found.is_empty(), "a statistical rule over twelve devices needs some");
    assert!(worst <= mean * 1.05 + 1e-9, "{worst} against {mean}");
}

/// An answer is computed once a generation, and a later generation computes its own
#[test]
fn the_cache_answers_by_generation() {
    let mut map = generated(5, 4, 1, None);
    let pool = PoolSpec::new("ssd", Redundancy::Replicas(3));
    let mut cache = PlacementCache::new(1024);
    let pg = PlacementGroup { tablet: 3, sub: 0 };
    let first = cache.answer(&map, "p", &pool, 1, pg, map.generation).expect("placed");
    assert_eq!(cache.answer(&map, "p", &pool, 1, pg, map.generation).expect("placed"), first);
    assert_eq!(cache.len(), 1);
    let generation = map.generation;
    map.observe(NodeId::from(77), &domains(77, None, None), &[facts(777, "ssd", 1, 1 << 50)])
        .expect("observed");
    // the old generation still answers as it did, from the eras
    assert_eq!(cache.answer(&map, "p", &pool, 1, pg, generation).expect("placed"), first);
    let _ = cache.answer(&map, "p", &pool, 1, pg, map.generation).expect("placed");
    assert_eq!(cache.len(), 2);
}
