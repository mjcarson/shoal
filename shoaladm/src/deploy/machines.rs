//! Nodes that share a machine, judged against what the machine is
//!
//! An inventory may place several nodes on one machine, each reached through the same ssh target
//! and listening on an address of its own: the loopback leg of the object store's benchmarks
//! runs three nodes on europa over `127.0.0.11–13` this way. Each such node gets a unit, a remote
//! directory and listeners of its own ([`super::inventory::Inventory::colocated`]); what the file
//! alone cannot say is whether their shards land on the same cpus, since which cpus a node's
//! shards take depends on the machine's topology. This module answers that from the machine's
//! own answer to the cpu probe, and refuses two ssh targets that turn out to reach one machine,
//! which would otherwise be deployed as two machines sharing a unit name and a directory.

use color_eyre::eyre::bail;
use std::collections::{BTreeMap, BTreeSet};

use super::inventory::{Inventory, Resources};
use crate::cpu::{CpuFacts, CpuPlace};

/// The cpus one node takes on its machine
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Taken {
    /// The control thread's cpu, and its core's other threads unless the core is shared
    pub control: BTreeSet<usize>,
    /// The cpus its shards run on
    pub shards: BTreeSet<usize>,
    /// The cpus its object executors run on
    pub objects: BTreeSet<usize>,
}

impl Taken {
    /// The cpus it runs work on that no other node may touch: its shards' and its object cpus
    #[must_use]
    pub fn own(&self) -> BTreeSet<usize> {
        self.shards.iter().chain(&self.objects).copied().collect()
    }

    /// The cpus another node's work may not run on: any of this one's, its control core's included
    ///
    /// Two nodes' control threads may share a core, as X3's and X8's loopback nodes did on cpu 0:
    /// each keeps its own shards off that core, and the control threads take turns on it.
    ///
    /// # Arguments
    ///
    /// * `other` - The other node's cpus
    #[must_use]
    pub fn clashes_with(&self, other: &Taken) -> BTreeSet<usize> {
        let own = self.own();
        let theirs = other.own();
        // a shard or object cpu of one on anything of the other's; control on control is shared
        own.intersection(&other.all())
            .chain(theirs.intersection(&self.all()))
            .copied()
            .collect()
    }

    /// Every cpu it takes, whatever for
    #[must_use]
    pub fn all(&self) -> BTreeSet<usize> {
        // the three sets as one
        self.control
            .iter()
            .chain(&self.shards)
            .chain(&self.objects)
            .copied()
            .collect()
    }
}

/// The cpus a node's shards take on a machine of this topology
///
/// A mirror of shoal-core's `Resources::cpus_reserving` with the control core reserved, as a
/// cluster node calls it, which this crate cannot name since it links no engine: cpu 0 is never
/// a shard's, an excluded core or the control thread's core is skipped whole, an object cpu is
/// skipped alone, and a core count takes one cpu of each physical core, lowest first, before it
/// doubles up on their other threads. The engine sorts by NUMA node first; every lab machine has
/// one node, and a machine with several is judged by package and core alone.
///
/// # Arguments
///
/// * `resources` - The node's resources
/// * `topology` - Every online cpu of the machine
#[must_use]
pub fn shard_cpus(resources: &Resources, topology: &[CpuPlace]) -> Vec<usize> {
    // the physical core the control thread keeps for itself, unless it shares it
    let control = resources.control_core.unwrap_or(0);
    let reserved: Vec<usize> = if resources.control_core_shared {
        Vec::new()
    } else {
        topology
            .iter()
            .filter(|place| place.cpu == control)
            .map(|place| place.core)
            .collect()
    };
    // every cpu a shard may run on, in the engine's stable order
    let mut candidates: Vec<CpuPlace> = topology
        .iter()
        .filter(|place| place.cpu != 0)
        .filter(|place| !resources.exclude_cores.contains(&place.core))
        .filter(|place| !reserved.contains(&place.core))
        .filter(|place| !resources.object_cpus.contains(&place.cpu))
        .copied()
        .collect();
    candidates.sort_unstable_by_key(|place| (place.package, place.core, place.cpu));
    // every candidate when no count was named
    let Some(cores) = resources.cores else {
        return candidates.iter().map(|place| place.cpu).collect();
    };
    // one cpu of each physical core first
    let mut selected: Vec<usize> = Vec::with_capacity(cores);
    let mut claimed = BTreeSet::new();
    for place in &candidates {
        if selected.len() == cores {
            break;
        }
        if claimed.insert(place.core) {
            selected.push(place.cpu);
        }
    }
    // then the other threads of the cores already taken
    for place in &candidates {
        if selected.len() == cores {
            break;
        }
        if !selected.contains(&place.cpu) {
            selected.push(place.cpu);
        }
    }
    selected
}

/// Every cpu a node takes on a machine of this topology
///
/// # Arguments
///
/// * `resources` - The node's resources
/// * `topology` - Every online cpu of the machine
#[must_use]
pub fn taken(resources: &Resources, topology: &[CpuPlace]) -> Taken {
    // the control thread's cpu, and the rest of its core unless the core is shared
    let control_cpu = resources.control_core.unwrap_or(0);
    let control = if resources.control_core_shared {
        BTreeSet::from([control_cpu])
    } else {
        let core = topology.iter().find(|place| place.cpu == control_cpu);
        match core {
            Some(core) => topology
                .iter()
                .filter(|place| place.core == core.core && place.package == core.package)
                .map(|place| place.cpu)
                .collect(),
            None => BTreeSet::from([control_cpu]),
        }
    };
    Taken {
        control,
        shards: shard_cpus(resources, topology).into_iter().collect(),
        objects: resources.object_cpus.iter().copied().collect(),
    }
}

/// Refuse nodes on one machine whose cpus overlap, and two ssh targets that reach one machine
///
/// # Arguments
///
/// * `inventory` - The deployment
/// * `probed` - What each ssh target's machine answered, for every target that answered
///
/// # Errors
///
/// Names the nodes, the targets and the cpus.
pub fn check(inventory: &Inventory, probed: &BTreeMap<String, CpuFacts>) -> color_eyre::Result<()> {
    let machines = inventory.machines();
    // a machine reached through two targets would be deployed as two machines, units and
    // directories colliding, so it is refused before anything is written
    let mut by_id: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (target, facts) in probed {
        if !facts.machine.is_empty() {
            by_id.entry(facts.machine.as_str()).or_default().push(target.as_str());
        }
    }
    if let Some(targets) = by_id.values().find(|targets| targets.len() > 1) {
        // the nodes behind each target, for the refusal
        let named: Vec<String> = targets
            .iter()
            .map(|target| {
                let nodes: Vec<&str> = machines
                    .get(*target)
                    .map(|specs| specs.iter().map(|spec| spec.name.as_str()).collect())
                    .unwrap_or_default();
                format!("{target} ({})", nodes.join(", "))
            })
            .collect();
        bail!(
            "the ssh targets {} reach one machine; nodes sharing a machine are given one ssh \
             target, so each is deployed with a unit and a directory of its own",
            named.join(" and ")
        );
    }
    // nodes on one machine, judged by the cpus its topology gives each
    for (target, specs) in &machines {
        if specs.len() < 2 {
            continue;
        }
        // a machine that did not answer was judged by the file alone
        let Some(facts) = probed.get(target) else {
            continue;
        };
        let taken: Vec<(&str, Taken)> = specs
            .iter()
            .map(|spec| {
                let (resources, _) = inventory.resolve_resources(spec);
                (spec.name.as_str(), taken(&resources, &facts.topology))
            })
            .collect();
        // every pair, once
        for (index, (first, first_taken)) in taken.iter().enumerate() {
            for (second, second_taken) in &taken[index + 1..] {
                // a control core may be shared; a shard or an object cpu never
                let shared: Vec<usize> = first_taken.clashes_with(second_taken).into_iter().collect();
                if !shared.is_empty() {
                    bail!(
                        "the nodes {first:?} and {second:?} share the machine reached as {target} \
                         and would both run on cpus {shared:?} ({first} takes {:?}, {second} takes \
                         {:?}); give each exclude_cores that keep it off the other's cores",
                        first_taken.all(),
                        second_taken.all()
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// europa's topology: sixteen cores of two threads, cpu N's other thread cpu N + 16
    fn europa() -> Vec<CpuPlace> {
        (0..32)
            .map(|cpu| CpuPlace {
                cpu,
                core: cpu % 16,
                package: 0,
            })
            .collect()
    }

    /// Resources naming a core count, the cores kept off, a control cpu and object cpus
    fn resources(cores: usize, exclude: &[usize], control: usize, objects: &[usize]) -> Resources {
        Resources {
            cores: Some(cores),
            exclude_cores: exclude.to_vec(),
            control_core: Some(control),
            object_cpus: objects.to_vec(),
            ..Resources::default()
        }
    }

    /// Shards take one thread of each core first, never cpu 0, an excluded core, the control
    /// core or an object cpu, as the engine's own selection does
    #[test]
    fn shards_take_what_the_engine_would() {
        let topology = europa();
        // four cores past the control thread's on core 0, the object cpu its other thread
        let first = shard_cpus(&resources(4, &[], 0, &[16]), &topology);
        assert_eq!(first, vec![1, 2, 3, 4]);
        // the control thread on core 5 keeps both its threads, and cores 0..4 are excluded
        let second = shard_cpus(&resources(4, &[0, 1, 2, 3, 4], 5, &[21]), &topology);
        assert_eq!(second, vec![6, 7, 8, 9]);
        // past one thread a core, the other threads of the cores already taken
        let small: Vec<CpuPlace> = europa().into_iter().filter(|place| place.core < 3).collect();
        assert_eq!(shard_cpus(&resources(4, &[], 0, &[]), &small), vec![1, 2, 17, 18]);
        // a shared control core gives its core to the shards, cpu 0 alone left out
        let mut shared = resources(2, &[], 1, &[]);
        shared.control_core_shared = true;
        assert_eq!(shard_cpus(&shared, &topology), vec![16, 1]);
        // and no count takes every candidate
        let mut every = resources(1, &[], 0, &[]);
        every.cores = None;
        assert_eq!(shard_cpus(&every, &topology).len(), 30);
    }

    /// The loopback inventory's three nodes take cpus apart on europa, and two nodes whose
    /// cores overlap are refused naming both and the cpus
    #[test]
    fn nodes_sharing_a_machine_are_judged_by_its_topology() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("inventories/loopback.yml");
        let inventory = Inventory::read(&path).expect("the loopback inventory");
        let facts = CpuFacts {
            arch: "x86_64".to_string(),
            machine: "europa-id".to_string(),
            topology: europa(),
            ..CpuFacts::default()
        };
        let probed = BTreeMap::from([("europa".to_string(), facts.clone())]);
        check(&inventory, &probed).expect("the loopback nodes keep apart");
        // every node's cpus, none of them the tmdb node's 0 and 16
        for spec in &inventory.nodes {
            let (resources, _) = inventory.resolve_resources(spec);
            let all = taken(&resources, &europa()).all();
            assert!(!all.contains(&0) && !all.contains(&16), "{}: {all:?}", spec.name);
        }
        // the same three with the exclusions dropped from one of them overlap
        let mut overlapping = inventory.clone();
        let second = overlapping.nodes[1].name.clone();
        let resources = overlapping.nodes[1].resources.as_mut().expect("a node's own resources");
        resources.exclude_cores.clear();
        let error = check(&overlapping, &probed).expect_err("overlapping cores were accepted").to_string();
        assert!(error.contains(&format!("{second:?}")) && error.contains("cpus ["), "{error}");
        // two targets that turn out to be one machine are refused naming both
        let mut split = inventory;
        split.nodes[2].ssh = Some("localhost".to_string());
        let probed = BTreeMap::from([
            ("europa".to_string(), facts.clone()),
            ("localhost".to_string(), facts),
        ]);
        let error = check(&split, &probed).expect_err("one machine through two targets").to_string();
        assert!(error.contains("europa (") && error.contains("localhost ("), "{error}");
    }

    /// Two nodes on one machine may share a control core, each keeping its shards off it, as X3's
    /// and X8's loopback nodes did on cpu 0; a shard of one on the other's control core may not
    #[test]
    fn nodes_may_share_a_control_core_and_nothing_else() {
        let topology = europa();
        // both control threads on core 0, the shards on cores 1-4 and 5-8
        let first = taken(&resources(4, &[0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], 0, &[]), &topology);
        let second = taken(&resources(4, &[0, 1, 2, 3, 4, 9, 10, 11, 12, 13, 14, 15], 0, &[]), &topology);
        assert!(first.clashes_with(&second).is_empty(), "{:?}", first.clashes_with(&second));
        // the second's shards over the first's control core clash
        let over = taken(&resources(4, &[1, 2, 3, 4, 9, 10, 11, 12, 13, 14, 15], 5, &[]), &topology);
        assert!(!first.clashes_with(&over).is_empty());
    }
}
