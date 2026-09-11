//! Explicit core allocation, recorded whether or not the machine can honor it
//!
//! [C10](../../../docs/src/distributed/performance.md) asks that every node's resource
//! allocation be recorded, that data, control and driver cores be disjoint where isolation is
//! claimed, and that sharing be recorded rather than hidden when the machine cannot support it.
//! Cores are handed out as whole physical cores, both SMT threads of each, because
//! `Resources::exclude_cores` excludes by physical core id and because a claim of isolation
//! that left a sibling thread to someone else would not be one.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use super::{FixtureError, NodeKind, NodeSpec};

/// The machine's cores, as physical core id to the cpus on it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    /// Each physical core and its cpus, in order
    pub cores: BTreeMap<usize, Vec<usize>>,
}

impl Topology {
    /// Read the machine
    ///
    /// From sysfs, the same place glommio reads it. Where sysfs is not there, every cpu is its
    /// own core, which is what a machine without SMT looks like anyway.
    pub fn detect() -> Self {
        let mut cores: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let online = std::thread::available_parallelism().map_or(1, |n| n.get());
        for cpu in 0..online {
            // the physical core this cpu is a thread of
            let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/core_id");
            let core = std::fs::read_to_string(&path)
                .ok()
                .and_then(|text| text.trim().parse::<usize>().ok())
                .unwrap_or(cpu);
            cores.entry(core).or_default().push(cpu);
        }
        Self { cores }
    }

    /// A machine described by hand, for a test of the allocator itself
    ///
    /// # Arguments
    ///
    /// * `physical` - How many physical cores
    /// * `threads` - How many threads each has
    pub fn synthetic(physical: usize, threads: usize) -> Self {
        let cores = (0..physical)
            .map(|core| (core, (0..threads).map(|t| core + t * physical).collect()))
            .collect();
        Self { cores }
    }

    /// The physical core cpu zero is on, which is reserved for coordination and never handed out
    pub fn reserved(&self) -> Option<usize> {
        self.cores
            .iter()
            .find(|(_, cpus)| cpus.contains(&0))
            .map(|(core, _)| *core)
    }
}

/// What a node or the driver asks for
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreClaim {
    /// This many whole physical cores, whichever are free; shared if there are not enough
    Count(usize),
    /// Exactly these physical cores; refused if any is already taken
    Exact(Vec<usize>),
    /// No isolation claimed: run on whatever is left, and say so
    Shared,
}

/// What one node or the driver got
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    /// The physical cores owned outright; empty when shared
    pub data: Vec<usize>,
    /// The cpus on those cores
    pub cpus: Vec<usize>,
    /// The core a control thread would run on; none until M1 gives a node one
    pub control: Option<usize>,
    /// Whether this allocation shares cores with something else
    pub shared: bool,
}

/// Everything a cluster was given, and what it bound
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterPlan {
    /// Each node's kind and allocation
    pub nodes: Vec<(NodeKind, Allocation)>,
    /// The driver's allocation
    pub driver: Allocation,
    /// The core reserved for coordination, on this machine
    pub reserved: Option<usize>,
    /// Each node's endpoints, once started
    pub endpoints: Vec<super::Endpoints>,
    /// Each directed link's proxy address, once started
    pub proxies: Vec<((usize, usize), SocketAddr)>,
}

impl ClusterPlan {
    /// Check that every allocation claiming isolation is disjoint from every other
    ///
    /// Shared allocations are exempt: they claimed nothing. The reserved core is nobody's.
    pub fn disjoint_where_claimed(&self) -> Result<(), String> {
        let mut owners: BTreeMap<usize, String> = BTreeMap::new();
        let claims = self
            .nodes
            .iter()
            .enumerate()
            .map(|(id, (kind, allocation))| (format!("node {id} ({kind:?})"), allocation))
            .chain(std::iter::once(("the driver".to_string(), &self.driver)));
        for (name, allocation) in claims {
            if allocation.shared {
                continue;
            }
            for core in &allocation.data {
                // the reserved core is never anyone's
                if Some(*core) == self.reserved {
                    return Err(format!("{name} was given the reserved core {core}"));
                }
                if let Some(other) = owners.insert(*core, name.clone()) {
                    return Err(format!("core {core} is claimed by both {other} and {name}"));
                }
            }
        }
        Ok(())
    }
}

/// Hand out cores
///
/// Exact claims are taken first, since they can only be met one way; counts are taken from
/// what is left, falling back to shared when the machine runs out; shared claims take nothing.
///
/// # Arguments
///
/// * `specs` - The nodes
/// * `driver` - The driver's claim
/// * `topology` - The machine
pub fn allocate(
    specs: &[NodeSpec],
    driver: &CoreClaim,
    topology: &Topology,
) -> Result<ClusterPlan, FixtureError> {
    let reserved = topology.reserved();
    // everything but the reserved core is available
    let mut free: BTreeSet<usize> = topology
        .cores
        .keys()
        .copied()
        .filter(|core| Some(*core) != reserved)
        .collect();
    let cpus_of = |cores: &[usize]| -> Vec<usize> {
        cores
            .iter()
            .flat_map(|core| topology.cores.get(core).cloned().unwrap_or_default())
            .collect()
    };
    // exact claims first
    let mut allocations: Vec<Option<Allocation>> = vec![None; specs.len() + 1];
    let claims: Vec<(usize, &CoreClaim)> = specs
        .iter()
        .map(|spec| &spec.cores)
        .chain(std::iter::once(driver))
        .enumerate()
        .collect();
    for (slot, claim) in &claims {
        if let CoreClaim::Exact(cores) = claim {
            for core in cores {
                // a core nobody has, and not the reserved one
                if !free.remove(core) {
                    return Err(FixtureError::Allocation(format!(
                        "core {core} is not free to be claimed exactly"
                    )));
                }
            }
            allocations[*slot] = Some(Allocation {
                data: cores.clone(),
                cpus: cpus_of(cores),
                control: None,
                shared: false,
            });
        }
    }
    // then counts, then shared
    for (slot, claim) in &claims {
        let allocation = match claim {
            CoreClaim::Exact(_) => continue,
            CoreClaim::Count(count) => {
                if free.len() >= *count {
                    let cores: Vec<usize> = free.iter().take(*count).copied().collect();
                    for core in &cores {
                        free.remove(core);
                    }
                    Allocation {
                        data: cores.clone(),
                        cpus: cpus_of(&cores),
                        control: None,
                        shared: false,
                    }
                } else {
                    // the machine is too small: run shared, and record it rather than hide it
                    Allocation {
                        data: Vec::new(),
                        cpus: Vec::new(),
                        control: None,
                        shared: true,
                    }
                }
            }
            CoreClaim::Shared => Allocation {
                data: Vec::new(),
                cpus: Vec::new(),
                control: None,
                shared: true,
            },
        };
        allocations[*slot] = Some(allocation);
    }
    let mut allocations = allocations.into_iter().map(|a| a.expect("every slot allocated"));
    let nodes = specs
        .iter()
        .map(|spec| (spec.kind, allocations.next().expect("a node's allocation")))
        .collect();
    let driver = allocations.next().expect("the driver's allocation");
    Ok(ClusterPlan {
        nodes,
        driver,
        reserved,
        endpoints: Vec::new(),
        proxies: Vec::new(),
    })
}

/// The physical cores a child must exclude to run on exactly its allocation
///
/// # Arguments
///
/// * `allocation` - What it was given
/// * `topology` - The machine
pub fn excluded_for(allocation: &Allocation, topology: &Topology) -> Vec<usize> {
    // a shared allocation excludes nothing and takes what the server picks
    if allocation.shared {
        return Vec::new();
    }
    topology
        .cores
        .keys()
        .copied()
        .filter(|core| !allocation.data.contains(core))
        .collect()
}
