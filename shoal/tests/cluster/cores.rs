//! Explicit core allocation, recorded whether or not the machine can honor it
//!
//! [C10](../../../docs/src/distributed/performance.md) asks that every node's resource
//! allocation be recorded, that data, control and driver cores be disjoint where isolation is
//! claimed, and that sharing be recorded rather than hidden when the machine cannot support it.
//! Cores are handed out as whole physical cores, both SMT threads of each, because
//! `Resources::exclude_cores` excludes by physical core id and because a claim of isolation
//! that left a sibling thread to someone else would not be one.
//!
//! Since M1 a server is a cluster node with a control thread, and the allocator gives each one
//! a whole core for it, disjoint from its data cores, the reserved core and every other node's.
//! A machine that runs out of cores for one records the control thread as shared rather than
//! failing, which is what the server itself does with `control_core_shared`.
//!
//! Since item 328 a cluster is allocated around the clusters this process already runs: every
//! started cluster holds its cores in one count for the process until it drops, and a counted
//! claim or a control core takes the cores held least, so clusters started beside each other get
//! disjoint cores while the machine has them and spread their overlap evenly once it has not, each
//! overlap recorded in its plan. Before, every cluster was allocated as if it ran alone, and six
//! tests at once pinned their nodes to the same lowest cores while the rest of the machine idled.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};

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
    /// The physical core the node's control thread owns; none for a mock peer, a standalone
    /// node, or a server the machine could not give one, whose control thread is then shared
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
    /// The cores this plan was given that another live cluster of this process held when it was
    /// allocated, recorded rather than hidden (item 328)
    pub overlap: Vec<usize>,
}

impl ClusterPlan {
    /// Every physical core the plan's nodes and driver were given, data and control, each once
    pub fn cores(&self) -> Vec<usize> {
        let mut cores: Vec<usize> = self
            .nodes
            .iter()
            .map(|(_, allocation)| allocation)
            .chain(std::iter::once(&self.driver))
            .flat_map(|allocation| allocation.data.iter().chain(allocation.control.iter()).copied())
            .collect();
        cores.sort_unstable();
        cores.dedup();
        cores
    }

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
            // the data cores and the control core alike: a control thread on a data core is
            // sharing, and is recorded as such rather than claimed
            for core in allocation.data.iter().chain(allocation.control.iter()) {
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
/// Then every cluster server gets a control core from what remains, or none - recorded as a
/// shared control thread - when the machine has no more to give. Control cores come last so
/// that the data cores a claim names are exactly the ones an M0 plan gave it.
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
    allocate_among(specs, driver, topology, &BTreeMap::new())
}

/// Hand out cores around the ones other live clusters hold (item 328)
///
/// As [`allocate`], but counted claims and control cores take the cores `held` names least, then
/// the lowest, so a cluster beside others gets cores none of them holds while the machine has
/// them, and an even share once it has not. Every core the plan takes that `held` names is
/// recorded in its `overlap`. An exact claim takes its cores whoever holds them, as it always did.
///
/// # Arguments
///
/// * `specs` - The nodes
/// * `driver` - The driver's claim
/// * `topology` - The machine
/// * `held` - How many other live clusters hold each core
pub fn allocate_among(
    specs: &[NodeSpec],
    driver: &CoreClaim,
    topology: &Topology,
    held: &BTreeMap<usize, usize>,
) -> Result<ClusterPlan, FixtureError> {
    let reserved = topology.reserved();
    // everything but the reserved core is available, the cores held least first
    let mut free: Vec<usize> = topology
        .cores
        .keys()
        .copied()
        .filter(|core| Some(*core) != reserved)
        .collect();
    free.sort_by_key(|core| (held.get(core).copied().unwrap_or(0), *core));
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
                match free.iter().position(|candidate| candidate == core) {
                    Some(at) => {
                        free.remove(at);
                    }
                    None => {
                        return Err(FixtureError::Allocation(format!(
                            "core {core} is not free to be claimed exactly"
                        )))
                    }
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
                    // the cores held least, which a cluster running alone takes in order
                    let cores: Vec<usize> = free.drain(..*count).collect();
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
    // then a control core for every cluster server, from whatever is left
    for (slot, spec) in specs.iter().enumerate() {
        if spec.kind != NodeKind::Server {
            continue;
        }
        let allocation = allocations[slot].as_mut().expect("every slot allocated");
        // the first free core, if there is one; a node without one shares its control thread
        allocation.control = (!free.is_empty()).then(|| free.remove(0));
    }
    let mut allocations = allocations
        .into_iter()
        .map(|a| a.expect("every slot allocated"));
    let nodes = specs
        .iter()
        .map(|spec| (spec.kind, allocations.next().expect("a node's allocation")))
        .collect();
    let driver = allocations.next().expect("the driver's allocation");
    let mut plan = ClusterPlan {
        nodes,
        driver,
        reserved,
        endpoints: Vec::new(),
        proxies: Vec::new(),
        overlap: Vec::new(),
    };
    // what it shares with the clusters beside it, said
    plan.overlap = plan
        .cores()
        .into_iter()
        .filter(|core| held.get(core).is_some_and(|holders| *holders > 0))
        .collect();
    Ok(plan)
}

/// How many live clusters of this fixture process hold each physical core (item 328)
static HELD: Mutex<BTreeMap<usize, usize>> = Mutex::new(BTreeMap::new());

/// A started cluster's hold on its cores, given back when it drops
#[derive(Debug)]
pub struct CoreLease {
    /// The count it holds them in
    held: &'static Mutex<BTreeMap<usize, usize>>,
    /// The cores it holds, each once
    cores: Vec<usize>,
}

impl CoreLease {
    /// The cores held, each once
    pub fn cores(&self) -> &[usize] {
        &self.cores
    }
}

impl Drop for CoreLease {
    /// Give the cores back, so the next cluster may take them first
    fn drop(&mut self) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        for core in &self.cores {
            // one fewer holder, and none kept at zero
            if let Some(holders) = held.get_mut(core) {
                *holders = holders.saturating_sub(1);
                if *holders == 0 {
                    held.remove(core);
                }
            }
        }
    }
}

/// Allocate a cluster around the ones this process already runs, and hold what it was given
/// until its lease drops (item 328)
///
/// # Arguments
///
/// * `specs` - The nodes
/// * `driver` - The driver's claim
/// * `topology` - The machine
pub fn lease(
    specs: &[NodeSpec],
    driver: &CoreClaim,
    topology: &Topology,
) -> Result<(ClusterPlan, CoreLease), FixtureError> {
    lease_from(&HELD, specs, driver, topology)
}

/// Allocate around the cores a count holds, and hold what was given in it until the lease drops
///
/// The count is locked from the read to the hold, so two clusters starting at once never both
/// take a core as unheld.
///
/// # Arguments
///
/// * `held` - The count, the process's or a test's own
/// * `specs` - The nodes
/// * `driver` - The driver's claim
/// * `topology` - The machine
pub fn lease_from(
    held: &'static Mutex<BTreeMap<usize, usize>>,
    specs: &[NodeSpec],
    driver: &CoreClaim,
    topology: &Topology,
) -> Result<(ClusterPlan, CoreLease), FixtureError> {
    let mut counts = held.lock().unwrap_or_else(PoisonError::into_inner);
    let plan = allocate_among(specs, driver, topology, &counts)?;
    // every core the plan took, held once by this cluster
    let cores = plan.cores();
    for core in &cores {
        *counts.entry(*core).or_insert(0) += 1;
    }
    Ok((plan, CoreLease { held, cores }))
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
