//! Where the control thread runs, checked against what the process is actually allowed
//!
//! [C1](../../../../docs/src/distributed/node-identity.md) asks three things of the control
//! core: that it be validated against the process's real affinity rather than against what is
//! online, that its physical core - both SMT threads - be kept away from the shards where
//! isolation is claimed, and that a machine too small to do that be allowed to share the core
//! *explicitly* and have the sharing recorded. This module is those three.
//!
//! The default is cpu 0, which is the coordinator cpu the shards have always left alone. But
//! only cpu 0: the shards were free to run on its SMT sibling, and the benchmark layout depends
//! on that ([`Resources::cpus`](super::super::conf::Resources::cpus)), so standalone mode keeps it
//! exactly. In cluster mode the sibling goes too, unless `control_core_shared` says the machine
//! cannot afford it.

use glommio::{CpuLocation, CpuSet};

use super::super::conf::Conf;
use super::super::errors::ShoalError;
use super::super::ServerError;

/// Where the control thread is pinned, and what that cost
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlacement {
    /// The cpu the control thread runs on
    pub cpu: usize,
    /// The physical core that cpu is a thread of
    pub core: usize,
    /// Every cpu on that physical core, the control cpu included
    pub siblings: Vec<usize>,
    /// Whether a shard is allowed on the same physical core
    ///
    /// Configured, not observed: `true` means the configuration said so. Whether a shard
    /// actually landed there is [`ControlPlacement::overlaps`].
    pub shared: bool,
    /// The cpus this process may run on at all
    pub allowed: Vec<usize>,
}

impl ControlPlacement {
    /// Work out where the control thread goes for a configuration
    ///
    /// # Arguments
    ///
    /// * `conf` - The configuration, which has to carry a `cluster:` block
    ///
    /// # Errors
    ///
    /// Refuses a control cpu outside the process's affinity, and fails if the cpu topology or
    /// the affinity cannot be read.
    pub fn resolve(conf: &Conf) -> Result<Self, ServerError> {
        // the cpu the configuration asks for; a standalone server never gets here
        let cluster = conf
            .cluster
            .as_ref()
            .ok_or(ServerError::Shoal(ShoalError::NotClustered))?;
        let cpu = cluster.control_core;
        // what this process may actually run on, which a container or a taskset may have
        // narrowed well below what is online
        let allowed = allowed_cpus()?;
        if !allowed.contains(&cpu) {
            return Err(ServerError::Shoal(ShoalError::ControlCoreNotAllowed {
                cpu,
                allowed,
            }));
        }
        // the physical core the cpu is a thread of, and every other thread on it
        let online: Vec<CpuLocation> = CpuSet::online()?.into_iter().collect();
        let location = online
            .iter()
            .find(|location| location.cpu == cpu)
            .ok_or_else(|| std::io::Error::other(format!("cpu {cpu} is allowed but not online")))?;
        let core = location.core;
        let mut siblings: Vec<usize> = online
            .iter()
            .filter(|other| other.core == core && other.package == location.package)
            .map(|other| other.cpu)
            .collect();
        siblings.sort_unstable();
        Ok(ControlPlacement {
            cpu,
            core,
            siblings,
            shared: cluster.control_core_shared,
            allowed,
        })
    }

    /// The physical cores a shard must stay off, which is this one unless sharing was allowed
    pub fn reserved_cores(&self) -> Vec<usize> {
        if self.shared {
            Vec::new()
        } else {
            vec![self.core]
        }
    }

    /// Whether any of these shard cpus is on the control thread's physical core
    ///
    /// # Arguments
    ///
    /// * `shards` - The cpus the shards run on
    pub fn overlaps(&self, shards: &CpuSet) -> bool {
        shards
            .iter()
            .any(|location| self.siblings.contains(&location.cpu))
    }

    /// Refuse a shard set that shares this core without the configuration having allowed it
    ///
    /// # Arguments
    ///
    /// * `shards` - The cpus the shards run on
    ///
    /// # Errors
    ///
    /// Refuses with [`ShoalError::ControlCoreOverlapsShards`] when a shard cpu is a sibling of
    /// the control cpu and sharing was not configured.
    pub fn check_isolation(&self, shards: &CpuSet) -> Result<(), ServerError> {
        if !self.shared && self.overlaps(shards) {
            return Err(ServerError::Shoal(ShoalError::ControlCoreOverlapsShards {
                cpu: self.cpu,
            }));
        }
        Ok(())
    }
}

/// The cpus this process may run on, in ascending order
///
/// Read from `sched_getaffinity`, which is what a cgroup cpuset, a `taskset` or a container
/// runtime narrows. `CpuSet::online` reads sysfs and does not know about any of those.
///
/// # Errors
///
/// Fails if the affinity cannot be read.
pub fn allowed_cpus() -> Result<Vec<usize>, ServerError> {
    // SAFETY: a zeroed `cpu_set_t` is a valid empty set, and `sched_getaffinity` of pid 0 fills
    // in the calling thread's mask, writing at most the size passed
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let rc =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if rc != 0 {
        return Err(ServerError::IO(std::io::Error::last_os_error()));
    }
    // every cpu the mask names, which is bounded by what a `cpu_set_t` can hold
    let allowed = (0..libc::CPU_SETSIZE as usize)
        // SAFETY: `CPU_ISSET` reads the bit for a cpu within the set's own size
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .collect();
    Ok(allowed)
}

#[cfg(test)]
mod tests {
    use super::{allowed_cpus, ControlPlacement};
    use crate::server::conf::{Cluster, Conf};
    use crate::server::errors::ShoalError;
    use crate::server::ServerError;

    /// The affinity this process runs under is a non empty, ascending list that is online
    #[test]
    fn the_affinity_is_read() {
        let allowed = allowed_cpus().expect("failed to read the affinity");
        assert!(!allowed.is_empty());
        assert!(allowed.windows(2).all(|pair| pair[0] < pair[1]));
        let online = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert!(allowed.len() <= online);
    }

    /// The default control core resolves to cpu 0's whole physical core, on a machine allowed it
    #[test]
    fn the_default_control_core_is_cpu_zeros_core() {
        let allowed = allowed_cpus().expect("failed to read the affinity");
        let conf = Conf::default().cluster(Cluster::default().bootstrap(true));
        let placement = ControlPlacement::resolve(&conf);
        // a process not allowed cpu 0 is refused, by name; everything else resolves
        if !allowed.contains(&0) {
            assert!(matches!(
                placement,
                Err(ServerError::Shoal(ShoalError::ControlCoreNotAllowed {
                    cpu: 0,
                    ..
                }))
            ));
            return;
        }
        let placement = placement.expect("failed to resolve the default control core");
        assert_eq!(placement.cpu, 0);
        assert!(placement.siblings.contains(&0));
        assert!(!placement.shared);
        assert_eq!(placement.reserved_cores(), vec![placement.core]);
    }

    /// A control cpu outside the affinity is refused, naming what is allowed
    #[test]
    fn a_cpu_outside_the_affinity_is_refused() {
        // no machine has this many cpus in one `cpu_set_t`
        let conf = Conf::default().cluster(Cluster::default().bootstrap(true).control_core(4095));
        let error = ControlPlacement::resolve(&conf).expect_err("an impossible cpu resolved");
        assert!(matches!(
            error,
            ServerError::Shoal(ShoalError::ControlCoreNotAllowed { cpu: 4095, .. })
        ));
        assert!(format!("{error}").contains("affinity"), "{error}");
    }

    /// A standalone configuration has no control core to resolve
    #[test]
    fn standalone_has_no_placement() {
        assert!(matches!(
            ControlPlacement::resolve(&Conf::default()),
            Err(ServerError::Shoal(ShoalError::NotClustered))
        ));
    }
}
