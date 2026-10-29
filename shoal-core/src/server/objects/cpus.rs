//! Where a node's object executors run: cpus of their own, which no shard holds
//!
//! Object work never runs on a table shard's executor: X9 found a step of a 1 MiB unit on the
//! table shards moved a table's read p99 two to four times, and the same work on a core of its own
//! moved it nothing measurable ([X9](../../../../docs/src/object-storage/table-latency.md#recommendation)).
//! So the cpus it runs on are named in `resources.object_cpus`, kept off the shards one thread at a
//! time, and refused by name where they cannot be had. A cluster node may name its control core's
//! other thread, which the control thread's whole-core reservation leaves idle, as the lab does
//! ([F85](../../../../docs/src/features/devices-and-pools.md)).

use glommio::{CpuLocation, CpuSet};

use crate::server::conf::Conf;
use crate::server::control::cores::ControlPlacement;
use crate::server::errors::ShoalError;
use crate::server::ServerError;

/// One object executor's place
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectCpu {
    /// The cpu its reactor runs on
    pub cpu: usize,
    /// The cpu its blocking thread runs on, where every rename and unlink goes: the other thread
    /// of its core when no shard holds that, and its own cpu otherwise
    pub blocking: usize,
}

/// Refuse an object cpu by name
///
/// # Arguments
///
/// * `cpu` - The cpu
/// * `why` - Why it cannot be one
fn refuse(cpu: usize, why: impl Into<String>) -> ServerError {
    ServerError::Shoal(ShoalError::ObjectCpuRefused {
        cpu,
        why: why.into(),
    })
}

/// Resolve and check where a node's object executors run
///
/// # Arguments
///
/// * `conf` - The node's configuration
/// * `control` - Where its control thread runs, on a cluster node
/// * `shards` - The cpus its shards run on
///
/// # Errors
///
/// Devices with no object cpu, or an object cpu that is cpu 0, the control cpu, a shard's, not
/// online or named twice; or a core count the shards can no longer have.
///
/// A cpu is judged as a shard's is, by whether it is online, and not by the starting thread's
/// affinity: the lab starts a benchmark's process under `taskset` for its client, and glommio pins
/// every executor past that mask. A cpu a cgroup forbids fails when its executor is pinned, by name.
pub fn resolve(
    conf: &Conf,
    control: Option<&ControlPlacement>,
    shards: &CpuSet,
) -> Result<Vec<ObjectCpu>, ServerError> {
    let named = &conf.resources.object_cpus;
    // a device needs an executor to own its slices
    if named.is_empty() {
        if !conf.storage.devices.is_empty() {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "storage.devices names {} devices and resources.object_cpus names no cpu; object \
                 work runs on cpus of its own, never on a table shard's",
                conf.storage.devices.len()
            ))));
        }
        return Ok(Vec::new());
    }
    let online: Vec<CpuLocation> = CpuSet::online()?.into_iter().collect();
    let shard_cpus: Vec<usize> = shards.iter().map(|location| location.cpu).collect();
    let mut resolved = Vec::with_capacity(named.len());
    for (index, cpu) in named.iter().copied().enumerate() {
        if cpu == 0 {
            return Err(refuse(cpu, "cpu 0 is the node's coordinator, which no executor of its own takes"));
        }
        if named[..index].contains(&cpu) {
            return Err(refuse(cpu, "it is named twice"));
        }
        if control.is_some_and(|control| control.cpu == cpu) {
            return Err(refuse(cpu, "the control thread runs on it"));
        }
        if shard_cpus.contains(&cpu) {
            return Err(refuse(cpu, "a shard runs on it"));
        }
        let Some(location) = online.iter().find(|location| location.cpu == cpu) else {
            return Err(refuse(cpu, "it is not online"));
        };
        // the blocking thread on the other thread of its core, unless a shard holds that
        let blocking = online
            .iter()
            .filter(|other| {
                other.core == location.core
                    && other.package == location.package
                    && other.cpu != cpu
                    && !shard_cpus.contains(&other.cpu)
            })
            .map(|other| other.cpu)
            .min()
            .unwrap_or(cpu);
        resolved.push(ObjectCpu { cpu, blocking });
    }
    // the rows keep some of the node's memory once object work has its share
    conf.resources
        .check_object_memory()
        .map_err(|why| ServerError::Shoal(ShoalError::InvalidConfig(why)))?;
    // and the shards still have the cores they were asked for
    if let Some(cores) = conf.resources.cores {
        if shard_cpus.len() < cores {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "resources.cores asks for {cores} shards and only {} cpus are left once \
                 resources.object_cpus {named:?} are kept off them",
                shard_cpus.len()
            ))));
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::resolve;
    use crate::server::conf::Conf;
    use crate::server::errors::ShoalError;
    use crate::server::ServerError;
    use crate::shared::devices::DeviceSpec;
    use glommio::CpuSet;

    /// A configuration naming object cpus, and devices beside them when asked
    ///
    /// # Arguments
    ///
    /// * `cpus` - The object cpus
    /// * `devices` - Whether a device is named
    fn conf(cpus: &[usize], devices: bool) -> Conf {
        let mut conf = Conf::default();
        conf.resources.object_cpus = cpus.to_vec();
        if devices {
            conf.storage.devices = vec![DeviceSpec::new("/nowhere/pool", "ssd")];
        }
        conf
    }

    /// The cpus a test's shards stand on: the one named, if it is online
    ///
    /// # Arguments
    ///
    /// * `cpu` - The shard's cpu
    fn shards_on(cpu: usize) -> CpuSet {
        CpuSet::online().expect("online cpus").filter(|location| location.cpu == cpu)
    }

    /// The highest online cpu, which no test's shard holds
    fn highest() -> usize {
        CpuSet::online()
            .expect("online cpus")
            .into_iter()
            .map(|location| location.cpu)
            .max()
            .expect("a cpu")
    }

    /// Every object cpu that cannot be had is refused by name: devices with none, cpu 0, one named
    /// twice, a shard's, one that is not online; and one that can resolves with its blocking pool
    /// off the shards
    #[test]
    fn object_cpus_are_refused_by_name() {
        let shards = shards_on(1);
        // devices need an executor to own their slices
        let error = resolve(&conf(&[], true), None, &shards).expect_err("devices with no cpu resolved");
        assert!(format!("{error}").contains("object_cpus"), "{error}");
        // and no devices need none
        assert!(resolve(&conf(&[], false), None, &shards).expect("resolved").is_empty());
        // each refusal names its cpu
        let refused = |cpus: &[usize]| match resolve(&conf(cpus, true), None, &shards) {
            Err(ServerError::Shoal(ShoalError::ObjectCpuRefused { cpu, why })) => (cpu, why),
            other => panic!("{cpus:?} was not refused by name: {other:?}"),
        };
        assert!(refused(&[0]).1.contains("coordinator"));
        let top = highest();
        if top >= 2 {
            assert_eq!(refused(&[top, top]), (top, "it is named twice".to_string()));
            assert_eq!(refused(&[1]), (1, "a shard runs on it".to_string()));
        }
        assert_eq!(refused(&[4095]), (4095, "it is not online".to_string()));
        // a cpu that can be had resolves, its blocking pool never on a shard's cpu
        if top >= 2 {
            let resolved = resolve(&conf(&[top], true), None, &shards).expect("resolved");
            assert_eq!(resolved.len(), 1);
            assert_eq!(resolved[0].cpu, top);
            assert_ne!(resolved[0].blocking, 1);
        }
    }
}
