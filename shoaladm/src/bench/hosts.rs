//! What the bench reads from and does to the hosts, and how it puts them back
//!
//! Every script is built by a pure function, so what it would run on a host can be read in a
//! test without a host. Everything the bench changes - a unit it stopped, a governor it set, a
//! drop-in that keeps a killed node down - is recorded in [`Restore`] as it is changed, and put
//! back on every way out of a run: an explicit [`Restore::finish`] on success and failure, and
//! the same scripts from `Drop` if a panic skipped that.

use color_eyre::eyre::eyre;
use shoal::serde_json::Value;
use shoal_loadgen::results::HostFacts;
use std::collections::BTreeMap;

use crate::deploy::remote::{quote, Host};

/// The script that prints a host's facts as `key=value` lines
pub const FACTS_SCRIPT: &str = "\
echo hostname=$(hostname); \
echo cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//'); \
echo cores=$(nproc); \
echo memory=$(awk '/MemTotal/ {printf \"%d\", $2 * 1024}' /proc/meminfo); \
echo governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo none); \
echo kernel=$(uname -r)";

/// Read a host's facts from what [`FACTS_SCRIPT`] printed
///
/// # Arguments
///
/// * `output` - What it printed
#[must_use]
pub fn parse_facts(output: &str) -> HostFacts {
    // each line a key and its value
    let facts: BTreeMap<&str, &str> = output
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    let text = |key: &str| facts.get(key).map(|value| value.trim().to_string()).unwrap_or_default();
    HostFacts {
        hostname: text("hostname"),
        cpu: text("cpu"),
        cores: text("cores").parse().unwrap_or(0),
        memory_bytes: text("memory").parse().unwrap_or(0),
        governor: text("governor"),
        kernel: text("kernel"),
    }
}

/// This machine's facts
///
/// # Errors
///
/// When the script cannot be run.
pub fn local_facts() -> color_eyre::Result<HostFacts> {
    // the same script, run here
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(FACTS_SCRIPT)
        .output()?;
    Ok(parse_facts(&String::from_utf8_lossy(&output.stdout)))
}

/// A host's facts
///
/// # Arguments
///
/// * `host` - The host
///
/// # Errors
///
/// When it cannot be reached.
pub fn probe(host: &Host) -> color_eyre::Result<HostFacts> {
    Ok(parse_facts(&host.run(FACTS_SCRIPT)?))
}

/// What else is running on a host the bench is about to use
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Neighbours {
    /// Active shoal units other than the bench's own
    pub units: Vec<String>,
    /// The bench's ports something already listens on
    pub ports: Vec<u16>,
}

/// The script that lists active shoal units and which of some ports are listened on
///
/// # Arguments
///
/// * `ports` - The ports the bench is about to bind
#[must_use]
pub fn neighbours_script(ports: &[u16]) -> String {
    // the units by name, then each port that is already bound
    let ports = ports
        .iter()
        .map(|port| format!("ss -Hltn 'sport = :{port}' | grep -q . && echo port={port}"))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "systemctl list-units --type=service --state=active --no-legend --plain 'shoal-*' \
         | awk '{{print \"unit=\" $1}}'; {ports}; true"
    )
}

/// Read what [`neighbours_script`] printed, leaving out the bench's own unit
///
/// # Arguments
///
/// * `output` - What it printed
/// * `own` - The bench's own unit
#[must_use]
pub fn parse_neighbours(output: &str, own: &str) -> Neighbours {
    let mut neighbours = Neighbours::default();
    for line in output.lines() {
        match line.split_once('=') {
            Some(("unit", unit)) if unit != own => neighbours.units.push(unit.to_string()),
            Some(("port", port)) => {
                if let Ok(port) = port.parse() {
                    neighbours.ports.push(port);
                }
            }
            _ => (),
        }
    }
    neighbours
}

/// The script that stops one unit, and only that unit
///
/// # Arguments
///
/// * `unit` - The unit
#[must_use]
pub fn stop_unit_script(unit: &str) -> String {
    format!("sudo -n systemctl stop {}", quote(unit))
}

/// The script that starts one unit and says whether it is active
///
/// # Arguments
///
/// * `unit` - The unit
#[must_use]
pub fn start_unit_script(unit: &str) -> String {
    format!(
        "sudo -n systemctl start {unit}; sleep 2; systemctl is-active {unit}",
        unit = quote(unit)
    )
}

/// The script that sets every cpu's governor
///
/// `cpupower` where it is installed, the sysfs files where it is not.
///
/// # Arguments
///
/// * `governor` - The governor
#[must_use]
pub fn governor_script(governor: &str) -> String {
    let governor = quote(governor);
    format!(
        "if command -v cpupower >/dev/null 2>&1; then sudo -n cpupower frequency-set -g {governor} >/dev/null; \
         else for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo {governor} | sudo -n tee \"$f\" >/dev/null; done; fi; \
         cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
    )
}

/// Where the drop-in that keeps a killed node down lives
///
/// A unit restarts itself five seconds after a crash (`Restart=on-failure`), which would end a
/// kill before the arm could see it; under `/run` the drop-in is gone after a reboot whatever
/// happens to the bench.
///
/// # Arguments
///
/// * `unit` - The unit
#[must_use]
pub fn dropin_path(unit: &str) -> String {
    format!("/run/systemd/system/{unit}.d/shoal-bench.conf")
}

/// The script that removes the drop-in and has systemd read the unit again
///
/// # Arguments
///
/// * `unit` - The unit
#[must_use]
pub fn remove_dropin_script(unit: &str) -> String {
    // the file, then its directory if nothing else is in it, so nothing of the bench is left
    let path = dropin_path(unit);
    let dir = path.rsplit_once('/').map(|(dir, _)| dir.to_string()).unwrap_or_default();
    format!(
        "sudo -n rm -f {path}; sudo -n rmdir {dir} 2>/dev/null; sudo -n systemctl daemon-reload",
        path = quote(&path),
        dir = quote(&dir)
    )
}

/// The script that reports each storage root's marker, for the wipe guard
///
/// One line a root: `root\tmissing`, `root\tempty`, `root\tother` for a directory holding
/// files but no marker, or `root\tmarker\t<json>`.
///
/// # Arguments
///
/// * `roots` - The roots
#[must_use]
pub fn markers_script(roots: &[String]) -> String {
    roots
        .iter()
        .map(|root| {
            let path = quote(root);
            let marker = quote(&format!("{root}/shoal-meta.json"));
            format!(
                "if [ ! -e {path} ]; then printf '%s\\tmissing\\n' {path}; \
                 elif sudo -n test -e {marker}; then printf '%s\\tmarker\\t' {path}; sudo -n cat {marker} | tr -d '\\n'; echo; \
                 elif [ -z \"$(sudo -n ls -A {path} 2>/dev/null)\" ]; then printf '%s\\tempty\\n' {path}; \
                 else printf '%s\\tother\\n' {path}; fi"
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Judge whether a node's roots may be wiped, from what [`markers_script`] printed
///
/// A root may be wiped when nothing is there, or when its marker names the bench's own cluster
/// or the bench's own node. A root holding anything else - another cluster's marker, files
/// with no marker - is refused by name, whatever the inventory says.
///
/// # Arguments
///
/// * `node` - The node's name
/// * `output` - What the script printed
/// * `cluster` - The bench's recorded cluster, if it has one
/// * `own_node` - The id the bench recorded for this node, if it has one
#[must_use]
pub fn judge_wipe(node: &str, output: &str, cluster: Option<&str>, own_node: Option<&str>) -> Vec<String> {
    let mut refused = Vec::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let mut parts = line.splitn(3, '\t');
        let root = parts.next().unwrap_or_default();
        match (parts.next(), parts.next()) {
            (Some("missing" | "empty"), _) => (),
            (Some("marker"), Some(json)) => {
                // a marker is the bench's when it names the bench's cluster or the bench's node
                let marker: Value = shoal::serde_json::from_str(json).unwrap_or(Value::Null);
                let theirs_cluster = marker.get("cluster").and_then(Value::as_str);
                let theirs_node = marker.get("node").and_then(Value::as_str);
                let ours = (cluster.is_some() && theirs_cluster == cluster)
                    || (own_node.is_some() && theirs_node == own_node);
                if !ours {
                    refused.push(format!(
                        "{node}: {root} holds a node of cluster {} that is not the bench's; \
                         the bench never wipes it",
                        theirs_cluster.unwrap_or("none")
                    ));
                }
            }
            (Some("other"), _) => refused.push(format!(
                "{node}: {root} holds files but no shoal marker; the bench never wipes it"
            )),
            _ => refused.push(format!("{node}: could not read what {root} holds: {line:?}")),
        }
    }
    refused
}

/// One change the bench made to a host, and how to undo it
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A unit it stopped, started again on the way out
    StoppedUnit {
        /// The host
        target: String,
        /// The unit
        unit: String,
    },
    /// A governor it set, put back to what it was
    Governor {
        /// The host
        target: String,
        /// What it was
        was: String,
    },
    /// A drop-in it wrote, removed
    DropIn {
        /// The host
        target: String,
        /// The unit it is for
        unit: String,
    },
}

impl Change {
    /// The host the change was made on, and the script that undoes it
    #[must_use]
    pub fn undo(&self) -> (Host, String) {
        // each change's own way back
        match self {
            Change::StoppedUnit { target, unit } => (Host { target: target.clone() }, start_unit_script(unit)),
            Change::Governor { target, was } => (Host { target: target.clone() }, governor_script(was)),
            Change::DropIn { target, unit } => (Host { target: target.clone() }, remove_dropin_script(unit)),
        }
    }
}

/// Everything the bench changed on the hosts, undone on every way out
#[derive(Default)]
pub struct Restore {
    /// The changes, in the order they were made
    changes: Vec<Change>,
    /// The bench's own cluster's teardown, run before anything else is put back
    teardown: Option<Box<dyn FnOnce() -> color_eyre::Result<()> + Send>>,
    /// Whether everything has been put back
    finished: bool,
}

impl std::fmt::Debug for Restore {
    /// The changes, and whether a teardown is waiting
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Restore")
            .field("changes", &self.changes)
            .field("teardown", &self.teardown.is_some())
            .field("finished", &self.finished)
            .finish()
    }
}

impl Restore {
    /// Record a change, so it is undone on the way out
    ///
    /// # Arguments
    ///
    /// * `change` - The change
    pub fn record(&mut self, change: Change) {
        self.changes.push(change);
    }

    /// Forget a change that was undone on purpose, such as a drop-in removed to restart a node
    ///
    /// # Arguments
    ///
    /// * `change` - The change
    pub fn forget(&mut self, change: &Change) {
        self.changes.retain(|known| known != change);
    }

    /// Set what tears the bench's cluster down, run first on the way out
    ///
    /// # Arguments
    ///
    /// * `teardown` - What tears it down
    pub fn on_teardown(&mut self, teardown: impl FnOnce() -> color_eyre::Result<()> + Send + 'static) {
        self.teardown = Some(Box::new(teardown));
    }

    /// The changes still to be undone
    #[must_use]
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Undo everything, newest first, carrying on past a failure and reporting each one
    ///
    /// A unit that was stopped is started last, after the bench's cluster is torn down and the
    /// governors are back, so it never shares its host with the bench.
    pub fn finish(&mut self) -> Vec<String> {
        // only once
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut failures = Vec::new();
        if let Some(teardown) = self.teardown.take() {
            if let Err(error) = teardown() {
                failures.push(format!("tearing the bench's cluster down: {error}"));
            }
        }
        // drop-ins and governors first, then the units, newest first within each
        let mut changes = std::mem::take(&mut self.changes);
        changes.reverse();
        changes.sort_by_key(|change| matches!(change, Change::StoppedUnit { .. }));
        for change in changes {
            let (host, script) = change.undo();
            match host.run(&script) {
                Ok(output) => {
                    if let Change::StoppedUnit { unit, .. } = &change {
                        if output.lines().last().map(str::trim) != Some("active") {
                            failures.push(format!("{unit} on {} did not come back active: {output}", host.target));
                        }
                    }
                }
                Err(error) => failures.push(format!("undoing {change:?}: {error}")),
            }
        }
        failures
    }
}

impl Drop for Restore {
    /// Put the hosts back if nothing did
    fn drop(&mut self) {
        // a panic or an early return skipped finish; say what failed, since nothing else will
        if !self.finished {
            for failure in self.finish() {
                eprintln!("shoaladm bench: {failure}");
            }
        }
    }
}

/// Stop a unit on a host, recording it to be started again
///
/// # Arguments
///
/// * `restore` - Where the change is recorded
/// * `host` - The host
/// * `unit` - The unit
///
/// # Errors
///
/// When it cannot be stopped.
pub fn stop_unit(restore: &mut Restore, host: &Host, unit: &str) -> color_eyre::Result<()> {
    // recorded first, so a stop that half happened is still started again
    restore.record(Change::StoppedUnit {
        target: host.target.clone(),
        unit: unit.to_string(),
    });
    host.run(&stop_unit_script(unit))
        .map(|_| ())
        .map_err(|error| eyre!("stopping {unit} on {}: {error}", host.target))
}

/// Set a host's governor, recording what it was
///
/// # Arguments
///
/// * `restore` - Where the change is recorded
/// * `host` - The host
/// * `was` - The governor it had
/// * `governor` - The governor to set
///
/// # Errors
///
/// When it cannot be set, or reads back as something else.
pub fn set_governor(restore: &mut Restore, host: &Host, was: &str, governor: &str) -> color_eyre::Result<()> {
    // a host with no governor has nothing to set or put back
    if was == "none" || was.is_empty() {
        return Ok(());
    }
    restore.record(Change::Governor {
        target: host.target.clone(),
        was: was.to_string(),
    });
    let now = host.run(&governor_script(governor))?;
    if now.trim() != governor {
        return Err(eyre!("{} reads governor {} after setting {governor}", host.target, now.trim()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        governor_script, judge_wipe, markers_script, parse_facts, parse_neighbours, remove_dropin_script,
        start_unit_script, stop_unit_script, Change, Restore,
    };

    /// Facts parse from their lines, whatever the order
    #[test]
    fn facts_parse_from_lines() {
        let facts = parse_facts(
            "cores=8\nhostname=titan\ncpu=AMD Ryzen 7 1700 Eight-Core Processor\nmemory=33554432000\ngovernor=schedutil\nkernel=6.8.0\n",
        );
        assert_eq!(facts.hostname, "titan");
        assert_eq!(facts.cores, 8);
        assert_eq!(facts.memory_bytes, 33_554_432_000);
        assert_eq!(facts.governor, "schedutil");
    }

    /// The bench's own unit is not a neighbour, and a bound port is
    #[test]
    fn neighbours_leave_out_the_bench_itself() {
        let neighbours = parse_neighbours(
            "unit=shoal-tmdb.service\nunit=shoal-tmdb-bench.service\nport=12100\n",
            "shoal-tmdb-bench.service",
        );
        assert_eq!(neighbours.units, vec!["shoal-tmdb.service"]);
        assert_eq!(neighbours.ports, vec![12100]);
    }

    /// A unit script names the one unit it was given and nothing that removes it
    #[test]
    fn unit_scripts_touch_only_their_unit() {
        for script in [stop_unit_script("shoal-tmdb.service"), start_unit_script("shoal-tmdb.service")] {
            assert!(script.contains("shoal-tmdb.service"));
            assert!(!script.contains("disable") && !script.contains("rm "), "{script}");
        }
        let remove = remove_dropin_script("shoal-tmdb-bench.service");
        assert!(remove.contains("rm -f /run/systemd/system/shoal-tmdb-bench.service.d/shoal-bench.conf"));
        // the directory goes too, but only empty: rmdir never removes another drop-in
        assert!(remove.contains("rmdir /run/systemd/system/shoal-tmdb-bench.service.d"));
        assert!(!remove.contains("rm -rf"));
        assert!(governor_script("performance").contains("cpupower frequency-set -g performance"));
    }

    /// Only a root that is empty, missing or the bench's own is wiped
    #[test]
    fn only_the_benchs_own_roots_are_wiped() {
        let ours = r#"{"node":"n1","cluster":"c1"}"#;
        let theirs = r#"{"node":"n9","cluster":"c9"}"#;
        let joining = r#"{"node":"n1","cluster":null}"#;
        let output = format!(
            "/a\tmissing\n/b\tempty\n/c\tmarker\t{ours}\n/d\tmarker\t{joining}\n"
        );
        assert!(judge_wipe("titan", &output, Some("c1"), Some("n1")).is_empty());
        let output = format!("/e\tmarker\t{theirs}\n/f\tother\n");
        let refused = judge_wipe("titan", &output, Some("c1"), Some("n1"));
        assert_eq!(refused.len(), 2, "{refused:?}");
        assert!(refused[0].contains("c9"));
        // with no record at all, any marker is someone else's
        assert_eq!(judge_wipe("titan", &format!("/c\tmarker\t{ours}\n"), None, None).len(), 1);
        // the script reports every root
        let script = markers_script(&["/optane/shoal-bench".to_string(), "/x y".to_string()]);
        assert!(script.contains("/optane/shoal-bench/shoal-meta.json") && script.contains("'/x y'"));
    }

    /// A finished restore runs nothing again, and a stopped unit is started after the rest
    #[test]
    fn a_restore_finishes_once_and_starts_units_last() {
        let mut restore = Restore::default();
        let drop_in = Change::DropIn {
            target: "nowhere.invalid".to_string(),
            unit: "u".to_string(),
        };
        restore.record(Change::StoppedUnit {
            target: "nowhere.invalid".to_string(),
            unit: "shoal-tmdb.service".to_string(),
        });
        restore.record(drop_in.clone());
        restore.forget(&drop_in);
        assert_eq!(restore.changes().len(), 1);
        // nothing is reachable, so finishing reports the failure, once
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();
        restore.on_teardown(move || {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        let failures = restore.finish();
        assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(failures.len(), 1);
        assert!(restore.finish().is_empty());
    }
}
