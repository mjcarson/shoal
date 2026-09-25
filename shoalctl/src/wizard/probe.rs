//! Asking a host what it has before a node is placed on it
//!
//! One ssh round trip in batch mode, the way preflight reaches a host, that reports the host's
//! cpus and memory, the room on the filesystem each of the node's storage directories would be
//! made on, and whether a node's marker is already in one. It only reads: nothing is created and
//! no sudo is asked for, so a probe is safe to run on a host before anything is decided.

use crate::deploy::inventory::{dialable, lookup};
use crate::deploy::remote::{Host, quote};

use super::form::{ProbeReport, Resolution};

/// Resolve a node's name the way bootstrap will, blocking until the resolver answers
///
/// # Arguments
///
/// * `name` - The node's name
#[must_use]
pub fn resolution(name: &str) -> Resolution {
    // every address it has here, then the one a peer could dial
    match lookup(name) {
        Ok(addresses) => dialable(&addresses).map_or(Resolution::Loopback, Resolution::Resolved),
        Err(error) => Resolution::Failed(format!("{error:#}")),
    }
}

/// The script a probe runs on a host
///
/// # Arguments
///
/// * `roots` - The node's storage directories
#[must_use]
pub fn script(roots: &[String]) -> String {
    // the host's size as key=value lines
    let mut script = String::from(
        "echo cpus=$(nproc); \
         echo mem=$(awk '/^MemTotal:/ {print $2 * 1024}' /proc/meminfo); ",
    );
    // then, for each directory, the free bytes under its nearest existing ancestor, and whether
    // it already holds a marker; tab separated, since a directory may hold a space
    for root in roots {
        script.push_str(&format!(
            "d={root}; p=$d; while [ ! -e \"$p\" ]; do p=$(dirname \"$p\"); done; \
             printf 'free=%s\\t%s\\n' \"$d\" \"$(df -Pk \"$p\" | awk 'NR==2 {{print $4 * 1024}}')\"; \
             if [ -e \"$d/shoal-meta.json\" ]; then printf 'claimed=%s\\n' \"$d\"; fi; ",
            root = quote(root),
        ));
    }
    script
}

/// Read what a probe's script printed
///
/// # Arguments
///
/// * `output` - What the script printed
#[must_use]
pub fn parse(output: &str) -> ProbeReport {
    let mut report = ProbeReport::default();
    // every line is a key and a value, and anything else is ignored
    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "cpus" => report.cpus = value.trim().parse().ok(),
            "mem" => report.memory = value.trim().parse().ok(),
            "free" => {
                if let Some((root, free)) = value.split_once('\t') {
                    report
                        .free
                        .push((root.to_string(), free.trim().parse().ok()));
                }
            }
            "claimed" => report.claimed.push(value.to_string()),
            _ => (),
        }
    }
    report
}

/// Probe a host over ssh, blocking until it answers
///
/// # Arguments
///
/// * `target` - What ssh is given
/// * `roots` - The node's storage directories
///
/// # Errors
///
/// When ssh cannot reach the host, or the script fails there.
pub fn probe(target: &str, roots: &[String]) -> color_eyre::Result<ProbeReport> {
    // one round trip, in batch mode so a host that would prompt is refused rather than hanging
    let host = Host {
        target: target.to_string(),
    };
    let output = host.run(&script(roots))?;
    Ok(parse(&output))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the script prints is read back into a report, and the script runs on this machine
    #[test]
    fn a_probe_reads_what_its_script_prints() {
        // a directory that exists and one that does not, whose ancestor is measured instead
        let here = std::env::temp_dir().display().to_string();
        let missing = format!("{here}/shoal-probe-test-missing/data");
        let roots = vec![here.clone(), missing.clone()];
        // run the script locally, which is what ssh would run on a host
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(script(&roots))
            .output()
            .expect("sh");
        let report = parse(&String::from_utf8_lossy(&output.stdout));
        // the host's size, and room under both directories
        assert!(report.cpus.is_some_and(|cpus| cpus > 0), "{report:?}");
        assert!(report.memory.is_some_and(|memory| memory > 0), "{report:?}");
        assert_eq!(report.free.len(), 2, "{report:?}");
        assert_eq!(report.free[1].0, missing);
        assert!(
            report.free.iter().all(|(_, free)| free.is_some()),
            "{report:?}"
        );
        assert!(report.claimed.is_empty());
        // a marker is reported by the directory it is in
        let parsed = parse("cpus=8\nmem=1024\nfree=/mnt/a b\t2048\nclaimed=/mnt/a b\n");
        assert_eq!(parsed.free, vec![("/mnt/a b".to_string(), Some(2048))]);
        assert_eq!(parsed.claimed, vec!["/mnt/a b".to_string()]);
    }

    /// A name is resolved as bootstrap resolves it, and loopback is told apart from nothing
    #[test]
    fn a_resolution_tells_loopback_from_nothing() {
        // localhost resolves, to nothing a peer can dial
        assert_eq!(resolution("localhost"), Resolution::Loopback);
        // an address is its own answer
        assert_eq!(
            resolution("10.0.0.1"),
            Resolution::Resolved("10.0.0.1".parse().unwrap())
        );
        // and a name under the reserved .invalid domain never resolves
        assert!(matches!(
            resolution("no-such-host.invalid"),
            Resolution::Failed(_)
        ));
    }
}
