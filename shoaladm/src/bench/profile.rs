//! A profile build's heap dumps, brought back from every node ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! A profile node writes `heap.<pid>.<seq>.i<n>.heap` into its working directory, the
//! deployment's directory on its host: one every 2 GiB allocated and one when it exits. They are
//! brought back before every reset, since a reset stops the node and the next one starts another
//! process, and before the cluster is torn down, since a teardown deletes the directory. Each is
//! moved off its host as it is copied, so a dump is brought back once.

use color_eyre::eyre::{bail, eyre};
use std::path::Path;
use std::process::{Command, Stdio};

use crate::deploy::remote::{quote, Host};
use crate::deploy::Inventory;

/// The script that writes every heap dump in a directory to stdout as a tar, removing each
#[must_use]
pub fn dumps_script(dir: &str) -> String {
    // nothing there is an empty tar, not a failure
    format!(
        "cd {dir} 2>/dev/null || exit 0; \
         set -- heap.*.heap; [ -e \"$1\" ] || {{ tar -cf - --files-from /dev/null; exit 0; }}; \
         sudo -n tar -cf - --remove-files \"$@\"",
        dir = quote(dir)
    )
}

/// Bring one node's heap dumps back into a local directory
///
/// # Arguments
///
/// * `host` - The node's host
/// * `remote` - The deployment's directory on it
/// * `local` - Where the dumps go
///
/// # Errors
///
/// When ssh or tar fails.
pub fn pull(host: &Host, remote: &str, local: &Path) -> color_eyre::Result<()> {
    std::fs::create_dir_all(local)?;
    // ssh's stdout straight into a local tar, so a dump is never held in memory
    let command = host.ssh_command(&dumps_script(remote));
    let mut ssh = Command::new(&command[0])
        .args(&command[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = ssh.stdout.take().ok_or_else(|| eyre!("ssh gave no stdout"))?;
    let untar = Command::new("tar")
        .args(["-xf", "-", "-C"])
        .arg(local)
        .stdin(stdout)
        .status()?;
    let output = ssh.wait_with_output()?;
    if !output.status.success() || !untar.success() {
        bail!(
            "bringing the heap dumps back from {} failed: {}",
            host.target,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Bring every node's heap dumps back under a directory, one directory a node
///
/// # Arguments
///
/// * `inventory` - The bench's inventory
/// * `dest` - Where the dumps go
///
/// Returns what failed, node by node, since one node's failure is no reason to leave another's.
#[must_use]
pub fn collect(inventory: &Inventory, dest: &Path) -> Vec<String> {
    let remote = inventory.remote_dir();
    inventory
        .nodes
        .iter()
        .filter_map(|spec| {
            let host = Host {
                target: spec.target().to_string(),
            };
            pull(&host, &remote, &dest.join(&spec.name)).err().map(|error| format!("{}: {error}", spec.name))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::dumps_script;

    /// The script tars only heap dumps, removes them as it goes, and is empty rather than failed
    /// when there are none
    #[test]
    fn the_script_moves_only_heap_dumps() {
        let script = dumps_script("/opt/shoal-deploy/tmdb-bench");
        assert!(script.contains("cd /opt/shoal-deploy/tmdb-bench"));
        assert!(script.contains("heap.*.heap"));
        assert!(script.contains("--remove-files"));
        assert!(script.contains("--files-from /dev/null"));
        assert!(!script.contains("rm "));
    }
}
