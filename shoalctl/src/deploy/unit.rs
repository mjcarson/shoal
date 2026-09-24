//! The systemd unit a deployed node runs as
//!
//! A system unit rather than a process held by an ssh session: it outlives the deployment,
//! comes back after a reboot, logs to the journal, and is stopped with the SIGTERM the node
//! program's `serve` answers by exiting its pool cleanly.

use super::inventory::Inventory;
use super::render::Layout;

/// Render a node's unit file
///
/// # Arguments
///
/// * `inventory` - The deployment
/// * `user` - The user the node runs as on its host
///
/// # Errors
///
/// When the server path names no file.
pub fn render(inventory: &Inventory, user: &str) -> color_eyre::Result<String> {
    // where the program and its configuration are on the host
    let layout = Layout {
        dir: inventory.remote_dir(),
    };
    let binary = layout.binary(&inventory.server_name()?);
    Ok(format!(
        "# written by shoalctl cluster; rewritten on every deploy
[Unit]
Description=Shoal node of the {name} cluster
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={user}
WorkingDirectory={dir}
# the peer lanes hand their keys to the kernel, which needs the tls module (F14, F50)
ExecStartPre=+/sbin/modprobe tls
ExecStart={binary} serve --conf {conf}
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=60
LimitMEMLOCK=infinity
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
",
        name = inventory.name,
        dir = layout.dir,
        conf = layout.conf(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unit runs the deployed program as the ssh user and loads the tls module first
    #[test]
    fn the_unit_runs_the_deployed_program() {
        // a one node inventory
        let server = std::env::current_exe().expect("the test binary");
        let name = server.file_name().unwrap().to_str().unwrap().to_string();
        let inventory: Inventory = serde_yaml::from_str(&format!(
            "server: {}\nname: lab\nreplication_factor: 1\nnodes:\n  - {{name: a, address: 10.0.0.1}}\n",
            server.display()
        ))
        .unwrap();
        let unit = render(&inventory, "ops").expect("a unit");
        assert!(unit.contains("User=ops\n"));
        assert!(unit.contains(&format!(
            "ExecStart=/opt/shoal-deploy/lab/bin/{name} serve --conf /opt/shoal-deploy/lab/shoal.yml\n"
        )));
        assert!(unit.contains("ExecStartPre=+/sbin/modprobe tls\n"));
        assert!(unit.contains("KillSignal=SIGTERM\n"));
    }
}
