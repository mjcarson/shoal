//! The systemd unit a deployed node runs as
//!
//! A system unit rather than a process held by an ssh session: it outlives the deployment,
//! comes back after a reboot, logs to the journal, and is stopped with the SIGTERM the node
//! program's `serve` answers by exiting its pool cleanly.

use super::inventory::{Inventory, Node};
use super::render::Layout;

/// Render a node's unit file
///
/// # Arguments
///
/// * `inventory` - The deployment
/// * `node` - The node it runs
/// * `server` - The file name the program has on every host
/// * `user` - The user the node runs as on its host
#[must_use]
pub fn render(inventory: &Inventory, node: &Node, server: &str, user: &str) -> String {
    // where the program and its configuration are on the host, the node's own directory when
    // it shares its machine
    let layout = Layout {
        dir: node.remote_dir.clone(),
    };
    let binary = layout.binary(server);
    // a node sharing its machine is named, since the machine runs one unit for each
    let described = if node.colocated {
        format!("Shoal node {} of the {} cluster", node.name, inventory.name)
    } else {
        format!("Shoal node of the {} cluster", inventory.name)
    };
    format!(
        "# written by shoaladm; rewritten on every deploy
[Unit]
Description={described}
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
        dir = layout.dir,
        conf = layout.conf(),
    )
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
        let unit = render(&inventory, &inventory.node("a").unwrap(), &name, "ops");
        assert!(unit.contains("User=ops\n"));
        assert!(unit.contains(&format!(
            "ExecStart=/opt/shoal-deploy/lab/bin/{name} serve --conf /opt/shoal-deploy/lab/shoal.yml\n"
        )));
        assert!(unit.contains("ExecStartPre=+/sbin/modprobe tls\n"));
        assert!(unit.contains("KillSignal=SIGTERM\n"));
        assert!(unit.contains("Description=Shoal node of the lab cluster\n"), "{unit}");
    }

    /// A node sharing its machine runs from a directory of its own and says which node it is
    #[test]
    fn a_unit_sharing_its_machine_runs_from_its_own_directory() {
        // two nodes reached through one target
        let server = std::env::current_exe().expect("the test binary");
        let name = server.file_name().unwrap().to_str().unwrap().to_string();
        let inventory: Inventory = serde_yaml::from_str(&format!(
            "server: {}\nname: loop\nreplication_factor: 1\nresources: {{cores: 2}}\nnodes:\n  \
             - {{name: a, address: 127.0.0.11, ssh: europa, resources: {{cores: 2, control_core: 1}}}}\n  \
             - {{name: b, address: 127.0.0.12, ssh: europa, resources: {{cores: 2, control_core: 2}}}}\n",
            server.display()
        ))
        .unwrap();
        let unit = render(&inventory, &inventory.node("b").unwrap(), &name, "ops");
        assert!(unit.contains(&format!(
            "ExecStart=/opt/shoal-deploy/loop/b/bin/{name} serve --conf /opt/shoal-deploy/loop/b/shoal.yml\n"
        )), "{unit}");
        assert!(unit.contains("WorkingDirectory=/opt/shoal-deploy/loop/b\n"), "{unit}");
        assert!(unit.contains("Description=Shoal node b of the loop cluster\n"), "{unit}");
    }
}
