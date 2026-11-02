//! The systemd unit a deployed node runs as
//!
//! A system unit rather than a process held by an ssh session: it outlives the deployment,
//! comes back after a reboot, logs to the journal, and is stopped with the SIGTERM the node
//! program's `serve` answers by exiting its pool cleanly.

use super::inventory::{Inventory, Node};
use super::render::Layout;

/// How long a node's allocator waits after a page of its memory empties before it hands the page
/// back to the kernel, in milliseconds: mimalloc's `purge_delay`, read from the node's environment
/// as it starts ([item 353](../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts))
///
/// A purge is an `madvise` that waits for every other cpu to flush its TLB, run inside whichever of
/// a shard's steps frees or allocates next: at mimalloc's 10 ms, about half of a scan's captures past
/// 100 µs on the lab's Zen1 hosts (2026-11-02). Ten times rarer here, and still under one of a
/// node's 250 ms memory checks, so memory an eviction frees leaves the resident set by the next check,
/// which judges the node's budget on it
/// ([Resolved #149](../../../docs/src/appendix/resolved/node-memory-budget.md)). Never purging, which
/// the lab ran to find this, would leave a node past its budget unable to evict its way back.
pub const ALLOCATOR_PURGE_DELAY_MS: u32 = 100;

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
    // the peer lanes hand their keys to the kernel, which needs its tls module (F14, F50), unless
    // the deployment's lanes are plaintext (F103)
    let modprobe = if inventory.plaintext_lanes {
        ""
    } else {
        "# the peer lanes hand their keys to the kernel, which needs the tls module (F14, F50)\n\
         ExecStartPre=+/sbin/modprobe tls\n"
    };
    // an object connection asks for the deployment's congestion control, which a node not running
    // as root may take only once its module is loaded and its host allows it: both are tried, and a
    // host without it starts the node all the same, its object connections the host's (F112)
    let congestion = match &inventory.object_congestion {
        Some(name) if shoal::shared::sockets::is_congestion_name(name) => format!(
            "# object connections run under {name}, which the node takes once it is loaded and allowed (F112)\n\
             ExecStartPre=-+/sbin/modprobe tcp_{name}\n\
             ExecStartPre=-+/bin/sh -c 'f=/proc/sys/net/ipv4/tcp_allowed_congestion_control; \
             grep -qw {name} $$f || echo \"$$(cat $$f) {name}\" > $$f'\n"
        ),
        _ => String::new(),
    };
    // its allocator purges freed memory rarely enough to stay off a shard's steps, often enough for
    // its memory checks (item 353)
    let allocator = format!(
        "# the allocator hands freed pages back {ALLOCATOR_PURGE_DELAY_MS} ms after they empty, not mimalloc's 10, off a shard's steps (item 353)\n\
         Environment=MIMALLOC_PURGE_DELAY={ALLOCATOR_PURGE_DELAY_MS}\n"
    );
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
{allocator}{modprobe}{congestion}ExecStart={binary} serve --conf {conf}
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
        // and no congestion control, since its object connections keep the host's unless named
        // (F112, F113)
        assert!(!unit.contains("tcp_"), "{unit}");
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

    /// A node whose deployment's peer lanes are plaintext loads no kernel tls module before it
    /// starts, where every other does (F103)
    #[test]
    fn a_plaintext_unit_loads_no_tls_module() {
        // one node, its lanes plaintext
        let server = std::env::current_exe().expect("the test binary");
        let name = server.file_name().unwrap().to_str().unwrap().to_string();
        let inventory: Inventory = serde_yaml::from_str(&format!(
            "server: {}\nname: lab\nreplication_factor: 1\nplaintext_lanes: true\nnodes:\n  - {{name: a, address: 10.0.0.1}}\n",
            server.display()
        ))
        .unwrap();
        let unit = render(&inventory, &inventory.node("a").unwrap(), &name, "ops");
        assert!(!unit.contains("modprobe tls"), "{unit}");
        // and it still runs the node the same way
        assert!(unit.contains(&format!(
            "ExecStart=/opt/shoal-deploy/lab/bin/{name} serve --conf /opt/shoal-deploy/lab/shoal.yml\n"
        )), "{unit}");
    }

    /// Every node's allocator hands freed pages back to the kernel 100 ms after they empty rather than
    /// mimalloc's 10 ms, whatever else its deployment names, colocated or alone
    /// ([item 353](../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts))
    #[test]
    fn every_unit_runs_its_allocator_at_a_longer_purge_delay() {
        // a node alone, plaintext, and one sharing its machine
        let server = std::env::current_exe().expect("the test binary");
        let name = server.file_name().unwrap().to_str().unwrap().to_string();
        let unit = |extra: &str, nodes: &str, node: &str| {
            let inventory: Inventory = serde_yaml::from_str(&format!(
                "server: {}\nname: lab\nreplication_factor: 1\n{extra}nodes:\n{nodes}",
                server.display()
            ))
            .unwrap();
            render(&inventory, &inventory.node(node).unwrap(), &name, "ops")
        };
        let alone = unit("", "  - {name: a, address: 10.0.0.1}\n", "a");
        let plaintext = unit("plaintext_lanes: true\n", "  - {name: a, address: 10.0.0.1}\n", "a");
        let colocated = unit(
            "resources: {cores: 2}\n",
            "  - {name: a, address: 127.0.0.11, ssh: europa, resources: {cores: 2, control_core: 1}}\n  \
             - {name: b, address: 127.0.0.12, ssh: europa, resources: {cores: 2, control_core: 2}}\n",
            "b",
        );
        for unit in [&alone, &plaintext, &colocated] {
            // once, in the service the node runs as, before it starts
            assert_eq!(unit.matches("Environment=MIMALLOC_PURGE_DELAY=100\n").count(), 1, "{unit}");
            let service = unit.find("[Service]").expect("a service section");
            let set = unit.find("Environment=MIMALLOC_PURGE_DELAY=").expect("the delay");
            let started = unit.find("ExecStart=").expect("a start");
            assert!(service < set && set < started, "{unit}");
        }
    }

    /// A node whose deployment leaves its object connections the host's congestion control loads
    /// and allows none, and one that names another loads and allows that one (F112)
    #[test]
    fn a_unit_loads_the_congestion_control_its_deployment_names() {
        // one node, its congestion control null and named
        let server = std::env::current_exe().expect("the test binary");
        let name = server.file_name().unwrap().to_str().unwrap().to_string();
        let unit = |extra: &str| {
            let inventory: Inventory = serde_yaml::from_str(&format!(
                "server: {}\nname: lab\nreplication_factor: 1\n{extra}nodes:\n  - {{name: a, address: 10.0.0.1}}\n",
                server.display()
            ))
            .unwrap();
            render(&inventory, &inventory.node("a").unwrap(), &name, "ops")
        };
        let hosts = unit("object_congestion: null\n");
        assert!(!hosts.contains("tcp_"), "{hosts}");
        let named = unit("object_congestion: htcp\n");
        assert!(named.contains("ExecStartPre=-+/sbin/modprobe tcp_htcp\n"), "{named}");
        assert!(named.contains("grep -qw htcp $$f"), "{named}");
        assert!(!named.contains("bbr"), "{named}");
        // and BBR, which a deployment may still name, the same way
        let bbr = unit("object_congestion: bbr\n");
        assert!(bbr.contains("ExecStartPre=-+/sbin/modprobe tcp_bbr\n"), "{bbr}");
        assert!(bbr.contains(
            "ExecStartPre=-+/bin/sh -c 'f=/proc/sys/net/ipv4/tcp_allowed_congestion_control; \
             grep -qw bbr $$f || echo \"$$(cat $$f) bbr\" > $$f'\n"
        ), "{bbr}");
    }
}
