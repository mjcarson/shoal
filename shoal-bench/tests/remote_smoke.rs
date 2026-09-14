//! The remote launcher against a real ssh target, when one is named
//!
//! `SHOAL_REMOTE_SMOKE=<user@host>:<dir>` names a host reachable over ssh without a prompt
//! whose directory holds a `shoal-workload` of this build and a `shoal.yml` with local
//! storage paths; the test then captures the three node overhead arm at smoke scale with
//! node one on that host and node zero here, and checks the capture records two machines when
//! the host is another and one when it is this one. Unset, it says so and passes: the launcher
//! is exercised by name where a host exists, and nowhere silently
//! ([F50](../../docs/src/features/cluster-operations.md)).

use std::process::Command;

/// A smoke capture of the three node arm with node one on the named host
#[test]
fn a_remote_node_serves_a_smoke_capture() {
    let Ok(spec) = std::env::var("SHOAL_REMOTE_SMOKE") else {
        eprintln!("SKIPPING a_remote_node_serves_a_smoke_capture: SHOAL_REMOTE_SMOKE names no <user@host>:<dir>");
        return;
    };
    let out = tempfile::tempdir().expect("a temp dir");
    // the driver's address is the loopback: the host named is reached from here, and it
    // reaches node zero back at 127.0.0.1 only when it is this machine, which a user@localhost
    // spec is; another host needs SHOAL_REMOTE_DRIVER
    let driver = std::env::var("SHOAL_REMOTE_DRIVER").unwrap_or_else(|_| "127.0.0.1".to_string());
    let status = Command::new(env!("CARGO_BIN_EXE_shoal-bench"))
        .args([
            "run",
            "--label",
            "remote-smoke",
            "--scale",
            "smoke",
            "--runs",
            "1",
            "--allow-dirty",
            "--remote",
            &format!("1={spec}"),
            "--driver-address",
            &driver,
            "--out",
            &out.path().display().to_string(),
            "--exact",
            "macro/cluster/overhead/nodes/3",
        ])
        .status()
        .expect("shoal-bench runs");
    assert!(status.success(), "the remote smoke capture failed: {status}");
    // the capture records node one's machine as the host reported it
    let macro_path = out.path().join("remote-smoke.macro.json");
    let text = std::fs::read_to_string(&macro_path).expect("the macro artifact was written");
    let capture: serde_json::Value = serde_json::from_str(&text).expect("the artifact is json");
    let cluster = &capture["workloads"]["macro/cluster/overhead/nodes/3"]["cluster"];
    let environments = cluster["environments"].as_array().expect("every node's environment");
    assert_eq!(environments.len(), 3, "{cluster}");
    let hosts: Vec<&str> = environments.iter().filter_map(|env| env["hostname"].as_str()).collect();
    let remote_host = spec.rsplit_once(':').map(|(target, _)| target.rsplit_once('@').map_or(target, |(_, host)| host)).unwrap_or_default();
    let same_machine = remote_host == "localhost" || remote_host == "127.0.0.1" || hosts[0] == hosts[1];
    assert_eq!(cluster["emulated"].as_bool(), Some(same_machine), "{cluster}");
    if !same_machine {
        assert_ne!(hosts[0], hosts[1], "node one ran on the driver's machine: {hosts:?}");
    }
}
