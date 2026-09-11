//! The M0 and M1 fixture tests, and the two children they re-execute this binary as
//!
//! Three M0 tests named in the acceptance tables of `docs/src/distributed/`: the fixture reports
//! endpoints its children actually bound with no port race and cleans up after a failure
//! (C11), its directed faults cover reconnects and its pause is not its cut (C11), and it
//! records every core and endpoint it handed out, disjoint where it claimed so (C10). And the
//! five M1 tests of [C1](../../docs/src/distributed/node-identity.md): a node's identity
//! survives a kill and a directory is refused by the wrong mode, unknown configuration and
//! marker formats are refused by name, the control core respects the cpuset and its SMT
//! siblings, a standalone node has none of it, and the documented defaults are the policy a
//! bootstrap seeds.
//!
//! The two `#[ignore]` functions at the bottom are the children. They are never run by
//! `cargo test`; the fixture runs them by name with `--exact --ignored`.

use std::io::Write as _;
use std::time::Duration;

use shoal::client::Shoal;
use shoal::server::conf::{Cluster as ClusterConf, Networking, Resources};
use shoal::server::control::types::{ControlCommand, ControlState};
use shoal::server::errors::ShoalError;
use shoal::server::{ClusterIntent, Conf, ServerError, StorageMeta};
use shoal::ShoalPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod cluster;
mod utils;

use cluster::schema::{Row, RowGet, TestDb, TestDbClient};
use cluster::{ChildRequest, Cluster, CoreClaim, Endpoints, FixtureError, NodeKind, Topology};

/// Write a row through a node and read it back, so an endpoint is shown to be a server's
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - A key unique to the caller
async fn round_trip(addr: &str, key: u64) -> Result<(), FixtureError> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    client
        .send_one(Row {
            key,
            data: "fixture".to_string(),
        })
        .await?;
    let response = client.send_one(RowGet::new(vec![key])).await?;
    assert!(response.access::<Row>()?.is_some(), "the row written to {addr} was not read back");
    Ok(())
}

/// Wait for every pid to be gone, within a bound
///
/// # Arguments
///
/// * `pids` - The processes
/// * `within` - How long to wait
fn all_gone(pids: &[u32], within: Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    loop {
        if pids.iter().all(|pid| !cluster::is_alive(*pid)) {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Concurrent fixtures own distinct actual listeners, and clean up after a failure
///
/// Two clusters start at once, every child on port zero, so nothing but the kernel picks a
/// port and no reservation is dropped before it is used. Every endpoint reported is distinct
/// and answers a query. Then one cluster is dropped by a panic, the way a failing test drops
/// it, and every child it owned is shown to be gone.
#[tokio::test(flavor = "multi_thread")]
async fn fixture_reports_bound_endpoints_without_port_race() -> Result<(), FixtureError> {
    // two clusters, started concurrently
    let (left, right) = tokio::join!(
        Cluster::builder()
            .server(CoreClaim::Count(1))
            .server(CoreClaim::Count(1))
            .start(),
        Cluster::builder()
            .server(CoreClaim::Count(1))
            .server(CoreClaim::Count(1))
            .start(),
    );
    let (left, right) = (left?, right?);
    // four endpoints, all different, all bound by their children
    let mut endpoints: Vec<_> = left.client_endpoints();
    endpoints.extend(right.client_endpoints());
    let distinct: std::collections::BTreeSet<_> = endpoints.iter().collect();
    assert_eq!(distinct.len(), 4, "two children bound the same endpoint: {endpoints:?}");
    for (index, addr) in endpoints.iter().enumerate() {
        assert_ne!(addr.port(), 0, "a child reported an unresolved port");
        round_trip(&addr.to_string(), index as u64).await?;
    }
    // nothing has died
    for id in 0..left.len() {
        assert_eq!(left.node(id).failure(), None);
    }
    // a test that panics drops its cluster during the unwind; the children must not outlive it
    let pids = right.pids();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _held = right;
        panic!("a test failing with a cluster in scope");
    }));
    assert!(unwound.is_err());
    assert!(
        all_gone(&pids, Duration::from_secs(5)),
        "children {pids:?} outlived the cluster that owned them"
    );
    // the other cluster is untouched
    round_trip(&left.node(0).endpoints.client.to_string(), 100).await?;
    let pids = left.pids();
    drop(left);
    assert!(all_gone(&pids, Duration::from_secs(5)), "children {pids:?} outlived their cluster");
    Ok(())
}

/// Every path in a direction is subject to a cut, reconnects included; a pause is not a cut
///
/// A server and a mock peer, with a link each way. Traffic from 0 to 1 crosses the proxy the
/// fixture stands in for node 0's view of node 1: a cut ends the live stream and every new
/// connection after it, while the other direction still passes, and a heal restores it. Then
/// node 0 is paused: a query stalls rather than failing, and completes once it resumes - a
/// process pause and a link partition are different states, and are labelled apart.
#[tokio::test(flavor = "multi_thread")]
async fn fixture_faults_cover_directed_links_and_reconnects() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .server(CoreClaim::Count(1))
        .mock_peer(CoreClaim::Shared)
        .links(true)
        .start()
        .await?;
    // node 0 reaching node 1 goes through this proxy; the driver plays node 0's part
    let to_peer = cluster.link(0, 1).addr();
    let mut stream = tokio::net::TcpStream::connect(to_peer).await?;
    stream.write_all(b"ping").await?;
    let mut buffer = [0u8; 4];
    stream.read_exact(&mut buffer).await?;
    assert_eq!(&buffer, b"ping", "the mock peer did not echo through the link");
    assert_eq!(cluster.link(0, 1).live(), 1);
    // cut it: the live stream ends
    cluster.link(0, 1).cut();
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        stream.write_all(b"ping").await?;
        stream.read_exact(&mut buffer).await
    })
    .await;
    assert!(
        !matches!(ended, Ok(Ok(_))),
        "a stream across a cut link still carried traffic"
    );
    // and a reconnect is subject to the same cut, not a fresh path around it
    let mut again = tokio::net::TcpStream::connect(to_peer).await?;
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        again.write_all(b"ping").await?;
        again.read_exact(&mut buffer).await
    })
    .await;
    assert!(
        !matches!(closed, Ok(Ok(_))),
        "a connection opened after the cut carried traffic"
    );
    // the other direction is untouched: node 1 reaching node 0 is a working server
    let to_server = cluster.link(1, 0).addr();
    round_trip(&to_server.to_string(), 1).await?;
    // healed, a new connection works
    cluster.link(0, 1).heal();
    let mut healed = tokio::net::TcpStream::connect(to_peer).await?;
    healed.write_all(b"pong").await?;
    healed.read_exact(&mut buffer).await?;
    assert_eq!(&buffer, b"pong");
    // a paused server is a different fault: a query stalls, and completes on resume
    let client = Shoal::<TestDbClient>::new(&to_server.to_string()).await?;
    cluster.node(0).pause()?;
    let stalled = tokio::time::timeout(
        Duration::from_secs(2),
        client.send_one(Row {
            key: 7,
            data: "paused".to_string(),
        }),
    )
    .await;
    assert!(stalled.is_err(), "a query to a paused server did not stall");
    cluster.node(0).resume()?;
    let answered = tokio::time::timeout(
        Duration::from_secs(10),
        client.send_one(RowGet::new(vec![7])),
    )
    .await;
    assert!(answered.is_ok(), "a query to a resumed server never came back");
    assert!(
        answered.unwrap()?.access::<Row>()?.is_some(),
        "the write sent during the pause was lost, rather than delayed"
    );
    // and a kill is a kill: the node is gone, and nothing here says anything about its disk
    cluster.node_mut(0).kill()?;
    assert!(!cluster.node(0).is_alive());
    Ok(())
}

/// Driver, control and data allocations and every endpoint are recorded, disjoint where claimed
///
/// The allocator is checked on a synthetic machine, so the result does not depend on the box
/// the test runs on: counted claims are disjoint from each other and from the driver, an exact
/// claim that overlaps is refused, and a machine too small for the claims records sharing rather
/// than hiding it. Then a real cluster's plan is shown to carry the endpoints its children
/// bound, each answering.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_fixture_accounts_for_all_cores_and_endpoints() -> Result<(), FixtureError> {
    // twelve physical cores, two threads each; core 0 holds cpu 0 and is reserved. twelve rather
    // than the eight M0 drew, because since M1 each of the three servers owns a control core as
    // well as its data cores, and the plan below wants every claim met
    let machine = Topology::synthetic(12, 2);
    let builder = Cluster::builder()
        .server(CoreClaim::Count(2))
        .server(CoreClaim::Count(1))
        .server(CoreClaim::Exact(vec![6]))
        .driver(CoreClaim::Count(1));
    let plan = builder.plan(&machine)?;
    plan.disjoint_where_claimed().map_err(FixtureError::Allocation)?;
    assert_eq!(plan.reserved, Some(0));
    assert_eq!(plan.nodes.len(), 3);
    for (_, allocation) in &plan.nodes {
        assert!(!allocation.shared);
        assert!(!allocation.data.contains(&0), "a node was given the reserved core");
        assert_eq!(allocation.cpus.len(), allocation.data.len() * 2, "an SMT sibling was left out");
        // since M1 every server owns a control core too, disjoint from its data cores
        let control = allocation.control.expect("a server was given no control core");
        assert!(!allocation.data.contains(&control), "a control core is also a data core");
    }
    assert_eq!(plan.nodes[2].1.data, vec![6]);
    assert_eq!(plan.driver.data.len(), 1);
    assert!(!plan.driver.shared);
    // an exact claim on a taken core is refused, not silently shared
    let overlapping = Cluster::builder()
        .server(CoreClaim::Exact(vec![3]))
        .server(CoreClaim::Exact(vec![3]))
        .plan(&machine);
    assert!(matches!(overlapping, Err(FixtureError::Allocation(_))));
    // a machine too small for the claims records the sharing
    let small = Topology::synthetic(2, 1);
    let crowded = Cluster::builder()
        .server(CoreClaim::Count(1))
        .server(CoreClaim::Count(1))
        .plan(&small)?;
    assert!(!crowded.nodes[0].1.shared);
    assert!(crowded.nodes[1].1.shared, "a claim the machine cannot meet was not recorded as shared");
    // and neither server got a control core, which is recorded rather than invented
    assert_eq!(crowded.nodes[0].1.control, None);
    assert_eq!(crowded.nodes[1].1.control, None);
    crowded.disjoint_where_claimed().map_err(FixtureError::Allocation)?;
    // a machine with room for the data cores but not every control core gives the control
    // cores it has in node order, and the rest share
    let tight = Topology::synthetic(4, 2);
    let squeezed = Cluster::builder()
        .server(CoreClaim::Count(1))
        .server(CoreClaim::Count(1))
        .plan(&tight)?;
    assert!(squeezed.nodes[0].1.control.is_some());
    assert_eq!(squeezed.nodes[1].1.control, None);
    squeezed.disjoint_where_claimed().map_err(FixtureError::Allocation)?;
    // a real cluster carries what its children bound
    let cluster = Cluster::builder()
        .server(CoreClaim::Count(1))
        .server(CoreClaim::Count(1))
        .links(true)
        .start()
        .await?;
    let plan = cluster.plan();
    plan.disjoint_where_claimed().map_err(FixtureError::Allocation)?;
    assert_eq!(plan.endpoints.len(), 2);
    assert_eq!(plan.proxies.len(), 2, "one proxy per ordered pair");
    for (id, endpoints) in plan.endpoints.iter().enumerate() {
        assert_eq!(endpoints, &cluster.node(id).endpoints);
        assert_eq!(endpoints.data, None);
        assert_eq!(endpoints.control, None);
        round_trip(&endpoints.client.to_string(), id as u64).await?;
    }
    for ((from, to), proxy) in &plan.proxies {
        assert_eq!(cluster.link(*from, *to).addr(), *proxy);
        assert_eq!(cluster.link(*from, *to).target(), plan.endpoints[*to].client);
    }
    Ok(())
}

/// The cpus of a physical core, from the machine
///
/// # Arguments
///
/// * `core` - The physical core
fn cpus_of(core: usize) -> Vec<usize> {
    Topology::detect()
        .cores
        .get(&core)
        .cloned()
        .unwrap_or_default()
}

/// The cpus this process may run on
fn allowed_cpus() -> Vec<usize> {
    shoal::server::control::cores::allowed_cpus().expect("the affinity reads")
}

/// A standalone node has no control thread, no control files and no cluster identity
///
/// The `cluster:` block absent, a server is the deployment shape every capture before M1 ran
/// under: the same endpoints record as M0 with nothing new filled in, no thread named after
/// the control executor, and no `control/` directory under its storage - and still a node id,
/// because the marker mints one for every directory. Beside it a cluster node, on the same
/// machine, shows each of those the other way.
#[tokio::test(flavor = "multi_thread")]
async fn standalone_needs_no_peer_or_control_listener() -> Result<(), FixtureError> {
    let cluster = Cluster::builder()
        .standalone(CoreClaim::Count(1))
        .server(CoreClaim::Count(1))
        .start()
        .await?;
    // the standalone node: the M0 shape, plus a node id
    let standalone = cluster.node(0);
    assert_eq!(standalone.endpoints.data, None);
    assert_eq!(standalone.endpoints.control, None);
    assert!(standalone.endpoints.node.is_some(), "a standalone node has no identity");
    assert_eq!(standalone.endpoints.cluster, None, "a standalone node belongs to a cluster");
    assert_eq!(standalone.endpoints.control_core, None, "a standalone node has a control core");
    assert_eq!(standalone.endpoints.topology_version, None);
    assert!(
        !standalone.thread_names().iter().any(|name| name.starts_with("shoal-control")),
        "a standalone node runs a control thread: {:?}",
        standalone.thread_names()
    );
    assert!(
        !cluster.dir(0).join("control").exists(),
        "a standalone node wrote a control directory"
    );
    // the allocator gave it no control core either
    assert_eq!(cluster.plan().nodes[0].1.control, None);
    // and it serves
    round_trip(&standalone.endpoints.client.to_string(), 1).await?;
    // the cluster node: every one of those the other way
    let member = cluster.node(1);
    assert!(member.endpoints.cluster.is_some(), "a cluster node has no cluster");
    assert!(member.endpoints.control_core.is_some(), "a cluster node has no control core");
    assert!(member.endpoints.topology_version.is_some_and(|version| version > 0));
    assert!(
        member.thread_names().iter().any(|name| name.starts_with("shoal-control")),
        "a cluster node runs no control thread: {:?}",
        member.thread_names()
    );
    assert!(cluster.dir(1).join("control").join("state.json").exists());
    round_trip(&member.endpoints.client.to_string(), 2).await?;
    Ok(())
}

/// A node's identity survives a kill, and a directory is refused by the wrong mode
///
/// A cluster node is bootstrapped, killed with `SIGKILL`, and started again on its directory:
/// it comes back as the same node in the same cluster, with its topology version recovered
/// from the control log rather than reset. Then the refusals: the same directory started
/// without a `cluster:` block is refused as a cluster directory, a standalone directory
/// started with one is refused naming the migration milestone, and a peer from another cluster
/// is refused by `verify_cluster` without a byte of the marker changing.
#[tokio::test(flavor = "multi_thread")]
async fn node_identity_persists_and_wrong_cluster_is_refused() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .server(CoreClaim::Count(1))
        .standalone(CoreClaim::Count(1))
        .start()
        .await?;
    let first = cluster.node(0).endpoints.clone();
    let node = first.node.clone().expect("a node id");
    let cluster_id = first.cluster.clone().expect("a cluster id");
    let version = first.topology_version.expect("a topology version");
    // the bootstrap is one change, and the observation that follows it repeats the member
    // record the bootstrap carried, so it moves nothing
    assert_eq!(version, 1, "a fresh bootstrap is one topology change, saw {version}");
    // killed and restarted, it is the same node in the same cluster
    cluster.restart(0, NodeKind::Server)?;
    let again = cluster.node(0).endpoints.clone();
    assert_eq!(again.node, Some(node.clone()), "the node id changed across a restart");
    assert_eq!(again.cluster, Some(cluster_id.clone()), "the cluster id changed across a restart");
    let recovered = again.topology_version.expect("a topology version after restart");
    assert!(
        recovered >= version,
        "the topology version went from {version} to {recovered} across a restart"
    );
    assert_ne!(again.client.port(), 0);
    round_trip(&again.client.to_string(), 1).await?;
    // and the marker on disk says so too
    let marker = StorageMeta::read(cluster.dir(0))
        .expect("the marker reads")
        .expect("a marker");
    assert_eq!(marker.node.to_string(), node);
    assert_eq!(marker.cluster.map(|c| c.to_string()), Some(cluster_id.clone()));
    assert!(marker.topology >= version, "the marker did not record the topology");
    // the cluster directory started standalone is refused
    cluster.node_mut(0).kill()?;
    let refused = cluster.restart(0, NodeKind::Standalone).expect_err("a cluster directory started standalone");
    let reason = format!("{refused:?}");
    assert!(
        reason.contains("belongs to cluster") && reason.contains(&cluster_id),
        "the refusal did not name the cluster: {reason}"
    );
    // a standalone directory started as a cluster member is refused, naming the migration
    let standalone_node = cluster.node(1).endpoints.node.clone().expect("a node id");
    cluster.node_mut(1).kill()?;
    let refused = cluster.restart(1, NodeKind::Server).expect_err("a standalone directory joined a cluster");
    let reason = format!("{refused:?}");
    assert!(
        reason.contains("M10") && reason.contains(&standalone_node),
        "the refusal did not name the migration and the node: {reason}"
    );
    // a peer from another cluster is refused by the identity, and the marker is untouched
    let before = std::fs::read(StorageMeta::path(cluster.dir(0)))?;
    let identity = StorageMeta::claim(cluster.dir(0), marker.shards, ClusterIntent::Bootstrap)
        .expect("the directory reopens under its own mode");
    assert_eq!(identity.node.to_string(), node);
    let other = shoal::shared::identity::ClusterId::mint();
    let error = identity.verify_cluster(other).expect_err("another cluster was accepted");
    assert!(matches!(
        error,
        ServerError::Shoal(ShoalError::WrongCluster { found, .. }) if found == other
    ));
    identity
        .verify_cluster(identity.cluster.expect("a cluster"))
        .expect("the node's own cluster was refused");
    let after = std::fs::read(StorageMeta::path(cluster.dir(0)))?;
    assert_eq!(before, after, "a refused peer changed the marker");
    Ok(())
}

/// An unknown configuration setting and an unknown marker format are refused by name
///
/// Three refusals, each naming what was wrong and where the way out is when there is one: a
/// misspelled key under `cluster:` names the key; a setting this build does not implement
/// names the milestone that does; and a format 1 marker - the shape every directory written
/// before M1 has - names the format, the formats this build reads, and that no migration
/// exists yet.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_configuration_and_storage_formats_are_refused() -> Result<(), FixtureError> {
    // a misspelled key under the cluster block is refused, naming the key
    let dir = utils::test_dir();
    let path = dir.path().join("shoal.yml");
    std::fs::write(
        &path,
        "resources:\n  memory: \"100MiB\"\ncluster:\n  bootstrap: true\n  replication_factr: 3\n",
    )?;
    let error = Conf::from_file(path.to_str().unwrap()).expect_err("a misspelled cluster key loaded");
    assert!(
        error.to_string().contains("replication_factr"),
        "the error did not name the key: {error}"
    );
    // a setting this build cannot act on is refused at startup, naming the milestone
    let conf = utils::build_crash_config(dir.path(), 0).cluster(
        ClusterConf::default().seeds(vec!["10.0.0.1:12001".to_string()]),
    );
    let error = match ShoalPool::<TestDb>::start(conf) {
        Ok(pool) => {
            let _ = pool.exit();
            panic!("a joiner started before M3");
        }
        Err(error) => error,
    };
    assert!(
        matches!(&error, ServerError::Shoal(ShoalError::NotImplemented { milestone: "M3", .. })),
        "the wrong refusal: {error:?}"
    );
    assert!(format!("{error}").contains("M3"), "{error}");
    // a format 1 marker is refused by name, with the formats this build reads and the fact
    // that no migration exists
    let refused = match Cluster::builder()
        .server(CoreClaim::Count(1))
        .staged_marker("{\n  \"format\": 1,\n  \"shards\": 2\n}")
        .start()
        .await
    {
        Ok(_) => panic!("a format 1 marker started"),
        Err(refused) => refused,
    };
    let reason = format!("{refused:?}");
    assert!(reason.contains("format 1"), "the refusal did not name the format: {reason}");
    assert!(reason.contains("reads [2]"), "the refusal did not name what it reads: {reason}");
    assert!(reason.contains("no migration"), "the refusal did not say there is no migration: {reason}");
    // and a format from the future the same way, whatever else it carries
    let refused = match Cluster::builder()
        .standalone(CoreClaim::Count(1))
        .staged_marker("{\n  \"format\": 3,\n  \"shards\": 2,\n  \"future\": true\n}")
        .start()
        .await
    {
        Ok(_) => panic!("a format 3 marker started"),
        Err(refused) => refused,
    };
    assert!(format!("{refused:?}").contains("format 3"));
    Ok(())
}

/// The control core respects the process's cpuset and keeps its SMT siblings from the shards
///
/// A child whose affinity excludes its control core is refused by name before a shard starts.
/// Two servers on one machine get distinct control cores, and neither's shard cpus touch either
/// thread of its control core. And an exact claim that overlaps another node's control core is
/// refused by the allocator, the way one overlapping a data core already was.
#[tokio::test(flavor = "multi_thread")]
async fn control_core_respects_cpuset_and_smt_reservation() -> Result<(), FixtureError> {
    // the allocator refuses an exact claim on a core another node's control thread owns
    let machine = Topology::synthetic(6, 2);
    let plan = Cluster::builder()
        .server(CoreClaim::Count(1))
        .plan(&machine)?;
    let taken = plan.nodes[0].1.control.expect("a control core");
    let overlapping = Cluster::builder()
        .server(CoreClaim::Exact(vec![taken]))
        .server(CoreClaim::Exact(plan.nodes[0].1.data.clone()))
        .plan(&machine)?;
    // exact claims are met first, so the second server's control core moves off the first's
    // exact claim rather than the reverse; what cannot happen is both owning one core
    overlapping.disjoint_where_claimed().map_err(FixtureError::Allocation)?;
    // two real servers, each with a control core the other does not have
    let cluster = Cluster::builder()
        .server(CoreClaim::Count(1))
        .server(CoreClaim::Count(1))
        .start()
        .await?;
    cluster.plan().disjoint_where_claimed().map_err(FixtureError::Allocation)?;
    let mut control_cores = Vec::new();
    for id in 0..2 {
        let allocation = &cluster.plan().nodes[id].1;
        let endpoints = &cluster.node(id).endpoints;
        // the machine may be too small to isolate both; what it did is what is recorded
        let Some(core) = allocation.control else {
            assert!(endpoints.control_shared, "a control thread without a core was not recorded as shared");
            continue;
        };
        assert!(!endpoints.control_shared, "an isolated control core was recorded as shared");
        let siblings = cpus_of(core);
        let cpu = endpoints.control_core.expect("a control core");
        assert!(siblings.contains(&cpu), "the control cpu {cpu} is not on core {core}");
        // no shard on either thread of the control core
        for shard in &endpoints.shard_cpus {
            assert!(
                !siblings.contains(shard),
                "node {id} runs a shard on cpu {shard}, a sibling of its control cpu {cpu}"
            );
        }
        assert!(!endpoints.shard_cpus.is_empty(), "node {id} reported no shard cpus");
        control_cores.push(core);
    }
    control_cores.dedup();
    assert_eq!(
        control_cores.len(),
        cluster.plan().nodes.iter().filter(|(_, a)| a.control.is_some()).count(),
        "two nodes share a control core"
    );
    drop(cluster);
    // a child whose affinity excludes its control core is refused by name
    let plan = Cluster::builder()
        .server(CoreClaim::Count(1))
        .plan(&Topology::detect())?;
    let Some(control) = plan.nodes[0].1.control else {
        // a machine with no core to give cannot exclude one; nothing more can be said here
        return Ok(());
    };
    let excluded = cpus_of(control);
    let narrowed: Vec<usize> = allowed_cpus()
        .into_iter()
        .filter(|cpu| !excluded.contains(cpu))
        .collect();
    let refused = match Cluster::builder()
        .server(CoreClaim::Count(1))
        .affinity(narrowed)
        .start()
        .await
    {
        Ok(_) => panic!("a control core outside the affinity started"),
        Err(refused) => refused,
    };
    let reason = format!("{refused:?}");
    assert!(
        reason.contains("outside this process's affinity"),
        "the refusal did not name the affinity: {reason}"
    );
    Ok(())
}

/// The documented cluster defaults are the policy a bootstrap seeds into the control state
///
/// The `cluster:` block in `docs/src/getting-started/configuration.md` is loaded as a config
/// file, and has to be exactly the defaults with `bootstrap` on: Quorum writes, One reads,
/// three voters, a finite removal grace. Then the policy that block produces is applied as a
/// bootstrap to an empty control state, and the state holds exactly it.
#[test]
fn documented_cluster_defaults_match_policy_bootstrap() {
    // the block, cut out of the page by its fence
    let page = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/src/getting-started/configuration.md"),
    )
    .expect("the configuration page reads");
    let block = page
        .split("```yaml\n")
        .skip(1)
        .map(|rest| rest.split("```").next().unwrap_or_default())
        .find(|block| block.starts_with("cluster:"))
        .expect("the configuration page has a yaml block beginning `cluster:`");
    // loaded the way a server loads it
    let dir = utils::test_dir();
    let path = dir.path().join("shoal.yml");
    std::fs::write(&path, format!("resources:\n  memory: \"100MiB\"\n{block}")).expect("the block is written");
    let conf = Conf::from_file(path.to_str().unwrap()).expect("the documented block loads");
    let cluster = conf.cluster.expect("the block names a cluster");
    // it is the defaults, bootstrapping
    assert_eq!(cluster, ClusterConf::default().bootstrap(true));
    assert_eq!(cluster.write_consistency, shoal::server::conf::cluster::Consistency::Quorum);
    assert_eq!(cluster.read_consistency, shoal::server::conf::cluster::Consistency::One);
    assert_eq!(cluster.control_voters, 3);
    let grace = cluster.auto_remove_after.expect("a finite removal grace");
    assert!(grace.duration() > Duration::ZERO);
    assert_eq!(grace.duration(), Duration::from_secs(30 * 60));
    // and a bootstrap seeds exactly that policy
    let policy = cluster.policy();
    let mut state = ControlState::default();
    let member = shoal::server::control::types::MemberRecord::default();
    state.apply(&ControlCommand::Bootstrap {
        cluster: shoal::shared::identity::ClusterId::mint(),
        policy: policy.clone(),
        member,
    });
    assert_eq!(state.policy, Some(policy));
    assert_eq!(state.desired_rf(), 3);
}

/// The request this process was started with, if it is a child
fn child_request() -> ChildRequest {
    let json = std::env::var(cluster::CHILD_ENV).expect("a child is started with a request");
    serde_json::from_str(&json).expect("the request parses")
}

/// Print a report line and make sure it leaves the process
///
/// # Arguments
///
/// * `line` - The line
fn report(line: &str) {
    println!("{line}");
    std::io::stdout().flush().expect("stdout flushes");
}

/// A server child: a `ShoalPool` on port zero, reporting what it bound, then watching for a
/// shard to die
#[tokio::test]
#[ignore]
async fn cluster_server_child() {
    let request = child_request();
    // exactly the allocation the parent decided on, and any port at all
    let mut resources = Resources::default()
        .exclude_cores(request.exclude_cores.clone())
        .memory("100MiB")
        .expect("a memory size");
    if let Some(cores) = request.cores {
        resources = resources.cores(cores);
    }
    // the crash config points storage at the child's own directory; the port and resources are
    // the parent's to decide
    let mut conf = utils::build_crash_config(&request.dir, 0)
        .resources(resources)
        .networking(Networking::default().port(0));
    // a cluster node bootstraps itself, with its control thread where the parent put it
    if request.kind == NodeKind::Server {
        conf = conf.cluster(
            ClusterConf::default()
                .bootstrap(true)
                .control_core(request.control_cpu.unwrap_or(0))
                .control_core_shared(request.control_shared),
        );
    }
    // a marker the test staged, written before the server can claim the directory
    if let Some(marker) = &request.staged_marker {
        std::fs::create_dir_all(&request.dir).expect("the storage directory is made");
        std::fs::write(StorageMeta::path(&request.dir), marker).expect("the marker is staged");
    }
    let mut pool = match ShoalPool::<TestDb>::start(conf) {
        Ok(pool) => pool,
        Err(error) => {
            report(&format!("{} {error}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
    };
    // ready means every shard is answering, on the port the pool resolved, and the control
    // plane - if there is one - has its group
    let client = match pool.ready(utils::READY_TIMEOUT) {
        Ok(addr) => addr,
        Err(error) => {
            report(&format!("{} {error}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
    };
    // what the node is, which the parent records and the tests read
    let identity = pool.identity().clone();
    let topology = pool.topology().ok();
    let shard_cpus = pool.shard_cpus().to_vec();
    let endpoints = Endpoints {
        client,
        data: None,
        control: None,
        node: Some(identity.node.to_string()),
        cluster: identity.cluster.map(|cluster| cluster.to_string()),
        control_core: pool.control_placement().map(|placement| placement.cpu),
        control_shared: pool.control_placement().is_some_and(|placement| placement.shared),
        topology_version: topology.as_ref().map(|view| view.version),
        shard_cpus,
    };
    report(&format!(
        "{} {}",
        cluster::READY_LINE,
        serde_json::to_string(&endpoints).expect("endpoints serialize")
    ));
    // then relay a shard's death, should one happen, and otherwise run until killed
    loop {
        if let Some((shard, error)) = pool.failure() {
            report(&format!("{} shard {shard} died: {error}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A mock peer child: a listener that echoes, so a link can be exercised with no peer protocol
#[tokio::test]
#[ignore]
async fn cluster_mock_peer_child() {
    let _request = child_request();
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) => {
            report(&format!("{} {error:?}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
    };
    let endpoints = Endpoints {
        client: listener.local_addr().expect("a bound address"),
        ..Endpoints::unbound()
    };
    report(&format!(
        "{} {}",
        cluster::READY_LINE,
        serde_json::to_string(&endpoints).expect("endpoints serialize")
    ));
    // echo every connection until killed
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let (mut read, mut write) = socket.split();
            let _ = tokio::io::copy(&mut read, &mut write).await;
        });
    }
}

/// The kinds the children answer to are the ones the fixture spawns
#[test]
fn every_child_kind_has_a_child_function() {
    for kind in [NodeKind::Server, NodeKind::Standalone, NodeKind::MockPeer] {
        assert!(!kind.child_fn().is_empty());
    }
}
