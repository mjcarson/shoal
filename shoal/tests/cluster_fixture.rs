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

use cluster::schema::{Note, NoteGet, Row, RowGet, TestDb, TestDbClient};
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
    // the group's own membership entry is one change and the bootstrap another; the observation
    // that follows repeats the member record the bootstrap carried, so it moves nothing
    assert_eq!(version, 2, "a fresh bootstrap is two topology changes, saw {version}");
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
    //
    // the claim itself counts one more start of the directory and writes that down, so the
    // bytes held to are the ones after it; a refusal is what must not move them
    let identity = StorageMeta::claim(cluster.dir(0), marker.shards, ClusterIntent::Bootstrap)
        .expect("the directory reopens under its own mode");
    assert_eq!(identity.node.to_string(), node);
    assert!(identity.incarnation > marker.incarnation, "a reopen did not count a start");
    let before = std::fs::read(StorageMeta::path(cluster.dir(0)))?;
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
/// misspelled key under `cluster:` names the key; a block that both creates a cluster and
/// joins one says so; and a format 1 marker - the shape every directory written before M1
/// has - names the format, the formats this build reads, and that no migration exists yet.
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
    // a block that both creates a cluster and joins one is refused at startup, saying which
    let conf = utils::build_crash_config(dir.path(), 0).cluster(
        ClusterConf::default()
            .bootstrap(true)
            .seeds(vec!["10.0.0.1:12002".to_string()]),
    );
    let error = match ShoalPool::<TestDb>::start(conf) {
        Ok(pool) => {
            let _ = pool.exit();
            panic!("a node that both bootstraps and joins started");
        }
        Err(error) => error,
    };
    assert!(
        matches!(&error, ServerError::Shoal(ShoalError::InvalidConfig(_))),
        "the wrong refusal: {error:?}"
    );
    assert!(format!("{error}").contains("not both"), "{error}");
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
    assert!(reason.contains("reads [2, 3]"), "the refusal did not name what it reads: {reason}");
    assert!(reason.contains("no migration"), "the refusal did not say there is no migration: {reason}");
    // and a format from the future the same way, whatever else it carries
    let refused = match Cluster::builder()
        .standalone(CoreClaim::Count(1))
        .staged_marker("{\n  \"format\": 4,\n  \"shards\": 2,\n  \"future\": true\n}")
        .start()
        .await
    {
        Ok(_) => panic!("a format 4 marker started"),
        Err(refused) => refused,
    };
    assert!(format!("{refused:?}").contains("format 4"));
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

/// A cross-node bundle preserves coverage, ordering and response identity (C2 M2)
///
/// Two nodes of one shard each, placed by name so tablet t is owned by node t % 2. A client on
/// node 0 inserts a range of keys - roughly half owned by node 1 - reads each back, then sends
/// one get naming keys on both nodes and asserts every index is answered exactly once with the
/// row it named. The reads for node 1's keys can only be answered by forwarding to node 1 and
/// relaying the answer back, so a correct read is a correct hop
/// ([F38](../../docs/src/features/inter-node-transport.md)).
#[tokio::test(flavor = "multi_thread")]
async fn remote_query_returns_one_result_per_index() -> Result<(), FixtureError> {
    // a placed cluster of two nodes, one shard each, plaintext lanes
    let cluster = Cluster::builder().cluster(2, CoreClaim::Count(1)).start().await?;
    // both nodes are in one cluster and each bound a data endpoint
    assert!(cluster.node(0).endpoints.data.is_some(), "node 0 bound no peer endpoint");
    assert!(cluster.node(1).endpoints.data.is_some(), "node 1 bound no peer endpoint");
    assert_eq!(
        cluster.node(0).endpoints.cluster, cluster.node(1).endpoints.cluster,
        "the two nodes are in different clusters"
    );
    // a client talks to node 0, which is the coordinator for every query it sends
    let addr = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    // insert a range of keys; node 0 keeps its own and forwards node 1's
    const KEYS: u64 = 200;
    for key in 0..KEYS {
        client
            .send_one(Row {
                key,
                data: format!("row-{key}"),
            })
            .await?;
    }
    // read each back, whichever node owns it, and check it is the row we wrote
    for key in 0..KEYS {
        let response = client.send_one(RowGet::new(vec![key])).await?;
        let rows = response.access::<Row>()?.expect("a get that found nothing");
        let row = rows.first().expect("a get that returned no rows");
        assert_eq!(row.key.to_native(), key);
        assert_eq!(row.data.as_str(), format!("row-{key}"), "the row for {key} came back changed");
    }
    // one get naming keys on both nodes at once: a query split across a local and a remote shard,
    // answered as one response per index in the order the keys were named
    let named: Vec<u64> = (0..24).collect();
    let response = client.send_one(RowGet::new(named.clone())).await?;
    let rows = response.access::<Row>()?.expect("the split get found nothing");
    let got: std::collections::BTreeSet<u64> = rows.iter().map(|row| row.key.to_native()).collect();
    for key in &named {
        assert!(got.contains(key), "the split get lost key {key}");
    }
    assert_eq!(got.len(), named.len(), "the split get answered a key more than once");
    // nothing died on either node
    assert_eq!(cluster.node(0).failure(), None);
    assert_eq!(cluster.node(1).failure(), None);
    drop(client);
    Ok(())
}

/// A stalled bulk stream is bounded and does not block progress traffic (C2 M2)
///
/// Two nodes with a proxy on each lane. Node 0 streams far more bulk bytes at node 1 than the
/// bulk queue holds, through a delayed data-and-bulk proxy: the queue plateaus at its bound and
/// sheds the rest, and node 0's memory does not run away. While it is stalled, control pings to
/// node 1 - a separate lane on a separate socket and thread - keep answering, which is the
/// progress traffic C2 says a bulk transfer must never block. Then the data lane is cut and a
/// forwarded query answers with a definite outcome inside its deadline rather than hanging, while
/// pings still answer ([F38](../../docs/src/features/inter-node-transport.md)).
#[tokio::test(flavor = "multi_thread")]
async fn slow_peer_has_bounded_bytes_and_independent_lanes() -> Result<(), FixtureError> {
    use cluster::LinkState;
    let mut cluster = Cluster::builder()
        .cluster(2, CoreClaim::Count(2))
        .lane_links(true)
        .start()
        .await?;
    // node 0's bulk link to node 1 shares its data proxy toward node 1; delay it so the stream stalls
    cluster.data_link(0, 1).delay(Duration::from_secs(60));
    let before = cluster.node(0).rss_kib();
    // stream far more than the 64 MiB bulk queue holds
    let probe = cluster.node_mut(0).command("PROBE_BULK 1 536870912")?;
    assert!(probe.get("ok").is_some(), "the bulk probe was refused: {probe}");
    // poll the transport view until the bulk link's queued bytes plateau under the bound and it
    // has started shedding
    let bound: u64 = 64 * 1024 * 1024;
    let mut shed = false;
    let mut queued = 0u64;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let view = cluster.node_mut(0).command("TRANSPORT")?;
        if let Some(links) = view.get("ok").and_then(|v| v.as_array()).and_then(|shards| shards.first()).and_then(|shard| shard.get("links")).and_then(|l| l.as_array()) {
            for link in links {
                if link.get("lane").and_then(|l| l.as_str()) == Some("bulk") {
                    queued = link.get("queued_bytes").and_then(serde_json::Value::as_u64).unwrap_or(0);
                    let s = link.get("shed_frames").and_then(serde_json::Value::as_u64).unwrap_or(0);
                    if s > 0 {
                        shed = true;
                    }
                }
            }
        }
        if shed && queued > 0 {
            break;
        }
    }
    assert!(shed, "the bulk lane never shed, so nothing bounded it (queued {queued})");
    assert!(queued <= bound, "the bulk queue held {queued}, past its {bound} byte bound");
    // memory did not run away with a stream eight times the bound
    let grew = cluster.node(0).rss_kib().saturating_sub(before);
    assert!(grew < (bound / 1024) * 3, "node 0 grew {grew} KiB, more than three bulk bounds");
    // progress traffic survives: a control ping to node 1 still answers
    let ping = cluster.node_mut(0).command("PING 1")?;
    assert!(ping.get("ok").is_some(), "a control ping did not survive the bulk stall: {ping}");

    // now cut the data lane to node 1: a forwarded query gets a definite outcome, not a hang
    cluster.data_link(0, 1).cut();
    assert_eq!(cluster.data_link(0, 1).state(), LinkState::Cut);
    // a key owned by node 1: node 0 forwards it, the cut lane fails it definitely
    let addr = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    // find a key node 1 owns, so the query must cross the cut lane
    let key = key_on_other_node(&cluster);
    let answered = tokio::time::timeout(
        Duration::from_secs(12),
        client.send_one(RowGet::new(vec![key])),
    )
    .await;
    match answered {
        // a definite outcome, whichever it is: a Shedding/Unavailable/OutcomeUnknown error, or an
        // empty read. What matters is that it answered at all rather than hanging on the cut lane
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            let text = format!("{error:?}");
            assert!(
                text.contains("Unavailable")
                    || text.contains("OutcomeUnknown")
                    || text.contains("Shedding"),
                "the cut lane gave an unexpected error: {text}"
            );
        }
        Err(_) => panic!("a forwarded query to a cut lane hung instead of failing definitely"),
    }
    // and control still answers through all of it
    let ping = cluster.node_mut(0).command("PING 1")?;
    assert!(ping.get("ok").is_some(), "a control ping did not survive the data cut: {ping}");
    drop(client);
    assert_eq!(cluster.node(0).failure(), None);
    assert_eq!(cluster.node(1).failure(), None);
    Ok(())
}

/// A partition key that a two-node placement owns on node 1 rather than node 0
///
/// Tablet `t` belongs to `nodes[t % 2]`, and the top twelve bits of the partition hash name the
/// tablet, so a key whose hash puts it in an odd tablet is node 1's.
fn key_on_other_node(_cluster: &Cluster) -> u64 {
    use shoal::shared::traits::PartitionKeySupport;
    // walk keys until one lands in an odd tablet (node 1 of two), the same hash and tablet split
    // the ring uses ([F38](../../docs/src/features/inter-node-transport.md))
    for candidate in 0..1_000_000u64 {
        let hash = Row::get_partition_key_from_values(&candidate);
        let tablet = (hash >> (u64::BITS - 12)) as usize;
        if tablet % 2 == 1 {
            return candidate;
        }
    }
    panic!("no key landed on node 1");
}

/// A forwarded query's trace crosses the node boundary without a false batch parent (C2 M2)
///
/// A client sends node 0 a bundle of three queries, all owned by node 1. Node 0 opens one
/// `Coordinator::route` span per query under the bundle's `Shoal::request`, and forwards each
/// carrying that span's context; node 1 opens a `Shoal::forwarded` span that adopts it. Every
/// node-1 forwarded span therefore hangs off node 0's per-query route span, in node 0's trace,
/// and never off the bundle's request root - which is what "without a false batch parent" means
/// ([F38](../../docs/src/features/inter-node-transport.md)).
#[tokio::test(flavor = "multi_thread")]
async fn trace_context_crosses_nodes_without_false_batch_parent() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder().cluster(2, CoreClaim::Count(2)).trace().start().await?;
    // three keys all owned by node 1, so every one is forwarded and traced across the hop
    let keys: Vec<u64> = {
        use shoal::shared::traits::PartitionKeySupport;
        let mut found = Vec::new();
        for candidate in 0..1_000_000u64 {
            let hash = Row::get_partition_key_from_values(&candidate);
            if (hash >> (u64::BITS - 12)) as usize % 2 == 1 {
                found.push(candidate);
                if found.len() == 3 {
                    break;
                }
            }
        }
        found
    };
    // send them in one bundle to node 0, then read them back so the spans are opened and closed
    let addr = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    for key in &keys {
        client.send_one(Row { key: *key, data: format!("t-{key}") }).await?;
    }
    let mut bundle = client.query();
    for key in &keys {
        bundle = bundle.add(RowGet::new(vec![*key]));
    }
    let mut stream = client.send(bundle).await?;
    while stream.next().await?.is_some() {}
    drop(client);
    // flush both nodes' spans, then read them back
    let _ = cluster.node_mut(0).command("FLUSH")?;
    let _ = cluster.node_mut(1).command("FLUSH")?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let node0_spans = read_spans(cluster.dir(0).join("trace.jsonl"));
    let node1_spans = read_spans(cluster.dir(1).join("trace.jsonl"));
    assert!(!node0_spans.is_empty(), "node 0 exported no spans");
    assert!(!node1_spans.is_empty(), "node 1 exported no spans");

    // node 0's request roots and its per-query route spans
    let request_ids: std::collections::BTreeSet<&str> = node0_spans
        .iter()
        .filter(|s| s.name == "Shoal::request")
        .map(|s| s.span_id.as_str())
        .collect();
    let route: std::collections::BTreeMap<&str, &str> = node0_spans
        .iter()
        .filter(|s| s.name == "Coordinator::route")
        .map(|s| (s.span_id.as_str(), s.trace_id.as_str()))
        .collect();
    assert!(route.len() >= 3, "node 0 opened {} route spans, expected at least 3", route.len());
    // every forwarded span on node 1 hangs off a route span on node 0, in that span's trace, and
    // none hangs off the request root
    let forwarded: Vec<_> = node1_spans.iter().filter(|s| s.name == "Shoal::forwarded").collect();
    assert!(forwarded.len() >= 3, "node 1 opened {} forwarded spans, expected at least 3", forwarded.len());
    for span in &forwarded {
        let parent_trace = route.get(span.parent_span_id.as_str());
        assert!(
            parent_trace.is_some(),
            "a forwarded span's parent {} is not a route span on node 0",
            span.parent_span_id
        );
        assert_eq!(
            parent_trace.copied(),
            Some(span.trace_id.as_str()),
            "a forwarded span is in a different trace than its parent route span"
        );
    }
    // no node-1 span is parented directly to node 0's request root (the false batch parent)
    for span in &node1_spans {
        assert!(
            !request_ids.contains(span.parent_span_id.as_str()),
            "node-1 span {} hangs off the bundle's request root rather than a query's route span",
            span.name
        );
    }
    assert_eq!(cluster.node(0).failure(), None);
    assert_eq!(cluster.node(1).failure(), None);
    Ok(())
}

/// One exported span, as the child wrote it
struct TraceSpan {
    /// The span's name
    name: String,
    /// The trace it belongs to
    trace_id: String,
    /// Its own id
    span_id: String,
    /// The span it hangs off, or all-zeroes if it is a root
    parent_span_id: String,
}

/// Read a node's exported spans back from its trace file
///
/// # Arguments
///
/// * `path` - The trace file
fn read_spans(path: std::path::PathBuf) -> Vec<TraceSpan> {
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    text.lines()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            Some(TraceSpan {
                name: value.get("name")?.as_str()?.to_string(),
                trace_id: value.get("trace_id")?.as_str()?.to_string(),
                span_id: value.get("span_id")?.as_str()?.to_string(),
                parent_span_id: value.get("parent_span_id")?.as_str()?.to_string(),
            })
        })
        .collect()
}

/// The control lane reaches a placed peer's own Raft (C2 M2, control lane)
///
/// Two nodes, placed by name, plaintext control lanes. Node 0 sends node 1 a vote for a low term
/// over the control lane; node 1, having elected itself and holding a newer term, does not grant
/// it - and that it answered at all is the proof node 0's `RaftNetworkV2` reached node 1's own
/// `Raft` end to end ([F38](../../docs/src/features/inter-node-transport.md)). A control ping
/// proves the listener answers too. The full "control survives a stalled *data* lane" assertion
/// needs the data-lane proxy and lands with the bounded-lanes test.
#[tokio::test(flavor = "multi_thread")]
async fn control_lane_answers_a_vote_from_a_placed_peer() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder().cluster(2, CoreClaim::Count(1)).start().await?;
    // node 0 pings node 1 over the control lane: the listener answers
    let ping = cluster.node_mut(0).command("PING 1")?;
    assert!(
        ping.get("ok").and_then(|ok| ok.get("micros")).is_some(),
        "the control ping did not answer: {ping}"
    );
    // node 0 sends node 1 a vote: node 1's own Raft answers, and does not grant a stale vote
    let probe = cluster.node_mut(0).command("VOTE_PROBE 1")?;
    let granted = probe
        .get("ok")
        .and_then(|ok| ok.get("granted"))
        .and_then(serde_json::Value::as_bool);
    assert_eq!(
        granted,
        Some(false),
        "node 1's Raft did not answer the vote, or granted a stale one: {probe}"
    );
    // and the reverse direction works too, proving both listeners
    let back = cluster.node_mut(1).command("VOTE_PROBE 0")?;
    assert_eq!(
        back.get("ok").and_then(|ok| ok.get("granted")).and_then(serde_json::Value::as_bool),
        Some(false),
        "node 0's Raft did not answer node 1's vote: {back}"
    );
    assert_eq!(cluster.node(0).failure(), None);
    assert_eq!(cluster.node(1).failure(), None);
    Ok(())
}

/// Install an OpenTelemetry subscriber that appends every exported span to a file
///
/// Returns the provider, which the child holds so a `FLUSH` command can force it. `None` when no
/// trace file was asked for, in which case the child installs no subscriber, exactly as it did
/// before this test existed.
///
/// # Arguments
///
/// * `path` - Where to write the spans, if anywhere
fn install_trace_exporter(path: Option<String>) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    let path = path?;
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(FileSpanExporter::new(path))
        .build();
    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "cluster_child");
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    Some(provider)
}

/// An exporter that appends each span to a file as one json line
///
/// The four fields the cross-node trace test asks about: the span's name, its trace, its own id
/// and the id of the span it hangs off. Written as hex so a person can diff two nodes' files.
#[derive(Debug)]
struct FileSpanExporter {
    /// The file every batch is appended to
    file: std::sync::Arc<std::sync::Mutex<std::fs::File>>,
}

impl FileSpanExporter {
    /// Open the file the spans go to
    ///
    /// # Arguments
    ///
    /// * `path` - Where to write
    fn new(path: String) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open a trace file");
        FileSpanExporter {
            file: std::sync::Arc::new(std::sync::Mutex::new(file)),
        }
    }
}

impl opentelemetry_sdk::trace::SpanExporter for FileSpanExporter {
    /// Append every span in a batch as a json line
    ///
    /// # Arguments
    ///
    /// * `batch` - The spans being exported
    fn export(
        &mut self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> futures::future::BoxFuture<'static, opentelemetry_sdk::error::OTelSdkResult> {
        use std::io::Write as _;
        let file = self.file.clone();
        let mut file = file.lock().expect("the trace file lock");
        for span in batch {
            let line = serde_json::json!({
                "name": span.name.to_string(),
                "trace_id": format!("{:032x}", u128::from_be_bytes(span.span_context.trace_id().to_bytes())),
                "span_id": format!("{:016x}", u64::from_be_bytes(span.span_context.span_id().to_bytes())),
                "parent_span_id": format!("{:016x}", u64::from_be_bytes(span.parent_span_id.to_bytes())),
            });
            let _ = writeln!(file, "{line}");
        }
        let _ = file.flush();
        Box::pin(std::future::ready(Ok(())))
    }
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
    // when the parent asked for a trace file, install an OpenTelemetry subscriber that writes
    // every exported span to it as one json line, so the cross-node trace test can read both
    // nodes' spans back and check the hop's parentage. The pool installs no subscriber, so this
    // global default is uncontested ([F38](../../docs/src/features/inter-node-transport.md))
    let trace_provider = install_trace_exporter(request.cluster.as_ref().and_then(|c| c.trace_file.clone()));
    // a log file per child when asked for, so a failing cluster test leaves its children's
    // side of the story behind: `SHOAL_CHILD_LOG=<dir>` writes `<dir>/child-<pid>.log`
    if trace_provider.is_none() {
        if let Ok(dir) = std::env::var("SHOAL_CHILD_LOG") {
            let path = std::path::Path::new(&dir).join(format!("child-{}.log", std::process::id()));
            if let Ok(file) = std::fs::File::create(path) {
                let _ = tracing_subscriber::fmt()
                    .with_writer(std::sync::Mutex::new(file))
                    .with_ansi(false)
                    .with_max_level(tracing::Level::DEBUG)
                    .try_init();
            }
        }
    }
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
    // a cluster node bootstraps itself or joins, with its control thread where the parent put it
    if request.kind == NodeKind::Server {
        let mut block = ClusterConf::default()
            .bootstrap(true)
            .control_core(request.control_cpu.unwrap_or(0))
            .control_core_shared(request.control_shared)
            .replication_factor(1);
        // a member of a fixture cluster gets its ports, its seeds, its policy and where it
        // dials its peers ([F39](../../docs/src/features/membership.md))
        if let Some(staged) = &request.cluster {
            use shoal::shared::identity::NodeId;
            block = block
                .bootstrap(staged.bootstrap)
                .seeds(staged.seeds.clone())
                .port(staged.data_port)
                .control_port(staged.control_port)
                .replication_factor(staged.replication_factor)
                .control_voters(staged.control_voters);
            block.admins = staged.admins.clone();
            if let Some(interval) = staged.detector_interval_ms {
                block = block.detector_interval_ms(interval);
            }
            for (node, control, data) in &staged.dial {
                let node = NodeId(node.parse().expect("a node id parses"));
                block = block.dial(node, Some(control.clone()), Some(data.clone()));
            }
            // a cluster that requires authentication names its users on every node
            if !staged.auth.is_empty() {
                let mut auth = shoal::server::conf::Auth::default().required(true);
                for (user, password) in &staged.auth {
                    auth = auth.user(user, password);
                }
                conf = conf.auth(auth);
            }
        }
        conf = conf.cluster(block);
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
    // plane - if there is one - is serving
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
    let readiness = pool.readiness().ok();
    let shard_cpus = pool.shard_cpus().to_vec();
    // the peer and control endpoints a cluster node bound, for the parent to record
    let (data, control_ep) = match &request.cluster {
        Some(staged) => (
            Some(format!("127.0.0.1:{}", staged.data_port).parse().expect("a data addr")),
            Some(format!("127.0.0.1:{}", staged.control_port).parse().expect("a control addr")),
        ),
        None => (None, None),
    };
    let endpoints = Endpoints {
        client,
        data,
        control: control_ep,
        node: Some(identity.node.to_string()),
        cluster: identity.cluster.map(|cluster| cluster.to_string()),
        control_core: pool.control_placement().map(|placement| placement.cpu),
        control_shared: pool.control_placement().is_some_and(|placement| placement.shared),
        topology_version: topology.as_ref().map(|view| view.version),
        shard_cpus,
        incarnation: Some(identity.incarnation),
        control_status: readiness.map(|view| view.control.name().to_string()),
    };
    report(&format!(
        "{} {}",
        cluster::READY_LINE,
        serde_json::to_string(&endpoints).expect("endpoints serialize")
    ));
    // a cluster node answers commands on its stdin, for the tests to drive it, while still
    // watching for a shard death; a standalone node has no peers and just watches
    if let Some(staged) = request.cluster.clone() {
        use tokio::io::AsyncBufReadExt as _;
        // resolve a node index in a command to the NodeId the fixture minted for it
        let peers: Vec<shoal::shared::identity::NodeId> = staged
            .peers
            .iter()
            .map(|node| shoal::shared::identity::NodeId(node.parse().expect("a node id")))
            .collect();
        let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
        let mut watch = tokio::time::interval(Duration::from_millis(50));
        // whether a shard death was asked for, in which case it is logged rather than fatal
        let mut tolerate_failure = false;
        loop {
            tokio::select! {
                // the next command line, or stdin closing
                line = stdin.next_line() => match line {
                    Ok(Some(line)) => {
                        let line = line.trim();
                        if line.starts_with("FAIL_SHARD") {
                            tolerate_failure = true;
                        }
                        report(&handle_command(&pool, &peers, &request.dir, trace_provider.as_ref(), line));
                    }
                    // stdin closed: the parent is done with us, run until killed
                    Ok(None) => break,
                    Err(_) => break,
                },
                // watch for a shard death between commands
                _ = watch.tick() => {
                    if let Some((shard, error)) = pool.failure() {
                        if tolerate_failure && shard != usize::MAX {
                            eprintln!("shard {shard} died as asked: {error}");
                        } else {
                            report(&format!("{} shard {shard} died: {error}", cluster::FAILED_LINE));
                            std::process::exit(1);
                        }
                    }
                }
            }
        }
    }
    // then relay a shard's death, should one happen, and otherwise run until killed
    loop {
        if let Some((shard, error)) = pool.failure() {
            report(&format!("{} shard {shard} died: {error}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Answer one command from a test, as a `SHOAL_CLUSTER_REPLY <json>` line
///
/// # Arguments
///
/// * `pool` - This node's pool
/// * `peers` - The node ids a command's index resolves against, in index order
/// * `dir` - This node's storage directory
/// * `trace_provider` - The span exporter, if one was installed
/// * `line` - The command line
fn handle_command(
    pool: &ShoalPool<TestDb>,
    peers: &[shoal::shared::identity::NodeId],
    dir: &std::path::Path,
    trace_provider: Option<&opentelemetry_sdk::trace::SdkTracerProvider>,
    line: &str,
) -> String {
    use shoal::server::{AdminKind, AdminRequest};
    let mut parts = line.split_whitespace();
    let verb = parts.next().unwrap_or("");
    // resolve the next token as a node index
    let node_at = |parts: &mut std::str::SplitWhitespace| -> Option<shoal::shared::identity::NodeId> {
        parts.next().and_then(|idx| idx.parse::<usize>().ok()).and_then(|idx| peers.get(idx).copied())
    };
    // an administrative request made as the process itself, against the current version
    //
    // a version that moved between the read and the proposal - a promotion committing, a
    // member observing itself - is what an operator's tool retries, so this does, a few times
    let admin = |kind: AdminKind| -> Result<serde_json::Value, String> {
        let op = uuid::Uuid::new_v4();
        let mut last = String::new();
        for _ in 0..8 {
            let version = pool.topology().map_err(|error| format!("{error:?}"))?.version;
            let response = pool
                .admin(AdminRequest {
                    op,
                    expected_version: version,
                    kind: kind.clone(),
                })
                .map_err(|error| format!("{error:?}"))?;
            match response.outcome {
                Ok(shoal::shared::protocol::admin::AdminOutcome::Applied { version })
                | Ok(shoal::shared::protocol::admin::AdminOutcome::Repeated { version }) => {
                    return Ok(serde_json::json!({ "version": version }));
                }
                Ok(shoal::shared::protocol::admin::AdminOutcome::Read(value)) => return Ok(value),
                Err(error) if error.code() == shoal::shared::protocol::error::ErrorCode::StaleVersion => {
                    last = format!("{}: {}", error.code(), error.msg);
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(error) => return Err(format!("{}: {}", error.code(), error.msg)),
            }
        }
        Err(last)
    };
    let result: Result<serde_json::Value, String> = match verb {
        // ping a peer over the control lane; the reply is how many microseconds it took
        "PING" => match node_at(&mut parts) {
            Some(node) => pool
                .control_ping(node)
                .map(|elapsed| serde_json::json!({ "micros": elapsed.as_micros() as u64 }))
                .map_err(|error| format!("{error:?}")),
            None => Err("PING needs a node index".to_string()),
        },
        // send a peer a vote for a low term; the reply is whether it granted it
        "VOTE_PROBE" => match node_at(&mut parts) {
            Some(node) => pool
                .control_vote_probe(node)
                .map(|probe| serde_json::json!({ "granted": probe.granted }))
                .map_err(|error| format!("{error:?}")),
            None => Err("VOTE_PROBE needs a node index".to_string()),
        },
        // this node's peer links, for the bounded-lanes test
        "TRANSPORT" => pool
            .transport()
            .map(|views| serde_json::to_value(views).expect("views serialize"))
            .map_err(|error| format!("{error:?}")),
        // stream bytes at a peer on the bulk lane, for the bounded-lanes test
        "PROBE_BULK" => match node_at(&mut parts) {
            Some(node) => parts
                .next()
                .and_then(|bytes| bytes.parse::<u64>().ok())
                .ok_or_else(|| "PROBE_BULK needs a byte count".to_string())
                .and_then(|bytes| {
                    pool.probe_bulk(node, bytes)
                        .map(|()| serde_json::json!({ "started": bytes }))
                        .map_err(|error| format!("{error:?}"))
                }),
            None => Err("PROBE_BULK needs a node index".to_string()),
        },
        // the cluster as this node sees it
        "MEMBERS" => pool
            .topology()
            .map(|view| serde_json::to_value(view).expect("a view serializes"))
            .map_err(|error| format!("{error:?}")),
        // where this node stands
        "READINESS" => pool
            .readiness()
            .map(|view| serde_json::to_value(view).expect("a view serializes"))
            .map_err(|error| format!("{error:?}")),
        // the map this node's shards route with
        "MAP" => pool
            .map()
            .map(|map| serde_json::to_value(&*map).expect("a map serializes"))
            .map_err(|error| format!("{error:?}")),
        // place the tablets over these nodes, in this order
        "INITIALIZE" => {
            let mut nodes = Vec::new();
            let mut bad = None;
            for token in parts.by_ref() {
                match token.parse::<usize>().ok().and_then(|idx| peers.get(idx).copied()) {
                    Some(node) => nodes.push(node),
                    None => bad = Some(token.to_string()),
                }
            }
            match bad {
                Some(token) => Err(format!("INITIALIZE: {token} is not a node index")),
                None => admin(AdminKind::Initialize { nodes }),
            }
        }
        // change the voter policy
        "SET_VOTERS" => match parts.next().and_then(|count| count.parse::<u32>().ok()) {
            Some(count) => admin(AdminKind::SetControlVoters { count }),
            None => Err("SET_VOTERS needs a count".to_string()),
        },
        // any administrative request, as json
        "ADMIN" => {
            let json = line.trim_start_matches("ADMIN").trim();
            match serde_json::from_str::<AdminRequest>(json) {
                Ok(request) => pool
                    .admin(request)
                    .map(|response| serde_json::to_value(response).expect("a response serializes"))
                    .map_err(|error| format!("{error:?}")),
                Err(error) => Err(format!("ADMIN: {error}")),
            }
        }
        // which start of this node this is
        "INCARNATION" => Ok(serde_json::json!({ "incarnation": pool.identity().incarnation })),
        // how many bytes the control log holds, so a test can see whether an entry was written
        "LOG_LEN" => std::fs::metadata(dir.join("control").join("log"))
            .map(|meta| serde_json::json!({ "bytes": meta.len() }))
            .map_err(|error| format!("{error}")),
        // kill one of this node's shards, for the shard-health test
        "FAIL_SHARD" => match parts.next().and_then(|idx| idx.parse::<usize>().ok()) {
            Some(shard) => pool
                .fail_shard(shard)
                .map(|()| serde_json::json!({ "failed": shard }))
                .map_err(|error| format!("{error:?}")),
            None => Err("FAIL_SHARD needs a shard index".to_string()),
        },
        // send the leader one report at an incarnation below this node's
        "STALE_REPORT" => pool
            .control_stale_report()
            .map(|()| serde_json::json!({ "sent": true }))
            .map_err(|error| format!("{error:?}")),
        // flush this node's exported spans to its trace file
        "FLUSH" => {
            if let Some(provider) = trace_provider {
                let _ = provider.force_flush();
            }
            Ok(serde_json::json!({ "flushed": true }))
        }
        other => Err(format!("unknown command {other:?}")),
    };
    // one reply line per command, an ok or an error object
    let json = match result {
        Ok(value) => serde_json::json!({ "ok": value }),
        Err(error) => serde_json::json!({ "error": error }),
    };
    format!("{} {}", cluster::REPLY_LINE, json)
}

/// Three Shoal processes converge on one cluster and recover its metadata after a restart (C3 M3)
///
/// Node 0 bootstraps, nodes 1 and 2 join through it, and the three-voter policy promotes both
/// joiners. Every node's view of the members, the voters and the leader is the same. Then all
/// three are killed and restarted on their directories: each comes back as itself, in the same
/// cluster, from its own log, and a leader is elected again from what they recovered - with no
/// process the test did not start ([C3](../../docs/src/distributed/membership.md)).
#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_bootstrap_without_external_membership() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder().cluster(3, CoreClaim::Count(1)).start().await?;
    cluster.wait_voters(0, 3)?;
    // every node agrees on the cluster, its members, its voters and its leader
    let ids = cluster.node_ids();
    // a follower applies the last membership entry a moment after the leader commits it
    for id in 0..3 {
        cluster.wait_voters(id, 3)?;
    }
    let views: Vec<serde_json::Value> = (0..3).map(|id| cluster.members(id)).collect::<Result<_, _>>()?;
    for (id, view) in views.iter().enumerate() {
        assert_eq!(view["cluster"], views[0]["cluster"], "node {id} is in another cluster");
        assert_eq!(view["members"].as_array().map(Vec::len), Some(3), "node {id} sees {}", view["members"]);
        let mut voters: Vec<String> = view["voters"].as_array().expect("voters").iter().map(|v| v.as_str().unwrap().to_string()).collect();
        voters.sort();
        let mut expected = ids.clone();
        expected.sort();
        assert_eq!(voters, expected, "node {id} sees other voters");
        assert!(view["learners"].as_array().is_some_and(Vec::is_empty), "node {id} sees a learner");
        assert_eq!(view["control"], "joined", "node {id} is not joined");
    }
    let leader_before = cluster.leader_of(0)?.expect("a leader");
    for id in 1..3 {
        assert_eq!(cluster.leader_of(id)?, Some(leader_before.clone()), "node {id} names another leader");
    }
    let version_before = views[0]["version"].as_u64().expect("a version");
    // the only processes are the three children
    assert_eq!(cluster.pids().len(), 3);
    // kill every node, then restart every one on its own directory
    for id in 0..3 {
        cluster.kill(id)?;
    }
    for id in 0..3 {
        cluster.restart(id, NodeKind::Server)?;
    }
    cluster.wait_joined(&[0, 1, 2])?;
    // each is the same node in the same cluster, with the same members, and a leader again
    let leader_after = cluster.wait_leader_among(0, &[0, 1, 2], Duration::from_secs(30))?;
    for id in 0..3 {
        let view = cluster.members(id)?;
        assert_eq!(view["cluster"], views[0]["cluster"], "node {id} came back in another cluster");
        assert_eq!(cluster.node(id).endpoints.node, Some(ids[id].clone()), "node {id} came back as somebody else");
        assert_eq!(view["members"].as_array().map(Vec::len), Some(3));
        assert!(view["version"].as_u64().expect("a version") >= version_before, "node {id} lost history");
        assert_eq!(cluster.wait_leader_among(id, &[0, 1, 2], Duration::from_secs(30))?, leader_after, "node {id} names another leader after the restart");
    }
    // and the data path works across them: a write through node 1 read through node 2
    let one = Shoal::<TestDbClient>::new(&cluster.node(1).endpoints.client.to_string()).await?;
    let two = Shoal::<TestDbClient>::new(&cluster.node(2).endpoints.client.to_string()).await?;
    for key in 0..30u64 {
        one.send_one(Row { key, data: format!("row-{key}") }).await?;
    }
    for key in 0..30u64 {
        let response = two.send_one(RowGet::new(vec![key])).await?;
        let rows = response.access::<Row>()?.expect("a get that found nothing");
        assert_eq!(rows.first().expect("a row").data.as_str(), format!("row-{key}"));
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A fourth node joins as a learner under the three-voter policy, and the policy decides (C3 M3)
///
/// Three voters, then a fourth node joins: it is admitted, it is up, it holds the log, and it
/// does not vote, because the policy says three. Raising the policy to five is what promotes
/// it - the count of nodes never does.
#[tokio::test(flavor = "multi_thread")]
async fn fourth_data_node_does_not_change_control_voter_count() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(4, CoreClaim::Count(1))
        .deferred_from(3)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    // the fourth joins
    cluster.start_deferred(3)?;
    cluster.wait_joined(&[3])?;
    let fourth = cluster.node_ids()[3].clone();
    // and stays a learner: given time to be promoted, it is not
    std::thread::sleep(Duration::from_secs(3));
    for id in 0..4 {
        let view = cluster.members(id)?;
        assert_eq!(view["voters"].as_array().map(Vec::len), Some(3), "node {id} sees {}", view["voters"]);
        assert_eq!(view["learners"], serde_json::json!([fourth]), "node {id} sees {}", view["learners"]);
        assert_eq!(view["members"].as_array().map(Vec::len), Some(4));
        let member = view["members"].as_array().unwrap().iter().find(|m| m["record"]["node"] == fourth).expect("the fourth");
        assert_eq!(member["health"], "up");
        assert_eq!(member["role"], "learner");
    }
    // the policy, not the count, decides: five voters asked for promotes the learner
    let reply = cluster.node_mut(0).command("SET_VOTERS 5")?;
    assert!(reply.get("ok").is_some(), "the policy change was refused: {reply}");
    cluster.wait_voters(0, 4)?;
    let view = cluster.members(0)?;
    assert_eq!(view["policy"]["control_voters"], 5);
    assert!(view["learners"].as_array().is_some_and(Vec::is_empty));
    Ok(())
}

/// An isolated control minority cannot change the membership (C3 M3)
///
/// Three voters with a proxy per direction per lane. Node 0's control lanes to and from the
/// other two are cut. A policy change asked of node 0 is refused for want of a leader; nodes 1
/// and 2 elect a leader between them and commit one. A fourth node whose only seed is node 0
/// is not admitted while node 0 is cut off. Healed, node 0 takes the majority's state and its
/// own attempt is nowhere, and the fourth node joins.
#[tokio::test(flavor = "multi_thread")]
async fn minority_cannot_commit_membership_changes() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(4, CoreClaim::Count(1))
        .deferred_from(3)
        .lane_links(true)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    // cut node 0 off from the other two voters, in both directions, on the control lane
    for (from, to) in [(0, 1), (0, 2), (1, 0), (2, 0)] {
        cluster.control_link(from, to).cut();
    }
    // the majority elects a leader between themselves
    let majority_leader = cluster.wait_leader_among(1, &[1, 2], Duration::from_secs(30))?;
    assert_eq!(cluster.wait_leader_among(2, &[1, 2], Duration::from_secs(30))?, majority_leader);
    // node 0 cannot commit a membership change: it has no quorum and can reach no leader
    let refused = cluster.node_mut(0).command("SET_VOTERS 5")?;
    let error = refused["error"].as_str().unwrap_or("");
    assert!(
        error.contains("NotLeader") || error.contains("StaleVersion") || error.contains("leader"),
        "the minority committed or answered oddly: {refused}"
    );
    // a fourth node seeded only through node 0 is not admitted
    cluster.start_deferred(3)?;
    std::thread::sleep(Duration::from_secs(5));
    let readiness = cluster.node_mut(3).command("READINESS")?;
    assert_ne!(readiness["ok"]["control"], "joined", "the minority admitted a joiner: {readiness}");
    let fourth = cluster.node_ids()[3].clone();
    let majority_view = cluster.members(majority_leader)?;
    assert!(
        !majority_view["members"].as_array().unwrap().iter().any(|m| m["record"]["node"] == fourth),
        "the majority saw the joiner the minority admitted: {majority_view}"
    );
    // the majority commits the same change
    let applied = cluster.node_mut(majority_leader).command("SET_VOTERS 5")?;
    assert!(applied.get("ok").is_some(), "the majority could not commit: {applied}");
    let majority_version = applied["ok"]["version"].as_u64().expect("a version");
    // healed, node 0 follows the majority's leader and holds the majority's state
    for (from, to) in [(0, 1), (0, 2), (1, 0), (2, 0)] {
        cluster.control_link(from, to).heal();
    }
    cluster.wait_version(0, majority_version)?;
    let leader = cluster.wait_leader_among(0, &[0, 1, 2], Duration::from_secs(30))?;
    for id in 1..3 {
        assert_eq!(cluster.wait_leader_among(id, &[0, 1, 2], Duration::from_secs(30))?, leader);
    }
    let view = cluster.members(0)?;
    assert_eq!(view["policy"]["control_voters"], 5);
    // and the fourth node joins now that its seed can reach a leader
    cluster.wait_joined(&[3])?;
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A restart with unreachable seeds keeps the established cluster and never bootstraps (C3 M3)
///
/// A member restarted with seeds naming nothing that answers comes up from its own log, as the
/// same node in the same cluster, recovering rather than joined, and writes no bootstrap. Once
/// a peer is back it joins again. And a fresh joiner whose seeds never answer stays a joiner:
/// its marker names no cluster, and bootstrapping it is refused by name.
#[tokio::test(flavor = "multi_thread")]
async fn lost_seeds_do_not_rebootstrap_existing_directory() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(4, CoreClaim::Count(1))
        .deferred_from(3)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let ids = cluster.node_ids();
    let before = cluster.members(1)?;
    let log_before = cluster.node_mut(1).command("LOG_LEN")?["ok"]["bytes"].as_u64().expect("a length");
    // kill both of node 1's peers, then restart it with seeds nothing answers at
    cluster.kill(0)?;
    cluster.kill(2)?;
    cluster.restart_with_seeds(1, vec!["127.0.0.1:1".to_string()])?;
    let endpoints = cluster.node(1).endpoints.clone();
    assert_eq!(endpoints.node, Some(ids[1].clone()), "node 1 came back as somebody else");
    assert_eq!(endpoints.cluster, before["cluster"].as_str().map(str::to_string), "node 1 changed cluster");
    // it is recovering: its log is there, and nobody leads
    let readiness = cluster.node_mut(1).command("READINESS")?;
    assert_eq!(readiness["ok"]["control"], "recovering", "{readiness}");
    let view = cluster.members(1)?;
    assert_eq!(view["members"].as_array().map(Vec::len), Some(3));
    assert!(view["version"].as_u64().unwrap() >= before["version"].as_u64().unwrap());
    let log_after = cluster.node_mut(1).command("LOG_LEN")?["ok"]["bytes"].as_u64().expect("a length");
    assert_eq!(log_after, log_before, "a restart with dead seeds wrote to the control log");
    // the same members, which a second cluster would not have
    assert_eq!(view["members"], before["members"], "node 1 came back with other members");
    // a peer back makes a majority, and node 1 is joined again through it
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0, 1])?;
    assert_eq!(cluster.members(1)?["cluster"], before["cluster"]);
    // a fresh joiner whose seeds never answer stays a joiner
    cluster.set_seeds(3, vec!["127.0.0.1:1".to_string()]);
    cluster.start_deferred(3)?;
    std::thread::sleep(Duration::from_secs(3));
    let readiness = cluster.node_mut(3).command("READINESS")?;
    assert_eq!(readiness["ok"]["control"], "joining", "{readiness}");
    let marker = StorageMeta::read(cluster.dir(3)).expect("a marker").expect("a marker");
    assert_eq!(marker.mode, shoal::server::meta::MarkerMode::Joining);
    assert_eq!(marker.cluster, None);
    // nothing was minted or committed: no cluster, no members, no version
    let view = cluster.members(3)?;
    assert_eq!(view["members"].as_array().map(Vec::len), Some(0), "{view}");
    assert_eq!(view["version"], 0, "{view}");
    // and it cannot be turned into a cluster of its own
    cluster.kill(3)?;
    let mut staged = cluster.staged(3).clone();
    staged.bootstrap = true;
    staged.seeds = Vec::new();
    let refused = cluster.restart_with(3, NodeKind::Server, Some(staged)).expect_err("a joiner's directory bootstrapped");
    let reason = format!("{refused:?}");
    assert!(reason.contains("joiner") && reason.contains("second cluster"), "{reason}");
    Ok(())
}

/// Isolated Shoal processes bootstrap, elect and recover with nothing but each other (C13 M3)
///
/// Three nodes, the tablets placed over nodes 1 and 2 alone. The bootstrapper leads; killing it
/// leaves the other two to elect a leader from their own storage and serve every write and
/// read. Restarted, the bootstrapper comes back as a follower of whoever leads now. Nothing but
/// the three children exists, and every address any of them dials is a member's.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_needs_no_external_coordinator() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .initialize(false)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    cluster.initialize(&[1, 2])?;
    assert_eq!(cluster.pids().len(), 3);
    // the bootstrapper leads a fresh cluster
    let leader = cluster.wait_leader_among(1, &[0, 1, 2], Duration::from_secs(30))?;
    // kill whoever leads; the survivors elect between themselves
    cluster.kill(leader)?;
    let survivors: Vec<usize> = (0..3).filter(|id| *id != leader).collect();
    let new_leader = cluster.wait_leader_among(survivors[0], &survivors, Duration::from_secs(30))?;
    assert_eq!(cluster.wait_leader_among(survivors[1], &survivors, Duration::from_secs(30))?, new_leader);
    // and serve: a write through one survivor read through the other, every tablet on them
    let a = Shoal::<TestDbClient>::new(&cluster.node(survivors[0]).endpoints.client.to_string()).await?;
    let b = Shoal::<TestDbClient>::new(&cluster.node(survivors[1]).endpoints.client.to_string()).await?;
    for key in 100..140u64 {
        a.send_one(Row { key, data: format!("row-{key}") }).await?;
    }
    for key in 100..140u64 {
        let response = b.send_one(RowGet::new(vec![key])).await?;
        let rows = response.access::<Row>()?.expect("a get that found nothing");
        assert_eq!(rows.first().expect("a row").data.as_str(), format!("row-{key}"));
    }
    // the killed node comes back a member, following the leader the survivors chose
    cluster.restart(leader, NodeKind::Server)?;
    cluster.wait_joined(&[leader])?;
    let agreed = cluster.wait_leader_among(leader, &[0, 1, 2], Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.wait_leader_among(id, &[0, 1, 2], Duration::from_secs(30))?, agreed);
    }
    // every address any node knows is a member's, and every member is one of the three
    let view = cluster.members(0)?;
    let ids = cluster.node_ids();
    for member in view["members"].as_array().expect("members") {
        let node = member["record"]["node"].as_str().expect("a node");
        assert!(ids.contains(&node.to_string()), "a member nobody started: {member}");
        let control = member["record"]["control"].as_str().expect("an address");
        assert!(control.starts_with("127.0.0.1:"), "a member reached somewhere else: {control}");
    }
    Ok(())
}

/// Two processes of one node identity cannot both serve (C1 M3)
///
/// Node 1 is killed and its directory copied. Restarted, it runs at the next incarnation; a
/// clone started from the copy runs at that same incarnation from another address, and is
/// refused as a duplicate identity. Started once more, the clone is a run later than the
/// original - and the original, superseded, stops. The cluster ends with one run of the node
/// on record, the newest.
#[tokio::test(flavor = "multi_thread")]
async fn duplicate_node_identity_is_fenced() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder().cluster(2, CoreClaim::Count(1)).start().await?;
    cluster.wait_voters(0, 2)?;
    let node1 = cluster.node_ids()[1].clone();
    let incarnation = cluster.node_mut(1).command("INCARNATION")?["ok"]["incarnation"].as_u64().expect("an incarnation");
    // stop node 1 and copy its directory as it stands
    cluster.kill(1)?;
    let copy = cluster.clone_dir(1)?;
    // the original comes back one start later
    cluster.restart(1, NodeKind::Server)?;
    cluster.wait_joined(&[1])?;
    assert_eq!(cluster.node(1).endpoints.incarnation, Some(incarnation + 1));
    // the clone starts from the copy at that same incarnation, from other ports: a duplicate
    let clone = cluster.spawn_clone(1, copy.path())?;
    let mut clone = clone;
    clone.wait_ready(Duration::from_secs(60))?;
    assert_eq!(clone.endpoints.incarnation, Some(incarnation + 1));
    let refused = Cluster::wait_failure(&clone, Duration::from_secs(60)).expect("the clone kept running");
    assert!(
        refused.contains("incarnation") || refused.contains("duplicate") || refused.contains("fenced"),
        "the clone failed for another reason: {refused}"
    );
    assert_eq!(cluster.node(1).failure(), None, "the original was fenced by a duplicate");
    drop(clone);
    // the clone's own start counted, so its next run is later than the original's, and wins
    let mut clone = cluster.spawn_clone(1, copy.path())?;
    clone.wait_ready(Duration::from_secs(60))?;
    assert_eq!(clone.endpoints.incarnation, Some(incarnation + 2));
    let fenced = Cluster::wait_failure(cluster.node(1), Duration::from_secs(60)).expect("the original kept running");
    assert!(fenced.contains("fenced") || fenced.contains("incarnation"), "the original stopped for another reason: {fenced}");
    // the cluster holds the newest run of the node, at the clone's address
    let view = cluster.members(0)?;
    let member = view["members"].as_array().unwrap().iter().find(|m| m["record"]["node"] == node1).expect("node 1");
    assert_eq!(member["record"]["incarnation"], incarnation + 2);
    assert_eq!(member["record"]["control"], format!("127.0.0.1:{}", clone.endpoints.control.expect("a control endpoint").port()));
    assert_eq!(clone.failure(), None, "the winning clone died");
    Ok(())
}

/// Control elections do not depend on the data lanes (C2 M3)
///
/// Three voters with every data lane delayed for a minute and then cut. The leader is killed
/// and the survivors elect a new one over their control lanes alone, which keep answering
/// pings; a data query that needs the cut lane fails with a named code rather than hanging.
#[tokio::test(flavor = "multi_thread")]
async fn control_elections_do_not_depend_on_data_shard_relay() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .lane_links(true)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    // stall then cut every data lane between every pair
    for from in 0..3 {
        for to in 0..3 {
            if from != to {
                cluster.data_link(from, to).delay(Duration::from_secs(60));
            }
        }
    }
    let leader = cluster.wait_leader_among(0, &[0, 1, 2], Duration::from_secs(30))?;
    cluster.kill(leader)?;
    let survivors: Vec<usize> = (0..3).filter(|id| *id != leader).collect();
    for from in &survivors {
        for to in &survivors {
            if from != to {
                cluster.data_link(*from, *to).cut();
            }
        }
    }
    // the survivors elect over their control lanes alone
    let new_leader = cluster.wait_leader_among(survivors[0], &survivors, Duration::from_secs(30))?;
    assert_eq!(cluster.wait_leader_among(survivors[1], &survivors, Duration::from_secs(30))?, new_leader);
    // and those lanes still answer pings both ways
    let ping = cluster.node_mut(survivors[0]).command(&format!("PING {}", survivors[1]))?;
    assert!(ping.get("ok").is_some(), "a control ping failed during the data stall: {ping}");
    let ping = cluster.node_mut(survivors[1]).command(&format!("PING {}", survivors[0]))?;
    assert!(ping.get("ok").is_some(), "a control ping failed during the data stall: {ping}");
    // a read that needs a cut data lane gets a named failure within the deadline, not a hang
    let client = Shoal::<TestDbClient>::new(&cluster.node(survivors[0]).endpoints.client.to_string()).await?;
    let mut failed = 0;
    for key in 0..12u64 {
        let outcome = tokio::time::timeout(Duration::from_secs(12), client.send_one(RowGet::new(vec![key]))).await;
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => failed += 1,
            Err(_) => panic!("a read for key {key} hung instead of failing"),
        }
    }
    assert!(failed > 0, "no read needed a cut lane, which the placement makes impossible");
    Ok(())
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

/// A get that found nothing, however the client reports it
///
/// A row that is not there comes back either as a response with no rows or as
/// `QueryDidNotSucceed`; a failure of any other kind is not "nothing".
fn found_nothing(result: Result<shoal::client::ShoalResponse<TestDbClient>, shoal::client::Errors>) -> bool {
    match result {
        Ok(response) => response.access::<Row>().map(|rows| rows.is_none_or(|rows| rows.is_empty())).unwrap_or(false),
        Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => true,
        Err(_) => false,
    }
}

/// The error code a query came back with, if it came back as a server failure
fn failure_code<T>(result: &Result<T, shoal::client::Errors>) -> Option<shoal::shared::protocol::error::ErrorCode> {
    match result {
        Err(shoal::client::Errors::Server { code, .. }) => Some(*code),
        _ => None,
    }
}

/// Map versions install atomically and clients resync (C4 M3)
///
/// Five rapid restarts of one node while another's control lanes are slowed: every node ends
/// on the newest version with three members, a client through the slowed node only ever moves
/// forward and reaches that version, and a client that connects after the burst is handed the
/// newest map on subscribing rather than any of the ones it missed.
#[tokio::test(flavor = "multi_thread")]
async fn map_versions_install_atomically_and_resync() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .lane_links(true)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    // a client through node 2, which subscribes as it connects
    let slow = Shoal::<TestDbClient>::new(&cluster.node(2).endpoints.client.to_string()).await?;
    let first = slow.topology_changed(0).await?;
    assert!(first >= 1, "the first frame carried version {first}");
    assert_eq!(slow.topology().expect("a frame").members.len(), 3);
    // slow every control lane into node 2 from here on
    for link in cluster.control_links_into(2) {
        link.delay(Duration::from_millis(200));
    }
    // five rapid restarts of node 1, each a new incarnation and at least one new version
    for _ in 0..5 {
        cluster.kill(1)?;
        cluster.restart(1, NodeKind::Server)?;
    }
    cluster.wait_joined(&[1])?;
    // everybody converges on the newest version node 0 knows, with three members and no more
    let newest = cluster.members(0)?["version"].as_u64().expect("a version");
    assert!(newest > first, "five restarts moved the version from {first} to {newest}");
    cluster.wait_map_version(&[0, 1, 2], newest)?;
    for id in 0..3 {
        let map = cluster.node_mut(id).command("MAP")?;
        assert_eq!(map["ok"]["version"], newest, "node {id} holds {}", map["ok"]);
        assert_eq!(map["ok"]["members"].as_object().map(serde_json::Map::len), Some(3), "node {id} holds {}", map["ok"]);
    }
    // the slowed client only ever moves forward, and gets there
    let mut seen = first;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen < newest {
        let reached = tokio::time::timeout(Duration::from_secs(30), slow.topology_changed(seen))
            .await
            .map_err(|_| FixtureError::NotReady(format!("the slowed client stopped at version {seen}")))??;
        assert!(reached > seen, "the topology went backwards from {seen} to {reached}");
        seen = reached;
        assert!(std::time::Instant::now() < deadline, "the slowed client never reached {newest}");
    }
    let frame = slow.topology().expect("a frame");
    assert_eq!(frame.version, newest);
    assert_eq!(frame.members.len(), 3, "a frame with another member count was installed: {frame:?}");
    // a client that connects after the burst is handed the newest map on subscribing
    let late = Shoal::<TestDbClient>::new(&cluster.node(0).endpoints.client.to_string()).await?;
    let version = late.topology_changed(0).await?;
    assert_eq!(version, newest, "a late client was handed an old map");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Table ids and streams are stable across restart (C4 M3)
///
/// The committed tables carry one id per table, distinct and equal to what the schema derives;
/// rows written to the persistent table before every node restarts are read back after, under
/// the same ids, and the ephemeral table's rows are gone, which is what ephemeral means.
#[tokio::test(flavor = "multi_thread")]
async fn table_ids_and_streams_are_stable_across_restart() -> Result<(), FixtureError> {
    use shoal::shared::traits::QuerySupport;
    let mut cluster = Cluster::builder().cluster(3, CoreClaim::Count(1)).start().await?;
    cluster.wait_voters(0, 3)?;
    // the ids the schema derives, and the ids the cluster committed at initialization
    let derived: Vec<(String, u64)> = <TestDbClient as QuerySupport>::table_ids()
        .into_iter()
        .map(|(name, id)| (name.to_string(), id.0))
        .collect();
    assert_eq!(derived.len(), 2);
    assert_ne!(derived[0].1, derived[1].1, "two tables share an id: {derived:?}");
    let committed = |cluster: &mut Cluster, id: usize| -> Result<Vec<(String, u64)>, FixtureError> {
        let map = cluster.node_mut(id).command("MAP")?;
        Ok(map["ok"]["tables"]
            .as_array()
            .expect("tables")
            .iter()
            .map(|pair| (pair[0].as_str().expect("a name").to_string(), pair[1].as_u64().expect("an id")))
            .collect())
    };
    for id in 0..3 {
        assert_eq!(committed(&mut cluster, id)?, derived, "node {id} committed other ids");
    }
    // rows in both tables, through node 0
    let client = Shoal::<TestDbClient>::new(&cluster.node(0).endpoints.client.to_string()).await?;
    for key in 0..10u64 {
        client.send_one(Row { key, data: format!("row-{key}") }).await?;
        client.send_one(Note { key, text: format!("note-{key}") }).await?;
    }
    drop(client);
    // every node restarts
    for id in 0..3 {
        cluster.kill(id)?;
    }
    for id in 0..3 {
        cluster.restart(id, NodeKind::Server)?;
    }
    cluster.wait_joined(&[0, 1, 2])?;
    cluster.wait_leader_among(0, &[0, 1, 2], Duration::from_secs(30))?;
    // the ids did not move
    for id in 0..3 {
        assert_eq!(committed(&mut cluster, id)?, derived, "node {id} came back with other ids");
    }
    // the persistent rows are still there, through another node; the ephemeral ones are not
    let client = Shoal::<TestDbClient>::new(&cluster.node(1).endpoints.client.to_string()).await?;
    for key in 0..10u64 {
        let response = client.send_one(NoteGet::new(vec![key])).await?;
        let notes = response.access::<Note>()?.expect("a note that was not found");
        assert_eq!(notes.first().expect("a note").text.as_str(), format!("note-{key}"));
        assert!(found_nothing(client.send_one(RowGet::new(vec![key])).await), "an ephemeral row survived a restart");
    }
    Ok(())
}

/// A client receives the topology with client endpoints (C9 M3)
///
/// The frame a client is handed names every member's client endpoint, each of which serves a
/// round trip; a fourth node joining moves the frame once it is a member; and a burst of
/// restarts leaves the client on the version the cluster is on.
#[tokio::test(flavor = "multi_thread")]
async fn client_receives_topology_with_client_endpoints() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(4, CoreClaim::Count(1))
        .deferred_from(3)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let client = Shoal::<TestDbClient>::new(&cluster.node(0).endpoints.client.to_string()).await?;
    let version = client.topology_changed(0).await?;
    let frame = client.topology().expect("a frame");
    assert_eq!(frame.version, version);
    assert_eq!(frame.members.len(), 3);
    assert_eq!(frame.placement.len(), 3, "the placement was not initialized: {frame:?}");
    // every member's client endpoint is one of the fixture's, and answers a round trip
    let known: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    for (at, member) in frame.members.iter().enumerate() {
        assert!(known.contains(&member.client), "{} is not a fixture endpoint", member.client);
        round_trip(&member.client, 1_000 + at as u64).await?;
    }
    // a fourth node joining moves the frame, and the frame then names it
    cluster.start_deferred(3)?;
    cluster.wait_joined(&[3])?;
    let fourth = cluster.node_ids()[3].clone();
    let mut seen = version;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        seen = tokio::time::timeout(Duration::from_secs(30), client.topology_changed(seen))
            .await
            .map_err(|_| FixtureError::NotReady(format!("the client never heard of the join past {seen}")))??;
        let frame = client.topology().expect("a frame");
        if frame.members.iter().any(|member| member.node.to_string() == fourth) {
            assert_eq!(frame.members.len(), 4);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the client never saw the fourth member");
    }
    // a burst of restarts of the fourth node, after which the client is where the cluster is
    for _ in 0..3 {
        cluster.kill(3)?;
        cluster.restart(3, NodeKind::Server)?;
    }
    cluster.wait_joined(&[3])?;
    let newest = cluster.members(0)?["version"].as_u64().expect("a version");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen < newest {
        seen = tokio::time::timeout(Duration::from_secs(30), client.topology_changed(seen))
            .await
            .map_err(|_| FixtureError::NotReady(format!("the client stopped at version {seen} of {newest}")))??;
        assert!(std::time::Instant::now() < deadline);
    }
    assert_eq!(client.topology().expect("a frame").version, newest);
    Ok(())
}

/// Readiness distinguishes process, control and data (C9 M3)
///
/// One node at a replication factor of three is up, joined and placed, and says its default
/// writes are short a node: an insert is refused naming the shortfall and a get is served. Two
/// more nodes joining lifts the shortfall before the placement is initialized, while a joiner
/// says it is unplaced and answers a get by saying so; initialization places it. A cluster at
/// a replication factor of one admits writes from the start.
#[tokio::test(flavor = "multi_thread")]
async fn readiness_distinguishes_process_control_and_data() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .deferred_from(1)
        .replication_factor(3)
        .initialize(false)
        .start()
        .await?;
    // one node: process and control ready, placed on itself, short of a write quorum
    let readiness = cluster.node_mut(0).command("READINESS")?;
    let view = &readiness["ok"];
    assert_eq!(view["process"], true, "{view}");
    assert_eq!(view["control"], "joined", "{view}");
    assert_eq!(view["is_leader"], true, "{view}");
    assert_eq!(view["data"]["initialized"], false, "{view}");
    assert_eq!(view["data"]["placed"], true, "{view}");
    assert_eq!(view["data"]["members_up"], 1, "{view}");
    assert_eq!(view["data"]["desired_rf"], 3, "{view}");
    assert_eq!(view["data"]["active_rf"], 1, "{view}");
    assert_eq!(view["data"]["default_writes"], serde_json::json!({ "Err": { "have": 1, "need": 2 } }), "{view}");
    // a write is refused naming the shortfall; a read is served
    let zero = Shoal::<TestDbClient>::new(&cluster.node(0).endpoints.client.to_string()).await?;
    let refused = zero.send_one(Row { key: 1, data: "one".to_string() }).await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::QuorumUnavailable), "{refused:?}");
    let msg = match &refused {
        Err(shoal::client::Errors::Server { msg, .. }) => msg.clone(),
        other => panic!("{other:?}"),
    };
    assert!(msg.contains("have 1") && msg.contains("need 2"), "{msg}");
    assert!(found_nothing(zero.send_one(RowGet::new(vec![1])).await));
    // two more nodes join: the shortfall lifts before the placement is initialized
    cluster.start_deferred(1)?;
    cluster.start_deferred(2)?;
    cluster.wait_joined(&[1, 2])?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let readiness = cluster.node_mut(0).command("READINESS")?;
        if readiness["ok"]["data"]["members_up"] == 3 {
            assert_eq!(readiness["ok"]["data"]["default_writes"], serde_json::json!({ "Ok": null }), "{readiness}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "node 0 never saw three up: {readiness}");
        std::thread::sleep(Duration::from_millis(100));
    }
    // the shards judge writes by the same map, once it reaches them
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let written = zero.send_one(Row { key: 2, data: "two".to_string() }).await;
        if written.is_ok() {
            break;
        }
        assert_eq!(failure_code(&written), Some(ErrorCode::QuorumUnavailable), "{written:?}");
        assert!(std::time::Instant::now() < deadline, "the shards never admitted a write: {written:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // a joiner before initialization is joined and unplaced, and says so to a query
    let readiness = cluster.node_mut(1).command("READINESS")?;
    let view = &readiness["ok"];
    assert_eq!(view["control"], "joined", "{view}");
    assert_eq!(view["data"]["placed"], false, "{view}");
    assert_eq!(view["data"]["initialized"], false, "{view}");
    let one = Shoal::<TestDbClient>::new(&cluster.node(1).endpoints.client.to_string()).await?;
    let unplaced = one.send_one(RowGet::new(vec![2])).await;
    assert_eq!(failure_code(&unplaced), Some(ErrorCode::NotInitialized), "{unplaced:?}");
    assert_eq!(cluster.members(1)?["members"].as_array().map(Vec::len), Some(3));
    // initialization places it
    cluster.initialize(&[0, 1, 2])?;
    let readiness = cluster.node_mut(1).command("READINESS")?;
    assert_eq!(readiness["ok"]["data"]["placed"], true, "{readiness}");
    assert_eq!(readiness["ok"]["data"]["initialized"], true, "{readiness}");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let written = one.send_one(Row { key: 3, data: "three".to_string() }).await;
        if written.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the joiner never served a write: {written:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = one.send_one(RowGet::new(vec![3])).await?;
    assert!(response.access::<Row>()?.is_some_and(|rows| rows.len() == 1));
    // a replication factor of one admits writes from the start
    let mut single = Cluster::builder().cluster(1, CoreClaim::Count(1)).start().await?;
    let readiness = single.node_mut(0).command("READINESS")?;
    assert_eq!(readiness["ok"]["data"]["default_writes"], serde_json::json!({ "Ok": null }), "{readiness}");
    assert_eq!(readiness["ok"]["data"]["desired_rf"], 1, "{readiness}");
    round_trip(&single.node(0).endpoints.client.to_string(), 4).await?;
    Ok(())
}

/// Admin mutations require a principal and operation identity (C9 M3)
///
/// With authentication required and one admin named, an unauthenticated connection is refused
/// before it can ask anything, a user who is not an admin may read but not change, and the
/// admin's change is refused against a stale version, applied against the right one, answered
/// the same way for the same operation id without a second log entry, and refused for a fresh
/// operation once the placement is initialized.
#[tokio::test(flavor = "multi_thread")]
async fn admin_mutations_require_principal_and_operation_identity() -> Result<(), FixtureError> {
    use shoal::server::{AdminKind, AdminRequest};
    use shoal::shared::auth::Credentials;
    use shoal::shared::identity::NodeId;
    use shoal::shared::protocol::admin::AdminOutcome;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .auth("alice", "alpha")
        .auth("bob", "bravo")
        .admins(vec!["alice".to_string()])
        .initialize(false)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addr = cluster.node(0).endpoints.client.to_string();
    // an unauthenticated connection never gets as far as a request
    assert!(Shoal::<TestDbClient>::new(&addr).await.is_err(), "an anonymous client was accepted");
    let bob = Shoal::<TestDbClient>::with_credentials(&addr, Credentials::scram("bob", "bravo")).await?;
    let alice = Shoal::<TestDbClient>::with_credentials(&addr, Credentials::scram("alice", "alpha")).await?;
    let nodes: Vec<NodeId> = cluster.node_ids().iter().map(|id| NodeId(id.parse().expect("a node id"))).collect();
    let version = cluster.members(0)?["version"].as_u64().expect("a version");
    let op = uuid::Uuid::new_v4();
    let initialize = |expected_version: u64, op: uuid::Uuid| AdminRequest {
        op,
        expected_version,
        kind: AdminKind::Initialize { nodes: nodes.clone() },
    };
    // a user who is not an admin may read, but not change
    let read = bob
        .admin(&AdminRequest { op: uuid::Uuid::new_v4(), expected_version: 0, kind: AdminKind::Members })
        .await?;
    assert!(matches!(read.outcome, Ok(AdminOutcome::Read(_))), "{read:?}");
    let refused = bob.admin(&initialize(version, op)).await?;
    assert_eq!(refused.outcome.as_ref().expect_err("bob was allowed").code(), ErrorCode::Unauthorized, "{refused:?}");
    // the admin against a stale version
    let stale = alice.admin(&initialize(version + 7, op)).await?;
    assert_eq!(stale.outcome.as_ref().expect_err("a stale version was applied").code(), ErrorCode::StaleVersion, "{stale:?}");
    // against the right one, retried only if the cluster moved underneath
    let mut applied = None;
    let mut sent_against = version;
    for _ in 0..8 {
        sent_against = cluster.members(0)?["version"].as_u64().expect("a version");
        let answer = alice.admin(&initialize(sent_against, op)).await?;
        match answer.outcome {
            Ok(AdminOutcome::Applied { version }) => {
                applied = Some(version);
                break;
            }
            Err(error) if error.code() == ErrorCode::StaleVersion => std::thread::sleep(Duration::from_millis(100)),
            other => panic!("the initialization was not applied: {other:?}"),
        }
    }
    let applied = applied.expect("the initialization never applied");
    assert!(applied > version);
    cluster.wait_map_version(&[0, 1, 2], applied)?;
    // the very same request again - the retry a client that lost the answer would send - is
    // answered the same way, is not refused as stale, and writes nothing
    std::thread::sleep(Duration::from_millis(500));
    let leader = cluster.leader_index(0)?.expect("a leader");
    let before = cluster.node_mut(leader).command("LOG_LEN")?["ok"]["bytes"].as_u64().expect("a length");
    let repeated = alice.admin(&initialize(sent_against, op)).await?;
    assert_eq!(repeated.outcome, Ok(AdminOutcome::Repeated { version: applied }), "{repeated:?}");
    std::thread::sleep(Duration::from_millis(500));
    let after = cluster.node_mut(leader).command("LOG_LEN")?["ok"]["bytes"].as_u64().expect("a length");
    assert_eq!(before, after, "a repeated operation wrote to the log");
    // a fresh operation is refused, since the placement is initialized
    let fresh = alice.admin(&initialize(applied, uuid::Uuid::new_v4())).await?;
    let error = fresh.outcome.as_ref().expect_err("a second initialization was applied");
    assert!(error.msg.contains("initialized"), "{fresh:?}");
    Ok(())
}

/// Fresh failure reports do not mask shard failure (C3 M3)
///
/// A member whose shard died keeps reporting, so the cluster holds it up with that shard marked
/// failed rather than calling it down; replayed reports are counted and change nothing; a member
/// that stops reporting is called down by the leader within a bounded time, and a quorum write
/// is still admitted with the two that remain; and it is called up again once it reports.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_failure_reports_do_not_mask_shard_failure() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(2))
        .detector_interval_ms(100)
        .replication_factor(3)
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let leader = cluster.leader_index(0)?.expect("a leader");
    let victim = (leader + 1) % 3;
    let other = (leader + 2) % 3;
    let victim_id = cluster.node_ids()[victim].clone();
    // what one node's map says about the victim
    let member_of = |cluster: &mut Cluster, at: usize| -> Result<serde_json::Value, FixtureError> {
        let map = cluster.node_mut(at).command("MAP")?;
        Ok(map["ok"]["members"][&victim_id].clone())
    };
    // wait until every node's map says something about the victim
    let wait_for = |cluster: &mut Cluster, what: &str, within: Duration, check: &dyn Fn(&serde_json::Value) -> bool| -> Result<(), FixtureError> {
        let deadline = std::time::Instant::now() + within;
        loop {
            let views: Vec<serde_json::Value> = (0..3).map(|at| member_of(cluster, at)).collect::<Result<_, _>>()?;
            if views.iter().all(|view| check(view)) {
                return Ok(());
            }
            if std::time::Instant::now() > deadline {
                return Err(FixtureError::NotReady(format!("{what} never happened: {views:?}")));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    // a shard dies on the victim: every node hears which, and the victim stays up
    let failed = cluster.node_mut(victim).command("FAIL_SHARD 1")?;
    assert!(failed.get("ok").is_some(), "{failed}");
    wait_for(&mut cluster, "the shard failure", Duration::from_secs(5), &|view| {
        view["shards_failed"] == serde_json::json!([1]) && view["health"] == "up"
    })?;
    let readiness = cluster.node_mut(victim).command("READINESS")?;
    assert_eq!(readiness["ok"]["data"]["shards_failed"], serde_json::json!([1]), "{readiness}");
    assert_eq!(readiness["ok"]["control"], "joined", "{readiness}");
    let ping = cluster.node_mut(leader).command(&format!("PING {victim}"))?;
    assert!(ping.get("ok").is_some(), "the victim stopped answering pings: {ping}");
    // replayed reports are counted and change nothing
    let version_before = cluster.members(leader)?["version"].as_u64().expect("a version");
    for _ in 0..3 {
        let sent = cluster.node_mut(victim).command("STALE_REPORT")?;
        assert!(sent.get("ok").is_some(), "{sent}");
    }
    let detector_of = |cluster: &mut Cluster| -> Result<serde_json::Value, FixtureError> {
        let request = serde_json::json!({ "op": uuid::Uuid::new_v4(), "expected_version": 0, "kind": "Detector" });
        let reply = cluster.node_mut(leader).command(&format!("ADMIN {request}"))?;
        Ok(reply["ok"]["outcome"]["Ok"]["Read"].clone())
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let detector = detector_of(&mut cluster)?;
        if detector["stale_ignored"].as_u64().unwrap_or(0) >= 3 {
            assert!(detector["is_leader"] == true, "{detector}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the leader never counted the stale reports: {detector}");
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(cluster.members(leader)?["version"].as_u64(), Some(version_before), "a stale report moved the topology");
    assert_eq!(member_of(&mut cluster, leader)?["health"], "up");
    // the victim falls silent: the leader calls it down, and a quorum write is still admitted
    let interval = Duration::from_millis(100);
    cluster.node(victim).pause().map_err(|error| FixtureError::ChildFailed(format!("pausing: {error}")))?;
    let deadline = std::time::Instant::now() + interval * 100;
    loop {
        let at_leader = member_of(&mut cluster, leader)?;
        let at_other = member_of(&mut cluster, other)?;
        if at_leader["health"] == "down" && at_other["health"] == "down" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the silent member was never called down: {at_leader} {at_other}");
        std::thread::sleep(Duration::from_millis(50));
    }
    let readiness = cluster.node_mut(leader).command("READINESS")?;
    assert_eq!(readiness["ok"]["data"]["members_up"], 2, "{readiness}");
    assert_eq!(readiness["ok"]["data"]["default_writes"], serde_json::json!({ "Ok": null }), "{readiness}");
    let client = Shoal::<TestDbClient>::new(&cluster.node(leader).endpoints.client.to_string()).await?;
    client.send_one(Row { key: 7, data: "seven".to_string() }).await?;
    // and back: its next fresh report brings it up again
    cluster.node(victim).resume().map_err(|error| FixtureError::ChildFailed(format!("resuming: {error}")))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let at_leader = member_of(&mut cluster, leader)?;
        let at_other = member_of(&mut cluster, other)?;
        if at_leader["health"] == "up" && at_other["health"] == "up" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the member was never called up again: {at_leader} {at_other}");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(member_of(&mut cluster, leader)?["shards_failed"], serde_json::json!([1]));
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}
