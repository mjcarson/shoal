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

use cluster::schema::{Note, NoteDelete, NoteGet, Row, RowGet, TestDb, TestDbClient};
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
    // the write was never acknowledged, and a read that overtakes an unacknowledged write is
    // not promised to see it: on a cluster node the write is committed by its tablet group off
    // the shard's loop, so the read is asked again until the row is there, within a bound
    // ([F40](../../docs/src/features/replication.md))
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let answered = tokio::time::timeout(Duration::from_secs(10), client.send_one(RowGet::new(vec![7]))).await;
        assert!(answered.is_ok(), "a query to a resumed server never came back");
        // a get that found nothing is a query that did not succeed, and is asked again
        match answered.unwrap() {
            Ok(response) if response.access::<Row>()?.is_some() => break,
            Ok(_) | Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the write sent during the pause was lost, rather than delayed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
            // the groups' timers, shortened so a failover fits a test
            // ([F40](../../docs/src/features/replication.md))
            if let Some(failover) = staged.failover_ms {
                block = block.primary_failover_after(Duration::from_millis(failover));
            }
            let mut replication = shoal::server::conf::cluster::Replication::default();
            if let Some(timeout) = staged.write_timeout_ms {
                replication.write_timeout = Duration::from_millis(timeout).into();
            }
            if let Some(bytes) = staged.pending_bytes {
                replication.pending_bytes = bytes;
            }
            // the checkpoint, retention and segment knobs, shortened so a member falls past
            // the purge point inside a test ([F43](../../docs/src/features/node-recovery.md))
            if let Some(entries) = staged.checkpoint_entries {
                replication.checkpoint_entries = entries;
            }
            if let Some(entries) = staged.retained_entries {
                replication.retained_entries = entries;
            }
            if let Some(bytes) = staged.segment_bytes {
                replication.segment_bytes = bytes;
            }
            if let Some(bytes) = staged.retained_bytes {
                replication.retained_bytes = bytes;
            }
            block = block.replication(replication);
            // the default read level, which every bundle without an override inherits
            // ([F41](../../docs/src/features/read-consistency.md))
            if let Some(level) = &staged.read_consistency {
                let level = match level.as_str() {
                    "quorum" => shoal::server::conf::cluster::Consistency::Quorum,
                    "all" => shoal::server::conf::cluster::Consistency::All,
                    _ => shoal::server::conf::cluster::Consistency::One,
                };
                block = block.read_consistency(level);
            }
            // and the bundle deadline, on the networking block every node has
            if let Some(ms) = staged.query_deadline_ms {
                conf.networking.query_deadline = Duration::from_millis(ms).into();
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
    // a table's durability, which a cluster node refuses `async` for and a standalone one serves
    if let Some(durability) = &request.durability {
        let durability = match durability.as_str() {
            "async" => shoal::storage::fs::conf::Durability::Async,
            _ => shoal::storage::fs::conf::Durability::Fsync,
        };
        conf.storage.default.filesystem.latency_sensitive.durability = durability;
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
        // set or clear one table's read level ([F41](../../docs/src/features/read-consistency.md))
        "SET_TABLE_READ_POLICY" => match (parts.next(), parts.next()) {
            (Some(table), Some(level)) => admin(AdminKind::SetTableReadPolicy {
                table: table.to_string(),
                level: (level != "clear").then(|| level.to_string()),
            }),
            _ => Err("SET_TABLE_READ_POLICY needs a table and one, quorum or clear".to_string()),
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
        // the hashed applied state of a table, with every group's applied index, over every
        // shard ([F40](../../docs/src/features/replication.md))
        "DIGEST" => match parts.next() {
            Some(table) => {
                let table = shoal::shared::identity::TableId::of(table);
                pool.replication_verb(shoal::server::replication::ReplicationVerb::Digest { table })
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|answers| {
                        // fold every shard's digest into one, in shard order
                        let mut rows = 0u64;
                        let mut hash = 0u64;
                        let mut groups = serde_json::Map::new();
                        for answer in answers {
                            let value = answer?;
                            rows += value["rows"].as_u64().unwrap_or(0);
                            let shard_hash = value["hash"].as_u64().unwrap_or(0);
                            let mut fold = Vec::with_capacity(16);
                            fold.extend_from_slice(&hash.to_le_bytes());
                            fold.extend_from_slice(&shard_hash.to_le_bytes());
                            hash = shoal::gxhash::gxhash64(&fold, 0);
                            if let Some(found) = value["groups"].as_object() {
                                for (group, applied) in found {
                                    groups.insert(group.clone(), applied.clone());
                                }
                            }
                        }
                        Ok(serde_json::json!({ "rows": rows, "hash": hash, "groups": groups }))
                    })
            }
            None => Err("DIGEST needs a table name".to_string()),
        },
        // what every shard's tablet groups look like
        "GROUPS" => pool
            .replication()
            .map(|view| serde_json::to_value(view).expect("a view serializes"))
            .map_err(|error| format!("{error:?}")),
        // force every shard's WAL into a new segment
        "ROTATE" => pool
            .replication_verb(shoal::server::replication::ReplicationVerb::Rotate)
            .map_err(|error| format!("{error:?}"))
            .and_then(|answers| answers.into_iter().collect::<Result<Vec<_>, _>>().map(serde_json::Value::Array)),
        // hand every resolved segment to the compactors now
        "COMPACT" => pool
            .replication_verb(shoal::server::replication::ReplicationVerb::Compact)
            .map_err(|error| format!("{error:?}"))
            .and_then(|answers| answers.into_iter().collect::<Result<Vec<_>, _>>().map(serde_json::Value::Array)),
        // hold back a group's flush completions, or release them
        "STALL_WAL" | "RELEASE_WAL" => match parts.next().and_then(|hex| u64::from_str_radix(hex, 16).ok()) {
            Some(group) => {
                let group = shoal::shared::identity::GroupId(group);
                let verb = if verb == "STALL_WAL" {
                    shoal::server::replication::ReplicationVerb::Stall { group }
                } else {
                    shoal::server::replication::ReplicationVerb::Release { group }
                };
                pool.replication_verb(verb)
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|answers| answers.into_iter().collect::<Result<Vec<_>, _>>().map(serde_json::Value::Array))
            }
            None => Err(format!("{verb} needs a group id in hex")),
        },
        // hold one shard's shares for a while, sending each twice on release if asked
        // ([F41](../../docs/src/features/read-consistency.md))
        "HOLD_SHARES" => {
            let shard = parts.next().and_then(|idx| idx.parse::<usize>().ok());
            let ms = parts.next().and_then(|ms| ms.parse::<u64>().ok());
            let dup = parts.next() == Some("dup");
            match (shard, ms) {
                (Some(shard), Some(ms)) => pool
                    .read_verb(Some(shard), shoal::server::replication::ReadVerb::HoldShares { ms, dup })
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|answers| answers.into_iter().next().unwrap_or_else(|| Err("no shard answered".to_string()))),
                _ => Err("HOLD_SHARES needs a shard index and a hold in milliseconds".to_string()),
            }
        }
        // the resident gathers and the read counters, folded over every shard
        "GATHERS" => pool
            .read_verb(None, shoal::server::replication::ReadVerb::Gathers)
            .map_err(|error| format!("{error:?}"))
            .and_then(|answers| {
                let mut resident = 0u64;
                let mut held = 0u64;
                let mut stats = shoal::server::replication::ReadStats::default();
                for answer in answers {
                    let view = answer?;
                    resident += view["resident"].as_u64().unwrap_or(0);
                    held += view["held"].as_u64().unwrap_or(0);
                    if let Ok(shard) = serde_json::from_value::<shoal::server::replication::ReadStats>(view["stats"].clone()) {
                        stats.absorb(&shard);
                    }
                }
                Ok(serde_json::json!({
                    "resident": resident,
                    "held": held,
                    "stats": serde_json::to_value(stats).expect("stats serialize"),
                }))
            }),
        // block one shard's executor for a while, so every group on it falls silent while the
        // control thread keeps reporting ([F42](../../docs/src/features/primary-failover.md))
        "STALL_SHARD" => {
            let shard = parts.next().and_then(|idx| idx.parse::<usize>().ok());
            let ms = parts.next().and_then(|ms| ms.parse::<u64>().ok());
            match (shard, ms) {
                (Some(shard), Some(ms)) => pool
                    .read_verb(Some(shard), shoal::server::replication::ReadVerb::StallShard { ms })
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|answers| answers.into_iter().next().unwrap_or_else(|| Err("no shard answered".to_string()))),
                _ => Err("STALL_SHARD needs a shard index and a stall in milliseconds".to_string()),
            }
        }
        // cut a snapshot of a group now, on the shard hosting it, and report its manifest
        // ([F43](../../docs/src/features/node-recovery.md))
        "SNAPSHOT" => match parts.next().and_then(|hex| u64::from_str_radix(hex, 16).ok()) {
            Some(group) => {
                let group = shoal::shared::identity::GroupId(group);
                pool.replication_verb(shoal::server::replication::ReplicationVerb::Snapshot { group })
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|answers| {
                        // the shard that hosts the group answers a manifest; the rest refuse by name
                        answers
                            .into_iter()
                            .find_map(Result::ok)
                            .ok_or_else(|| format!("no shard cut a snapshot of group {group}"))
                    })
            }
            None => Err("SNAPSHOT needs a group id in hex".to_string()),
        },
        // drop the next committed write replies every shard of this node would send
        "DROP_REPLIES" => match parts.next().and_then(|n| n.parse::<u64>().ok()) {
            Some(n) => pool
                .replication_verb(shoal::server::replication::ReplicationVerb::DropReplies { n })
                .map_err(|error| format!("{error:?}"))
                .and_then(|answers| answers.into_iter().collect::<Result<Vec<_>, _>>().map(serde_json::Value::Array)),
            None => Err("DROP_REPLIES needs a count".to_string()),
        },
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
    // the write's tablet group may have been led by the paused member, in which case the two
    // members left elect another within the failover base and a retry lands; a write proposed
    // in the middle of that is refused by name rather than lost
    // ([F40](../../docs/src/features/replication.md))
    let client = Shoal::<TestDbClient>::new(&cluster.node(leader).endpoints.client.to_string()).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let written = client.send_one(Row { key: 7, data: "seven".to_string() }).await;
        if written.is_ok() {
            break;
        }
        let code = failure_code(&written);
        assert!(
            matches!(code, Some(shoal::shared::protocol::error::ErrorCode::NotLeader | shoal::shared::protocol::error::ErrorCode::OutcomeUnknown)),
            "a write with two members up was refused for another reason: {written:?}"
        );
        assert!(std::time::Instant::now() < deadline, "the write never landed: {written:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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

// ---------------------------------------------------------------------------------------------
// M4: replication and quorum writes (F40)
// ---------------------------------------------------------------------------------------------

/// The tablet a row key of either fixture table belongs to
///
/// Both tables hash one `u64` into their partition key, so the tablet is the ring's cut of that
/// hash and not of the key itself.
///
/// # Arguments
///
/// * `key` - The row key
fn tablet_of(key: u64) -> usize {
    use shoal::shared::traits::PartitionKeySupport as _;
    shoal::server::ring::Ring::tablet_of(Note::get_partition_key_from_values(&key))
}

/// The `GROUPS` view of one node
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
fn groups_of(cluster: &mut Cluster, node: usize) -> Result<serde_json::Value, FixtureError> {
    Ok(cluster.node_mut(node).command("GROUPS")?["ok"].clone())
}

/// The group serving a key of a table, and which node leads it, as one node sees it
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask
/// * `table` - The table's name
/// * `key` - The partition key
fn group_of(
    cluster: &mut Cluster,
    node: usize,
    table: &str,
    key: u64,
) -> Result<(String, Option<usize>), FixtureError> {
    let tablet = tablet_of(key) as u64;
    let view = groups_of(cluster, node)?;
    let ids = cluster.node_ids();
    for shard in view["shards"].as_array().into_iter().flatten() {
        for group in shard["groups"].as_array().into_iter().flatten() {
            if group["table_name"] != table {
                continue;
            }
            let serves = group["tablet_ids"]
                .as_array()
                .is_some_and(|tablets| tablets.iter().any(|t| t.as_u64() == Some(tablet)));
            if serves {
                let leader = group["leader"]["node"]
                    .as_str()
                    .and_then(|leader| ids.iter().position(|id| id == leader));
                // the report carries the group id as a number; the verbs take it as hex
                let id = group["group"].as_u64().unwrap_or_default();
                return Ok((format!("{id:016x}"), leader));
            }
        }
    }
    Err(FixtureError::NotReady(format!("node {node} hosts no group for key {key} of {table}: {view}")))
}

/// Wait until a key's group has a leader, as node zero sees it, and say which node it is
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `table` - The table's name
/// * `key` - The partition key
fn wait_group_leader(cluster: &mut Cluster, table: &str, key: u64) -> Result<(String, usize), FixtureError> {
    wait_group_leader_via(cluster, 0, table, key)
}

/// Wait until a key's group has a leader, as one node sees it, and say which node it is
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask, which a test that killed node zero cannot leave at zero
/// * `table` - The table's name
/// * `key` - The partition key
fn wait_group_leader_via(cluster: &mut Cluster, via: usize, table: &str, key: u64) -> Result<(String, usize), FixtureError> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (group, leader) = group_of(cluster, via, table, key)?;
        if let Some(leader) = leader {
            return Ok((group, leader));
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("group {group} never elected a leader")));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until a key's group is led by somebody other than a given node, as one node sees it
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `table` - The table's name
/// * `key` - The partition key
/// * `not` - The node that must not lead: the one that was killed, paused, stalled or cut off
fn wait_group_leader_change(
    cluster: &mut Cluster,
    via: usize,
    table: &str,
    key: u64,
    not: usize,
) -> Result<(String, usize), FixtureError> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (group, leader) = group_of(cluster, via, table, key)?;
        if let Some(leader) = leader {
            if leader != not {
                return Ok((group, leader));
            }
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("group {group} was never led by anybody but node {not}")));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The health one node's map gives a member
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `at` - The node whose map to read
/// * `member` - The member
fn health_of(cluster: &mut Cluster, at: usize, member: usize) -> Result<String, FixtureError> {
    let id = cluster.node_ids()[member].clone();
    let map = cluster.node_mut(at).command("MAP")?;
    Ok(map["ok"]["members"][&id]["health"].as_str().unwrap_or_default().to_string())
}

/// Write one note through a node, trying again by name while its group is between leaders
///
/// A write proposed in the middle of an election is refused `NotLeader`, one proposed to a
/// leader that lost its quorum is `OutcomeUnknown` at its deadline, and one the coordinator
/// cannot admit is `QuorumUnavailable`; a test that just killed, paused or cut off a leader
/// tolerates those and nothing else until the write lands.
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `text` - The text
/// * `within` - How long to keep trying
async fn write_note_eventually(addr: &str, key: u64, text: &str, within: Duration) -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let deadline = std::time::Instant::now() + within;
    loop {
        let written = write_note(addr, key, text).await;
        if written.is_ok() {
            return Ok(());
        }
        let code = failure_code(&written);
        assert!(
            matches!(
                code,
                Some(ErrorCode::NotLeader | ErrorCode::OutcomeUnknown | ErrorCode::QuorumUnavailable | ErrorCode::Unavailable | ErrorCode::Timeout)
            ),
            "a write through {addr} was refused for another reason: {written:?}"
        );
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("note {key} never landed through {addr}: {written:?}")));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The `DIGEST` of a table on one node
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `table` - The table's name
fn digest_of(cluster: &mut Cluster, node: usize, table: &str) -> Result<serde_json::Value, FixtureError> {
    Ok(cluster.node_mut(node).command(&format!("DIGEST {table}"))?["ok"].clone())
}

/// Wait until every named node's digest of a table agrees with the first's
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `nodes` - The nodes
/// * `table` - The table's name
/// * `within` - How long to wait
fn wait_digests_equal(
    cluster: &mut Cluster,
    nodes: &[usize],
    table: &str,
    within: Duration,
) -> Result<serde_json::Value, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let digests: Vec<serde_json::Value> = nodes
            .iter()
            .map(|node| digest_of(cluster, *node, table))
            .collect::<Result<_, _>>()?;
        // the hash and the row count are the applied state; a group's applied index can lag
        // on a node whose leader is cut off, so it is the caller's to compare where it matters
        if digests.iter().all(|digest| digest["hash"] == digests[0]["hash"] && digest["rows"] == digests[0]["rows"]) {
            return Ok(digests[0].clone());
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("the digests of {table} never agreed: {digests:?}")));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Read one note through a node's client endpoint
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
async fn read_note(addr: &str, key: u64) -> Result<Option<String>, shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    match client.send_one(NoteGet::new(vec![key])).await {
        Ok(response) => Ok(response
            .access::<Note>()?
            .and_then(|notes| notes.into_iter().next())
            .map(|note| note.text.to_string())),
        Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Write one note through a node's client endpoint
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `text` - The text
async fn write_note(addr: &str, key: u64, text: &str) -> Result<(), shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    client
        .send_one(Note {
            key,
            text: text.to_string(),
        })
        .await?;
    Ok(())
}

/// Write many notes through a node in one bundle, each answered
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `keys` - The keys
/// * `text` - The text every note gets
async fn write_notes_batch(addr: &str, keys: &[u64], text: &str) -> Result<(), FixtureError> {
    use shoal::client::QuerySuceededOpts;
    let client = Shoal::<TestDbClient>::new(addr).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    let mut queries = client.query();
    for key in keys {
        queries = queries.add(Note {
            key: *key,
            text: text.to_string(),
        });
    }
    let mut stream = client.send(queries).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    let mut answered = 0usize;
    while let Some(response) = stream.next().await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))? {
        response
            .suceeded(QuerySuceededOpts::default())
            .map_err(|error| FixtureError::NotReady(format!("a note in the batch was refused: {error:?}")))?;
        answered += 1;
    }
    if answered != keys.len() {
        return Err(FixtureError::NotReady(format!("{answered} of {} notes were answered", keys.len())));
    }
    Ok(())
}

/// The sealed segments a node's shards are still compacting, ascending
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
fn compacting_of(cluster: &mut Cluster, node: usize) -> Result<Vec<u64>, FixtureError> {
    let view = groups_of(cluster, node)?;
    let mut generations: Vec<u64> = view["shards"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|shard| shard["compacting"].as_array().into_iter().flatten())
        .filter_map(serde_json::Value::as_u64)
        .collect();
    generations.sort_unstable();
    Ok(generations)
}

/// The records of a snapshot file: each partition key with its archived bytes
///
/// Parsed with the file's own layout rather than the engine's reader, since the fixture runs
/// on tokio and the partition types are not public
/// ([F43](../../docs/src/features/node-recovery.md)).
///
/// # Arguments
///
/// * `path` - The file
fn snapshot_records(path: &std::path::Path) -> Vec<(u64, Vec<u8>)> {
    let bytes = std::fs::read(path).expect("the snapshot file reads");
    assert_eq!(&bytes[..8], b"SHOALSNP", "not a snapshot file");
    let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"));
    let count = word(33);
    let mut at = 41usize;
    let mut records = Vec::new();
    for _ in 0..count {
        let key = word(at);
        let len = u32::from_le_bytes(bytes[at + 8..at + 12].try_into().expect("four bytes")) as usize;
        records.push((key, bytes[at + 12..at + 12 + len].to_vec()));
        at += 12 + len;
    }
    records
}

/// Wait until a note reads back with a text through a node, or say it never did
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `expected` - The text, or none for absent
/// * `within` - How long to wait
async fn wait_note(addr: &str, key: u64, expected: Option<&str>, within: Duration) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let found = read_note(addr, key).await?;
        if found.as_deref() == expected {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!(
                "note {key} through {addr} is {found:?}, not {expected:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Read one note through a node, saying how the read is served
///
/// A note the server could not serve is an error; one that is not there is `None`
/// ([F41](../../docs/src/features/read-consistency.md)).
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `options` - How the read is served
async fn read_note_with(
    addr: &str,
    key: u64,
    options: &shoal::client::SendOptions,
) -> Result<Option<String>, shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    match client.send_one_with(NoteGet::new(vec![key]), options).await {
        Ok(response) => Ok(response
            .access::<Note>()?
            .and_then(|notes| notes.into_iter().next())
            .map(|note| note.text.to_string())),
        Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Write one note through a node and keep the session token its answer carried
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `text` - The text
async fn write_note_token(
    addr: &str,
    key: u64,
    text: &str,
) -> Result<Option<shoal::shared::protocol::read::SessionToken>, shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    let response = client
        .send_one(Note {
            key,
            text: text.to_string(),
        })
        .await?;
    Ok(response.session_token())
}

/// Write one note through a node as the options say, keeping the answer
///
/// The options are where a write's identity and its retry budget go
/// ([F42](../../docs/src/features/primary-failover.md)).
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `text` - The text
/// * `options` - How the write is sent
async fn write_note_as(
    addr: &str,
    key: u64,
    text: &str,
    options: &shoal::client::SendOptions,
) -> Result<shoal::ShoalResponse<TestDbClient>, shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    client
        .send_one_with(
            Note {
                key,
                text: text.to_string(),
            },
            options,
        )
        .await
}

/// Delete one note through a node as the options say, keeping the answer
///
/// A delete of a note that is not there is answered as a query that did not succeed, which
/// is the original result a retry of a delete that did succeed must never be given.
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `options` - How the delete is sent
async fn delete_note_as(
    addr: &str,
    key: u64,
    options: &shoal::client::SendOptions,
) -> Result<shoal::ShoalResponse<TestDbClient>, shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    client.send_one_with(NoteDelete::new(key), options).await
}

/// Rotate and compact every named node until a group's checkpoint on each is at or past an index
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `nodes` - The nodes
/// * `group` - The group, as `group_of` names it
/// * `index` - The index the checkpoint has to reach
/// * `within` - How long to keep trying
fn wait_checkpoint_past(
    cluster: &mut Cluster,
    nodes: &[usize],
    group: &str,
    index: u64,
    within: Duration,
) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let mut behind = Vec::new();
        for node in nodes {
            let _ = cluster.node_mut(*node).command("ROTATE")?;
            let _ = cluster.node_mut(*node).command("COMPACT")?;
            let view = groups_of(cluster, *node)?;
            let mut checkpoint = None;
            for shard in view["shards"].as_array().into_iter().flatten() {
                for found in shard["groups"].as_array().into_iter().flatten() {
                    let id = found["group"].as_u64().unwrap_or_default();
                    if format!("{id:016x}") == group {
                        checkpoint = found["checkpoint"].as_u64();
                    }
                }
            }
            if checkpoint.is_none_or(|checkpoint| checkpoint < index) {
                behind.push((*node, checkpoint));
            }
        }
        if behind.is_empty() {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("group {group}'s checkpoint never reached {index}: {behind:?}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Delete one note through a node
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
async fn delete_note(addr: &str, key: u64) -> Result<(), shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    client.send_one(NoteDelete::new(key)).await?;
    Ok(())
}

/// Read several notes through a node in one get, in the order the server returned them
///
/// A get that found nothing is an empty list; one the server could not serve is an error.
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `keys` - The keys, in the order the get names them
/// * `limit` - The most rows to ask for, if any
/// * `options` - How the read is served
async fn read_notes(
    addr: &str,
    keys: &[u64],
    limit: Option<usize>,
    options: &shoal::client::SendOptions,
) -> Result<Vec<(u64, String)>, shoal::client::Errors> {
    let client = Shoal::<TestDbClient>::new(addr).await?;
    let mut get = NoteGet::new(keys.to_vec());
    if let Some(limit) = limit {
        get = get.limit(limit);
    }
    match client.send_one_with(get, options).await {
        Ok(response) => Ok(response
            .access::<Note>()?
            .map(|notes| notes.into_iter().map(|note| (note.key.to_native(), note.text.to_string())).collect())
            .unwrap_or_default()),
        Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

/// Read a row and a note in one bundle through a node, and say how each half was answered
///
/// The two halves are independent: one may fail while the other succeeds, which is what a
/// mixed bundle promises ([C6](../../docs/src/distributed/reads.md)).
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `row_key` - The row's key
/// * `note_key` - The note's key
/// * `options` - How the bundle's reads are served
async fn read_mixed(
    addr: &str,
    row_key: u64,
    note_key: u64,
    options: &shoal::client::SendOptions,
) -> Result<(Result<Option<String>, shoal::client::Errors>, Result<Option<String>, shoal::client::Errors>), shoal::client::Errors> {
    use shoal::client::QuerySuceededOpts;
    let client = Shoal::<TestDbClient>::new(addr).await?;
    let queries = client.query().add(RowGet::new(vec![row_key])).add(NoteGet::new(vec![note_key]));
    let mut stream = client.send_with(queries, options).await?;
    // the row's half, at index zero
    let row = match stream.next().await? {
        Some(response) => match response.suceeded(QuerySuceededOpts::default()) {
            Ok(()) => Ok(response
                .access::<Row>()?
                .and_then(|rows| rows.into_iter().next())
                .map(|row| row.data.to_string())),
            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Ok(None),
            Err(error) => Err(error),
        },
        None => Err(shoal::client::Errors::StreamAlreadyTerminated),
    };
    // the note's half, at index one; a failure at either index is that index's alone
    let note = match stream.next().await {
        Ok(Some(response)) => match response.suceeded(QuerySuceededOpts::default()) {
            Ok(()) => Ok(response
                .access::<Note>()?
                .and_then(|notes| notes.into_iter().next())
                .map(|note| note.text.to_string())),
            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Ok(None),
            Err(error) => Err(error),
        },
        Ok(None) => Err(shoal::client::Errors::StreamAlreadyTerminated),
        Err(error) => Err(error),
    };
    Ok((row, note))
}

/// Some note keys placed on each node, under the placement the map names
///
/// Under a factor of one a node hosts only its own tablets, so a key is placed by the rule
/// the ring routes with - the tablet of its hashed partition key to `placement[t % N]` -
/// rather than by asking a node which group serves it
/// ([C4](../../docs/src/distributed/tablet-map.md)).
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `per_node` - How many keys to find on each node
/// * `from` - The first key to try
fn keys_placed_per_node(cluster: &mut Cluster, per_node: usize, from: u64) -> Result<Vec<Vec<u64>>, FixtureError> {
    let map = cluster.node_mut(0).command("MAP")?;
    let placement: Vec<String> = map["ok"]["placement"]
        .as_array()
        .expect("a placement")
        .iter()
        .map(|node| node.as_str().expect("a node id").to_string())
        .collect();
    // where each fixture node sits in the placement
    let positions: Vec<usize> = (0..cluster.pids().len())
        .map(|id| {
            let node = cluster.node(id).endpoints.node.clone().expect("a node id");
            placement.iter().position(|placed| *placed == node).expect("a placed node")
        })
        .collect();
    let mut keys: Vec<Vec<u64>> = vec![Vec::new(); positions.len()];
    for key in from..from + 100_000 {
        if keys.iter().all(|found| found.len() >= per_node) {
            break;
        }
        // the ring hashes the partition key before it picks a tablet, as the table does
        let hashed = <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
        let slot = shoal::server::ring::Ring::tablet_of(hashed) % placement.len();
        if let Some(node) = positions.iter().position(|position| *position == slot) {
            if keys[node].len() < per_node {
                keys[node].push(key);
            }
        }
    }
    Ok(keys)
}

/// The read counters a node's shards report, folded
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
fn read_stats(cluster: &mut Cluster, node: usize) -> Result<serde_json::Value, FixtureError> {
    let view = cluster.node_mut(node).command("GATHERS")?;
    Ok(view["ok"].clone())
}

/// Some keys served by one group, found by trying keys in turn
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `table` - The table's name
/// * `group` - The group, as `group_of` names it
/// * `from` - The first key to try
/// * `count` - How many keys to find
fn keys_in_group(cluster: &mut Cluster, table: &str, group: &str, from: u64, count: usize) -> Result<Vec<u64>, FixtureError> {
    let mut keys = Vec::with_capacity(count);
    for key in from..from + 4096 {
        if keys.len() == count {
            break;
        }
        let (candidate, _) = group_of(cluster, 0, table, key)?;
        if candidate == group {
            keys.push(key);
        }
    }
    if keys.len() < count {
        return Err(FixtureError::NotReady(format!("fewer than {count} keys from {from} are served by group {group}")));
    }
    Ok(keys)
}

/// Some keys whose groups are led by a given node, found by trying keys in turn
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `table` - The table's name
/// * `leader` - The node that has to lead
/// * `from` - The first key to try
/// * `count` - How many keys to find
fn keys_led_by(cluster: &mut Cluster, table: &str, leader: usize, from: u64, count: usize) -> Result<Vec<u64>, FixtureError> {
    let mut keys = Vec::with_capacity(count);
    let mut next = from;
    while keys.len() < count {
        let (key, _) = key_led_by(cluster, table, leader, next)?;
        keys.push(key);
        next = key + 1;
    }
    Ok(keys)
}

/// A key whose group is led by a given node, found by trying keys in turn
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `table` - The table's name
/// * `leader` - The node that has to lead
/// * `from` - The first key to try
fn key_led_by(cluster: &mut Cluster, table: &str, leader: usize, from: u64) -> Result<(u64, String), FixtureError> {
    for key in from..from + 256 {
        let (group, led) = wait_group_leader(cluster, table, key)?;
        if led == leader {
            return Ok((key, group));
        }
    }
    Err(FixtureError::NotReady(format!("no key from {from} is led by node {leader}")))
}

/// A quorum success needs distinct durable voters, and nothing releases it early (C5 M4)
///
/// Three nodes at a factor of three, every lane through a proxy. The replication lane from the
/// group's leader into both followers is cut, and a write through the leader gets no success
/// within its deadline: its outcome is unknown, and rotating and flushing the leader's WAL three
/// times releases nothing, since the leader's own durable copy is one voter and not two. Healing
/// one follower is the second voter: the write commits, reads back on the leader and on that
/// follower, and the two agree on the table's digest while the cut follower does not
/// ([P3](../../docs/src/distributed/protocol.md)).
#[tokio::test(flavor = "multi_thread")]
async fn quorum_success_requires_distinct_durable_voters() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_millis(500))
        .start()
        .await?;
    // a key, its group and the node leading it
    let (key, _group) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let leader = 0;
    let followers = [1usize, 2];
    let addr = cluster.node(leader).endpoints.client.to_string();
    // a write before the cut lands, proving the path
    write_note(&addr, key, "before").await?;
    // cut the replication lane from the leader into both followers, both ways
    for follower in followers {
        cluster.data_link(leader, follower).cut();
        cluster.data_link(follower, leader).cut();
    }
    // a write through the leader gets no success: the outcome is unknown
    let refused = write_note(&addr, key, "during").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::OutcomeUnknown), "{refused:?}");
    // the leader's own durability releases nothing, however often it rotates and flushes
    for _ in 0..3 {
        let _ = cluster.node_mut(leader).command("ROTATE")?;
        let _ = cluster.node_mut(leader).command("FLUSH")?;
    }
    assert_eq!(read_note(&addr, key).await?, Some("before".to_string()), "an uncommitted write was read");
    // one follower back: two distinct durable voters, and the write commits
    cluster.data_link(leader, followers[0]).heal();
    cluster.data_link(followers[0], leader).heal();
    wait_note(&addr, key, Some("during"), Duration::from_secs(20)).await?;
    let follower_addr = cluster.node(followers[0]).endpoints.client.to_string();
    wait_note(&follower_addr, key, Some("during"), Duration::from_secs(20)).await?;
    // the two members that have it agree; the cut one is behind
    let agreed = wait_digests_equal(&mut cluster, &[leader, followers[0]], "Note", Duration::from_secs(20))?;
    let behind = digest_of(&mut cluster, followers[1], "Note")?;
    assert_ne!(agreed["hash"], behind["hash"], "the cut follower has the write: {behind}");
    Ok(())
}

/// A bootstrap under a factor of three does not serve default writes until placed (C5 M4)
///
/// RF=3 on one node: readiness reports one active copy of three and refuses default writes, and
/// a write is refused naming the shortfall. Two joiners and an initialization give every tablet
/// three members, readiness reports three, and the same write is admitted and committed.
#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_does_not_reduce_configured_quorum() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .deferred_from(1)
        .replication_factor(3)
        .initialize(false)
        .start()
        .await?;
    let readiness = cluster.node_mut(0).command("READINESS")?;
    assert_eq!(readiness["ok"]["data"]["desired_rf"], 3, "{readiness}");
    assert_eq!(readiness["ok"]["data"]["active_rf"], 1, "{readiness}");
    assert_eq!(readiness["ok"]["data"]["default_writes"], serde_json::json!({ "Err": { "have": 1, "need": 2 } }), "{readiness}");
    let addr = cluster.node(0).endpoints.client.to_string();
    let refused = write_note(&addr, 5, "alone").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::QuorumUnavailable), "{refused:?}");
    // every group has one member: the bootstrapper serves one copy and says so
    let view = groups_of(&mut cluster, 0)?;
    assert!(view["groups"].as_u64().unwrap_or(0) > 0, "{view}");
    for shard in view["shards"].as_array().into_iter().flatten() {
        for group in shard["groups"].as_array().into_iter().flatten() {
            assert_eq!(group["members"].as_array().map(Vec::len), Some(1), "{group}");
        }
    }
    // two joiners and an initialization: three members per group
    cluster.start_deferred(1)?;
    cluster.start_deferred(2)?;
    cluster.wait_joined(&[1, 2])?;
    cluster.initialize(&[0, 1, 2])?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let readiness = cluster.node_mut(0).command("READINESS")?;
        if readiness["ok"]["data"]["active_rf"] == 3 && readiness["ok"]["data"]["members_up"] == 3 {
            assert_eq!(readiness["ok"]["data"]["default_writes"], serde_json::json!({ "Ok": null }), "{readiness}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the placement never reached three copies: {readiness}");
        std::thread::sleep(Duration::from_millis(100));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let view = groups_of(&mut cluster, 0)?;
        let three = view["shards"].as_array().into_iter().flatten().all(|shard| {
            shard["groups"].as_array().is_some_and(|groups| {
                !groups.is_empty() && groups.iter().all(|group| group["members"].as_array().map(Vec::len) == Some(3) && group["up"] == true)
            })
        });
        if three {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the groups never had three members: {view}");
        std::thread::sleep(Duration::from_millis(100));
    }
    // the write that was refused is admitted and committed now
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let written = write_note(&addr, 5, "placed").await;
        if written.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the write was never admitted: {written:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_note(&cluster.node(1).endpoints.client.to_string(), 5, Some("placed"), Duration::from_secs(20)).await?;
    Ok(())
}

/// An asynchronously durable replica cannot weaken a durable quorum (C5 M4)
///
/// A cluster node whose persistent table is configured `Async` refuses to start, naming C5; a
/// `One` write policy is refused at validation the same way; a standalone node with the same
/// table setting starts, since nothing counts its acknowledgement as a vote.
#[tokio::test(flavor = "multi_thread")]
async fn async_replica_cannot_weaken_durable_quorum() -> Result<(), FixtureError> {
    // a cluster node with an async persistent table does not come up
    let refused = Cluster::builder()
        .cluster(1, CoreClaim::Count(1))
        .durability(0, "async")
        .ready_timeout(Duration::from_secs(20))
        .start()
        .await;
    match refused {
        Err(FixtureError::ChildFailed(msg)) => {
            assert!(msg.contains("fdatasync") && msg.contains("Async"), "{msg}");
        }
        Err(other) => return Err(other),
        Ok(_) => panic!("a cluster node with an async table started"),
    }
    // a One write policy is refused before anything starts
    let error = ClusterConf::default()
        .bootstrap(true)
        .write_consistency(shoal::server::conf::cluster::Consistency::One)
        .validate("127.0.0.1", shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES)
        .expect_err("a One write policy was accepted");
    assert!(format!("{error}").contains("fdatasync"), "{error}");
    // a standalone node with the same table setting starts and serves
    let standalone = Cluster::builder()
        .standalone(CoreClaim::Count(1))
        .durability(0, "async")
        .start()
        .await?;
    round_trip(&standalone.node(0).endpoints.client.to_string(), 1).await?;
    Ok(())
}

/// Duplicate appends, a lost frame, reordered responses and a stale term do not reapply (C5 M4)
///
/// Writes flow through node zero while one follower's replication lane is delayed, then cut,
/// then healed - which is retransmission, duplicate appends and a gap the protocol recovers.
/// Then the node leading the keys' groups is killed, another is elected and takes more writes,
/// and the old one restarts with a stale term and catches up. Every acknowledged key is present
/// exactly once on every node, and the digests agree at equal applied indexes.
#[tokio::test(flavor = "multi_thread")]
async fn duplicates_gaps_and_old_terms_do_not_reapply() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .start()
        .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // keys whose groups node zero leads, so every write is proposed there and replicated out
    let keys = keys_led_by(&mut cluster, "Note", 0, 2000, 24)?;
    // writes while a follower's lane is delayed, then cut, then healed
    cluster.data_link(0, 1).delay(Duration::from_millis(150));
    for key in &keys[..8] {
        write_note(&addr0, *key, &format!("v{key}")).await?;
    }
    cluster.data_link(0, 1).cut();
    for key in &keys[8..16] {
        write_note(&addr0, *key, &format!("v{key}")).await?;
    }
    cluster.data_link(0, 1).heal();
    for key in &keys[16..] {
        write_note(&addr0, *key, &format!("v{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // kill a node that leads some of the groups, write more through a survivor, restart it
    let (victim_key, _) = key_led_by(&mut cluster, "Note", 1, 3000)?;
    cluster.kill(1)?;
    let addr2 = cluster.node(2).endpoints.client.to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let written = write_note(&addr2, victim_key, "after").await;
        if written.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the survivors never elected: {written:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for key in 3100..3108u64 {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if write_note(&addr2, key, &format!("v{key}")).await.is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "a write through a survivor never landed");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    cluster.restart(1, NodeKind::Server)?;
    cluster.wait_joined(&[1])?;
    // every acknowledged key is on every node exactly once, and the digests agree
    for node in 0..3 {
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in keys.iter().chain((3100..3108u64).collect::<Vec<_>>().iter()) {
            wait_note(&addr, *key, Some(&format!("v{key}")), Duration::from_secs(30)).await?;
        }
        wait_note(&addr, victim_key, Some("after"), Duration::from_secs(30)).await?;
    }
    let digest = wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    assert_eq!(digest["rows"].as_u64(), Some((keys.len() + 8 + 1) as u64), "{digest}");
    Ok(())
}

/// An uncommitted suffix never enters a checkpoint (C5 M4, P4)
///
/// The node leading a key's group is isolated and a write through it gets an unknown outcome.
/// Its WAL is rotated and its compactors driven: the sealed segment holds an unapplied entry, so
/// it is not handed over and nothing reaches an archive. The majority elects another leader and
/// commits a write of its own; the old leader is healed, restarted, and comes back with the
/// majority's history: its write is absent everywhere, the majority's present, the digests
/// agree, and only then is its segment compacted.
#[tokio::test(flavor = "multi_thread")]
async fn uncommitted_suffix_never_enters_checkpoint() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_millis(500))
        .start()
        .await?;
    let (key, _) = key_led_by(&mut cluster, "Note", 0, 4000)?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    write_note(&addr0, key, "committed").await?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(20))?;
    // isolated, the leader takes a write it can never commit
    cluster.isolate(0);
    let refused = write_note(&addr0, key, "speculative").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::OutcomeUnknown), "{refused:?}");
    // rotate and compact: the segment with the unapplied entry is not resolved, so not handed
    let _ = cluster.node_mut(0).command("ROTATE")?;
    let compacted = cluster.node_mut(0).command("COMPACT")?;
    assert_eq!(compacted["ok"][0]["handed"], 0, "an unresolved segment was compacted: {compacted}");
    assert_eq!(read_note(&addr0, key).await?, Some("committed".to_string()), "a speculative write was applied");
    // the majority elects and moves on
    let addr1 = cluster.node(1).endpoints.client.to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if write_note(&addr1, key, "majority").await.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the majority never elected");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // healed, the old leader is told what really happened; restarted, it comes back with it
    cluster.heal(0);
    wait_note(&addr0, key, Some("majority"), Duration::from_secs(30)).await?;
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    wait_note(&addr0, key, Some("majority"), Duration::from_secs(30)).await?;
    for node in 0..3 {
        let addr = cluster.node(node).endpoints.client.to_string();
        assert_eq!(read_note(&addr, key).await?, Some("majority".to_string()), "node {node}");
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // now that the suffix is truncated and the majority's entries applied, the segment resolves
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let compacted = cluster.node_mut(0).command("COMPACT")?;
        if compacted["ok"][0]["handed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the segment never resolved: {compacted}");
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}


/// A restart does not merge a segment below the checkpoint again (Resolved #104)
///
/// Five hundred notes at `v1` are sealed and compacted on every node, then the same five
/// hundred at `v2`, and the group's checkpoint passes both. Node two is restarted: every sealed
/// segment looks unhanded again, and before the fix the sweep handed both to the compactor,
/// which merged the `v1` generation over archives already holding `v2`. In the window between
/// the two merges the archive - and so anything read from it, and the digest - is `v1`. After
/// the fix a frame at or below the checkpoint is never handed, so nothing is compacting after
/// the sweep and the digest agrees with node zero's at once
/// ([Resolved #104](../../docs/src/appendix/resolved/segments-recompacted-after-restart.md)).
#[tokio::test(flavor = "multi_thread")]
async fn restart_does_not_recompact_segments_below_the_checkpoint() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .start()
        .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let keys: Vec<u64> = (7000..7500).collect();
    let (group, _) = group_of(&mut cluster, 0, "Note", keys[0])?;
    // the first generation, sealed and compacted past on every node
    write_notes_batch(&addr0, &keys, "v1").await?;
    let first = write_note_token(&addr0, 7999, "v1").await?.expect("a committed write carries a token");
    wait_checkpoint_past(&mut cluster, &[0, 1, 2], &group, first.index, Duration::from_secs(60))?;
    // the second generation over the same keys, compacted past too
    write_notes_batch(&addr0, &keys, "v2").await?;
    let second = write_note_token(&addr0, 7999, "v2").await?.expect("a committed write carries a token");
    wait_checkpoint_past(&mut cluster, &[0, 1, 2], &group, second.index, Duration::from_secs(60))?;
    let expected = wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // node two comes back with nothing resident and every sealed segment looking unhanded
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    let compacted = cluster.node_mut(2).command("COMPACT")?;
    assert!(compacted["ok"][0]["handed"].as_u64().unwrap_or(0) >= 2, "the sealed segments were not judged: {compacted}");
    let initial = compacting_of(&mut cluster, 2)?;
    // before the fix: wait for the first merge to finish while a later one is still to come,
    // which is the window in which the archive holds the older generation
    if let Some(first_handed) = initial.first().copied() {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let now = compacting_of(&mut cluster, 2)?;
            if now.is_empty() || !now.contains(&first_handed) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "the first merge never finished: {now:?}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let digest = digest_of(&mut cluster, 2, "Note")?;
    assert_eq!(
        (digest["rows"].clone(), digest["hash"].clone()),
        (expected["rows"].clone(), expected["hash"].clone()),
        "node two's archives no longer hold the state its checkpoint names: {digest} vs {expected}"
    );
    assert!(initial.is_empty(), "a segment below the checkpoint was handed to the compactor again: {initial:?}");
    // and the node converges with the rest, as ever
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A volatile group builds a snapshot at its applied position and purges its log (Resolved #105)
///
/// The ephemeral table's groups keep their log in memory, and before the fix nothing ever moved
/// their checkpoint: the snapshot builder was never offered, openraft never purged, and the
/// memory log grew to its bound. With the checkpoint at the applied position on every apply,
/// the policy builder fires every `checkpoint_entries` and the purge follows
/// ([Resolved #105](../../docs/src/appendix/resolved/volatile-groups-never-purged.md)).
#[tokio::test(flavor = "multi_thread")]
async fn a_volatile_group_purges_its_log() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .start()
        .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    // well past the checkpoint and retention counts, on every group of the table
    for key in 9000..9096u64 {
        client
            .send_one(Row {
                key,
                data: format!("row-{key}"),
            })
            .await
            .map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    }
    // a volatile group with a purge point, on the node that led the writes
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let view = groups_of(&mut cluster, 0)?;
        let purged: Vec<(u64, u64, u64)> = view["shards"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
            .filter(|group| group["table_name"] == "Row" && group["volatile"] == true)
            .map(|group| {
                (
                    group["applied"].as_u64().unwrap_or(0),
                    group["checkpoint"].as_u64().unwrap_or(0),
                    group["purged"].as_u64().unwrap_or(0),
                )
            })
            .collect();
        assert!(!purged.is_empty(), "no volatile group of Row: {view}");
        if purged.iter().any(|(_, _, purged)| *purged > 0) {
            // the checkpoint is the applied position, and the purge stays behind it
            for (applied, checkpoint, purged) in &purged {
                assert!(checkpoint <= applied, "{purged:?}");
                assert!(purged < applied, "{purged:?}");
            }
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no volatile group ever purged its log (applied, checkpoint, purged): {purged:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A snapshot is one stable cut at exactly its boundary (C7 M7, Q3)
///
/// Fifty notes take a mix of inserts, deletes, reinserts and updates through the leader, every
/// write's committed index kept from its session token. The first half of the mix is sealed
/// and compacted; a cut is asked for; the second half is written and compacted after it. The
/// cut's boundary is read from its manifest and the oracle is the last write to each key at or
/// below it: every key the oracle has is a record holding exactly that text, every key it
/// deleted is absent, and nothing written after the boundary is in the file
/// ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_has_one_stable_boundary_under_writes() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .start()
        .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    let (group, _) = group_of(&mut cluster, 0, "Note", 7000)?;
    let keys = keys_in_group(&mut cluster, "Note", &group, 7000, 50)?;
    // every write, with the index it committed at: (index, key, text or none for a delete)
    let mut history: Vec<(u64, u64, Option<String>)> = Vec::new();
    let mut step = |history: &mut Vec<(u64, u64, Option<String>)>, response: shoal::ShoalResponse<TestDbClient>, key: u64, text: Option<String>| {
        let token = response.session_token().expect("a committed write carries a token");
        history.push((token.index, key, text));
    };
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    // the first half: every key inserted, a third deleted, some of those reinserted, some updated
    for key in &keys {
        let text = format!("k{key}-v1");
        let response = client.send_one(Note { key: *key, text: text.clone() }).await.map_err(ok)?;
        step(&mut history, response, *key, Some(text));
    }
    for key in keys.iter().step_by(3) {
        let response = client.send_one(NoteDelete::new(*key)).await.map_err(ok)?;
        step(&mut history, response, *key, None);
    }
    for key in keys.iter().step_by(6) {
        let text = format!("k{key}-v2");
        let response = client.send_one(Note { key: *key, text: text.clone() }).await.map_err(ok)?;
        step(&mut history, response, *key, Some(text));
    }
    for key in keys.iter().skip(1).step_by(4) {
        let text = format!("k{key}-v3");
        let update = cluster::schema::NoteUpdate {
            partition_key: *key,
            text: Some(text.clone()),
        };
        match client.send_one(update).await {
            Ok(response) => step(&mut history, response, *key, Some(text)),
            // an update of a deleted key is refused and changes nothing
            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => {}
            Err(error) => return Err(ok(error)),
        }
    }
    let first_half = history.iter().map(|(index, _, _)| *index).max().expect("writes happened");
    wait_checkpoint_past(&mut cluster, &[0], &group, first_half, Duration::from_secs(60))?;
    // the cut, between the two halves
    let cut = cluster.node_mut(0).command(&format!("SNAPSHOT {group}"))?;
    let manifest = cut["ok"].clone();
    let boundary = manifest["boundary"].as_u64().unwrap_or_else(|| panic!("no boundary in {cut}"));
    assert!(boundary >= first_half, "the cut's boundary {boundary} is below the compacted writes {first_half}");
    let path = std::path::PathBuf::from(manifest["path"].as_str().expect("a path"));
    // the second half, after the cut: more of the same, compacted past too
    for key in keys.iter().step_by(2) {
        let text = format!("k{key}-v4");
        let response = client.send_one(Note { key: *key, text: text.clone() }).await.map_err(ok)?;
        step(&mut history, response, *key, Some(text));
    }
    for key in keys.iter().skip(2).step_by(5) {
        match client.send_one(NoteDelete::new(*key)).await {
            Ok(response) => step(&mut history, response, *key, None),
            // a delete of a key already gone is refused and changes nothing
            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => {}
            Err(error) => return Err(ok(error)),
        }
    }
    let second_half = history.iter().map(|(index, _, _)| *index).max().expect("writes happened");
    assert!(second_half > boundary, "the second half committed below the boundary");
    wait_checkpoint_past(&mut cluster, &[0], &group, second_half, Duration::from_secs(60))?;
    // the oracle: the last write to each key at or below the boundary
    let mut oracle: std::collections::BTreeMap<u64, Option<String>> = std::collections::BTreeMap::new();
    let mut ordered = history.clone();
    ordered.sort_by_key(|(index, _, _)| *index);
    for (index, key, text) in ordered {
        if index <= boundary {
            oracle.insert(key, text);
        }
    }
    let records = snapshot_records(&path);
    assert_eq!(records.len() as u64, manifest["records"].as_u64().unwrap_or(0), "the manifest's record count");
    // a record is keyed by the partition hash, not the note's key
    let hashed = |key: u64| <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
    let present: std::collections::BTreeMap<u64, Vec<u8>> = records.into_iter().collect();
    for (key, expected) in &oracle {
        match (expected, present.get(&hashed(*key))) {
            (Some(text), Some(bytes)) => {
                assert!(
                    bytes.windows(text.len()).any(|window| window == text.as_bytes()),
                    "key {key}: the record does not hold {text:?}"
                );
                // no other version of the key is in the record
                for other in ["v1", "v2", "v3", "v4"] {
                    let stale = format!("k{key}-{other}");
                    if stale != *text {
                        assert!(
                            !bytes.windows(stale.len()).any(|window| window == stale.as_bytes()),
                            "key {key}: the record holds {stale:?} beside {text:?}"
                        );
                    }
                }
            }
            (Some(text), None) => panic!("key {key} should hold {text:?} at {boundary} and is absent"),
            (None, Some(_)) => panic!("key {key} was deleted before {boundary} and is in the file"),
            (None, None) => {}
        }
    }
    let live: std::collections::BTreeSet<u64> = oracle
        .iter()
        .filter(|(_, text)| text.is_some())
        .map(|(key, _)| hashed(*key))
        .collect();
    for key in present.keys() {
        assert!(live.contains(key), "partition {key:016x} is in the file and not in the oracle");
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Conditional results follow committed order on every replica (C5 M4, Q4)
///
/// Every key is inserted once, then updates, deletes and no-ops on a small key set are sent
/// concurrently through all three nodes, each answer recorded in a ledger with its invocation
/// and completion order. The sequential oracle from the protocol model accepts the history, a
/// read of every key on every node after convergence is added to it and accepted too, and the
/// three digests agree.
#[tokio::test(flavor = "multi_thread")]
async fn conditional_results_follow_committed_order() -> Result<(), FixtureError> {
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .start()
        .await?;
    let keys: Vec<u64> = (1..=6).collect();
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    // the ledger and the clock every attempt is stamped by
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    // every key inserted once, sequentially, before anything concurrent
    for key in &keys {
        let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
        let attempt = Attempt { id, retry: 0 };
        let op = ClientOp::Mutate(MutationOp::Insert { key: Key(*key as u8), value: Value(0) });
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().invoke(attempt, tablet_id(*key), op, invoke);
        write_note(&addrs[0], *key, "0").await?;
        let complete = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Applied(true)));
    }
    // then updates and deletes through every node at once
    let mut tasks = Vec::new();
    for (node, addr) in addrs.iter().enumerate() {
        let addr = addr.clone();
        let keys = keys.clone();
        let ledger = ledger.clone();
        let clock = clock.clone();
        let next_id = next_id.clone();
        tasks.push(tokio::spawn(async move {
            let client = Shoal::<TestDbClient>::new(&addr).await?;
            for round in 0..8u32 {
                for (at, key) in keys.iter().enumerate() {
                    // a mix decided by the node and the round, so the three interleave
                    let value = Value(node as u32 * 100 + round + 1);
                    let delete = (round as usize + at + node) % 4 == 0;
                    let op = if delete {
                        MutationOp::Delete { key: Key(*key as u8) }
                    } else {
                        MutationOp::Update { key: Key(*key as u8), value }
                    };
                    let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
                    let attempt = Attempt { id, retry: 0 };
                    let invoke = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().invoke(attempt, TabletId {
                        table: shoal_model::ids::TableId(1),
                        range: tablet_of(*key) as u16,
                    }, ClientOp::Mutate(op), invoke);
                    // a delete or an update that did nothing is answered `false`, which the
                    // client reports as a query that did not succeed
                    let outcome = if delete {
                        match client.send_one(cluster::schema::NoteDelete::new(*key)).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(_) => Outcome::Unknown,
                        }
                    } else {
                        let update = cluster::schema::NoteUpdate {
                            partition_key: *key,
                            text: Some(value.0.to_string()),
                        };
                        match client.send_one(update).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(_) => Outcome::Unknown,
                        }
                    };
                    let complete = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().complete(attempt, complete, outcome);
                }
            }
            Ok::<(), shoal::client::Errors>(())
        }));
    }
    for task in tasks {
        task.await.expect("a writer task panicked")?;
    }
    // the replicas converge, and a read of every key on every node joins the ledger
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for addr in &addrs {
        for key in &keys {
            let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
            let attempt = Attempt { id, retry: 0 };
            let invoke = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().invoke(
                attempt,
                tablet_id(*key),
                ClientOp::Read {
                    key: Key(*key as u8),
                    level: ReadLevel::One,
                },
                invoke,
            );
            let seen = read_note(addr, *key).await?.map(|text| Value(text.parse().expect("a value")));
            let complete = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Value(seen)));
        }
    }
    let ledger = ledger.lock().unwrap().clone();
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    Ok(())
}

/// A tablet without a quorum consumes bounded resources while the others continue (C5 M4)
///
/// One group's flush completions are held on both followers, so its writes cannot reach a
/// durable quorum: the first few pend and are answered unknown at the deadline, the rest are
/// shed at the group's pending bound rather than queued, and the leader's memory stays bounded.
/// Writes to every other group commit at once meanwhile. Released, the held completions let the
/// pending writes commit and apply.
#[tokio::test(flavor = "multi_thread")]
async fn slow_tablet_does_not_block_other_tablets() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    // two shards a node, so a node leads two groups of the table and one can stall while
    // the other is written
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(2))
        .replication_factor(3)
        .write_timeout(Duration::from_millis(1500))
        .pending_bytes(2048)
        .start()
        .await?;
    let (slow_key, group) = key_led_by(&mut cluster, "Note", 0, 5000)?;
    let slow_keys = keys_in_group(&mut cluster, "Note", &group, slow_key, 24)?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // a key of another group, led by the same node
    let (other_key, other_group) = (5000..5200u64)
        .find_map(|key| {
            let (candidate, leader) = wait_group_leader(&mut cluster, "Note", key).ok()?;
            (candidate != group && leader == 0).then_some((key, candidate))
        })
        .expect("a key of another group led by node 0");
    assert_ne!(group, other_group);
    let other_keys = keys_in_group(&mut cluster, "Note", &other_group, other_key, 8)?;
    // both followers hold the slow group's completions
    for follower in [1, 2] {
        let stalled = cluster.node_mut(follower).command(&format!("STALL_WAL {group}"))?;
        assert!(stalled.get("ok").is_some(), "{stalled}");
    }
    let rss_before = cluster.node(0).rss_kib();
    // many writes to the slow group at once: some pend to the deadline, the rest are shed
    let mut tasks = Vec::new();
    for key in slow_keys {
        let addr = addr0.clone();
        tasks.push(tokio::spawn(async move { (key, write_note(&addr, key, &"x".repeat(200)).await) }));
    }
    // meanwhile writes to the other group land at once
    let started = std::time::Instant::now();
    for key in other_keys {
        write_note(&addr0, key, "fast").await?;
    }
    let fast = started.elapsed();
    assert!(fast < Duration::from_secs(1), "writes to an unaffected group took {fast:?}");
    // which writes pended to the deadline, and which were shed before entering the log
    let mut unknown = Vec::new();
    let mut shed = 0;
    for task in tasks {
        match task.await.expect("a writer task panicked") {
            (_, Ok(())) => panic!("a write to a group without a quorum succeeded"),
            (key, Err(error)) => match failure_code(&Err::<(), _>(error)) {
                Some(ErrorCode::OutcomeUnknown) => unknown.push(key),
                Some(ErrorCode::Shedding) => shed += 1,
                other => panic!("a write to the slow group failed for another reason: {other:?}"),
            },
        }
    }
    assert!(!unknown.is_empty(), "no write pended to the deadline");
    assert!(shed > 0, "no write was shed at the pending bound");
    let rss_after = cluster.node(0).rss_kib();
    assert!(
        rss_after < rss_before + 64 * 1024,
        "the leader grew by {} KiB with one group stalled",
        rss_after.saturating_sub(rss_before)
    );
    // released, the pending writes commit and apply: an unknown outcome was a write in the
    // leader's log waiting on the followers, and a shed one never entered it
    for follower in [1, 2] {
        let released = cluster.node_mut(follower).command(&format!("RELEASE_WAL {group}"))?;
        assert!(released.get("ok").is_some(), "{released}");
    }
    for key in unknown {
        wait_note(&addr0, key, Some(&"x".repeat(200)), Duration::from_secs(30)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    Ok(())
}

/// Volatile replication uses the common encoding, with weaker durability said out loud (C5 M4)
///
/// Rows of the ephemeral table written through one node are read from the other two, which
/// replicate them through the same command encoding the persistent table uses; the three
/// digests agree; the groups say their logs are volatile; and once every node has restarted
/// the rows are gone while the persistent table's notes are not.
#[tokio::test(flavor = "multi_thread")]
async fn volatile_replication_uses_common_encoding() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .start()
        .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await?;
    for key in 100..110u64 {
        client.send_one(Row { key, data: format!("row {key}") }).await?;
        write_note(&addr0, key, &format!("note {key}")).await?;
    }
    // read back from the other two, locally
    for node in 1..3 {
        let other = Shoal::<TestDbClient>::new(&cluster.node(node).endpoints.client.to_string()).await?;
        for key in 100..110u64 {
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            loop {
                let found = match other.send_one(RowGet::new(vec![key])).await {
                    Ok(response) => response.access::<Row>()?.is_some_and(|rows| rows.len() == 1),
                    Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => false,
                    Err(error) => return Err(error.into()),
                };
                if found {
                    break;
                }
                assert!(std::time::Instant::now() < deadline, "row {key} never reached node {node}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let rows = wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(20))?;
    assert_eq!(rows["rows"].as_u64(), Some(10), "{rows}");
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(20))?;
    // the groups of the ephemeral table say so
    let view = groups_of(&mut cluster, 0)?;
    let mut volatile = 0;
    let mut durable = 0;
    for shard in view["shards"].as_array().into_iter().flatten() {
        for group in shard["groups"].as_array().into_iter().flatten() {
            match (group["table_name"].as_str(), group["volatile"].as_bool()) {
                (Some("Row"), Some(true)) => volatile += 1,
                (Some("Note"), Some(false)) => durable += 1,
                other => panic!("a group is labelled wrongly: {other:?} in {group}"),
            }
        }
    }
    assert!(volatile > 0 && durable > 0, "{view}");
    // every node restarted: the rows are gone, the notes are not
    for node in 0..3 {
        cluster.kill(node)?;
    }
    for node in 0..3 {
        cluster.restart(node, NodeKind::Server)?;
    }
    cluster.wait_joined(&[0, 1, 2])?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    wait_note(&addr0, 100, Some("note 100"), Duration::from_secs(30)).await?;
    let client = Shoal::<TestDbClient>::new(&addr0).await?;
    assert!(found_nothing(client.send_one(RowGet::new(vec![100])).await), "an ephemeral row survived a full restart");
    Ok(())
}

/// `One` reads converge without exposing uncommitted state (C6 M4, P4)
///
/// A follower cut off from the leader serves its committed, applied state - the old value -
/// while the leader and the other follower move on; healed, it converges. Then the leader is
/// isolated and takes a write it cannot commit: a read through it does not show the appended
/// entry, and once healed every node converges on what the majority committed.
#[tokio::test(flavor = "multi_thread")]
async fn one_reads_converge_without_exposing_uncommitted_state() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_millis(500))
        .start()
        .await?;
    let (key, _) = key_led_by(&mut cluster, "Note", 0, 6000)?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr1 = cluster.node(1).endpoints.client.to_string();
    write_note(&addr0, key, "one").await?;
    wait_note(&addr1, key, Some("one"), Duration::from_secs(20)).await?;
    // a cut follower keeps serving the old committed value
    cluster.data_link(0, 1).cut();
    cluster.data_link(1, 0).cut();
    write_note(&addr0, key, "two").await?;
    assert_eq!(read_note(&addr0, key).await?, Some("two".to_string()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(read_note(&addr1, key).await?, Some("one".to_string()), "a cut follower saw a newer value");
    cluster.data_link(0, 1).heal();
    cluster.data_link(1, 0).heal();
    wait_note(&addr1, key, Some("two"), Duration::from_secs(20)).await?;
    // an isolated leader never shows what it could not commit
    cluster.isolate(0);
    let refused = write_note(&addr0, key, "three").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::OutcomeUnknown), "{refused:?}");
    assert_eq!(read_note(&addr0, key).await?, Some("two".to_string()), "an uncommitted write was read");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let written = write_note(&addr1, key, "four").await;
        if written.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the majority never elected: {written:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cluster.heal(0);
    for node in 0..3 {
        let addr = cluster.node(node).endpoints.client.to_string();
        wait_note(&addr, key, Some("four"), Duration::from_secs(30)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    Ok(())
}


/// A strong read through another coordinator sees a committed write, and never a stale value;
/// a session token is served past the write or not at all (C6 M5, F41)
///
/// Three nodes at a factor of three. Twenty keys led by node zero are written through it and
/// read at `Quorum` through the other two at once: every read sees the write. The lane between
/// node zero and node one is cut both ways and a second value is written through zero, which
/// commits on zero and two. A `One` read through one is the old value, which is what `One`
/// promises. Node one is then cut from node two as well, so no election can bring it the new
/// value: a `Quorum` read through one cannot obtain a barrier and is answered `Timeout`, never
/// the old value, and a session read through one carrying the write's token waits and times
/// out. Healed, the `Quorum` read and the session read through one both see the new value, and
/// node one's counters show the barrier hopped to the leader and the replica waited to apply.
#[tokio::test(flavor = "multi_thread")]
async fn barrier_read_observes_prior_quorum_write() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_millis(500))
        .query_deadline(Duration::from_secs(2))
        .start()
        .await?;
    let keys = keys_led_by(&mut cluster, "Note", 0, 2000, 20)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    let one = SendOptions::new().read(ReadLevel::One);
    // every write through zero is seen at once by a strong read through one and two
    for (round, key) in keys.iter().enumerate() {
        let text = format!("round {round}");
        write_note(&addrs[0], *key, &text).await?;
        for reader in 1..3 {
            let seen = read_note_with(&addrs[reader], *key, &quorum).await?;
            assert_eq!(seen.as_deref(), Some(text.as_str()), "a strong read through node {reader} was stale for key {key}");
        }
    }
    let key = keys[0];
    write_note(&addrs[0], key, "one").await?;
    // seen at Quorum through node one, which is what makes it applied there
    assert_eq!(read_note_with(&addrs[1], key, &quorum).await?.as_deref(), Some("one"));
    // cut node one off from the leader, both ways
    cluster.data_link(0, 1).cut();
    cluster.data_link(1, 0).cut();
    // a second value commits on zero and two, and hands back a token
    let token = write_note_token(&addrs[0], key, "two").await?.expect("a committed write mints a token");
    assert!(token.index > 0, "the token names no index");
    // a One read through the cut node is the old committed value, which is what One promises
    assert_eq!(read_note_with(&addrs[1], key, &one).await?.as_deref(), Some("one"));
    // cut it from node two as well, so no election can bring it the new value
    cluster.data_link(1, 2).cut();
    cluster.data_link(2, 1).cut();
    // a strong read through it cannot confirm a leader: timeout, never "one"
    let stale = read_note_with(&addrs[1], key, &quorum).await;
    assert_eq!(failure_code(&stale), Some(ErrorCode::Timeout), "a strong read through the cut node answered {stale:?}");
    // and a session read past the token waits for an apply that cannot come, and times out
    let session = SendOptions::new().read(ReadLevel::One).token(token);
    let behind = read_note_with(&addrs[1], key, &session).await;
    assert_eq!(failure_code(&behind), Some(ErrorCode::Timeout), "a session read through the cut node answered {behind:?}");
    // healed, the strong read sees the write; a group mid-election may time out once or twice
    for (from, to) in [(0, 1), (1, 0), (1, 2), (2, 1)] {
        cluster.data_link(from, to).heal();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match read_note_with(&addrs[1], key, &quorum).await {
            Ok(Some(text)) if text == "two" => break,
            Ok(other) => panic!("a strong read through the healed node was stale: {other:?}"),
            Err(error) => {
                assert_eq!(failure_code::<()>(&Err(error)), Some(ErrorCode::Timeout));
                assert!(std::time::Instant::now() < deadline, "a strong read never succeeded after the heal");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    // and so does the session read, without a barrier
    let hops_before = read_stats(&mut cluster, 1)?["stats"]["barrier_hops"].as_u64().unwrap_or(0);
    assert_eq!(read_note_with(&addrs[1], key, &session).await?.as_deref(), Some("two"));
    let stats = read_stats(&mut cluster, 1)?;
    assert_eq!(stats["stats"]["barrier_hops"].as_u64().unwrap_or(0), hops_before, "a session read hopped: {stats}");
    assert!(stats["stats"]["barriers"].as_u64().unwrap_or(0) >= 1, "{stats}");
    assert!(hops_before >= 1, "the strong reads through a follower never hopped: {stats}");
    assert!(stats["stats"]["apply_wait_ns_max"].as_u64().unwrap_or(0) > 0, "no apply wait was recorded: {stats}");
    assert!(stats["stats"]["session_waits"].as_u64().unwrap_or(0) >= 1, "no session wait was recorded: {stats}");
    assert!(stats["stats"]["timeouts"].as_u64().unwrap_or(0) >= 2, "the timeouts were not counted: {stats}");
    Ok(())
}

/// A session token is judged by name: another cluster, another lineage, no cluster at all, and
/// a client that did not ask for tokens is sent none (F41)
///
/// A real token from a committed write is forged three ways. Naming another cluster it is
/// refused `WrongCluster`; naming another group for its tablet it is refused `UnknownLineage`;
/// sent to a standalone node it is refused `WrongCluster`, since that node is in no cluster. A
/// raw connection that does not ask for the token section gets an answer with no token on it,
/// and one that does gets the token.
#[tokio::test(flavor = "multi_thread")]
async fn session_token_lineage_is_checked_by_name() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::identity::{ClusterId, GroupId};
    use shoal::shared::protocol::error::ErrorCode;
    use shoal::shared::protocol::{self, handshake, read};
    use shoal::shared::protocol::auth::AuthMechanisms;
    use shoal::shared::queries::Queries;
    use shoal::shared::traits::QuerySupport;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .query_deadline(Duration::from_secs(2))
        .start()
        .await?;
    let addr = cluster.node(0).endpoints.client.to_string();
    let key = 4242;
    let token = write_note_token(&addr, key, "minted").await?.expect("a committed write mints a token");
    // the token as minted is honoured
    let honest = SendOptions::new().token(token);
    assert_eq!(read_note_with(&addr, key, &honest).await?.as_deref(), Some("minted"));
    // another cluster is refused by name
    let mut foreign = token;
    foreign.cluster = ClusterId::mint();
    let refused = read_note_with(&addr, key, &SendOptions::new().token(foreign)).await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::WrongCluster), "{refused:?}");
    // another group for the same tablet is another lineage
    let mut moved = token;
    moved.group = GroupId(token.group.0 ^ 0xdead_beef);
    let refused = read_note_with(&addr, key, &SendOptions::new().token(moved)).await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::UnknownLineage), "{refused:?}");
    let stats = read_stats(&mut cluster, 0)?;
    assert!(stats["stats"]["lineage_refusals"].as_u64().unwrap_or(0) >= 1, "{stats}");
    // a standalone node is in no cluster, so any token is the wrong cluster; one is started
    // in this process, since the fixture's directories all belong to the cluster
    let temp_dir = utils::test_dir();
    let (_client, standalone) = utils::start_with_conf::<TestDb>(utils::build_config(&temp_dir))
        .await
        .map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    let standalone_addr = standalone.bound_addr().to_string();
    write_note(&standalone_addr, key, "alone").await?;
    let refused = read_note_with(&standalone_addr, key, &honest).await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::WrongCluster), "{refused:?}");
    standalone.exit().map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    // a raw connection that asks for nothing gets no token section, and one that asks does
    for (caps, expect_token) in [(0u8, false), (read::CLIENT_CAP_READ_OPTIONS, true)] {
        let mut sock = tokio::net::TcpStream::connect(&addr).await.map_err(FixtureError::Io)?;
        let hello = handshake::Hello {
            schema_fingerprint: TestDbClient::SCHEMA_FINGERPRINT,
            max_frame_bytes: protocol::DEFAULT_MAX_FRAME_BYTES,
            mechanisms: AuthMechanisms::NONE,
            caps,
        };
        sock.write_all(&hello.frame(protocol::DEFAULT_MAX_FRAME_BYTES).expect("a hello frames")).await.map_err(FixtureError::Io)?;
        let mut ack = [0u8; handshake::HANDSHAKE_FRAME_LEN];
        sock.read_exact(&mut ack).await.map_err(FixtureError::Io)?;
        let mut body = [0u8; handshake::HANDSHAKE_BODY_LEN];
        body.copy_from_slice(&ack[protocol::HEADER_LEN..]);
        let granted = handshake::HelloAck::decode(&body);
        assert!(granted.reason.is_accepted());
        assert_eq!(granted.caps, caps, "the server granted other than what was asked");
        // a write, framed the way the client frames one
        let queries = Queries::<TestDbClient> {
            id: uuid::Uuid::new_v4(),
            queries: vec![Note { key: key + 1, text: "raw".to_string() }.into()],
            base_index: 0,
        };
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&queries).expect("a bundle archives");
        let preamble = protocol::request_preamble(archived.len(), protocol::DEFAULT_MAX_FRAME_BYTES).expect("a preamble");
        sock.write_all(&preamble).await.map_err(FixtureError::Io)?;
        sock.write_all(&archived).await.map_err(FixtureError::Io)?;
        // the answer's frame says whether a token sits ahead of its payload
        loop {
            let mut raw = [0u8; protocol::RESPONSE_PREAMBLE_LEN];
            sock.read_exact(&mut raw).await.map_err(FixtureError::Io)?;
            let frame = protocol::decode_server_frame(&raw, protocol::DEFAULT_MAX_FRAME_BYTES).expect("a server frame");
            let mut rest = vec![0u8; frame.rest_len];
            sock.read_exact(&mut rest).await.map_err(FixtureError::Io)?;
            // the topology push a connection is handed first is skipped
            if frame.header.kind != protocol::MessageType::Response {
                continue;
            }
            assert_eq!(frame.token_len() == read::SESSION_TOKEN_LEN, expect_token, "caps {caps}: token {} bytes", frame.token_len());
            if expect_token {
                let mut raw_token = [0u8; read::SESSION_TOKEN_LEN];
                raw_token.copy_from_slice(&rest[..read::SESSION_TOKEN_LEN]);
                let minted = read::SessionToken::decode(&raw_token).expect("a token decodes");
                assert_eq!(minted.cluster, token.cluster);
                assert_eq!(minted.table, token.table);
            }
            break;
        }
    }
    Ok(())
}

/// The rows each node's shard of a get contributes are counted by slot, so an empty partition,
/// a deleted row and a missing share are three different answers (C6 M5, F41)
///
/// Three nodes at a factor of one, three keys placed one per node. A get over six keys - the
/// three and three never written - through node zero returns exactly three rows and succeeds,
/// since an empty share covers its slot. The node two key deleted through node two: two rows. A
/// get over unwritten keys alone is a successful empty answer. The lane between zero and two
/// cut: the six key get is one error, never two rows presented as the answer, since a share
/// that never arrives covers nothing. Healed: two rows again, and no gather left resident.
#[tokio::test(flavor = "multi_thread")]
async fn empty_and_deleted_partitions_have_explicit_coverage() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .lane_links(true)
        .query_deadline(Duration::from_secs(2))
        .start()
        .await?;
    // one key per node, placed by the rule the ring routes with
    let keys: Vec<u64> = keys_placed_per_node(&mut cluster, 1, 5000)?.into_iter().map(|found| found[0]).collect();
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    for (node, key) in keys.iter().enumerate() {
        write_note(&addrs[0], *key, &format!("on node {node}")).await?;
    }
    // three present and three that were never written, in one get: three rows, and success
    let never = [9_000_001u64, 9_000_002, 9_000_003];
    let mut asked: Vec<u64> = keys.clone();
    asked.extend(never);
    let options = SendOptions::default();
    let found = read_notes(&addrs[0], &asked, None, &options).await?;
    assert_eq!(found.len(), 3, "{found:?}");
    for (node, key) in keys.iter().enumerate() {
        assert!(found.contains(&(*key, format!("on node {node}"))), "{found:?}");
    }
    // the node two key deleted through node two is gone from the answer, not resurrected
    delete_note(&addrs[2], keys[2]).await?;
    let found = read_notes(&addrs[0], &asked, None, &options).await?;
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(!found.iter().any(|(key, _)| *key == keys[2]), "{found:?}");
    // a get over nothing but unwritten keys is a successful empty answer
    assert!(read_notes(&addrs[0], &never, None, &options).await?.is_empty());
    // with node two unreachable, the get is one error and never the two rows that did arrive
    cluster.data_link(0, 2).cut();
    cluster.data_link(2, 0).cut();
    let missing = read_notes(&addrs[0], &asked, None, &options).await;
    assert!(missing.is_err(), "a get missing a share answered {missing:?}");
    assert!(
        matches!(failure_code(&missing), Some(shoal::shared::protocol::error::ErrorCode::Timeout | shoal::shared::protocol::error::ErrorCode::OutcomeUnknown | shoal::shared::protocol::error::ErrorCode::Unavailable)),
        "{missing:?}"
    );
    // healed, the two rows again, and nothing left resident
    cluster.data_link(0, 2).heal();
    cluster.data_link(2, 0).heal();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match read_notes(&addrs[0], &asked, None, &options).await {
            Ok(found) if found.len() == 2 => break,
            Ok(found) => panic!("the healed get answered {found:?}"),
            Err(error) => {
                assert!(std::time::Instant::now() < deadline, "the healed get never succeeded: {error:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    let stats = read_stats(&mut cluster, 0)?;
    assert_eq!(stats["resident"].as_u64(), Some(0), "{stats}");
    Ok(())
}

/// A limited get over several nodes keeps the first rows in the order it named, and never
/// answers with fewer required shares than it has (C6 M5, F41)
///
/// Six keys over three nodes at a factor of one. A get over all six with a limit of four
/// answers the first four in the order the get named them, which is the first four `One` reads
/// would give. With node one's shares held, the same get is `Timeout` rather than the four
/// rows the other two nodes could have supplied. The unit half of this row is in
/// `shoal-proto`'s `responses.rs`, where the pushdown is proved over random layouts.
#[tokio::test(flavor = "multi_thread")]
async fn limits_apply_after_complete_ordered_gather() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .query_deadline(Duration::from_secs(1))
        .start()
        .await?;
    // two keys per node, interleaved so every prefix of the get spans nodes
    let placed = keys_placed_per_node(&mut cluster, 2, 6000)?;
    let keys: Vec<u64> = (0..2).flat_map(|round| placed.iter().map(move |found| found[round])).collect();
    let addr = cluster.node(0).endpoints.client.to_string();
    for (at, key) in keys.iter().enumerate() {
        write_note(&addr, *key, &format!("row {at}")).await?;
    }
    // the limited get is the first four, in named order
    let options = SendOptions::default();
    let found = read_notes(&addr, &keys, Some(4), &options).await?;
    let expected: Vec<(u64, String)> = keys.iter().take(4).enumerate().map(|(at, key)| (*key, format!("row {at}"))).collect();
    assert_eq!(found, expected);
    // and the same get named in the reverse order is the first four of that order
    let reversed: Vec<u64> = keys.iter().rev().copied().collect();
    let found = read_notes(&addr, &reversed, Some(4), &options).await?;
    let expected: Vec<(u64, String)> = reversed.iter().take(4).map(|key| (*key, format!("row {}", keys.iter().position(|k| k == key).expect("a key")))).collect();
    assert_eq!(found, expected);
    // with one node's shares held, the answer is a timeout and never four rows of six
    let held = cluster.node_mut(1).command("HOLD_SHARES 0 3000")?;
    assert_eq!(held["ok"]["holding"], true, "{held}");
    let missing = read_notes(&addr, &keys, Some(4), &options).await;
    assert_eq!(failure_code(&missing), Some(ErrorCode::Timeout), "{missing:?}");
    // once released, the answer is whole again
    tokio::time::sleep(Duration::from_secs(3)).await;
    let found = read_notes(&addr, &keys, Some(4), &options).await?;
    assert_eq!(found.len(), 4);
    Ok(())
}

/// A gather that expires answers once, a late share is dropped, and a duplicate is too
/// (C6 M5, F41)
///
/// Three nodes at a factor of one, node one holding every share it would send for three
/// seconds and sending each twice on release, node two holding its own for four. A bundle of a
/// six key get and a single node zero key get, with a half second deadline: the first is one
/// `Timeout`, the second succeeds, and the stream ends once; the held shares arrive late. A
/// second six key get with the server's budget completes when node two releases; node one's
/// second copy arrived while it was still waiting on node two and is a duplicate. Afterwards
/// no gather is resident and both kinds of dropped share were counted.
#[tokio::test(flavor = "multi_thread")]
async fn gather_timeout_completes_once_and_discards_late_replies() -> Result<(), FixtureError> {
    use shoal::client::{QuerySuceededOpts, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder().cluster(3, CoreClaim::Count(1)).start().await?;
    // two keys per node
    let placed = keys_placed_per_node(&mut cluster, 2, 7000)?;
    let keys: Vec<u64> = (0..2).flat_map(|round| placed.iter().map(move |found| found[round])).collect();
    let addr = cluster.node(0).endpoints.client.to_string();
    for (at, key) in keys.iter().enumerate() {
        write_note(&addr, *key, &format!("row {at}")).await?;
    }
    // hold node one's shares, sending each twice when they go, and node two's a second longer
    let held = cluster.node_mut(1).command("HOLD_SHARES 0 3000 dup")?;
    assert_eq!(held["ok"]["dup"], true, "{held}");
    let held = cluster.node_mut(2).command("HOLD_SHARES 0 4000")?;
    assert_eq!(held["ok"]["dup"], false, "{held}");
    // a bundle: a get that needs node one, then one that does not, at a half second deadline
    let client = Shoal::<TestDbClient>::new(&addr).await?;
    let queries = client.query().add(NoteGet::new(keys.clone())).add(NoteGet::new(vec![keys[0]]));
    let mut stream = client.send_with(queries, &SendOptions::new().deadline(Duration::from_millis(500))).await?;
    let first = stream.next().await?.expect("the first answer");
    assert_eq!(first.get_index(), 0);
    match first.suceeded(QuerySuceededOpts::default()) {
        Err(shoal::client::Errors::Server { code, .. }) => assert_eq!(code, ErrorCode::Timeout),
        other => panic!("the held get answered {other:?}"),
    }
    let second = stream.next().await?.expect("the second answer");
    assert_eq!(second.get_index(), 1);
    second.suceeded(QuerySuceededOpts::default())?;
    assert_eq!(second.access::<Note>()?.map(|notes| notes.len()), Some(1));
    // the stream ends once, and only once
    assert!(stream.next().await?.is_none());
    // a second get under the server's own budget completes when the last hold releases
    let started = std::time::Instant::now();
    let found = read_notes(&addr, &keys, None, &SendOptions::default()).await?;
    assert_eq!(found.len(), 6, "{found:?}");
    assert!(started.elapsed() >= Duration::from_millis(2500), "the held get answered before the release: {:?}", started.elapsed());
    // the late copies and the duplicate copies were dropped and counted, and nothing is resident
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stats = read_stats(&mut cluster, 0)?;
    assert_eq!(stats["resident"].as_u64(), Some(0), "{stats}");
    assert!(stats["stats"]["timeouts"].as_u64().unwrap_or(0) >= 1, "{stats}");
    assert!(stats["stats"]["late_shares"].as_u64().unwrap_or(0) >= 1, "{stats}");
    assert!(stats["stats"]["duplicate_shares"].as_u64().unwrap_or(0) >= 1, "{stats}");
    Ok(())
}

/// A mixed bundle resolves each table's read policy on its own, and an override covers both
/// (C6 M5, F41)
///
/// Three nodes at a factor of three. `Note` is set to `quorum` through the control plane; a
/// bundle of a `Row` get and a `Note` get through node one with no override obtains one barrier,
/// with a `Quorum` override two, with a `One` override none. Node one cut from the others, the
/// `Note` half of the bundle is `Timeout` and the `Row` half succeeds. A table the schema does
/// not have is refused by name, and `read_consistency: All` is refused at validation
/// (`validation_refuses_what_is_not_built`). The control state's own rules are the unit half,
/// `mixed_table_bundle_resolves_each_table_policy` in `control/types.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_table_bundle_resolves_each_table_policy() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_millis(500))
        .query_deadline(Duration::from_secs(2))
        .start()
        .await?;
    // a key of each table, both led by node zero
    let (row_key, _) = key_led_by(&mut cluster, "Row", 0, 8000)?;
    let (note_key, _) = key_led_by(&mut cluster, "Note", 0, 8000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    {
        let client = Shoal::<TestDbClient>::new(&addrs[0]).await?;
        client.send_one(Row { key: row_key, data: "a row".to_string() }).await?;
    }
    write_note(&addrs[0], note_key, "a note").await?;
    // notes are served at quorum unless a bundle says otherwise; every node learns it
    let set = cluster.node_mut(0).command("SET_TABLE_READ_POLICY Note quorum")?;
    let version = set["ok"]["version"].as_u64().unwrap_or_else(|| panic!("{set}"));
    cluster.wait_map_version(&[0, 1, 2], version)?;
    let map = cluster.node_mut(1).command("MAP")?;
    assert!(map["ok"]["table_read_policy"].to_string().contains("Quorum"), "{map}");
    // both halves are seen through node one, and only the note half paid a barrier
    let barriers = |cluster: &mut Cluster| -> Result<u64, FixtureError> {
        Ok(read_stats(cluster, 1)?["stats"]["barriers"].as_u64().unwrap_or(0))
    };
    let before = barriers(&mut cluster)?;
    let (row, note) = read_mixed(&addrs[1], row_key, note_key, &SendOptions::default()).await?;
    assert_eq!(row?.as_deref(), Some("a row"));
    assert_eq!(note?.as_deref(), Some("a note"));
    assert_eq!(barriers(&mut cluster)? - before, 1, "an inherited policy did not resolve per table");
    // an override to quorum covers both halves
    let before = barriers(&mut cluster)?;
    let (row, note) = read_mixed(&addrs[1], row_key, note_key, &SendOptions::new().read(ReadLevel::Quorum)).await?;
    assert_eq!(row?.as_deref(), Some("a row"));
    assert_eq!(note?.as_deref(), Some("a note"));
    assert_eq!(barriers(&mut cluster)? - before, 2, "a quorum override did not cover both tables");
    // and an override to one covers neither
    let before = barriers(&mut cluster)?;
    let (row, note) = read_mixed(&addrs[1], row_key, note_key, &SendOptions::new().read(ReadLevel::One)).await?;
    assert_eq!(row?.as_deref(), Some("a row"));
    assert_eq!(note?.as_deref(), Some("a note"));
    assert_eq!(barriers(&mut cluster)? - before, 0, "a one override still paid a barrier");
    // node one cut off: the note half cannot obtain a barrier, the row half is served
    for (from, to) in [(0, 1), (1, 0), (1, 2), (2, 1)] {
        cluster.data_link(from, to).cut();
    }
    let (row, note) = read_mixed(&addrs[1], row_key, note_key, &SendOptions::default()).await?;
    assert_eq!(row?.as_deref(), Some("a row"));
    assert_eq!(failure_code(&note), Some(ErrorCode::Timeout), "the note half answered {note:?}");
    for (from, to) in [(0, 1), (1, 0), (1, 2), (2, 1)] {
        cluster.data_link(from, to).heal();
    }
    // a table the schema does not have is refused by name, and an unknown level too
    let refused = cluster.node_mut(0).command("SET_TABLE_READ_POLICY Nope quorum")?;
    assert!(refused["error"].to_string().contains("Nope"), "{refused}");
    let refused = cluster.node_mut(0).command("SET_TABLE_READ_POLICY Note all")?;
    assert!(refused["error"].to_string().contains("not a read level"), "{refused}");
    // clearing puts the note back at the cluster's default
    let cleared = cluster.node_mut(0).command("SET_TABLE_READ_POLICY Note clear")?;
    let version = cleared["ok"]["version"].as_u64().unwrap_or_else(|| panic!("{cleared}"));
    cluster.wait_map_version(&[1], version)?;
    let before = barriers(&mut cluster)?;
    let (row, note) = read_mixed(&addrs[1], row_key, note_key, &SendOptions::default()).await?;
    assert_eq!(row?.as_deref(), Some("a row"));
    assert_eq!(note?.as_deref(), Some("a note"));
    assert_eq!(barriers(&mut cluster)? - before, 0, "a cleared policy still paid a barrier");
    Ok(())
}

/// Cached reports cannot choose a history that loses an acknowledged write (C7 M6, F42)
///
/// The B=100/C=101/A+B=102 schedule from [C7](../../docs/src/distributed/failover.md), on real
/// nodes: node zero leads a key's group; 100 reaches everybody; node one is cut off and 101
/// commits on zero and two; node one is healed and catches up; node two is cut off and 102
/// commits on zero and one; node zero is killed. A rule that promoted the freshest report
/// would pick node two, whose last report was 101, and lose 102. Raft's election restriction
/// cannot: node two's log is shorter, so node one refuses it a vote and is the only member that
/// can win, and 102 is on both survivors before node zero comes back and converges too.
#[tokio::test(flavor = "multi_thread")]
async fn stale_heartbeat_reports_cannot_lose_acked_write() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    // 100 everywhere
    write_note(&addrs[0], key, "100").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("100"), Duration::from_secs(10)).await?;
    }
    // 101 on zero and two: node one is cut off from the leader
    cluster.data_link(0, 1).cut();
    cluster.data_link(1, 0).cut();
    write_note(&addrs[0], key, "101").await?;
    wait_note(&addrs[2], key, Some("101"), Duration::from_secs(10)).await?;
    assert_eq!(read_note(&addrs[1], key).await?.as_deref(), Some("100"), "the cut member saw 101");
    // node one is healed and catches up to 101
    cluster.data_link(0, 1).heal();
    cluster.data_link(1, 0).heal();
    wait_note(&addrs[1], key, Some("101"), Duration::from_secs(10)).await?;
    // 102 on zero and one: node two is cut off, and its last word is 101
    cluster.data_link(0, 2).cut();
    cluster.data_link(2, 0).cut();
    write_note(&addrs[0], key, "102").await?;
    wait_note(&addrs[1], key, Some("102"), Duration::from_secs(10)).await?;
    assert_eq!(read_note(&addrs[2], key).await?.as_deref(), Some("101"), "the cut member saw 102");
    // the leader dies: only node one's log holds 102, so only node one can win
    cluster.kill(0)?;
    let (elected_group, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    assert_eq!(elected_group, group);
    assert_eq!(leader, 1, "a member whose log lacks the acknowledged write was elected");
    // and 102 is on both survivors, strongly
    for reader in 1..3 {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match read_note_with(&addrs[reader], key, &quorum).await {
                Ok(Some(text)) if text == "102" => break,
                Ok(other) => panic!("node {reader} lost the acknowledged write: {other:?}"),
                Err(error) => {
                    assert!(std::time::Instant::now() < deadline, "node {reader} never served 102: {error:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
    wait_digests_equal(&mut cluster, &[1, 2], "Note", Duration::from_secs(30))?;
    // node zero comes back and converges on the same history
    cluster.data_link(0, 2).heal();
    cluster.data_link(2, 0).heal();
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    wait_note(&addr0, key, Some("102"), Duration::from_secs(30)).await?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A delayed map and a delayed term cannot make an old primary authoritative (C7 M6, F42)
///
/// Node zero leads a key's group. Its data lanes are cut both ways while its control lanes
/// stay up, and what control traffic reaches it is delayed: the control plane keeps calling it
/// `Up`, the map does not move, and nothing it hears says it lost the lead. The survivors
/// elect and commit through the new leader. Once its lease lapses a write through node zero
/// is `NotLeader` - refused before anything is appended, not `OutcomeUnknown` at a deadline -
/// and a strong read through it is refused too, never the stale value. With the data lanes
/// healed and the control lanes cut instead, writes through every node still commit, since a
/// tablet's authority is its own group's. Healed, everybody holds the survivors' history and
/// the write the old primary refused is nowhere.
#[tokio::test(flavor = "multi_thread")]
async fn delayed_topology_cannot_authorize_old_primary() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let keys = keys_led_by(&mut cluster, "Note", 0, 1000, 2)?;
    let (key, other) = (keys[0], keys[1]);
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    write_note(&addrs[0], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // the data lanes round node zero go, the control lanes stay and slow down
    for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
        cluster.data_link(from, to).cut();
    }
    for link in cluster.control_links_into(0) {
        link.delay(Duration::from_millis(1500));
    }
    let cut_at = std::time::Instant::now();
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    write_note_eventually(&addrs[leader], key, "v2", Duration::from_secs(30)).await?;
    // the control plane still calls node zero up: nothing it commits is a data election
    assert_eq!(health_of(&mut cluster, 1, 0)?, "up");
    // past its lease - two failover bases - the old primary refuses a write by name, before
    // appending anything
    let lease = Duration::from_millis(2500);
    if let Some(left) = lease.checked_sub(cut_at.elapsed()) {
        tokio::time::sleep(left).await;
    }
    let refused = write_note(&addrs[0], other, "v3").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::NotLeader), "the old primary answered {refused:?}");
    // and cannot pass a fresh read barrier
    let stale = read_note_with(&addrs[0], key, &quorum).await;
    assert!(
        matches!(failure_code(&stale), Some(ErrorCode::QuorumUnavailable | ErrorCode::Timeout | ErrorCode::NotLeader)),
        "a strong read through the old primary answered {stale:?}"
    );
    // the data lanes back, the control lanes cut instead: writes still commit through everybody
    for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
        cluster.data_link(from, to).heal();
    }
    wait_note(&addrs[0], key, Some("v2"), Duration::from_secs(30)).await?;
    for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
        cluster.control_link(from, to).cut();
    }
    write_note_eventually(&addrs[0], key, "v4", Duration::from_secs(30)).await?;
    write_note_eventually(&addrs[1], key, "v5", Duration::from_secs(30)).await?;
    for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
        cluster.control_link(from, to).heal();
    }
    for link in cluster.control_links_into(0) {
        link.heal();
    }
    // everybody holds the survivors' history, and the refused write is nowhere
    for addr in &addrs {
        wait_note(addr, key, Some("v5"), Duration::from_secs(30)).await?;
        wait_note(addr, other, None, Duration::from_secs(10)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A stalled data shard fails over while its control thread stays alive (C7 M6, F42)
///
/// Node zero leads a key's group. Its one shard's executor is blocked for six seconds, so
/// every group on it stops heartbeating while the control thread keeps reporting and the
/// control plane keeps calling the node `Up`: `Down` is not what an election waits for. The
/// survivors elect within the stall and a write through the new leader commits. When the
/// shard runs again it hears the higher term, follows, and converges.
#[tokio::test(flavor = "multi_thread")]
async fn shard_stall_with_live_control_plane_can_fail_over() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let (key, _) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    write_note(&addrs[0], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // the leader's shard stops running; its control thread does not
    let stalled = cluster.node_mut(0).command("STALL_SHARD 0 6000")?;
    assert_eq!(stalled["ok"]["stalling"], true, "{stalled}");
    let stalled_at = std::time::Instant::now();
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    assert!(stalled_at.elapsed() < Duration::from_secs(6), "the election waited for the stall to end");
    write_note_eventually(&addrs[leader], key, "v2", Duration::from_secs(30)).await?;
    // the node was never called down: the shard's silence is not the node's
    assert_eq!(health_of(&mut cluster, 1, 0)?, "up");
    assert_eq!(health_of(&mut cluster, leader, 0)?, "up");
    // the shard runs again, follows, and converges
    if let Some(left) = Duration::from_millis(6500).checked_sub(stalled_at.elapsed()) {
        tokio::time::sleep(left).await;
    }
    wait_note(&addrs[0], key, Some("v2"), Duration::from_secs(30)).await?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let (_, led) = wait_group_leader_via(&mut cluster, 1, "Note", key)?;
    assert_ne!(led, 0, "leadership went back to the stalled shard on its own");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// An isolated old primary cannot pass a fresh read barrier, and refuses writes at its lease (C7 M6, F42)
///
/// Node zero leads a key's group and is isolated on every lane. The survivors elect and
/// commit a new value. A strong read through node zero is refused - `QuorumUnavailable` once
/// its lease lapsed, `Timeout` before - and never the old value; a `One` read through it is
/// the old value, which is what `One` promises; past the lease a write through it is
/// `NotLeader`. Healed, it hears the higher term and a strong read through it hops to the new
/// leader and sees the new value.
#[tokio::test(flavor = "multi_thread")]
async fn strong_read_refuses_isolated_old_primary() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let keys = keys_led_by(&mut cluster, "Note", 0, 1000, 2)?;
    let (key, other) = (keys[0], keys[1]);
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    let one = SendOptions::new().read(ReadLevel::One);
    write_note(&addrs[0], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    cluster.isolate(0);
    let isolated_at = std::time::Instant::now();
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    write_note_eventually(&addrs[leader], key, "v2", Duration::from_secs(30)).await?;
    // a strong read through the old primary is refused, never stale
    let stale = read_note_with(&addrs[0], key, &quorum).await;
    assert!(
        matches!(failure_code(&stale), Some(ErrorCode::QuorumUnavailable | ErrorCode::Timeout | ErrorCode::NotLeader)),
        "a strong read through the isolated primary answered {stale:?}"
    );
    // a One read through it is the old committed value
    assert_eq!(read_note_with(&addrs[0], key, &one).await?.as_deref(), Some("v1"));
    // past its lease: a write is refused by name, and a strong read at once
    if let Some(left) = Duration::from_millis(2500).checked_sub(isolated_at.elapsed()) {
        tokio::time::sleep(left).await;
    }
    let refused = write_note(&addrs[0], other, "v3").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::NotLeader), "the isolated primary answered {refused:?}");
    let started = std::time::Instant::now();
    let lapsed = read_note_with(&addrs[0], key, &quorum).await;
    assert_eq!(failure_code(&lapsed), Some(ErrorCode::QuorumUnavailable), "{lapsed:?}");
    assert!(started.elapsed() < Duration::from_millis(1500), "a lapsed lease was waited out rather than answered");
    // healed, the old primary hears the term and a strong read through it hops
    cluster.heal(0);
    let hops_before = read_stats(&mut cluster, 0)?["stats"]["barrier_hops"].as_u64().unwrap_or(0);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match read_note_with(&addrs[0], key, &quorum).await {
            Ok(Some(text)) if text == "v2" => break,
            Ok(other) => panic!("a strong read through the healed primary was stale: {other:?}"),
            Err(error) => {
                assert!(std::time::Instant::now() < deadline, "a strong read never succeeded after the heal: {error:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    let hops = read_stats(&mut cluster, 0)?["stats"]["barrier_hops"].as_u64().unwrap_or(0);
    assert!(hops > hops_before, "the strong read through the old primary did not hop to the leader");
    wait_note(&addrs[0], other, None, Duration::from_secs(10)).await?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A minority never commits, and healing restores progress with the acknowledged history intact (C7 M6, F42)
///
/// Eight keys are written and converge. Two of three nodes are killed. A write through the
/// survivor is not acknowledged - `OutcomeUnknown` at its deadline while its lease lasts,
/// `NotLeader` after - and a strong read through it is refused; the control plane has no
/// quorum either, so nobody is called `Down` and admission is not what refuses the write. Both
/// nodes restarted, a new write commits, every acknowledged key is on every node, and the key
/// the unknown write named holds the same value everywhere.
#[tokio::test(flavor = "multi_thread")]
async fn quorum_loss_is_unavailable_without_data_loss() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let keys = keys_led_by(&mut cluster, "Note", 0, 1000, 8)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    for key in &keys {
        write_note(&addrs[0], *key, "v1").await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the majority dies
    cluster.kill(1)?;
    cluster.kill(2)?;
    // a write through the survivor is never acknowledged
    let unknown = write_note(&addrs[0], keys[0], "v2").await;
    assert!(
        matches!(failure_code(&unknown), Some(ErrorCode::OutcomeUnknown | ErrorCode::NotLeader | ErrorCode::QuorumUnavailable)),
        "a write with the majority dead answered {unknown:?}"
    );
    // and a strong read through it cannot be served
    let refused = read_note_with(&addrs[0], keys[1], &quorum).await;
    assert!(
        matches!(failure_code(&refused), Some(ErrorCode::QuorumUnavailable | ErrorCode::Timeout | ErrorCode::NotLeader)),
        "a strong read with the majority dead answered {refused:?}"
    );
    // nobody was called down: a minority commits nothing, on either plane
    assert_eq!(health_of(&mut cluster, 0, 1)?, "up");
    assert_eq!(health_of(&mut cluster, 0, 2)?, "up");
    // the majority comes back, and progress with it
    cluster.restart(1, NodeKind::Server)?;
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[1, 2])?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    write_note_eventually(&addrs[0], keys[1], "v3", Duration::from_secs(30)).await?;
    for addr in &addrs {
        wait_note(addr, keys[1], Some("v3"), Duration::from_secs(30)).await?;
        for key in &keys[2..] {
            wait_note(addr, *key, Some("v1"), Duration::from_secs(30)).await?;
        }
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the unknown write is everywhere or nowhere, never somewhere
    let mut seen = Vec::new();
    for addr in &addrs {
        seen.push(read_note_with(addr, keys[0], &quorum).await?);
    }
    assert!(seen.iter().all(|value| *value == seen[0]), "the unknown write diverged: {seen:?}");
    assert!(matches!(seen[0].as_deref(), Some("v1" | "v2")), "{seen:?}");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A paused leader that resumes cannot authorize a stale strong read (C6 M6, F42)
///
/// Node zero leads a key's group and is paused with SIGSTOP. The survivors elect and commit a
/// new value; the control lanes into node zero are slowed so what it hears about the world is
/// late. Resumed, node zero still believes it leads: its lease lapsed while it slept, so its
/// own barrier is refused at once, and once the new leader's higher term reaches it the
/// barrier hops and sees the new value. No strong read through it is ever the old value.
#[tokio::test(flavor = "multi_thread")]
async fn read_barrier_survives_leader_change_and_delayed_messages() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let (key, _) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    write_note(&addrs[0], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    assert_eq!(read_note_with(&addrs[0], key, &quorum).await?.as_deref(), Some("v1"));
    let hops_before = read_stats(&mut cluster, 0)?["stats"]["barrier_hops"].as_u64().unwrap_or(0);
    // the leader sleeps; the others elect and move on
    cluster.node(0).pause().map_err(|error| FixtureError::ChildFailed(format!("pausing: {error}")))?;
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    write_note_eventually(&addrs[leader], key, "v2", Duration::from_secs(30)).await?;
    for link in cluster.control_links_into(0) {
        link.delay(Duration::from_secs(1));
    }
    cluster.node(0).resume().map_err(|error| FixtureError::ChildFailed(format!("resuming: {error}")))?;
    // every strong read through the old leader is the new value or a refusal, never the old
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut refusals = 0;
    loop {
        match read_note_with(&addrs[0], key, &quorum).await {
            Ok(Some(text)) if text == "v2" => break,
            Ok(other) => panic!("a strong read through the resumed leader was stale: {other:?}"),
            Err(error) => {
                refusals += 1;
                assert!(std::time::Instant::now() < deadline, "a strong read never succeeded after the resume: {error:?}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    // and a few more, now that it knows: the new value or a refusal under load, never the old
    for _ in 0..5 {
        match read_note_with(&addrs[0], key, &quorum).await {
            Ok(seen) => assert_eq!(seen.as_deref(), Some("v2")),
            Err(error) => assert!(failure_code::<()>(&Err(error)).is_some(), "a strong read failed off the wire"),
        }
    }
    let hops = read_stats(&mut cluster, 0)?["stats"]["barrier_hops"].as_u64().unwrap_or(0);
    assert!(hops > hops_before, "the strong reads through the old leader never hopped ({refusals} refusals)");
    for link in cluster.control_links_into(0) {
        link.heal();
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A member called `Down` keeps its placement and its group memberships through the grace (C3 M6, F42)
///
/// Three nodes at a factor of three. Node two is killed and the leader's detector calls it
/// `Down` with an episode: the placement and every group's members are exactly what they
/// were, on every survivor - an election may move a tablet's leader, and nothing moves a
/// replica before the removal M9b delivers. A key node two led is written through node zero
/// once its group elected. Restarted, node two is `Up` again in the same placement and
/// converges.
#[tokio::test(flavor = "multi_thread")]
async fn down_retains_placement_during_grace() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .detector_interval_ms(200)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let (key, group) = key_led_by(&mut cluster, "Note", 2, 1000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    write_note(&addrs[2], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // what the placement and every group's members are, as nodes zero and one see them
    let placement_of = |cluster: &mut Cluster, at: usize| -> Result<serde_json::Value, FixtureError> {
        Ok(cluster.node_mut(at).command("MAP")?["ok"]["placement"].clone())
    };
    let members_of = |cluster: &mut Cluster, at: usize| -> Result<std::collections::BTreeMap<u64, serde_json::Value>, FixtureError> {
        let view = groups_of(cluster, at)?;
        let mut members = std::collections::BTreeMap::new();
        for shard in view["shards"].as_array().into_iter().flatten() {
            for group in shard["groups"].as_array().into_iter().flatten() {
                members.insert(group["group"].as_u64().unwrap_or_default(), group["members"].clone());
            }
        }
        Ok(members)
    };
    let placement_before = placement_of(&mut cluster, 0)?;
    assert_eq!(placement_before.as_array().map(Vec::len), Some(3), "{placement_before}");
    let members_before: Vec<_> = (0..2).map(|at| members_of(&mut cluster, at)).collect::<Result<_, _>>()?;
    assert!(!members_before[0].is_empty());
    // node two dies, and the leader calls it down with an episode
    cluster.kill(2)?;
    let victim = cluster.node_ids()[2].clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let members = cluster.members(0)?;
        let member = members["members"]
            .as_array()
            .and_then(|members| members.iter().find(|m| m["record"]["node"] == victim).cloned())
            .unwrap_or_default();
        if member["health"] == "down" {
            assert!(!member["episode"].is_null(), "a down verdict without an episode: {member}");
            break;
        }
        if std::time::Instant::now() > deadline {
            let request = serde_json::json!({ "op": uuid::Uuid::new_v4(), "expected_version": 0, "kind": "Detector" });
            let detector = cluster.node_mut(0).command(&format!("ADMIN {request}"))?;
            panic!("the dead member was never called down: {members}\ndetector: {detector}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // nothing moved: the placement and every group's members are what they were
    assert_eq!(placement_of(&mut cluster, 0)?, placement_before);
    assert_eq!(placement_of(&mut cluster, 1)?, placement_before);
    for at in 0..2 {
        assert_eq!(members_of(&mut cluster, at)?, members_before[at], "node {at}'s groups moved");
    }
    // the key's group elected another leader, and a write through zero lands
    let (elected, leader) = wait_group_leader_change(&mut cluster, 0, "Note", key, 2)?;
    assert_eq!(elected, group);
    assert_ne!(leader, 2);
    write_note_eventually(&addrs[0], key, "v2", Duration::from_secs(30)).await?;
    // back: up again, in the same placement, converged
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if health_of(&mut cluster, 0, 2)? == "up" && health_of(&mut cluster, 1, 2)? == "up" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the restarted member was never called up");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(placement_of(&mut cluster, 2)?, placement_before);
    assert_eq!(members_of(&mut cluster, 2)?, members_before[0]);
    let addr2 = cluster.node(2).endpoints.client.to_string();
    wait_note(&addr2, key, Some("v2"), Duration::from_secs(30)).await?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A control majority cannot activate a tablet minority (C13 M6, F42)
///
/// Node zero leads a key's group. Its data lanes are cut both ways while every control lane
/// stays up, so the control plane - all three voters, node zero among them - keeps committing:
/// a table policy change moves the map's version on every node, node zero included. That
/// commit authorizes nothing: past its lease a write through node zero is `NotLeader`, a
/// strong read through it is refused, and the survivors elect and commit through the new
/// leader. Healed, node zero holds the majority's history and the write it refused is
/// nowhere ([P5](../../docs/src/distributed/protocol.md)).
#[tokio::test(flavor = "multi_thread")]
async fn metadata_quorum_cannot_replace_a_missing_data_quorum() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let keys = keys_led_by(&mut cluster, "Note", 0, 1000, 2)?;
    let (key, other) = (keys[0], keys[1]);
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    write_note(&addrs[0], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // the data lanes round node zero go; the control lanes stay
    for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
        cluster.data_link(from, to).cut();
    }
    let cut_at = std::time::Instant::now();
    // the control plane commits a change and every node installs it, node zero included
    let set = cluster.node_mut(1).command("SET_TABLE_READ_POLICY Note quorum")?;
    let version = set["ok"]["version"].as_u64().unwrap_or_else(|| panic!("{set}"));
    cluster.wait_map_version(&[0, 1, 2], version)?;
    assert_eq!(health_of(&mut cluster, 1, 0)?, "up");
    // the survivors elect and commit
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    write_note_eventually(&addrs[leader], key, "v2", Duration::from_secs(30)).await?;
    write_note_eventually(&addrs[3 - leader], key, "v3", Duration::from_secs(30)).await?;
    // the committed map moved node zero's authority not at all
    if let Some(left) = Duration::from_millis(2500).checked_sub(cut_at.elapsed()) {
        tokio::time::sleep(left).await;
    }
    let refused = write_note(&addrs[0], other, "v4").await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::NotLeader), "the cut primary answered {refused:?}");
    let stale = read_note_with(&addrs[0], key, &quorum).await;
    assert!(
        matches!(failure_code(&stale), Some(ErrorCode::QuorumUnavailable | ErrorCode::Timeout | ErrorCode::NotLeader)),
        "a strong read through the cut primary answered {stale:?}"
    );
    // healed: the majority's history everywhere, the refused write nowhere
    for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
        cluster.data_link(from, to).heal();
    }
    let cleared = cluster.node_mut(1).command("SET_TABLE_READ_POLICY Note clear")?;
    let version = cleared["ok"]["version"].as_u64().unwrap_or_else(|| panic!("{cleared}"));
    cluster.wait_map_version(&[0, 1, 2], version)?;
    for addr in &addrs {
        wait_note(addr, key, Some("v3"), Duration::from_secs(30)).await?;
        wait_note(addr, other, None, Duration::from_secs(10)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Established tablet groups progress without a control quorum, and metadata stops (C13 M6, F42)
///
/// Every control lane is cut, so the control group has no quorum: an admin mutation through a
/// follower is refused by name. The data lanes are whole, so writes and strong reads through
/// every node still commit - a tablet's authority is its own group's, and the map each shard
/// holds is a perfectly good map while it is frozen. Healed, a control leader is elected and
/// the same mutation commits ([P5](../../docs/src/distributed/protocol.md)).
#[tokio::test(flavor = "multi_thread")]
async fn established_tablets_survive_control_quorum_loss() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let keys = keys_led_by(&mut cluster, "Note", 0, 1000, 3)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    for key in &keys {
        write_note(&addrs[0], *key, "v1").await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let control_leader = cluster.wait_leader_among(0, &[0, 1, 2], Duration::from_secs(30))?;
    let follower = (control_leader + 1) % 3;
    // every control lane goes
    for from in 0..3 {
        for to in 0..3 {
            if from != to {
                cluster.control_link(from, to).cut();
            }
        }
    }
    // the control group's lease runs out, and a mutation through a follower is refused
    tokio::time::sleep(Duration::from_secs(2)).await;
    let refused = cluster.node_mut(follower).command("SET_TABLE_READ_POLICY Note quorum")?;
    let error = refused["error"].as_str().unwrap_or_default().to_string();
    assert!(
        error.contains("NotLeader") || error.contains("leader") || error.contains("quorum"),
        "the mutation without a control quorum answered {refused}"
    );
    // the data plane is whole: writes and strong reads through every node
    for (node, key) in keys.iter().enumerate() {
        write_note_eventually(&addrs[node], *key, "v2", Duration::from_secs(30)).await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            match read_note_with(&addrs[(node + 1) % 3], *key, &quorum).await {
                Ok(seen) => {
                    assert_eq!(seen.as_deref(), Some("v2"), "a strong read without a control quorum was stale");
                    break;
                }
                Err(error) => {
                    assert!(failure_code::<()>(&Err(error)).is_some(), "a strong read failed off the wire");
                    assert!(std::time::Instant::now() < deadline, "a strong read never succeeded without a control quorum");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
    // healed: a control leader, and the mutation commits
    for from in 0..3 {
        for to in 0..3 {
            if from != to {
                cluster.control_link(from, to).heal();
            }
        }
    }
    cluster.wait_leader_among(0, &[0, 1, 2], Duration::from_secs(30))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let version = loop {
        let set = cluster.node_mut(0).command("SET_TABLE_READ_POLICY Note quorum")?;
        if let Some(version) = set["ok"]["version"].as_u64() {
            break version;
        }
        assert!(std::time::Instant::now() < deadline, "the mutation never committed after the heal: {set}");
        std::thread::sleep(Duration::from_millis(200));
    };
    cluster.wait_map_version(&[0, 1, 2], version)?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A lost reply is recovered by a retry under the same identity, which returns the original result once (C5 M6, F42)
///
/// A note is written. Its delete is sent under an identity with the reply dropped on the
/// leader, so the client sees only its own deadline: the outcome is unknown. The leader is
/// killed, and the same delete under the same identity through a survivor is answered with the
/// original result - the note was there and is gone - rather than as a delete of nothing; a
/// fresh delete finds nothing. Then the entry is checkpointed and purged on every node, every
/// node restarted, and the same identity is answered the same way again: the retry table
/// came from the sidecar beside the checkpoint, since the log no longer held it.
#[tokio::test(flavor = "multi_thread")]
async fn lost_response_retry_returns_original_result() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    write_note(&addrs[0], key, "v1").await?;
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // the delete commits and applies, and its reply is dropped: the client times out
    let identity = uuid::Uuid::new_v4();
    let dropped = cluster.node_mut(0).command("DROP_REPLIES 1")?;
    assert!(dropped.get("ok").is_some(), "{dropped}");
    let started = std::time::Instant::now();
    let lost = delete_note_as(&addrs[0], key, &SendOptions::new().identity(identity).deadline(Duration::from_secs(1))).await;
    assert_eq!(failure_code(&lost), Some(ErrorCode::Timeout), "the dropped reply was answered {lost:?}");
    assert!(started.elapsed() < Duration::from_secs(3), "the client waited past its deadline and slack");
    // the delete happened: every node has no note
    for addr in &addrs {
        wait_note(addr, key, None, Duration::from_secs(10)).await?;
    }
    // the leader dies; the same identity through a survivor is the original result
    cluster.kill(0)?;
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    let again = delete_note_as(&addrs[leader], key, &SendOptions::new().identity(identity).retry(Duration::from_secs(15))).await;
    let again = again.unwrap_or_else(|error| panic!("the retry under the same identity was not the original result: {error:?}"));
    assert!(again.attempts() >= 1);
    assert_eq!(again.bundle(), identity);
    let token = again.session_token().expect("a duplicate answers with a token");
    // and a delete of its own finds nothing
    let fresh = delete_note(&addrs[leader], key).await;
    assert!(matches!(fresh, Err(shoal::client::Errors::QueryDidNotSucceed { .. })), "{fresh:?}");
    // the entry goes below every survivor's checkpoint, node zero comes back and catches up,
    // and every node restarts: the retry table is what the sidecar held
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_checkpoint_past(&mut cluster, &[0, 1, 2], &group, token.index, Duration::from_secs(60))?;
    for id in 0..3 {
        cluster.restart(id, NodeKind::Server)?;
    }
    cluster.wait_joined(&[0, 1, 2])?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let (_, leader) = wait_group_leader_via(&mut cluster, 0, "Note", key)?;
    let restored = delete_note_as(&addrs[leader], key, &SendOptions::new().identity(identity).retry(Duration::from_secs(15))).await;
    assert!(restored.is_ok(), "after a checkpoint and a restart the identity was applied as new: {restored:?}");
    let fresh = delete_note(&addrs[leader], key).await;
    assert!(matches!(fresh, Err(shoal::client::Errors::QueryDidNotSucceed { .. })), "{fresh:?}");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A session token is served past its lower bound on a behind replica, and through a leader change (C6 M6, F42)
///
/// A write through the leader hands back a token; the lane to one follower is cut and a
/// second write hands back another. A session read through the cut follower past the second
/// token waits for an apply that cannot come and times out; healed, it is served. The leader
/// is killed: the token names the group and an index, neither of which an election changes,
/// so a session read through either survivor is served past it, a write through the new
/// leader mints a token the other survivor serves past, and a token forged to another lineage
/// is still refused by name.
#[tokio::test(flavor = "multi_thread")]
async fn session_read_waits_for_committed_lower_bound() -> Result<(), FixtureError> {
    use shoal::client::{ReadLevel, SendOptions};
    use shoal::shared::identity::GroupId;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let (key, _) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let first = write_note_token(&addrs[0], key, "v1").await?.expect("a committed write mints a token");
    for addr in &addrs {
        wait_note(addr, key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // node two falls behind: a session read past the new write waits, and times out
    cluster.data_link(0, 2).cut();
    cluster.data_link(2, 0).cut();
    let second = write_note_token(&addrs[0], key, "v2").await?.expect("a committed write mints a token");
    assert!(second.index > first.index);
    let behind = read_note_with(&addrs[2], key, &SendOptions::new().read(ReadLevel::One).token(second).deadline(Duration::from_secs(1))).await;
    assert_eq!(failure_code(&behind), Some(ErrorCode::Timeout), "a session read on a behind replica answered {behind:?}");
    // healed, the same read is served past the bound
    cluster.data_link(0, 2).heal();
    cluster.data_link(2, 0).heal();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match read_note_with(&addrs[2], key, &SendOptions::new().read(ReadLevel::One).token(second)).await {
            Ok(Some(text)) if text == "v2" => break,
            Ok(other) => panic!("a session read was served before its bound: {other:?}"),
            Err(error) => {
                assert!(std::time::Instant::now() < deadline, "the session read never succeeded after the heal: {error:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    // the leader dies: the token outlives it
    cluster.kill(0)?;
    let (_, leader) = wait_group_leader_change(&mut cluster, 1, "Note", key, 0)?;
    let other = 3 - leader;
    for reader in [leader, other] {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match read_note_with(&addrs[reader], key, &SendOptions::new().read(ReadLevel::One).token(second)).await {
                Ok(Some(text)) if text == "v2" => break,
                Ok(seen) => panic!("a session read through node {reader} was served before its bound: {seen:?}"),
                Err(error) => {
                    assert!(std::time::Instant::now() < deadline, "the session read through node {reader} never succeeded: {error:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
    // a write through the new leader mints a token the other survivor serves past
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let third = loop {
        match write_note_token(&addrs[leader], key, "v3").await {
            Ok(Some(token)) => break token,
            Ok(None) => panic!("a committed write minted no token"),
            Err(error) => {
                assert!(std::time::Instant::now() < deadline, "the write through the new leader never landed: {error:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    assert_eq!(third.group, second.group, "an election moved the lineage");
    assert!(third.index > second.index);
    assert_eq!(
        read_note_with(&addrs[other], key, &SendOptions::new().read(ReadLevel::One).token(third)).await?.as_deref(),
        Some("v3")
    );
    // and a token from another lineage is refused by name, still
    let mut forged = third;
    forged.group = GroupId(third.group.0 ^ 0xdead_beef);
    let refused = read_note_with(&addrs[other], key, &SendOptions::new().read(ReadLevel::One).token(forged)).await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::UnknownLineage), "{refused:?}");
    for id in 1..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A forwarded write keeps its identity and its budget through a redirect, and is sent to another holder when the link is down (C2 M6, F42)
///
/// Four nodes at a factor of three, so the fourth holds no copy of a quarter of the tablets
/// and forwards their writes. A delete under one identity through the non-holder and then
/// through a holder is answered with the original result once and never applied twice. A
/// write through the non-holder while the group cannot commit is answered at the bundle's own
/// deadline rather than the server's proposal deadline: the budget counted down across the
/// hop. And with the lane to the primary cut, a write through the non-holder is sent to
/// another holder that is up, under the same attempt, and lands once.
#[tokio::test(flavor = "multi_thread")]
async fn deadline_and_operation_id_survive_forwarding() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(4, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let addrs: Vec<String> = (0..4).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let ids = cluster.node_ids();
    // the placement, and a key the fourth node holds no copy of
    let map = cluster.node_mut(0).command("MAP")?;
    let placement: Vec<String> = map["ok"]["placement"]
        .as_array()
        .expect("a placement")
        .iter()
        .map(|id| id.as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(placement.len(), 4, "{map}");
    let index_of = |id: &str| ids.iter().position(|node| node == id).expect("a placed node");
    let mut keys = Vec::new();
    for key in 1000u64.. {
        let tablet = tablet_of(key);
        let non_holder = index_of(&placement[(tablet + 3) % 4]);
        if non_holder == 3 {
            keys.push(key);
            if keys.len() == 3 {
                break;
            }
        }
    }
    let (key, budget_key, reroute_key) = (keys[0], keys[1], keys[2]);
    let tablet = tablet_of(key);
    let primary = index_of(&placement[tablet % 4]);
    let holders: Vec<usize> = (1..3).map(|k| index_of(&placement[(tablet + k) % 4])).collect();
    // the write is forwarded from the non-holder and lands on the holders
    write_note(&addrs[3], key, "v1").await?;
    for holder in std::iter::once(&primary).chain(holders.iter()) {
        wait_note(&addrs[*holder], key, Some("v1"), Duration::from_secs(10)).await?;
    }
    // one identity, through the non-holder and then through a holder: the original result once
    let identity = uuid::Uuid::new_v4();
    let first = delete_note_as(&addrs[3], key, &SendOptions::new().identity(identity)).await;
    assert!(first.is_ok(), "the forwarded delete answered {first:?}");
    let again = delete_note_as(&addrs[holders[0]], key, &SendOptions::new().identity(identity)).await;
    assert!(again.is_ok(), "the same identity through a holder answered {again:?}");
    let fresh = delete_note(&addrs[holders[0]], key).await;
    assert!(matches!(fresh, Err(shoal::client::Errors::QueryDidNotSucceed { .. })), "{fresh:?}");
    // the budget counts down across the hop: with the group unable to commit, a write through
    // the non-holder is answered at its own deadline, not the server's proposal deadline
    let (group, _) = group_of(&mut cluster, primary, "Note", budget_key)?;
    assert_eq!(group_of(&mut cluster, primary, "Note", key)?.0, group, "the keys are on two groups");
    for holder in &holders {
        let _ = cluster.node_mut(*holder).command(&format!("STALL_WAL {group}"))?;
    }
    let started = std::time::Instant::now();
    let stalled = write_note_as(&addrs[3], budget_key, "v", &SendOptions::new().deadline(Duration::from_millis(500))).await;
    let elapsed = started.elapsed();
    assert!(
        matches!(failure_code(&stalled), Some(ErrorCode::OutcomeUnknown | ErrorCode::Timeout)),
        "a write that could not commit answered {stalled:?}"
    );
    assert!(elapsed < Duration::from_millis(2500), "the forwarded write waited {elapsed:?}, past its bundle's budget");
    for holder in &holders {
        let _ = cluster.node_mut(*holder).command(&format!("RELEASE_WAL {group}"))?;
    }
    // the lane to the primary is down: the forward the link never wrote goes to another holder
    let before = read_stats(&mut cluster, 3)?["stats"]["reroutes"].as_u64().unwrap_or(0);
    cluster.data_link(3, primary).cut();
    cluster.data_link(primary, 3).cut();
    let rerouted = write_note_as(
        &addrs[3],
        reroute_key,
        "v",
        &SendOptions::new().identity(uuid::Uuid::new_v4()).retry(Duration::from_secs(15)),
    )
    .await;
    assert!(rerouted.is_ok(), "a write through the non-holder with the primary's lane cut answered {rerouted:?}");
    let reroutes = read_stats(&mut cluster, 3)?["stats"]["reroutes"].as_u64().unwrap_or(0);
    assert!(reroutes > before, "the forward was not sent to another holder");
    for holder in &holders {
        wait_note(&addrs[*holder], reroute_key, Some("v"), Duration::from_secs(10)).await?;
    }
    cluster.data_link(3, primary).heal();
    cluster.data_link(primary, 3).heal();
    wait_digests_equal(&mut cluster, &[primary, holders[0], holders[1]], "Note", Duration::from_secs(30))?;
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Every acknowledged result survives repeated elections and lost replies (C7 M6, F42)
///
/// The oracle-driven mix of updates, deletes and no-ops through all three nodes, every
/// operation under an identity of its own with a retry budget, while the leader of the hot
/// group has its replies dropped, is killed, restarted, and the next leader is killed and
/// restarted in turn. The sequential oracle accepts the history - a retry answered with its
/// first result is one operation, an unknown outcome may have happened or not - and a read of
/// every key on every node joins it and is accepted too.
#[tokio::test(flavor = "multi_thread")]
async fn quorum_history_survives_repeated_elections() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .detector_interval_ms(200)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    // six keys on one group, so the chaos below is aimed at the group they all live on
    let (hot, group) = key_led_by(&mut cluster, "Note", 0, 1000)?;
    let keys = keys_in_group(&mut cluster, "Note", &group, hot, 6)?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    // every key inserted once, before anything concurrent
    for key in &keys {
        let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
        let attempt = Attempt { id, retry: 0 };
        let op = ClientOp::Mutate(MutationOp::Insert { key: Key((*key % 251) as u8), value: Value(0) });
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().invoke(attempt, tablet_id(*key), op, invoke);
        write_note(&addrs[0], *key, "0").await?;
        let complete = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Applied(true)));
    }
    // updates and deletes through every node at once, each under an identity with a budget
    let mut tasks = Vec::new();
    for node in 0..3 {
        let endpoints = addrs.clone();
        let keys = keys.clone();
        let ledger = ledger.clone();
        let clock = clock.clone();
        let next_id = next_id.clone();
        tasks.push(tokio::spawn(async move {
            // a client over every node, so a killed one is routed around
            let mut ordered = endpoints.clone();
            ordered.rotate_left(node);
            let client = Shoal::<TestDbClient>::builder().endpoints(ordered).build().await?;
            for round in 0..8u32 {
                for (at, key) in keys.iter().enumerate() {
                    let value = Value(node as u32 * 100 + round + 1);
                    let delete = (round as usize + at + node) % 4 == 0;
                    let op = if delete {
                        MutationOp::Delete { key: Key((*key % 251) as u8) }
                    } else {
                        MutationOp::Update { key: Key((*key % 251) as u8), value }
                    };
                    let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
                    let attempt = Attempt { id, retry: 0 };
                    let invoke = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().invoke(attempt, TabletId {
                        table: shoal_model::ids::TableId(1),
                        range: tablet_of(*key) as u16,
                    }, ClientOp::Mutate(op), invoke);
                    let options = SendOptions::new().identity(uuid::Uuid::new_v4()).retry(Duration::from_secs(20));
                    let outcome = if delete {
                        match client.send_one_with(cluster::schema::NoteDelete::new(*key), &options).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(_) => Outcome::Unknown,
                        }
                    } else {
                        let update = cluster::schema::NoteUpdate {
                            partition_key: *key,
                            text: Some(value.0.to_string()),
                        };
                        match client.send_one_with(update, &options).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(_) => Outcome::Unknown,
                        }
                    };
                    let complete = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().complete(attempt, complete, outcome);
                }
            }
            Ok::<(), shoal::client::Errors>(())
        }));
    }
    // meanwhile: replies dropped on the leader, the leader killed and restarted, twice over
    let mut leader = 0;
    for _ in 0..2 {
        std::thread::sleep(Duration::from_millis(500));
        let _ = cluster.node_mut(leader).command("DROP_REPLIES 3")?;
        std::thread::sleep(Duration::from_millis(300));
        cluster.kill(leader)?;
        let via = (leader + 1) % 3;
        let (_, elected) = wait_group_leader_change(&mut cluster, via, "Note", hot, leader)?;
        cluster.restart(leader, NodeKind::Server)?;
        cluster.wait_joined(&[leader])?;
        leader = elected;
    }
    for task in tasks {
        task.await.expect("a writer task panicked")?;
    }
    // the replicas converge, and a read of every key on every node joins the ledger
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    for addr in &addrs {
        for key in &keys {
            let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
            let attempt = Attempt { id, retry: 0 };
            let invoke = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().invoke(
                attempt,
                tablet_id(*key),
                ClientOp::Read {
                    key: Key((*key % 251) as u8),
                    level: ReadLevel::One,
                },
                invoke,
            );
            let seen = read_note(addr, *key).await?.map(|text| Value(text.parse().expect("a value")));
            let complete = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Value(seen)));
        }
    }
    let ledger = ledger.lock().unwrap().clone();
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}
