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
use std::time::{Duration, Instant};

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
        reason.contains("export_standalone") && reason.contains(&standalone_node),
        "the refusal did not name the migration and the node: {reason}"
    );
    // a peer from another cluster is refused by the identity, and the marker is untouched
    //
    // the claim itself counts one more start of the directory and writes that down, so the
    // bytes held to are the ones after it; a refusal is what must not move them
    let identity = StorageMeta::claim(cluster.dir(0), marker.shards, None, ClusterIntent::Bootstrap)
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
    // that a marker is never migrated in place ([F48](../../docs/src/features/rolling-compatibility.md))
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
    assert!(reason.contains("never migrated"), "the refusal did not say a marker is never migrated in place: {reason}");
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
            // the slots this node claims apart from its cores ([F47](../../docs/src/features/local-rehome.md))
            block = block.slots(staged.slots);
            // the wire version this node is pinned at, as an unupgraded member
            // ([F48](../../docs/src/features/rolling-compatibility.md))
            block.transport.wire_version = staged.wire_version;
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
            if let Some(bytes) = staged.snapshot_chunk_bytes {
                replication.snapshot_chunk_bytes = bytes;
            }
            if let Some(ms) = staged.snapshot_timeout_ms {
                replication.snapshot_timeout = Duration::from_millis(ms).into();
            }
            // the retry window ([F45](../../docs/src/features/replica-migration.md))
            if let Some(ms) = staged.retry_window_ms {
                replication.retry_window = Duration::from_millis(ms).into();
            }
            block = block.replication(replication);
            if let Some(bytes) = staged.bulk_queue_bytes {
                block.transport.bulk_queue_bytes = bytes;
            }
            // the scrub schedule and deadline ([F44](../../docs/src/features/repair.md))
            if let Some(ms) = staged.repair_timeout_ms {
                block.repair.timeout = Duration::from_millis(ms).into();
            }
            if let Some(ms) = staged.scrub_interval_ms {
                block.repair.scrub_interval = Some(Duration::from_millis(ms).into());
            }
            // the move's lag, deadline and grace ([F45](../../docs/src/features/replica-migration.md))
            if let Some(ms) = staged.retire_after_ms {
                block.migration.retire_after = Duration::from_millis(ms).into();
            }
            if let Some(lag) = staged.catchup_lag {
                block.migration.catchup_lag = lag;
            }
            if let Some(ms) = staged.migration_timeout_ms {
                block.migration.timeout = Duration::from_millis(ms).into();
            }
            // the grace, the weight, the budgets and the plan knobs
            // ([F46](../../docs/src/features/capacity-rebalancing.md))
            if let Some(ms) = staged.auto_remove_after_ms {
                block = block.auto_remove_after((ms > 0).then(|| Duration::from_millis(ms)));
            }
            if let Some(weight) = staged.weight {
                block = block.weight(Some(weight));
            }
            if let Some(bytes) = staged.stream_bytes_per_sec {
                block.migration.stream_bytes_per_sec = bytes;
            }
            if let Some(streams) = staged.concurrent_streams {
                block.migration.concurrent_streams = streams;
            }
            if let Some(bytes) = staged.disk_reserve {
                block.migration.disk_reserve = bytes;
            }
            if let Some(moves) = staged.moves_per_node {
                block.rebalance.moves_per_node = moves;
            }
            if let Some(ms) = staged.plan_interval_ms {
                block.rebalance.plan_interval = Duration::from_millis(ms).into();
            }
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
            // the peer lanes' material, if the fixture minted it
            // ([F50](../../docs/src/features/cluster-operations.md))
            if let Some(tls) = &staged.tls {
                block = block.tls(shoal::server::conf::cluster::PeerTls {
                    cert: tls.cert.clone().into(),
                    key: tls.key.clone().into(),
                    ca: tls.ca.clone().into(),
                    bind_identity: tls.bind_identity,
                });
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
    // a rehome crash point, armed before the pool starts since the rehome runs inside the
    // start ([F47](../../docs/src/features/local-rehome.md))
    if let Some(point) = &request.rehome_crash_at {
        shoal::server::rehome::crash_point::arm_named(point).expect("a rehome crash point the fixture names exists");
    }
    let mut pool = match ShoalPool::<TestDb>::start(conf) {
        Ok(pool) => pool,
        Err(error) => {
            report(&format!("{} {error}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
    };
    // a crash point or a pause the test staged, armed before anything can install
    // ([F43](../../docs/src/features/node-recovery.md))
    if let Some(staged) = &request.cluster {
        if let Some(point) = &staged.crash_at {
            pool.crash_at(point).expect("a crash point the fixture names exists");
        }
        if let Some(ms) = staged.install_hold_ms {
            pool.hold_install(ms);
        }
        // a move phase to die right after committing ([F45](../../docs/src/features/replica-migration.md))
        if let Some(phase) = &staged.move_crash_at {
            pool.move_crash_at(phase, None).expect("a move phase the fixture names exists");
        }
    }
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
    // every server answers commands on its stdin, for the tests to drive it, while still
    // watching for a shard death; ~~a standalone node has no peers and just watches~~ a
    // standalone node answers the verbs that need no peer too, since the rehome is tested on
    // one ([F47](../../docs/src/features/local-rehome.md))
    {
        use tokio::io::AsyncBufReadExt as _;
        // resolve a node index in a command to the NodeId the fixture minted for it
        let peers: Vec<shoal::shared::identity::NodeId> = request
            .cluster
            .as_ref()
            .map(|staged| {
                staged
                    .peers
                    .iter()
                    .map(|node| shoal::shared::identity::NodeId(node.parse().expect("a node id")))
                    .collect()
            })
            .unwrap_or_default();
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
        // the wire version every link of this node negotiated, by peer index and lane, and
        // the version the cluster activated ([F48](../../docs/src/features/rolling-compatibility.md))
        "WIRE" => pool
            .transport()
            .map_err(|error| format!("{error:?}"))
            .and_then(|views| {
                let mut links = Vec::new();
                for view in views {
                    for link in view.links {
                        let peer = peers.iter().position(|node| *node == link.node);
                        links.push(serde_json::json!({
                            "shard": view.shard,
                            "peer": peer,
                            "lane": link.lane,
                            "state": link.state,
                            "wire_version": link.wire_version,
                            "capabilities": link.capabilities,
                        }));
                    }
                }
                let topology = pool.topology().map_err(|error| format!("{error:?}"))?;
                Ok(serde_json::json!({
                    "links": links,
                    "activated": topology.wire.activated,
                    "min_member": topology.wire.min_member,
                    "max_member": topology.wire.max_member,
                    "newest": topology.wire.newest,
                    "floor": topology.wire.floor,
                }))
            }),
        // activate a wire version, as the process
        "ACTIVATE" => match parts.next().and_then(|wire| wire.parse::<u8>().ok()) {
            Some(wire) => admin(AdminKind::Activate { wire }),
            None => Err("ACTIVATE needs a wire version".to_string()),
        },
        // back a table, or every table, up under a directory, as the process, answering the
        // operation ([F49](../../docs/src/features/backup-and-recovery.md))
        "BACKUP" => match parts.next() {
            Some(path) => {
                let table = parts.next().map(str::to_string);
                plan_op(pool, AdminKind::Backup { table, path: path.to_string() })
            }
            None => Err("BACKUP needs a directory and an optional table".to_string()),
        },
        "BACKUP_STATUS" => match parts.next().and_then(|text| text.parse::<uuid::Uuid>().ok()) {
            Some(op) => admin(AdminKind::BackupStatus { op }),
            None => Err("BACKUP_STATUS needs an operation id".to_string()),
        },
        "BACKUPS" => admin(AdminKind::Backups),
        // restore a backup from a directory, as the process, answering the operation
        "RESTORE" => match parts.next() {
            Some(path) => plan_op(pool, AdminKind::Restore { path: path.to_string() }),
            None => Err("RESTORE needs a directory".to_string()),
        },
        "RESTORE_STATUS" => match parts.next().and_then(|text| text.parse::<uuid::Uuid>().ok()) {
            Some(op) => admin(AdminKind::RestoreStatus { op }),
            None => Err("RESTORE_STATUS needs an operation id".to_string()),
        },
        // every recovery an operator ran on this cluster
        "RECOVERIES" => admin(AdminKind::Recoveries),
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
        // read the peer certificate, key and authority again ([F50](../../docs/src/features/cluster-operations.md))
        "RELOAD_TLS" => pool
            .reload_tls()
            .map(|report| serde_json::to_value(report).unwrap_or_default())
            .map_err(|error| format!("{error}")),
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
                        // sum every shard's digest into one: a sum of row hashes, so the
                        // executor count is not in it and an export's digest is its source's
                        let mut rows = 0u64;
                        let mut hash = 0u64;
                        let mut groups = serde_json::Map::new();
                        for answer in answers {
                            let value = answer?;
                            rows += value["rows"].as_u64().unwrap_or(0);
                            hash = hash.wrapping_add(value["hash"].as_u64().unwrap_or(0));
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
        // arm a crash point, so the next snapshot install dies there
        // ([F43](../../docs/src/features/node-recovery.md))
        "CRASH_AT" => match parts.next() {
            Some(point) => pool
                .crash_at(point)
                .map(|()| serde_json::json!({ "armed": point }))
                .map_err(|error| format!("{error:?}")),
            None => Err("CRASH_AT needs a point name".to_string()),
        },
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
        // ask for a repair as the process, with the request's kind as JSON, and answer the
        // operation it was recorded under ([F44](../../docs/src/features/repair.md))
        "REPAIR" => {
            let json = line.trim_start_matches("REPAIR").trim();
            match serde_json::from_str::<AdminKind>(json) {
                Ok(kind) => {
                    let op = uuid::Uuid::new_v4();
                    let mut last = String::new();
                    let mut answer = None;
                    for _ in 0..8 {
                        let version = pool.topology().map(|topology| topology.version).unwrap_or(0);
                        match pool.admin(AdminRequest { op, expected_version: version, kind: kind.clone() }) {
                            Ok(response) => match response.outcome {
                                Ok(shoal::shared::protocol::admin::AdminOutcome::Applied { version })
                                | Ok(shoal::shared::protocol::admin::AdminOutcome::Repeated { version }) => {
                                    answer = Some(Ok(serde_json::json!({ "op": op.to_string(), "version": version })));
                                    break;
                                }
                                Ok(other) => {
                                    answer = Some(Err(format!("REPAIR answered {other:?}")));
                                    break;
                                }
                                Err(error) if error.code() == shoal::shared::protocol::error::ErrorCode::StaleVersion => {
                                    last = format!("{}: {}", error.code(), error.msg);
                                    std::thread::sleep(Duration::from_millis(100));
                                }
                                Err(error) => {
                                    answer = Some(Err(format!("{}: {}", error.code(), error.msg)));
                                    break;
                                }
                            },
                            Err(error) => {
                                answer = Some(Err(format!("{error:?}")));
                                break;
                            }
                        }
                    }
                    answer.unwrap_or(Err(last))
                }
                Err(error) => Err(format!("REPAIR: {error}")),
            }
        }
        // the record of a repair, by its operation
        "REPAIR_STATUS" => match parts.next().and_then(|text| text.parse::<uuid::Uuid>().ok()) {
            Some(op) => admin(AdminKind::RepairStatus { op }),
            None => Err("REPAIR_STATUS needs an operation id".to_string()),
        },
        // move the replica set holding a key's tablet from one node to another, as the
        // process, and answer the operation it was recorded under
        // ([F45](../../docs/src/features/replica-migration.md))
        "MOVE" => {
            let key = parts.next().and_then(|hex| u64::from_str_radix(hex, 16).ok());
            let from = node_at(&mut parts);
            let to = node_at(&mut parts);
            match (key, from, to) {
                (Some(key), Some(from), Some(to)) => {
                    // truncation cannot happen: a tablet id is twelve bits
                    #[allow(clippy::cast_possible_truncation)]
                    let tablet = tablet_of(key) as u16;
                    let kind = AdminKind::Move { tablet, from, to };
                    let op = uuid::Uuid::new_v4();
                    let mut last = String::new();
                    let mut answer = None;
                    for _ in 0..8 {
                        let version = pool.topology().map(|topology| topology.version).unwrap_or(0);
                        match pool.admin(AdminRequest { op, expected_version: version, kind: kind.clone() }) {
                            Ok(response) => match response.outcome {
                                Ok(shoal::shared::protocol::admin::AdminOutcome::Applied { version })
                                | Ok(shoal::shared::protocol::admin::AdminOutcome::Repeated { version }) => {
                                    answer = Some(Ok(serde_json::json!({ "op": op.to_string(), "version": version, "tablet": tablet })));
                                    break;
                                }
                                Ok(other) => {
                                    answer = Some(Err(format!("MOVE answered {other:?}")));
                                    break;
                                }
                                Err(error) if error.code() == shoal::shared::protocol::error::ErrorCode::StaleVersion => {
                                    last = format!("{}: {}", error.code(), error.msg);
                                    std::thread::sleep(Duration::from_millis(100));
                                }
                                Err(error) => {
                                    answer = Some(Err(format!("{}: {}", error.code(), error.msg)));
                                    break;
                                }
                            },
                            Err(error) => {
                                answer = Some(Err(format!("{error:?}")));
                                break;
                            }
                        }
                    }
                    answer.unwrap_or(Err(last))
                }
                _ => Err("MOVE needs a key in hex, a source node index and a destination node index".to_string()),
            }
        }
        // arm a move phase, so this node's driver of a group - or of whichever group commits
        // the phase first - dies right after committing it
        "MOVE_CRASH_AT" => match parts.next() {
            Some(phase) => {
                let group = parts.next().and_then(|hex| u64::from_str_radix(hex, 16).ok()).map(shoal::shared::identity::GroupId);
                pool.move_crash_at(phase, group)
                    .map(|()| serde_json::json!({ "armed": phase }))
                    .map_err(|error| format!("{error:?}"))
            }
            None => Err("MOVE_CRASH_AT needs a phase name".to_string()),
        },
        // the record of a move, by its operation
        "MOVE_STATUS" => match parts.next().and_then(|text| text.parse::<uuid::Uuid>().ok()) {
            Some(op) => admin(AdminKind::MoveStatus { op }),
            None => Err("MOVE_STATUS needs an operation id".to_string()),
        },
        // the placement operations, as the process, answering the operation each was
        // recorded under, which is its plan's identity
        // ([F46](../../docs/src/features/capacity-rebalancing.md))
        "DECOMMISSION" => match node_at(&mut parts) {
            Some(node) => plan_op(pool, AdminKind::Decommission { node }),
            None => Err("DECOMMISSION needs a node index".to_string()),
        },
        "REMOVE" => match node_at(&mut parts) {
            Some(node) => {
                let replacement = node_at(&mut parts);
                plan_op(pool, AdminKind::Remove { node, replacement })
            }
            None => Err("REMOVE needs a node index and an optional replacement index".to_string()),
        },
        "MAINTENANCE" => match (node_at(&mut parts), parts.next()) {
            (Some(node), Some(switch)) => admin(AdminKind::Maintenance {
                node,
                suspend: switch == "on",
            }),
            _ => Err("MAINTENANCE needs a node index and on or off".to_string()),
        },
        "REBALANCE" => plan_op(pool, AdminKind::Rebalance),
        // the record of a plan, by its operation, and every plan
        "PLAN_STATUS" => match parts.next().and_then(|text| text.parse::<uuid::Uuid>().ok()) {
            Some(op) => admin(AdminKind::PlanStatus { op }),
            None => Err("PLAN_STATUS needs an operation id".to_string()),
        },
        "PLANS" => admin(AdminKind::Plans),
        // override the free bytes this node reports and checks, or lift the override
        "FREE_BYTES" => match parts.next() {
            Some("none") => {
                pool.free_bytes_override(0);
                Ok(serde_json::json!({ "override": null }))
            }
            Some(bytes) => match bytes.parse::<u64>() {
                Ok(bytes) => {
                    pool.free_bytes_override(bytes);
                    Ok(serde_json::json!({ "override": bytes }))
                }
                Err(_) => Err("FREE_BYTES needs a byte count or none".to_string()),
            },
            None => Err("FREE_BYTES needs a byte count or none".to_string()),
        },
        // propose a scrub of a group through this node, which has to lead it, and poll every
        // member's digest ([F44](../../docs/src/features/repair.md))
        "SCRUB" => match parts.next().and_then(|hex| u64::from_str_radix(hex, 16).ok()) {
            Some(group) => {
                let group = shoal::shared::identity::GroupId(group);
                pool.replication_verb(shoal::server::replication::ReplicationVerb::Scrub { group })
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|answers| {
                        // the shard that leads the group answers the reports; the rest refuse by name
                        let mut refusals = Vec::new();
                        for answer in answers {
                            match answer {
                                Ok(value) => return Ok(value),
                                Err(error) => refusals.push(error),
                            }
                        }
                        Err(format!("no shard scrubbed group {group}: {refusals:?}"))
                    })
            }
            None => Err("SCRUB needs a group id in hex".to_string()),
        },
        // fault one partition's archived copy on this node: CORRUPT, FORGET or ERASE <table> <key-hex>
        "CORRUPT" | "FORGET" | "ERASE" => {
            let fault = match verb {
                "CORRUPT" => shoal::storage::ArchiveFault::Corrupt,
                "FORGET" => shoal::storage::ArchiveFault::Forget,
                _ => shoal::storage::ArchiveFault::Erase,
            };
            match (parts.next(), parts.next().and_then(|hex| u64::from_str_radix(hex, 16).ok())) {
                (Some(table), Some(key)) => {
                    let table = shoal::shared::identity::TableId::of(table);
                    pool.replication_verb(shoal::server::replication::ReplicationVerb::Fault { table, fault, key })
                        .map_err(|error| format!("{error:?}"))
                        .and_then(|answers| {
                            // the shard whose archives hold the partition answers; the rest refuse by name
                            let mut refusals = Vec::new();
                            for answer in answers {
                                match answer {
                                    Ok(value) => return Ok(value),
                                    Err(error) => refusals.push(error),
                                }
                            }
                            Err(format!("no shard faulted partition {key:016x}: {refusals:?}"))
                        })
                }
                _ => Err(format!("{verb} needs a table and a partition key in hex")),
            }
        }
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
        // what the rehome this start ran moved, or null when the count had not changed
        // ([F47](../../docs/src/features/local-rehome.md))
        "REHOME" => Ok(serde_json::to_value(pool.rehome()).unwrap_or(serde_json::Value::Null)),
        // which executor hosts each slot, and how many tablets each executor owns
        "HOSTING" => {
            let hosting = pool.hosting();
            Ok(serde_json::json!({
                "slots": hosting.slots,
                "physical": hosting.physical,
                "hosts": hosting.hosts,
                "tablets_per_executor": hosting.tablets_per_executor(),
            }))
        }
        // which executors still have files in the directory, for the reclaim assertions
        "SHARD_DIRS" => {
            let conf = utils::build_crash_config(dir, 0);
            let tables = <TestDb as shoal::ShoalDatabase>::persistent_tables();
            Ok(serde_json::json!({
                "executors": shoal::server::rehome::executors_with_files(&conf, &tables, 16),
            }))
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

/// Ask for a placement operation as the process, answering the operation it was recorded under
///
/// The `MOVE` verb's shape: a version that moved between the read and the proposal is retried
/// a few times, and the answer carries the operation id, which is the plan's
/// ([F46](../../docs/src/features/capacity-rebalancing.md)).
///
/// # Arguments
///
/// * `pool` - This node's pool
/// * `kind` - What is asked
fn plan_op(pool: &ShoalPool<TestDb>, kind: shoal::server::AdminKind) -> Result<serde_json::Value, String> {
    use shoal::server::AdminRequest;
    let op = uuid::Uuid::new_v4();
    let mut last = String::new();
    for _ in 0..8 {
        let version = pool.topology().map(|topology| topology.version).unwrap_or(0);
        match pool.admin(AdminRequest { op, expected_version: version, kind: kind.clone() }) {
            Ok(response) => match response.outcome {
                Ok(shoal::shared::protocol::admin::AdminOutcome::Applied { version })
                | Ok(shoal::shared::protocol::admin::AdminOutcome::Repeated { version }) => {
                    return Ok(serde_json::json!({ "op": op.to_string(), "version": version }));
                }
                Ok(other) => return Err(format!("{} answered {other:?}", kind.name())),
                Err(error) if error.code() == shoal::shared::protocol::error::ErrorCode::StaleVersion => {
                    last = format!("{}: {}", error.code(), error.msg);
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(error) => return Err(format!("{}: {}", error.code(), error.msg)),
            },
            Err(error) => return Err(format!("{error:?}")),
        }
    }
    Err(last)
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
    // a standalone child runs the server function, and so the command loop with it: the
    // rehome verbs are answered by a node with no peers ([F47](../../docs/src/features/local-rehome.md))
    assert_eq!(NodeKind::Standalone.child_fn(), NodeKind::Server.child_fn());
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

/// The snapshot counters of one node, from its `GROUPS` view
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
fn snapshots_of(cluster: &mut Cluster, node: usize) -> Result<serde_json::Value, FixtureError> {
    Ok(groups_of(cluster, node)?["snapshots"].clone())
}

/// Wait until every group of a table on some nodes is purged past a position, driving compaction
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `nodes` - The nodes
/// * `table` - The table's name
/// * `past` - The position each group's purge point has to pass, by the group's id as the
///   report carries it; a group not named needs nothing
/// * `within` - How long to keep trying
fn wait_purged_past(
    cluster: &mut Cluster,
    nodes: &[usize],
    table: &str,
    past: &std::collections::HashMap<u64, u64>,
    within: Duration,
) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let mut behind = Vec::new();
        for node in nodes {
            let _ = cluster.node_mut(*node).command("ROTATE")?;
            let _ = cluster.node_mut(*node).command("COMPACT")?;
            let view = groups_of(cluster, *node)?;
            for shard in view["shards"].as_array().into_iter().flatten() {
                for group in shard["groups"].as_array().into_iter().flatten() {
                    let id = group["group"].as_u64().unwrap_or(0);
                    let Some(needed) = past.get(&id) else { continue };
                    if group["table_name"] == table && group["purged"].as_u64().unwrap_or(0) <= *needed {
                        behind.push((*node, id, group["purged"].clone(), group["checkpoint"].clone(), *needed));
                    }
                }
            }
        }
        if behind.is_empty() {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("{table}'s groups were never purged past what node two saw: {behind:?}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait until every group of a table on a node has a checkpoint, driving compaction
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `table` - The table's name
/// * `within` - How long to keep trying
fn wait_checkpointed(cluster: &mut Cluster, node: usize, table: &str, within: Duration) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let _ = cluster.node_mut(node).command("ROTATE")?;
        let _ = cluster.node_mut(node).command("COMPACT")?;
        let view = groups_of(cluster, node)?;
        let behind: Vec<serde_json::Value> = view["shards"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
            .filter(|group| group["table_name"] == table && group["checkpoint"].as_u64().unwrap_or(0) == 0)
            .cloned()
            .collect();
        if behind.is_empty() {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("{table}'s groups on node {node} never checkpointed: {behind:?}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// What each group of a table had applied on a node, by the group's id as the report carries it
///
/// # Arguments
///
/// * `view` - The node's `GROUPS` view
/// * `table` - The table's name
fn applied_by_group(view: &serde_json::Value, table: &str) -> std::collections::HashMap<u64, u64> {
    view["shards"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
        .filter(|group| group["table_name"] == table)
        .map(|group| (group["group"].as_u64().unwrap_or(0), group["applied"].as_u64().unwrap_or(0)))
        .collect()
}

/// Kill a node, write enough on both tables to purge past what it holds, and leave it dead
///
/// What every snapshot test starts from: a member behind the purge point of every group of
/// both tables on the survivors ([F43](../../docs/src/features/node-recovery.md)). Needs
/// `checkpoint_entries` and `retained_entries` shortened enough that the writes pass them.
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to leave behind
/// * `via` - The node to write through
/// * `from` - The first key to write
/// * `count` - How many keys, on each table
/// * `text` - The text, repeated to the width wanted
async fn leave_behind_purge(
    cluster: &mut Cluster,
    node: usize,
    via: usize,
    from: u64,
    count: u64,
    text: &str,
) -> Result<(), FixtureError> {
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let behind = groups_of(cluster, node)?;
    let notes_seen = applied_by_group(&behind, "Note");
    let rows_seen = applied_by_group(&behind, "Row");
    cluster.kill(node)?;
    let addr = cluster.node(via).endpoints.client.to_string();
    let survivors: Vec<usize> = (0..cluster.len()).filter(|id| *id != node).collect();
    for key in from..from + count {
        write_note_eventually(&addr, key, &format!("{text}-{key}"), Duration::from_secs(15)).await?;
    }
    let client = Shoal::<TestDbClient>::new(&addr).await.map_err(ok)?;
    for key in from..from + count {
        client.send_one(Row { key, data: format!("{text}-{key}") }).await.map_err(ok)?;
    }
    wait_purged_past(cluster, &survivors, "Note", &notes_seen, Duration::from_secs(90))?;
    wait_purged_past(cluster, &survivors, "Row", &rows_seen, Duration::from_secs(30))?;
    Ok(())
}

/// Wait until a node's process has exited, or say it never did
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `within` - How long to wait
fn wait_dead(cluster: &Cluster, node: usize, within: Duration) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    // a process that exited on its own is a zombie until it is reaped, and still answers a
    // signal; its stdout closing is what says it is gone
    while cluster.node(node).failure().is_none() {
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {node} never died")));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

/// Wait until no group of a node is installing a snapshot
///
/// A digest agrees a few milliseconds before the install's cleanup lands, so a test that has
/// seen convergence still waits for the flag ([F43](../../docs/src/features/node-recovery.md)).
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `within` - How long to wait
fn wait_not_installing(cluster: &mut Cluster, node: usize, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let view = groups_of(cluster, node)?;
        if view["installing"].as_u64().unwrap_or(0) == 0 {
            return Ok(view);
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {node} never finished installing: {view}")));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
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

/// A returning node catches up by log within the retained window and by snapshot past it (C7 M7)
///
/// Node two is killed, a few writes land, and it comes back: it is fed from the retained log
/// and installs nothing. Killed again, enough writes land on the persistent and the ephemeral
/// table to checkpoint and purge past what it holds, and it comes back: every group it is
/// behind on installs a snapshot - the persistent ones through the compactor, the volatile
/// ones into memory - and every digest agrees. A delete under an identity made while it was
/// down is answered as the original result through it afterwards
/// ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn returning_node_catches_up_by_log_or_snapshot() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    // a base every node holds
    for key in 8000..8010u64 {
        client.send_one(Note { key, text: format!("base-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key, data: format!("base-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(30))?;
    // killed, a few writes inside the retained window, and back: fed from the log
    cluster.kill(2)?;
    for key in 8010..8014u64 {
        write_note_eventually(&addr0, key, &format!("log-{key}"), Duration::from_secs(15)).await?;
    }
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let by_log = snapshots_of(&mut cluster, 2)?;
    assert_eq!(by_log["installed"], 0, "a node inside the retained window installed a snapshot: {by_log}");
    // killed again; enough writes land to checkpoint and purge past what it holds, on both
    // tables, and a delete under an identity is made while it is away
    let behind = groups_of(&mut cluster, 2)?;
    cluster.kill(2)?;
    let identity = uuid::Uuid::new_v4();
    let original = delete_note_as(&addr0, 8003, &SendOptions::new().identity(identity)).await.map_err(ok)?;
    let original_token = original.session_token().expect("a committed delete carries a token");
    for key in 8100..8200u64 {
        write_note_eventually(&addr0, key, &format!("snap-{key}"), Duration::from_secs(15)).await?;
    }
    for key in 8100..8200u64 {
        client.send_one(Row { key, data: format!("snap-{key}") }).await.map_err(ok)?;
    }
    // every group of both tables purged past what node two saw
    wait_purged_past(&mut cluster, &[0, 1], "Note", &applied_by_group(&behind, "Note"), Duration::from_secs(90))?;
    wait_purged_past(&mut cluster, &[0, 1], "Row", &applied_by_group(&behind, "Row"), Duration::from_secs(30))?;
    // back, and past the purge point on every group: it installs snapshots and converges
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(60))?;
    wait_not_installing(&mut cluster, 2, Duration::from_secs(10))?;
    let by_snapshot = snapshots_of(&mut cluster, 2)?;
    let installed = by_snapshot["installed"].as_u64().unwrap_or(0);
    assert!(installed >= 2, "node two did not install a snapshot for both tables: {by_snapshot}");
    assert_eq!(by_snapshot["dropped_chunks"], 0, "{by_snapshot}");
    // the senders counted what they sent
    let sent: u64 = (0..2)
        .map(|node| snapshots_of(&mut cluster, node).map(|s| s["sent"].as_u64().unwrap_or(0)))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum();
    assert!(sent >= installed, "the senders counted {sent} transfers and node two installed {installed}");
    // every row is readable through the returning node, from the installed archives
    let addr2 = cluster.node(2).endpoints.client.to_string();
    for key in [8000u64, 8011, 8150] {
        wait_note(&addr2, key, Some(&read_note(&addr0, key).await.map_err(ok)?.expect("the note is there")), Duration::from_secs(10)).await?;
    }
    assert_eq!(read_note(&addr2, 8003).await.map_err(ok)?, None, "the delete did not travel");
    // the identity from before the kill is answered as the original result through node two
    let again = delete_note_as(&addr2, 8003, &SendOptions::new().identity(identity).retry(Duration::from_secs(15))).await;
    let again = again.unwrap_or_else(|error| panic!("the retry under the old identity was not the original result: {error:?}"));
    assert_eq!(again.bundle(), identity);
    let token = again.session_token().expect("a duplicate answers with a token");
    assert_eq!(token.group, original_token.group);
    assert!(token.index >= original_token.index);
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A durable follower that lost its log is fed by the leader, not fatal to it (C7, item 99)
///
/// Node two is killed and its WAL segments removed, then killed again and its whole WAL
/// directory removed - a member whose disk lost what it acknowledged. Both times the leader's
/// process is the one it was, writes through it keep committing, the member is fed by log or
/// by snapshot until every digest agrees, and it reports that it lost its log
/// ([Resolved #99](../../docs/src/appendix/resolved/durable-log-reversion.md)).
#[tokio::test(flavor = "multi_thread")]
async fn durable_log_reversion_is_fed_not_fatal() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    // a base every node holds, checkpointed on node two so its checkpoint and its archives
    // both say it held every group of the table
    for key in 21_000..21_060u64 {
        client.send_one(Note { key, text: format!("base-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_checkpointed(&mut cluster, 2, "Note", Duration::from_secs(60))?;
    let survivors = [cluster.node(0).pid, cluster.node(1).pid];
    let wal_dir = cluster.dir(2).join("wal").join("Shard-0");
    // variant A: the segments alone, so the checkpoint and the archives outlive the log
    cluster.kill(2)?;
    let mut removed = 0;
    for entry in std::fs::read_dir(&wal_dir).map_err(|error| FixtureError::NotReady(format!("{error:?}")))? {
        let path = entry.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?.path();
        if path.extension().is_some_and(|ext| ext == "wal") {
            std::fs::remove_file(&path).map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
            removed += 1;
        }
    }
    assert!(removed > 0, "node two had no segments to lose under {}", wal_dir.display());
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    // the leader lives and writes through it keep committing
    for key in 21_020..21_030u64 {
        write_note_eventually(&addr0, key, &format!("after-segments-{key}"), Duration::from_secs(15)).await?;
    }
    for id in 0..2 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died after node two lost its segments");
        assert_eq!(cluster.node(id).pid, survivors[id], "node {id} is not the process it was");
    }
    // the member is fed until it agrees, and says what it lost
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    let view = groups_of(&mut cluster, 2)?;
    let lost = view["integrity"]["log_lost"].as_u64().unwrap_or(0);
    assert!(lost > 0, "node two did not report a lost log: {}", view["integrity"]);
    // variant B: the whole directory, so the checkpoint and the sidecar go with the log and
    // the archives alone say the node once held the group
    wait_checkpointed(&mut cluster, 2, "Note", Duration::from_secs(60))?;
    cluster.kill(2)?;
    std::fs::remove_dir_all(&wal_dir).map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    for key in 21_030..21_040u64 {
        write_note_eventually(&addr0, key, &format!("after-dir-{key}"), Duration::from_secs(15)).await?;
    }
    for id in 0..2 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died after node two lost its WAL directory");
        assert_eq!(cluster.node(id).pid, survivors[id], "node {id} is not the process it was");
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))?;
    let view = groups_of(&mut cluster, 2)?;
    let lost = view["integrity"]["log_lost"].as_u64().unwrap_or(0);
    assert!(lost > 0, "node two did not report a lost log after losing its directory: {}", view["integrity"]);
    // every key is the leader's value through the member that lost everything
    let addr2 = cluster.node(2).endpoints.client.to_string();
    for key in [21_000u64, 21_025, 21_035] {
        let expected = read_note(&addr0, key).await.map_err(ok)?.expect("the note is there");
        wait_note(&addr2, key, Some(&expected), Duration::from_secs(10)).await?;
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// The `SCRUB` of a group through the node that leads it: every member's report
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node, which has to lead the group
/// * `group` - The group, in hex
fn scrub_of(cluster: &mut Cluster, node: usize, group: &str) -> Result<serde_json::Value, FixtureError> {
    Ok(cluster.node_mut(node).command(&format!("SCRUB {group}"))?["ok"].clone())
}

/// Every member's report of a scrub, by the node index that holds it, in node order
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `scrub` - What `SCRUB` answered
fn reports_by_node(cluster: &Cluster, scrub: &serde_json::Value) -> Vec<(usize, serde_json::Value)> {
    let ids = cluster.node_ids();
    let mut reports: Vec<(usize, serde_json::Value)> = scrub["reports"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(member, report)| {
            // a member is `node/shard`
            let node = member.split('/').next()?;
            let index = ids.iter().position(|id| id == node)?;
            Some((index, report.clone()))
        })
        .collect();
    reports.sort_by_key(|(index, _)| *index);
    reports
}

/// Compact everything a node holds of a table into its archives, now
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `table` - The table
fn compact_now(cluster: &mut Cluster, node: usize, table: &str) -> Result<(), FixtureError> {
    let _ = table;
    let _ = cluster.node_mut(node).command("ROTATE")?;
    let _ = cluster.node_mut(node).command("COMPACT")?;
    // a compaction is a job on another task; give it a moment to merge and sync
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

/// The canonical digest depends on the rows and not on how the archives lie (C9 M8)
///
/// Three replicas of one group hold the same rows three ways: node zero merged them into its
/// archives in three rounds with deletes and updates between, node one in one round, node two
/// never - its rows are resident, its tombstones too. A scrub proposed through the leader is
/// applied at one committed index on all three, and every report is verified, at that index,
/// and equal. Then a partition forgotten on one node, a partition erased on another and a
/// record corrupted on a third are three reports the scrub tells apart: a verified digest
/// that differs, a verified digest that differs, and an invalid copy. The fixture's own fold
/// of applied state - a different function, on purpose - agrees with each verdict
/// ([F44](../../docs/src/features/repair.md)).
#[tokio::test(flavor = "multi_thread")]
async fn canonical_digest_ignores_archive_layout_at_same_boundary() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    let hashed = |key: u64| <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
    // three rounds of writes; node zero merges each into its archives as it lands, with
    // deletes and updates between so its archives hold superseded and pruned copies
    for round in 0..3u64 {
        let base = 23_000 + round * 100;
        for key in base..base + 40 {
            client.send_one(Note { key, text: format!("note-{key}") }).await.map_err(ok)?;
        }
        for key in base..base + 10 {
            delete_note(&addr0, key).await.map_err(ok)?;
        }
        for key in base + 10..base + 20 {
            client.send_one(Note { key, text: format!("note-{key}-again") }).await.map_err(ok)?;
        }
        wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
        compact_now(&mut cluster, 0, "Note")?;
    }
    // node one merges everything in one round; node two never does
    compact_now(&mut cluster, 1, "Note")?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let independent = digest_of(&mut cluster, 0, "Note")?;
    // one group of the table, and the node that leads it
    let probe = 23_015u64;
    let (group, leader) = group_of(&mut cluster, 0, "Note", probe)?;
    let leader = leader.expect("the group has a leader");
    // a scrub through the leader: one boundary, three verified reports, one digest
    let scrub = scrub_of(&mut cluster, leader, &group)?;
    let boundary = scrub["boundary"].as_u64().expect("the scrub committed at an index");
    let reports = reports_by_node(&cluster, &scrub);
    assert_eq!(reports.len(), 3, "{scrub}");
    for (node, report) in &reports {
        assert_eq!(report["boundary"], boundary, "node {node} reported at another boundary: {report}");
        assert_eq!(report["integrity"], "Verified", "node {node} is not verified: {report}");
        assert_eq!(report["digest"], reports[0].1["digest"], "node {node} disagrees on a clean group: {scrub}");
        assert_eq!(report["rows"], reports[0].1["rows"], "node {node} counts differently: {scrub}");
    }
    let clean = reports[0].1["digest"].clone();
    let rows = reports[0].1["rows"].as_u64().unwrap_or(0);
    assert!(rows > 0, "the group holds rows: {scrub}");
    // the same through the admin frame: a verify of the whole table is clean on every group
    let verify = |tablet: Option<u16>, release: bool| shoal::server::AdminKind::Repair {
        table: "Note".to_string(),
        tablet,
        mode: "verify".to_string(),
        source: None,
        release,
    };
    let op = repair_as_process(&mut cluster, 0, &verify(None, false))?;
    let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(60))?;
    for (id, progress) in record["groups"].as_object().expect("groups") {
        assert!(progress["outcome"]["Clean"].is_object(), "group {id} is not clean: {progress}");
    }
    // three live keys of the group: the first twenty of every round were deleted or rewritten,
    // so the live ones are chosen from past them
    let live: Vec<u64> = keys_in_group(&mut cluster, "Note", &group, 23_020, 12)?
        .into_iter()
        .filter(|key| key % 100 >= 20 && key % 100 < 40)
        .collect();
    assert!(live.len() >= 3, "fewer than three live keys fall in group {group}: {live:?}");
    let (forgotten, erased, corrupted) = (live[0], live[1], live[2]);
    // forget a partition of the group on node one: its verified digest differs by a row
    let answer = cluster.node_mut(1).command(&format!("FORGET Note {:016x}", hashed(forgotten)))?;
    assert_eq!(answer["ok"]["fault"], "forget", "{answer}");
    let scrub = scrub_of(&mut cluster, leader, &group)?;
    let reports = reports_by_node(&cluster, &scrub);
    assert_eq!(reports[1].1["integrity"], "Verified", "{scrub}");
    assert_ne!(reports[1].1["digest"], clean, "a forgotten partition was not seen: {scrub}");
    assert_eq!(reports[1].1["rows"].as_u64(), Some(rows - 1), "{scrub}");
    for node in [0usize, 2] {
        assert_eq!(reports[node].1["digest"], clean, "node {node} changed without cause: {scrub}");
    }
    // the fixture's own fold sees it too, and it is not the scrub's function
    let after_forget = digest_of(&mut cluster, 1, "Note")?;
    assert_ne!(after_forget["hash"], independent["hash"]);
    assert_eq!(digest_of(&mut cluster, 0, "Note")?["hash"], independent["hash"]);
    // a verify of that tablet judges node one divergent and quarantines its copy
    let tablet = tablet_of(forgotten) as u16;
    let op = repair_as_process(&mut cluster, 0, &verify(Some(tablet), false))?;
    let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(60))?;
    let group_id = u64::from_str_radix(&group, 16).expect("a group id");
    let progress = &record["groups"][group_id.to_string()];
    let quarantined = &progress["outcome"]["Divergent"]["quarantined"];
    assert_eq!(quarantined.as_array().map_or(0, Vec::len), 1, "the forgotten copy was not quarantined: {record}");
    assert_eq!(quarantined[0][0]["node"], cluster.node_ids()[1], "{record}");
    assert_eq!(quarantined[0][1], "Divergent", "{record}");
    // the node reports it and readiness counts it
    let one = groups_of(&mut cluster, 1)?;
    assert_eq!(one["quarantined"], 1, "{one}");
    assert!(one["integrity"]["quarantined"].as_u64().unwrap_or(0) >= 1, "{}", one["integrity"]);
    // the copy reaches the committed state through the node's report, a tick or two later,
    // and the frame every client is handed names it
    let member = wait_member_quarantined(&mut cluster, 1, true, Duration::from_secs(20))?;
    assert_eq!(member["quarantined"][0]["group"], group_id, "{member}");
    assert_eq!(member["quarantined"][0]["reason"], "Divergent", "{member}");
    // a read of the group through node one is routed to another holder and served from
    // there, never from the quarantined copy; the local refusal is the backstop for the
    // window before the map carries the quarantine
    let addr1 = cluster.node(1).endpoints.client.to_string();
    wait_note_routed(&addr1, erased, &format!("note-{erased}"), Duration::from_secs(20)).await?;
    assert_eq!(groups_of(&mut cluster, 1)?["quarantined"], 1);
    // an operator releases it after reading the record, and it serves again
    let op = repair_as_process(&mut cluster, 0, &verify(Some(tablet), true))?;
    let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(60))?;
    assert_eq!(record["groups"][group_id.to_string()]["outcome"], "Released", "{record}");
    wait_note(&addr1, erased, Some(&format!("note-{erased}")), Duration::from_secs(10)).await?;
    assert_eq!(groups_of(&mut cluster, 1)?["quarantined"], 0);
    wait_member_quarantined(&mut cluster, 1, false, Duration::from_secs(20))?;
    // erase a partition on node zero: a valid record whose content changed
    let answer = cluster.node_mut(0).command(&format!("ERASE Note {:016x}", hashed(erased)))?;
    assert_eq!(answer["ok"]["fault"], "erase", "{answer}");
    let scrub = scrub_of(&mut cluster, leader, &group)?;
    let reports = reports_by_node(&cluster, &scrub);
    assert_eq!(reports[0].1["integrity"], "Verified", "{scrub}");
    assert_ne!(reports[0].1["digest"], clean, "an erased partition was not seen: {scrub}");
    assert_eq!(reports[0].1["rows"].as_u64(), Some(rows - 1), "{scrub}");
    assert_eq!(reports[2].1["digest"], clean, "node two changed without cause: {scrub}");
    assert_ne!(digest_of(&mut cluster, 0, "Note")?["hash"], independent["hash"]);
    // corrupt a record on node one: an invalid copy, which its digest says by name
    let answer = cluster.node_mut(1).command(&format!("CORRUPT Note {:016x}", hashed(corrupted)))?;
    assert_eq!(answer["ok"]["fault"], "corrupt", "{answer}");
    let scrub = scrub_of(&mut cluster, leader, &group)?;
    let reports = reports_by_node(&cluster, &scrub);
    assert_eq!(reports[1].1["integrity"]["Invalid"]["checksum_failures"], 1, "the corrupt record was not found: {scrub}");
    assert_eq!(reports[2].1["digest"], clean, "node two changed without cause: {scrub}");
    // and a read of that key through node one is refused by name, not answered from bad
    // bytes: the read that met the record answers with the checksum, or - when the gather
    // tried the share again after the failure - with the quarantine that failure decided
    let read = read_note(&addr1, corrupted).await;
    assert!(
        matches!(
            failure_code(&read),
            Some(shoal::shared::protocol::error::ErrorCode::CorruptArchive | shoal::shared::protocol::error::ErrorCode::Quarantined)
        ),
        "the corrupt record was served: {read:?}"
    );
    // the read that met it quarantined the copy on the spot: the next read, inside the
    // window before the map carries the quarantine, is refused for that by name
    let again = read_note(&addr1, corrupted).await;
    assert_eq!(failure_code(&again), Some(shoal::shared::protocol::error::ErrorCode::Quarantined), "{again:?}");
    let one = groups_of(&mut cluster, 1)?;
    assert_eq!(one["quarantined"], 1, "{one}");
    // the integrity counters say what happened on the node, before a restart resets them
    assert!(one["integrity"]["checksum_failures"].as_u64().unwrap_or(0) >= 1, "{}", one["integrity"]);
    assert!(one["integrity"]["scrubs"].as_u64().unwrap_or(0) >= 4, "{}", one["integrity"]);
    assert!(one["integrity"]["quarantined"].as_u64().unwrap_or(0) >= 2, "{}", one["integrity"]);
    // once the map carries it, a read through node one is served from another holder
    wait_member_quarantined(&mut cluster, 1, true, Duration::from_secs(20))?;
    wait_note_routed(&addr1, corrupted, &format!("note-{corrupted}"), Duration::from_secs(20)).await?;
    // and the marker outlives a restart: node one comes back with the copy still quarantined
    cluster.restart(1, NodeKind::Server)?;
    cluster.wait_joined(&[1])?;
    let addr1 = cluster.node(1).endpoints.client.to_string();
    wait_note_routed(&addr1, corrupted, &format!("note-{corrupted}"), Duration::from_secs(20)).await?;
    assert_eq!(groups_of(&mut cluster, 1)?["quarantined"], 1, "the quarantine did not outlive the restart");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Wait until the committed state does, or does not, name a member's copies as quarantined
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The member
/// * `some` - Whether to wait for at least one quarantined copy, or for none
/// * `within` - How long to wait
fn wait_member_quarantined(cluster: &mut Cluster, node: usize, some: bool, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    let id = cluster.node_ids()[node].clone();
    loop {
        let members = cluster.members(0)?;
        let member = members["members"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|member| member["record"]["node"] == id)
            .cloned()
            .unwrap_or_default();
        let has = member["quarantined"].as_array().is_some_and(|copies| !copies.is_empty());
        if has == some {
            return Ok(member);
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {node}'s committed quarantines never became {some}: {member}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait until a note reads through a node, treating a quarantine refusal as not yet
///
/// A read through a node holding a quarantined copy is refused by name until the map carries
/// the quarantine and the node routes the read elsewhere
/// ([F44](../../docs/src/features/repair.md)).
///
/// # Arguments
///
/// * `addr` - The endpoint
/// * `key` - The key
/// * `expected` - The text expected
/// * `within` - How long to wait
async fn wait_note_routed(addr: &str, key: u64, expected: &str, within: Duration) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let read = read_note(addr, key).await;
        match read {
            Ok(Some(text)) if text == expected => return Ok(()),
            Err(shoal::client::Errors::Server { code: shoal::shared::protocol::error::ErrorCode::Quarantined, .. }) | Ok(_) => {}
            Err(error) => return Err(FixtureError::NotReady(format!("{error:?}"))),
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("note {key} through {addr} never read as {expected:?}: {read:?}")));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Ask for a repair as the process through a node, and hand back the operation
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask
/// * `kind` - The request
fn repair_as_process(cluster: &mut Cluster, node: usize, kind: &shoal::server::AdminKind) -> Result<uuid::Uuid, FixtureError> {
    let json = serde_json::to_string(kind).expect("a kind serializes");
    let reply = cluster.node_mut(node).command(&format!("REPAIR {json}"))?;
    reply["ok"]["op"]
        .as_str()
        .and_then(|op| op.parse().ok())
        .ok_or_else(|| FixtureError::NotReady(format!("the repair was not recorded: {reply}")))
}

/// Ask for a move of the set holding a key through one node, as the process
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask through
/// * `key` - A key in the set
/// * `from` - The node leaving the set
/// * `to` - The node replacing it
fn move_as_process(cluster: &mut Cluster, node: usize, key: u64, from: usize, to: usize) -> Result<uuid::Uuid, FixtureError> {
    let started = Instant::now();
    loop {
        let reply = cluster.node_mut(node).command(&format!("MOVE {key:016x} {from} {to}"))?;
        if let Some(op) = reply["ok"]["op"].as_str() {
            return op.parse().map_err(|error| FixtureError::ChildFailed(format!("MOVE answered {op}: {error}")));
        }
        // a control leader being elected after a kill is waited for, as an operator would
        let electing = reply["error"].as_str().is_some_and(|error| error.starts_with("NotLeader"));
        if !electing || started.elapsed() > Duration::from_secs(30) {
            return Err(FixtureError::ChildFailed(format!("MOVE answered {reply}")));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Poll a move's record through one node until it is done, or the time is up
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask through
/// * `op` - The operation
/// * `within` - How long to wait
fn wait_move_done_via(cluster: &mut Cluster, node: usize, op: uuid::Uuid, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let started = Instant::now();
    let mut last = serde_json::Value::Null;
    while started.elapsed() < within {
        let record = cluster.node_mut(node).command(&format!("MOVE_STATUS {op}"))?["ok"].clone();
        if record["phase"] == "Done" {
            return Ok(record);
        }
        last = record;
        std::thread::sleep(Duration::from_millis(250));
    }
    Err(FixtureError::ChildFailed(format!("move {op} did not finish within {within:?}: {last}")))
}

/// Wait until every group of a repair is done, asking a node as the process
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask
/// * `op` - The operation
/// * `within` - How long to wait
fn wait_repair_done_via(cluster: &mut Cluster, node: usize, op: uuid::Uuid, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        // a node asked before its control state applied the record has no record yet
        let record = cluster.node_mut(node).command(&format!("REPAIR_STATUS {op}"))?["ok"].clone();
        let done = record["groups"]
            .as_object()
            .is_some_and(|groups| !groups.is_empty() && groups.values().all(|group| group["phase"] == "Done"));
        if done {
            return Ok(record);
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("repair {op} never finished: {record}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Ask a node for a repair's record, as the control state holds it
///
/// # Arguments
///
/// * `client` - A client on the node to ask
/// * `op` - The operation
async fn repair_status(client: &Shoal<TestDbClient>, op: uuid::Uuid) -> Result<serde_json::Value, FixtureError> {
    use shoal::server::{AdminKind, AdminRequest};
    use shoal::shared::protocol::admin::AdminOutcome;
    let response = client
        .admin(&AdminRequest {
            op: uuid::Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::RepairStatus { op },
        })
        .await
        .map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    match response.outcome {
        Ok(AdminOutcome::Read(value)) => Ok(value),
        other => Err(FixtureError::NotReady(format!("no record of {op}: {other:?}"))),
    }
}

/// Wait until every group of a repair is done, and hand back the record
///
/// # Arguments
///
/// * `client` - A client on the node to ask
/// * `op` - The operation
/// * `within` - How long to wait
async fn wait_repair_done(client: &Shoal<TestDbClient>, op: uuid::Uuid, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        // a node asked before its control state applied the record has no record yet
        let record = match repair_status(client, op).await {
            Ok(record) => record,
            Err(FixtureError::NotReady(text)) if text.contains("is recorded") && std::time::Instant::now() <= deadline => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let done = record["groups"]
            .as_object()
            .is_some_and(|groups| !groups.is_empty() && groups.values().all(|group| group["phase"] == "Done"));
        if done {
            return Ok(record);
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("repair {op} never finished: {record}")));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Ask for a repair through a client, retrying only a stale version, and hand back the operation
///
/// # Arguments
///
/// * `client` - A client on the node to ask, whose principal has to be an admin
/// * `cluster` - The cluster, for the version
/// * `kind` - The request
async fn ask_repair(
    client: &Shoal<TestDbClient>,
    cluster: &mut Cluster,
    kind: shoal::server::AdminKind,
) -> Result<uuid::Uuid, FixtureError> {
    use shoal::server::AdminRequest;
    use shoal::shared::protocol::admin::AdminOutcome;
    use shoal::shared::protocol::error::ErrorCode;
    let op = uuid::Uuid::new_v4();
    for _ in 0..10 {
        let version = cluster.members(0)?["version"].as_u64().expect("a version");
        let response = client
            .admin(&AdminRequest { op, expected_version: version, kind: kind.clone() })
            .await
            .map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
        match response.outcome {
            Ok(AdminOutcome::Applied { .. } | AdminOutcome::Repeated { .. }) => return Ok(op),
            Err(error) if error.code() == ErrorCode::StaleVersion => tokio::time::sleep(Duration::from_millis(200)).await,
            other => return Err(FixtureError::NotReady(format!("the repair was not applied: {other:?}"))),
        }
    }
    Err(FixtureError::NotReady("the cluster's version kept moving under the repair request".to_string()))
}

/// A repair is authorized, versioned, idempotent and readable by its id from any node (C9 M8)
///
/// A user who is not an admin is refused, a stale version is refused, the same operation sent
/// again is answered as the first time, and the record is readable through every node until
/// every group of the table is done and clean. A second operation is asked for and the control
/// leader killed while its groups scrub: the drivers wait out the election, commit their
/// progress to the new leader, and the record completes through a survivor
/// ([F44](../../docs/src/features/repair.md)).
#[tokio::test(flavor = "multi_thread")]
async fn repair_is_authorized_versioned_and_resumable_by_id() -> Result<(), FixtureError> {
    use shoal::server::{AdminKind, AdminRequest};
    use shoal::shared::auth::Credentials;
    use shoal::shared::protocol::admin::AdminOutcome;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .auth("alice", "alpha")
        .auth("bob", "bravo")
        .admins(vec!["alice".to_string()])
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(20))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let alice = Shoal::<TestDbClient>::with_credentials(&addr0, Credentials::scram("alice", "alpha")).await.map_err(ok)?;
    let bob = Shoal::<TestDbClient>::with_credentials(&addr0, Credentials::scram("bob", "bravo")).await.map_err(ok)?;
    // rows on every node, compacted everywhere so the scrub has archives to read
    for key in 25_000..25_040u64 {
        alice.send_one(Note { key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for node in 0..3 {
        compact_now(&mut cluster, node, "Note")?;
    }
    let verify = AdminKind::Repair {
        table: "Note".to_string(),
        tablet: None,
        mode: "verify".to_string(),
        source: None,
        release: false,
    };
    let version = cluster.members(0)?["version"].as_u64().expect("a version");
    let op = uuid::Uuid::new_v4();
    // not an admin: refused by name
    let refused = bob
        .admin(&AdminRequest { op, expected_version: version, kind: verify.clone() })
        .await
        .map_err(ok)?;
    assert_eq!(refused.outcome.as_ref().expect_err("bob was allowed").code(), ErrorCode::Unauthorized, "{refused:?}");
    // a stale version: refused by name
    let stale = alice
        .admin(&AdminRequest { op, expected_version: version + 7, kind: verify.clone() })
        .await
        .map_err(ok)?;
    assert_eq!(stale.outcome.as_ref().expect_err("a stale version was applied").code(), ErrorCode::StaleVersion, "{stale:?}");
    // the right one: applied, and the same request again is the same answer
    let mut applied = None;
    for _ in 0..10 {
        let version = cluster.members(0)?["version"].as_u64().expect("a version");
        let answer = alice
            .admin(&AdminRequest { op, expected_version: version, kind: verify.clone() })
            .await
            .map_err(ok)?;
        match answer.outcome {
            Ok(AdminOutcome::Applied { version }) => {
                applied = Some(version);
                break;
            }
            Err(error) if error.code() == ErrorCode::StaleVersion => tokio::time::sleep(Duration::from_millis(200)).await,
            other => panic!("the repair was not applied: {other:?}"),
        }
    }
    let applied = applied.expect("the repair never applied");
    let repeated = alice
        .admin(&AdminRequest { op, expected_version: applied, kind: verify.clone() })
        .await
        .map_err(ok)?;
    assert!(matches!(repeated.outcome, Ok(AdminOutcome::Repeated { .. })), "{repeated:?}");
    // the record is readable through every node, and completes clean
    let record = wait_repair_done(&alice, op, Duration::from_secs(60)).await?;
    assert_eq!(record["mode"], "Verify", "{record}");
    assert_eq!(record["principal"], "alice", "{record}");
    let groups = record["groups"].as_object().expect("groups");
    assert!(!groups.is_empty(), "{record}");
    for (group, progress) in groups {
        assert!(progress["outcome"]["Clean"].is_object(), "group {group} is not clean: {progress}");
        assert_eq!(progress["outcome"]["Clean"]["unreported"], serde_json::json!([]), "{progress}");
        assert!(progress["boundary"].as_u64().unwrap_or(0) > 0, "{progress}");
        assert_eq!(progress["reports"].as_array().map_or(0, Vec::len), 3, "{progress}");
    }
    for node in 1..3 {
        let addr = cluster.node(node).endpoints.client.to_string();
        let client = Shoal::<TestDbClient>::with_credentials(&addr, Credentials::scram("bob", "bravo")).await.map_err(ok)?;
        let through = repair_status(&client, op).await?;
        assert_eq!(through["groups"], record["groups"], "node {node} holds another record");
    }
    // a second operation, and the control leader killed while its groups are scrubbing
    let leader = cluster.leader_index(0)?.expect("a control leader");
    let survivor = (0..3).find(|node| *node != leader).expect("a survivor");
    let addr = cluster.node(survivor).endpoints.client.to_string();
    let alice_elsewhere = Shoal::<TestDbClient>::with_credentials(&addr, Credentials::scram("alice", "alpha")).await.map_err(ok)?;
    let second = ask_repair(&alice_elsewhere, &mut cluster, verify.clone()).await?;
    cluster.kill(leader)?;
    let record = wait_repair_done(&alice_elsewhere, second, Duration::from_secs(120)).await?;
    for (group, progress) in record["groups"].as_object().expect("groups") {
        assert_eq!(progress["phase"], "Done", "group {group}: {progress}");
        // the killed member reported or did not, depending on when it died; either way the
        // outcome is named and nothing was quarantined
        let outcome = &progress["outcome"];
        assert!(
            outcome["Clean"].is_object() || outcome["Unresolved"].is_object(),
            "group {group} came to something else: {progress}"
        );
        assert!(outcome["Divergent"].is_null(), "group {group} quarantined a copy: {progress}");
    }
    for node in 0..3 {
        if node != leader {
            assert_eq!(cluster.node(node).failure(), None, "node {node} died");
        }
    }
    Ok(())
}

/// A scheduled scrub quarantines a divergent copy with no operator, and installs nothing (C9 M8)
///
/// With `cluster.repair.scrub_interval` set, every group's leader verifies the group on the
/// interval. A partition forgotten on one node is found by the next pass: that node's copy is
/// quarantined and reads through it refused, the committed state names the copy, and no
/// snapshot is installed anywhere - a scheduled pass is verification only, which is Q12's
/// answer ([F44](../../docs/src/features/repair.md)).
#[tokio::test(flavor = "multi_thread")]
async fn scheduled_scrub_quarantines_without_an_operator() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(4))
        .scrub_interval(Duration::from_secs(4))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    let hashed = |key: u64| <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
    for key in 27_000..27_040u64 {
        client.send_one(Note { key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for node in 0..3 {
        compact_now(&mut cluster, node, "Note")?;
    }
    // a pass or two with nothing wrong quarantines nothing
    std::thread::sleep(Duration::from_secs(9));
    for node in 0..3 {
        let view = groups_of(&mut cluster, node)?;
        assert_eq!(view["quarantined"], 0, "node {node} quarantined a clean copy: {}", view["integrity"]);
        assert!(view["integrity"]["scrubs"].as_u64().unwrap_or(0) >= 1, "node {node} was never scrubbed: {}", view["integrity"]);
    }
    // a partition forgotten on node one is found by the next pass
    let forgotten = 27_010u64;
    let answer = cluster.node_mut(1).command(&format!("FORGET Note {:016x}", hashed(forgotten)))?;
    assert_eq!(answer["ok"]["fault"], "forget", "{answer}");
    let member = wait_member_quarantined(&mut cluster, 1, true, Duration::from_secs(40))?;
    assert_eq!(member["quarantined"][0]["reason"], "Divergent", "{member}");
    let (group, _) = group_of(&mut cluster, 0, "Note", forgotten)?;
    assert_eq!(member["quarantined"][0]["group"], u64::from_str_radix(&group, 16).expect("a group id"), "{member}");
    // the copy is quarantined on node one, and a read of the group through it is routed to
    // another holder and served from there
    assert_eq!(groups_of(&mut cluster, 1)?["quarantined"], 1);
    let addr1 = cluster.node(1).endpoints.client.to_string();
    let probe = keys_in_group(&mut cluster, "Note", &group, 27_000, 40)?
        .into_iter()
        .find(|key| *key != forgotten && *key < 27_040)
        .expect("another live key of the group");
    wait_note_routed(&addr1, probe, &format!("note-{probe}"), Duration::from_secs(20)).await?;
    assert_eq!(read_note(&addr0, probe).await.map_err(ok)?, Some(format!("note-{probe}")));
    // and nothing was installed anywhere: a scheduled pass verifies and stops
    for node in 0..3 {
        let view = groups_of(&mut cluster, node)?;
        assert_eq!(view["snapshots"]["installed"], 0, "node {node} installed a snapshot: {}", view["snapshots"]);
        assert_eq!(cluster.node(node).failure(), None, "node {node} died");
    }
    Ok(())
}

/// A corrupt follower is quarantined and repaired from a verified source (C7 M8)
///
/// A follower's record is corrupted in place. The read that meets it is refused by name and
/// quarantines the copy; a `Repair` of that tablet scrubs, finds the copy invalid against a
/// verified majority, cuts a snapshot on the leader past the follower's checkpoint, streams
/// it, restarts the follower's group from its held checkpoint to install it, re-merges what
/// the old generation had above the boundary, scrubs again, and lifts the quarantine once the
/// copies agree. The follower alone installed a snapshot, every digest agrees, and writes
/// after the repair land and compact on it as before ([F44](../../docs/src/features/repair.md)).
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_follower_is_quarantined_and_repaired_from_a_verified_source() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(30))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    let hashed = |key: u64| <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
    for key in 29_000..29_060u64 {
        client.send_one(Note { key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for node in 0..3 {
        wait_checkpointed(&mut cluster, node, "Note", Duration::from_secs(60))?;
    }
    // a group, its leader, and a follower holding it
    let probe = 29_030u64;
    let (group, leader) = group_of(&mut cluster, 0, "Note", probe)?;
    let leader = leader.expect("the group has a leader");
    let follower = (0..3).find(|node| *node != leader).expect("a follower");
    let live: Vec<u64> = keys_in_group(&mut cluster, "Note", &group, 29_000, 8)?.into_iter().filter(|key| *key < 29_060).collect();
    assert!(live.len() >= 2, "too few live keys in group {group}: {live:?}");
    let (corrupted, other) = (live[0], live[1]);
    let answer = cluster.node_mut(follower).command(&format!("CORRUPT Note {:016x}", hashed(corrupted)))?;
    assert_eq!(answer["ok"]["fault"], "corrupt", "{answer}");
    // the read that meets it is refused by name, and the copy is quarantined
    let addr_f = cluster.node(follower).endpoints.client.to_string();
    let read = read_note(&addr_f, corrupted).await;
    assert!(
        matches!(
            failure_code(&read),
            Some(shoal::shared::protocol::error::ErrorCode::CorruptArchive | shoal::shared::protocol::error::ErrorCode::Quarantined)
        ),
        "the corrupt record was served: {read:?}"
    );
    let view = groups_of(&mut cluster, follower)?;
    assert_eq!(view["quarantined"], 1, "{view}");
    wait_member_quarantined(&mut cluster, follower, true, Duration::from_secs(20))?;
    // the repair: scrubbed, judged, installed from the leader, verified and lifted
    let repair = shoal::server::AdminKind::Repair {
        table: "Note".to_string(),
        tablet: Some(tablet_of(corrupted) as u16),
        mode: "repair".to_string(),
        source: None,
        release: false,
    };
    let op = repair_as_process(&mut cluster, 0, &repair)?;
    let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(120))?;
    let group_id = u64::from_str_radix(&group, 16).expect("a group id");
    let progress = &record["groups"][group_id.to_string()];
    let repaired = &progress["outcome"]["Repaired"];
    assert!(repaired.is_object(), "the copy was not repaired: {record}");
    assert_eq!(repaired["source"]["node"], cluster.node_ids()[leader], "{record}");
    assert_eq!(repaired["targets"][0]["node"], cluster.node_ids()[follower], "{record}");
    assert!(repaired["verified"].as_u64().unwrap_or(0) > repaired["boundary"].as_u64().unwrap_or(0), "{record}");
    // the follower alone installed a snapshot, and its quarantine is lifted everywhere
    for node in 0..3 {
        let view = groups_of(&mut cluster, node)?;
        let expected = u64::from(node == follower);
        assert_eq!(view["snapshots"]["installed"], expected, "node {node}: {}", view["snapshots"]);
        assert_eq!(view["quarantined"], 0, "node {node}: {view}");
    }
    wait_member_quarantined(&mut cluster, follower, false, Duration::from_secs(20))?;
    // every key reads through the repaired copy, and every digest agrees
    for key in [corrupted, other, probe] {
        wait_note_routed(&addr_f, key, &format!("note-{key}"), Duration::from_secs(20)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the held checkpoint was released: writes after the repair land, compact and move it
    let before = groups_of(&mut cluster, follower)?;
    let checkpoint_before = applied_by_group(&before, "Note");
    for key in 29_060..29_090u64 {
        client.send_one(Note { key, text: format!("after-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    compact_now(&mut cluster, follower, "Note")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let view = groups_of(&mut cluster, follower)?;
        let checkpoint = view["shards"][0]["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| entry["group"].as_u64() == Some(group_id))
            .and_then(|entry| entry["checkpoint"].as_u64())
            .unwrap_or(0);
        if checkpoint > repaired["boundary"].as_u64().unwrap_or(0) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("the follower's checkpoint stayed held at {checkpoint}: {view}");
        }
        compact_now(&mut cluster, follower, "Note")?;
    }
    let _ = checkpoint_before;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A corrupt primary is repaired from a healthy quorum, and an unresolved split preserves evidence (C9 M8)
///
/// The leader's own record of a group is corrupted: the scrub finds its copy invalid against
/// two verified copies that agree, the leader hands the lead to one of them and that member
/// repairs the old leader from a snapshot. Then a different partition is erased on each of two
/// followers - two valid copies that disagree with each other and with the leader - and a
/// repair stops `Unresolved` with all three digests recorded, nothing quarantined but what the
/// checksums said, nothing installed, and every copy readable as it was. An operator's `source`
/// resolves it: the two are quarantined under the operator's word and repaired from the named
/// node. Every key read through every node afterwards joins a ledger with the inserts that
/// made it, and the oracle accepts the history ([F44](../../docs/src/features/repair.md)).
#[tokio::test(flavor = "multi_thread")]
async fn repair_detects_corrupt_primary_and_preserves_evidence() -> Result<(), FixtureError> {
    use shoal::shared::identity::NodeId;
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(30))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let hashed = |key: u64| <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
    let keys: Vec<u64> = (1..=40).collect();
    // the ledger: every key inserted once, with its value as its text
    let mut ledger = Ledger::default();
    let mut clock = 0u64;
    let mut next_id = 0u32;
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    for key in &keys {
        let attempt = Attempt { id: OpId(next_id), retry: 0 };
        next_id += 1;
        ledger.invoke(attempt, tablet_id(*key), ClientOp::Mutate(MutationOp::Insert { key: Key(*key as u8), value: Value(*key as u32) }), clock);
        clock += 1;
        write_note(&addrs[0], *key, &key.to_string()).await.map_err(ok)?;
        ledger.complete(attempt, clock, Outcome::Ok(OpResult::Applied(true)));
        clock += 1;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for node in 0..3 {
        wait_checkpointed(&mut cluster, node, "Note", Duration::from_secs(60))?;
    }
    // a group and the node that leads it; three live keys of it
    let (group, leader) = group_of(&mut cluster, 0, "Note", keys[0])?;
    let leader = leader.expect("the group has a leader");
    let group_id = u64::from_str_radix(&group, 16).expect("a group id");
    let live: Vec<u64> = keys_in_group(&mut cluster, "Note", &group, 1, 40)?.into_iter().filter(|key| *key <= 40).collect();
    assert!(live.len() >= 3, "too few live keys in group {group}: {live:?}");
    let (on_leader, on_first, on_second) = (live[0], live[1], live[2]);
    let followers: Vec<usize> = (0..3).filter(|node| *node != leader).collect();
    // the primary corrupted: the read that meets it quarantines the leader's own copy
    let answer = cluster.node_mut(leader).command(&format!("CORRUPT Note {:016x}", hashed(on_leader)))?;
    assert_eq!(answer["ok"]["fault"], "corrupt", "{answer}");
    let read = read_note(&addrs[leader], on_leader).await;
    assert!(failure_code(&read).is_some(), "the corrupt primary served its record: {read:?}");
    wait_member_quarantined(&mut cluster, leader, true, Duration::from_secs(20))?;
    let repair = |mode: &str, source: Option<NodeId>| shoal::server::AdminKind::Repair {
        table: "Note".to_string(),
        tablet: Some(tablet_of(on_leader) as u16),
        mode: mode.to_string(),
        source,
        release: false,
    };
    let op = repair_as_process(&mut cluster, followers[0], &repair("repair", None))?;
    let record = wait_repair_done_via(&mut cluster, followers[0], op, Duration::from_secs(180))?;
    let progress = &record["groups"][group_id.to_string()];
    let repaired = &progress["outcome"]["Repaired"];
    assert!(repaired.is_object(), "the primary was not repaired: {record}");
    // the source is a verified member, never the corrupt primary; the lead moved to it
    let source = repaired["source"]["node"].as_str().expect("a source").to_string();
    assert_ne!(source, cluster.node_ids()[leader], "the corrupt primary repaired itself: {record}");
    assert_eq!(repaired["targets"][0]["node"], cluster.node_ids()[leader], "{record}");
    assert_eq!(progress["driver"], source, "the driver is not the source: {record}");
    let (_, new_leader) = group_of(&mut cluster, followers[0], "Note", on_leader)?;
    assert_ne!(new_leader, Some(leader), "the corrupt primary still leads");
    wait_member_quarantined(&mut cluster, leader, false, Duration::from_secs(20))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let installed_before: Vec<u64> = (0..3)
        .map(|node| groups_of(&mut cluster, node).map(|view| view["snapshots"]["installed"].as_u64().unwrap_or(0)))
        .collect::<Result<_, _>>()?;
    assert_eq!(installed_before[leader], 1, "{installed_before:?}");
    // two followers of the group, each with a different partition erased under a valid checksum
    let leader_now = new_leader.expect("a leader");
    let others: Vec<usize> = (0..3).filter(|node| *node != leader_now).collect();
    for (node, key) in others.iter().zip([on_first, on_second]) {
        let answer = cluster.node_mut(*node).command(&format!("ERASE Note {:016x}", hashed(key)))?;
        assert_eq!(answer["ok"]["fault"], "erase", "{answer}");
    }
    let op = repair_as_process(&mut cluster, 0, &repair("repair", None))?;
    let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(120))?;
    let progress = &record["groups"][group_id.to_string()];
    let unresolved = &progress["outcome"]["Unresolved"];
    assert!(unresolved.is_object(), "a three way split was resolved: {record}");
    assert_eq!(unresolved["digests"].as_array().map_or(0, Vec::len), 3, "{record}");
    assert_eq!(unresolved["invalid"], serde_json::json!([]), "{record}");
    // nothing quarantined, nothing installed, every copy readable as it was
    for node in 0..3 {
        let view = groups_of(&mut cluster, node)?;
        assert_eq!(view["quarantined"], 0, "node {node}: {view}");
        assert_eq!(view["snapshots"]["installed"], installed_before[node], "node {node} installed something: {}", view["snapshots"]);
    }
    for (node, key) in others.iter().zip([on_first, on_second]) {
        assert_eq!(read_note(&addrs[*node], key).await.map_err(ok)?, None, "the erased copy on node {node} is not as it was");
        assert_eq!(read_note(&addrs[leader_now], key).await.map_err(ok)?, Some(key.to_string()));
    }
    // the operator names the leader's copy as trusted, and the split resolves
    let trusted = NodeId(cluster.node_ids()[leader_now].parse().expect("a node id"));
    let op = repair_as_process(&mut cluster, 0, &repair("repair", Some(trusted)))?;
    let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(180))?;
    let progress = &record["groups"][group_id.to_string()];
    let repaired = &progress["outcome"]["Repaired"];
    assert!(repaired.is_object(), "the split was not repaired from the named source: {record}");
    assert_eq!(repaired["source"]["node"], cluster.node_ids()[leader_now], "{record}");
    assert_eq!(repaired["targets"].as_array().map_or(0, Vec::len), 2, "{record}");
    assert_eq!(record["source"], cluster.node_ids()[leader_now], "{record}");
    for node in 0..3 {
        wait_member_quarantined(&mut cluster, node, false, Duration::from_secs(20))?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // every key through every node joins the ledger, and the history is sequential
    for addr in &addrs {
        for key in &keys {
            let attempt = Attempt { id: OpId(next_id), retry: 0 };
            next_id += 1;
            ledger.invoke(attempt, tablet_id(*key), ClientOp::Read { key: Key(*key as u8), level: ReadLevel::One }, clock);
            clock += 1;
            let seen = read_note(addr, *key).await.map_err(ok)?.map(|text| Value(text.parse().expect("a value")));
            ledger.complete(attempt, clock, Outcome::Ok(OpResult::Value(seen)));
            clock += 1;
        }
    }
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A repair install is atomic at every crash point (C7 M8)
///
/// Node two, armed to die at one of the seven points of an install, has a record corrupted
/// and is repaired: the install kills it at the point. Restarted clean, whatever the crash
/// left is redone or cleaned up - the copy is still quarantined by its marker - and a second
/// repair finds it either installed already and agreeing, which lifts the quarantine, or still
/// corrupt, which installs it again. At every point node two ends with one generation, every
/// digest agrees, every key reads the survivors' value through it, and nothing is left in the
/// install directory ([F44](../../docs/src/features/repair.md)).
#[tokio::test(flavor = "multi_thread")]
async fn repair_install_is_atomic_at_every_crash_point() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(30))
        .snapshot_timeout(Duration::from_secs(10))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    let hashed = |key: u64| <Note as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(&key);
    for key in 31_000..31_060u64 {
        client.send_one(Note { key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for node in 0..3 {
        wait_checkpointed(&mut cluster, node, "Note", Duration::from_secs(60))?;
    }
    // a group node two follows, and its live keys, one per round
    let mut chosen = None;
    for key in 31_000..31_060u64 {
        let (group, leader) = group_of(&mut cluster, 0, "Note", key)?;
        if leader.is_some_and(|leader| leader != 2) {
            chosen = Some((group, key));
            break;
        }
    }
    let (group, first) = chosen.expect("a group node two does not lead");
    let group_id = u64::from_str_radix(&group, 16).expect("a group id");
    let live: Vec<u64> = keys_in_group(&mut cluster, "Note", &group, first, 10)?.into_iter().filter(|key| *key < 31_060).collect();
    assert!(live.len() >= 7, "too few live keys in group {group}: {live:?}");
    let points = ["before_pending", "pending_written", "mid_install", "map_saved", "before_checkpoint", "after_checkpoint", "after_cleanup"];
    for (round, point) in points.iter().enumerate() {
        let key = live[round];
        // armed to die at the point, then its copy corrupted
        let mut staged = cluster.staged(2).clone();
        staged.crash_at = Some((*point).to_string());
        cluster.restart_with(2, NodeKind::Server, Some(staged))?;
        cluster.wait_joined(&[2])?;
        wait_checkpointed(&mut cluster, 2, "Note", Duration::from_secs(60))?;
        let answer = cluster.node_mut(2).command(&format!("CORRUPT Note {:016x}", hashed(key)))?;
        assert_eq!(answer["ok"]["fault"], "corrupt", "at {point}: {answer}");
        // the repair's install reaches the point, and node two dies there
        let repair = shoal::server::AdminKind::Repair {
            table: "Note".to_string(),
            tablet: Some(tablet_of(key) as u16),
            mode: "repair".to_string(),
            source: None,
            release: false,
        };
        let op = repair_as_process(&mut cluster, 0, &repair)?;
        wait_dead(&cluster, 2, Duration::from_secs(90)).map_err(|_| FixtureError::NotReady(format!("node two never died at {point}")))?;
        let _ = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(120))?;
        // back clean: the marker still quarantines the copy, and a second repair finishes it
        cluster.restart(2, NodeKind::Server)?;
        cluster.wait_joined(&[2])?;
        wait_not_installing(&mut cluster, 2, Duration::from_secs(30))
            .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        let view = groups_of(&mut cluster, 2)?;
        assert_eq!(view["quarantined"], 1, "after dying at {point} the copy is not quarantined: {view}");
        let op = repair_as_process(&mut cluster, 0, &repair)?;
        let record = wait_repair_done_via(&mut cluster, 0, op, Duration::from_secs(180))?;
        let outcome = &record["groups"][group_id.to_string()]["outcome"];
        assert!(
            outcome["Repaired"].is_object() || outcome["Clean"].is_object(),
            "after dying at {point} the second repair came to {record}"
        );
        for node in 0..3 {
            assert_eq!(groups_of(&mut cluster, node)?["quarantined"], 0, "after dying at {point} node {node} still holds a quarantine");
        }
        wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))
            .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        // every key of the group reads the survivors' value through node two
        let addr2 = cluster.node(2).endpoints.client.to_string();
        for probe in &live {
            wait_note_routed(&addr2, *probe, &format!("note-{probe}"), Duration::from_secs(20))
                .await
                .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        }
        let install_dir = cluster.dir(2).join("wal").join("Shard-0").join("install");
        let left: Vec<String> = std::fs::read_dir(&install_dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(left.is_empty(), "after dying at {point} the install directory still holds {left:?}");
    }
    for id in 0..2 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A snapshot install is atomic at every crash point (C7 M7)
///
/// Node two is left behind the purge point of every group, restarted armed to die at one of
/// the seven points of an install, and restarted again clean. At every point it comes back
/// with exactly one generation: before the marker it is sent a fresh snapshot; from the marker
/// to the checkpoint the marker is found and the install redone; past the checkpoint the
/// marker is cleaned up and the log feeds it. Every digest agrees afterwards and every key
/// read through it is the survivors' value, never a mix
/// ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_install_is_atomic_at_every_crash_point() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // a base every node holds, so every group is led before anything is killed
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    for key in 19_990..20_000u64 {
        client.send_one(Note { key, text: format!("base-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key, data: format!("base-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(30))?;
    let points = [
        ("before_pending", false),
        ("pending_written", true),
        ("mid_install", true),
        ("map_saved", true),
        ("before_checkpoint", true),
        ("after_checkpoint", false),
        ("after_cleanup", false),
    ];
    for (round, (point, redone)) in points.iter().enumerate() {
        let from = 20_000 + round as u64 * 200;
        leave_behind_purge(&mut cluster, 2, 0, from, 100, point).await?;
        // back, armed to die at the point, which every group's install reaches
        let mut staged = cluster.staged(2).clone();
        staged.crash_at = Some((*point).to_string());
        cluster.restart_with(2, NodeKind::Server, Some(staged))?;
        wait_dead(&cluster, 2, Duration::from_secs(60))
            .map_err(|_| FixtureError::NotReady(format!("node two never died at {point}")))?;
        // back clean: whatever the crash left is redone or cleaned up, and it converges
        cluster.restart(2, NodeKind::Server)?;
        cluster.wait_joined(&[2])?;
        wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))
            .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(60))
            .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        wait_not_installing(&mut cluster, 2, Duration::from_secs(10))
            .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        let stats = snapshots_of(&mut cluster, 2)?;
        if *redone {
            assert!(stats["redone"].as_u64().unwrap_or(0) >= 1, "no install was redone after dying at {point}: {stats}");
        }
        // every key of the round reads the survivors' value through node two
        let addr2 = cluster.node(2).endpoints.client.to_string();
        for key in [from, from + 50, from + 99] {
            let expected = read_note(&addr0, key).await.map_err(ok)?;
            assert!(expected.is_some(), "key {key} is not on node zero");
            wait_note(&addr2, key, expected.as_deref(), Duration::from_secs(10))
                .await
                .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        }
        // no marker or partial is left behind once the install is complete
        let install_dir = cluster.dir(2).join("wal").join("Shard-0").join("install");
        let left: Vec<String> = std::fs::read_dir(&install_dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(left.is_empty(), "after dying at {point} the install directory still holds {left:?}");
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// An installing tablet serves no read, and the rest of the node does (C7 M7)
///
/// Node two is left behind the purge point and comes back with every install paused after its
/// first record. While a persistent group installs, a `One` read of one of its keys through
/// node two is `Unavailable`, `GROUPS` shows the group installing and readiness counts it; a
/// read of the ephemeral table, whose groups installed in memory at once, is served. Once the
/// pause is over every read is the new value ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn installing_tablet_never_serves_partial_state() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    use shoal::shared::protocol::read::ReadLevel;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // a base node two holds, then enough to leave it behind
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    for key in 21_000..21_010u64 {
        write_note(&addr0, key, &format!("old-{key}")).await.map_err(ok)?;
        client.send_one(Row { key, data: format!("old-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(30))?;
    leave_behind_purge(&mut cluster, 2, 0, 21_000, 100, "new").await?;
    // back, with every install held for three seconds after its first record
    let mut staged = cluster.staged(2).clone();
    staged.install_hold_ms = Some(3000);
    cluster.restart_with(2, NodeKind::Server, Some(staged))?;
    cluster.wait_joined(&[2])?;
    let addr2 = cluster.node(2).endpoints.client.to_string();
    // wait for a persistent group to be installing
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let installing_key = loop {
        let view = groups_of(&mut cluster, 2)?;
        let installing = view["shards"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
            .find(|group| group["table_name"] == "Note" && group["installing"] == true)
            .map(|group| group["tablet_ids"].as_array().cloned().unwrap_or_default());
        if let Some(tablets) = installing {
            assert!(view["installing"].as_u64().unwrap_or(0) >= 1, "{view}");
            // readiness counts it too, a report tick behind the shard
            let until = std::time::Instant::now() + Duration::from_millis(1500);
            loop {
                let readiness = cluster.node_mut(2).command("READINESS")?;
                if readiness["ok"]["data"]["replication"]["installing"].as_u64().unwrap_or(0) >= 1 {
                    break;
                }
                assert!(std::time::Instant::now() < until, "readiness never counted the install: {readiness}");
                std::thread::sleep(Duration::from_millis(50));
            }
            // a key of the installing group, from the base every node held
            let key = (21_000..21_010u64)
                .find(|key| tablets.iter().any(|tablet| tablet.as_u64() == Some(tablet_of(*key) as u64)))
                .expect("a base key of the installing group");
            break key;
        }
        assert!(std::time::Instant::now() < deadline, "no group was ever installing: {view}");
        std::thread::sleep(Duration::from_millis(20));
    };
    // a One read of its key through node two is refused, never the old value
    let refused = read_note_with(&addr2, installing_key, &SendOptions::new().read(ReadLevel::One)).await;
    assert_eq!(failure_code(&refused), Some(ErrorCode::Unavailable), "an installing tablet answered: {refused:?}");
    // and the ephemeral table, whose groups install in memory and are not held, is served:
    // answered from what node two holds, which is nothing until its own snapshot lands and
    // the row once it has, and never refused for the persistent table's install
    let client = Shoal::<TestDbClient>::new(&addr2).await.map_err(ok)?;
    match client
        .send_one_with(RowGet::new(vec![21_005]), &SendOptions::new().read(ReadLevel::One))
        .await
    {
        Ok(_) | Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => {}
        Err(error) => panic!("the ephemeral table was not served while a persistent group installed: {error:?}"),
    }
    // once the pause is over and every install is cleaned up - the digests agree once the
    // archives hold the state, and a group is still installing until the checkpoint that
    // carries it is durable - every key reads the new value through node two
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    wait_not_installing(&mut cluster, 2, Duration::from_secs(30))?;
    for key in [installing_key, 21_000, 21_099] {
        let expected = read_note(&addr0, key).await.map_err(ok)?;
        wait_note(&addr2, key, expected.as_deref(), Duration::from_secs(10)).await?;
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Duplicate and dropped chunks are safe, a cut stream resumes from its prefix, and a sender
/// that dies mid-stream leaves one installed copy (C7 M7)
///
/// Node two is left behind the purge point with wide rows, so every group's snapshot is many
/// small chunks, and comes back with the lanes into it cut and healed while the streams run:
/// whatever the lane lost is dropped past the prefix, the end answers where to resume from,
/// and the install completes once. Then it is left behind again and the leader of one of its
/// groups is killed while it streams: the new leader's stream replaces the partial and the
/// group installs once ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_duplicates_and_resume_are_safe() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .snapshot_chunk_bytes(4096)
        .bulk_queue_bytes(12 * 1024)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // a base every node holds, so every group is led before anything is killed
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    for key in 21_990..22_000u64 {
        client.send_one(Note { key, text: format!("base-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key, data: format!("base-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(30))?;
    // wide rows, so every group's snapshot is hundreds of chunks, and the lanes into node
    // two throttled so a stream takes seconds rather than the milliseconds a loopback would
    let wide = "w".repeat(30_000);
    leave_behind_purge(&mut cluster, 2, 0, 22_000, 100, &wide).await?;
    for link in cluster.data_links_into(2) {
        link.throttle(512 * 1024);
    }
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    // cut and heal the lanes into node two while a stream is running: when the bytes held
    // grew since the last look, one is
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let mut cuts = 0;
    let mut last_bytes = 0u64;
    loop {
        let stats = snapshots_of(&mut cluster, 2)?;
        let installed = stats["installed"].as_u64().unwrap_or(0);
        if installed >= 6 {
            break;
        }
        let bytes = stats["bytes_received"].as_u64().unwrap_or(0);
        if bytes > last_bytes && bytes > 64 * 1024 && cuts < 3 {
            for link in cluster.data_links_into(2) {
                link.cut();
            }
            std::thread::sleep(Duration::from_millis(300));
            for link in cluster.data_links_into(2) {
                link.throttle(512 * 1024);
            }
            cuts += 1;
            std::thread::sleep(Duration::from_millis(500));
        }
        last_bytes = bytes;
        assert!(std::time::Instant::now() < deadline, "the installs never completed: {stats}");
        std::thread::sleep(Duration::from_millis(100));
    }
    for link in cluster.data_links_into(2) {
        link.heal();
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(180))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(120))?;
    wait_not_installing(&mut cluster, 2, Duration::from_secs(10))?;
    let stats = snapshots_of(&mut cluster, 2)?;
    assert!(cuts >= 1, "the lanes were never cut while a stream ran: {stats}");
    assert!(
        stats["resumed"].as_u64().unwrap_or(0) + stats["dropped_chunks"].as_u64().unwrap_or(0) >= 1,
        "no stream was resumed or lost a chunk across {cuts} cuts: {stats}"
    );
    assert_eq!(stats["installed"], 6, "every group installs exactly once: {stats}");
    let expected = wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    assert_eq!(expected["rows"], 110, "{expected}");
    // left behind again, and the leader of a group it needs killed while it streams
    leave_behind_purge(&mut cluster, 2, 0, 23_000, 100, &wide).await?;
    let (_, leader) = wait_group_leader(&mut cluster, "Note", 23_000)?;
    let survivor = if leader == 0 { 1 } else { 0 };
    // throttled again, so the leader dies with a stream of its own in flight
    for link in cluster.data_links_into(2) {
        link.throttle(512 * 1024);
    }
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let before = snapshots_of(&mut cluster, 2)?["bytes_received"].as_u64().unwrap_or(0);
    loop {
        let stats = snapshots_of(&mut cluster, 2)?;
        if stats["bytes_received"].as_u64().unwrap_or(0) > before + 64 * 1024 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no stream ever started: {stats}");
        std::thread::sleep(Duration::from_millis(20));
    }
    cluster.kill(leader)?;
    // the throttle holds for the life of a connection: cut and heal, so the survivor's lanes
    // into node two are dialled afresh at full speed
    for link in cluster.data_links_into(2) {
        link.cut();
    }
    std::thread::sleep(Duration::from_millis(200));
    for link in cluster.data_links_into(2) {
        link.heal();
    }
    wait_digests_equal(&mut cluster, &[survivor, 2], "Note", Duration::from_secs(180))?;
    wait_digests_equal(&mut cluster, &[survivor, 2], "Row", Duration::from_secs(120))?;
    wait_not_installing(&mut cluster, 2, Duration::from_secs(10))?;
    // the counters are this start's: every group installed at least once, and a group whose
    // dead leader's stream landed whole before the election installed it and then the new
    // leader's, which is two whole generations and never a mix
    let stats = snapshots_of(&mut cluster, 2)?;
    let installed = stats["installed"].as_u64().unwrap_or(0);
    assert!((6..=12).contains(&installed), "every group installs at least once after the leader died: {stats}");
    let expected = wait_digests_equal(&mut cluster, &[survivor, 2], "Note", Duration::from_secs(30))?;
    assert_eq!(expected["rows"], 210, "{expected}");
    cluster.restart(leader, NodeKind::Server)?;
    cluster.wait_joined(&[leader])?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Retention and recovery memory are bounded (C7 M7, Q9)
///
/// Node two's lanes are cut both ways and the leader takes wide writes well past
/// `retained_bytes`, at a segment size that makes the budget four segments. The sealed WAL
/// on the leader stays under twice the budget, since every sweep past it forces the groups
/// pinning the oldest segments to snapshot and purge; the leader's memory grows by less than
/// a bound; writes keep committing on the majority throughout. Healed, node two is behind
/// the forced purge point, installs snapshots, and converges
/// ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn retention_and_recovery_memory_are_bounded() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(4096)
        .segment_bytes(1024 * 1024)
        .retained_bytes(4 * 1024 * 1024)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    for key in 24_000..24_010u64 {
        client.send_one(Note { key, text: format!("base-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // node two's data lanes cut both ways - its control lanes stay up, so the control plane is
    // not what this measures (item 106) - and the leader written to well past the budget
    for (from, to) in [(0, 2), (1, 2), (2, 0), (2, 1)] {
        cluster.data_link(from, to).cut();
    }
    let rss_before = cluster.node(0).rss_kib();
    let wal_dir = cluster.dir(0).join("wal").join("Shard-0");
    let sealed_bytes = |dir: &std::path::Path| -> u64 {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".wal"))
            .filter_map(|entry| entry.metadata().ok().map(|meta| meta.len()))
            .sum()
    };
    let wide = "r".repeat(20_000);
    let mut most_held = 0u64;
    for key in 24_100..24_900u64 {
        write_note_eventually(&addr0, key, &format!("{wide}-{key}"), Duration::from_secs(15)).await?;
        if key % 25 == 0 {
            // a sweep every so often, and the budget judged after it
            let _ = cluster.node_mut(0).command("COMPACT")?;
            most_held = most_held.max(sealed_bytes(&wal_dir));
        }
    }
    let _ = cluster.node_mut(0).command("COMPACT")?;
    std::thread::sleep(Duration::from_secs(2));
    let _ = cluster.node_mut(0).command("COMPACT")?;
    most_held = most_held.max(sealed_bytes(&wal_dir));
    let stats = snapshots_of(&mut cluster, 0)?;
    // sixteen megabytes of rows went through a four megabyte budget of one megabyte segments:
    // the WAL never held more than twice the budget and the active segment, and purges were forced
    assert!(
        most_held <= 3 * 4 * 1024 * 1024,
        "the sealed WAL on the leader reached {most_held} bytes against a {} byte budget: {stats}",
        4 * 1024 * 1024
    );
    assert!(stats["forced"].as_u64().unwrap_or(0) >= 1, "no purge was ever forced: {stats}");
    let rss_after = cluster.node(0).rss_kib();
    assert!(
        rss_after.saturating_sub(rss_before) < 400 * 1024,
        "the leader grew by {} KiB while a follower was cut off",
        rss_after.saturating_sub(rss_before)
    );
    // healed, node two is behind the forced purge point: it installs and converges
    for (from, to) in [(0, 2), (1, 2), (2, 0), (2, 1)] {
        cluster.data_link(from, to).heal();
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(180))?;
    wait_not_installing(&mut cluster, 2, Duration::from_secs(10))?;
    let installed = snapshots_of(&mut cluster, 2)?;
    assert!(installed["installed"].as_u64().unwrap_or(0) >= 1, "node two caught up without a snapshot: {installed}");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A member called `Down` within the grace keeps its placement, and comes back into it (C3/C7 M7)
///
/// Node one, which leads a third of the groups, is killed and called `Down`: the groups it led
/// elect elsewhere, and the placement and every group's members are unchanged on both
/// survivors. Enough is written to purge past what it held. Restarted within the grace it is
/// `Up` again in the same placement, catches up by snapshot, and leads nothing until an
/// election it wins ([F43](../../docs/src/features/node-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn down_within_grace_moves_no_replicas() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .detector_interval_ms(200)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let (key, group) = key_led_by(&mut cluster, "Note", 1, 25_000)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let client = Shoal::<TestDbClient>::new(&addrs[0]).await.map_err(ok)?;
    for key in 25_500..25_510u64 {
        client.send_one(Note { key, text: format!("base-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key, data: format!("base-{key}") }).await.map_err(ok)?;
    }
    write_note(&addrs[1], key, "v1").await.map_err(ok)?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(30))?;
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
    let members_before = members_of(&mut cluster, 0)?;
    // node one dies and is called down; the groups it led elect elsewhere; nothing moves
    leave_behind_purge(&mut cluster, 1, 0, 25_100, 100, "away").await?;
    let victim = cluster.node_ids()[1].clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let members = cluster.members(0)?;
        let member = members["members"]
            .as_array()
            .and_then(|members| members.iter().find(|m| m["record"]["node"] == victim).cloned())
            .unwrap_or_default();
        if member["health"] == "down" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the dead member was never called down: {members}");
        std::thread::sleep(Duration::from_millis(100));
    }
    let (elected, leader) = wait_group_leader_change(&mut cluster, 0, "Note", key, 1)?;
    assert_eq!(elected, group);
    assert_ne!(leader, 1);
    for at in [0, 2] {
        assert_eq!(placement_of(&mut cluster, at)?, placement_before, "the placement moved on node {at}");
        assert_eq!(members_of(&mut cluster, at)?, members_before, "node {at}'s groups moved");
    }
    // back within the grace: up, in the same placement, caught up by snapshot, leading nothing yet
    cluster.restart(1, NodeKind::Server)?;
    cluster.wait_joined(&[1])?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if health_of(&mut cluster, 0, 1)? == "up" && health_of(&mut cluster, 2, 1)? == "up" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the restarted member was never called up");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(placement_of(&mut cluster, 1)?, placement_before);
    assert_eq!(members_of(&mut cluster, 1)?, members_before);
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(60))?;
    wait_not_installing(&mut cluster, 1, Duration::from_secs(10))?;
    let stats = snapshots_of(&mut cluster, 1)?;
    assert!(stats["installed"].as_u64().unwrap_or(0) >= 1, "the returning member caught up without a snapshot: {stats}");
    // it leads nothing on its return: every group it hosted is led by a survivor
    let view = groups_of(&mut cluster, 1)?;
    let leading = view["leading"].as_u64().unwrap_or(0);
    assert_eq!(leading, 0, "the returning member leads {leading} groups before any election: {view}");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A whole-cluster restart preserves the durable history (C7 M7)
///
/// Writes land with one node's WAL completions held back and a batch of replies dropped on
/// the leader, so some keys are acknowledged, some unknown to their client and some in one
/// node's log only; every node rotates and compacts; every node is killed at once and
/// restarted. Every acknowledged key is on every node once, every unknown key holds one value
/// everywhere, the digests agree, and no segment below a checkpoint was compacted again
/// ([F43](../../docs/src/features/node-recovery.md),
/// [Resolved #104](../../docs/src/appendix/resolved/segments-recompacted-after-restart.md)).
#[tokio::test(flavor = "multi_thread")]
async fn whole_cluster_restart_preserves_durable_history() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(2))
        .query_deadline(Duration::from_secs(2))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 26_000)?;
    let keys = keys_in_group(&mut cluster, "Note", &group, key, 40)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    // the first half acknowledged plainly
    let mut acknowledged: Vec<(u64, String)> = Vec::new();
    for key in &keys[..20] {
        let text = format!("ack-{key}");
        write_note(&addrs[0], *key, &text).await.map_err(ok)?;
        acknowledged.push((*key, text));
    }
    // node two's completions held back and four replies dropped on the leader: the next
    // writes commit on nodes zero and one, some with their answers lost
    let stalled = cluster.node_mut(2).command(&format!("STALL_WAL {group}"))?;
    assert!(stalled.get("ok").is_some(), "{stalled}");
    let dropped = cluster.node_mut(0).command("DROP_REPLIES 4")?;
    assert!(dropped.get("ok").is_some(), "{dropped}");
    let mut unknown: Vec<u64> = Vec::new();
    for key in &keys[20..30] {
        let text = format!("maybe-{key}");
        match write_note_as(&addrs[0], *key, &text, &SendOptions::new().deadline(Duration::from_secs(1))).await {
            Ok(_) => acknowledged.push((*key, text)),
            Err(error) => {
                assert!(
                    matches!(failure_code(&Err::<(), _>(error)), Some(ErrorCode::Timeout | ErrorCode::OutcomeUnknown)),
                    "a write was refused for another reason"
                );
                unknown.push(*key);
            }
        }
    }
    let released = cluster.node_mut(2).command(&format!("RELEASE_WAL {group}"))?;
    assert!(released.get("ok").is_some(), "{released}");
    // the rest acknowledged, then every node rotates and compacts past everything
    for key in &keys[30..] {
        let text = format!("late-{key}");
        write_note_eventually(&addrs[0], *key, &text, Duration::from_secs(15)).await?;
        acknowledged.push((*key, text.clone()));
    }
    let token = write_note_token(&addrs[0], 26_999, "last").await.map_err(ok)?.expect("a committed write carries a token");
    wait_checkpoint_past(&mut cluster, &[0, 1, 2], &group, token.index, Duration::from_secs(60))?;
    let before = wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // every node killed at once, then every node started again
    for id in 0..3 {
        cluster.node_mut(id).kill().map_err(|error| FixtureError::NotReady(format!("{error}")))?;
    }
    for id in 0..3 {
        cluster.restart(id, NodeKind::Server)?;
    }
    cluster.wait_joined(&[0, 1, 2])?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let after = wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    assert_eq!(after["hash"], before["hash"], "the history changed across the restart");
    // no segment below a checkpoint was compacted again on any node
    for id in 0..3 {
        let _ = cluster.node_mut(id).command("COMPACT")?;
        let compacting = compacting_of(&mut cluster, id)?;
        assert!(compacting.is_empty(), "node {id} is compacting a segment below its checkpoint again: {compacting:?}");
    }
    // every acknowledged key is on every node, and every unknown key holds one value everywhere
    for (key, text) in &acknowledged {
        for addr in &addrs {
            assert_eq!(read_note(addr, *key).await.map_err(ok)?.as_deref(), Some(text.as_str()), "key {key} through {addr}");
        }
    }
    for key in &unknown {
        let values: std::collections::BTreeSet<Option<String>> = {
            let mut set = std::collections::BTreeSet::new();
            for addr in &addrs {
                set.insert(read_note(addr, *key).await.map_err(ok)?);
            }
            set
        };
        assert_eq!(values.len(), 1, "unknown key {key} holds several values: {values:?}");
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

/// A four node cluster with three placed at a factor of three and a fourth member unplaced
///
/// The shape every migration test starts from: node three joined after the placement was
/// initialized and holds nothing until a move brings it in
/// ([F45](../../docs/src/features/replica-migration.md)).
///
/// # Arguments
///
/// * `builder` - The rest of the cluster's shape
async fn three_placed_one_spare(builder: cluster::ClusterBuilder) -> Result<Cluster, FixtureError> {
    let mut cluster = builder.cluster(4, CoreClaim::Count(1)).replication_factor(3).lane_links(true).initialize(false).start().await?;
    cluster.initialize(&[0, 1, 2])?;
    Ok(cluster)
}

/// Every key of the persistent table that node zero's map puts in a group, from a start
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `group` - The group, in hex
/// * `from` - The first key to try
/// * `count` - How many to find
fn note_keys_in_group(cluster: &mut Cluster, group: &str, from: u64, count: usize) -> Result<Vec<u64>, FixtureError> {
    keys_in_group(cluster, "Note", group, from, count)
}

/// Whether a node hosts a group, as its own `GROUPS` view has it
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `group` - The group, in hex
fn hosts_group(cluster: &mut Cluster, node: usize, group: &str) -> Result<bool, FixtureError> {
    let view = groups_of(cluster, node)?;
    let wanted = u64::from_str_radix(group, 16).unwrap_or_default();
    Ok(view["shards"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
        .any(|found| found["group"].as_u64() == Some(wanted)))
}

/// A write acknowledged after the destination reports zero lag is on the destination (C8 M9a)
///
/// Three placed nodes and a spare. The set node two leads is moved to node three while
/// writes keep landing on it: the destination is fed as a learner, made a voter through the
/// group's own transition, published, and the source's copy retired. Once the destination
/// has reported no lag, the source's shard holds its shares and a batch through the source is
/// acknowledged well after that report. Every write acknowledged during the move reads back
/// through the destination and both survivors once the source has retired, the source no
/// longer hosts the group, and every digest agrees
/// ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn move_preserves_write_after_zero_lag_report() -> Result<(), FixtureError> {
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(2))
            .catchup_lag(0),
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr3 = cluster.node(3).endpoints.client.to_string();
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let client = Shoal::<TestDbClient>::new(&addr0).await.map_err(ok)?;
    // a set node two leads, and keys in it on both tables
    let (key, group) = key_led_by(&mut cluster, "Note", 2, 9000)?;
    let keys = note_keys_in_group(&mut cluster, &group, 9000, 12)?;
    assert!(keys.contains(&key));
    for key in &keys {
        client.send_one(Note { key: *key, text: format!("before-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key: *key, data: format!("before-{key}") }).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    assert!(hosts_group(&mut cluster, 2, &group)?);
    assert!(!hosts_group(&mut cluster, 3, &group)?);
    // the move, with writes landing on the set throughout; once the destination has reported
    // no lag, the source's shard holds its shares so a batch through the source is answered
    // well after that report, which is exactly the write the barrier is for
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    wait_move_phase(&mut cluster, 0, op, 3, Duration::from_secs(60))?;
    let _ = cluster.node_mut(2).command("HOLD_SHARES 0 1500")?;
    let addr2 = cluster.node(2).endpoints.client.to_string();
    for key in &keys {
        write_note_eventually(&addr2, *key, &format!("held-{key}"), Duration::from_secs(20)).await?;
    }
    let mut round = 0u64;
    let started = std::time::Instant::now();
    let record = loop {
        for key in &keys {
            write_note_eventually(&addr0, *key, &format!("during-{key}-{round}"), Duration::from_secs(15)).await?;
        }
        round += 1;
        let record = cluster.node_mut(0).command(&format!("MOVE_STATUS {op}"))?["ok"].clone();
        if record["phase"] == "Done" {
            break record;
        }
        assert!(started.elapsed() < Duration::from_secs(180), "the move never finished: {record}");
    };
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    // one more round after the source retired, and every key reads back on the destination
    for key in &keys {
        write_note_eventually(&addr0, *key, &format!("after-{key}"), Duration::from_secs(15)).await?;
    }
    for key in &keys {
        wait_note(&addr3, *key, Some(&format!("after-{key}")), Duration::from_secs(15)).await?;
    }
    // the source no longer hosts the group, the destination does, and every copy agrees
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while hosts_group(&mut cluster, 2, &group)? {
        assert!(std::time::Instant::now() < deadline, "node two still hosts the moved group");
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(hosts_group(&mut cluster, 3, &group)?);
    wait_digests_equal(&mut cluster, &[0, 1, 3], "Note", Duration::from_secs(30))?;
    wait_digests_equal(&mut cluster, &[0, 1, 3], "Row", Duration::from_secs(30))?;
    // the record carries the transfer's timings and the map carries the configuration
    let group_record = &record["groups"];
    assert!(group_record.as_object().is_some_and(|groups| groups.len() == 2), "{record}");
    let map = cluster.node_mut(3).command("MAP")?;
    assert_eq!(map["ok"]["configurations"].as_array().map_or(0, Vec::len), 1, "{}", map["ok"]["configurations"]);
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// The record of a move, as one node holds it
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `op` - The operation
fn move_record_via(cluster: &mut Cluster, via: usize, op: uuid::Uuid) -> Result<serde_json::Value, FixtureError> {
    Ok(cluster.node_mut(via).command(&format!("MOVE_STATUS {op}"))?["ok"].clone())
}

/// Where a move phase stands in the order a move goes through, by its record spelling
///
/// # Arguments
///
/// * `phase` - The phase, as the record's JSON spells it
fn move_phase_rank(phase: &serde_json::Value) -> u8 {
    match phase.as_str() {
        Some("Planned") => 1,
        Some("Learner") => 2,
        Some("CatchingUp") => 3,
        Some("Reconfiguring") => 4,
        Some("Configured") => 5,
        Some("Activated") => 6,
        Some("Published") => 7,
        Some("Retiring") => 8,
        Some("Done") => 9,
        // queued is an object
        _ => 0,
    }
}

/// The highest phase any group of a move has reached, and whether the record is done
///
/// # Arguments
///
/// * `record` - The record
fn move_progress(record: &serde_json::Value) -> (u8, bool) {
    let groups = record["groups"].as_object();
    let highest = groups
        .into_iter()
        .flat_map(|groups| groups.values())
        .map(|group| move_phase_rank(&group["phase"]))
        .max()
        .unwrap_or(0);
    let published = record["phase"] == "Published" || record["phase"] == "Done";
    (highest.max(if published { 7 } else { 0 }), record["phase"] == "Done")
}

/// Wait until any group of a move has reached a phase, or the record is done
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `op` - The operation
/// * `rank` - The phase's rank
/// * `within` - How long to wait
fn wait_move_phase(cluster: &mut Cluster, via: usize, op: uuid::Uuid, rank: u8, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let started = Instant::now();
    loop {
        let record = move_record_via(cluster, via, op)?;
        let (highest, done) = move_progress(&record);
        if highest >= rank || done {
            return Ok(record);
        }
        if started.elapsed() > within {
            return Err(FixtureError::ChildFailed(format!("move {op} never reached phase rank {rank}: {record}")));
        }
        std::thread::sleep(Duration::from_millis(30));
    }
}

/// The committed voters of a group, as one node's handle has them, by node index
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `group` - The group, in hex
fn voters_of(cluster: &mut Cluster, node: usize, group: &str) -> Result<Vec<usize>, FixtureError> {
    let view = groups_of(cluster, node)?;
    let ids = cluster.node_ids();
    let wanted = u64::from_str_radix(group, 16).unwrap_or_default();
    let found = view["shards"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
        .find(|found| found["group"].as_u64() == Some(wanted))
        .ok_or_else(|| FixtureError::ChildFailed(format!("node {node} does not host group {group}")))?;
    let mut voters: Vec<usize> = found["voters"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|voter| voter["node"].as_str().and_then(|id| ids.iter().position(|known| known == id)))
        .collect();
    voters.sort_unstable();
    Ok(voters)
}

/// Restart the named nodes, and wait until they have joined again
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `dead` - The nodes
fn restart_all(cluster: &mut Cluster, dead: &[usize]) -> Result<(), FixtureError> {
    for id in dead {
        cluster.restart(*id, NodeKind::Server)?;
    }
    if !dead.is_empty() {
        cluster.wait_joined(dead)?;
    }
    Ok(())
}

/// Wait until some node has died on its own, and say which
///
/// A process that exited is a zombie until the fixture reaps it and still answers a signal,
/// so its stdout closing is what says it is gone; a second driver committing the same phase
/// a moment later dies too, which the pause after the first is for.
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `within` - How long to wait
fn wait_any_dead(cluster: &Cluster, within: Duration) -> Result<Vec<usize>, FixtureError> {
    let started = Instant::now();
    let mut dead = Vec::new();
    let mut first_seen: Option<Instant> = None;
    loop {
        for id in 0..cluster.len() {
            if cluster.is_started(id) && !dead.contains(&id) && cluster.node(id).failure().is_some() {
                dead.push(id);
                first_seen.get_or_insert_with(Instant::now);
            }
        }
        if first_seen.is_some_and(|seen| seen.elapsed() > Duration::from_millis(750)) {
            dead.sort_unstable();
            return Ok(dead);
        }
        if started.elapsed() > within {
            return Err(FixtureError::ChildFailed("no node died at the armed phase".to_string()));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A caught-up learner never counts toward the old quorum before the configuration commits (C8 M9a)
///
/// The set node zero leads is moved from node two to node three. Once the destination is
/// being fed and has caught up, the old quorum is made short by one - node one paused and the
/// data lanes between the leader and node two cut - so the leader has itself and a learner
/// with every entry. A write through the leader is acknowledged unknown and never committed:
/// it is not visible on the leader's own copy, and no group passes `Reconfiguring`. Healed,
/// the write commits, the transition commits, and the move finishes with every key on the
/// destination ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn learner_never_counts_before_configuration_commit() -> Result<(), FixtureError> {
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(2))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .detector_interval_ms(200),
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr3 = cluster.node(3).endpoints.client.to_string();
    // a set node zero leads, so the leader survives the cut, with node two as the source
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 9100)?;
    let keys = note_keys_in_group(&mut cluster, &group, 9100, 4)?;
    for key in &keys {
        write_note(&addr0, *key, &format!("base-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the move, and the destination being fed
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    wait_move_phase(&mut cluster, 0, op, 3, Duration::from_secs(60))?;
    // the old quorum short by one: node one paused, node two cut off from the leader
    cluster.node(1).pause()?;
    cluster.data_link(0, 2).cut();
    cluster.data_link(2, 0).cut();
    // a write through the leader is proposed, reaches the learner, and is never acknowledged
    let held = write_note(&addr0, keys[0], "held").await;
    let code = failure_code(&held);
    assert!(
        matches!(
            code,
            Some(ErrorCode::OutcomeUnknown | ErrorCode::NotLeader | ErrorCode::QuorumUnavailable | ErrorCode::Timeout | ErrorCode::Unavailable)
        ),
        "a write with the old quorum short by one was answered {held:?}"
    );
    // not committed: the leader's own copy still reads the base, and nothing was configured
    let seen = read_note(&addr0, keys[0]).await;
    assert!(
        matches!(&seen, Ok(Some(text)) if *text == format!("base-{}", keys[0])) || seen.is_err(),
        "the leader served a write that never committed: {seen:?}"
    );
    let record = move_record_via(&mut cluster, 0, op)?;
    let (highest, _) = move_progress(&record);
    assert!(highest < 5, "a group was configured with the old quorum short by one: {record}");
    // healed: the write commits, the transition commits, and the move finishes
    cluster.node(1).resume()?;
    cluster.data_link(0, 2).heal();
    cluster.data_link(2, 0).heal();
    wait_note(&addr0, keys[0], Some("held"), Duration::from_secs(30)).await?;
    let record = wait_move_done_via(&mut cluster, 0, op, Duration::from_secs(150))?;
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    wait_note(&addr3, keys[0], Some("held"), Duration::from_secs(30)).await?;
    for key in &keys[1..] {
        wait_note(&addr3, *key, Some(&format!("base-{key}")), Duration::from_secs(15)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 3], "Note", Duration::from_secs(60))?;
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A committed data configuration outlives the stale placement the map still carries (C4 M9a)
///
/// The driver of the set node one leads is armed to die right after committing `Configured`,
/// before `Activated` and so before the configuration is published: the group's uniform
/// membership naming node three is committed on the group while every node's map still
/// places the set on node two. Restarted, the driver finishes the record forward from the
/// group's committed membership - the destination's voters name node three and never node
/// two again - the move completes, and writes commit through the new configuration
/// ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn data_configuration_outlives_stale_placement_hint() -> Result<(), FixtureError> {
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .detector_interval_ms(200),
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr3 = cluster.node(3).endpoints.client.to_string();
    let (key, group) = key_led_by(&mut cluster, "Note", 1, 9200)?;
    let keys = note_keys_in_group(&mut cluster, &group, 9200, 4)?;
    for key in &keys {
        write_note(&addr0, *key, &format!("base-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // whichever node drives the persistent table's group dies right after committing
    // `Configured`; the ephemeral table's group goes on, so one node dies
    for node in 0..4 {
        let _ = cluster.node_mut(node).command(&format!("MOVE_CRASH_AT configured {group}"))?;
    }
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    let dead = wait_any_dead(&cluster, Duration::from_secs(90))?;
    assert!(!dead.is_empty(), "no driver died at configured");
    let via = (0..4).find(|id| !dead.contains(id) && *id != 2 && *id != 3).unwrap_or(3);
    // committed on the group and not published: the destination's voters name it and not the
    // source, while the map still places the set by the rule
    let record = move_record_via(&mut cluster, via, op)?;
    assert_ne!(record["phase"], "Published", "{record}");
    assert_ne!(record["phase"], "Done", "{record}");
    let configured: Vec<String> = record["groups"]
        .as_object()
        .into_iter()
        .flat_map(|groups| groups.iter())
        .filter(|(_, progress)| move_phase_rank(&progress["phase"]) >= 5)
        .map(|(id, _)| format!("{:016x}", id.parse::<u64>().unwrap_or_default()))
        .collect();
    assert!(!configured.is_empty(), "{record}");
    for group in &configured {
        let voters = voters_of(&mut cluster, 3, group)?;
        assert!(voters.contains(&3) && !voters.contains(&2), "group {group}'s committed voters are {voters:?}");
    }
    let map = cluster.node_mut(via).command("MAP")?;
    assert!(map["ok"]["configurations"].as_array().is_some_and(Vec::is_empty), "{}", map["ok"]["configurations"]);
    // restarted, the record is finished forward, never backward
    restart_all(&mut cluster, &dead)?;
    let record = wait_move_done_via(&mut cluster, via, op, Duration::from_secs(150))?;
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    for group in &configured {
        let voters = voters_of(&mut cluster, 3, group)?;
        assert_eq!(voters, vec![0, 1, 3], "group {group}'s committed voters after the move");
    }
    // writes commit through the new configuration and read back on the destination
    for key in &keys {
        write_note_eventually(&addr0, *key, &format!("after-{key}"), Duration::from_secs(15)).await?;
        wait_note(&addr3, *key, Some(&format!("after-{key}")), Duration::from_secs(15)).await?;
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while hosts_group(&mut cluster, 2, &group)? {
        assert!(Instant::now() < deadline, "node two still hosts the moved group");
        std::thread::sleep(Duration::from_millis(200));
    }
    wait_digests_equal(&mut cluster, &[0, 1, 3], "Note", Duration::from_secs(60))?;
    let map = cluster.node_mut(via).command("MAP")?;
    assert_eq!(map["ok"]["configurations"].as_array().map_or(0, Vec::len), 1, "{}", map["ok"]["configurations"]);
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Every move resumes from its record after the driver, the destination or the control
/// leader fails at every phase (C8 M9a)
///
/// One set is moved back and forth between node two and node three, eighteen times: at each
/// of the six phases a driver commits, once with the driver armed to die right after the
/// commit, once with the destination killed as the phase is reached, and once with the
/// control leader killed there. Every move completes from its record with `Moved`. Throughout,
/// writers through every node update and delete the set's keys under identities with a retry
/// budget; the ledger of their answers and a read of every key on every holder afterwards is
/// accepted by the sequential oracle, and every holder's digest agrees
/// ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn migration_resumes_after_each_phase_failure() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .detector_interval_ms(200),
    )
    .await?;
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 9300)?;
    // six keys per writer, each writer's own, so the oracle's bound on operations per key holds
    let keys = note_keys_in_group(&mut cluster, &group, 9300, 24)?;
    let addrs: Vec<String> = (0..4).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
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
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // writers through every node, each on keys of its own under an identity with a budget,
    // until told to stop or the oracle's bound on operations per key is near
    let mut tasks = Vec::new();
    for node in 0..4 {
        let endpoints = addrs.clone();
        let keys: Vec<u64> = keys[node * 6..node * 6 + 6].to_vec();
        let ledger = ledger.clone();
        let clock = clock.clone();
        let next_id = next_id.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let mut ordered = endpoints.clone();
            ordered.rotate_left(node);
            let mut round = 0u32;
            while !stop.load(Ordering::SeqCst) && round < 26 {
                // a client built per round, so a node killed meanwhile is dialled afresh
                let Ok(client) = Shoal::<TestDbClient>::builder().endpoints(ordered.clone()).build().await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                for (at, key) in keys.iter().enumerate() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let value = Value(node as u32 * 1000 + round + 1);
                    let delete = (round as usize + at + node) % 5 == 0;
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
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                round += 1;
            }
            Ok::<(), shoal::client::Errors>(())
        }));
    }
    // the matrix: each phase a driver commits, under each kind of failure
    let phases = [("learner", 2u8), ("catching_up", 3), ("reconfiguring", 4), ("configured", 5), ("activated", 6), ("retiring", 8)];
    let kinds = ["driver", "destination", "control_leader"];
    let mut holder = 2usize;
    for kind in kinds {
        for (phase, rank) in phases {
            let (from, to) = (holder, if holder == 2 { 3 } else { 2 });
            let via = if from == 0 { 1 } else { 0 };
            eprintln!("--- {kind} at {phase}: moving {from} -> {to}");
            // the driver of the persistent table's group alone, wherever it is: the two
            // groups commit a phase within milliseconds of each other, and two voters of the
            // ephemeral table's group losing their memory log at once is that table's data
            // gone by definition, not a move failing
            if kind == "driver" {
                for node in 0..4 {
                    let _ = cluster.node_mut(node).command(&format!("MOVE_CRASH_AT {phase} {group}"))?;
                }
            }
            let op = move_as_process(&mut cluster, via, key, from, to)?;
            match kind {
                "driver" => {
                    let dead = wait_any_dead(&cluster, Duration::from_secs(120))?;
                    eprintln!("    died: {dead:?}");
                    restart_all(&mut cluster, &dead)?;
                    for node in 0..4 {
                        let _ = cluster.node_mut(node).command("MOVE_CRASH_AT none")?;
                    }
                    assert_eq!(dead.len(), 1, "{kind} at {phase}: more than one node died: {dead:?}");
                }
                "destination" => {
                    wait_move_phase(&mut cluster, via, op, rank, Duration::from_secs(120))?;
                    cluster.kill(to)?;
                    std::thread::sleep(Duration::from_secs(1));
                    cluster.restart(to, NodeKind::Server)?;
                    cluster.wait_joined(&[to])?;
                }
                _ => {
                    wait_move_phase(&mut cluster, via, op, rank, Duration::from_secs(120))?;
                    let leader = cluster.leader_index(via)?.unwrap_or(0);
                    cluster.kill(leader)?;
                    std::thread::sleep(Duration::from_secs(1));
                    cluster.restart(leader, NodeKind::Server)?;
                    cluster.wait_joined(&[leader])?;
                }
            }
            let record = wait_move_done_via(&mut cluster, via, op, Duration::from_secs(240))?;
            assert_eq!(record["outcome"], serde_json::json!("Moved"), "{kind} at {phase}: {record}");
            holder = to;
            let holders: Vec<usize> = (0..4).filter(|id| *id != from).collect();
            wait_digests_equal(&mut cluster, &holders, "Note", Duration::from_secs(90))?;
        }
    }
    stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await.expect("a writer task panicked")?;
    }
    // a read of every key on every holder joins the ledger, and the oracle accepts it
    let holders: Vec<usize> = (0..4).filter(|id| *id != (if holder == 2 { 3 } else { 2 })).collect();
    wait_digests_equal(&mut cluster, &holders, "Note", Duration::from_secs(60))?;
    for node in &holders {
        let addr = cluster.node(*node).endpoints.client.to_string();
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
            let seen = read_note(&addr, *key).await?.map(|text| Value(text.parse().expect("a value")));
            let complete = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Value(seen)));
        }
    }
    let ledger = ledger.lock().unwrap().clone();
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A four node cluster with three placed at a given factor and a fourth member unplaced
///
/// # Arguments
///
/// * `builder` - The rest of the cluster's shape
/// * `rf` - The replication factor
async fn three_placed_one_spare_at(builder: cluster::ClusterBuilder, rf: u32) -> Result<Cluster, FixtureError> {
    let mut cluster = builder.cluster(4, CoreClaim::Count(1)).replication_factor(rf).lane_links(true).initialize(false).start().await?;
    cluster.initialize(&[0, 1, 2])?;
    Ok(cluster)
}

/// A key of the persistent table whose pair is led by one node and completed by another
///
/// At a factor of two a set is a pair, and the third placed node routes to it by forwarding;
/// the key's group is read through the leader, since node zero does not host every set.
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `leader` - The node that has to lead
/// * `other` - The pair's other member
/// * `from` - The first key to try
fn pair_led_by(cluster: &mut Cluster, leader: usize, other: usize, from: u64) -> Result<(u64, String), FixtureError> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for key in from..from + 512 {
            let Ok((group, led)) = group_of(cluster, leader, "Note", key) else {
                continue;
            };
            // the primary too, so a router that holds no copy sends to the leader first
            if led == Some(leader) && primary_of(cluster, leader, &group)? == Some(leader) && hosts_group(cluster, other, &group)? {
                return Ok((key, group));
            }
        }
        if Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("no key from {from} has a pair led by node {leader} with node {other}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The placement primary of a group, as one node's view has its members, by node index
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask
/// * `group` - The group, in hex
fn primary_of(cluster: &mut Cluster, node: usize, group: &str) -> Result<Option<usize>, FixtureError> {
    let view = groups_of(cluster, node)?;
    let ids = cluster.node_ids();
    let wanted = u64::from_str_radix(group, 16).unwrap_or_default();
    Ok(view["shards"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|shard| shard["groups"].as_array().into_iter().flatten())
        .find(|found| found["group"].as_u64() == Some(wanted))
        .and_then(|found| found["members"][0]["node"].as_str().and_then(|id| ids.iter().position(|known| known == id))))
}

/// Keys of the persistent table a given node's map puts in a group, skipping what it does not host
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask, which hosts the group
/// * `group` - The group, in hex
/// * `from` - The first key to try
/// * `count` - How many to find
fn keys_in_group_via(cluster: &mut Cluster, via: usize, group: &str, from: u64, count: usize) -> Result<Vec<u64>, FixtureError> {
    let mut keys = Vec::with_capacity(count);
    for key in from..from + 4096 {
        if keys.len() == count {
            break;
        }
        if let Ok((candidate, _)) = group_of(cluster, via, "Note", key) {
            if candidate == group {
                keys.push(key);
            }
        }
    }
    if keys.len() < count {
        return Err(FixtureError::NotReady(format!("fewer than {count} keys from {from} are served by group {group}")));
    }
    Ok(keys)
}

/// The generations of the WAL segments on a node's first shard, from their file names
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
fn segments_on(cluster: &Cluster, node: usize) -> Vec<u64> {
    let mut generations: Vec<u64> = std::fs::read_dir(cluster.dir(node).join("wal").join("Shard-0"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
                .filter_map(|entry| entry.path().file_stem().and_then(|stem| stem.to_str().and_then(|stem| stem.parse().ok())))
                .collect()
        })
        .unwrap_or_default();
    generations.sort_unstable();
    generations
}

/// Whether a node holds a retired copy's marker for a group
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `group` - The group, in hex
fn has_retired_marker(cluster: &Cluster, node: usize, group: &str) -> bool {
    cluster.dir(node).join("wal").join("Shard-0").join("retired").join(group).exists()
}

/// Whether a partition of the persistent table is in a node's archives, by faulting it harmlessly
///
/// A corruption of a copy the cluster no longer counts is harmless, and the verb refuses a
/// partition the archives do not hold, which is what says the files are gone.
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `key` - The note's key
fn archived_on(cluster: &mut Cluster, node: usize, key: u64) -> Result<bool, FixtureError> {
    use shoal::shared::traits::PartitionKeySupport as _;
    let hash = Note::get_partition_key_from_values(&key);
    let reply = cluster.node_mut(node).command(&format!("CORRUPT Note {hash:016x}"))?;
    Ok(reply.get("ok").is_some())
}

/// Every key of a set read through one node, with what each answered
///
/// # Arguments
///
/// * `addr` - The node's client endpoint
/// * `keys` - The keys
async fn read_all(addr: &str, keys: &[u64]) -> Vec<(u64, Result<Option<String>, shoal::client::Errors>)> {
    let mut seen = Vec::with_capacity(keys.len());
    for key in keys {
        seen.push((*key, read_note(addr, *key).await));
    }
    seen
}

/// A retired copy never serves from its grace files, and the files go after the grace (C8 M9a)
///
/// Three placed at a factor of two and a spare, so a set is a pair and the third placed node
/// routes to it by forwarding. Node one's control lanes are cut so its map stays at the
/// placement while the set node two holds with node zero is moved to node three. After the
/// move, node zero writes new values that only the new configuration holds. A read through
/// node one is forwarded to node two by its stale map; node two refuses it `StaleTopology`
/// rather than answering from the rows it retained, node one sends it once to node zero, and
/// the read is the new value - never the retained one. Node two's marker and archived
/// partitions exist during the grace and are gone after it, and node one reads its own way
/// once its map catches up ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn retired_copy_never_serves_from_grace_files() -> Result<(), FixtureError> {
    let mut cluster = three_placed_one_spare_at(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(6))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .detector_interval_ms(200),
        2,
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr1 = cluster.node(1).endpoints.client.to_string();
    // a set node two leads whose other member is node zero, so node one is the router
    let (key, group) = pair_led_by(&mut cluster, 2, 0, 9400)?;
    let keys = keys_in_group_via(&mut cluster, 2, &group, key, 6)?;
    for key in &keys {
        write_note(&addr0, *key, &format!("old-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 2], "Note", Duration::from_secs(30))?;
    // archived on the source, so the retained rows are files and not only a log
    compact_now(&mut cluster, 2, "Note")?;
    assert!(archived_on(&mut cluster, 2, keys[0])?, "the rows were not archived on the source");
    // node one keeps the placement: nothing the control plane commits reaches it
    for other in [0, 2, 3] {
        cluster.control_link(other, 1).cut();
        cluster.control_link(1, other).cut();
    }
    let stale_version = cluster.node_mut(1).command("MAP")?["ok"]["version"].as_u64().unwrap_or(0);
    // the move, driven and published without node one; the grace is observed from the
    // publication, since the record is done only once the source's files are gone
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    wait_move_phase(&mut cluster, 0, op, 7, Duration::from_secs(150))?;
    let deadline = Instant::now() + Duration::from_secs(20);
    while !has_retired_marker(&cluster, 2, &group) {
        assert!(Instant::now() < deadline, "the source never retired its copy");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(cluster.node_mut(1).command("MAP")?["ok"]["version"].as_u64().unwrap_or(0), stale_version, "node one's map moved");
    // the new configuration holds new values; the retained copy on node two holds the old
    for key in &keys {
        write_note_eventually(&addr0, *key, &format!("new-{key}"), Duration::from_secs(15)).await?;
    }
    assert!(has_retired_marker(&cluster, 2, &group), "no retired marker on the source");
    assert!(archived_on(&mut cluster, 2, keys[0])?, "the retained partition is not on the source during the grace");
    // a read through the stale router: forwarded to the source, refused there, sent on to
    // node zero, and the new value - never the retained one
    let before = read_stats(&mut cluster, 1)?;
    for (key, seen) in read_all(&addr1, &keys).await {
        match seen {
            Ok(Some(text)) => assert_eq!(text, format!("new-{key}"), "a read through the stale router served a retained row"),
            other => panic!("a read through the stale router answered {other:?}"),
        }
    }
    let after = read_stats(&mut cluster, 1)?;
    assert!(
        after["stats"]["stale_refusals"].as_u64() > before["stats"]["stale_refusals"].as_u64(),
        "the source never refused a stale forward: {after}"
    );
    assert!(after["stats"]["reroutes"].as_u64() > before["stats"]["reroutes"].as_u64(), "nothing was sent on: {after}");
    let source = read_stats(&mut cluster, 2)?;
    assert!(source["stats"]["stale_served"].as_u64().unwrap_or(0) > 0, "the source did not refuse by name: {source}");
    // the grace over, the marker and the archived partitions are gone, and the move is done
    let deadline = Instant::now() + Duration::from_secs(40);
    while has_retired_marker(&cluster, 2, &group) {
        assert!(Instant::now() < deadline, "the retired copy was never reclaimed");
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(!archived_on(&mut cluster, 2, keys[0])?, "the retained partition survived the grace");
    let record = wait_move_done_via(&mut cluster, 0, op, Duration::from_secs(150))?;
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    // healed, node one's map catches up and it reads its own way
    for other in [0, 2, 3] {
        cluster.control_link(other, 1).heal();
        cluster.control_link(1, other).heal();
    }
    let current = cluster.node_mut(0).command("MAP")?["ok"]["version"].as_u64().unwrap_or(0);
    cluster.wait_map_version(&[1], current)?;
    for (key, seen) in read_all(&addr1, &keys).await {
        assert!(matches!(&seen, Ok(Some(text)) if *text == format!("new-{key}")), "after healing: {seen:?}");
    }
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Writes through a stale router terminate, in bound, without a duplicate (C4 M9a)
///
/// The same shape: node one's map is held at the placement while the pair node two holds
/// with node zero moves to node three. Writes through node one under identities, during the
/// move and after it, are each answered - the value, or a named error inside the bundle
/// deadline - and the source is killed after it retired, so a forward to it is a lost link
/// rather than a refusal. Every acknowledged key reads back on both holders with exactly the
/// value acknowledged, once ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn stale_routes_terminate_without_duplicate_writes() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = three_placed_one_spare_at(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(4))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .detector_interval_ms(200),
        2,
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr1 = cluster.node(1).endpoints.client.to_string();
    let addr3 = cluster.node(3).endpoints.client.to_string();
    let (key, group) = pair_led_by(&mut cluster, 2, 0, 9500)?;
    let keys = keys_in_group_via(&mut cluster, 2, &group, key, 12)?;
    for key in &keys {
        write_note(&addr0, *key, "base").await?;
    }
    wait_digests_equal(&mut cluster, &[0, 2], "Note", Duration::from_secs(30))?;
    for other in [0, 2, 3] {
        cluster.control_link(other, 1).cut();
        cluster.control_link(1, other).cut();
    }
    let client = Shoal::<TestDbClient>::new(&addr1).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    // a write through the stale router under an identity: the value acknowledged, or a named
    // error inside the bundle deadline, never silence
    let mut acknowledged: Vec<(u64, String)> = Vec::new();
    let mut write_via_stale = |key: u64, value: String| {
        let client = &client;
        async move {
            let started = Instant::now();
            let options = SendOptions::new().identity(uuid::Uuid::new_v4()).retry(Duration::from_secs(15));
            let update = cluster::schema::NoteUpdate {
                partition_key: key,
                text: Some(value.clone()),
            };
            let outcome = client.send_one_with(update, &options).await;
            let elapsed = started.elapsed();
            match outcome {
                Ok(_) => Some(value),
                Err(error) => {
                    let code = failure_code(&Err::<(), _>(error));
                    assert!(
                        matches!(
                            code,
                            Some(
                                ErrorCode::StaleTopology
                                    | ErrorCode::OutcomeUnknown
                                    | ErrorCode::NotLeader
                                    | ErrorCode::Unavailable
                                    | ErrorCode::Timeout
                                    | ErrorCode::QuorumUnavailable
                            )
                        ),
                        "a write through the stale router was answered {code:?} after {elapsed:?}"
                    );
                    assert!(elapsed < Duration::from_secs(25), "a write through the stale router took {elapsed:?}");
                    None
                }
            }
        }
    };
    // during the move
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    for (at, key) in keys.iter().enumerate() {
        if let Some(value) = write_via_stale(*key, format!("during-{at}")).await {
            acknowledged.push((*key, value));
        }
    }
    let record = wait_move_done_via(&mut cluster, 0, op, Duration::from_secs(150))?;
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    // after it, with the source's copy retired
    for (at, key) in keys.iter().enumerate() {
        if let Some(value) = write_via_stale(*key, format!("retired-{at}")).await {
            acknowledged.push((*key, value));
        }
    }
    // and with the source dead: a forward to it is a lost link, sent on the same way
    cluster.kill(2)?;
    for (at, key) in keys.iter().enumerate() {
        if let Some(value) = write_via_stale(*key, format!("dead-{at}")).await {
            acknowledged.push((*key, value));
        }
    }
    assert!(acknowledged.len() > keys.len(), "too few writes through the stale router were acknowledged: {}", acknowledged.len());
    // every acknowledged key reads back on both holders with the value last acknowledged
    let mut last: std::collections::BTreeMap<u64, String> = std::collections::BTreeMap::new();
    for (key, value) in &acknowledged {
        last.insert(*key, value.clone());
    }
    for addr in [&addr0, &addr3] {
        for (key, value) in &last {
            wait_note(addr, *key, Some(value), Duration::from_secs(20)).await?;
        }
    }
    let stats = read_stats(&mut cluster, 1)?;
    assert!(stats["stats"]["stale_refusals"].as_u64().unwrap_or(0) > 0, "no stale refusal was met: {stats}");
    assert!(stats["stats"]["reroutes"].as_u64().unwrap_or(0) > 0, "nothing was sent on: {stats}");
    for other in [0, 3] {
        cluster.control_link(other, 1).heal();
        cluster.control_link(1, other).heal();
    }
    for id in [0, 1, 3] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A shared WAL's cleanup after a retirement preserves the source's other tablets (C8 M9a)
///
/// Three placed at a factor of three and a spare. One set is moved from node two to node
/// three and retired there while writes land on node two's other sets, before and after the
/// move; the WAL is rotated and compacted, node two is restarted, and compacted again. Every
/// key of the other sets reads through node two as it does through node zero, the retired
/// tablets' partitions are gone from node two, and the sealed segments that held the retired
/// group's frames beside the others' are reclaimed
/// ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn shared_wal_cleanup_preserves_other_tablets() -> Result<(), FixtureError> {
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(2))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .checkpoint_entries(8)
            .retained_entries(16)
            .segment_bytes(64 * 1024),
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let (key, moved) = key_led_by(&mut cluster, "Note", 2, 9600)?;
    let moved_keys = note_keys_in_group(&mut cluster, &moved, 9600, 8)?;
    // keys of every other set node two hosts
    let mut others = Vec::new();
    for candidate in 9600..9800u64 {
        let (group, _) = group_of(&mut cluster, 0, "Note", candidate)?;
        if group != moved {
            others.push(candidate);
        }
        if others.len() == 24 {
            break;
        }
    }
    for key in moved_keys.iter().chain(&others) {
        write_note(&addr0, *key, &format!("before-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the segments that hold the retired group's frames beside the others': every one on
    // disk before the move
    let _ = cluster.node_mut(2).command("ROTATE")?;
    let holding = segments_on(&cluster, 2);
    assert!(!holding.is_empty());
    // the move, with writes landing on the other sets throughout
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    for key in &others {
        write_note_eventually(&addr0, *key, &format!("during-{key}"), Duration::from_secs(15)).await?;
    }
    let record = wait_move_done_via(&mut cluster, 0, op, Duration::from_secs(150))?;
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    let deadline = Instant::now() + Duration::from_secs(40);
    while has_retired_marker(&cluster, 2, &moved) {
        assert!(Instant::now() < deadline, "the retired copy was never reclaimed");
        std::thread::sleep(Duration::from_millis(250));
    }
    // rotate and compact, restart, and compact again
    for key in &others {
        write_note_eventually(&addr0, *key, &format!("after-{key}"), Duration::from_secs(15)).await?;
    }
    let _ = cluster.node_mut(2).command("ROTATE")?;
    let _ = cluster.node_mut(2).command("COMPACT")?;
    std::thread::sleep(Duration::from_secs(1));
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    // a restarted node binds its client port afresh
    let addr2 = cluster.node(2).endpoints.client.to_string();
    for key in &others {
        write_note_eventually(&addr0, *key, &format!("final-{key}"), Duration::from_secs(15)).await?;
    }
    let _ = cluster.node_mut(2).command("ROTATE")?;
    let _ = cluster.node_mut(2).command("COMPACT")?;
    // every key of the other sets reads through node two as through node zero
    for key in &others {
        wait_note(&addr2, *key, Some(&format!("final-{key}")), Duration::from_secs(20)).await?;
        wait_note(&addr0, *key, Some(&format!("final-{key}")), Duration::from_secs(20)).await?;
    }
    // the retired tablets' partitions are gone from node two, and it does not host the group
    assert!(!hosts_group(&mut cluster, 2, &moved)?, "node two still hosts the moved group");
    assert!(!archived_on(&mut cluster, 2, moved_keys[0])?, "a retired partition is still archived on node two");
    assert!(!has_retired_marker(&cluster, 2, &moved));
    // and the moved set is whole on its new holders
    for key in &moved_keys {
        wait_note(&cluster.node(3).endpoints.client.to_string(), *key, Some(&format!("before-{key}")), Duration::from_secs(20)).await?;
    }
    // the segments that held the retired group's frames beside the others' are reclaimed
    // once the others purge past them: driven by more writes, rotations and compactions
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut round = 0u64;
    loop {
        for key in &others {
            write_note_eventually(&addr0, *key, &format!("churn-{round}-{key}"), Duration::from_secs(15)).await?;
        }
        round += 1;
        let _ = cluster.node_mut(2).command("ROTATE")?;
        let _ = cluster.node_mut(2).command("COMPACT")?;
        std::thread::sleep(Duration::from_millis(500));
        let remaining: Vec<u64> = segments_on(&cluster, 2).into_iter().filter(|generation| holding.contains(generation)).collect();
        if remaining.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "the segments holding the retired group's frames were never reclaimed: {remaining:?} of {holding:?}");
    }
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A retry identity survives a checkpoint and a move, and one past the window is refused (C5 M9a)
///
/// A note is written under a time-ordered identity through the leader of its set, the leader
/// checkpoints past it, and the set is moved from node two to node three. A retry of the
/// identity through the destination and through the leader is the original result, once: the
/// row still holds what the first write put there. The identity under a changed payload is
/// refused by name. An identity minted before the window is `IdentityExpired` through every
/// node, and a fresh identity that is not time-ordered is applied
/// ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn retry_identity_survives_snapshot_and_migration() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(2))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .checkpoint_entries(8)
            .retained_entries(16)
            .retry_window(Duration::from_secs(30)),
    )
    .await?;
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 9700)?;
    let keys = note_keys_in_group(&mut cluster, &group, 9700, 6)?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // the write under a time-ordered identity, answered once
    let identity = uuid::Uuid::now_v7();
    let options = SendOptions::new().identity(identity);
    write_note_as(&addr0, key, "first", &options).await?;
    for other in &keys[1..] {
        write_note(&addr0, *other, "filler").await?;
    }
    // checkpointed past it on the leader, so the identity lives in the sidecar and the trailer
    for round in 0..3u32 {
        for other in &keys[1..] {
            write_note(&addr0, *other, &format!("filler-{round}")).await?;
        }
        compact_now(&mut cluster, 0, "Note")?;
    }
    wait_checkpointed(&mut cluster, 0, "Note", Duration::from_secs(30))?;
    // the set moved to node three
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    let record = wait_move_done_via(&mut cluster, 0, op, Duration::from_secs(150))?;
    assert_eq!(record["outcome"], serde_json::json!("Moved"), "{record}");
    let addr3 = cluster.node(3).endpoints.client.to_string();
    wait_note(&addr3, key, Some("first"), Duration::from_secs(20)).await?;
    // a retry through the destination and through the leader is the original result, once
    let retried = write_note_as(&addr3, key, "first", &options).await?;
    assert!(retried.suceeded(shoal::client::QuerySuceededOpts::default()).is_ok());
    let retried = write_note_as(&addr0, key, "first", &options).await?;
    assert!(retried.suceeded(shoal::client::QuerySuceededOpts::default()).is_ok());
    // and a changed payload under the same identity is refused by name
    let reused = write_note_as(&addr3, key, "second", &options).await;
    match reused {
        Err(shoal::client::Errors::Server { msg, .. }) => assert!(msg.contains("reused"), "{msg}"),
        Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => {}
        other => panic!("a reused identity with another payload was answered {other:?}"),
    }
    wait_note(&addr3, key, Some("first"), Duration::from_secs(10)).await?;
    wait_note(&addr0, key, Some("first"), Duration::from_secs(10)).await?;
    // an identity minted before the window is expired through every node
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("after the epoch");
    let stale = uuid::Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, now.as_secs() - 120, 0));
    let expired = SendOptions::new().identity(stale);
    for node in [0, 1, 3] {
        let addr = cluster.node(node).endpoints.client.to_string();
        let answered = write_note_as(&addr, key, "late", &expired).await;
        assert_eq!(failure_code(&answered), Some(ErrorCode::IdentityExpired), "through node {node}: {answered:?}");
    }
    wait_note(&addr3, key, Some("first"), Duration::from_secs(10)).await?;
    // a fresh identity that is not time-ordered is applied
    let random = SendOptions::new().identity(uuid::Uuid::new_v4());
    write_note_as(&addr3, key, "random", &random).await?;
    wait_note(&addr0, key, Some("random"), Duration::from_secs(10)).await?;
    wait_digests_equal(&mut cluster, &[0, 1, 3], "Note", Duration::from_secs(30))?;
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A repair and a move of one set serialize, and writes commit throughout (C9 M9a)
///
/// A verify of the set node zero leads is asked for as it is moved from node two to node
/// three: the repair's group is queued behind the move, writes keep committing, the move
/// finishes, and the repair then runs on the new configuration and reports clean with node
/// three among its reports. A move back asked for while a second verify runs is queued behind
/// the repair and runs after it. A partition corrupted on the source before the first move
/// never reaches the destination: the leader's verified copy is the one fed, and every holder
/// agrees afterwards ([F45](../../docs/src/features/replica-migration.md)).
#[tokio::test(flavor = "multi_thread")]
async fn repair_serializes_with_migration_and_new_commits() -> Result<(), FixtureError> {
    use shoal::server::AdminKind;
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .migration_timeout(Duration::from_secs(300))
            .repair_timeout(Duration::from_secs(60)),
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let (key, group) = key_led_by(&mut cluster, "Note", 0, 9800)?;
    let keys = note_keys_in_group(&mut cluster, &group, 9800, 8)?;
    for key in &keys {
        write_note(&addr0, *key, &format!("base-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the source's archived copy of one partition corrupted: never a snapshot's source
    compact_now(&mut cluster, 2, "Note")?;
    let _ = cluster.node_mut(2).command(&format!("CORRUPT Note {:016x}", {
        use shoal::shared::traits::PartitionKeySupport as _;
        Note::get_partition_key_from_values(&keys[0])
    }))?;
    // the move, and a verify of the set asked for while it runs
    let op = move_as_process(&mut cluster, 0, key, 2, 3)?;
    let verify = AdminKind::Repair {
        table: "Note".to_string(),
        tablet: Some(tablet_of(key) as u16),
        mode: "verify".to_string(),
        source: None,
        release: false,
    };
    let repair = repair_as_process(&mut cluster, 0, &verify)?;
    let record = cluster.node_mut(0).command(&format!("REPAIR_STATUS {repair}"))?["ok"].clone();
    let queued = record["groups"]
        .as_object()
        .into_iter()
        .flat_map(|groups| groups.values())
        .any(|progress| progress["phase"]["Queued"]["behind"] == serde_json::json!(op.to_string()));
    assert!(queued, "the repair was not queued behind the move: {record}");
    // writes commit throughout
    for round in 0..3u32 {
        for key in &keys {
            write_note_eventually(&addr0, *key, &format!("during-{round}-{key}"), Duration::from_secs(15)).await?;
        }
    }
    let moved = wait_move_done_via(&mut cluster, 0, op, Duration::from_secs(150))?;
    assert_eq!(moved["outcome"], serde_json::json!("Moved"), "{moved}");
    // the repair runs on the new configuration: clean, with node three among the reports
    let repaired = wait_repair_done_via(&mut cluster, 0, repair, Duration::from_secs(120))?;
    let ids = cluster.node_ids();
    for (id, progress) in repaired["groups"].as_object().expect("groups") {
        assert!(progress["outcome"]["Clean"].is_object(), "group {id} of the repair: {progress}");
        let reported: Vec<usize> = progress["reports"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|report| report[0]["node"].as_str().and_then(|node| ids.iter().position(|known| known == node)))
            .collect();
        assert!(reported.contains(&3), "node three did not report for group {id}: {progress}");
        assert!(!reported.contains(&2), "the retired source reported for group {id}: {progress}");
    }
    // a second verify under way, and a move back asked for meanwhile: queued behind it
    let second = repair_as_process(&mut cluster, 0, &verify)?;
    let back = move_as_process(&mut cluster, 0, key, 3, 2)?;
    let record = move_record_via(&mut cluster, 0, back)?;
    let behind = record["phase"]["Queued"]["behind"].as_str().map(str::to_string);
    let done_already = wait_repair_done_via(&mut cluster, 0, second, Duration::from_secs(120)).is_ok();
    assert!(behind == Some(second.to_string()) || done_already, "the move was not queued behind the repair: {record}");
    let moved_back = wait_move_done_via(&mut cluster, 0, back, Duration::from_secs(150))?;
    assert_eq!(moved_back["outcome"], serde_json::json!("Moved"), "{moved_back}");
    // every holder agrees, and the corrupted partition was never fed anywhere
    for key in &keys {
        write_note_eventually(&addr0, *key, &format!("after-{key}"), Duration::from_secs(15)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    let integrity = groups_of(&mut cluster, 2)?["integrity"].clone();
    assert_eq!(integrity["checksum_failures"], 0, "the returned copy met a corrupt record: {integrity}");
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

// ========================================================================
// M9b: capacity-aware rebalancing and removal (F46)
// ========================================================================

/// One member as a node's `MEMBERS` view has it, by fixture index
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `at` - The node to ask
/// * `member` - The member
fn member_view(cluster: &mut Cluster, at: usize, member: usize) -> Result<serde_json::Value, FixtureError> {
    let id = cluster.node_ids()[member].clone();
    let view = cluster.members(at)?;
    view["members"]
        .as_array()
        .and_then(|members| members.iter().find(|m| m["record"]["node"] == id).cloned())
        .ok_or_else(|| FixtureError::ChildFailed(format!("node {at} does not know member {member}: {view}")))
}

/// Wait until a member's one-name state, as a node sees it, is the one wanted
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `at` - The node to ask
/// * `member` - The member
/// * `state` - The state's name: `up`, `down`, `leaving`, `removing` or `removed`
/// * `within` - How long to wait
fn wait_member_state(cluster: &mut Cluster, at: usize, member: usize, state: &str, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = Instant::now() + within;
    loop {
        let view = member_view(cluster, at, member)?;
        if view["state_name"] == state {
            return Ok(view);
        }
        if Instant::now() > deadline {
            let plans = cluster.node_mut(at).command("PLANS")?;
            return Err(FixtureError::NotReady(format!("member {member} never became {state} as node {at} sees it: {view}\nplans: {plans}")));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The record of a plan, as one node holds it
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `op` - The plan
fn plan_record_via(cluster: &mut Cluster, via: usize, op: uuid::Uuid) -> Result<serde_json::Value, FixtureError> {
    let reply = cluster.node_mut(via).command(&format!("PLAN_STATUS {op}"))?;
    reply
        .get("ok")
        .cloned()
        .ok_or_else(|| FixtureError::ChildFailed(format!("PLAN_STATUS {op} answered {reply}")))
}

/// Every plan a node holds, done or not, in request order
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
fn plans_via(cluster: &mut Cluster, via: usize) -> Result<Vec<serde_json::Value>, FixtureError> {
    let reply = cluster.node_mut(via).command("PLANS")?;
    Ok(reply["ok"].as_array().cloned().unwrap_or_default())
}

/// Wait until a plan's record is in a phase, or done
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `op` - The plan
/// * `phase` - The phase's name
/// * `within` - How long to wait
fn wait_plan_phase(cluster: &mut Cluster, via: usize, op: uuid::Uuid, phase: &str, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = Instant::now() + within;
    loop {
        let record = plan_record_via(cluster, via, op)?;
        if record["phase"] == phase || record["phase"] == "Done" {
            return Ok(record);
        }
        if Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("plan {op} never reached {phase}: {record}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Ask a node for a placement operation and answer the plan's identity
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `verb` - The command line
fn plan_as_process(cluster: &mut Cluster, via: usize, verb: &str) -> Result<uuid::Uuid, FixtureError> {
    let started = Instant::now();
    loop {
        let reply = cluster.node_mut(via).command(verb)?;
        if let Some(op) = reply["ok"]["op"].as_str() {
            return op.parse().map_err(|error| FixtureError::ChildFailed(format!("{verb} answered {op}: {error}")));
        }
        let electing = reply["error"].as_str().is_some_and(|error| error.starts_with("NotLeader"));
        if !electing || started.elapsed() > Duration::from_secs(30) {
            return Err(FixtureError::ChildFailed(format!("{verb} answered {reply}")));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// The fixture indices of the voters a node names
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `at` - The node to ask
fn voter_indices(cluster: &mut Cluster, at: usize) -> Result<Vec<usize>, FixtureError> {
    let ids = cluster.node_ids();
    let view = cluster.members(at)?;
    let mut voters: Vec<usize> = view["voters"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|voter| voter.as_str().and_then(|id| ids.iter().position(|known| known == id)))
        .collect();
    voters.sort_unstable();
    Ok(voters)
}

/// How many replica sets each node holds, as one node's map serves them, by fixture index
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `at` - The node to ask
fn sets_held(cluster: &mut Cluster, at: usize) -> Result<Vec<usize>, FixtureError> {
    let ids = cluster.node_ids();
    let map = cluster.node_mut(at).command("MAP")?["ok"].clone();
    let placement: Vec<String> = map["placement"].as_array().into_iter().flatten().filter_map(|node| node.as_str().map(str::to_string)).collect();
    let shards: std::collections::HashMap<String, u64> = map["members"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(node, member)| (node.clone(), member["shards"].as_u64().unwrap_or(1)))
        .collect();
    // the rule's sets: every distinct ordered list the rule derives, over the configurations
    let n = placement.len();
    let rf = map["desired_rf"].as_u64().unwrap_or(1).min(n as u64) as usize;
    let configurations: Vec<serde_json::Value> = map["configurations"].as_array().cloned().unwrap_or_default();
    let mut counts = vec![0usize; ids.len()];
    let mut seen: std::collections::HashSet<Vec<(String, u64)>> = std::collections::HashSet::new();
    for tablet in 0..4096usize {
        let rule: Vec<(String, u64)> = (0..rf)
            .map(|k| {
                let node = placement[(tablet + k) % n].clone();
                let shard = (tablet / n) as u64 % shards.get(&node).copied().unwrap_or(1).max(1);
                (node, shard)
            })
            .collect();
        if !seen.insert(rule.clone()) {
            continue;
        }
        // served by the configuration covering the tablet, or the rule
        let served: Vec<String> = configurations
            .iter()
            .find(|configuration| configuration["tablets"].as_array().is_some_and(|tablets| tablets.iter().any(|t| t.as_u64() == Some(tablet as u64))))
            .map(|configuration| configuration["members"].as_array().into_iter().flatten().filter_map(|member| member["node"].as_str().map(str::to_string)).collect())
            .unwrap_or_else(|| rule.iter().map(|(node, _)| node.clone()).collect());
        for node in served {
            if let Some(index) = ids.iter().position(|known| *known == node) {
                counts[index] += 1;
            }
        }
    }
    Ok(counts)
}

/// Grace expiry moves a dead member's sets to the spare, refills its voter seat and
/// tombstones it; its return at a higher incarnation, and a clone of it, are refused as a
/// removed identity with the directory preserved and no group naming it (C8 M9b)
///
/// Four nodes, three placed at a factor of three and a spare. Node one is killed and the
/// leader calls it down; the grace elapses under the leader's count; the member is
/// `removing` under an expiry plan that moves each of its sets to node three; the plan
/// finishes with the member out of the control group and tombstoned, and node three takes
/// its voter seat. Writes and reads go on throughout. Node one started again from its
/// directory, one incarnation later, is refused as removed and its pool fails so; a clone of
/// its directory is refused the same way; the directory is still there
/// ([F46](../../docs/src/features/capacity-rebalancing.md)).
#[tokio::test(flavor = "multi_thread")]
async fn automatic_removal_and_rejoin_preserve_fencing() -> Result<(), FixtureError> {
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .detector_interval_ms(200)
            .auto_remove_after(Some(Duration::from_secs(8)))
            .plan_interval(Duration::from_millis(500))
            .moves_per_node(3),
    )
    .await?;
    cluster.wait_voters(0, 3)?;
    // three of the four vote, in node id order; whether node one is among them is the id's
    let voters_before = voter_indices(&mut cluster, 0)?;
    assert_eq!(voters_before.len(), 3, "{voters_before:?}");
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let addr3 = cluster.node(3).endpoints.client.to_string();
    // rows in every set, so every set has something to move: thirty keys over three sets
    let keys: Vec<u64> = (4000..4030).collect();
    for key in &keys {
        write_note(&addr0, *key, &format!("v1-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 3, 3, 0]);
    // node one dies; its directory as it stands is kept for a clone later
    cluster.kill(1)?;
    let copy = cluster.clone_dir(1)?;
    let down = wait_member_state(&mut cluster, 0, 1, "down", Duration::from_secs(30))?;
    assert!(down["grace"].is_object(), "a down member under the policy has a grace: {down}");
    assert!(down["grace_remaining_ms"].as_u64().is_some_and(|ms| ms <= 8000), "{down}");
    // writes go on meanwhile
    for key in &keys[..4] {
        write_note_eventually(&addr0, *key, &format!("v2-{key}"), Duration::from_secs(20)).await?;
    }
    // the grace elapses and the member is removing under an expiry plan
    let removing = wait_member_state(&mut cluster, 0, 1, "removing", Duration::from_secs(40))?;
    assert!(removing["grace"]["expired"].as_bool().unwrap_or(false), "{removing}");
    let plan: uuid::Uuid = removing["grace"]["plan"].as_str().expect("the expiry names its plan").parse().expect("a uuid");
    let record = plan_record_via(&mut cluster, 0, plan)?;
    assert!(record["kind"]["Expiry"].is_object(), "{record}");
    assert_eq!(record["principal"], "policy");
    // every set moves to node three and the member is removed and tombstoned
    let removed = wait_member_state(&mut cluster, 0, 1, "removed", Duration::from_secs(240))?;
    assert!(removed["grace"].is_null(), "a removed member's grace is gone: {removed}");
    let record = wait_plan_phase(&mut cluster, 0, plan, "Done", Duration::from_secs(60))?;
    assert!(record["outcome"]["Completed"].is_object(), "{record}");
    assert_eq!(record["outcome"]["Completed"]["moved"], 3, "{record}");
    let steps = record["steps"].as_array().expect("steps");
    assert!(steps.iter().all(|step| step["state"] == "Moved"), "{record}");
    let members = cluster.members(0)?;
    let node1 = cluster.node_ids()[1].clone();
    assert!(members["tombstones"][&node1].is_object(), "{members}");
    assert_eq!(members["under_replicated_sets"], 0);
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 0, 3, 3]);
    // its voter seat, if it had one, is refilled by the spare: three voters, none of them node one
    cluster.wait_voters(0, 3)?;
    let voters_after = voter_indices(&mut cluster, 0)?;
    assert_eq!(voters_after, vec![0, 2, 3], "before: {voters_before:?}");
    // every note reads through the spare, and the copies agree
    for key in &keys {
        wait_note(&addr3, *key, Some(&if keys[..4].contains(key) { format!("v2-{key}") } else { format!("v1-{key}") }), Duration::from_secs(30)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 2, 3], "Note", Duration::from_secs(60))?;
    // no group names node one a voter any more
    let view = groups_of(&mut cluster, 3)?;
    for shard in view["shards"].as_array().into_iter().flatten() {
        for group in shard["groups"].as_array().into_iter().flatten() {
            let voters: Vec<&str> = group["voters"].as_array().into_iter().flatten().filter_map(|voter| voter["node"].as_str()).collect();
            assert!(!voters.contains(&node1.as_str()), "group {} still names node one a voter", group["group"]);
        }
    }
    // node one back from its directory, one incarnation later: refused as removed, its
    // pool fails so, and its directory is still there
    cluster.restart(1, NodeKind::Server)?;
    let refused = Cluster::wait_failure(cluster.node(1), Duration::from_secs(60)).expect("the removed node kept running");
    assert!(refused.contains("removed"), "the removed node failed for another reason: {refused}");
    assert!(cluster.dir(1).join(StorageMeta::path(cluster.dir(1)).file_name().expect("a marker name")).exists(), "the directory was not preserved");
    // and a clone of the directory it died with, under the same identity, the same way
    let mut clone = cluster.spawn_clone(1, copy.path())?;
    clone.wait_ready(Duration::from_secs(60))?;
    let refused = Cluster::wait_failure(&clone, Duration::from_secs(60)).expect("the clone kept running");
    assert!(refused.contains("removed"), "the clone failed for another reason: {refused}");
    drop(clone);
    // the cluster went on the whole time: a write and a read through node zero
    write_note_eventually(&addr0, keys[0], "v3", Duration::from_secs(20)).await?;
    wait_note(&addr3, keys[0], Some("v3"), Duration::from_secs(20)).await?;
    assert_eq!(voter_indices(&mut cluster, 0)?, vec![0, 2, 3]);
    for id in [0, 2, 3] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Three nodes at a factor of three, one lost past the grace: removing with a blocked plan
/// naming the missing member, the desired factor still three, two copies serving reads and
/// quorum writes, no tombstone and no copy dropped; a fourth identity joined rebuilds every
/// set on it and completes the removal (C8 M9b)
///
/// The three-node RF=3 case [C8](../../docs/src/distributed/rebalancing.md) singles out:
/// there is no fourth distinct node to rebuild on, so expiry cannot finish, and it says so
/// rather than shrinking the factor or dropping a copy
/// ([F46](../../docs/src/features/capacity-rebalancing.md)).
#[tokio::test(flavor = "multi_thread")]
async fn remove_without_replacement_capacity_stays_blocked() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(4, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .initialize(false)
        .deferred_from(3)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .retire_after(Duration::from_secs(1))
        .catchup_lag(0)
        .detector_interval_ms(200)
        .auto_remove_after(Some(Duration::from_secs(6)))
        .plan_interval(Duration::from_millis(500))
        .moves_per_node(3)
        .start()
        .await?;
    cluster.initialize(&[0, 1, 2])?;
    cluster.wait_voters(0, 3)?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let keys: Vec<u64> = (5000..5030).collect();
    for key in &keys {
        write_note(&addr0, *key, &format!("v1-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // node two is lost for good; its grace elapses and it is removing
    cluster.kill(2)?;
    wait_member_state(&mut cluster, 0, 2, "down", Duration::from_secs(30))?;
    let removing = wait_member_state(&mut cluster, 0, 2, "removing", Duration::from_secs(40))?;
    let plan: uuid::Uuid = removing["grace"]["plan"].as_str().expect("a plan").parse().expect("a uuid");
    // the plan is blocked naming the missing member, and stays so
    let record = wait_plan_phase(&mut cluster, 0, plan, "Blocked", Duration::from_secs(30))?;
    assert_eq!(record["phase"], "Blocked", "{record}");
    let reason = record["blocked"]["reason"].as_str().unwrap_or_default().to_string();
    assert!(reason.contains("a further member is needed"), "{record}");
    assert!(record["steps"].as_array().is_some_and(Vec::is_empty), "{record}");
    // the factor is still three, the shortfall is visible, nothing is tombstoned or dropped
    let members = cluster.members(0)?;
    assert_eq!(members["desired_rf"], 3);
    assert_eq!(members["active_rf"], 3);
    assert_eq!(members["under_replicated_sets"], 3, "{members}");
    assert!(members["tombstones"].as_object().is_some_and(|tombstones| tombstones.is_empty()), "{members}");
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 3, 3, 0]);
    // the two survivors serve reads and quorum writes throughout
    for key in &keys[..5] {
        write_note_eventually(&addr0, *key, &format!("v2-{key}"), Duration::from_secs(20)).await?;
    }
    let addr1 = cluster.node(1).endpoints.client.to_string();
    for key in &keys[..5] {
        wait_note(&addr1, *key, Some(&format!("v2-{key}")), Duration::from_secs(20)).await?;
    }
    std::thread::sleep(Duration::from_secs(2));
    let record = plan_record_via(&mut cluster, 0, plan)?;
    assert_eq!(record["phase"], "Blocked", "the plan moved on without a member to move to: {record}");
    assert_eq!(member_view(&mut cluster, 0, 2)?["state_name"], "removing");
    // a fourth identity joins: the plan runs, every set is rebuilt on it, node two is removed
    cluster.start_deferred(3)?;
    cluster.wait_joined(&[3])?;
    let record = wait_plan_phase(&mut cluster, 0, plan, "Done", Duration::from_secs(240))?;
    assert!(record["outcome"]["Completed"].is_object(), "{record}");
    assert_eq!(record["outcome"]["Completed"]["moved"], 3, "{record}");
    assert!(record["blocked"].is_null(), "{record}");
    wait_member_state(&mut cluster, 0, 2, "removed", Duration::from_secs(60))?;
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 3, 0, 3]);
    let members = cluster.members(0)?;
    assert_eq!(members["under_replicated_sets"], 0, "{members}");
    assert_eq!(members["desired_rf"], 3);
    let addr3 = cluster.node(3).endpoints.client.to_string();
    for key in &keys {
        let expected = if keys[..5].contains(key) { format!("v2-{key}") } else { format!("v1-{key}") };
        wait_note(&addr3, *key, Some(&expected), Duration::from_secs(30)).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 3], "Note", Duration::from_secs(60))?;
    cluster.wait_voters(0, 3)?;
    assert_eq!(voter_indices(&mut cluster, 0)?, vec![0, 1, 3]);
    for id in [0, 1, 3] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// The committed elapsed grace is never lower after a control leader restart, and removal is
/// neither early nor forgotten (C3 M9b)
///
/// Four nodes, three placed and a spare, a twelve second grace. Node one is killed and the
/// leader counts; half way through, the control leader is killed and started again. The
/// elapsed time read through the new leader is at least what was committed before, the
/// member is removing no earlier than the grace after it was called down, and no later than
/// the grace plus two increments and an election ([F46](../../docs/src/features/capacity-rebalancing.md), Q7).
#[tokio::test(flavor = "multi_thread")]
async fn removal_grace_survives_control_leader_restart() -> Result<(), FixtureError> {
    let grace = Duration::from_secs(12);
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .detector_interval_ms(250)
            .auto_remove_after(Some(grace))
            .plan_interval(Duration::from_millis(500)),
    )
    .await?;
    cluster.wait_voters(0, 3)?;
    // node one dies and is called down; the grace opens
    cluster.kill(1)?;
    let down = wait_member_state(&mut cluster, 0, 1, "down", Duration::from_secs(30))?;
    let called_down = Instant::now();
    assert_eq!(down["grace"]["elapsed_ms"], 0, "{down}");
    // half way through, the count has been committed at least once
    let deadline = Instant::now() + Duration::from_secs(10);
    let committed_before = loop {
        let view = member_view(&mut cluster, 0, 1)?;
        let elapsed = view["grace"]["elapsed_ms"].as_u64().unwrap_or(0);
        if elapsed >= 4000 {
            break elapsed;
        }
        assert!(Instant::now() < deadline, "the grace was never counted: {view}");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(committed_before < 12_000, "{committed_before}");
    // the control leader is killed and started again
    let leader = cluster.leader_index(0)?.expect("a leader");
    assert_ne!(leader, 1);
    cluster.kill(leader)?;
    std::thread::sleep(Duration::from_secs(1));
    cluster.restart(leader, NodeKind::Server)?;
    cluster.wait_joined(&[leader])?;
    let via = if leader == 0 { 2 } else { 0 };
    cluster.wait_leader_among(via, &[0, 2, 3], Duration::from_secs(30))?;
    // what the new leader holds is at least what was committed before, and never less after
    let view = member_view(&mut cluster, via, 1)?;
    let after = view["grace"]["elapsed_ms"].as_u64().unwrap_or(0);
    assert!(after >= committed_before, "the count went backwards: {committed_before} then {after}: {view}");
    assert_eq!(view["state_name"], "down", "{view}");
    let mut last = after;
    let deadline = Instant::now() + grace + Duration::from_secs(20);
    let removing_at = loop {
        let view = member_view(&mut cluster, via, 1)?;
        let elapsed = view["grace"]["elapsed_ms"].as_u64().unwrap_or(0);
        assert!(elapsed >= last, "the count went backwards: {last} then {elapsed}: {view}");
        last = elapsed;
        if view["state_name"] == "removing" {
            break Instant::now();
        }
        assert!(Instant::now() < deadline, "the member was never removed: {view}");
        std::thread::sleep(Duration::from_millis(100));
    };
    // neither early nor forgotten: no sooner than the grace, no later than two increments and
    // an election past it
    let took = removing_at.saturating_duration_since(called_down);
    assert!(took >= grace, "removed early: {took:?} of {grace:?}");
    assert!(took <= grace + Duration::from_secs(3) + Duration::from_secs(8), "removed late: {took:?}");
    assert_eq!(last, 12_000, "the expiry commits the whole grace");
    for id in [0, 2, 3] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Maintenance holds a down member past its grace with a constant reported remaining deadline,
/// and resumption removes it at that deadline (C3 M9b)
#[tokio::test(flavor = "multi_thread")]
async fn maintenance_suspends_automatic_removal() -> Result<(), FixtureError> {
    let grace = Duration::from_secs(6);
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .detector_interval_ms(200)
            .auto_remove_after(Some(grace))
            .plan_interval(Duration::from_millis(500)),
    )
    .await?;
    cluster.wait_voters(0, 3)?;
    // maintenance on a member that is up is refused: there is no grace to suspend
    let refused = cluster.node_mut(0).command("MAINTENANCE 1 on")?;
    assert!(refused["error"].as_str().is_some_and(|error| error.contains("no grace")), "{refused}");
    // node one dies; inside the grace, maintenance is switched on
    cluster.kill(1)?;
    wait_member_state(&mut cluster, 0, 1, "down", Duration::from_secs(30))?;
    let reply = cluster.node_mut(0).command("MAINTENANCE 1 on")?;
    assert!(reply["ok"].is_object(), "{reply}");
    let suspended_at = Instant::now();
    // past the grace it is still down, a member, and suspended, with a constant remaining
    std::thread::sleep(grace + Duration::from_secs(2));
    let first = member_view(&mut cluster, 0, 1)?;
    assert_eq!(first["state_name"], "down", "{first}");
    assert_eq!(first["phase"], "member", "{first}");
    assert_eq!(first["grace"]["suspended"], true, "{first}");
    let remaining = first["grace_remaining_ms"].as_u64().expect("a remaining deadline");
    assert!(remaining > 0 && remaining <= 6000, "{first}");
    std::thread::sleep(Duration::from_secs(1));
    let second = member_view(&mut cluster, 0, 1)?;
    assert_eq!(second["grace_remaining_ms"], first["grace_remaining_ms"], "the deadline moved while suspended: {second}");
    assert_eq!(second["state_name"], "down");
    // switched off, the count resumes from where it stood and the member is removing at
    // about the remaining deadline
    let reply = cluster.node_mut(0).command("MAINTENANCE 1 off")?;
    assert!(reply["ok"].is_object(), "{reply}");
    let resumed_at = Instant::now();
    let removing = wait_member_state(&mut cluster, 0, 1, "removing", Duration::from_millis(remaining) + Duration::from_secs(6))?;
    let took = resumed_at.elapsed();
    assert!(took + Duration::from_millis(500) >= Duration::from_millis(remaining), "removed before the remaining deadline: {took:?} of {remaining}ms");
    assert!(removing["grace"]["expired"].as_bool().unwrap_or(false), "{removing}");
    let _ = suspended_at;
    // and once removing, maintenance cannot bring it back
    let refused = cluster.node_mut(0).command("MAINTENANCE 1 on")?;
    assert!(refused["error"].as_str().is_some_and(|error| error.contains("cannot suspend a removal")), "{refused}");
    for id in [0, 2, 3] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Four nodes at three with weights 3:1:1:1 rebalanced to within one set of the feasible
/// weighted byte share, a second plan moves nothing and no move follows two intervals; three
/// nodes at three report the full-copy constraint at once (C8 M9b)
///
/// The heavy node already holds every set, which is the most any member can hold, so its
/// target is capped there and the rest is shared by weight over the three light ones: two
/// sets each, one of them moving off each of the placed light nodes onto the spare
/// ([F46](../../docs/src/features/capacity-rebalancing.md), Q8).
#[tokio::test(flavor = "multi_thread")]
async fn heterogeneous_placement_obeys_feasible_weights() -> Result<(), FixtureError> {
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .detector_interval_ms(200)
            .plan_interval(Duration::from_millis(500))
            .moves_per_node(3)
            .weight(0, 3)
            .weight(1, 1)
            .weight(2, 1)
            .weight(3, 1),
    )
    .await?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    // rows in every set, archived, so the bytes a set is weighed by are real
    let keys: Vec<u64> = (6000..6090).collect();
    for key in &keys {
        write_note(&addr0, *key, &format!("{key}-{}", "x".repeat(200))).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    for node in 0..3 {
        compact_now(&mut cluster, node, "Note")?;
    }
    // the weights are reported, and the bytes held with them, once a report has landed
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let view = member_view(&mut cluster, 0, 1)?;
        if view["held_bytes"].as_u64().is_some_and(|bytes| bytes > 0) {
            break;
        }
        assert!(Instant::now() < deadline, "node one never reported its bytes: {view}");
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(member_view(&mut cluster, 0, 0)?["weight"], 3);
    assert_eq!(member_view(&mut cluster, 0, 3)?["weight"], 1);
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 3, 3, 0]);
    // the rebalance: two sets move onto the spare, one off each light placed node
    let plan = plan_as_process(&mut cluster, 0, "REBALANCE")?;
    let record = wait_plan_phase(&mut cluster, 0, plan, "Done", Duration::from_secs(180))?;
    assert!(record["outcome"]["Completed"].is_object(), "{record}");
    assert_eq!(record["outcome"]["Completed"]["moved"], 2, "{record}");
    let steps = record["steps"].as_array().expect("steps");
    let node0 = cluster.node_ids()[0].clone();
    let node3 = cluster.node_ids()[3].clone();
    assert!(steps.iter().all(|step| step["to"] == node3 && step["from"] != node0), "{record}");
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 2, 2, 2]);
    // held bytes: the heavy node most, the light ones within one set's bytes of each other.
    // the spare was fed by log and holds its rows resident until it compacts, and what a set
    // is weighed by is what the archives hold, so every node compacts first
    for node in 0..4 {
        compact_now(&mut cluster, node, "Note")?;
    }
    std::thread::sleep(Duration::from_secs(2));
    let held: Vec<u64> = (0..4).map(|node| member_view(&mut cluster, 0, node).map(|view| view["held_bytes"].as_u64().unwrap_or(0))).collect::<Result<_, _>>()?;
    let set_bytes = held[0] / 3;
    assert!(held[0] > held[1] && held[0] > held[2] && held[0] > held[3], "{held:?}");
    for pair in [(1, 2), (2, 3), (1, 3)] {
        let (a, b) = (held[pair.0], held[pair.1]);
        assert!(a.abs_diff(b) <= set_bytes, "nodes {} and {} differ by more than a set: {held:?}", pair.0, pair.1);
    }
    // a second rebalance is nothing, and no move follows two intervals
    let moves_before = cluster.node_mut(0).command("MAP")?["ok"]["moves"].as_array().map_or(0, Vec::len);
    let again = plan_as_process(&mut cluster, 0, "REBALANCE")?;
    let record = wait_plan_phase(&mut cluster, 0, again, "Done", Duration::from_secs(30))?;
    assert!(record["outcome"]["Nothing"].is_object(), "{record}");
    assert!(record["steps"].as_array().is_some_and(Vec::is_empty), "{record}");
    std::thread::sleep(Duration::from_millis(1200));
    let moves_after = cluster.node_mut(0).command("MAP")?["ok"]["moves"].as_array().map_or(0, Vec::len);
    assert_eq!(moves_after, moves_before, "a move followed a rebalance that had nothing to do");
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 2, 2, 2]);
    for id in 0..4 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    drop(cluster);
    // the N = RF half: three nodes at three hold every set everywhere, and say so at once
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .detector_interval_ms(200)
        .plan_interval(Duration::from_millis(500))
        .weight(0, 3)
        .start()
        .await?;
    let plan = plan_as_process(&mut cluster, 0, "REBALANCE")?;
    let record = wait_plan_phase(&mut cluster, 0, plan, "Done", Duration::from_secs(30))?;
    let reason = record["outcome"]["Nothing"]["reason"].as_str().unwrap_or_default().to_string();
    assert!(reason.contains("every member holds every set"), "{record}");
    assert!(record["steps"].as_array().is_some_and(Vec::is_empty), "{record}");
    let again = plan_as_process(&mut cluster, 0, "REBALANCE")?;
    let record = wait_plan_phase(&mut cluster, 0, again, "Done", Duration::from_secs(30))?;
    assert!(record["outcome"]["Nothing"].is_object(), "{record}");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// The snapshot counters a node's shards report, folded
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
fn snapshot_stats_of(cluster: &mut Cluster, node: usize) -> Result<serde_json::Value, FixtureError> {
    let view = groups_of(cluster, node)?;
    Ok(view["snapshots"].clone())
}

/// Wait until every group's row count agrees across the nodes hosting it
///
/// The digest verb hashes a node's groups together, so nodes holding different sets never
/// agree on it; at a factor below the node count this compares each group where it is held.
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `nodes` - The nodes
/// * `table` - The table
/// * `within` - How long to wait
fn wait_group_rows_agree(cluster: &mut Cluster, nodes: &[usize], table: &str, within: Duration) -> Result<(), FixtureError> {
    let deadline = Instant::now() + within;
    loop {
        let digests: Vec<serde_json::Value> = nodes.iter().map(|node| digest_of(cluster, *node, table)).collect::<Result<_, _>>()?;
        let mut rows: std::collections::BTreeMap<String, std::collections::BTreeSet<u64>> = std::collections::BTreeMap::new();
        for digest in &digests {
            for (group, count) in digest["groups"].as_object().into_iter().flatten() {
                rows.entry(group.clone()).or_default().insert(count.as_u64().unwrap_or(0));
            }
        }
        if rows.values().all(|counts| counts.len() == 1) {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("the groups of {table} never agreed: {digests:?}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Several sets rebalanced onto one destination from distinct sources under a stream cap of
/// one and a byte budget: peak concurrent streams one, bytes per second under the budget,
/// foreground writes accepted by the oracle throughout, every step moved; a destination under
/// its disk reserve blocks a drain by name and unblocks when the reserve is met (C8 M9b)
///
/// Four placed at a factor of two and a spare of twice their weight, the retention short so a
/// learner is fed a snapshot, every node sending under one byte budget and the spare
/// installing one stream at a time. A rebalance under a cap of four moves per node issues
/// three moves onto the spare at once, from three sources; the spare takes one stream at a
/// time and refuses the others until it is done, and what it receives never passes the
/// budget's bound. Then every
/// member's free bytes are overridden below the reserve: a decommission is planned and
/// blocked naming the reserve, nothing is fed, and lifting the override lets it run
/// ([F46](../../docs/src/features/capacity-rebalancing.md)).
#[tokio::test(flavor = "multi_thread")]
async fn node_transfer_budgets_bound_concurrent_sources() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    const BUDGET: usize = 512 * 1024;
    let mut builder = Cluster::builder()
        .cluster(5, CoreClaim::Count(1))
        .replication_factor(2)
        .lane_links(true)
        .initialize(false)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .retire_after(Duration::from_secs(1))
        .catchup_lag(0)
        .detector_interval_ms(250)
        .checkpoint_entries(8)
        .retained_entries(16)
        .snapshot_chunk_bytes(64 * 1024)
        .plan_interval(Duration::from_millis(500))
        .moves_per_node(4)
        .weight(4, 2);
    for node in 0..5 {
        builder = builder.stream_budget(node, BUDGET, if node == 4 { 1 } else { 2 });
    }
    let mut cluster = builder.start().await?;
    cluster.initialize(&[0, 1, 2, 3])?;
    let addrs: Vec<String> = (0..5).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    // enough archived bytes in every set that a stream takes seconds under the budget
    let text = "x".repeat(2048);
    for batch in 0..20u64 {
        let keys: Vec<u64> = (7000 + batch * 100..7000 + batch * 100 + 100).collect();
        write_notes_batch(&addrs[0], &keys, &text).await?;
    }
    wait_group_rows_agree(&mut cluster, &[0, 1, 2, 3], "Note", Duration::from_secs(60))?;
    for node in 0..4 {
        compact_now(&mut cluster, node, "Note")?;
    }
    assert_eq!(sets_held(&mut cluster, 0)?, vec![2, 2, 2, 2, 0]);
    // writers through node zero under identities with a retry budget, on keys of their own
    let keys: Vec<u64> = (7900..7906).collect();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    for key in &keys {
        let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
        let attempt = Attempt { id, retry: 0 };
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Mutate(MutationOp::Insert { key: Key((*key % 251) as u8), value: Value(0) }), invoke);
        write_note(&addrs[0], *key, "0").await?;
        let complete = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Applied(true)));
    }
    let writer = {
        let endpoints = addrs.clone();
        let keys = keys.clone();
        let (ledger, clock, next_id, stop) = (ledger.clone(), clock.clone(), next_id.clone(), stop.clone());
        tokio::spawn(async move {
            let mut round = 0u32;
            while !stop.load(Ordering::SeqCst) && round < 40 {
                let Ok(client) = Shoal::<TestDbClient>::builder().endpoints(endpoints.clone()).build().await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                for key in &keys {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let value = Value(round + 1);
                    let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
                    let attempt = Attempt { id, retry: 0 };
                    let invoke = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().invoke(attempt, TabletId { table: shoal_model::ids::TableId(1), range: tablet_of(*key) as u16 }, ClientOp::Mutate(MutationOp::Update { key: Key((*key % 251) as u8), value }), invoke);
                    let options = SendOptions::new().identity(uuid::Uuid::new_v4()).retry(Duration::from_secs(20));
                    let update = cluster::schema::NoteUpdate { partition_key: *key, text: Some(value.0.to_string()) };
                    let outcome = match client.send_one_with(update, &options).await {
                        Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                        Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                        Err(_) => Outcome::Unknown,
                    };
                    let complete = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().complete(attempt, complete, outcome);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                round += 1;
            }
            Ok::<(), shoal::client::Errors>(())
        })
    };
    // the rebalance: three sets onto the spare from three sources, planned at once
    let before = snapshot_stats_of(&mut cluster, 4)?;
    let received_before = before["bytes_received"].as_u64().unwrap_or(0);
    let started = Instant::now();
    let plan = plan_as_process(&mut cluster, 0, "REBALANCE")?;
    // sample what the spare has received against the bucket's bound while the plan runs
    let record = loop {
        let record = plan_record_via(&mut cluster, 0, plan)?;
        let stats = snapshot_stats_of(&mut cluster, 4)?;
        let received = stats["bytes_received"].as_u64().unwrap_or(0).saturating_sub(received_before);
        let elapsed = started.elapsed().as_secs_f64();
        // a full bucket to begin with, then the rate, plus a chunk of slack
        let bound = (BUDGET as f64) * (elapsed + 1.0) + 2.0 * 64.0 * 1024.0;
        assert!((received as f64) <= bound, "the spare received {received} bytes in {elapsed:.1}s, over the budget's bound of {bound:.0}: {stats}");
        if record["phase"] == "Done" {
            break record;
        }
        assert!(started.elapsed() < Duration::from_secs(240), "the rebalance never finished: {record}");
        std::thread::sleep(Duration::from_millis(250));
    };
    assert!(record["outcome"]["Completed"].is_object(), "{record}");
    let steps = record["steps"].as_array().expect("steps");
    assert_eq!(steps.len(), 3, "{record}");
    assert!(steps.iter().all(|step| step["state"] == "Moved"), "{record}");
    let sources: std::collections::BTreeSet<&str> = steps.iter().filter_map(|step| step["from"].as_str()).collect();
    assert_eq!(sources.len(), 3, "the three sets did not come from three sources: {record}");
    let node4 = cluster.node_ids()[4].clone();
    assert!(steps.iter().all(|step| step["to"] == node4), "{record}");
    // the spare was fed by snapshot, one stream at a time, and the other was refused for it
    let stats = snapshot_stats_of(&mut cluster, 4)?;
    assert!(stats["installed"].as_u64().unwrap_or(0) >= 3, "the spare was not fed by snapshot: {stats}");
    assert_eq!(stats["peak_streams"], 1, "{stats}");
    assert!(stats["refused_budget"].as_u64().unwrap_or(0) >= 1, "the second stream was never refused: {stats}");
    let mut senders_waited = 0u64;
    for node in 0..4 {
        senders_waited += snapshot_stats_of(&mut cluster, node)?["budget_wait_ns"].as_u64().unwrap_or(0);
    }
    assert!(senders_waited > 0, "no sender ever waited on its budget");
    let held = sets_held(&mut cluster, 0)?;
    assert_eq!(held[4], 3, "{held:?}");
    assert_eq!(held.iter().sum::<usize>(), 8, "{held:?}");
    // then every member is short of its reserve: a decommission is blocked by name and feeds nothing
    for node in 0..5 {
        let _ = cluster.node_mut(node).command("FREE_BYTES 1000")?;
    }
    std::thread::sleep(Duration::from_secs(1));
    let fed_before: u64 = (0..5).map(|node| snapshot_stats_of(&mut cluster, node).map(|stats| stats["bytes_received"].as_u64().unwrap_or(0))).sum::<Result<u64, _>>()?;
    let drain = plan_as_process(&mut cluster, 0, "DECOMMISSION 3")?;
    let record = wait_plan_phase(&mut cluster, 0, drain, "Blocked", Duration::from_secs(30))?;
    assert_eq!(record["phase"], "Blocked", "{record}");
    assert!(record["blocked"]["reason"].as_str().is_some_and(|reason| reason.contains("disk reserve")), "{record}");
    assert!(record["steps"].as_array().is_some_and(Vec::is_empty), "{record}");
    assert_eq!(member_view(&mut cluster, 0, 3)?["state_name"], "leaving");
    std::thread::sleep(Duration::from_secs(2));
    let fed_after: u64 = (0..5).map(|node| snapshot_stats_of(&mut cluster, node).map(|stats| stats["bytes_received"].as_u64().unwrap_or(0))).sum::<Result<u64, _>>()?;
    assert_eq!(fed_after, fed_before, "a blocked plan fed bytes");
    assert_eq!(plan_record_via(&mut cluster, 0, drain)?["phase"], "Blocked");
    // the reserve met again, the drain runs to the end
    for node in 0..5 {
        let _ = cluster.node_mut(node).command("FREE_BYTES none")?;
    }
    let record = wait_plan_phase(&mut cluster, 0, drain, "Done", Duration::from_secs(240))?;
    assert!(record["outcome"]["Completed"].is_object(), "{record}");
    wait_member_state(&mut cluster, 0, 3, "removed", Duration::from_secs(60))?;
    // the writers' history, joined by a read of every key on the holders, is sequential
    stop.store(true, Ordering::SeqCst);
    writer.await.expect("the writer task panicked")?;
    let survivors = [0usize, 1, 2, 4];
    wait_group_rows_agree(&mut cluster, &survivors, "Note", Duration::from_secs(60))?;
    for node in survivors {
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in &keys {
            let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
            let attempt = Attempt { id, retry: 0 };
            let invoke = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Read { key: Key((*key % 251) as u8), level: ReadLevel::One }, invoke);
            let seen = read_note(&addr, *key).await?.map(|text| Value(text.parse().expect("a value")));
            let complete = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Value(seen)));
        }
    }
    let ledger = ledger.lock().unwrap().clone();
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    for id in survivors {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A live member drained one set at a time while writers through every node continue: zero
/// final errors under the oracle, the member leaving then removed, every key on the new
/// holders; the p99 before and during recorded (C8 M9b)
///
/// Three placed at a factor of three and a spare, writers through every node under
/// identities with a retry budget throughout. `DECOMMISSION 1`: every set leaves node one one
/// at a time under `moves_per_node`, node one is leaving the while - it still serves and
/// counts - then removed with its voter seat, if it had one, refilled by the spare. Every
/// write is acknowledged inside its retry budget, the history is sequential, and every key
/// reads back on the new holders. The p99 before and during the drain are printed and
/// carried by the arm's capture; the two-times budget is judged there, not here on a shared
/// machine at smoke scale ([F46](../../docs/src/features/capacity-rebalancing.md)).
#[tokio::test(flavor = "multi_thread")]
async fn decommission_drains_within_supported_load_envelope() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    let mut cluster = three_placed_one_spare(
        Cluster::builder()
            .write_timeout(Duration::from_secs(3))
            .query_deadline(Duration::from_secs(3))
            .retire_after(Duration::from_secs(1))
            .catchup_lag(0)
            .plan_interval(Duration::from_millis(500)),
    )
    .await?;
    cluster.wait_voters(0, 3)?;
    let voters_before = voter_indices(&mut cluster, 0)?;
    let addrs: Vec<String> = (0..4).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    // six keys per writer, one writer per node, inserted before anything is concurrent
    let keys: Vec<u64> = (8000..8024).collect();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let latencies: Arc<Mutex<Vec<(Instant, Duration)>>> = Arc::new(Mutex::new(Vec::new()));
    let unknown = Arc::new(AtomicU64::new(0));
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    for key in &keys {
        let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
        let attempt = Attempt { id, retry: 0 };
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Mutate(MutationOp::Insert { key: Key((*key % 251) as u8), value: Value(0) }), invoke);
        write_note(&addrs[0], *key, "0").await?;
        let complete = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Applied(true)));
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the writers go through the placed nodes: the spare coordinates nothing until a set is
    // on it, and a write sent to it is refused by name rather than forwarded
    let mut tasks = Vec::new();
    for node in 0..4 {
        let endpoints: Vec<String> = addrs[..3].to_vec();
        let keys: Vec<u64> = keys[node * 6..node * 6 + 6].to_vec();
        let (ledger, clock, next_id, stop, latencies, unknown) =
            (ledger.clone(), clock.clone(), next_id.clone(), stop.clone(), latencies.clone(), unknown.clone());
        tasks.push(tokio::spawn(async move {
            let mut ordered = endpoints.clone();
            ordered.rotate_left(node % 3);
            let mut round = 0u32;
            while !stop.load(Ordering::SeqCst) && round < 26 {
                let Ok(client) = Shoal::<TestDbClient>::builder().endpoints(ordered.clone()).build().await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                for (at, key) in keys.iter().enumerate() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let value = Value(node as u32 * 1000 + round + 1);
                    let delete = (round as usize + at + node) % 5 == 0;
                    let op = if delete {
                        MutationOp::Delete { key: Key((*key % 251) as u8) }
                    } else {
                        MutationOp::Update { key: Key((*key % 251) as u8), value }
                    };
                    let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
                    let attempt = Attempt { id, retry: 0 };
                    let invoke = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().invoke(attempt, TabletId { table: shoal_model::ids::TableId(1), range: tablet_of(*key) as u16 }, ClientOp::Mutate(op), invoke);
                    let options = SendOptions::new().identity(uuid::Uuid::new_v4()).retry(Duration::from_secs(20));
                    let sent = Instant::now();
                    let outcome = if delete {
                        match client.send_one_with(cluster::schema::NoteDelete::new(*key), &options).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(_) => Outcome::Unknown,
                        }
                    } else {
                        let update = cluster::schema::NoteUpdate { partition_key: *key, text: Some(value.0.to_string()) };
                        match client.send_one_with(update, &options).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(error) => {
                                eprintln!("writer {node}: {error:?}");
                                Outcome::Unknown
                            }
                        }
                    };
                    if outcome == Outcome::Unknown {
                        unknown.fetch_add(1, Ordering::SeqCst);
                        eprintln!("writer {node} round {round} key {key}: unknown after {:?}", sent.elapsed());
                    }
                    latencies.lock().unwrap().push((sent, sent.elapsed()));
                    let complete = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().complete(attempt, complete, outcome);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                round += 1;
            }
            Ok::<(), shoal::client::Errors>(())
        }));
    }
    // a few seconds of the writers alone, then the drain
    tokio::time::sleep(Duration::from_secs(4)).await;
    let drained_at = Instant::now();
    let plan = plan_as_process(&mut cluster, 0, "DECOMMISSION 1")?;
    // leaving throughout: it still serves and counts, and never holds more than one moving step
    let record = loop {
        let record = plan_record_via(&mut cluster, 0, plan)?;
        let moving = record["steps"].as_array().into_iter().flatten().filter(|step| step["state"] == "Moving").count();
        assert!(moving <= 1, "more than one set moves at a time under a cap of one: {record}");
        if record["phase"] == "Done" {
            break record;
        }
        // leaving while its sets move; the tombstone lands as the plan finishes
        let view = member_view(&mut cluster, 0, 1)?;
        if record["phase"] == "Finishing" {
            assert!(view["state_name"] == "leaving" || view["state_name"] == "removed", "{view}");
        } else {
            assert_eq!(view["state_name"], "leaving", "{view}");
            assert_eq!(view["health"], "up", "{view}");
        }
        assert!(drained_at.elapsed() < Duration::from_secs(300), "the drain never finished: {record}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let finished_at = Instant::now();
    assert!(record["outcome"]["Completed"].is_object(), "{record}");
    assert_eq!(record["outcome"]["Completed"]["moved"], 3, "{record}");
    wait_member_state(&mut cluster, 0, 1, "removed", Duration::from_secs(60))?;
    stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await.expect("a writer task panicked")?;
    }
    // zero final errors: every write was acknowledged inside its retry budget
    assert_eq!(unknown.load(Ordering::SeqCst), 0, "a write was not acknowledged inside its budget");
    // the p99 before and during, for the record; the budget is the arm's to judge
    let samples = latencies.lock().unwrap().clone();
    let p99 = |window: &[Duration]| -> Duration {
        let mut sorted = window.to_vec();
        sorted.sort();
        sorted.get(sorted.len().saturating_sub(1).saturating_mul(99) / 100).copied().unwrap_or_default()
    };
    let before: Vec<Duration> = samples.iter().filter(|(at, _)| *at < drained_at).map(|(_, took)| *took).collect();
    let during: Vec<Duration> = samples.iter().filter(|(at, _)| *at >= drained_at && *at < finished_at).map(|(_, took)| *took).collect();
    eprintln!(
        "decommission: {} writes before at p99 {:?}, {} during at p99 {:?}, drained in {:?}",
        before.len(),
        p99(&before),
        during.len(),
        p99(&during),
        finished_at.saturating_duration_since(drained_at)
    );
    assert!(!before.is_empty() && !during.is_empty());
    // the voter seat, if node one had one, is refilled by the spare
    cluster.wait_voters(0, 3)?;
    assert_eq!(voter_indices(&mut cluster, 0)?, vec![0, 2, 3], "before: {voters_before:?}");
    assert_eq!(sets_held(&mut cluster, 0)?, vec![3, 0, 3, 3]);
    // every key on the new holders, and the history joined by those reads is sequential
    let holders = [0usize, 2, 3];
    wait_digests_equal(&mut cluster, &holders, "Note", Duration::from_secs(60))?;
    for node in holders {
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in &keys {
            let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
            let attempt = Attempt { id, retry: 0 };
            let invoke = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Read { key: Key((*key % 251) as u8), level: ReadLevel::One }, invoke);
            let seen = read_note(&addr, *key).await?.map(|text| Value(text.parse().expect("a value")));
            let complete = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Value(seen)));
        }
    }
    let ledger = ledger.lock().unwrap().clone();
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    // the drained member stopped on its own once it learned it was removed
    let stopped = Cluster::wait_failure(cluster.node(1), Duration::from_secs(30)).expect("the removed node kept running");
    assert!(stopped.contains("removed"), "node one stopped for another reason: {stopped}");
    for id in holders {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A node's file layout survives a change of core count with a crash at every point of the
/// rehome (C8 M9c)
///
/// Three nodes at a factor of three; node two claims four slots on two cores, so from its
/// first start it hosts two slots per executor. Rows land in every set - some archived on node
/// two by a rotate and a compaction, some left in its WAL past the checkpoint - and one delete
/// under an identity is remembered by its retry table. Then, for each of the six points a
/// rehome on a cluster node can die at, node two is restarted with its core count changed and
/// the point armed: it dies there, and is started again clean at the new count. Every round
/// alternates between one executor hosting all four slots and two hosting two each, so the
/// vanishing executor's path and the live donor's are each crossed at every point. After
/// every round the rehome reports the step it redid, every key reads through node two, every
/// group's row counts agree across the three holders, the retry under the remembered identity
/// is still the original result, the vanished executor's files are gone, the hosting holds
/// four slots on the new count, and every peer still records four shards for node two. The
/// ephemeral table's rows, which the rehome moves nothing of, read through node two once its
/// leaders have fed its groups again. Writers through the other two nodes run throughout under
/// identities, and their ledger with a read of every key on every node is accepted by the
/// sequential oracle ([F47](../../docs/src/features/local-rehome.md)).
#[tokio::test(flavor = "multi_thread")]
async fn local_rehome_recovers_after_each_crash_point() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .node_cores(2, CoreClaim::Count(2))
        .slots(2, 4)
        .replication_factor(3)
        .lane_links(true)
        .checkpoint_entries(8)
        .retained_entries(64)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .retry_window(Duration::from_secs(1800))
        .start()
        .await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    // node two claimed four slots on two cores from the start: every peer records four
    let two = cluster.node(2).endpoints.node.clone().expect("node two has an id");
    let record_of = |members: &serde_json::Value| -> serde_json::Value {
        members["members"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|member| member["record"]["node"] == serde_json::json!(two))
            .map(|member| member["record"].clone())
            .unwrap_or_else(|| panic!("node two is not in {members}"))
    };
    let record = record_of(&cluster.members(0)?);
    assert_eq!(record["shards"], 4, "{record}");
    assert_eq!(record["physical"], 2, "{record}");
    let hosting = cluster.node_mut(2).command("HOSTING")?["ok"].clone();
    assert_eq!(hosting["slots"], 4, "{hosting}");
    assert_eq!(hosting["physical"], 2, "{hosting}");
    // rows in every set: a base archived on node two, and a tail left in its WAL
    let client = Shoal::<TestDbClient>::new(&addrs[0]).await.map_err(ok)?;
    let fixed: Vec<u64> = (47_000..47_060u64).collect();
    for key in &fixed[..40] {
        client.send_one(Note { key: *key, text: format!("note-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key: *key, data: format!("row-{key}") }).await.map_err(ok)?;
    }
    // node two's digest folds over two executors where the others fold over one, so the
    // hashes never agree by construction; the per group row counts are what is compared
    wait_group_rows_agree(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let _ = cluster.node_mut(2).command("ROTATE")?;
    let _ = cluster.node_mut(2).command("COMPACT")?;
    wait_checkpointed(&mut cluster, 2, "Note", Duration::from_secs(60))?;
    for key in &fixed[40..] {
        client.send_one(Note { key: *key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    // a delete under an identity through node two, remembered by the group's retry table
    let remembered = uuid::Uuid::new_v4();
    let deleted_key = fixed[59];
    let first = delete_note_as(&addrs[2], deleted_key, &SendOptions::new().identity(remembered).retry(Duration::from_secs(15)))
        .await
        .map_err(ok)?;
    assert_eq!(first.bundle(), remembered);
    wait_group_rows_agree(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // writers through nodes zero and one, on keys of their own, until told to stop
    let writer_keys: Vec<u64> = (47_100..47_112u64).collect();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    for key in &writer_keys {
        let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
        let attempt = Attempt { id, retry: 0 };
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Mutate(MutationOp::Insert { key: Key((*key % 251) as u8), value: Value(0) }), invoke);
        write_note(&addrs[0], *key, "0").await.map_err(ok)?;
        let complete = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Applied(true)));
    }
    let mut tasks = Vec::new();
    for node in 0..2 {
        let endpoints = vec![addrs[0].clone(), addrs[1].clone()];
        let keys: Vec<u64> = writer_keys[node * 6..node * 6 + 6].to_vec();
        let ledger = ledger.clone();
        let clock = clock.clone();
        let next_id = next_id.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let mut ordered = endpoints.clone();
            ordered.rotate_left(node);
            let mut round = 0u32;
            while !stop.load(Ordering::SeqCst) && round < 26 {
                let Ok(client) = Shoal::<TestDbClient>::builder().endpoints(ordered.clone()).build().await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                for (at, key) in keys.iter().enumerate() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let value = Value(node as u32 * 1000 + round + 1);
                    let delete = (round as usize + at + node) % 5 == 0;
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
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                round += 1;
            }
            Ok::<(), shoal::client::Errors>(())
        }));
    }
    // the matrix: every point a cluster node's rehome can die at, alternating between one
    // executor hosting every slot and two hosting two each; a fold happens on no cluster node
    let points = ["planned", "after_archives", "after_log", "after_reclaim", "before_finalize", "after_finalize"];
    let mut cores = 2usize;
    for (round, point) in points.iter().enumerate() {
        let target = if cores == 2 { 1 } else { 2 };
        eprintln!("--- {point}: node two from {cores} to {target} executors");
        // more rows into the WAL between rounds, some archived on node two
        let extra: Vec<u64> = (47_200 + round as u64 * 10..47_210 + round as u64 * 10).collect();
        for key in &extra {
            write_note_eventually(&addrs[0], *key, &format!("note-{key}"), Duration::from_secs(20)).await?;
        }
        if round % 2 == 0 {
            let _ = cluster.node_mut(2).command("ROTATE")?;
            let _ = cluster.node_mut(2).command("COMPACT")?;
        }
        // restarted at the new count and armed to die at the point, it dies there
        let staged = cluster.staged(2).clone();
        cluster.restart_with_overrides(
            2,
            NodeKind::Server,
            Some(staged),
            cluster::ChildOverrides {
                cores: Some(target),
                rehome_crash_at: Some((*point).to_string()),
                ..cluster::ChildOverrides::default()
            },
        )?;
        wait_dead(&cluster, 2, Duration::from_secs(120)).map_err(|_| FixtureError::NotReady(format!("node two never died at {point}")))?;
        // started again clean at the new count, the rehome resumes and finishes
        cluster.restart_with_cores(2, NodeKind::Server, target)?;
        cluster.wait_joined(&[2])?;
        let report = cluster.node_mut(2).command("REHOME")?["ok"].clone();
        assert!(!report.is_null(), "after dying at {point} the restart ran no rehome");
        assert_eq!(report["from"], cores, "after dying at {point}: {report}");
        assert_eq!(report["to"], target, "after dying at {point}: {report}");
        assert!(report["steps_redone"].as_u64().unwrap_or(0) >= 1, "after dying at {point} no step was redone: {report}");
        assert!(report["slots_moved"].as_u64().unwrap_or(0) >= 1, "after dying at {point} no slot moved: {report}");
        assert!(report["groups"].as_u64().unwrap_or(0) >= 1, "after dying at {point} no group moved: {report}");
        // the hosting holds four slots on the new count, and the vanished executor's files are gone
        let hosting = cluster.node_mut(2).command("HOSTING")?["ok"].clone();
        assert_eq!(hosting["slots"], 4, "after dying at {point}: {hosting}");
        assert_eq!(hosting["physical"], target, "after dying at {point}: {hosting}");
        let dirs = cluster.node_mut(2).command("SHARD_DIRS")?["ok"]["executors"].clone();
        let expected: Vec<u64> = (0..target as u64).collect();
        assert_eq!(dirs, serde_json::json!(expected), "after dying at {point} the executors with files are {dirs}");
        // every peer still records four shards for node two, and the executors it runs
        let record = record_of(&cluster.members(0)?);
        assert_eq!(record["shards"], 4, "after dying at {point}: {record}");
        assert_eq!(record["physical"], target, "after dying at {point}: {record}");
        // every key reads through node two, and every group's rows agree across the holders
        let addr2 = cluster.node(2).endpoints.client.to_string();
        for key in fixed.iter().filter(|key| **key != deleted_key).chain(&extra) {
            wait_note_routed(&addr2, *key, &format!("note-{key}"), Duration::from_secs(30))
                .await
                .map_err(|error| FixtureError::NotReady(format!("after dying at {point}, key {key}: {error:?}")))?;
        }
        // the ephemeral table moves nothing: its groups are fed again by their leaders, and
        // every row reads through node two once they have been
        let rows = Shoal::<TestDbClient>::new(&addr2).await.map_err(ok)?;
        for key in &fixed[..40] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let found = match rows.send_one(RowGet::new(vec![*key])).await {
                    Ok(found) => found.access::<Row>().map_err(ok)?.map(|rows| rows.len()).unwrap_or(0),
                    Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => 0,
                    Err(error) => return Err(ok(error)),
                };
                if found == 1 {
                    break;
                }
                assert!(Instant::now() < deadline, "after dying at {point} row {key} never read through node two");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        wait_group_rows_agree(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))
            .map_err(|error| FixtureError::NotReady(format!("after dying at {point}: {error:?}")))?;
        // the remembered identity through node two is still the original result, once
        let again = delete_note_as(&addr2, deleted_key, &SendOptions::new().identity(remembered).retry(Duration::from_secs(20)))
            .await
            .unwrap_or_else(|error| panic!("after dying at {point} the remembered identity was applied as new: {error:?}"));
        assert_eq!(again.bundle(), remembered);
        let fresh = delete_note(&addr2, deleted_key).await;
        assert!(matches!(fresh, Err(shoal::client::Errors::QueryDidNotSucceed { .. })), "after dying at {point}: {fresh:?}");
        cores = target;
    }
    // the writers stop, and a read of every key on every node joins the ledger
    stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await.expect("a writer task panicked").map_err(ok)?;
    }
    wait_group_rows_agree(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    for addr in &addrs {
        for key in &writer_keys {
            let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
            let attempt = Attempt { id, retry: 0 };
            let invoke = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Read { key: Key((*key % 251) as u8), level: ReadLevel::One }, invoke);
            let seen = read_note(addr, *key).await.map_err(ok)?.map(|text| Value(text.parse().expect("a value")));
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

/// A standalone node deals its tablets across a change of core count, growing and shrinking,
/// with a crash at the fold and at the copy on the way down (C8 M9c)
///
/// A standalone node at two executors is seeded with rows on every tablet, some left in its
/// active intent logs. Restarted at three, the two donors deal a third of their tablets to
/// the new executor - the counts within one of even - and every row reads back. Restarted at
/// one and armed to die after the first fold, it dies; started again armed to die after the
/// first copy, the resumed rehome dies there too; started clean, it finishes with two steps
/// redone, every row reads back, and only the survivor's files remain
/// ([F47](../../docs/src/features/local-rehome.md)).
#[tokio::test(flavor = "multi_thread")]
async fn standalone_rehome_rebalances_tablets_across_restarts() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder().standalone(CoreClaim::Count(2)).start().await?;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let addr = cluster.node(0).endpoints.client.to_string();
    // rows on every tablet's executor, then a rotate so some are archived and the rest are in
    // the active logs when the node is restarted
    let client = Shoal::<TestDbClient>::new(&addr).await.map_err(ok)?;
    let keys: Vec<u64> = (51_000..51_400u64).collect();
    for key in &keys[..200] {
        client.send_one(Note { key: *key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    let _ = cluster.node_mut(0).command("ROTATE")?;
    for key in &keys[200..] {
        client.send_one(Note { key: *key, text: format!("note-{key}") }).await.map_err(ok)?;
    }
    let hosting = cluster.node_mut(0).command("HOSTING")?["ok"].clone();
    assert_eq!(hosting["physical"], 2, "{hosting}");
    assert_eq!(hosting["slots"], 2, "{hosting}");
    // grown to three: tablets dealt per tablet, within one of even, every row read back
    cluster.restart_with_cores(0, NodeKind::Standalone, 3)?;
    let addr = cluster.node(0).endpoints.client.to_string();
    let report = cluster.node_mut(0).command("REHOME")?["ok"].clone();
    assert_eq!(report["from"], 2, "{report}");
    assert_eq!(report["to"], 3, "{report}");
    assert!(report["tablets_moved"].as_u64().unwrap_or(0) > 0, "{report}");
    assert!(report["folded"].as_u64().unwrap_or(0) > 0, "a growth folded no intent logs: {report}");
    assert!(report["records"].as_u64().unwrap_or(0) > 0, "{report}");
    assert_eq!(report["steps_redone"], 0, "{report}");
    let hosting = cluster.node_mut(0).command("HOSTING")?["ok"].clone();
    assert_eq!(hosting["physical"], 3, "{hosting}");
    let counts: Vec<u64> = hosting["tablets_per_executor"].as_array().expect("counts").iter().map(|count| count.as_u64().unwrap_or(0)).collect();
    assert_eq!(counts.len(), 3);
    assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1, "{counts:?}");
    for key in &keys {
        wait_note_routed(&addr, *key, &format!("note-{key}"), Duration::from_secs(10)).await?;
    }
    // down to one, dying after the first fold, then after the first copy of the resumed rehome
    for point in ["after_fold", "after_archives"] {
        cluster.restart_with_overrides(
            0,
            NodeKind::Standalone,
            None,
            cluster::ChildOverrides {
                cores: Some(1),
                rehome_crash_at: Some(point.to_string()),
                ..cluster::ChildOverrides::default()
            },
        )?;
        wait_dead(&cluster, 0, Duration::from_secs(60)).map_err(|_| FixtureError::NotReady(format!("the node never died at {point}")))?;
    }
    // started clean, the rehome finishes: two steps redone, every row back, one executor's files
    cluster.restart_with_cores(0, NodeKind::Standalone, 1)?;
    let addr = cluster.node(0).endpoints.client.to_string();
    let report = cluster.node_mut(0).command("REHOME")?["ok"].clone();
    assert_eq!(report["from"], 3, "{report}");
    assert_eq!(report["to"], 1, "{report}");
    assert_eq!(report["steps_redone"], 2, "{report}");
    assert!(report["records"].as_u64().unwrap_or(0) > 0, "{report}");
    let hosting = cluster.node_mut(0).command("HOSTING")?["ok"].clone();
    assert_eq!(hosting["physical"], 1, "{hosting}");
    assert_eq!(hosting["tablets_per_executor"], serde_json::json!([4096]), "{hosting}");
    for key in &keys {
        wait_note_routed(&addr, *key, &format!("note-{key}"), Duration::from_secs(10)).await?;
    }
    let dirs = cluster.node_mut(0).command("SHARD_DIRS")?["ok"]["executors"].clone();
    assert_eq!(dirs, serde_json::json!([0]), "the vanished executors left files: {dirs}");
    assert_eq!(cluster.node(0).failure(), None, "the node died");
    Ok(())
}

/// The wire version every up link of a node negotiated, by (peer index, lane), and the
/// cluster's activated version as the node reports it
/// ([F48](../../docs/src/features/rolling-compatibility.md))
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node to ask
fn wire_of(cluster: &mut Cluster, node: usize) -> Result<(Vec<(usize, String, u8)>, serde_json::Value), FixtureError> {
    let wire = cluster.node_mut(node).command("WIRE")?;
    let Some(ok) = wire.get("ok").cloned() else {
        return Err(FixtureError::ChildFailed(format!("node {node} answered WIRE with {wire}")));
    };
    let links = ok["links"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|link| link["state"] == "up")
        .filter_map(|link| {
            Some((
                link["peer"].as_u64()? as usize,
                link["lane"].as_str()?.to_string(),
                link["wire_version"].as_u64()? as u8,
            ))
        })
        .collect();
    Ok((links, ok))
}

/// Wait until every up link of a node speaks one version, and there is at least one
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `version` - The version every link has to report
/// * `within` - How long to wait
fn wait_links_at(cluster: &mut Cluster, node: usize, version: u8, within: Duration) -> Result<Vec<(usize, String, u8)>, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let (links, _) = wire_of(cluster, node)?;
        if !links.is_empty() && links.iter().all(|(_, _, spoken)| *spoken == version) {
            return Ok(links);
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {node}'s links never all spoke {version}: {links:?}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait until a node reports the cluster's activated wire version
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `node` - The node
/// * `version` - The version
/// * `within` - How long to wait
fn wait_activated(cluster: &mut Cluster, node: usize, version: u8, within: Duration) -> Result<(), FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let (_, wire) = wire_of(cluster, node)?;
        if wire["activated"].as_u64() == Some(u64::from(version)) {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {node} never saw wire version {version} activated: {wire}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Members at two wire versions serve forwards, quorum writes, barrier reads, a snapshot
/// over the older link and an election, and the newer version cannot be activated until
/// every member speaks it (C2 M10)
///
/// Three nodes at a factor of three, nodes one and two pinned at wire version 4 as members
/// not yet upgraded, node zero at the build's 5. Every link negotiates 4, and reports so.
/// Writes led on every node forward and commit at quorum, barrier reads through every node
/// see them. Node two is left behind the purge point and fed snapshots by leaders on both
/// versions over version 4 links, which is the manifest's codec on the wire. Node zero -
/// the only member at 5 - is killed, and an election among the members elects a leader
/// that commits writes; it comes back. `ACTIVATE 5` is refused naming the pinned members,
/// the digests agree, and nobody died ([F48](../../docs/src/features/rolling-compatibility.md)).
#[tokio::test(flavor = "multi_thread")]
async fn mixed_versions_exchange_real_cluster_operations() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::read::ReadLevel;
    use shoal::shared::protocol::{MIN_PEER_VERSION, PROTOCOL_VERSION};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .lane_links(true)
        .wire_version(1, MIN_PEER_VERSION)
        .wire_version(2, MIN_PEER_VERSION)
        .checkpoint_entries(8)
        .retained_entries(16)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|id| cluster.node(id).endpoints.client.to_string()).collect();
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    // the members report what they speak: two at the floor, one at the newest, none activated
    let (_, wire) = wire_of(&mut cluster, 0)?;
    assert_eq!(wire["activated"], u64::from(MIN_PEER_VERSION), "{wire}");
    assert_eq!(wire["min_member"], u64::from(MIN_PEER_VERSION), "{wire}");
    assert_eq!(wire["max_member"], u64::from(PROTOCOL_VERSION), "{wire}");
    assert_eq!(wire["newest"], u64::from(PROTOCOL_VERSION), "{wire}");
    // writes led on every node, forwarded from every other, and committed at quorum
    let mut keys = Vec::new();
    for leader in 0..3 {
        keys.extend(keys_led_by(&mut cluster, "Note", leader, 9000 + leader as u64 * 100, 4)?);
    }
    for (at, key) in keys.iter().enumerate() {
        write_note(&addrs[at % 3], *key, &format!("mixed-{key}")).await?;
    }
    // every link that carried one negotiated the floor, since two members speak nothing else
    for node in 0..3 {
        let links = wait_links_at(&mut cluster, node, MIN_PEER_VERSION, Duration::from_secs(10))?;
        assert!(links.iter().any(|(_, lane, _)| lane == "replication"), "node {node} replicated nothing: {links:?}");
    }
    // barrier reads through every node see every write
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    for key in &keys {
        for reader in 0..3 {
            let seen = read_note_with(&addrs[reader], *key, &quorum).await.map_err(ok)?;
            assert_eq!(seen.as_deref(), Some(format!("mixed-{key}").as_str()), "a barrier read through node {reader} missed key {key}");
        }
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // node two left behind the purge point of every group, and fed snapshots over version
    // 4 links by whichever version leads each group
    let behind = groups_of(&mut cluster, 2)?;
    cluster.kill(2)?;
    let client = Shoal::<TestDbClient>::new(&addrs[0]).await.map_err(ok)?;
    for key in 9500..9600u64 {
        write_note_eventually(&addrs[0], key, &format!("snap-{key}"), Duration::from_secs(15)).await?;
    }
    for key in 9500..9600u64 {
        client.send_one(Row { key, data: format!("snap-{key}") }).await.map_err(ok)?;
    }
    wait_purged_past(&mut cluster, &[0, 1], "Note", &applied_by_group(&behind, "Note"), Duration::from_secs(90))?;
    wait_purged_past(&mut cluster, &[0, 1], "Row", &applied_by_group(&behind, "Row"), Duration::from_secs(30))?;
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(90))?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Row", Duration::from_secs(60))?;
    wait_not_installing(&mut cluster, 2, Duration::from_secs(10))?;
    let installed = snapshots_of(&mut cluster, 2)?;
    assert!(installed["installed"].as_u64().unwrap_or(0) > 0, "node two installed no snapshot: {installed}");
    // still pinned: every link it came back on speaks the floor
    wait_links_at(&mut cluster, 2, MIN_PEER_VERSION, Duration::from_secs(10))?;
    // the one member at the newest version killed: the two at the floor elect, commit, serve
    cluster.kill(0)?;
    let leader = cluster.wait_leader_among(1, &[1, 2], Duration::from_secs(30))?;
    // node two came back on a new client port, so the address is read again
    let addr = cluster.node(leader).endpoints.client.to_string();
    for key in 9700..9710u64 {
        write_note_eventually(&addr, key, &format!("elected-{key}"), Duration::from_secs(20)).await?;
    }
    for key in 9700..9710u64 {
        assert_eq!(read_note_with(&addr, key, &quorum).await.map_err(ok)?.as_deref(), Some(format!("elected-{key}").as_str()));
    }
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    wait_links_at(&mut cluster, 0, MIN_PEER_VERSION, Duration::from_secs(10))?;
    // the newest version cannot be activated while two members speak the floor
    let refused = cluster.node_mut(0).command(&format!("ACTIVATE {PROTOCOL_VERSION}"))?;
    let reason = refused["error"].as_str().unwrap_or_default().to_string();
    let ids = cluster.node_ids();
    assert!(reason.contains(&ids[1]) && reason.contains(&ids[2]), "the refusal did not name the pinned members: {refused}");
    assert!(!reason.contains(&ids[0]), "the refusal named the member that speaks it: {refused}");
    let (_, wire) = wire_of(&mut cluster, 0)?;
    assert_eq!(wire["activated"], u64::from(MIN_PEER_VERSION), "{wire}");
    // the floor itself is already activated, and asking for it changes nothing
    let same = cluster.node_mut(0).command(&format!("ACTIVATE {MIN_PEER_VERSION}"))?;
    assert!(same["ok"].is_object(), "{same}");
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A rolling upgrade under writers, with a failure inside the mixed window, ends in an
/// activation past which a member at the old version is refused (C9 M10)
///
/// Three nodes at a factor of three, all pinned at wire version 4 - the cluster as it ran
/// before the upgrade - under writers through every node with identities and a retry
/// budget. Each node is restarted at the build's newest in turn, readiness waited on
/// between; inside the mixed window node one is killed and brought back still at the old
/// version. Once every member reports the newest, `ACTIVATE 5` commits and every node sees
/// it. A restart of node two pinned at 4 is refused by name before it serves anything, and
/// it comes back at 5. The writers' ledger, joined by a read of every key on every node, is
/// accepted by the sequential oracle; the transport report showed 4 on every link before
/// and 5 on every link after ([F48](../../docs/src/features/rolling-compatibility.md)).
#[tokio::test(flavor = "multi_thread")]
async fn rolling_upgrade_survives_operations_and_failure() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::{MIN_PEER_VERSION, PROTOCOL_VERSION};
    use shoal_model::event::{ClientOp, MutationOp, OpResult, ReadLevel};
    use shoal_model::ids::{Attempt, Key, OpId, TabletId, Value};
    use shoal_model::oracle::{Ledger, Outcome};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .wire_version(0, MIN_PEER_VERSION)
        .wire_version(1, MIN_PEER_VERSION)
        .wire_version(2, MIN_PEER_VERSION)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let tablet_id = |key: u64| TabletId {
        table: shoal_model::ids::TableId(1),
        range: tablet_of(key) as u16,
    };
    // six keys per writer, one writer per node, inserted before anything is concurrent
    let keys: Vec<u64> = (9800..9818).collect();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let clock = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let unknown = Arc::new(AtomicU64::new(0));
    for key in &keys {
        let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
        let attempt = Attempt { id, retry: 0 };
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Mutate(MutationOp::Insert { key: Key((*key % 251) as u8), value: Value(0) }), invoke);
        write_note(&addrs[0], *key, "0").await?;
        let complete = clock.fetch_add(1, Ordering::SeqCst);
        ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Applied(true)));
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // every link speaks the old version before anything is upgraded
    for node in 0..3 {
        wait_links_at(&mut cluster, node, MIN_PEER_VERSION, Duration::from_secs(10))?;
    }
    let (_, wire) = wire_of(&mut cluster, 0)?;
    assert_eq!(wire["max_member"], u64::from(MIN_PEER_VERSION), "{wire}");
    // the writers, through every node, under identities with a retry budget
    let mut tasks = Vec::new();
    for node in 0..3 {
        let endpoints: Vec<String> = addrs.clone();
        let keys: Vec<u64> = keys[node * 6..node * 6 + 6].to_vec();
        let (ledger, clock, next_id, stop, unknown) = (ledger.clone(), clock.clone(), next_id.clone(), stop.clone(), unknown.clone());
        tasks.push(tokio::spawn(async move {
            let mut ordered = endpoints.clone();
            ordered.rotate_left(node);
            let mut round = 0u32;
            while !stop.load(Ordering::SeqCst) && round < 200 {
                let Ok(client) = Shoal::<TestDbClient>::builder().endpoints(ordered.clone()).build().await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                for (at, key) in keys.iter().enumerate() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let value = Value(node as u32 * 1000 + round + 1);
                    let delete = (round as usize + at + node) % 5 == 0;
                    let op = if delete {
                        MutationOp::Delete { key: Key((*key % 251) as u8) }
                    } else {
                        MutationOp::Update { key: Key((*key % 251) as u8), value }
                    };
                    let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
                    let attempt = Attempt { id, retry: 0 };
                    let invoke = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Mutate(op), invoke);
                    let options = SendOptions::new().identity(uuid::Uuid::new_v4()).retry(Duration::from_secs(20));
                    let outcome = if delete {
                        match client.send_one_with(cluster::schema::NoteDelete::new(*key), &options).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(_) => Outcome::Unknown,
                        }
                    } else {
                        let update = cluster::schema::NoteUpdate { partition_key: *key, text: Some(value.0.to_string()) };
                        match client.send_one_with(update, &options).await {
                            Ok(_) => Outcome::Ok(OpResult::Applied(true)),
                            Err(shoal::client::Errors::QueryDidNotSucceed { .. }) => Outcome::Ok(OpResult::Applied(false)),
                            Err(error) => {
                                eprintln!("writer {node} round {round} key {key}: {error:?}");
                                Outcome::Unknown
                            }
                        }
                    };
                    if outcome == Outcome::Unknown {
                        unknown.fetch_add(1, Ordering::SeqCst);
                    }
                    let complete = clock.fetch_add(1, Ordering::SeqCst);
                    ledger.lock().unwrap().complete(attempt, complete, outcome);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                round += 1;
            }
            Ok::<(), shoal::client::Errors>(())
        }));
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    // each node restarted at the newest in turn, with readiness and a leader waited on; the
    // failure inside the mixed window is node one, killed and back still at the old version
    for node in 0..3 {
        cluster.restart_with_wire(node, None)?;
        cluster.wait_joined(&[node])?;
        cluster.wait_leader_among(node, &[0, 1, 2], Duration::from_secs(30))?;
        if node == 0 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            cluster.kill(1)?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            cluster.restart(1, NodeKind::Server)?;
            cluster.wait_joined(&[1])?;
            // still at the old version, so still spoken to at the floor
            wait_links_at(&mut cluster, 1, MIN_PEER_VERSION, Duration::from_secs(15))?;
            let (_, wire) = wire_of(&mut cluster, 0)?;
            assert_eq!(wire["min_member"], u64::from(MIN_PEER_VERSION), "{wire}");
            assert_eq!(wire["max_member"], u64::from(PROTOCOL_VERSION), "{wire}");
            // and the newest cannot be activated yet
            let refused = cluster.node_mut(0).command(&format!("ACTIVATE {PROTOCOL_VERSION}"))?;
            assert!(refused["error"].as_str().unwrap_or_default().contains("speak"), "{refused}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    // every member reports the newest, and every link speaks it
    let (_, wire) = wire_of(&mut cluster, 0)?;
    assert_eq!(wire["min_member"], u64::from(PROTOCOL_VERSION), "{wire}");
    for node in 0..3 {
        wait_links_at(&mut cluster, node, PROTOCOL_VERSION, Duration::from_secs(15))?;
    }
    // the activation commits, and every node sees it
    let activated = cluster.node_mut(0).command(&format!("ACTIVATE {PROTOCOL_VERSION}"))?;
    assert!(activated["ok"]["version"].is_number(), "{activated}");
    for node in 0..3 {
        wait_activated(&mut cluster, node, PROTOCOL_VERSION, Duration::from_secs(30))?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await.expect("a writer task panicked")?;
    }
    // a restart pinned below the activated version is refused by name, and the node comes
    // back once the pin is lifted
    let refused = cluster.restart_with_wire(2, Some(MIN_PEER_VERSION)).expect_err("a member below the activated wire started");
    let text = format!("{refused:?}");
    assert!(text.contains("activated") && text.contains("wire version"), "{text}");
    cluster.restart_with_wire(2, None)?;
    cluster.wait_joined(&[2])?;
    wait_links_at(&mut cluster, 2, PROTOCOL_VERSION, Duration::from_secs(15))?;
    // every key read on every node joins the history, which the oracle accepts
    wait_group_rows_agree(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    for node in 0..3 {
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in &keys {
            let id = OpId(next_id.fetch_add(1, Ordering::SeqCst) as u32);
            let attempt = Attempt { id, retry: 0 };
            let invoke = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().invoke(attempt, tablet_id(*key), ClientOp::Read { key: Key((*key % 251) as u8), level: ReadLevel::One }, invoke);
            let seen = read_note(&addr, *key).await?.map(|text| Value(text.parse().expect("a value")));
            let complete = clock.fetch_add(1, Ordering::SeqCst);
            ledger.lock().unwrap().complete(attempt, complete, Outcome::Ok(OpResult::Value(seen)));
        }
    }
    let ledger = ledger.lock().unwrap().clone();
    shoal_model::oracle::check(&ledger).unwrap_or_else(|error| panic!("the history is not sequential: {error:?}"));
    eprintln!("writes unknown at the end of their budget: {}", unknown.load(Ordering::SeqCst));
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A rolling upgrade from a real previous build of this test binary, when one is given
///
/// `SHOAL_PREVIOUS_TEST_BINARY` names a `cluster_fixture` test binary built from an earlier
/// commit; unset, the test says so and passes. Set, three nodes start from it, rows are
/// written, each node is restarted on this binary in turn with rows written between, and
/// the newest version is activated once all three report it; every row reads back on every
/// node ([F48](../../docs/src/features/rolling-compatibility.md)).
#[tokio::test(flavor = "multi_thread")]
async fn rolling_upgrade_from_previous_binary() -> Result<(), FixtureError> {
    use shoal::shared::protocol::{MIN_PEER_VERSION, PROTOCOL_VERSION};
    let Ok(previous) = std::env::var("SHOAL_PREVIOUS_TEST_BINARY") else {
        eprintln!("skipped: rolling_upgrade_from_previous_binary needs SHOAL_PREVIOUS_TEST_BINARY, a cluster_fixture test binary from an earlier commit");
        return Ok(());
    };
    let previous = std::path::PathBuf::from(previous);
    assert!(previous.is_file(), "SHOAL_PREVIOUS_TEST_BINARY names no file: {}", previous.display());
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .exe(previous)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    // rows written on the previous build
    let mut keys: Vec<u64> = (9900..9910).collect();
    for key in &keys {
        write_note(&addrs[0], *key, &format!("previous-{key}")).await?;
    }
    // each node restarted on this build in turn, with rows written through the mixed cluster
    for node in 0..3 {
        cluster.restart_with_binary(node, None)?;
        cluster.wait_joined(&[node])?;
        cluster.wait_leader_among(node, &[0, 1, 2], Duration::from_secs(30))?;
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in 9910 + node as u64 * 10..9920 + node as u64 * 10 {
            write_note_eventually(&addr, key, &format!("mixed-{key}"), Duration::from_secs(20)).await?;
            keys.push(key);
        }
        // the upgraded node speaks the floor to whoever is left on the previous build, and
        // the newest to whoever is upgraded
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (links, _) = wire_of(&mut cluster, node)?;
            let right = !links.is_empty()
                && links.iter().all(|(peer, _, spoken)| *spoken == if *peer <= node { PROTOCOL_VERSION } else { MIN_PEER_VERSION });
            if right {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "node {node}'s links never spoke the right versions: {links:?}");
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    // every member reports the newest, and it activates
    let (_, wire) = wire_of(&mut cluster, 0)?;
    assert_eq!(wire["min_member"], u64::from(PROTOCOL_VERSION), "{wire}");
    let activated = cluster.node_mut(0).command(&format!("ACTIVATE {PROTOCOL_VERSION}"))?;
    assert!(activated["ok"]["version"].is_number(), "{activated}");
    for node in 0..3 {
        wait_activated(&mut cluster, node, PROTOCOL_VERSION, Duration::from_secs(30))?;
    }
    // every row reads back on every node
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    for node in 0..3 {
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in &keys {
            let expected = if *key < 9910 { format!("previous-{key}") } else { format!("mixed-{key}") };
            wait_note(&addr, *key, Some(&expected), Duration::from_secs(10)).await?;
        }
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// The record of a backup, restore or recovery operation as a node answers it
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `verb` - `BACKUP_STATUS` or `RESTORE_STATUS`
/// * `op` - The operation
fn operation_record(cluster: &mut Cluster, via: usize, verb: &str, op: uuid::Uuid) -> Result<serde_json::Value, FixtureError> {
    let reply = cluster.node_mut(via).command(&format!("{verb} {op}"))?;
    reply
        .get("ok")
        .cloned()
        .ok_or_else(|| FixtureError::ChildFailed(format!("node {via} answered {verb} with {reply}")))
}

/// Wait until every group of a backup or restore record is done, and return the record
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `via` - The node to ask
/// * `verb` - `BACKUP_STATUS` or `RESTORE_STATUS`
/// * `op` - The operation
/// * `within` - How long to wait
fn wait_operation_done(cluster: &mut Cluster, via: usize, verb: &str, op: uuid::Uuid, within: Duration) -> Result<serde_json::Value, FixtureError> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let record = operation_record(cluster, via, verb, op)?;
        let done = record["groups"]
            .as_object()
            .is_some_and(|groups| !groups.is_empty() && groups.values().all(|group| group["phase"] == "Done"));
        if done {
            return Ok(record);
        }
        if std::time::Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("{verb} {op} never finished: {record}")));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// A backup restored into a new cluster holds every acknowledged write and retry, and the
/// old cluster's identities are refused by name (C9 M10)
///
/// Three nodes at a factor of three under writers with identities and wire version 5
/// activated. `BACKUP <dir>`: every persistent group is written to a verified file with a
/// checksum and a boundary, and every ephemeral group is skipped by name. A key deleted and a
/// retry identity acknowledged before the backup are in the files. A fresh three-node cluster
/// with new identities is bootstrapped and `RESTORE <dir/op>` run: every persistent group is
/// restored and verified, every key acknowledged before its group's boundary reads on every
/// new node with its last value, the deleted key is absent, the digests agree, the remembered
/// identity is answered its original result through the new cluster and a fresh identity
/// applies. A session token minted on the old cluster is `WrongCluster` on the new one, an old
/// node's directory started against the new cluster is refused as removed and stops, and a
/// second restore is refused by name ([F49](../../docs/src/features/backup-and-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn backup_restore_verifies_history_in_new_cluster() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    use shoal::shared::protocol::PROTOCOL_VERSION;
    let backups = utils::test_dir();
    let mut old = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    old.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|node| old.node(node).endpoints.client.to_string()).collect();
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    // the backup needs the version whose file header names the cluster: refused before, applied after
    let refused = old.node_mut(0).command(&format!("BACKUP {}", backups.path().display()))?;
    assert!(refused["error"].as_str().unwrap_or_default().contains("wire version"), "{refused}");
    let activated = old.node_mut(0).command(&format!("ACTIVATE {PROTOCOL_VERSION}"))?;
    assert!(activated["ok"]["version"].is_number(), "{activated}");
    for node in 0..3 {
        wait_activated(&mut old, node, PROTOCOL_VERSION, Duration::from_secs(30))?;
    }
    // rows on both tables, through every node; one key deleted, one written twice, and a
    // retry identity acknowledged, all before the backup
    let keys: Vec<u64> = (12000..12030).collect();
    for (at, key) in keys.iter().enumerate() {
        write_note(&addrs[at % 3], *key, &format!("v1-{key}")).await?;
    }
    let client = Shoal::<TestDbClient>::new(&addrs[0]).await.map_err(ok)?;
    for key in &keys {
        client.send_one(Row { key: *key, data: format!("row-{key}") }).await.map_err(ok)?;
    }
    write_note(&addrs[1], keys[1], &format!("v2-{}", keys[1])).await?;
    delete_note(&addrs[2], keys[2]).await.map_err(ok)?;
    let identity = uuid::Uuid::new_v4();
    let original = delete_note_as(&addrs[0], keys[3], &SendOptions::new().identity(identity)).await.map_err(ok)?;
    let original_token = original.session_token().expect("a committed delete carries a token");
    let old_token = write_note_token(&addrs[0], keys[4], &format!("v2-{}", keys[4])).await.map_err(ok)?.expect("a token");
    wait_digests_equal(&mut old, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let old_digest = digest_of(&mut old, 0, "Note")?;
    // the backup: every persistent group written and verified, every ephemeral one skipped
    let op = plan_as_process(&mut old, 0, &format!("BACKUP {}", backups.path().display()))?;
    let record = wait_operation_done(&mut old, 0, "BACKUP_STATUS", op, Duration::from_secs(120))?;
    let groups = record["groups"].as_object().expect("groups");
    let mut written = 0;
    let mut skipped = 0;
    for (group, progress) in groups {
        let outcome = &progress["outcome"];
        if outcome["Written"].is_object() {
            written += 1;
            let file = outcome["Written"]["file"].as_str().expect("a file");
            assert!(std::path::Path::new(file).is_file(), "group {group}'s file is missing: {file}");
            assert!(outcome["Written"]["bytes"].as_u64().unwrap_or(0) > 0, "{progress}");
            assert!(progress["boundary"].as_u64().unwrap_or(0) > 0, "{progress}");
            assert!(std::path::Path::new(&format!("{file}.json")).is_file(), "group {group}'s manifest is missing");
        } else if outcome["Skipped"].is_object() {
            skipped += 1;
        } else {
            panic!("group {group} came to {outcome}: {record}");
        }
    }
    assert!(written > 0 && skipped > 0, "the backup wrote {written} and skipped {skipped}: {record}");
    let backup_dir = backups.path().join(op.to_string());
    // the new cluster, with new identities
    let mut new = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(20))
        .start()
        .await?;
    new.wait_voters(0, 3)?;
    let new_addrs: Vec<String> = (0..3).map(|node| new.node(node).endpoints.client.to_string()).collect();
    assert_ne!(old.node(0).endpoints.cluster, new.node(0).endpoints.cluster);
    // the restore: every persistent group restored and verified, every ephemeral one skipped
    let restore = plan_as_process(&mut new, 0, &format!("RESTORE {}", backup_dir.display()))?;
    let record = wait_operation_done(&mut new, 0, "RESTORE_STATUS", restore, Duration::from_secs(300))?;
    let mut restored = 0;
    for (group, progress) in record["groups"].as_object().expect("groups") {
        let outcome = &progress["outcome"];
        if outcome["Restored"].is_object() {
            restored += 1;
            assert!(outcome["Restored"]["verified"].as_u64().unwrap_or(0) > 0, "{progress}");
        } else {
            assert!(outcome["Skipped"].is_object(), "group {group} came to {outcome}: {record}");
        }
    }
    assert_eq!(restored, written, "{record}");
    assert_eq!(record["source"], old.node(0).endpoints.cluster.clone().expect("a cluster").as_str(), "{record}");
    // every key acknowledged before the backup reads on every new node with its last value
    wait_digests_equal(&mut new, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    for node in 0..3 {
        for (at, key) in keys.iter().enumerate() {
            let expected = match at {
                1 | 4 => Some(format!("v2-{key}")),
                2 | 3 => None,
                _ => Some(format!("v1-{key}")),
            };
            wait_note(&new_addrs[node], *key, expected.as_deref(), Duration::from_secs(10)).await?;
        }
    }
    let new_digest = digest_of(&mut new, 0, "Note")?;
    assert_eq!(new_digest["rows"], old_digest["rows"], "the new cluster holds other rows than the old: {new_digest} vs {old_digest}");
    // the remembered identity is answered its original result through the new cluster, and a
    // fresh identity applies
    let again = delete_note_as(&new_addrs[1], keys[3], &SendOptions::new().identity(identity).retry(Duration::from_secs(15)))
        .await
        .unwrap_or_else(|error| panic!("the retry under the old identity was not the original result: {error:?}"));
    assert_eq!(again.bundle(), identity);
    // the token it answers with is the new cluster's, whose groups are minted from new identities
    let token = again.session_token().expect("a duplicate answers with a token");
    assert_ne!(token.group, original_token.group, "the restored cluster reused the old cluster's group identity");
    assert_eq!(token.cluster.to_string(), new.node(0).endpoints.cluster.clone().expect("a cluster"));
    let fresh = delete_note_as(&new_addrs[2], keys[5], &SendOptions::new().identity(uuid::Uuid::new_v4())).await.map_err(ok)?;
    assert!(fresh.session_token().is_some());
    wait_note(&new_addrs[0], keys[5], None, Duration::from_secs(10)).await?;
    // a session token minted on the old cluster is the wrong cluster here
    let stale = read_note_with(&new_addrs[0], keys[4], &SendOptions::new().token(old_token)).await;
    assert_eq!(failure_code(&stale), Some(ErrorCode::WrongCluster), "{stale:?}");
    // a second restore is refused by name
    let second = new.node_mut(0).command(&format!("RESTORE {}", backup_dir.display()))?;
    assert!(second["error"].as_str().unwrap_or_default().contains("already restored"), "{second}");
    // an old node's directory started against the new cluster is refused as removed and stops
    old.kill(1)?;
    let new_control = new.node(0).endpoints.control.expect("a control endpoint").to_string();
    let new_data = new.node(0).endpoints.data.expect("a data endpoint").to_string();
    let mut staged = old.staged(1).clone();
    staged.seeds = vec![new_control.clone()];
    staged.dial = staged.peers.iter().map(|peer| (peer.clone(), new_control.clone(), new_data.clone())).collect();
    let started = old.restart_with(1, NodeKind::Server, Some(staged));
    let reason = match started {
        Ok(()) => {
            let node = old.node(1);
            Cluster::wait_failure(node, Duration::from_secs(30)).unwrap_or_else(|| panic!("the old node was not refused by the new cluster"))
        }
        Err(FixtureError::ChildFailed(reason)) => reason,
        Err(error) => panic!("the old node's restart failed another way: {error:?}"),
    };
    assert!(reason.contains("removed"), "the old node was refused for another reason: {reason}");
    for id in 0..3 {
        assert_eq!(new.node(id).failure(), None, "new node {id} died");
    }
    Ok(())
}

/// A permanent majority loss is never repaired by itself: a survivor stays unavailable
/// without data loss until an operator recovers it, after which it leads alone, the lost
/// identities are refused, and fresh identities rebuild every set (C9 M10)
///
/// Three nodes at a factor of three with rows on every node, and two more identities held
/// back. Nodes one and two are killed for good: a write through node zero is unknown or
/// refused for want of a leader, a strong read is refused, and an admin mutation is refused
/// naming the voters, what this node reaches and `force_recover`. Node zero restarted with
/// `bootstrap: true` keeps its cluster and still has no leader - no empty bootstrap.
/// `force_recover` on its stopped directory: it starts leading alone, writes commit, every
/// key acknowledged before the loss reads back, `Members` shows one and two removing and
/// tombstoned with the recovery recorded and its boundary, and node one started again from
/// its directory is refused as removed and stops. The two held-back identities join, the
/// recovery's plans rebuild every set on them, one and two are removed, and every key reads
/// through the newcomers ([F49](../../docs/src/features/backup-and-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn permanent_quorum_loss_requires_explicit_recovery() -> Result<(), FixtureError> {
    use shoal::client::SendOptions;
    use shoal::shared::protocol::error::ErrorCode;
    use shoal::shared::protocol::read::ReadLevel;
    let mut cluster = Cluster::builder()
        .cluster(5, CoreClaim::Count(1))
        .replication_factor(3)
        .deferred_from(3)
        .write_timeout(Duration::from_secs(2))
        .query_deadline(Duration::from_secs(3))
        .catchup_lag(0)
        .plan_interval(Duration::from_millis(500))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let cluster_id = cluster.node(0).endpoints.cluster.clone().expect("a cluster");
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let keys: Vec<u64> = (13000..13024).collect();
    for key in &keys {
        write_note(&addr0, *key, &format!("before-{key}")).await?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // the majority is lost for good
    cluster.kill(1)?;
    cluster.kill(2)?;
    // a write is unknown or refused for want of a leader, never lost or acknowledged alone
    let unavailable = write_note(&addr0, 13100, "lost").await;
    let code = failure_code(&unavailable);
    assert!(
        matches!(code, Some(ErrorCode::OutcomeUnknown | ErrorCode::NotLeader | ErrorCode::QuorumUnavailable | ErrorCode::Unavailable | ErrorCode::Timeout)),
        "a write without a majority answered {unavailable:?}"
    );
    // a strong read is refused
    let strong = read_note_with(&addr0, keys[0], &SendOptions::new().read(ReadLevel::Quorum)).await;
    assert!(strong.is_err(), "a strong read without a majority answered {strong:?}");
    // an admin mutation is refused naming the voters, what this node reaches, and the way out
    let refused = cluster.node_mut(0).command("SET_VOTERS 1")?;
    let reason = refused["error"].as_str().unwrap_or_default().to_string();
    assert!(reason.contains("force_recover") && reason.contains("voters"), "{refused}");
    // restarted with bootstrap: true, node zero keeps its cluster and still has no leader
    cluster.restart(0, NodeKind::Server)?;
    assert_eq!(cluster.node(0).endpoints.cluster.as_deref(), Some(cluster_id.as_str()), "the restart minted a cluster");
    std::thread::sleep(Duration::from_secs(3));
    let members = cluster.members(0)?;
    assert!(members["leader"].is_null(), "a survivor found a leader without a majority: {members}");
    let addr0 = cluster.node(0).endpoints.client.to_string();
    let unavailable = write_note(&addr0, 13101, "still lost").await;
    assert!(unavailable.is_err(), "a write committed without a majority: {unavailable:?}");
    // the recovery, on the stopped directory: refused for a survivor list that is not this
    // node alone, then run
    cluster.kill(0)?;
    let conf = utils::build_crash_config(cluster.dir(0), 0).cluster(shoal::server::conf::Cluster::default().bootstrap(true));
    let ids = cluster.node_ids();
    let me: shoal::shared::identity::NodeId = ids[0].parse().map(shoal::shared::identity::NodeId).expect("a node id");
    let other: shoal::shared::identity::NodeId = ids[1].parse().map(shoal::shared::identity::NodeId).expect("a node id");
    let refused = shoal::server::recover::force_recover(&conf, &[me, other]).expect_err("a recovery keeping two survivors ran");
    assert!(format!("{refused}").contains("nothing else"), "{refused}");
    let report = shoal::server::recover::force_recover(&conf, &[me]).map_err(|error| FixtureError::ChildFailed(format!("{error}")))?;
    assert_eq!(report.survivor, me);
    assert_eq!(report.lost.len(), 2, "{report:?}");
    assert!(!report.groups_rewritten.is_empty(), "{report:?}");
    // and again is nothing: every step is idempotent by inspection
    let again = shoal::server::recover::force_recover(&conf, &[me]).map_err(|error| FixtureError::ChildFailed(format!("{error}")))?;
    assert_eq!(again.recovered_at, report.recovered_at, "a second run wrote a second recovery: {again:?}");
    assert!(again.groups_rewritten.is_empty(), "{again:?}");
    assert_eq!(again.groups_kept.len(), report.groups_rewritten.len() + report.groups_kept.len(), "{again:?}");
    // the survivor leads alone, writes commit, and every key from before the loss reads back
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    cluster.wait_leader_among(0, &[0], Duration::from_secs(30))?;
    let addr0 = cluster.node(0).endpoints.client.to_string();
    for key in 13200..13210u64 {
        write_note_eventually(&addr0, key, &format!("after-{key}"), Duration::from_secs(30)).await?;
    }
    for key in &keys {
        wait_note(&addr0, *key, Some(&format!("before-{key}")), Duration::from_secs(10)).await?;
    }
    let members = cluster.members(0)?;
    assert_eq!(members["voters"].as_array().map(Vec::len), Some(1), "{members}");
    for lost in 1..3 {
        let view = member_view(&mut cluster, 0, lost)?;
        assert_eq!(view["phase"], "removing", "{view}");
        assert!(members["tombstones"].get(&ids[lost]).is_some(), "node {lost} is not tombstoned: {members}");
    }
    let recoveries = cluster.node_mut(0).command("RECOVERIES")?["ok"].clone();
    assert_eq!(recoveries.as_array().map(Vec::len), Some(1), "{recoveries}");
    assert_eq!(recoveries[0]["survivors"], serde_json::json!([ids[0]]), "{recoveries}");
    assert_eq!(recoveries[0]["last_committed"], report.last_committed, "{recoveries}");
    assert!(recoveries[0]["lost"].as_array().is_some_and(|lost| lost.len() == 2), "{recoveries}");
    // node one started again from its directory is refused as removed, and stops
    let started = cluster.restart(1, NodeKind::Server);
    let reason = match started {
        Ok(()) => Cluster::wait_failure(cluster.node(1), Duration::from_secs(30)).unwrap_or_else(|| panic!("the lost node was not refused")),
        Err(FixtureError::ChildFailed(reason)) => reason,
        Err(error) => panic!("the lost node's restart failed another way: {error:?}"),
    };
    assert!(reason.contains("removed"), "the lost node was refused for another reason: {reason}");
    // two fresh identities join, and the recovery's plans rebuild every set on them
    cluster.start_deferred(3)?;
    cluster.start_deferred(4)?;
    cluster.wait_joined(&[3, 4])?;
    wait_member_state(&mut cluster, 0, 1, "removed", Duration::from_secs(180))?;
    wait_member_state(&mut cluster, 0, 2, "removed", Duration::from_secs(180))?;
    let members = cluster.members(0)?;
    assert_eq!(members["under_replicated_sets"], 0, "{members}");
    wait_group_rows_agree(&mut cluster, &[0, 3, 4], "Note", Duration::from_secs(60))?;
    for node in [3usize, 4] {
        let addr = cluster.node(node).endpoints.client.to_string();
        for key in keys.iter().chain((13200..13210u64).collect::<Vec<_>>().iter()) {
            let expected = if *key < 13200 { format!("before-{key}") } else { format!("after-{key}") };
            wait_note(&addr, *key, Some(&expected), Duration::from_secs(10)).await?;
        }
    }
    let _ = ok;
    for id in [0usize, 3, 4] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// Single node data has a verified path into a cluster: an export in the backup's shape,
/// restored into a fresh cluster and judged by digest, with the source as the rollback (C10 M10)
///
/// A standalone node at two executors is seeded with rows on the persistent table and the
/// ephemeral one, half of the former archived by a rotate and half left in the active intent
/// logs. While it runs, an export of its directory is refused as locked. Stopped, an export
/// into a directory that is not empty is refused by name, and `export_standalone` into a
/// fresh one folds the intent logs and writes the persistent table's archives as one snapshot
/// file with a backup manifest beside it, under an identity no cluster has. A fresh cluster
/// of three at a factor of three restores the export as it would a backup: every group of
/// the table is restored from the file's records of its tablets and verified, the `DIGEST` on
/// every node equals the source's, every row reads through every node, and a write commits at
/// quorum. The source started standalone again serves every row it had with the same digest,
/// and a cluster member's directory offered as a source is refused
/// ([F49](../../docs/src/features/backup-and-recovery.md)).
#[tokio::test(flavor = "multi_thread")]
async fn single_node_data_has_a_verified_cluster_migration_path() -> Result<(), FixtureError> {
    use shoal::server::export::export_standalone;
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    // the configuration an export is run with: the standalone node's own, naming its directory
    let source_conf = |dir: &std::path::Path| {
        utils::build_crash_config(dir, 0).resources(Resources::default().cores(2).memory("100MiB").expect("a memory size"))
    };
    let mut source = Cluster::builder().standalone(CoreClaim::Count(2)).start().await?;
    let source_addr = source.node(0).endpoints.client.to_string();
    let source_node = source.node(0).endpoints.node.clone().expect("a node id");
    // rows on both tables, half of the persistent ones archived by a rotate and half left in
    // the active logs
    let client = Shoal::<TestDbClient>::new(&source_addr).await.map_err(ok)?;
    let keys: Vec<u64> = (61_000..61_400u64).collect();
    for key in &keys[..200] {
        client.send_one(Note { key: *key, text: format!("note-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key: *key, data: format!("row-{key}") }).await.map_err(ok)?;
    }
    let _ = source.node_mut(0).command("ROTATE")?;
    for key in &keys[200..] {
        client.send_one(Note { key: *key, text: format!("note-{key}") }).await.map_err(ok)?;
        client.send_one(Row { key: *key, data: format!("row-{key}") }).await.map_err(ok)?;
    }
    drop(client);
    let source_notes = digest_of(&mut source, 0, "Note")?;
    assert_eq!(source_notes["rows"], keys.len() as u64, "{source_notes}");
    // a running source is locked, and refused
    let scratch = utils::test_dir();
    let target = scratch.path().join("export");
    let refused = export_standalone::<TestDb>(&source_conf(source.dir(0)), &target).expect_err("a running source was exported");
    assert!(
        matches!(refused, ServerError::Shoal(ShoalError::StorageDirectoryLocked { .. })),
        "the refusal did not name the lock: {refused:?}"
    );
    source.kill(0)?;
    // a target that is not empty is refused, and nothing of it is touched
    std::fs::create_dir_all(&target)?;
    std::fs::write(target.join("stale"), b"not an export")?;
    let refused = export_standalone::<TestDb>(&source_conf(source.dir(0)), &target).expect_err("a non-empty target was exported into");
    assert!(format!("{refused}").contains("is not empty"), "the refusal did not say why: {refused}");
    assert_eq!(std::fs::read_dir(&target)?.count(), 1, "the refused export wrote into the target");
    // the export: the persistent table as one file with its manifest, the ephemeral one not at all
    let target = scratch.path().join("export-2");
    let report = export_standalone::<TestDb>(&source_conf(source.dir(0)), &target)
        .map_err(|error| FixtureError::ChildFailed(format!("the export failed: {error}")))?;
    assert_eq!(report.source_node.to_string(), source_node, "{report:?}");
    assert_eq!(report.executors, 2, "{report:?}");
    assert!(report.rows_folded > 0, "the export folded no intent logs: {report:?}");
    assert_eq!(report.tables, vec![("Note".to_string(), keys.len() as u64)], "{report:?}");
    assert!(report.bytes_written > 0, "{report:?}");
    let files: Vec<String> = std::fs::read_dir(target.join("Note"))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    assert_eq!(files.len(), 2, "the export wrote {files:?}");
    assert!(files.iter().any(|name| name.ends_with(".snap")) && files.iter().any(|name| name.ends_with(".snap.json")), "{files:?}");
    assert!(!target.join("Row").exists(), "the ephemeral table was exported");
    // a fresh cluster of three at a factor of three, restoring the export as it would a backup
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .repair_timeout(Duration::from_secs(20))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let restore = plan_as_process(&mut cluster, 0, &format!("RESTORE {}", target.display()))?;
    let record = wait_operation_done(&mut cluster, 0, "RESTORE_STATUS", restore, Duration::from_secs(300))?;
    // every group the export's one table has, restored and verified; the ephemeral table has
    // no file, so no group of it is in the record
    let groups = record["groups"].as_object().expect("groups");
    assert!(!groups.is_empty(), "{record}");
    for (group, progress) in groups {
        let outcome = &progress["outcome"];
        assert!(outcome["Restored"].is_object(), "group {group} came to {outcome}: {record}");
        assert!(outcome["Restored"]["verified"].as_u64().unwrap_or(0) > 0, "group {group} was not verified: {progress}");
        assert_eq!(progress["files"], serde_json::json!([format!("Note/{}", files.iter().find(|name| name.ends_with(".snap")).expect("a snap"))]), "{progress}");
    }
    let records: u64 = groups.values().map(|progress| progress["outcome"]["Restored"]["records"].as_u64().unwrap_or(0)).sum();
    assert_eq!(records, keys.len() as u64, "the groups restored other records than the export holds: {record}");
    assert_eq!(record["source"], report.export_cluster.to_string(), "{record}");
    // the same rows on every node, by digest and by reading every one; the ephemeral table
    // holds nothing, since its rows were the source's memory
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(60))?;
    for node in 0..3 {
        let notes = digest_of(&mut cluster, node, "Note")?;
        assert_eq!(notes["rows"], source_notes["rows"], "node {node}: {notes} against {source_notes}");
        assert_eq!(notes["hash"], source_notes["hash"], "node {node}: {notes} against {source_notes}");
        let rows = digest_of(&mut cluster, node, "Row")?;
        assert_eq!(rows["rows"], 0, "node {node} holds ephemeral rows from the source: {rows}");
        for key in keys.iter().step_by(3) {
            wait_note_routed(&addrs[node], *key, &format!("note-{key}"), Duration::from_secs(10)).await?;
        }
    }
    // and the cluster is a cluster: a write through a follower commits at quorum
    write_note(&addrs[1], 61_400, "after-export").await.map_err(ok)?;
    for addr in &addrs {
        wait_note(addr, 61_400, Some("after-export"), Duration::from_secs(10)).await?;
    }
    // the source is the rollback: started standalone again, it serves every row it had
    source.restart(0, NodeKind::Standalone)?;
    let rolled_back = source.node(0).endpoints.clone();
    assert_eq!(rolled_back.node.as_deref(), Some(source_node.as_str()), "the source's identity changed");
    assert_eq!(rolled_back.cluster, None, "the source became a cluster member");
    let source_addr = rolled_back.client.to_string();
    for key in keys.iter().step_by(5) {
        wait_note_routed(&source_addr, *key, &format!("note-{key}"), Duration::from_secs(10)).await?;
    }
    wait_note(&source_addr, 61_400, None, Duration::from_secs(5)).await?;
    let again = digest_of(&mut source, 0, "Note")?;
    assert_eq!(again["hash"], source_notes["hash"], "the fold changed the source's rows: {again} against {source_notes}");
    // a cluster member's directory is not a source: its data is a Backup's, never an export's
    cluster.kill(2)?;
    let elsewhere = scratch.path().join("elsewhere");
    let refused = export_standalone::<TestDb>(&source_conf(cluster.dir(2)), &elsewhere).expect_err("a cluster directory was exported");
    assert!(format!("{refused}").contains("not a standalone node's directory"), "the refusal did not say why: {refused}");
    assert!(!elsewhere.exists(), "the refused export created the target");
    for id in [0usize, 1] {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    assert_eq!(source.node(0).failure(), None, "the source died");
    Ok(())
}

/// Every link of one node to another, as the transport view shows it: lane, state and the
/// last dial failure
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `from` - The node whose links are read
/// * `to` - The peer
fn links_to(cluster: &mut Cluster, from: usize, to: usize) -> Result<Vec<(String, String, Option<String>)>, FixtureError> {
    let peer = cluster.node_ids()[to].clone();
    let view = cluster.node_mut(from).command("TRANSPORT")?;
    let mut links = Vec::new();
    for shard in view["ok"].as_array().into_iter().flatten() {
        for link in shard["links"].as_array().into_iter().flatten() {
            if link["node"].as_str() == Some(peer.as_str()) {
                links.push((
                    link["lane"].as_str().unwrap_or_default().to_string(),
                    link["state"].as_str().unwrap_or_default().to_string(),
                    link["last_failure"].as_str().map(str::to_string),
                ));
            }
        }
    }
    Ok(links)
}

/// Wait until some link of one node to another has failed its last dial for a reason naming
/// a phrase, or until every link is up if the phrase is empty
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `from` - The node whose links are read
/// * `to` - The peer
/// * `phrase` - What the failure has to say, or empty for every link up
/// * `within` - How long to wait
fn wait_link_failure(cluster: &mut Cluster, from: usize, to: usize, phrase: &str, within: Duration) -> Result<Vec<(String, String, Option<String>)>, FixtureError> {
    let deadline = Instant::now() + within;
    loop {
        let links = links_to(cluster, from, to)?;
        let found = if phrase.is_empty() {
            !links.is_empty() && links.iter().all(|(_, state, _)| state == "up")
        } else {
            links.iter().any(|(_, _, failure)| failure.as_deref().is_some_and(|failure| failure.contains(phrase)))
        };
        if found {
            return Ok(links);
        }
        if Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {from}'s links to {to} never {}: {links:?}", if phrase.is_empty() { "came up".to_string() } else { format!("failed naming {phrase:?}") })));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Wait until a node sees a member's record at an incarnation and a control address
///
/// # Arguments
///
/// * `cluster` - The cluster
/// * `at` - The node asked
/// * `member` - The member
/// * `incarnation` - The incarnation wanted
/// * `control` - The control address wanted
/// * `within` - How long to wait
fn wait_member_record(cluster: &mut Cluster, at: usize, member: usize, incarnation: u64, control: &str, within: Duration) -> Result<(), FixtureError> {
    let deadline = Instant::now() + within;
    loop {
        let view = member_view(cluster, at, member)?;
        if view["record"]["incarnation"].as_u64() == Some(incarnation) && view["record"]["control"].as_str() == Some(control) {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(FixtureError::NotReady(format!("node {at} never saw member {member} at incarnation {incarnation} and {control}: {view}")));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// A member's address changes at a restart under its own identity, is observed at a higher
/// incarnation, and a clone left at the old address is refused (C1 M10)
///
/// Three nodes at a factor of three with rows on every node. Node two is stopped, its
/// directory copied, and started again at fresh peer ports: it joins as the same node one
/// start later, every member's record of it names the new address at the new incarnation,
/// its links to the others and theirs to it come up at the new address, and writes through it
/// commit while every earlier row reads through it. The copy started at the old address is
/// the same identity at the same incarnation from another address, refused as a duplicate
/// and stopping on its own, while the restarted node keeps serving and stays on record
/// ([F50](../../docs/src/features/cluster-operations.md)).
#[tokio::test(flavor = "multi_thread")]
async fn address_change_is_observed_and_a_stale_clone_is_fenced() -> Result<(), FixtureError> {
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let keys: Vec<u64> = (71_000..71_030).collect();
    for (at, key) in keys.iter().enumerate() {
        write_note(&addrs[at % 3], *key, &format!("before-{key}")).await.map_err(|error| FixtureError::NotReady(format!("{error:?}")))?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    let before = member_view(&mut cluster, 0, 2)?;
    let incarnation = before["record"]["incarnation"].as_u64().expect("an incarnation");
    let old_control = before["record"]["control"].as_str().expect("a control address").to_string();
    // stopped, copied, and started again at fresh ports: the same node one start later
    cluster.kill(2)?;
    let copy = cluster.clone_dir(2)?;
    let old_ports = cluster.restart_at_new_address(2)?;
    cluster.wait_joined(&[2])?;
    assert_eq!(cluster.node(2).endpoints.incarnation, Some(incarnation + 1));
    let new_control = format!("127.0.0.1:{}", cluster.node(2).endpoints.control.expect("a control endpoint").port());
    assert_ne!(new_control, old_control, "the restart kept the old address");
    assert_eq!(old_control, format!("127.0.0.1:{}", old_ports.1), "the staged control port is not the one on record");
    // every member's record of it moves to the new address at the new incarnation
    for at in 0..3 {
        wait_member_record(&mut cluster, at, 2, incarnation + 1, &new_control, Duration::from_secs(60))?;
    }
    // the links come up at the new address, in both directions
    wait_link_failure(&mut cluster, 0, 2, "", Duration::from_secs(60))?;
    wait_link_failure(&mut cluster, 2, 0, "", Duration::from_secs(60))?;
    // and it serves: writes through it commit, every earlier row reads through it
    let moved = cluster.node(2).endpoints.client.to_string();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let written = write_note(&moved, 71_100, "after-move").await;
        if written.is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "the write through the moved node was never admitted: {written:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for key in &keys {
        wait_note(&moved, *key, Some(&format!("before-{key}")), Duration::from_secs(10)).await?;
    }
    wait_note(&addrs[0], 71_100, Some("after-move"), Duration::from_secs(10)).await?;
    // the copy at the old address: the same identity and incarnation from another address,
    // which is a duplicate the cluster refuses, and it stops on its own
    let mut stale = cluster.spawn_clone_at(2, copy.path(), old_ports)?;
    stale.wait_ready(Duration::from_secs(60))?;
    assert_eq!(stale.endpoints.incarnation, Some(incarnation + 1));
    let refused = Cluster::wait_failure(&stale, Duration::from_secs(60)).expect("the stale clone kept running");
    assert!(
        refused.contains("incarnation") || refused.contains("duplicate") || refused.contains("fenced"),
        "the stale clone failed for another reason: {refused}"
    );
    drop(stale);
    // the restarted node is untouched by it, on record and serving
    assert_eq!(cluster.node(2).failure(), None, "the restarted node was fenced by the stale clone");
    let after = member_view(&mut cluster, 0, 2)?;
    assert_eq!(after["record"]["control"], new_control, "{after}");
    assert_eq!(after["record"]["incarnation"], incarnation + 1, "{after}");
    wait_note(&moved, keys[0], Some(&format!("before-{}", keys[0])), Duration::from_secs(10)).await?;
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}

/// A peer's certificate is bound to the node it claims, a leaf and an authority rotate on a
/// live cluster, and a certificate naming another node or none is refused (C1 M10)
///
/// Three nodes at a factor of three on mutual TLS under a fixture authority, every leaf
/// naming its node, with rows on every node. Node one's leaf is reissued and reloaded: the
/// report names the node, and node two restarted dials it under the new leaf and is dialled
/// by it, with writes through both. The authority is rotated through a bundle: every node
/// trusts old and new, every leaf is reissued under the new one and reloaded, the old is
/// retired from every bundle, and a restarted node still joins with every link up. Node two's
/// leaf is then reissued naming node one: node zero restarted refuses node two's hello as an
/// identity mismatch and its own dials to node two fail naming the certificate; reissued
/// with no node in it, the same dials fail as unauthorized; reissued as itself and reloaded,
/// every link comes up and the cluster serves. A reload of material that does not parse is
/// refused and changes nothing. Skips by name without the kernel's TLS module
/// ([F50](../../docs/src/features/cluster-operations.md)).
#[tokio::test(flavor = "multi_thread")]
async fn certificate_rotation_binds_identity() -> Result<(), FixtureError> {
    skip_without_ktls!("certificate_rotation_binds_identity");
    let mut cluster = Cluster::builder()
        .cluster(3, CoreClaim::Count(1))
        .replication_factor(3)
        .peer_tls()
        .write_timeout(Duration::from_secs(3))
        .query_deadline(Duration::from_secs(3))
        .start()
        .await?;
    cluster.wait_voters(0, 3)?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    let ok = |error: shoal::client::Errors| FixtureError::NotReady(format!("{error:?}"));
    let keys: Vec<u64> = (72_000..72_030).collect();
    for (at, key) in keys.iter().enumerate() {
        write_note(&addrs[at % 3], *key, &format!("tls-{key}")).await.map_err(ok)?;
    }
    wait_digests_equal(&mut cluster, &[0, 1, 2], "Note", Duration::from_secs(30))?;
    // a leaf rotated on a live member: reissued, reloaded, and used by every handshake after
    let node1 = cluster.minted_node(1).to_string();
    cluster.reissue_leaf(1, Some(&node1))?;
    let report = cluster.node_mut(1).command("RELOAD_TLS")?;
    assert_eq!(report["ok"]["own_identity"], node1, "{report}");
    assert_eq!(report["ok"]["chain"], 1, "{report}");
    cluster.restart(2, NodeKind::Server)?;
    cluster.wait_joined(&[2])?;
    wait_link_failure(&mut cluster, 2, 1, "", Duration::from_secs(60))?;
    wait_link_failure(&mut cluster, 1, 2, "", Duration::from_secs(60))?;
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    write_note(&addrs[1], 72_100, "after-leaf").await.map_err(ok)?;
    write_note(&addrs[2], 72_101, "after-leaf").await.map_err(ok)?;
    wait_note(&addrs[0], 72_100, Some("after-leaf"), Duration::from_secs(10)).await?;
    wait_note(&addrs[0], 72_101, Some("after-leaf"), Duration::from_secs(10)).await?;
    // the authority rotated through a bundle: both trusted, every leaf reissued under the new
    // one, the old retired, and a restart under the new authority alone joins
    cluster.rotate_authority()?;
    for id in 0..3 {
        cluster.write_authorities(id)?;
        let report = cluster.node_mut(id).command("RELOAD_TLS")?;
        assert_eq!(report["ok"]["authorities"], 2, "{report}");
    }
    for id in 0..3 {
        let node = cluster.minted_node(id).to_string();
        cluster.reissue_leaf(id, Some(&node))?;
        let report = cluster.node_mut(id).command("RELOAD_TLS")?;
        assert_eq!(report["ok"]["own_identity"], node, "{report}");
    }
    cluster.retire_previous_authority()?;
    for id in 0..3 {
        cluster.write_authorities(id)?;
        let report = cluster.node_mut(id).command("RELOAD_TLS")?;
        assert_eq!(report["ok"]["authorities"], 1, "{report}");
    }
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    for (from, to) in [(0usize, 1usize), (0, 2), (1, 0), (2, 0)] {
        wait_link_failure(&mut cluster, from, to, "", Duration::from_secs(60))?;
    }
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    write_note(&addrs[0], 72_102, "after-authority").await.map_err(ok)?;
    wait_note(&addrs[2], 72_102, Some("after-authority"), Duration::from_secs(10)).await?;
    // a leaf naming another node: refused as a mismatch at both ends of every new handshake
    cluster.reissue_leaf(2, Some(&node1))?;
    let report = cluster.node_mut(2).command("RELOAD_TLS")?;
    assert_eq!(report["ok"]["own_identity"], node1, "{report}");
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    wait_link_failure(&mut cluster, 0, 2, "certificate names node", Duration::from_secs(60))?;
    wait_link_failure(&mut cluster, 2, 0, "identity does not match", Duration::from_secs(60))?;
    // a leaf naming no node: unauthorized
    cluster.reissue_leaf(2, None)?;
    let report = cluster.node_mut(2).command("RELOAD_TLS")?;
    assert!(report["ok"]["own_identity"].is_null(), "{report}");
    cluster.restart(0, NodeKind::Server)?;
    cluster.wait_joined(&[0])?;
    wait_link_failure(&mut cluster, 0, 2, "names no node", Duration::from_secs(60))?;
    wait_link_failure(&mut cluster, 2, 0, "not authorized", Duration::from_secs(60))?;
    // material that does not parse reloads nothing
    let paths = cluster.staged_tls(2).expect("node two was staged with tls");
    std::fs::write(&paths.key, b"not a key")?;
    let refused = cluster.node_mut(2).command("RELOAD_TLS")?;
    assert!(refused["error"].as_str().unwrap_or_default().contains("not reloaded"), "{refused}");
    // reissued as itself and reloaded, every link comes up and the cluster serves
    let node2 = cluster.minted_node(2).to_string();
    cluster.reissue_leaf(2, Some(&node2))?;
    let report = cluster.node_mut(2).command("RELOAD_TLS")?;
    assert_eq!(report["ok"]["own_identity"], node2, "{report}");
    for (from, to) in [(0usize, 2usize), (2, 0), (1, 2), (2, 1)] {
        wait_link_failure(&mut cluster, from, to, "", Duration::from_secs(60))?;
    }
    let addrs: Vec<String> = (0..3).map(|node| cluster.node(node).endpoints.client.to_string()).collect();
    write_note(&addrs[2], 72_103, "after-identity").await.map_err(ok)?;
    wait_note(&addrs[0], 72_103, Some("after-identity"), Duration::from_secs(10)).await?;
    for key in keys.iter().step_by(5) {
        wait_note(&addrs[2], *key, Some(&format!("tls-{key}")), Duration::from_secs(10)).await?;
    }
    for id in 0..3 {
        assert_eq!(cluster.node(id).failure(), None, "node {id} died");
    }
    Ok(())
}
