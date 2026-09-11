//! The M0 fixture tests, and the two children they re-execute this binary as
//!
//! Three tests named in the acceptance tables of `docs/src/distributed/`: the fixture reports
//! endpoints its children actually bound with no port race and cleans up after a failure
//! (C11), its directed faults cover reconnects and its pause is not its cut (C11), and it
//! records every core and endpoint it handed out, disjoint where it claimed so (C10).
//!
//! The two `#[ignore]` functions at the bottom are the children. They are never run by
//! `cargo test`; the fixture runs them by name with `--exact --ignored`.

use std::io::Write as _;
use std::time::Duration;

use shoal::client::Shoal;
use shoal::server::conf::{Networking, Resources};
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
    // eight physical cores, two threads each; core 0 holds cpu 0 and is reserved
    let machine = Topology::synthetic(8, 2);
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
        assert_eq!(allocation.control, None, "no node has a control core before M1");
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
    crowded.disjoint_where_claimed().map_err(FixtureError::Allocation)?;
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
    let conf = utils::build_crash_config(&request.dir, 0)
        .resources(resources)
        .networking(Networking::default().port(0));
    let mut pool = match ShoalPool::<TestDb>::start(conf) {
        Ok(pool) => pool,
        Err(error) => {
            report(&format!("{} {error:?}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
    };
    // ready means every shard is answering, on the port the pool resolved
    let client = match pool.ready(utils::READY_TIMEOUT) {
        Ok(addr) => addr,
        Err(error) => {
            report(&format!("{} {error:?}", cluster::FAILED_LINE));
            std::process::exit(1);
        }
    };
    let endpoints = Endpoints {
        client,
        data: None,
        control: None,
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
        data: None,
        control: None,
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
    for kind in [NodeKind::Server, NodeKind::MockPeer] {
        assert!(!kind.child_fn().is_empty());
    }
}
