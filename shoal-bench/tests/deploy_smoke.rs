//! `shoal-benchctl cluster` against real hosts, when an inventory is named
//!
//! `SHOAL_DEPLOY_INVENTORY=<file>` names an inventory whose hosts are reachable over keyless ssh
//! with passwordless sudo and whose `server` is a `shoal-node` built for their cpus. The test then
//! bootstraps it with `--wipe`, writes rows through one node and reads every one of them back
//! through every node, adds each inventory node the bootstrap left out with `--rebalance` and
//! reads again through it, prints the status, and destroys the cluster unless
//! `SHOAL_DEPLOY_KEEP` is set. Unset, it says so and passes: the deployment is exercised by name
//! where hosts exist, and nowhere silently ([F51](../../docs/src/features/cluster-deployment.md)).

use shoal::shared::queries::Queries;
use shoal::Shoal;
use shoal_bench::workloads::schema::{BenchClient, Item, ItemGet};
use shoalctl::deploy::inventory::{socket, Inventory};
use shoalctl::deploy::Deployment;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

/// How many rows the smoke test writes
const ROWS: u64 = 1_000;

/// Run one `shoal-benchctl cluster` command and refuse a failure
///
/// # Arguments
///
/// * `inventory` - The inventory
/// * `args` - The cluster command and its arguments
fn ctl(inventory: &Path, args: &[&str]) {
    // the ctl built with this test, against the same schema as the rows below
    let mut command = Command::new(env!("CARGO_BIN_EXE_shoal-benchctl"));
    command.arg("cluster").args(&args[..1]);
    command.arg("--inventory").arg(inventory).args(&args[1..]);
    let status = command.status().expect("shoal-benchctl runs");
    assert!(status.success(), "shoal-benchctl cluster {args:?} failed: {status}");
}

/// Write the smoke rows through one client
///
/// # Arguments
///
/// * `client` - The client to write through
async fn write(client: &Shoal<BenchClient>) {
    // every row in one bundle, drained so every write is acknowledged
    let mut queries = Queries::<BenchClient>::default();
    for id in 0..ROWS {
        queries.add_mut(Item {
            id,
            bucket: id % 16,
            label: format!("label-{id}"),
            payload: format!("payload-{id}"),
        });
    }
    let mut responses = client.send(queries).await.expect("a sent bundle");
    let mut acknowledged = 0;
    while responses.next().await.expect("an answer").is_some() {
        acknowledged += 1;
    }
    assert_eq!(acknowledged, ROWS as usize, "every write is acknowledged");
}

/// Read every smoke row back through one client
///
/// # Arguments
///
/// * `client` - The client to read through
/// * `through` - Which node it is, for the assertion
async fn read(client: &Shoal<BenchClient>, through: &str) {
    // one get for every key
    let response = client
        .send_one(ItemGet::new((0..ROWS).collect()))
        .await
        .expect("a get");
    let rows = response
        .access::<Item>()
        .expect("readable rows")
        .map_or(0, |rows| rows.iter().count());
    assert_eq!(rows, ROWS as usize, "every row reads back through {through}");
}

/// Connect to one deployed node as the admin
///
/// # Arguments
///
/// * `deployment` - The deployment
/// * `address` - The node's address
async fn connect(deployment: &Deployment, address: &str) -> Arc<Shoal<BenchClient>> {
    let addr = socket(address.parse().expect("an address"), deployment.inventory.ports.client);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    deployment
        .connect::<BenchClient>(&addr, deadline)
        .await
        .expect("an admin connection")
}

/// Bootstrap, write, read through every node, add, read again, and destroy
#[test]
fn a_deployed_cluster_serves_every_row_from_every_node() {
    let Ok(path) = std::env::var("SHOAL_DEPLOY_INVENTORY") else {
        eprintln!(
            "SKIPPING a_deployed_cluster_serves_every_row_from_every_node: SHOAL_DEPLOY_INVENTORY names no inventory"
        );
        return;
    };
    let path = Path::new(&path);
    let inventory = Inventory::load(path).expect("an inventory");
    // the nodes the bootstrap leaves for add
    let bootstrap = inventory.bootstrap_names();
    let later: Vec<String> = inventory
        .nodes
        .iter()
        .map(|node| node.name.clone())
        .filter(|name| !bootstrap.contains(name))
        .collect();
    // bootstrap over whatever an earlier run left
    ctl(path, &["bootstrap", "--wipe"]);
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let deployment = Deployment::open(path).expect("a deployment");
    runtime.block_on(async {
        // write through the first node
        let record = deployment.state.record().expect("a record");
        let first = &record.nodes[&bootstrap[0]];
        write(&*connect(&deployment, &first.address).await).await;
        // and read through every one of them
        for (name, node) in &record.nodes {
            read(&*connect(&deployment, &node.address).await, name).await;
        }
    });
    // every node the bootstrap left out joins, takes a share, and serves every row
    for name in &later {
        ctl(path, &["add", name, "--wipe", "--rebalance"]);
        let record = deployment.state.record().expect("a record");
        runtime.block_on(async {
            read(&*connect(&deployment, &record.nodes[name].address).await, name).await;
        });
    }
    ctl(path, &["status"]);
    // leave it up only when asked to
    if std::env::var_os("SHOAL_DEPLOY_KEEP").is_none() {
        ctl(path, &["destroy", "--yes"]);
    }
}
