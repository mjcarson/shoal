//! What `shoalctl cluster` writes is a configuration the engine starts from
//!
//! shoalctl links the client half alone, so the `shoal.yml` it renders is a mirror of `Conf`
//! rather than a `Conf` ([F51](../../docs/src/features/cluster-deployment.md)). This is the test
//! that keeps the mirror honest: a bootstrap file and a join file rendered by the deployment are
//! parsed by the engine's own loader and validated by its own cluster check, the node program's
//! `claim` is run on the bootstrap file, a leaf is issued for the id it printed, and the node is
//! started from exactly those files and initialized through the admin credential they carry -
//! everything `bootstrap` does on a host, without the ssh.

use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::Conf;
use shoal_bench::workloads::schema::{Bench, BenchClient};
use shoalctl::deploy::inventory::Inventory;
use shoalctl::deploy::pki::Authority;
use shoalctl::deploy::render::{self, Entry, Layout};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The client port this test's node listens on, under the ephemeral range
const CLIENT_PORT: u16 = 23110;

/// The admin password the files are rendered with
const PASSWORD: &str = "deploy-render-test";

/// Build an inventory whose remote directory is a local one
///
/// # Arguments
///
/// * `dir` - The directory standing in for the remote one
fn inventory(dir: &std::path::Path) -> Inventory {
    // this test binary stands in for the server program, which the inventory only checks is an executable file
    let server = std::env::current_exe().expect("the test binary");
    let yaml = format!(
        "name: render\nserver: {server}\nremote_dir: {dir}\nreplication_factor: 1\ncontrol_voters: 1\nretire_after: 15s\nfailover: 1500ms\n\
         ports: {{client: {CLIENT_PORT}, peer: {peer}, control: {control}}}\n\
         resources: {{cores: 1, memory: 512Mi, control_core_shared: true}}\n\
         nodes:\n  - {{name: a, address: 127.0.0.1}}\n  - {{name: b, address: 127.0.0.2}}\n\
         bootstrap: [a]\n",
        server = server.display(),
        dir = dir.display(),
        peer = CLIENT_PORT + 1,
        control = CLIENT_PORT + 2,
    );
    let inventory: Inventory = serde_yaml::from_str(&yaml).expect("an inventory");
    inventory.validate().expect("a valid inventory");
    inventory
}

/// Load a rendered file the way the node program does, and run the engine's cluster check
///
/// # Arguments
///
/// * `path` - The rendered file
fn load(path: &std::path::Path) -> Conf {
    // the engine's own loader, which refuses unknown keys in every section rendered
    let conf = Conf::from_file(path.to_str().expect("a utf-8 path")).expect("a Conf");
    // and its own cluster validation, which loads the tls files as a start would
    let cluster = conf.cluster.as_ref().expect("a cluster block");
    cluster
        .validate(&conf.networking.interface, conf.networking.max_frame_bytes)
        .expect("a valid cluster block");
    conf
}

/// A rendered bootstrap file claims, starts and initializes; a rendered join file validates
#[test]
fn a_rendered_node_claims_starts_and_initializes() {
    // a directory under target/, since the storage path has to take direct io
    let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temp dir");
    let inventory = inventory(root.path());
    let layout = Layout {
        dir: inventory.remote_dir(),
    };
    std::fs::create_dir_all(layout.tls()).expect("a tls dir");
    let a = inventory.node("a").expect("node a");
    let b = inventory.node("b").expect("node b");
    // the bootstrap file, written where the deployment writes it
    let conf_path = std::path::PathBuf::from(layout.conf());
    let first = render::render(&inventory, &a, &Entry::Bootstrap, PASSWORD).expect("a file");
    std::fs::write(&conf_path, first).expect("the file");
    // claim it before any certificate exists, as a deployment does
    let raw = Conf::from_file(conf_path.to_str().unwrap()).expect("a Conf");
    let claimed = shoal::server::node::claim(&raw).expect("a claim");
    assert!(claimed.cluster.is_some(), "a bootstrap claim mints the cluster");
    // a second claim of the same directory is the same node
    assert_eq!(shoal::server::node::claim(&raw).expect("a claim"), claimed);
    // issue the leaf naming it, and the files the engine will load
    let authority = Authority::mint(&inventory.name).expect("an authority");
    let leaf = authority
        .issue(&claimed.node.to_string(), &a.name, a.address)
        .expect("a leaf");
    std::fs::write(layout.cert(), &leaf.cert).unwrap();
    std::fs::write(layout.key(), &leaf.key).unwrap();
    std::fs::write(layout.ca(), authority.cert_pem()).unwrap();
    // the engine accepts the file whole, tls included
    let conf = load(&conf_path);
    assert!(conf.auth.required);
    // the one move setting an inventory can name reaches the engine
    assert_eq!(
        conf.cluster.as_ref().unwrap().migration.retire_after.duration(),
        Duration::from_secs(15)
    );
    // the failover base the inventory names reaches the cluster block the engine reads
    assert_eq!(
        conf.cluster.as_ref().unwrap().primary_failover_after.duration(),
        std::time::Duration::from_millis(1500)
    );
    // a join file for the second node validates too, against the same authority
    let second = render::render(
        &inventory,
        &b,
        &Entry::Join(vec![a.control_addr(&inventory.ports)]),
        PASSWORD,
    )
    .expect("a file");
    let join_path = root.path().join("join.yml");
    std::fs::write(&join_path, second).unwrap();
    let join = load(&join_path);
    let cluster = join.cluster.as_ref().unwrap();
    assert!(!cluster.bootstrap);
    assert_eq!(cluster.seeds, vec![format!("127.0.0.1:{}", CLIENT_PORT + 2)]);
    // the node starts from the file as the node the claim named
    let mut pool = shoal::ShoalPool::<Bench>::start(conf).expect("a started node");
    pool.ready(Duration::from_secs(60)).expect("a ready node");
    assert_eq!(pool.identity().node, claimed.node);
    // and the admin credential the file carries is one the deployment can initialize with
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(initialize(claimed.node));
    pool.exit().expect("a clean exit");
}

/// Connect as the rendered admin, wait for the node, initialize it and wait for writes
///
/// # Arguments
///
/// * `node` - The node the claim named
async fn initialize(node: NodeId) {
    // the credential the rendered file derived from the password
    let options = shoal::client::ClientOptions::new().credentials(
        shoal::shared::auth::Credentials::scram("admin".to_string(), PASSWORD.to_string()),
    );
    let shoal = Arc::new(
        shoal::Shoal::<BenchClient>::with_options(format!("127.0.0.1:{CLIENT_PORT}"), options)
            .await
            .expect("an admin connection"),
    );
    // the member up and voting, as the deployment waits for it
    let deadline = Instant::now() + Duration::from_secs(60);
    let model = loop {
        let model = shoalctl::cluster::poll(&shoal).await.expect("a poll");
        if shoalctl::deploy::ops::members_ready(&model, &[node], 1) {
            break model;
        }
        assert!(Instant::now() < deadline, "the node never came up: {model:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // an initialize the rendered admin is allowed to send
    let response = shoal
        .admin(&AdminRequest {
            op: uuid::Uuid::new_v4(),
            expected_version: model.version,
            kind: AdminKind::Initialize { nodes: vec![node] },
        })
        .await
        .expect("an admin answer");
    assert!(
        matches!(response.outcome, Ok(AdminOutcome::Applied { .. })),
        "{:?}",
        response.outcome
    );
    // and default writes admitted afterwards
    loop {
        let model = shoalctl::cluster::poll(&shoal).await.expect("a poll");
        if model.default_writes == "admitted" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "writes were never admitted: {}",
            model.default_writes
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A group splitting a node's logs from its archives renders the roots the engine claims
///
/// The deployment creates, wipes and destroys the roots `Node::storage` names, and preflight
/// looks for a marker in each, so they have to be exactly the roots the engine locks and marks
/// ([F53](../../docs/src/features/inventory-wizard.md)).
#[test]
fn a_group_split_renders_the_roots_the_engine_claims() {
    // a directory under target/, since the storage paths have to take direct io
    let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temp dir");
    let base = inventory(root.path());
    // the same deployment with a group whose nodes keep logs and archives apart
    let fast = root.path().join("fast");
    let bulk = root.path().join("bulk");
    let mut inventory = base.clone();
    inventory.groups.insert(
        "split".to_string(),
        shoalctl::deploy::inventory::GroupSpec {
            resources: None,
            storage: Some(shoalctl::deploy::inventory::StorageSpec {
                latency: Some(fast.display().to_string()),
                throughput: Some(format!("{}/", bulk.display())),
            }),
        },
    );
    inventory.nodes[0].group = Some("split".to_string());
    inventory.validate().expect("a valid inventory");
    let a = inventory.node("a").expect("node a");
    // the rendered file, loaded by the engine's own loader
    let layout = Layout {
        dir: inventory.remote_dir(),
    };
    std::fs::create_dir_all(&layout.dir).expect("a remote dir");
    let conf_path = std::path::PathBuf::from(layout.conf());
    let file = render::render(&inventory, &a, &Entry::Bootstrap, PASSWORD).expect("a file");
    std::fs::write(&conf_path, file).expect("the file");
    let conf = Conf::from_file(conf_path.to_str().unwrap()).expect("a Conf");
    // the engine's roots are the deployment's, in the same order
    let roots: Vec<String> = conf
        .storage
        .roots()
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    assert_eq!(roots, a.storage.roots());
    assert_eq!(roots, vec![fast.display().to_string(), bulk.display().to_string()]);
    // a claim marks the primary, the first root, which is what preflight finds on a node that
    // was claimed and never started; a start mirrors it into the rest, which preflight also reads
    shoal::server::node::claim(&conf).expect("a claim");
    let roots = a.storage.roots();
    assert!(
        std::path::Path::new(&roots[0]).join("shoal-meta.json").is_file(),
        "{} holds no marker after a claim",
        roots[0]
    );
}
