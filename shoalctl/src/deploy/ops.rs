//! Bootstrapping a cluster, adding a node to one, and the day-to-day around them
//!
//! Each operation is the matching runbook
//! ([1](../../../docs/src/operations/runbooks.md#1-bootstrap),
//! [2](../../../docs/src/operations/runbooks.md#2-add-a-node)) done by a program: stage every
//! node's files, claim its directory so its id is known, issue it a leaf naming that id, start
//! it under systemd, and wait for what the runbook says to wait for before the next step. Only
//! the admin half is generic over the schema, since the client refuses a schema it was not
//! built for at the hello.

use color_eyre::eyre::{bail, eyre, WrapErr};
use rkyv::Archive;
use shoal::client::ClientOptions;
use shoal::serde_json::Value;
use shoal::shared::auth::Credentials;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::traits::QuerySupport;
use shoal::Shoal;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::inventory::{Inventory, Node};
use super::pki::Authority;
use super::remote::{quote, Host, SIGILL_STATUS};
use super::render::{self, Entry, Layout};
use super::state::{ClusterRecord, NodeRecord, State};
use super::unit;
use crate::cluster::{ClusterModel, Follow};

/// How long a started node has to be seen up
pub(super) const UP_TIMEOUT: Duration = Duration::from_secs(180);

/// How long the cluster has to admit default writes after `Initialize`
const READY_TIMEOUT: Duration = Duration::from_secs(180);

/// The lines of a followed record that name a group's failure
///
/// A repair, backup or restore is done when every group is, failed ones included, so the
/// failures are read from the record's own lines
/// ([Resolved #154](../../../docs/src/appendix/resolved/admin-hides-failed-groups.md)).
///
/// # Arguments
///
/// * `lines` - The record as the cluster tab draws it
fn failed_lines(lines: &[String]) -> Vec<&str> {
    // a group's line carries its outcome, which says Failed when it did
    lines
        .iter()
        .filter(|line| line.contains("\"Failed\""))
        .map(|line| line.trim())
        .collect()
}

/// How long a rebalance plan is followed before the deployment stops waiting on it
const PLAN_TIMEOUT: Duration = Duration::from_secs(1800);

/// How often a wait polls
pub(super) const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The file the authority's certificate is kept in
const CA_CERT: &str = "ca.pem";

/// The file the authority's key is kept in
const CA_KEY: &str = "ca.key";

/// What a host told the preflight about itself
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// The user ssh logs in as
    pub login: String,
    /// The user the node runs as: the inventory's `user`, or the login
    pub user: String,
    /// How many cpus it has
    pub cpus: usize,
    /// Whether the node's directory already holds a claimed marker
    pub claimed: bool,
}

/// A deployment: an inventory and the state it keeps
pub struct Deployment {
    /// What to deploy
    pub inventory: Inventory,
    /// What has been deployed
    pub state: State,
}

/// Quote every directory for a shell and join them with spaces
///
/// # Arguments
///
/// * `paths` - The directories
pub(super) fn quoted(paths: &[String]) -> String {
    paths.iter().map(|path| quote(path)).collect::<Vec<_>>().join(" ")
}

/// Say what the deployment is doing, on stderr so stdout stays the command's answer
///
/// # Arguments
///
/// * `node` - The node it is about, if one
/// * `message` - What is happening
pub(super) fn step(node: Option<&str>, message: &str) {
    match node {
        Some(node) => eprintln!("[{node}] {message}"),
        None => eprintln!("{message}"),
    }
}

impl Deployment {
    /// Open a deployment from its inventory
    ///
    /// # Arguments
    ///
    /// * `path` - The inventory file
    ///
    /// # Errors
    ///
    /// When the inventory is invalid or its state cannot be located.
    pub fn open(path: &Path) -> color_eyre::Result<Self> {
        // the inventory, validated
        let inventory = Inventory::load(path)?;
        // and where its state lives
        let state = State::locate(&inventory.name)?;
        Ok(Deployment { inventory, state })
    }

    /// Open a deployed cluster to talk to it, without the server program it was deployed with
    ///
    /// Everything that connects as the admin and nothing more - the terminal UI, a loader -
    /// opens a deployment this way, so it runs on a machine that never built the node binary.
    ///
    /// # Arguments
    ///
    /// * `path` - The inventory file
    ///
    /// # Errors
    ///
    /// When the inventory describes no cluster or its state cannot be located.
    pub fn attach(path: &Path) -> color_eyre::Result<Self> {
        // the inventory, judged as a cluster only
        let inventory = Inventory::read(path)?;
        // and where its state lives
        let state = State::locate(&inventory.name)?;
        Ok(Deployment { inventory, state })
    }

    /// Where every node's files live on its host
    fn layout(&self) -> Layout {
        Layout {
            dir: self.inventory.remote_dir(),
        }
    }

    /// The cluster's authority, minted on first use and rebuilt from its key after
    fn authority(&self) -> color_eyre::Result<Authority> {
        // an authority this cluster already has
        if let Some(key) = self.state.read_secret(CA_KEY)? {
            return Authority::from_key(&self.inventory.name, &key);
        }
        // or a new one, with the certificate every node trusts written beside its key
        let authority = Authority::mint(&self.inventory.name)?;
        self.state.write_secret(CA_CERT, &authority.cert_pem())?;
        self.state.write_secret(CA_KEY, &authority.key_pem())?;
        Ok(authority)
    }

    /// The authority's certificate as it was distributed
    fn ca_pem(&self) -> color_eyre::Result<String> {
        self.state
            .read_secret(CA_CERT)?
            .ok_or_else(|| eyre!("{} has no {CA_CERT}", self.state.dir().display()))
    }

    /// Check a host can take a node, and wipe an old one from it if asked
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `wipe` - Whether an existing directory may be deleted
    ///
    /// # Errors
    ///
    /// When the host cannot be reached, has no passwordless sudo or tls module, or already
    /// holds a claimed directory and `wipe` was not given.
    pub fn preflight(&self, node: &Node, wipe: bool) -> color_eyre::Result<Facts> {
        step(Some(&node.name), "checking the host");
        let host = Host {
            target: node.target.clone(),
        };
        let layout = self.layout();
        // a node is claimed if any of its roots holds a marker, since every root past the
        // primary carries a mirror of it and refuses a node that is not the one it names
        let markers = node
            .storage
            .roots()
            .iter()
            .map(|root| format!("test -e {}", quote(&format!("{root}/shoal-meta.json"))))
            .collect::<Vec<_>>()
            .join(" || ");
        // one round trip that reports everything as key=value lines
        let script = format!(
            "echo user=$(id -un); echo cpus=$(nproc); \
             if sudo -n true 2>/dev/null; then echo sudo=yes; else echo sudo=no; fi; \
             if {markers}; then echo claimed=yes; else echo claimed=no; fi; \
             if lsmod | grep -qw '^tls' || modinfo tls >/dev/null 2>&1; then echo tls=yes; else echo tls=no; fi; \
             if command -v systemctl >/dev/null; then echo systemd=yes; else echo systemd=no; fi",
        );
        let output = host.run(&script)?;
        let facts: std::collections::HashMap<&str, &str> = output
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect();
        let fact = |key: &str| facts.get(key).copied().unwrap_or_default();
        // every requirement is refused by name
        if fact("sudo") != "yes" {
            bail!("{} has no passwordless sudo, which installing a unit needs", node.name);
        }
        if fact("systemd") != "yes" {
            bail!("{} has no systemctl", node.name);
        }
        if fact("tls") != "yes" {
            bail!("{} has no kernel tls module, which the peer lanes need", node.name);
        }
        let mut claimed = fact("claimed") == "yes";
        // an old node there is deleted only when the operator said so
        if claimed {
            if !wipe {
                bail!(
                    "{} already holds a claimed node under {}; `destroy` it or pass --wipe",
                    node.name,
                    layout.dir
                );
            }
            step(Some(&node.name), "wiping the node that was there");
            host.run(&format!(
                "sudo -n systemctl disable --now {unit} 2>/dev/null || true; sudo -n rm -rf {roots} {tls}",
                unit = quote(&self.inventory.unit_name()),
                roots = quoted(&node.storage.roots()),
                tls = quote(&layout.tls()),
            ))?;
            claimed = false;
        }
        // the user the node runs as, created if the host lacks it
        let login = fact("user").to_string();
        let user = self.inventory.user.clone().unwrap_or_else(|| login.clone());
        if user != login {
            let created = host.run(&format!(
                "if id -u {user} >/dev/null 2>&1; then echo exists; else \
                 sudo -n useradd --system --no-create-home --shell /usr/sbin/nologin {user} && echo created; fi",
                user = quote(&user),
            ))?;
            if created.trim() == "created" {
                step(Some(&node.name), &format!("created the system user {user}"));
            }
        }
        Ok(Facts {
            login,
            user,
            cpus: fact("cpus").parse().unwrap_or(0),
            claimed,
        })
    }

    /// Put a node's program and configuration on its host
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `facts` - What its preflight found
    /// * `entry` - Whether it mints the cluster or joins it
    /// * `password` - The admin password
    ///
    /// # Errors
    ///
    /// When a directory, the copy or its digest check fails.
    pub fn stage(
        &self,
        node: &Node,
        facts: &Facts,
        entry: &Entry,
        password: &str,
    ) -> color_eyre::Result<()> {
        let host = Host {
            target: node.target.clone(),
        };
        let layout = self.layout();
        let server = self.inventory.server_name()?;
        // the directories, owned by the user the node runs as
        step(Some(&node.name), &format!("staging {} for {}", layout.dir, facts.user));
        // a storage root outside the remote directory is handed over on its own (F53)
        let roots = quoted(&node.storage.roots());
        host.run(&format!(
            "set -e; sudo -n mkdir -p {bin} {roots} {tls}; sudo -n chown -R {user}: {dir} {roots}; sudo -n chmod 700 {tls}",
            bin = quote(&format!("{}/bin", layout.dir)),
            tls = quote(&layout.tls()),
            user = quote(&facts.user),
            dir = quote(&layout.dir),
        ))?;
        // the program, copied to the login's own temp dir and checked before it replaces anything,
        // since the node's directory is not the login's to write
        let partial = self.push_binary(&host, &node.name)?;
        install_binary(&host, &partial, &layout.binary(&server), &facts.user)?;
        // the configuration, which holds a credential and so is the node's alone
        let conf = render::render(&self.inventory, node, entry, password)?;
        host.write(&layout.conf(), conf.as_bytes(), 0o600, Some(&facts.user))?;
        Ok(())
    }

    /// Copy the inventory's server program to a host's temp dir and check its digest there
    ///
    /// Nothing the node runs is touched: the copy lands in the login's own temp dir, which
    /// the caller installs from once it has judged the program.
    ///
    /// # Arguments
    ///
    /// * `host` - The host
    /// * `name` - The node's inventory name, for the refusal
    ///
    /// # Errors
    ///
    /// When the copy fails or lands with another digest than the local program's.
    pub(super) fn push_binary(&self, host: &Host, name: &str) -> color_eyre::Result<String> {
        let server = self.inventory.server_name()?;
        // the login's own temp dir, since the node's directory is not the login's to write
        let partial = format!("/tmp/shoalctl-{}-{server}.new", self.inventory.name);
        host.copy(&self.inventory.server, &partial)?;
        // and the bytes that landed are the bytes we meant to send
        let local = digest(&self.inventory.server)?;
        let remote = host.run(&format!("sha256sum {}", quote(&partial)))?;
        let remote = remote.split_whitespace().next().unwrap_or_default();
        if remote != local {
            bail!(
                "{partial} on {name} has digest {remote}, but {} has {local}",
                self.inventory.server.display()
            );
        }
        Ok(partial)
    }

    /// Claim a node's directory on its host and read back who it is
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    ///
    /// # Errors
    ///
    /// When the program fails, naming a build for another CPU when that is why.
    pub fn claim(&self, node: &Node, facts: &Facts) -> color_eyre::Result<(NodeId, Option<String>)> {
        step(Some(&node.name), "claiming its directory");
        let host = Host {
            target: node.target.clone(),
        };
        let layout = self.layout();
        // run the program's own claim as the user it will serve as, so the files it writes are
        // that user's
        let script = format!(
            "cd {dir} && sudo -n -u {user} {binary} claim --conf {conf}",
            dir = quote(&layout.dir),
            user = quote(&facts.user),
            binary = quote(&layout.binary(&self.inventory.server_name()?)),
            conf = quote(&layout.conf()),
        );
        let output = host.output(&script, None)?;
        // an illegal instruction is a build for a newer cpu than this host's
        if output.status == Some(SIGILL_STATUS) {
            bail!(
                "{} died of an illegal instruction on {}: it was built for another cpu. build it \
                 with RUSTFLAGS=\"-C target-cpu=<the oldest host's cpu>\" rather than native",
                self.inventory.server.display(),
                node.name
            );
        }
        if output.status != Some(0) {
            bail!(
                "claim failed on {} with status {:?}: {}",
                node.name,
                output.status,
                output.stderr.trim()
            );
        }
        // its last line is the report
        let line = output
            .stdout
            .lines()
            .last()
            .ok_or_else(|| eyre!("claim on {} printed nothing", node.name))?;
        let report: Value = serde_json::from_str(line)
            .wrap_err_with(|| format!("claim on {} printed {line:?}", node.name))?;
        let id = report["node"]
            .as_str()
            .and_then(|id| id.parse::<Uuid>().ok())
            .ok_or_else(|| eyre!("claim on {} named no node: {line}", node.name))?;
        let cluster = report["cluster"].as_str().map(str::to_string);
        Ok((NodeId(id), cluster))
    }

    /// Issue a node its leaf and put it, its key and the authority on its host
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `facts` - What its preflight found
    /// * `id` - The id its claim printed
    ///
    /// # Errors
    ///
    /// When the leaf cannot be issued or written.
    pub fn provision_tls(&self, node: &Node, facts: &Facts, id: NodeId) -> color_eyre::Result<()> {
        step(Some(&node.name), &format!("issuing a leaf for shoal-node://{id}"));
        let host = Host {
            target: node.target.clone(),
        };
        let layout = self.layout();
        // a leaf naming the node, the host and the address peers dial
        let leaf = self
            .authority()?
            .issue(&id.to_string(), &node.name, node.address)?;
        let owner = Some(facts.user.as_str());
        host.write(&layout.cert(), leaf.cert.as_bytes(), 0o644, owner)?;
        host.write(&layout.key(), leaf.key.as_bytes(), 0o600, owner)?;
        host.write(&layout.ca(), self.ca_pem()?.as_bytes(), 0o644, owner)?;
        Ok(())
    }

    /// Install a node's unit and start it
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `facts` - What its preflight found
    ///
    /// # Errors
    ///
    /// When the unit cannot be written or started.
    pub fn start_unit(&self, node: &Node, facts: &Facts) -> color_eyre::Result<()> {
        step(Some(&node.name), &format!("starting {}", self.inventory.unit_name()));
        let host = Host {
            target: node.target.clone(),
        };
        let unit_name = self.inventory.unit_name();
        // the unit, written as root where systemd reads it
        let text = unit::render(&self.inventory, &facts.user)?;
        host.write(
            &format!("/etc/systemd/system/{unit_name}"),
            text.as_bytes(),
            0o644,
            Some("root"),
        )?;
        // and started, and enabled for the next boot
        host.run(&format!(
            "set -e; sudo -n systemctl daemon-reload; sudo -n systemctl enable --now {}",
            quote(&unit_name)
        ))?;
        Ok(())
    }

    /// Run a systemctl verb on some nodes, or on every node deployed
    ///
    /// # Arguments
    ///
    /// * `verb` - `start`, `stop`, `restart` or `is-active`
    /// * `only` - The node to act on, or none for every node the state records
    ///
    /// # Errors
    ///
    /// When a node is not deployed or the verb fails on it.
    pub fn systemctl(&self, verb: &str, only: Option<&str>) -> color_eyre::Result<()> {
        let record = self.state.record()?;
        for (name, node) in &record.nodes {
            // skip every node but the one asked for
            if only.is_some_and(|only| only != name) {
                continue;
            }
            let host = Host {
                target: node.target.clone(),
            };
            let output = host.output(
                &format!("sudo -n systemctl {verb} {}", quote(&self.inventory.unit_name())),
                None,
            )?;
            // is-active answers on stdout with a non-zero status for an inactive unit
            let answer = output.stdout.trim();
            if verb == "is-active" {
                println!("{name}: {}", if answer.is_empty() { "unknown" } else { answer });
            } else if output.status != Some(0) {
                bail!("systemctl {verb} failed on {name}: {}", output.stderr.trim());
            } else {
                step(Some(name), &format!("{verb} done"));
            }
        }
        // a name that matched nothing is a typo
        if let Some(only) = only {
            if !record.nodes.contains_key(only) {
                bail!("no node named {only:?} has been deployed");
            }
        }
        Ok(())
    }

    /// Print a node's journal
    ///
    /// # Arguments
    ///
    /// * `name` - The node
    /// * `lines` - How many lines from the end
    ///
    /// # Errors
    ///
    /// When the node is not deployed or its journal cannot be read.
    pub fn logs(&self, name: &str, lines: usize) -> color_eyre::Result<()> {
        let record = self.state.record()?;
        let node = record
            .nodes
            .get(name)
            .ok_or_else(|| eyre!("no node named {name:?} has been deployed"))?;
        let host = Host {
            target: node.target.clone(),
        };
        let output = host.run(&format!(
            "sudo -n journalctl --no-pager -u {} -n {lines}",
            quote(&self.inventory.unit_name())
        ))?;
        print!("{output}");
        Ok(())
    }

    /// Stop and delete every node of the inventory and forget the cluster
    ///
    /// Every host the inventory lists is visited, deployed or not, so a run that died halfway
    /// leaves nothing behind.
    ///
    /// # Errors
    ///
    /// When a host cannot be reached.
    pub fn destroy(&self) -> color_eyre::Result<()> {
        let layout = self.layout();
        let unit_name = self.inventory.unit_name();
        for spec in &self.inventory.nodes {
            // the directories this node's data was given, which need no address to resolve
            let (storage, _) = self.inventory.resolve_storage(spec);
            let roots = storage.roots();
            step(
                Some(&spec.name),
                &format!("removing {unit_name}, {} and {}", layout.dir, roots.join(", ")),
            );
            let host = Host {
                target: spec.target().to_string(),
            };
            host.run(&format!(
                "sudo -n systemctl disable --now {unit} 2>/dev/null || true; \
                 sudo -n rm -f /etc/systemd/system/{unit}; sudo -n systemctl daemon-reload; \
                 sudo -n systemctl reset-failed {unit} 2>/dev/null || true; sudo -n rm -rf {dir} {roots}",
                unit = quote(&unit_name),
                dir = quote(&layout.dir),
                roots = quoted(&roots),
            ))?;
        }
        // and the local state, authority and all
        step(None, &format!("deleting {}", self.state.dir().display()));
        self.state.delete()
    }

    /// Connect to a node as the cluster's admin
    ///
    /// # Arguments
    ///
    /// * `addr` - The node's client address
    /// * `deadline` - How long to keep trying while it comes up
    ///
    /// # Errors
    ///
    /// When no connection could be had by the deadline.
    pub async fn connect<S>(&self, addr: &str, deadline: Instant) -> color_eyre::Result<Arc<Shoal<S>>>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let password = self.state.password()?;
        loop {
            // authenticate as the admin every node was configured with
            let options = ClientOptions::new()
                .credentials(Credentials::scram(self.inventory.admin.clone(), password.clone()));
            match Shoal::<S>::with_options(addr, options).await {
                Ok(shoal) => return Ok(Arc::new(shoal)),
                // a node that is still starting refuses; keep trying until the deadline
                Err(_) if Instant::now() < deadline => tokio::time::sleep(POLL_INTERVAL).await,
                Err(error) => return Err(eyre!("could not connect to {addr}: {error:?}")),
            }
        }
    }

    /// Bootstrap a new cluster from the inventory's bootstrap set
    ///
    /// # Arguments
    ///
    /// * `wipe` - Whether an old node found on a host may be deleted
    ///
    /// # Errors
    ///
    /// When the cluster was already deployed, or any step fails.
    pub async fn bootstrap<S>(&self, wipe: bool) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // a cluster is bootstrapped once; a second bootstrap would mint a second cluster
        let mut record = self.state.record()?;
        if record.initialized || !record.nodes.is_empty() {
            bail!(
                "{} was already deployed from {}; `destroy` it first, or `add` to it",
                self.inventory.name,
                self.state.dir().display()
            );
        }
        self.state.create()?;
        let password = self.state.password()?;
        let nodes = self.inventory.bootstrap_nodes()?;
        let first = &nodes[0];
        // every host is checked before anything is written to any of them
        let mut facts = Vec::with_capacity(nodes.len());
        for node in &nodes {
            facts.push(self.preflight(node, wipe)?);
        }
        // the first node mints the cluster and the rest join through it
        let seeds = vec![first.control_addr(&self.inventory.ports)];
        let mut ids = Vec::with_capacity(nodes.len());
        for (index, (node, facts)) in nodes.iter().zip(&facts).enumerate() {
            let entry = if index == 0 {
                Entry::Bootstrap
            } else {
                Entry::Join(seeds.clone())
            };
            self.stage(node, facts, &entry, &password)?;
            // its id first, then the leaf that names it
            let (id, cluster) = self.claim(node, facts)?;
            if index == 0 {
                record.cluster = cluster;
            }
            self.provision_tls(node, facts, id)?;
            record.nodes.insert(
                node.name.clone(),
                NodeRecord {
                    node: id.to_string(),
                    address: node.address.to_string(),
                    target: node.target.clone(),
                },
            );
            self.state.save(&record)?;
            ids.push(id);
        }
        // the first node alone, until it leads a control group of one
        self.start_unit(first, &facts[0])?;
        let shoal = self
            .connect::<S>(&first.client_addr(&self.inventory.ports), Instant::now() + UP_TIMEOUT)
            .await?;
        wait_for_members(&shoal, &ids[..1], 1).await?;
        // then every other node, which joins through it and is promoted to voter
        for (node, facts) in nodes.iter().zip(&facts).skip(1) {
            self.start_unit(node, facts)?;
        }
        let voters = (self.inventory.control_voters as usize).min(nodes.len());
        wait_for_members(&shoal, &ids, voters).await?;
        // place every tablet over them, once, in the inventory's order
        self.initialize(&shoal, &mut record, &ids).await?;
        // and wait for what the runbook says to wait for before opening it to clients
        wait_for_writes(&shoal).await?;
        step(
            None,
            &format!(
                "{} is up: {} node{} at factor {}; admin {} with the password in {}",
                self.inventory.name,
                nodes.len(),
                if nodes.len() == 1 { "" } else { "s" },
                self.inventory.replication_factor,
                self.inventory.admin,
                self.state.dir().join("admin.password").display()
            ),
        );
        Ok(())
    }

    /// Send `Initialize` for the bootstrap set, once
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    /// * `record` - The cluster record, which keeps the operation id
    /// * `ids` - The nodes, in placement order
    async fn initialize<S>(
        &self,
        shoal: &Arc<Shoal<S>>,
        record: &mut ClusterRecord,
        ids: &[NodeId],
    ) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
    {
        // the operation id is decided and kept before it is sent, so a resend repeats it
        let op = *record.initialize_op.get_or_insert_with(Uuid::new_v4);
        self.state.save(record)?;
        step(None, &format!("initializing over {} nodes as {op}", ids.len()));
        let model = crate::cluster::poll(shoal).await.map_err(|error| eyre!(error))?;
        let response = shoal
            .admin(&AdminRequest {
                op,
                expected_version: model.version,
                kind: AdminKind::Initialize {
                    nodes: ids.to_vec(),
                },
            })
            .await
            .map_err(|error| eyre!("initialize: {error:?}"))?;
        match response.outcome {
            Ok(AdminOutcome::Applied { .. } | AdminOutcome::Repeated { .. }) => {}
            Ok(other) => return Err(eyre!("initialize answered {other:?}")),
            Err(error) => return Err(eyre!("initialize was refused: {} ({:?})", error.msg, error.code())),
        }
        record.initialized = true;
        self.state.save(record)?;
        Ok(())
    }

    /// Add a node of the inventory to the deployed cluster
    ///
    /// # Arguments
    ///
    /// * `name` - The node
    /// * `wipe` - Whether an old node found on its host may be deleted
    /// * `rebalance` - Whether to move a share of the cluster's data onto it afterwards
    ///
    /// # Errors
    ///
    /// When the cluster is not deployed, the node already is, or any step fails.
    pub async fn add<S>(&self, name: &str, wipe: bool, rebalance: bool) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // a node joins a cluster that exists and that it is not already in
        let mut record = self.state.record()?;
        if !record.initialized {
            bail!("{} has not been bootstrapped from {}", self.inventory.name, self.state.dir().display());
        }
        if record.nodes.contains_key(name) {
            bail!("{name} is already deployed in {}", self.inventory.name);
        }
        let node = self.inventory.node(name)?;
        let password = self.state.password()?;
        // any node already deployed answers for the cluster
        let shoal = self.any_member::<S>(&record).await?;
        // it joins through every member the cluster committed, not only the first
        let seeds = control_addresses(&shoal).await?;
        if seeds.is_empty() {
            bail!("the cluster names no member to join through");
        }
        let facts = self.preflight(&node, wipe)?;
        self.stage(&node, &facts, &Entry::Join(seeds), &password)?;
        let (id, _) = self.claim(&node, &facts)?;
        self.provision_tls(&node, &facts, id)?;
        record.nodes.insert(
            node.name.clone(),
            NodeRecord {
                node: id.to_string(),
                address: node.address.to_string(),
                target: node.target.clone(),
            },
        );
        self.state.save(&record)?;
        self.start_unit(&node, &facts)?;
        // wait until the cluster sees it up, beside every node it already had
        let mut ids = record
            .nodes
            .values()
            .map(|node| node.node.parse::<Uuid>().map(NodeId))
            .collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        let voters = (self.inventory.control_voters as usize).min(ids.len());
        wait_for_members(&shoal, &ids, voters).await?;
        step(Some(name), "joined");
        // a joined node holds nothing until something is moved onto it
        if rebalance {
            self.rebalance(&shoal).await?;
        } else {
            step(
                Some(name),
                "holds no data yet; run `cluster rebalance` to move a share onto it",
            );
        }
        Ok(())
    }

    /// Ask the cluster to rebalance and follow the plan until it is done
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    ///
    /// # Errors
    ///
    /// When the request is refused or the plan does not finish in time.
    pub async fn rebalance<S>(&self, shoal: &Arc<Shoal<S>>) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let op = Uuid::new_v4();
        step(None, &format!("rebalancing as {op}"));
        let model = crate::cluster::poll(shoal).await.map_err(|error| eyre!(error))?;
        let response = shoal
            .admin(&AdminRequest {
                op,
                expected_version: model.version,
                kind: AdminKind::Rebalance,
            })
            .await
            .map_err(|error| eyre!("rebalance: {error:?}"))?;
        if let Err(error) = response.outcome {
            bail!("rebalance was refused: {} ({:?})", error.msg, error.code());
        }
        // follow the plan's record the way the cluster tab does
        let deadline = Instant::now() + PLAN_TIMEOUT;
        let mut last = Vec::new();
        let mut leader = None;
        loop {
            let (lines, done) = crate::components::follow_once(shoal, op, Follow::Plan)
                .await
                .map_err(|error| eyre!(error))?;
            if lines != last {
                for line in &lines {
                    step(None, line);
                }
                last = lines;
                // and its pace, when a step moved and the cluster answers the figures
                // ([F52](../../../docs/src/features/cluster-stats.md))
                if let Some(line) = self.plan_pace(shoal, op, &mut leader).await {
                    step(None, &line);
                }
            }
            if done {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("the rebalance {op} was not done after {PLAN_TIMEOUT:?}; `status` follows it");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Send one operation the cluster tab's command line takes, and follow its record until done
    ///
    /// The same parser and records as the tab's query bar, without its preview: what is typed is
    /// sent, which is what a script needs. `status <op>` reads the record of an operation sent
    /// earlier and follows it the same way.
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    /// * `line` - The operation as the tab takes it, like `repair movie verify`
    /// * `timeout` - How long to follow the record before giving up on it
    ///
    /// # Errors
    ///
    /// When the line does not parse, the request is refused, or the record is not done in time.
    pub async fn admin<S>(
        &self,
        shoal: &Arc<Shoal<S>>,
        line: &str,
        timeout: Duration,
    ) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // the tab's own parser, so the two cannot disagree about what a line means
        let action = crate::cluster::ClusterAction::parse(line).map_err(|error| eyre!(error))?;
        // a status names an existing operation: find which kind of record answers by its id
        let (op, follow) = if let crate::cluster::ClusterAction::Status { op } = action {
            let mut found = None;
            for follow in [Follow::Plan, Follow::Repair, Follow::Backup, Follow::Restore, Follow::Move] {
                if crate::components::follow_once(shoal, op, follow).await.is_ok() {
                    found = Some(follow);
                    break;
                }
            }
            let follow = found.ok_or_else(|| eyre!("no record of {op} on this member"))?;
            (op, follow)
        } else {
            // a new operation, written against the topology version as the member serves it
            let op = Uuid::new_v4();
            let model = crate::cluster::poll(shoal).await.map_err(|error| eyre!(error))?;
            let (kind, follow) = action.request();
            step(None, &format!("sending {line:?} as {op}"));
            let response = shoal
                .admin(&AdminRequest {
                    op,
                    expected_version: model.version,
                    kind,
                })
                .await
                .map_err(|error| eyre!("{line}: {error:?}"))?;
            match response.outcome {
                Ok(outcome) => step(None, &format!("accepted: {outcome:?}")),
                Err(error) => {
                    return Err(eyre!("{line} was refused: {} ({:?})", error.msg, error.code()))
                }
            }
            (op, follow)
        };
        // follow the record, printing it whenever it changes, until it is done
        let deadline = Instant::now() + timeout;
        let mut last = Vec::new();
        loop {
            let (lines, done) = crate::components::follow_once(shoal, op, follow)
                .await
                .map_err(|error| eyre!(error))?;
            if lines != last {
                for line in &lines {
                    step(None, line);
                }
                last = lines;
            }
            if done {
                // done is every group finished, which includes a group that failed: say so, and
                // fail the command, so a script never reads a failed restore as a restored one
                // ([Resolved #154](../../../docs/src/appendix/resolved/admin-hides-failed-groups.md))
                let failed = failed_lines(&last);
                if !failed.is_empty() {
                    bail!("{op} finished with {} failed: {}", failed.len(), failed.join("; "));
                }
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("{op} was not done after {timeout:?}; `admin \"status {op}\"` follows it");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// A plan's progress as one line, from the leader's figures, if the cluster answers them
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    /// * `op` - The plan
    /// * `leader` - The leader's client from an earlier call, by its address
    async fn plan_pace<S>(
        &self,
        shoal: &Arc<Shoal<S>>,
        op: Uuid,
        leader: &mut Option<(String, Arc<Shoal<S>>)>,
    ) -> Option<String>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        // only a node that says it answers the figures is asked for them
        let model = crate::cluster::poll(shoal).await.ok()?;
        if !model.answers("stats") {
            return None;
        }
        let dial = |addr: String| async move {
            self.connect::<S>(&addr, Instant::now())
                .await
                .map_err(|error| error.to_string())
        };
        let figures = crate::cluster::stats::leader_stats(shoal, None, leader, dial)
            .await
            .ok()?;
        figures
            .view
            .plans
            .iter()
            .find(|plan| plan.op == op)
            .map(crate::cluster::stats::plan_line)
    }

    /// Print every member's figures and every plan's progress, as the leader holds them
    ///
    /// # Arguments
    ///
    /// * `table` - The table to narrow the figures to, if one
    /// * `watch` - Print again every so many seconds, if given
    /// * `json` - Print the answer as json rather than as lines
    ///
    /// # Errors
    ///
    /// When no node answers, or the cluster does not answer the figures.
    pub async fn stats<S>(
        &self,
        table: Option<&str>,
        watch: Option<u64>,
        json: bool,
    ) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let record = self.state.record()?;
        if record.nodes.is_empty() {
            bail!("{} has not been deployed", self.inventory.name);
        }
        // any member answers, and names the leader if it is not the one
        let shoal = self.any_member::<S>(&record).await?;
        let model = crate::cluster::poll(&shoal).await.map_err(|error| eyre!(error))?;
        if !model.answers("stats") {
            bail!(
                "the node reached does not answer the Stats read; it runs a build from before F52"
            );
        }
        let mut leader = None;
        loop {
            // the leader's answer, or the reached node's own with why
            let dial = |addr: String| async move {
                self.connect::<S>(&addr, Instant::now())
                    .await
                    .map_err(|error| error.to_string())
            };
            let figures = crate::cluster::stats::leader_stats(&shoal, table, &mut leader, dial)
                .await
                .map_err(|error| eyre!(error))?;
            // a watch redraws from the top of the screen
            if watch.is_some() && !json {
                print!("\x1b[2J\x1b[H");
            }
            if json {
                println!("{}", shoal::serde_json::to_string_pretty(&figures.view)?);
            } else {
                for line in figures.render_lines() {
                    println!("{line}");
                }
            }
            let Some(every) = watch else {
                return Ok(());
            };
            tokio::time::sleep(Duration::from_secs(every.max(1))).await;
        }
    }

    /// Connect to the first deployed node that answers
    ///
    /// # Arguments
    ///
    /// * `record` - The cluster record
    ///
    /// # Errors
    ///
    /// When no deployed node answers.
    pub async fn any_member<S>(&self, record: &ClusterRecord) -> color_eyre::Result<Arc<Shoal<S>>>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let mut errors = Vec::new();
        for (name, node) in &record.nodes {
            // each deployed node's client address, tried once
            let address: std::net::IpAddr = node.address.parse()?;
            let addr = super::inventory::socket(address, self.inventory.ports.client);
            match self.connect::<S>(&addr, Instant::now()).await {
                Ok(shoal) => return Ok(shoal),
                Err(error) => errors.push(format!("{name}: {error}")),
            }
        }
        Err(eyre!("no deployed node answered: {}", errors.join("; ")))
    }

    /// Print what the cluster looks like, and whether every unit is running
    ///
    /// # Errors
    ///
    /// When no node answers.
    pub async fn status<S>(&self) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
        for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
            rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let record = self.state.record()?;
        if record.nodes.is_empty() {
            bail!("{} has not been deployed", self.inventory.name);
        }
        // which name is which id, so the model's rows can be read
        for (name, node) in &record.nodes {
            println!("{name}: {} at {}", node.node, node.address);
        }
        // the units, as systemd sees them
        self.systemctl("is-active", None)?;
        // and the cluster, as a member sees it
        let shoal = self.any_member::<S>(&record).await?;
        let model = crate::cluster::poll(&shoal).await.map_err(|error| eyre!(error))?;
        for line in model.render_lines() {
            println!("{line}");
        }
        Ok(())
    }
}

/// Install a program pushed by `push_binary` over a node's program and delete the push
///
/// The program is installed beside its target and renamed over it, so a node running the
/// old one keeps its inode and a crash leaves one whole program or the other.
///
/// # Arguments
///
/// * `host` - The host
/// * `partial` - Where `push_binary` left the program
/// * `binary` - The node's program
/// * `user` - The user the node runs as, who owns it
///
/// # Errors
///
/// When the install or the rename fails.
pub(super) fn install_binary(
    host: &Host,
    partial: &str,
    binary: &str,
    user: &str,
) -> color_eyre::Result<()> {
    // beside the target first, then over it in one rename
    host.run(&format!(
        "set -e; sudo -n install -o {user} -g {user} -m 755 {partial} {binary}.new; \
         sudo -n mv -f {binary}.new {binary}; rm -f {partial}",
        user = quote(user),
        partial = quote(partial),
        binary = quote(binary),
    ))?;
    Ok(())
}

/// The sha256 of a local file as hex, which is what `sha256sum` prints
///
/// # Arguments
///
/// * `path` - The file
pub(super) fn digest(path: &Path) -> color_eyre::Result<String> {
    // read it whole: a server program is tens of megabytes
    let bytes = std::fs::read(path).wrap_err_with(|| format!("failed to read {}", path.display()))?;
    let hash = Sha256::digest(&bytes);
    Ok(hash.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Whether every one of these nodes is an up member and the control group has its voters
///
/// # Arguments
///
/// * `model` - The cluster as a member sees it
/// * `ids` - The nodes that have to be up
/// * `voters` - How many voters the control group has to have
#[must_use]
pub fn members_ready(model: &ClusterModel, ids: &[NodeId], voters: usize) -> bool {
    // every node named is a member and up
    let up = ids.iter().all(|id| {
        let id = id.to_string();
        model
            .members
            .iter()
            .any(|member| member.node == id && member.health.eq_ignore_ascii_case("up"))
    });
    up && model.voters >= voters
}

/// Wait until every one of these nodes is up and the control group has its voters
///
/// # Arguments
///
/// * `shoal` - The admin client
/// * `ids` - The nodes that have to be up
/// * `voters` - How many voters the control group has to have
pub(super) async fn wait_for_members<S>(
    shoal: &Arc<Shoal<S>>,
    ids: &[NodeId],
    voters: usize,
) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
{
    step(None, &format!("waiting for {} member{} up and {voters} voter{}", ids.len(),
        if ids.len() == 1 { "" } else { "s" }, if voters == 1 { "" } else { "s" }));
    let deadline = Instant::now() + UP_TIMEOUT;
    let mut last;
    loop {
        // a poll that fails is a node still settling, not a failure yet
        match crate::cluster::poll(shoal).await {
            Ok(model) if members_ready(&model, ids, voters) => return Ok(()),
            Ok(model) => {
                last = format!(
                    "{} up of {} members, {} voters",
                    model.members.iter().filter(|m| m.health.eq_ignore_ascii_case("up")).count(),
                    model.members.len(),
                    model.voters
                );
            }
            Err(error) => last = error,
        }
        if Instant::now() > deadline {
            bail!("the members were not up after {UP_TIMEOUT:?}: {last}; `logs <node>` says why");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Wait until the cluster admits a default write
///
/// # Arguments
///
/// * `shoal` - The admin client
async fn wait_for_writes<S>(shoal: &Arc<Shoal<S>>) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
{
    step(None, "waiting for default writes to be admitted");
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last;
    loop {
        match crate::cluster::poll(shoal).await {
            Ok(model) if model.default_writes == "admitted" => return Ok(()),
            Ok(model) => last = model.default_writes,
            Err(error) => last = error,
        }
        if Instant::now() > deadline {
            bail!("default writes were not admitted after {READY_TIMEOUT:?}: {last}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The control address of every placeable member, as the cluster committed it
///
/// # Arguments
///
/// * `shoal` - The admin client
async fn control_addresses<S>(shoal: &Arc<Shoal<S>>) -> color_eyre::Result<Vec<String>>
where
    S: QuerySupport + Send + Sync + 'static,
{
    // the raw frame, since the model keeps only the client address
    let response = shoal
        .admin(&AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::Members,
        })
        .await
        .map_err(|error| eyre!("members: {error:?}"))?;
    let frame = match response.outcome {
        Ok(AdminOutcome::Read(frame)) => frame,
        Ok(other) => return Err(eyre!("members answered {other:?}")),
        Err(error) => return Err(eyre!("members was refused: {} ({:?})", error.msg, error.code())),
    };
    Ok(seed_addresses(&frame))
}

/// The control addresses of the members a joiner may join through, out of a `Members` frame
///
/// # Arguments
///
/// * `frame` - The frame
#[must_use]
pub fn seed_addresses(frame: &Value) -> Vec<String> {
    frame["members"]
        .as_array()
        .into_iter()
        .flatten()
        // a member leaving or removed is not one to join through
        .filter(|member| member["phase"].as_str().unwrap_or("member") == "member")
        .filter_map(|member| member["record"]["control"].as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {

    /// A followed record's failed groups are read out of it, so the command fails with them (item 154)
    ///
    /// On the lab a restore whose group failed was reported "done" and the command succeeded; a
    /// read of the dataset afterwards found 66,191 movies missing
    /// ([Resolved #154](../../../docs/src/appendix/resolved/admin-hides-failed-groups.md)).
    #[test]
    fn a_followed_records_failed_groups_are_read_out() {
        let lines = vec![
            "following 1: done".to_string(),
            "  17 Done {\"Restored\":{\"records\":3}}".to_string(),
            "  15 Done {\"Failed\":{\"reason\":\"a peer reset the connection\"}}".to_string(),
        ];
        assert_eq!(
            super::failed_lines(&lines),
            vec!["15 Done {\"Failed\":{\"reason\":\"a peer reset the connection\"}}"]
        );
        // a record with no failure has none, whatever else it says
        assert!(super::failed_lines(&lines[..2]).is_empty());
    }
    use super::*;
    use crate::cluster::model::MemberRow;

    /// A member row that is up or not
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `health` - Its health
    fn row(node: NodeId, health: &str) -> MemberRow {
        MemberRow {
            node: node.to_string(),
            role: "voter".to_string(),
            health: health.to_string(),
            phase: "member".to_string(),
            incarnation: 1,
            grace_remaining_ms: None,
            weight: 1,
            free_bytes: None,
            held_bytes: None,
            wire_max: 5,
            client: String::new(),
        }
    }

    /// The members are ready only when every node named is up and the voters are there
    #[test]
    fn members_are_ready_when_every_node_is_up_and_voting() {
        let (a, b) = (NodeId::mint(), NodeId::mint());
        let mut model = ClusterModel {
            members: vec![row(a, "up"), row(b, "joining")],
            voters: 1,
            ..ClusterModel::default()
        };
        // one up node of a one voter group is enough for the bootstrapper alone
        assert!(members_ready(&model, &[a], 1));
        // a joining node is not up
        assert!(!members_ready(&model, &[a, b], 1));
        // an up node without its voter is not ready either
        model.members[1].health = "up".to_string();
        assert!(!members_ready(&model, &[a, b], 2));
        model.voters = 2;
        assert!(members_ready(&model, &[a, b], 2));
        // a node the cluster has never heard of is not up
        assert!(!members_ready(&model, &[a, b, NodeId::mint()], 2));
    }

    /// A joiner joins through every placeable member's committed control address
    #[test]
    fn seeds_are_the_placeable_members_control_addresses() {
        let frame = shoal::serde_json::json!({"members": [
            {"phase": "member", "record": {"control": "10.0.0.1:12002"}},
            {"record": {"control": "10.0.0.2:12002"}},
            {"phase": "leaving", "record": {"control": "10.0.0.3:12002"}},
            {"phase": "member", "record": {}},
        ]});
        assert_eq!(seed_addresses(&frame), vec!["10.0.0.1:12002", "10.0.0.2:12002"]);
    }
}
