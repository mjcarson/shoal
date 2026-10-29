//! A rolling upgrade of every deployed node's program, one node at a time
//!
//! Runbook [7](../../../docs/src/operations/runbooks.md#7-rolling-upgrade) done by a program
//! ([F55](../../../docs/src/features/cluster-upgrade.md)): judge the cluster healthy, then for
//! each node - the control leader last - push the inventory's program, run it once to prove
//! it starts on this cpu, keep the program it replaces as `<program>.prev`, restart the unit
//! and wait until the node is an up member again and its own groups have caught up. A node
//! that does not come back is swapped back onto its previous program and the upgrade stops.
//! Activating the new wire version is the rollback point and is only done when asked.
//!
//! Only the program changes: the configuration, the unit, the schema and every file under the
//! storage roots are left as they are.

use color_eyre::eyre::{bail, eyre};
use rkyv::Archive;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::traits::QuerySupport;
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;

use super::ops::{
    digest, install_binary, riding_elections, step, wait_for_members, Deployment, ELECTION_PAUSE,
    ELECTION_RIDE, POLL_INTERVAL, UP_TIMEOUT,
};
use super::remote::{quote, Host, SIGILL_STATUS};
use super::render::{Entry, Layout};
use super::state::{ClusterRecord, NodeRecord};
use crate::cluster::ClusterModel;

/// How many lines of a node's journal a failed upgrade prints
const JOURNAL_LINES: usize = 50;

/// How long to try reading a node's shards and groups before restarting it
///
/// Short, because a node that does not answer is one being repaired, and waiting for it
/// would only delay the repair.
const BEFORE_RESTART_POLL: std::time::Duration = std::time::Duration::from_secs(3);

/// What a node's host said about the program it runs
struct Installed {
    /// The user that owns the program, which is the user the node runs as
    user: String,
    /// The sha256 of the program the node runs
    digest: String,
    /// Whether a previous program is kept beside it
    prev: bool,
}

/// Whether the cluster is healthy enough to take one node down at a time
///
/// Every node the deployment recorded has to be an up member that is not leaving, default
/// writes have to be admitted, no set may be under its factor and no plan may be running,
/// since a restart on top of any of those can cost the quorum a set is served by.
///
/// The one exception is a node that is down and was named to be upgraded: that is a repair,
/// and replacing the program of a node that is already down cannot cost a quorum more than
/// its being down does. Without it a node crash-looping on a defect could not be given the
/// program that fixes it ([Resolved #136](../../../docs/src/appendix/resolved/upgrade-a-down-node.md)).
///
/// # Arguments
///
/// * `model` - The cluster as a member sees it
/// * `record` - What the deployment recorded
/// * `repairing` - The nodes named to be upgraded, which may be down
///
/// # Errors
///
/// Why the cluster is not ready, naming the node or the plan.
pub fn judge_health(
    model: &ClusterModel,
    record: &ClusterRecord,
    repairing: &[String],
) -> Result<(), String> {
    // every recorded node is an up member of the cluster
    for (name, node) in &record.nodes {
        let Some(member) = model.members.iter().find(|member| member.node == node.node) else {
            return Err(format!(
                "{name} ({}) is not a member the cluster knows",
                node.node
            ));
        };
        // a down node named to be upgraded is being repaired, and is let through
        let repair = repairing.iter().any(|named| named == name)
            && member.health.eq_ignore_ascii_case("down");
        if !member.health.eq_ignore_ascii_case("up") && !repair {
            return Err(format!("{name} is {}, not up", member.health));
        }
        if member.phase != "member" {
            return Err(format!("{name} is {}, not a member", member.phase));
        }
    }
    // writes are admitted, so one node down still leaves a write quorum
    if model.default_writes != "admitted" {
        return Err(format!("default writes are {}", model.default_writes));
    }
    // every set has its copies, so one node down still leaves a majority of each
    if model.under_replicated_sets > 0 {
        return Err(format!(
            "{} replica set{} under the factor",
            model.under_replicated_sets,
            if model.under_replicated_sets == 1 {
                " is"
            } else {
                "s are"
            }
        ));
    }
    // and no plan is moving data a restart would interrupt
    if let Some(plan) = model.plans.first() {
        return Err(format!(
            "the {} plan {} is {}; wait for it to finish",
            plan.kind, plan.op, plan.phase
        ));
    }
    Ok(())
}

/// The nodes to upgrade, in order, with the control leader last
///
/// # Arguments
///
/// * `record` - What the deployment recorded
/// * `only` - The nodes asked for, or none for every recorded node
/// * `leader` - The control leader's id, if one is known
///
/// # Errors
///
/// When a node asked for was never deployed.
pub fn upgrade_order(
    record: &ClusterRecord,
    only: &[String],
    leader: Option<&str>,
) -> Result<Vec<String>, String> {
    // a name that matches nothing is a typo
    if let Some(unknown) = only.iter().find(|name| !record.nodes.contains_key(*name)) {
        return Err(format!("no node named {unknown:?} has been deployed"));
    }
    // the nodes asked for, in the record's order
    let selected = record
        .nodes
        .iter()
        .filter(|(name, _)| only.is_empty() || only.contains(name));
    // the leader moves once, at the end, rather than at every step
    let (leaders, rest): (Vec<_>, Vec<_>) =
        selected.partition(|(_, node)| leader.is_some_and(|leader| node.node == leader));
    Ok(rest
        .into_iter()
        .chain(leaders)
        .map(|(name, _)| name.clone())
        .collect())
}

/// What a restarted node has to show before it counts as back
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackWhen {
    /// How many of its shards have to have reported their groups
    pub shards: u64,
    /// How many groups it has to host
    pub groups: u64,
}

/// Whether a node has caught up, as it sees itself
///
/// A node whose control thread answers while its shards are still starting has no groups to
/// lag behind, so writes admitted and a lag of zero say nothing on their own. Every shard it
/// had has to have reported, with at least the groups it had, and none of them still starting
/// ([Resolved #137](../../../docs/src/appendix/resolved/upgrade-waits-for-groups.md)).
///
/// # Arguments
///
/// * `model` - The cluster as the node itself sees it
/// * `back` - What the node has to show
#[must_use]
pub fn caught_up(model: &ClusterModel, back: BackWhen) -> bool {
    // every shard reported, with every group it had, and none of them is still starting
    let started = model.shards_reporting >= back.shards.max(1)
        && model.groups >= back.groups.max(1)
        && model.starting == 0;
    // it admits writes, none of its groups lags its leader, and none is installing a snapshot
    started && model.default_writes == "admitted" && model.lag_max == 0 && model.installing == 0
}

/// What to say before a rollback, once the cluster has committed a schema
///
/// A build that does not hold the committed schema stops itself at start, so a node rolled back
/// onto a program from before an extension stays down; the tool cannot read which schema a kept
/// program serves, so it names the committed one and leaves the judgment to the operator
/// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md)).
///
/// # Arguments
///
/// * `model` - The cluster as a member sees it
#[must_use]
pub fn rollback_past_extension(model: &ClusterModel) -> Option<String> {
    model.schema.map(|committed| {
        format!(
            "the cluster committed schema {committed:#018x}: a node rolled back onto a program \
             that does not hold it stops at its start, and an extension is never undone"
        )
    })
}

/// The wire version to activate: the lowest every member speaks, if it is above the activated one
///
/// # Arguments
///
/// * `model` - The cluster as a member sees it
#[must_use]
pub fn activation_target(model: &ClusterModel) -> Option<u8> {
    // the lowest version a member reports is the highest every member speaks
    let (lowest, _) = model.wire_range;
    if lowest > model.activated_wire {
        u8::try_from(lowest).ok()
    } else {
        None
    }
}

impl Deployment {
    /// Upgrade every deployed node's program, or roll it back, one node at a time
    ///
    /// # Arguments
    ///
    /// * `only` - The nodes to act on, or none for every deployed node
    /// * `force` - Restart a node even when it already runs this program
    /// * `activate` - Activate the wire version every member speaks once every node is done
    /// * `rollback` - Swap every node back onto the program it kept, rather than upgrading
    ///
    /// # Errors
    ///
    /// When the cluster is not healthy, a node does not come back, or the activation is refused.
    pub async fn upgrade<S>(
        &self,
        only: &[String],
        force: bool,
        activate: bool,
        rollback: bool,
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
        // only a cluster that was bootstrapped has anything to upgrade
        let record = self.state.record()?;
        if !record.initialized {
            bail!(
                "{} has not been bootstrapped from {}",
                self.inventory.name,
                self.state.dir().display()
            );
        }
        // and with the kind of peer lanes it was bootstrapped with (F103)
        record.judge_lanes(&self.inventory)?;
        // judge it healthy before anything is touched
        let shoal = self.any_member_as_admin::<S>(&record).await?;
        let model = crate::cluster::poll(&shoal)
            .await
            .map_err(|error| eyre!(error))?;
        judge_health(&model, &record, only).map_err(|why| {
            eyre!(
                "{} is not ready for a rolling restart: {why}",
                self.inventory.name
            )
        })?;
        // a rollback past a schema the cluster extended leaves every node of a build that does
        // not hold it stopped at its start, so say so before any host is touched (F93)
        if rollback {
            if let Some(warning) = rollback_past_extension(&model) {
                step(None, &warning);
            }
        }
        // the order, with the leader last
        let order =
            upgrade_order(&record, only, model.leader.as_deref()).map_err(|error| eyre!(error))?;
        // every node's program ready before any host is touched; a rollback swaps back what
        // each host kept and builds nothing
        if !rollback {
            let nodes = order
                .iter()
                .map(|name| self.inventory.node(name))
                .collect::<color_eyre::Result<Vec<_>>>()?;
            // the machines they run on, judged as a bootstrap judges them
            self.check_machines(&nodes)?;
            self.prepare(&nodes)?;
        }
        // every recorded node has to be back, with the voters bootstrap asked for
        let ids = record
            .nodes
            .values()
            .map(|node| node.node.parse::<Uuid>().map(NodeId))
            .collect::<Result<Vec<_>, _>>()?;
        let voters = (self.inventory.control_voters as usize).min(ids.len());
        let mut changed = 0;
        for name in &order {
            let node = &record.nodes[name];
            // one node at a time, and the next only once this one is back
            let restarted = if rollback {
                self.rollback_node::<S>(&record, name, node, &ids, voters)
                    .await?
            } else {
                self.upgrade_node::<S>(&record, name, node, &ids, voters, force)
                    .await?
            };
            if restarted {
                changed += 1;
            }
        }
        step(
            None,
            &format!(
                "{} of {} node{} {}",
                changed,
                order.len(),
                if order.len() == 1 { "" } else { "s" },
                if rollback { "rolled back" } else { "upgraded" }
            ),
        );
        // the wire versions as a member sees them now, from a fresh connection since the
        // node the first one reached may have restarted under it
        let shoal = self.any_member_as_admin::<S>(&record).await?;
        let model = crate::cluster::poll(&shoal)
            .await
            .map_err(|error| eyre!(error))?;
        step(
            None,
            &format!(
                "wire: activated {}, members speak {} to {}",
                model.activated_wire, model.wire_range.0, model.wire_range.1
            ),
        );
        // a rollback never activates, and an upgrade only when asked
        if rollback {
            return Ok(());
        }
        match (activation_target(&model), activate) {
            (Some(wire), true) => self.activate(&shoal, model.version, wire).await?,
            (Some(wire), false) => {
                step(
                    None,
                    &format!(
                        "every member speaks wire {wire}; `shoaladm upgrade --activate` activates it, \
                         after which no node can roll back below it"
                    ),
                );
            }
            (None, _) => {
                if activate {
                    step(None, "nothing to activate: no version every member speaks is above the activated one");
                }
            }
        }
        // and the schema, when this build's appends to the one the cluster committed and every
        // member runs it: an extension is the same boundary as an activation, so it is asked
        // for by the same flag (F93)
        let model = crate::cluster::poll(&shoal)
            .await
            .map_err(|error| eyre!(error))?;
        match (model.extension_to(S::SCHEMA_ID), activate) {
            (Some(schema), true) => self.extend_schema(&shoal, model.version, schema).await,
            (Some(schema), false) => {
                step(
                    None,
                    &format!(
                        "every member runs schema {schema:#018x}, which appends to the committed \
                         one; `shoaladm upgrade --activate` extends the cluster to it, after which \
                         no node can roll back below it"
                    ),
                );
                Ok(())
            }
            (None, _) => Ok(()),
        }
    }

    /// Extend the cluster's committed schema to the one this build serves
    ///
    /// The pools and bindings sent are the inventory's, so a bucket the schema appends is bound
    /// to the pool the inventory names ([F93](../../../docs/src/features/operations-and-the-real-cluster.md)).
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    /// * `version` - The topology version the request is judged against
    /// * `schema` - The schema to extend to, this build's
    pub(super) async fn extend_schema<S>(
        &self,
        shoal: &std::sync::Arc<shoal::Shoal<S>>,
        version: u64,
        schema: u64,
    ) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
    {
        let op = Uuid::new_v4();
        step(None, &format!("extending the schema to {schema:#018x} as {op}"));
        // the inventory's pools and bindings, which keep every committed one as it is
        let pools = shoal::shared::pools::PoolsConf {
            pools: self.inventory.pools.clone(),
            buckets: self.inventory.buckets.clone(),
        };
        // asked again while the control group elects, as it may once the leader's node restarts
        // (item 337); a later try reads the version again, which the election may have moved
        let mut first = Some(version);
        let response = riding_elections(ELECTION_RIDE, ELECTION_PAUSE, || {
            let given = first.take();
            let pools = pools.clone();
            async move {
                let version = match given {
                    Some(version) => version,
                    None => crate::cluster::poll(shoal).await.map_err(|error| eyre!(error))?.version,
                };
                shoal
                    .admin(&AdminRequest {
                        op,
                        expected_version: version,
                        kind: AdminKind::ExtendSchema {
                            schema,
                            pools: Some(pools),
                        },
                    })
                    .await
                    .map_err(|error| eyre!("extend schema: {error:?}"))
            }
        })
        .await?;
        match response.outcome {
            Ok(AdminOutcome::Applied { .. } | AdminOutcome::Repeated { .. }) => {
                step(
                    None,
                    &format!("schema {schema:#018x} is committed; no node rolls back below it"),
                );
                Ok(())
            }
            Ok(other) => Err(eyre!("extend schema answered {other:?}")),
            Err(error) => Err(eyre!(
                "the schema extension was refused: {} ({:?})",
                error.msg,
                error.code()
            )),
        }
    }

    /// Activate a wire version
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    /// * `version` - The topology version the request is judged against
    /// * `wire` - The version to activate
    pub(super) async fn activate<S>(
        &self,
        shoal: &std::sync::Arc<shoal::Shoal<S>>,
        version: u64,
        wire: u8,
    ) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
    {
        let op = Uuid::new_v4();
        step(None, &format!("activating wire {wire} as {op}"));
        // asked again while the control group elects, as it may once the leader's node restarts
        // (item 337); a later try reads the version again, which the election may have moved
        let mut first = Some(version);
        let response = riding_elections(ELECTION_RIDE, ELECTION_PAUSE, || {
            let given = first.take();
            async move {
                let version = match given {
                    Some(version) => version,
                    None => crate::cluster::poll(shoal).await.map_err(|error| eyre!(error))?.version,
                };
                shoal
                    .admin(&AdminRequest {
                        op,
                        expected_version: version,
                        kind: AdminKind::Activate { wire },
                    })
                    .await
                    .map_err(|error| eyre!("activate: {error:?}"))
            }
        })
        .await?;
        match response.outcome {
            Ok(AdminOutcome::Applied { .. } | AdminOutcome::Repeated { .. }) => {
                step(
                    None,
                    &format!("wire {wire} is activated; no node rolls back below it"),
                );
                Ok(())
            }
            Ok(other) => Err(eyre!("activate answered {other:?}")),
            Err(error) => Err(eyre!(
                "activate was refused: {} ({:?})",
                error.msg,
                error.code()
            )),
        }
    }

    /// Read what a node's host runs: its program's owner, its digest, and whether one is kept
    ///
    /// # Arguments
    ///
    /// * `host` - The host
    /// * `binary` - The node's program
    fn installed(&self, host: &Host, binary: &str) -> color_eyre::Result<Installed> {
        // one round trip that reports everything as key=value lines
        let output = host.run(&format!(
            "set -e; echo user=$(sudo -n stat -c %U {binary}); \
             echo digest=$(sudo -n sha256sum {binary} | cut -d' ' -f1); \
             if sudo -n test -e {prev}; then echo prev=yes; else echo prev=no; fi",
            binary = quote(binary),
            prev = quote(&format!("{binary}.prev")),
        ))?;
        let facts: HashMap<&str, &str> = output
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect();
        let fact = |key: &str| facts.get(key).copied().unwrap_or_default().to_string();
        Ok(Installed {
            user: fact("user"),
            digest: fact("digest"),
            prev: fact("prev") == "yes",
        })
    }

    /// Upgrade one node to the inventory's program
    ///
    /// # Arguments
    ///
    /// * `record` - What the deployment recorded
    /// * `name` - The node's inventory name
    /// * `node` - Its record
    /// * `ids` - Every recorded node, which all have to be up afterwards
    /// * `voters` - How many voters the control group has to have afterwards
    /// * `force` - Restart it even when it already runs this program
    ///
    /// Returns whether the node was restarted.
    ///
    /// # Errors
    ///
    /// When the program does not start on this host, or the node does not come back on it.
    async fn upgrade_node<S>(
        &self,
        record: &ClusterRecord,
        name: &str,
        node: &NodeRecord,
        ids: &[NodeId],
        voters: usize,
        force: bool,
    ) -> color_eyre::Result<bool>
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
        let host = Host {
            target: node.target.clone(),
        };
        let binary = self.layout_binary(name)?;
        let program = self.program(name)?;
        // a node already on this program is done, which is what lets a rerun resume
        let installed = self.installed(&host, &binary)?;
        let local = digest(&program)?;
        let same = installed.digest == local;
        if same && !force {
            step(Some(name), "already runs this program");
            return Ok(false);
        }
        // the program beside the node's, owned by the user it runs as
        step(Some(name), &format!("pushing {}", program.display()));
        let partial = self.push_binary(&host, name)?;
        let candidate = format!("{binary}.candidate");
        install_binary(&host, &partial, &candidate, &installed.user)?;
        // run once, so a build for another cpu is refused before anything the node runs changes
        let output = host.output(
            &format!(
                "sudo -n -u {} {} --version",
                quote(&installed.user),
                quote(&candidate)
            ),
            None,
        )?;
        if output.status != Some(0) {
            // nothing was swapped, so the candidate is all there is to clean up
            host.run(&format!("sudo -n rm -f {}", quote(&candidate)))?;
            if output.status == Some(SIGILL_STATUS) {
                bail!(
                    "{} died of an illegal instruction on {name}: it was built for another cpu. {}. \
                     no node was changed by this step",
                    program.display(),
                    super::ops::cpu_advice(&self.inventory)
                );
            }
            bail!(
                "{} --version failed on {name} with status {:?}: {}",
                program.display(),
                output.status,
                output.stderr.trim()
            );
        }
        // its file the inventory's pools and bindings, read, judged and written beside it before
        // anything the node runs changes, so a failure here leaves the node as it was (item 268)
        let conf = self.layout_conf(name);
        let carried = match self.carry_object_policy(&host, name) {
            Ok(carried) => carried,
            Err(error) => {
                // nothing was swapped, so the candidates are all there is to clean up
                let _ = host.run(&format!(
                    "sudo -n rm -f {} {}",
                    quote(&candidate),
                    quote(&format!("{conf}.candidate"))
                ));
                return Err(error);
            }
        };
        // keep the program it replaces and the file it ran on, unless it is this program, then
        // move the candidates in together
        step(Some(name), "installing it");
        host.run(&install_script(&binary, &candidate, &conf, !same, carried.is_some()))?;
        // restart it, and put the previous program back if it does not come back
        let Err(error) = self
            .restart_and_wait::<S>(record, name, node, ids, voters)
            .await
        else {
            return Ok(true);
        };
        // the file it had goes back with the program it had: a swap takes the pair back when this
        // upgrade kept both, and a file changed beside the program it already ran is put back here
        if same {
            if let Some((previous, owner)) = &carried {
                step(Some(name), "putting back the file it ran on");
                if let Err(write_error) = host.write(&conf, previous.as_bytes(), 0o600, Some(owner)) {
                    step(Some(name), &format!("the file it ran on could not be put back: {write_error}"));
                }
            }
        }
        // a forced restart onto the program it already ran has nothing to go back to
        if same && !installed.prev {
            bail!("{name} did not come back: {error}{}", self.journal(&host, name));
        }
        Err(self
            .revert::<S>(record, name, node, ids, voters, &error)
            .await)
    }

    /// Carry the inventory's pools and bindings into a candidate beside a node's file
    ///
    /// A build that appends a bucket starts only with the bucket bound in its file, and the build
    /// before it refuses a binding of a bucket its schema does not declare, so the binding is
    /// written with the program and not by a `reconfigure` before it
    /// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md)). Only
    /// `storage.pools` and `storage.buckets` are written: everything else in the file stays as it
    /// was deployed, and only the bootstrapper's copy was ever committed. A cluster extends its
    /// committed schema, and binds the new bucket, at `--activate`. The file is written as
    /// `<file>.candidate`, which the install script moves in with the program
    /// ([Resolved #267, 268](../../../docs/src/appendix/resolved/upgrade-keeps-the-file.md)).
    ///
    /// Returns what the file held before and its owner, when a candidate was written.
    ///
    /// # Arguments
    ///
    /// * `host` - The node's host
    /// * `name` - The node's inventory name
    ///
    /// # Errors
    ///
    /// When the file cannot be read, parsed or written.
    fn carry_object_policy(
        &self,
        host: &Host,
        name: &str,
    ) -> color_eyre::Result<Option<(String, String)>> {
        let path = self.layout_conf(name);
        // the file the node runs on now, with the inventory's pools and bindings in it
        let current = host.run(&format!("sudo -n cat {}", quote(&path)))?;
        let Some(written) = with_object_policy(&current, &self.inventory.pools, &self.inventory.buckets)
            .map_err(|error| eyre!("{name}'s {path}: {error}"))?
        else {
            return Ok(None);
        };
        // written whole, owned as it was
        let owner = host
            .run(&format!("sudo -n stat -c %U {}", quote(&path)))?
            .trim()
            .to_string();
        step(Some(name), &format!("writing the inventory's pools and bindings beside {path}"));
        host.write(&format!("{path}.candidate"), written.as_bytes(), 0o600, Some(&owner))?;
        Ok(Some((current, owner)))
    }

    /// Where a node's file is on its host
    ///
    /// # Arguments
    ///
    /// * `name` - The node's inventory name
    fn layout_conf(&self, name: &str) -> String {
        Layout {
            dir: self.inventory.remote_dir_of(name),
        }
        .conf()
    }

    /// Render every deployed node's `shoal.yml` again from the inventory, one node at a time
    ///
    /// `upgrade` swaps a node's program and never its configuration, so a change to what the
    /// inventory says, or to what the renderer writes, reached no deployed node: the lab's
    /// nodes had `node_memory` added by hand, and O61's commit delay set by hand on europa. This
    /// renders each node's file as a bootstrap would, keeping the entry it was deployed with,
    /// and restarts only the nodes whose file changed, the control leader last
    /// ([O61](../../../docs/src/appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)).
    ///
    /// # Arguments
    ///
    /// * `only` - The nodes to act on, or none for every deployed node
    /// * `force` - Restart a node even when its file did not change
    ///
    /// # Errors
    ///
    /// When the cluster is not healthy, or a node does not come back on its new file; that
    /// node is put back on the file it had.
    pub async fn reconfigure<S>(&self, only: &[String], force: bool) -> color_eyre::Result<()>
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
        // only a cluster that was bootstrapped has files to render again
        let record = self.state.record()?;
        if !record.initialized {
            bail!(
                "{} has not been bootstrapped from {}",
                self.inventory.name,
                self.state.dir().display()
            );
        }
        // and with the kind of peer lanes it was bootstrapped with (F103)
        record.judge_lanes(&self.inventory)?;
        // an inventory that does not validate renders nothing
        self.inventory.validate()?;
        // judged healthy before anything is touched, and the leader last, as an upgrade is
        let shoal = self.any_member_as_admin::<S>(&record).await?;
        let model = crate::cluster::poll(&shoal)
            .await
            .map_err(|error| eyre!(error))?;
        judge_health(&model, &record, only).map_err(|why| {
            eyre!(
                "{} is not ready for a rolling restart: {why}",
                self.inventory.name
            )
        })?;
        let order =
            upgrade_order(&record, only, model.leader.as_deref()).map_err(|error| eyre!(error))?;
        let ids = record
            .nodes
            .values()
            .map(|node| node.node.parse::<Uuid>().map(NodeId))
            .collect::<Result<Vec<_>, _>>()?;
        let voters = (self.inventory.control_voters as usize).min(ids.len());
        // the machines the files are rendered for, judged as a bootstrap judges them, since a
        // reconfigured node may name other cpus than it ran on
        let nodes = order
            .iter()
            .map(|name| self.inventory.node(name))
            .collect::<color_eyre::Result<Vec<_>>>()?;
        self.check_machines(&nodes)?;
        // the credential every file is rendered with
        let passwords = self.passwords()?;
        let mut changed = 0;
        for name in &order {
            let node = &record.nodes[name];
            if self
                .reconfigure_node::<S>(&record, name, node, &ids, voters, &passwords, force)
                .await?
            {
                changed += 1;
            }
        }
        step(
            None,
            &format!(
                "{} of {} node{} reconfigured",
                changed,
                order.len(),
                if order.len() == 1 { "" } else { "s" }
            ),
        );
        Ok(())
    }

    /// Render one node's file again, and restart it on the new one if it changed
    ///
    /// # Arguments
    ///
    /// * `record` - What the deployment recorded
    /// * `name` - The node's inventory name
    /// * `node` - Its record
    /// * `ids` - Every recorded node, which all have to be up afterwards
    /// * `voters` - How many voters the control group has to have afterwards
    /// * `passwords` - The admin's and every user's passwords the file's credentials are derived from
    /// * `force` - Restart it even when its file did not change
    ///
    /// Returns whether the node was restarted.
    ///
    /// # Errors
    ///
    /// When its file cannot be read or written, or it does not come back on the new one.
    #[allow(clippy::too_many_arguments)]
    async fn reconfigure_node<S>(
        &self,
        record: &ClusterRecord,
        name: &str,
        node: &NodeRecord,
        ids: &[NodeId],
        voters: usize,
        passwords: &super::render::Passwords,
        force: bool,
    ) -> color_eyre::Result<bool>
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
        let host = Host {
            target: node.target.clone(),
        };
        let layout = Layout {
            dir: self.inventory.remote_dir_of(name),
        };
        let path = layout.conf();
        // the file the node runs on now, and who owns it
        let current = host.run(&format!("sudo -n cat {}", quote(&path)))?;
        let owner = host
            .run(&format!("sudo -n stat -c %U {}", quote(&path)))?
            .trim()
            .to_string();
        // the entry it was deployed with, kept: a node that bootstrapped says so, a joiner
        // keeps the seeds it was given
        let parsed: serde_yaml::Value = serde_yaml::from_str(&current)
            .map_err(|error| eyre!("{name}'s {path} is not YAML: {error}"))?;
        let bootstrap = parsed["cluster"]["bootstrap"].as_bool().unwrap_or(false);
        let entry = if bootstrap {
            Entry::Bootstrap
        } else {
            let seeds = parsed["cluster"]["seeds"]
                .as_sequence()
                .map(|seeds| {
                    seeds
                        .iter()
                        .filter_map(|seed| seed.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            Entry::Join(seeds)
        };
        // rendered as the inventory says now
        let resolved = self.inventory.node(name)?;
        let rendered = super::render::render(&self.inventory, &resolved, &entry, passwords)?;
        // the credential's salt is new every render, so the two are judged without it
        if !force && same_but_credentials(&current, &rendered)? {
            step(Some(name), "its file is already what the inventory renders");
            return Ok(false);
        }
        step(Some(name), &format!("writing its new {path}"));
        host.write(&path, rendered.as_bytes(), 0o600, Some(&owner))?;
        // restarted on it, and put back on the old file if it does not come back
        let Err(error) = self
            .restart_and_wait::<S>(record, name, node, ids, voters)
            .await
        else {
            return Ok(true);
        };
        step(
            Some(name),
            "it did not come back on its new file; putting the old one back",
        );
        host.write(&path, current.as_bytes(), 0o600, Some(&owner))?;
        let restored = self
            .restart_and_wait::<S>(record, name, node, ids, voters)
            .await;
        match restored {
            Ok(()) => Err(eyre!(
                "{name} did not come back on its new file and is back on its old one: {error}"
            )),
            Err(again) => Err(eyre!(
                "{name} did not come back on its new file ({error}), nor on its old one ({again}){}",
                self.journal(&host, name)
            )),
        }
    }

    /// Roll one node back onto the program it kept
    ///
    /// # Arguments
    ///
    /// * `record` - What the deployment recorded
    /// * `name` - The node's inventory name
    /// * `node` - Its record
    /// * `ids` - Every recorded node, which all have to be up afterwards
    /// * `voters` - How many voters the control group has to have afterwards
    ///
    /// Returns whether the node was restarted.
    ///
    /// # Errors
    ///
    /// When the node kept no program, or does not come back on it.
    async fn rollback_node<S>(
        &self,
        record: &ClusterRecord,
        name: &str,
        node: &NodeRecord,
        ids: &[NodeId],
        voters: usize,
    ) -> color_eyre::Result<bool>
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
        let host = Host {
            target: node.target.clone(),
        };
        let binary = self.layout_binary(name)?;
        // a node that kept nothing has nothing to roll back to
        let installed = self.installed(&host, &binary)?;
        if !installed.prev {
            bail!("{name} kept no previous program at {binary}.prev; it was never upgraded");
        }
        // swap the two, and the file it ran on with them, so the rollback can itself be undone
        step(Some(name), "swapping its previous program back in");
        swap(&host, &binary, &self.layout_conf(name))?;
        let Err(error) = self
            .restart_and_wait::<S>(record, name, node, ids, voters)
            .await
        else {
            return Ok(true);
        };
        Err(self
            .revert::<S>(record, name, node, ids, voters, &error)
            .await)
    }

    /// Swap a node that did not come back onto the program it ran before, and say what happened
    ///
    /// # Arguments
    ///
    /// * `record` - What the deployment recorded
    /// * `name` - The node's inventory name
    /// * `node` - Its record
    /// * `ids` - Every recorded node, which all have to be up afterwards
    /// * `voters` - How many voters the control group has to have afterwards
    /// * `error` - Why it did not come back
    async fn revert<S>(
        &self,
        record: &ClusterRecord,
        name: &str,
        node: &NodeRecord,
        ids: &[NodeId],
        voters: usize,
        error: &color_eyre::Report,
    ) -> color_eyre::Report
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
        let host = Host {
            target: node.target.clone(),
        };
        // the journal of the run that failed, before the revert's own run adds to it
        let journal = self.journal(&host, name);
        step(
            Some(name),
            &format!("did not come back ({error}); swapping its previous program back in"),
        );
        // swap back and wait for it the same way
        let reverted = match self.layout_binary(name) {
            Ok(binary) => match swap(&host, &binary, &self.layout_conf(name)) {
                Ok(()) => {
                    self.restart_and_wait::<S>(record, name, node, ids, voters)
                        .await
                }
                Err(swap_error) => Err(swap_error),
            },
            Err(layout_error) => Err(layout_error),
        };
        let outcome = match reverted {
            Ok(()) => "it is back on the program it ran before".to_string(),
            Err(revert_error) => {
                format!(
                    "and it did not come back on the program it ran before either: {revert_error}"
                )
            }
        };
        eyre!("{name} did not come back: {error}; {outcome}. the upgrade stopped here{journal}")
    }

    /// Restart a node's unit and wait until it is back: active, an up member, and caught up
    ///
    /// # Arguments
    ///
    /// * `record` - What the deployment recorded
    /// * `name` - The node's inventory name
    /// * `node` - Its record
    /// * `ids` - Every recorded node, which all have to be up afterwards
    /// * `voters` - How many voters the control group has to have afterwards
    ///
    /// # Errors
    ///
    /// When the unit fails, or the node is not back by the deadline.
    async fn restart_and_wait<S>(
        &self,
        record: &ClusterRecord,
        name: &str,
        node: &NodeRecord,
        ids: &[NodeId],
        voters: usize,
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
        let host = Host {
            target: node.target.clone(),
        };
        let unit = quote(&self.inventory.unit_name_of(name));
        let address: std::net::IpAddr = node.address.parse()?;
        let addr = super::inventory::socket(address, self.inventory.ports.client);
        // what the node has to show to count as back: the shards and groups it had before this
        // restart if it answers now, else the cores it is configured with. a node being
        // repaired is down and answers nothing
        let back = match self
            .connect_as_admin::<S>(&addr, Instant::now() + BEFORE_RESTART_POLL)
            .await
        {
            Ok(before) => match crate::cluster::poll(&before).await {
                Ok(model) => BackWhen {
                    shards: model.shards_reporting,
                    groups: model.groups,
                },
                Err(_) => self.back_by_resources(name),
            },
            Err(_) => self.back_by_resources(name),
        };
        // the unit stops the old process with SIGTERM and starts the program now installed
        step(
            Some(name),
            &format!("restarting {}", self.inventory.unit_name_of(name)),
        );
        host.run(&format!("sudo -n systemctl restart {unit}"))?;
        let deadline = Instant::now() + UP_TIMEOUT;
        // the unit is running, and a failed one is failed now rather than at the deadline
        loop {
            let output = host.output(&format!("sudo -n systemctl is-active {unit}"), None)?;
            match output.stdout.trim() {
                "active" => break,
                "failed" => return Err(eyre!("its unit failed")),
                state if Instant::now() > deadline => {
                    return Err(eyre!("its unit was {state} after {UP_TIMEOUT:?}"));
                }
                _ => tokio::time::sleep(POLL_INTERVAL).await,
            }
        }
        // the cluster sees it up again beside every other node
        let member = self.any_member_as_admin::<S>(record).await?;
        wait_for_members(&member, ids, voters).await?;
        // and the node itself answers, admits writes, and has caught its groups up
        step(Some(name), "waiting for it to catch up");
        let itself = self.connect_as_admin::<S>(&addr, deadline).await?;
        let mut last;
        loop {
            match crate::cluster::poll(&itself).await {
                Ok(model) if caught_up(&model, back) => break,
                Ok(model) => {
                    last = format!(
                        "{} of {} shards reported, {} of {} groups, {} starting, writes {}, lag {}, {} installing",
                        model.shards_reporting,
                        back.shards,
                        model.groups,
                        back.groups,
                        model.starting,
                        model.default_writes,
                        model.lag_max,
                        model.installing
                    );
                }
                Err(error) => last = error,
            }
            if Instant::now() > deadline {
                bail!("it had not caught up after {UP_TIMEOUT:?}: {last}");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        step(Some(name), "back and caught up");
        Ok(())
    }

    /// What a node that did not answer before its restart has to show to count as back
    ///
    /// # Arguments
    ///
    /// * `name` - The node's inventory name
    fn back_by_resources(&self, name: &str) -> BackWhen {
        // a shard per configured core, or one when the node takes every core it is given
        let shards = self
            .inventory
            .node(name)
            .ok()
            .and_then(|node| node.resources.cores)
            .map_or(1, |cores| cores as u64);
        BackWhen { shards, groups: 1 }
    }

    /// Where a node's program is on its host, under its own directory when it shares its machine
    ///
    /// # Arguments
    ///
    /// * `name` - The node's inventory name
    fn layout_binary(&self, name: &str) -> color_eyre::Result<String> {
        let layout = Layout {
            dir: self.inventory.remote_dir_of(name),
        };
        Ok(layout.binary(&self.server_name()?))
    }

    /// The end of a node's journal, as a suffix for an error, or nothing if it cannot be read
    ///
    /// # Arguments
    ///
    /// * `host` - The host
    /// * `name` - The node's inventory name, whose unit the journal is read for
    fn journal(&self, host: &Host, name: &str) -> String {
        // best effort: an error about the node is still worth more than one about its journal
        match host.run(&format!(
            "sudo -n journalctl --no-pager -u {} -n {JOURNAL_LINES}",
            quote(&self.inventory.unit_name_of(name))
        )) {
            Ok(lines) => format!(". its journal ends:\n{lines}"),
            Err(_) => String::new(),
        }
    }
}

/// Swap a node's program with the one it kept
///
/// # Arguments
///
/// * `host` - The host
/// * `binary` - The node's program
///
/// # Errors
///
/// When a rename fails.
fn swap(host: &Host, binary: &str, conf: &str) -> color_eyre::Result<()> {
    // three renames through a spare name; the running process keeps its own inode throughout
    host.run(&swap_script(binary, conf))?;
    Ok(())
}

/// The script that swaps a node's program with the one it kept, and its file with the one kept beside it
///
/// A program is swapped with the file it ran on, since a build that does not declare a bucket
/// refuses a file that binds it: an upgrade keeps the file beside the program it keeps, and a
/// rollback or a revert takes both back. A node upgraded before it kept a file swaps its program
/// alone ([Resolved #267, 268](../../../docs/src/appendix/resolved/upgrade-keeps-the-file.md)).
///
/// # Arguments
///
/// * `binary` - The node's program
/// * `conf` - The node's file
fn swap_script(binary: &str, conf: &str) -> String {
    let swap_pair = |current: &str| {
        format!(
            "sudo -n mv -f {current} {spare}; sudo -n mv -f {prev} {current}; sudo -n mv -f {spare} {prev}",
            current = quote(current),
            prev = quote(&format!("{current}.prev")),
            spare = quote(&format!("{current}.swap")),
        )
    };
    // the program, then the file when one was kept beside it
    format!(
        "set -e; {program}; if sudo -n test -e {conf_prev}; then {file}; fi",
        program = swap_pair(binary),
        conf_prev = quote(&format!("{conf}.prev")),
        file = swap_pair(conf),
    )
}

/// The script that puts a node's new program in place, its file with it
///
/// The program and the file it runs on are kept beside them first, as a pair a rollback swaps
/// back, unless the node is given the program it runs; a carried file written beside the node's
/// as a candidate is moved in with the program. Every read and write that can fail is made before
/// this runs, so nothing the node runs changes until both are in hand
/// ([Resolved #267, 268](../../../docs/src/appendix/resolved/upgrade-keeps-the-file.md)).
///
/// # Arguments
///
/// * `binary` - The node's program
/// * `candidate` - The new program, beside it
/// * `conf` - The node's file
/// * `keep` - Whether to keep the program and the file it replaces
/// * `carried` - Whether a carried file is waiting beside the node's as a candidate
fn install_script(binary: &str, candidate: &str, conf: &str, keep: bool, carried: bool) -> String {
    // the pair kept, the program moved in, then the file
    let keep = if keep {
        format!(
            "sudo -n cp -p {binary} {binary_prev}; sudo -n cp -p {conf} {conf_prev}; ",
            binary = quote(binary),
            binary_prev = quote(&format!("{binary}.prev")),
            conf = quote(conf),
            conf_prev = quote(&format!("{conf}.prev")),
        )
    } else {
        String::new()
    };
    let file = if carried {
        format!(
            "; sudo -n mv -f {candidate} {conf}",
            candidate = quote(&format!("{conf}.candidate")),
            conf = quote(conf),
        )
    } else {
        String::new()
    };
    format!(
        "set -e; {keep}sudo -n mv -f {candidate} {binary}{file}",
        candidate = quote(candidate),
        binary = quote(binary),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::model::{MemberRow, PlanRow};

    /// Run a script built for a host here, in a directory, with a `sudo` that runs what it is given
    ///
    /// # Arguments
    ///
    /// * `dir` - Where the script runs, and where its `sudo` is put
    /// * `script` - The script
    fn run_here(dir: &std::path::Path, script: &str) -> bool {
        // a sudo that drops its own options and runs the command it was given
        let shim = dir.join("bin");
        std::fs::create_dir_all(&shim).expect("a directory");
        let sudo = shim.join("sudo");
        std::fs::write(
            &sudo,
            "#!/bin/sh\nwhile [ $# -gt 0 ]; do case \"$1\" in -u) shift 2;; -*) shift;; *) break;; esac; done\nexec \"$@\"\n",
        )
        .expect("a shim");
        std::fs::set_permissions(&sudo, std::os::unix::fs::PermissionsExt::from_mode(0o755)).expect("executable");
        let path = format!("{}:{}", shim.display(), std::env::var("PATH").unwrap_or_default());
        std::process::Command::new("sh")
            .arg("-c")
            .arg(script)
            .current_dir(dir)
            .env("PATH", path)
            .status()
            .expect("sh")
            .success()
    }

    /// An upgrade keeps the program and the file it ran on as a pair and moves its new program and
    /// carried file in together, and a rollback of it puts the node back as it was (items 267, 268)
    #[test]
    fn an_upgrade_and_its_rollback_move_the_program_and_its_file_together() {
        let dir = std::env::temp_dir().join(format!("shoaladm-install-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("a directory");
        let at = |name: &str| dir.join(name).display().to_string();
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).expect("reads");
        // the node as it runs, and the candidates an upgrade wrote beside it
        for (name, text) in [
            ("node", "old"),
            ("shoal.yml", "old file"),
            ("node.candidate", "new"),
            ("shoal.yml.candidate", "carried file"),
        ] {
            std::fs::write(dir.join(name), text).expect("writes");
        }
        assert!(run_here(&dir, &install_script(&at("node"), &at("node.candidate"), &at("shoal.yml"), true, true)));
        assert_eq!((read("node"), read("node.prev")), ("new".to_string(), "old".to_string()));
        assert_eq!((read("shoal.yml"), read("shoal.yml.prev")), ("carried file".to_string(), "old file".to_string()));
        assert!(!dir.join("shoal.yml.candidate").exists() && !dir.join("node.candidate").exists());
        // its rollback: the old program on the file it ran on
        assert!(run_here(&dir, &swap_script(&at("node"), &at("shoal.yml"))));
        assert_eq!((read("node"), read("shoal.yml")), ("old".to_string(), "old file".to_string()));
        // a forced restart onto the program it runs keeps nothing, and moves a carried file in
        std::fs::write(dir.join("node.candidate"), "old").expect("writes");
        std::fs::write(dir.join("shoal.yml.candidate"), "carried again").expect("writes");
        assert!(run_here(&dir, &install_script(&at("node"), &at("node.candidate"), &at("shoal.yml"), false, true)));
        assert_eq!((read("node.prev"), read("shoal.yml.prev")), ("new".to_string(), "carried file".to_string()));
        assert_eq!(read("shoal.yml"), "carried again");
        // and an upgrade that carries nothing keeps the file it ran on all the same
        std::fs::write(dir.join("node.candidate"), "newer").expect("writes");
        assert!(run_here(&dir, &install_script(&at("node"), &at("node.candidate"), &at("shoal.yml"), true, false)));
        assert_eq!((read("node"), read("shoal.yml"), read("shoal.yml.prev")), ("newer".to_string(), "carried again".to_string(), "carried again".to_string()));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A rollback's swap takes the program and the file it ran on back together, and a node that
    /// kept no file beside its program swaps its program alone (item 267)
    #[test]
    fn a_swap_takes_the_program_and_its_file_back_together() {
        let dir = std::env::temp_dir().join(format!("shoaladm-swap-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("a directory");
        let at = |name: &str| dir.join(name).display().to_string();
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).expect("reads");
        // the new program and its file, and the ones kept beside them
        for (name, text) in [("node", "new"), ("node.prev", "old"), ("shoal.yml", "new file"), ("shoal.yml.prev", "old file")] {
            std::fs::write(dir.join(name), text).expect("writes");
        }
        assert!(run_here(&dir, &swap_script(&at("node"), &at("shoal.yml"))));
        assert_eq!((read("node"), read("node.prev")), ("old".to_string(), "new".to_string()));
        assert_eq!((read("shoal.yml"), read("shoal.yml.prev")), ("old file".to_string(), "new file".to_string()));
        // swapped again, both are back
        assert!(run_here(&dir, &swap_script(&at("node"), &at("shoal.yml"))));
        assert_eq!((read("node"), read("shoal.yml")), ("new".to_string(), "new file".to_string()));
        // a node upgraded before it kept a file: the program alone, the file left
        std::fs::remove_file(dir.join("shoal.yml.prev")).expect("removes");
        assert!(run_here(&dir, &swap_script(&at("node"), &at("shoal.yml"))));
        assert_eq!((read("node"), read("shoal.yml")), ("old".to_string(), "new file".to_string()));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A member row
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `health` - Its health
    fn row(node: &str, health: &str) -> MemberRow {
        MemberRow {
            node: node.to_string(),
            role: "voter".to_string(),
            health: health.to_string(),
            phase: "member".to_string(),
            incarnation: 1,
            grace_remaining_ms: None,
            weight: 1,
            free_bytes: None,
            roots: Vec::new(),
            devices: Vec::new(),
            held_bytes: None,
            wire_max: 5,
            schema_id: 0,
            schema_lineage: Vec::new(),
            client: String::new(),
            host: None,
            rack: None,
            zone: None,
        }
    }

    /// A record of three nodes, `a`, `b` and `c`, whose ids are `id-a` and so on
    fn record() -> ClusterRecord {
        let mut record = ClusterRecord {
            initialized: true,
            ..ClusterRecord::default()
        };
        for name in ["a", "b", "c"] {
            record.nodes.insert(
                name.to_string(),
                NodeRecord {
                    node: format!("id-{name}"),
                    address: "10.0.0.1".to_string(),
                    target: name.to_string(),
                },
            );
        }
        record
    }

    /// A healthy model of that record's three nodes
    fn healthy() -> ClusterModel {
        ClusterModel {
            members: vec![row("id-a", "up"), row("id-b", "up"), row("id-c", "up")],
            default_writes: "admitted".to_string(),
            ..ClusterModel::default()
        }
    }

    /// A healthy cluster passes, and every way of not being healthy is refused by name
    #[test]
    fn an_unhealthy_cluster_is_refused_by_name() {
        let record = record();
        assert_eq!(judge_health(&healthy(), &record, &[]), Ok(()));
        // a down member
        let mut model = healthy();
        model.members[1].health = "down".to_string();
        assert!(judge_health(&model, &record, &[])
            .unwrap_err()
            .contains("b is down"));
        // a leaving member
        let mut model = healthy();
        model.members[2].phase = "leaving".to_string();
        assert!(judge_health(&model, &record, &[])
            .unwrap_err()
            .contains("c is leaving"));
        // a recorded node the cluster does not know
        let mut model = healthy();
        model.members.pop();
        assert!(judge_health(&model, &record, &[])
            .unwrap_err()
            .contains("c (id-c)"));
        // writes refused
        let mut model = healthy();
        model.default_writes = "refused: have 1 need 2".to_string();
        assert!(judge_health(&model, &record, &[])
            .unwrap_err()
            .contains("refused"));
        // a set under its factor
        let mut model = healthy();
        model.under_replicated_sets = 2;
        assert!(judge_health(&model, &record, &[])
            .unwrap_err()
            .contains("2 replica sets"));
        // a plan still running
        let mut model = healthy();
        model.plans.push(PlanRow {
            op: "op-1".to_string(),
            kind: "Rebalance".to_string(),
            phase: "running".to_string(),
            steps: 3,
            moved: 1,
            blocked: None,
        });
        assert!(judge_health(&model, &record, &[]).unwrap_err().contains("op-1"));
    }

    /// A down node named to be upgraded is a repair, and nothing else is let through (item 136)
    #[test]
    fn a_down_node_named_for_upgrade_is_a_repair() {
        let record = record();
        let mut model = healthy();
        model.members[1].health = "down".to_string();
        // named, it is repaired; named with another, still
        assert_eq!(judge_health(&model, &record, &["b".to_string()]), Ok(()));
        assert_eq!(
            judge_health(&model, &record, &["b".to_string(), "c".to_string()]),
            Ok(())
        );
        // not named, it still stops an upgrade of the others
        assert!(judge_health(&model, &record, &["c".to_string()])
            .unwrap_err()
            .contains("b is down"));
        // a named node that is leaving rather than down is not a repair
        let mut model = healthy();
        model.members[1].phase = "leaving".to_string();
        assert!(judge_health(&model, &record, &["b".to_string()])
            .unwrap_err()
            .contains("b is leaving"));
        // and a repair does not excuse refused writes
        let mut model = healthy();
        model.members[1].health = "down".to_string();
        model.default_writes = "refused: have 1 need 2".to_string();
        assert!(judge_health(&model, &record, &["b".to_string()])
            .unwrap_err()
            .contains("refused"));
    }

    /// The leader goes last, the rest keep the record's order, and a typo is refused
    #[test]
    fn the_leader_is_upgraded_last() {
        let record = record();
        assert_eq!(
            upgrade_order(&record, &[], Some("id-a")).unwrap(),
            vec!["b", "c", "a"]
        );
        assert_eq!(
            upgrade_order(&record, &[], None).unwrap(),
            vec!["a", "b", "c"]
        );
        // only the nodes asked for, still with the leader last
        let only = vec!["b".to_string(), "c".to_string()];
        assert_eq!(
            upgrade_order(&record, &only, Some("id-b")).unwrap(),
            vec!["c", "b"]
        );
        // a name nobody deployed
        let typo = vec!["d".to_string()];
        assert!(upgrade_order(&record, &typo, None)
            .unwrap_err()
            .contains("\"d\""));
    }

    /// A node has caught up only when it admits writes, lags nothing and installs nothing
    #[test]
    fn caught_up_needs_no_lag_and_no_install() {
        let back = BackWhen {
            shards: 6,
            groups: 36,
        };
        let mut model = healthy();
        model.shards_reporting = 6;
        model.groups = 36;
        assert!(caught_up(&model, back));
        model.lag_max = 3;
        assert!(!caught_up(&model, back));
        model.lag_max = 0;
        model.installing = 1;
        assert!(!caught_up(&model, back));
        model.installing = 0;
        model.default_writes = "unknown".to_string();
        assert!(!caught_up(&model, back));
    }

    /// A node whose shards are still starting has not caught up, whatever its lag says (item 137)
    ///
    /// On the lab a rolling upgrade judged a node back five seconds before its shards had
    /// started: it admitted writes and lagged nothing because it hosted nothing yet.
    #[test]
    fn a_node_still_starting_its_shards_has_not_caught_up() {
        let back = BackWhen {
            shards: 6,
            groups: 36,
        };
        // the control thread answers and no shard has reported: zero lag, zero groups
        let mut model = healthy();
        assert!(!caught_up(&model, back));
        // some shards reported
        model.shards_reporting = 4;
        model.groups = 24;
        assert!(!caught_up(&model, back));
        // every shard reported, and some groups are still starting
        model.shards_reporting = 6;
        model.groups = 36;
        model.starting = 5;
        assert!(!caught_up(&model, back));
        // every group is up
        model.starting = 0;
        assert!(caught_up(&model, back));
        // a repair, where nothing was known before: one shard and one group at least
        let unknown = BackWhen { shards: 1, groups: 1 };
        assert!(!caught_up(&healthy(), unknown));
    }

    /// Only a version every member speaks and that is above the activated one is activated
    #[test]
    fn only_a_higher_common_version_is_activated() {
        let mut model = healthy();
        model.activated_wire = 4;
        // every member speaks 5
        model.wire_range = (5, 5);
        assert_eq!(activation_target(&model), Some(5));
        // one member still speaks only 4
        model.wire_range = (4, 5);
        assert_eq!(activation_target(&model), None);
        // already activated
        model.activated_wire = 5;
        model.wire_range = (5, 5);
        assert_eq!(activation_target(&model), None);
    }
}

/// A node's file with the inventory's pools and bindings in its storage block, or none when it
/// already holds them
///
/// Only `storage.pools` and `storage.buckets` are touched, an empty map left out as the renderer
/// leaves it, so everything else the file was deployed with is kept
/// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md)).
///
/// # Arguments
///
/// * `current` - The file as the node runs on it
/// * `pools` - The inventory's pools
/// * `buckets` - The inventory's bindings
///
/// # Errors
///
/// When the file is not YAML or has no storage block.
fn with_object_policy(
    current: &str,
    pools: &std::collections::BTreeMap<String, shoal::shared::pools::PoolSpec>,
    buckets: &std::collections::BTreeMap<String, shoal::shared::pools::BindingSpec>,
) -> color_eyre::Result<Option<String>> {
    let mut parsed: serde_yaml::Value =
        serde_yaml::from_str(current).map_err(|error| eyre!("not YAML: {error}"))?;
    // the pools and bindings as the renderer writes them
    let pools_value = serde_yaml::to_value(pools).map_err(|error| eyre!("the pools do not encode: {error}"))?;
    let buckets_value =
        serde_yaml::to_value(buckets).map_err(|error| eyre!("the bindings do not encode: {error}"))?;
    let Some(storage) = parsed
        .get_mut("storage")
        .and_then(serde_yaml::Value::as_mapping_mut)
    else {
        bail!("no storage block");
    };
    // what the file holds now, read as the node reads it, so a default written or left out is
    // the same policy
    let held_pools: Option<std::collections::BTreeMap<String, shoal::shared::pools::PoolSpec>> = storage
        .get("pools")
        .map(|value| serde_yaml::from_value(value.clone()))
        .transpose()
        .map_err(|error| eyre!("its pools do not parse: {error}"))?;
    let held_buckets: Option<std::collections::BTreeMap<String, shoal::shared::pools::BindingSpec>> = storage
        .get("buckets")
        .map(|value| serde_yaml::from_value(value.clone()))
        .transpose()
        .map_err(|error| eyre!("its bindings do not parse: {error}"))?;
    let same = held_pools.unwrap_or_default() == *pools && held_buckets.unwrap_or_default() == *buckets;
    if same {
        return Ok(None);
    }
    // both written as the inventory has them, an empty map left out
    for (key, value, empty) in [
        ("pools", pools_value, pools.is_empty()),
        ("buckets", buckets_value, buckets.is_empty()),
    ] {
        let key = serde_yaml::Value::from(key);
        if empty {
            storage.remove(&key);
        } else {
            storage.insert(key, value);
        }
    }
    serde_yaml::to_string(&parsed)
        .map(Some)
        .map_err(|error| eyre!("does not encode: {error}"))
}

/// Whether two renderings of a node's file say the same thing apart from the admin credential
///
/// The credential is derived with a fresh salt every render, so it always differs.
///
/// # Arguments
///
/// * `current` - The file a node runs on
/// * `rendered` - The file the inventory renders for it now
///
/// # Errors
///
/// When either is not YAML.
fn same_but_credentials(current: &str, rendered: &str) -> color_eyre::Result<bool> {
    // both as values, each without the users the credential is kept under
    let strip = |text: &str| -> color_eyre::Result<serde_yaml::Value> {
        let mut value: serde_yaml::Value = serde_yaml::from_str(text)?;
        if let Some(auth) = value.get_mut("auth").and_then(serde_yaml::Value::as_mapping_mut) {
            auth.remove("users");
        }
        Ok(value)
    };
    Ok(strip(current)? == strip(rendered)?)
}

#[cfg(test)]
mod reconfigure_tests {
    use super::{same_but_credentials, with_object_policy};

    /// A node's file takes the inventory's pools and bindings with its program, and nothing else
    /// of it moves; a file that already holds them is left alone (F93)
    #[test]
    fn a_new_bucket_is_bound_in_the_file_and_nothing_else_moves() {
        use shoal::shared::pools::{BindingSpec, PoolSpec, Redundancy};
        use std::collections::BTreeMap;
        let current = "networking:\n  port: 12000\nstorage:\n  default:\n    filesystem: {}\n  pools:\n    fast:\n      class: ssd\n      redundancy:\n        replicas: 3\n  buckets:\n    Posters:\n      pool: fast\n";
        let mut pools = BTreeMap::new();
        pools.insert("fast".to_string(), PoolSpec::new("ssd", Redundancy::Replicas(3)));
        let mut buckets = BTreeMap::new();
        buckets.insert("Posters".to_string(), BindingSpec { pool: "fast".to_string() });
        // the same policy leaves the file as it is
        assert_eq!(with_object_policy(current, &pools, &buckets).expect("judged"), None);
        // an appended bucket is bound, and the rest of the file is as it was
        buckets.insert("Stills".to_string(), BindingSpec { pool: "fast".to_string() });
        let written = with_object_policy(current, &pools, &buckets)
            .expect("judged")
            .expect("changed");
        let parsed: serde_yaml::Value = serde_yaml::from_str(&written).expect("YAML");
        assert_eq!(parsed["storage"]["buckets"]["Stills"]["pool"], "fast");
        assert_eq!(parsed["storage"]["buckets"]["Posters"]["pool"], "fast");
        assert_eq!(parsed["networking"]["port"], 12000);
        assert!(parsed["storage"]["default"]["filesystem"].is_mapping());
        // and a file with no storage block is refused, never guessed at
        assert!(with_object_policy("networking: {}\n", &pools, &buckets).is_err());
    }

    /// Two renderings that differ only in the credential's salt are the same file; any other
    /// difference is a change
    #[test]
    fn only_the_credential_is_ignored() {
        let a = "auth:\n  required: true\n  users:\n    admin:\n      scram_sha_256: {salt: aa}\ncluster:\n  bootstrap: true\n";
        let b = "auth:\n  required: true\n  users:\n    admin:\n      scram_sha_256: {salt: bb}\ncluster:\n  bootstrap: true\n";
        let c = "auth:\n  required: true\n  users:\n    admin:\n      scram_sha_256: {salt: bb}\ncluster:\n  bootstrap: true\n  replication: {wal_commit_delay: 3ms}\n";
        assert!(same_but_credentials(a, b).expect("yaml"));
        assert!(!same_but_credentials(a, c).expect("yaml"));
    }
}
