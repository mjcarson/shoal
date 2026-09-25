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
    digest, install_binary, step, wait_for_members, Deployment, POLL_INTERVAL, UP_TIMEOUT,
};
use super::remote::{quote, Host, SIGILL_STATUS};
use super::render::Layout;
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
        // judge it healthy before anything is touched
        let shoal = self.any_member::<S>(&record).await?;
        let model = crate::cluster::poll(&shoal)
            .await
            .map_err(|error| eyre!(error))?;
        judge_health(&model, &record, only).map_err(|why| {
            eyre!(
                "{} is not ready for a rolling restart: {why}",
                self.inventory.name
            )
        })?;
        // the order, with the leader last
        let order =
            upgrade_order(&record, only, model.leader.as_deref()).map_err(|error| eyre!(error))?;
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
        let shoal = self.any_member::<S>(&record).await?;
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
            (Some(wire), true) => self.activate(&shoal, model.version, wire).await,
            (Some(wire), false) => {
                step(
                    None,
                    &format!(
                        "every member speaks wire {wire}; `cluster upgrade --activate` activates it, \
                         after which no node can roll back below it"
                    ),
                );
                Ok(())
            }
            (None, _) => {
                if activate {
                    step(None, "nothing to activate: no version every member speaks is above the activated one");
                }
                Ok(())
            }
        }
    }

    /// Activate a wire version
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client
    /// * `version` - The topology version the request is judged against
    /// * `wire` - The version to activate
    async fn activate<S>(
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
        let response = shoal
            .admin(&AdminRequest {
                op,
                expected_version: version,
                kind: AdminKind::Activate { wire },
            })
            .await
            .map_err(|error| eyre!("activate: {error:?}"))?;
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
        let binary = self.layout_binary()?;
        // a node already on this program is done, which is what lets a rerun resume
        let installed = self.installed(&host, &binary)?;
        let local = digest(&self.inventory.server)?;
        let same = installed.digest == local;
        if same && !force {
            step(Some(name), "already runs this program");
            return Ok(false);
        }
        // the program beside the node's, owned by the user it runs as
        step(
            Some(name),
            &format!("pushing {}", self.inventory.server.display()),
        );
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
                    "{} died of an illegal instruction on {name}: it was built for another cpu. build it \
                     with RUSTFLAGS=\"-C target-cpu=<the oldest host's cpu>\" rather than native. \
                     no node was changed by this step",
                    self.inventory.server.display()
                );
            }
            bail!(
                "{} --version failed on {name} with status {:?}: {}",
                self.inventory.server.display(),
                output.status,
                output.stderr.trim()
            );
        }
        // keep the program it replaces, unless it is this one, then swap the candidate in
        step(Some(name), "installing it");
        host.run(&format!(
            "set -e; {keep} sudo -n mv -f {candidate} {binary}",
            keep = if same {
                String::new()
            } else {
                format!(
                    "sudo -n cp -p {} {};",
                    quote(&binary),
                    quote(&format!("{binary}.prev"))
                )
            },
            candidate = quote(&candidate),
            binary = quote(&binary),
        ))?;
        // restart it, and put the previous program back if it does not come back
        let Err(error) = self
            .restart_and_wait::<S>(record, name, node, ids, voters)
            .await
        else {
            return Ok(true);
        };
        // a forced restart onto the program it already ran has nothing to go back to
        if same && !installed.prev {
            bail!("{name} did not come back: {error}{}", self.journal(&host));
        }
        Err(self
            .revert::<S>(record, name, node, ids, voters, &error)
            .await)
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
        let binary = self.layout_binary()?;
        // a node that kept nothing has nothing to roll back to
        let installed = self.installed(&host, &binary)?;
        if !installed.prev {
            bail!("{name} kept no previous program at {binary}.prev; it was never upgraded");
        }
        // swap the two, so the rollback can itself be undone
        step(Some(name), "swapping its previous program back in");
        swap(&host, &binary)?;
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
        let journal = self.journal(&host);
        step(
            Some(name),
            &format!("did not come back ({error}); swapping its previous program back in"),
        );
        // swap back and wait for it the same way
        let reverted = match self.layout_binary() {
            Ok(binary) => match swap(&host, &binary) {
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
        let unit = quote(&self.inventory.unit_name());
        let address: std::net::IpAddr = node.address.parse()?;
        let addr = super::inventory::socket(address, self.inventory.ports.client);
        // what the node has to show to count as back: the shards and groups it had before this
        // restart if it answers now, else the cores it is configured with. a node being
        // repaired is down and answers nothing
        let back = match self
            .connect::<S>(&addr, Instant::now() + BEFORE_RESTART_POLL)
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
            &format!("restarting {}", self.inventory.unit_name()),
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
        let member = self.any_member::<S>(record).await?;
        wait_for_members(&member, ids, voters).await?;
        // and the node itself answers, admits writes, and has caught its groups up
        step(Some(name), "waiting for it to catch up");
        let itself = self.connect::<S>(&addr, deadline).await?;
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

    /// Where every node's program is on its host
    fn layout_binary(&self) -> color_eyre::Result<String> {
        let layout = Layout {
            dir: self.inventory.remote_dir(),
        };
        Ok(layout.binary(&self.inventory.server_name()?))
    }

    /// The end of a node's journal, as a suffix for an error, or nothing if it cannot be read
    ///
    /// # Arguments
    ///
    /// * `host` - The host
    fn journal(&self, host: &Host) -> String {
        // best effort: an error about the node is still worth more than one about its journal
        match host.run(&format!(
            "sudo -n journalctl --no-pager -u {} -n {JOURNAL_LINES}",
            quote(&self.inventory.unit_name())
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
fn swap(host: &Host, binary: &str) -> color_eyre::Result<()> {
    // three renames through a spare name; the running process keeps its own inode throughout
    host.run(&format!(
        "set -e; sudo -n mv -f {binary} {spare}; sudo -n mv -f {prev} {binary}; sudo -n mv -f {spare} {prev}",
        binary = quote(binary),
        prev = quote(&format!("{binary}.prev")),
        spare = quote(&format!("{binary}.swap")),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::model::{MemberRow, PlanRow};

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
            held_bytes: None,
            wire_max: 5,
            client: String::new(),
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
