//! Rebuild one node from its peers under a new identity
//!
//! A node whose disk cannot start it, or whose data is to be thrown away, is stopped, wiped and
//! started again as a new member that joins through the others, and its old identity is removed
//! with the new one as its replacement: every set the old node held is moved onto the new one by
//! the leader's plan ([F56](../../../docs/src/features/cluster-rebuild.md)).
//!
//! The same identity is never reused. A voter that comes back empty under its old identity
//! grants its vote to any candidate, so a committed write held by one other member alone could
//! be lost if that member failed before the refill. A new identity has no vote to give until
//! the plan has made it a member of each set.

use color_eyre::eyre::{bail, eyre};
use rkyv::Archive;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminRequest};
use shoal::shared::traits::QuerySupport;
use std::time::Instant;
use uuid::Uuid;

use super::ops::{control_addresses, step, wait_for_members, Deployment, POLL_INTERVAL, UP_TIMEOUT};
use super::remote::{quote, Host};
use super::render::Entry;
use super::state::{ClusterRecord, NodeRecord};
use crate::cluster::ClusterModel;

/// Why a node cannot be rebuilt now, judged from what the cluster says, or none
///
/// A rebuild takes the node's copies away until the plan has moved them onto its replacement,
/// so every other member has to be up and placeable, and the node has to be one the cluster
/// still counts as a plain member.
///
/// # Arguments
///
/// * `model` - The cluster, as a member answered it
/// * `old` - The identity the node has now
#[must_use]
pub fn rebuild_refusal(model: &ClusterModel, old: &str) -> Option<String> {
    // the node has to be a member the cluster still counts
    let Some(member) = model.members.iter().find(|member| member.node == old) else {
        return Some(format!("{old} is not a member of the cluster"));
    };
    if !member.phase.eq_ignore_ascii_case("member") {
        return Some(format!(
            "{old} is {}, not a plain member; let its plan finish first",
            member.phase
        ));
    }
    // every other member up and a plain member, since the rebuilt node's copies are gone until
    // the plan refills them; one already removed, a previous rebuild's old identity among them,
    // holds nothing and is listed only as history
    let others: Vec<String> = model
        .members
        .iter()
        .filter(|other| other.node != old)
        .filter(|other| !other.phase.eq_ignore_ascii_case("removed"))
        .filter(|other| {
            !other.health.eq_ignore_ascii_case("up") || !other.phase.eq_ignore_ascii_case("member")
        })
        .map(|other| format!("{} is {} and {}", other.node, other.health, other.phase))
        .collect();
    if !others.is_empty() {
        return Some(format!(
            "every other member has to be up while a node is rebuilt: {}",
            others.join("; ")
        ));
    }
    // an open plan would compete with the removal for the same sets
    if !model.plans.is_empty() {
        return Some(format!(
            "{} plan{} open; a rebuild waits for {}",
            model.plans.len(),
            if model.plans.len() == 1 { " is" } else { "s are" },
            if model.plans.len() == 1 { "it" } else { "them" }
        ));
    }
    None
}

impl Deployment {
    /// Rebuild one deployed node from its peers under a new identity
    ///
    /// Stops and disables the node, waits for the cluster to commit it down, wipes every storage
    /// root and its certificate, stages it to join through the other members, claims a new
    /// identity with a new leaf, starts it, and removes the old identity with the new one as its
    /// replacement, following the plan until every set it held has moved.
    ///
    /// # Arguments
    ///
    /// * `name` - The node, by its inventory name
    /// * `yes` - Whether the operator confirmed that the node's data is deleted
    ///
    /// # Errors
    ///
    /// When the node is not deployed, the cluster is not healthy enough to lose its copies for
    /// a while, the operator did not confirm, or any step fails.
    pub async fn rebuild<S>(&self, name: &str, yes: bool) -> color_eyre::Result<()>
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
        let started = Instant::now();
        // the node has to be deployed, and the operator has to mean it
        let mut record = self.state.record()?;
        let Some(old_record) = record.nodes.get(name).cloned() else {
            bail!("{name} is not deployed in {}", self.inventory.name);
        };
        if !yes {
            bail!("rebuilding {name} deletes everything under its storage roots; pass --yes");
        }
        let old = old_record.node.clone();
        let node = self.inventory.node(name)?;
        let password = self.state.password()?;
        // the cluster is reached through the other members, never the node being rebuilt
        let others = ClusterRecord {
            nodes: record
                .nodes
                .iter()
                .filter(|(other, _)| other.as_str() != name)
                .map(|(other, node)| (other.clone(), node.clone()))
                .collect(),
            ..record.clone()
        };
        if others.nodes.is_empty() {
            bail!("{name} is the only deployed node; there is nobody to rebuild it from");
        }
        let shoal = self.any_member::<S>(&others).await?;
        let model = crate::cluster::poll(&shoal).await.map_err(|error| eyre!(error))?;
        if let Some(refusal) = rebuild_refusal(&model, &old) {
            bail!("{name} cannot be rebuilt now: {refusal}");
        }
        let seeds: Vec<String> = control_addresses(&shoal).await?;
        // 1. stopped, and kept stopped: a unit that restarts on failure would come back mid-wipe
        let host = Host {
            target: node.target.clone(),
        };
        step(Some(name), &format!("stopping {old}"));
        host.run(&format!(
            "sudo -n systemctl disable --now {} 2>/dev/null || true",
            quote(&self.inventory.unit_name())
        ))?;
        // 2. committed down, which is what lets it be removed
        self.wait_down(&shoal, &old).await?;
        step(Some(name), &format!("down after {:.0?}", started.elapsed()));
        // 3. wiped, staged to join through the others, and claimed as a new identity
        let facts = self.preflight(&node, true)?;
        self.stage(&node, &facts, &Entry::Join(seeds), &password)?;
        let (new, _) = self.claim(&node, &facts)?;
        if new.to_string() == old {
            bail!("{name} claimed its old identity {old}; its storage was not wiped");
        }
        step(Some(name), &format!("claimed {new}, replacing {old}"));
        self.provision_tls(&node, &facts, new)?;
        record.nodes.insert(
            node.name.clone(),
            NodeRecord {
                node: new.to_string(),
                address: node.address.to_string(),
                target: node.target.clone(),
            },
        );
        self.state.save(&record)?;
        // 4. started, and up as a member the plan can place onto
        self.start_unit(&node, &facts)?;
        wait_for_members(&shoal, &[new], 0).await?;
        step(Some(name), &format!("joined as {new} after {:.0?}", started.elapsed()));
        // 5. the old identity removed, with the new one taking every set it held
        let old_id = NodeId(old.parse::<Uuid>()?);
        let op = Uuid::new_v4();
        let model = crate::cluster::poll(&shoal).await.map_err(|error| eyre!(error))?;
        step(None, &format!("removing {old} onto {new} as {op}"));
        let response = shoal
            .admin(&AdminRequest {
                op,
                expected_version: model.version,
                kind: AdminKind::Remove {
                    node: old_id,
                    replacement: Some(new),
                },
            })
            .await
            .map_err(|error| eyre!("remove: {error:?}"))?;
        if let Err(error) = response.outcome {
            bail!("the removal of {old} was refused: {} ({:?})", error.msg, error.code());
        }
        self.follow_plan(&shoal, op, "removal").await?;
        // 6. every deployed node up and the control group whole again
        let mut ids = record
            .nodes
            .values()
            .map(|node| node.node.parse::<Uuid>().map(NodeId))
            .collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        let voters = (self.inventory.control_voters as usize).min(ids.len());
        wait_for_members(&shoal, &ids, voters).await?;
        step(Some(name), &format!("rebuilt as {new} in {:.0?}", started.elapsed()));
        Ok(())
    }

    /// Wait until the cluster has committed a member down
    ///
    /// # Arguments
    ///
    /// * `shoal` - The admin client, of another member
    /// * `node` - The member, as its id prints
    async fn wait_down<S>(
        &self,
        shoal: &std::sync::Arc<shoal::Shoal<S>>,
        node: &str,
    ) -> color_eyre::Result<()>
    where
        S: QuerySupport + Send + Sync + 'static,
    {
        let deadline = Instant::now() + UP_TIMEOUT;
        loop {
            // a poll that fails is a cluster still settling, not a failure yet
            let health = crate::cluster::poll(shoal).await.ok().and_then(|model| {
                model
                    .members
                    .iter()
                    .find(|member| member.node == node)
                    .map(|member| member.health.clone())
            });
            if health
                .as_deref()
                .is_some_and(|health| health.eq_ignore_ascii_case("down"))
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("{node} was not committed down after {UP_TIMEOUT:?}: it is {health:?}");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::rebuild_refusal;
    use crate::cluster::{ClusterModel, MemberRow};

    /// A member row at a health and a phase
    fn row(node: &str, health: &str, phase: &str) -> MemberRow {
        MemberRow {
            node: node.to_string(),
            role: "voter".to_string(),
            health: health.to_string(),
            phase: phase.to_string(),
            incarnation: 1,
            grace_remaining_ms: None,
            weight: 0,
            free_bytes: None,
            held_bytes: None,
            wire_max: 6,
            client: String::new(),
        }
    }

    /// A node is rebuilt only while every other member is up and nothing else moves the sets
    #[test]
    fn a_rebuild_needs_every_other_member_up_and_no_plan() {
        let mut model = ClusterModel {
            members: vec![row("a", "up", "Member"), row("b", "up", "Member"), row("c", "up", "Member")],
            ..ClusterModel::default()
        };
        // healthy: a node can be rebuilt, and one that is down already can too
        assert_eq!(rebuild_refusal(&model, "a"), None);
        model.members[0].health = "down".to_string();
        assert_eq!(rebuild_refusal(&model, "a"), None);
        // not a member at all
        assert!(rebuild_refusal(&model, "z").is_some_and(|why| why.contains("not a member")));
        // another member down: its copies and the rebuilt one's would both be gone
        model.members[1].health = "down".to_string();
        assert!(rebuild_refusal(&model, "a").is_some_and(|why| why.contains("every other member")));
        model.members[1].health = "up".to_string();
        // a member already removed, a previous rebuild's old identity, is history and no bar
        model.members.push(row("z", "down", "Removed"));
        assert_eq!(rebuild_refusal(&model, "a"), None);
        // the node leaving already
        model.members[0].phase = "Leaving".to_string();
        assert!(rebuild_refusal(&model, "a").is_some_and(|why| why.contains("plain member")));
    }
}
