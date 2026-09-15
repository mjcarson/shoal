//! The cluster as the admin frames describe it, rendered to lines

use shoal::serde_json::Value;

/// One member of the cluster, as the tab shows it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRow {
    /// The node, as its id prints
    pub node: String,
    /// Voter or learner
    pub role: String,
    /// Up, down or joining
    pub health: String,
    /// Member, leaving, removing or removed
    pub phase: String,
    /// Which start of the node this is
    pub incarnation: u64,
    /// How much of a down member's grace is left, if it is in one
    pub grace_remaining_ms: Option<u64>,
    /// Its placement weight
    pub weight: u64,
    /// The bytes it reported free, if it has
    pub free_bytes: Option<u64>,
    /// The bytes its groups hold, if it has reported
    pub held_bytes: Option<u64>,
    /// The newest wire version it speaks
    pub wire_max: u64,
    /// Where clients reach it
    pub client: String,
}

/// One open plan, as the tab shows it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRow {
    /// The operation
    pub op: String,
    /// What kind of plan it is
    pub kind: String,
    /// Where it stands
    pub phase: String,
    /// How many steps it holds
    pub steps: usize,
    /// How many of them moved
    pub moved: usize,
    /// Why it cannot go on, if it cannot
    pub blocked: Option<String>,
}

/// What a cluster looks like right now, built from the admin frames
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterModel {
    /// The cluster's identity
    pub cluster: String,
    /// The node the client reached
    pub node: String,
    /// The topology version the frames were taken at
    pub version: u64,
    /// The control leader, if one is known
    pub leader: Option<String>,
    /// How many voters and learners the control group has
    pub voters: usize,
    /// How many learners
    pub learners: usize,
    /// The replication factor the policy asks for
    pub desired_rf: u64,
    /// The factor the placement serves
    pub active_rf: u64,
    /// How many members are up
    pub up_members: u64,
    /// Whether a default write is admitted right now, and if not why
    pub default_writes: String,
    /// How many replica sets hold a copy on a member the cluster has given up on
    pub under_replicated_sets: u64,
    /// How many identities are tombstoned
    pub tombstones: usize,
    /// The wire version the cluster activated
    pub activated_wire: u64,
    /// The lowest and highest wire version any member speaks
    pub wire_range: (u64, u64),
    /// Every member, in node order
    pub members: Vec<MemberRow>,
    /// How many tablet groups this node hosts
    pub groups: u64,
    /// How many of them it leads
    pub leading: u64,
    /// The widest lag behind a leader among its groups, in entries
    pub lag_max: u64,
    /// How many snapshots it is installing
    pub installing: u64,
    /// How many of its copies are quarantined
    pub quarantined: u64,
    /// Every open plan
    pub plans: Vec<PlanRow>,
    /// Every backup record, as `op phase written/skipped/failed of groups`
    pub backups: Vec<(String, String, String)>,
    /// Every recovery an operator ran, as `at lost=[..] boundary`
    pub recoveries: Vec<String>,
}

impl ClusterModel {
    /// Build the model from the frames the node answered
    ///
    /// Every field is read defensively: a frame from an older build that lacks one leaves it
    /// at its default rather than failing the tab.
    ///
    /// # Arguments
    ///
    /// * `members` - The `Members` frame
    /// * `readiness` - The `Readiness` frame
    /// * `replication` - The `Replication` frame
    /// * `plans` - The `Plans` frame
    /// * `backups` - The `Backups` frame
    /// * `recoveries` - The `Recoveries` frame
    #[must_use]
    pub fn from_frames(
        members: &Value,
        readiness: &Value,
        replication: &Value,
        plans: &Value,
        backups: &Value,
        recoveries: &Value,
    ) -> Self {
        let rows = members["members"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|member| MemberRow {
                node: member["record"]["node"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                role: member["role"].as_str().unwrap_or_default().to_string(),
                health: member["health"].as_str().unwrap_or_default().to_string(),
                phase: member["phase"].as_str().unwrap_or("member").to_string(),
                incarnation: member["record"]["incarnation"].as_u64().unwrap_or(0),
                grace_remaining_ms: member["grace_remaining_ms"].as_u64(),
                weight: member["weight"].as_u64().unwrap_or(0),
                free_bytes: member["free_bytes"].as_u64(),
                held_bytes: member["held_bytes"].as_u64(),
                wire_max: member["record"]["wire_max"].as_u64().unwrap_or(0),
                client: member["record"]["client"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            })
            .collect();
        let plans = plans
            .as_array()
            .into_iter()
            .flatten()
            .filter(|plan| plan["outcome"].is_null())
            .map(|plan| {
                let steps = plan["steps"].as_array().cloned().unwrap_or_default();
                PlanRow {
                    op: plan["op"].as_str().unwrap_or_default().to_string(),
                    kind: kind_name(&plan["kind"]),
                    phase: plan["phase"].as_str().unwrap_or_default().to_string(),
                    steps: steps.len(),
                    moved: steps.iter().filter(|step| step["state"] == "Moved").count(),
                    blocked: plan["blocked"]["reason"].as_str().map(str::to_string),
                }
            })
            .collect();
        let backups = backups
            .as_array()
            .into_iter()
            .flatten()
            .map(|record| {
                let groups = record["groups"].as_object().cloned().unwrap_or_default();
                let written = groups
                    .values()
                    .filter(|group| group["outcome"]["Written"].is_object())
                    .count();
                let skipped = groups
                    .values()
                    .filter(|group| group["outcome"]["Skipped"].is_object())
                    .count();
                let failed = groups
                    .values()
                    .filter(|group| group["outcome"]["Failed"].is_object())
                    .count();
                let done = groups.values().all(|group| group["phase"] == "Done");
                (
                    record["op"].as_str().unwrap_or_default().to_string(),
                    if done {
                        "done".to_string()
                    } else {
                        "running".to_string()
                    },
                    format!(
                        "{written} written, {skipped} skipped, {failed} failed of {}",
                        groups.len()
                    ),
                )
            })
            .collect();
        let recoveries = recoveries
            .as_array()
            .into_iter()
            .flatten()
            .map(|record| {
                format!(
                    "at {} lost {} boundary {}",
                    record["at"].as_str().unwrap_or_default(),
                    record["lost"].as_array().map_or(0, Vec::len),
                    record["last_committed"].as_u64().unwrap_or(0)
                )
            })
            .collect();
        ClusterModel {
            cluster: members["cluster"].as_str().unwrap_or_default().to_string(),
            node: members["node"].as_str().unwrap_or_default().to_string(),
            version: members["version"].as_u64().unwrap_or(0),
            leader: members["leader"].as_str().map(str::to_string),
            voters: members["voters"].as_u64().unwrap_or(0) as usize,
            learners: members["learners"].as_u64().unwrap_or(0) as usize,
            desired_rf: members["desired_rf"].as_u64().unwrap_or(0),
            active_rf: members["active_rf"].as_u64().unwrap_or(0),
            up_members: members["up_members"].as_u64().unwrap_or(0),
            default_writes: match &readiness["data"]["default_writes"] {
                Value::Object(map) if map.contains_key("Ok") => "admitted".to_string(),
                Value::Object(map) => match map.get("Err") {
                    Some(err) => format!(
                        "refused: have {} need {}",
                        err["have"].as_u64().unwrap_or(0),
                        err["need"].as_u64().unwrap_or(0)
                    ),
                    None => "unknown".to_string(),
                },
                _ => "unknown".to_string(),
            },
            under_replicated_sets: members["under_replicated_sets"].as_u64().unwrap_or(0),
            tombstones: members["tombstones"].as_object().map_or(0, |map| map.len()),
            activated_wire: members["wire"]["activated"].as_u64().unwrap_or(0),
            wire_range: (
                members["wire"]["min_member"].as_u64().unwrap_or(0),
                members["wire"]["max_member"].as_u64().unwrap_or(0),
            ),
            members: rows,
            groups: replication["groups"].as_u64().unwrap_or(0),
            leading: replication["leading"].as_u64().unwrap_or(0),
            lag_max: replication["lag_max"].as_u64().unwrap_or(0),
            installing: replication["installing"].as_u64().unwrap_or(0),
            quarantined: replication["quarantined"].as_u64().unwrap_or(0),
            plans,
            backups,
            recoveries,
        }
    }

    /// The figure an operator reads first: copies against the factor, and who is missing
    ///
    /// The M9b figure - "two copies, desired three, awaiting a member" - said in one line.
    #[must_use]
    pub fn headline(&self) -> String {
        let missing = self.desired_rf.saturating_sub(self.active_rf);
        let mut line = format!("{} of {} copies", self.active_rf, self.desired_rf);
        if missing > 0 {
            line.push_str(&format!(
                ", awaiting {missing} member{}",
                if missing == 1 { "" } else { "s" }
            ));
        }
        if self.under_replicated_sets > 0 {
            line.push_str(&format!(
                ", {} set{} under-replicated",
                self.under_replicated_sets,
                if self.under_replicated_sets == 1 {
                    ""
                } else {
                    "s"
                }
            ));
        }
        line.push_str(&format!("; writes {}", self.default_writes));
        line
    }

    /// The model as the lines the tab draws
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        lines.push(format!(
            "cluster {} via {} at version {}",
            self.cluster, self.node, self.version
        ));
        lines.push(format!(
            "leader {}  voters {}  learners {}  up {}  tombstones {}",
            self.leader.as_deref().unwrap_or("none"),
            self.voters,
            self.learners,
            self.up_members,
            self.tombstones
        ));
        lines.push(self.headline());
        lines.push(format!(
            "wire activated {}  members speak {}..={}",
            self.activated_wire, self.wire_range.0, self.wire_range.1
        ));
        lines.push(format!(
            "this node: {} groups, leading {}, lag {} entries, installing {}, quarantined {}",
            self.groups, self.leading, self.lag_max, self.installing, self.quarantined
        ));
        lines.push(String::new());
        lines.push(format!(
            "{:<36} {:<7} {:<8} {:<9} {:>4} {:>8} {:>10} {:>10} {:>4}  {}",
            "member", "role", "health", "phase", "inc", "grace", "free", "held", "wire", "client"
        ));
        for member in &self.members {
            lines.push(format!(
                "{:<36} {:<7} {:<8} {:<9} {:>4} {:>8} {:>10} {:>10} {:>4}  {}",
                member.node,
                member.role,
                member.health,
                member.phase,
                member.incarnation,
                member
                    .grace_remaining_ms
                    .map_or("-".to_string(), |ms| format!("{}s", ms / 1000)),
                member.free_bytes.map_or("-".to_string(), bytes),
                member.held_bytes.map_or("-".to_string(), bytes),
                member.wire_max,
                member.client
            ));
        }
        lines.push(String::new());
        if self.plans.is_empty() {
            lines.push("no open plans".to_string());
        } else {
            lines.push("open plans".to_string());
            for plan in &self.plans {
                let mut line = format!(
                    "  {} {} {} {}/{} moved",
                    plan.op, plan.kind, plan.phase, plan.moved, plan.steps
                );
                if let Some(blocked) = &plan.blocked {
                    line.push_str(&format!(" - blocked: {blocked}"));
                }
                lines.push(line);
            }
        }
        if !self.backups.is_empty() {
            lines.push("backups".to_string());
            for (op, phase, summary) in &self.backups {
                lines.push(format!("  {op} {phase} {summary}"));
            }
        }
        if !self.recoveries.is_empty() {
            lines.push("recoveries".to_string());
            for recovery in &self.recoveries {
                lines.push(format!("  {recovery}"));
            }
        }
        lines
    }
}

/// A plan kind's name, whichever shape the record spells it in
///
/// # Arguments
///
/// * `kind` - The `kind` field
fn kind_name(kind: &Value) -> String {
    match kind {
        Value::String(name) => name.to_lowercase(),
        Value::Object(map) => map
            .keys()
            .next()
            .map(|key| key.to_lowercase())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Bytes as a short figure
///
/// # Arguments
///
/// * `bytes` - The count
fn bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}{}", UNITS[0])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterModel;
    use shoal::serde_json::json;

    /// The model reads the admin frames as the server writes them: the M9b figure of two
    /// copies against a desired three awaiting a member, the members with their phases, a
    /// blocked plan, a backup, a recovery and the wire; a frame from before a field leaves
    /// the default; and the lines say what the figure says
    #[test]
    fn the_cluster_model_reads_the_admin_frames() {
        let members = json!({
            "cluster": "c1", "node": "n0", "version": 17, "leader": "n0", "voters": 2, "learners": 1,
            "desired_rf": 3, "active_rf": 2, "up_members": 2, "under_replicated_sets": 4,
            "tombstones": { "n9": { "incarnation": 1, "removed_at": 12 } },
            "wire": { "activated": 5, "floor": 4, "newest": 5, "min_member": 4, "max_member": 5 },
            "members": [
                { "record": { "node": "n0", "incarnation": 2, "wire_max": 5, "client": "127.0.0.1:1" }, "role": "voter", "health": "up", "phase": "member", "weight": 2, "free_bytes": 2048, "held_bytes": 1024 },
                { "record": { "node": "n1", "incarnation": 1, "wire_max": 4, "client": "127.0.0.1:2" }, "role": "voter", "health": "down", "phase": "member", "weight": 1, "grace_remaining_ms": 61000 },
                { "record": { "node": "n2", "incarnation": 3, "wire_max": 5, "client": "127.0.0.1:3" }, "role": "learner", "health": "up", "phase": "removing", "weight": 1 }
            ]
        });
        let readiness =
            json!({ "data": { "default_writes": { "Err": { "have": 2, "need": 2 } } } });
        let replication =
            json!({ "groups": 6, "leading": 2, "lag_max": 3, "installing": 1, "quarantined": 0 });
        let plans = json!([
            { "op": "p1", "kind": { "Decommission": { "node": "n2" } }, "phase": "Running", "outcome": null,
              "steps": [ { "state": "Moved" }, { "state": "Pending" } ], "blocked": { "reason": "tablet 0: every up member holds the set" } },
            { "op": "p0", "kind": "Rebalance", "phase": "Done", "outcome": "Nothing", "steps": [] }
        ]);
        let backups = json!([ { "op": "b1", "groups": { "g1": { "phase": "Done", "outcome": { "Written": {} } }, "g2": { "phase": "Done", "outcome": { "Skipped": {} } } } } ]);
        let recoveries = json!([ { "at": "n0", "lost": ["n7", "n8"], "last_committed": 40 } ]);
        let model = ClusterModel::from_frames(
            &members,
            &readiness,
            &replication,
            &plans,
            &backups,
            &recoveries,
        );
        assert_eq!(
            (model.desired_rf, model.active_rf, model.up_members),
            (3, 2, 2)
        );
        assert_eq!(
            model.headline(),
            "2 of 3 copies, awaiting 1 member, 4 sets under-replicated; writes refused: have 2 need 2"
        );
        assert_eq!(model.members.len(), 3);
        assert_eq!(model.members[1].grace_remaining_ms, Some(61000));
        assert_eq!(model.members[2].phase, "removing");
        assert_eq!(model.members[0].held_bytes, Some(1024));
        assert_eq!((model.activated_wire, model.wire_range), (5, (4, 5)));
        assert_eq!(model.tombstones, 1);
        assert_eq!(
            (model.groups, model.leading, model.lag_max, model.installing),
            (6, 2, 3, 1)
        );
        // the done plan is not open; the running one is, with its blocked reason and its steps
        assert_eq!(model.plans.len(), 1);
        assert_eq!(
            (
                model.plans[0].kind.as_str(),
                model.plans[0].steps,
                model.plans[0].moved
            ),
            ("decommission", 2, 1)
        );
        assert!(
            model.plans[0]
                .blocked
                .as_deref()
                .unwrap_or_default()
                .contains("every up member")
        );
        assert_eq!(
            model.backups[0],
            (
                "b1".to_string(),
                "done".to_string(),
                "1 written, 1 skipped, 0 failed of 2".to_string()
            )
        );
        assert_eq!(
            model.recoveries,
            vec!["at n0 lost 2 boundary 40".to_string()]
        );
        let lines = model.render_lines();
        assert!(
            lines
                .iter()
                .any(|line| line.contains("2 of 3 copies, awaiting 1 member")),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("n2") && line.contains("removing")),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("blocked: tablet 0")),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("wire activated 5")),
            "{lines:?}"
        );
        // frames from before every field: nothing fails and the defaults stand
        let older = ClusterModel::from_frames(
            &json!({ "cluster": "c", "members": [] }),
            &json!({}),
            &json!({}),
            &json!([]),
            &json!([]),
            &json!([]),
        );
        assert_eq!(older.default_writes, "unknown");
        assert_eq!(older.headline(), "0 of 0 copies; writes unknown");
        assert!(
            older
                .render_lines()
                .iter()
                .any(|line| line == "no open plans")
        );
    }
}
