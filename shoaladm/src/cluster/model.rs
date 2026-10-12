//! The cluster as the admin frames describe it, rendered to lines

use shoal::serde_json::Value;
use shoal::shared::capacity::RootCapacity;
use shoal::shared::devices::DeviceReport;
use shoal::shared::domains::{domain_at, HOST, RACK, ZONE};

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
    /// The bytes free on its tightest storage root, if it has reported
    pub free_bytes: Option<u64>,
    /// Every storage root it reported, with its size, free bytes and uses
    /// ([F82](../../../docs/src/features/root-capacity.md))
    pub roots: Vec<RootCapacity>,
    /// Every device it claimed, with its slices, figures and health
    /// ([F85](../../../docs/src/features/devices-and-pools.md))
    pub devices: Vec<DeviceReport>,
    /// The bytes its groups hold, if it has reported
    pub held_bytes: Option<u64>,
    /// The newest wire version it speaks
    pub wire_max: u64,
    /// Where clients reach it
    pub client: String,
    /// The host it stands on, if its record names one
    /// ([F81](../../../docs/src/features/failure-domains.md))
    pub host: Option<String>,
    /// The rack its host stands in, if its record names one
    /// ([F83](../../../docs/src/features/rack-and-zone-domains.md))
    pub rack: Option<String>,
    /// The zone its rack stands in, if its record names one
    pub zone: Option<String>,
}

impl MemberRow {
    /// Where it stands, by level, as its record named it
    #[must_use]
    pub fn domains(&self) -> std::collections::BTreeMap<String, String> {
        // only the levels its record names
        [(HOST, &self.host), (RACK, &self.rack), (ZONE, &self.zone)]
            .into_iter()
            .filter_map(|(level, name)| name.clone().map(|name| (level.to_string(), name)))
            .collect()
    }
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
    /// How many of this node's groups have no handle up yet
    pub starting: u64,
    /// How many of this node's shards have reported their groups
    pub shards_reporting: u64,
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
    /// The admin reads the node advertises beyond the ones every cluster build answers
    pub admin_reads: Vec<String>,
    /// The stats lines drawn under the model, once a poll has read them
    /// ([F52](../../../docs/src/features/cluster-stats.md))
    pub stats: Vec<String>,
}

/// How many nodes a frame's list of them names
///
/// `TopologyView` writes `voters` and `learners` as the node ids themselves; a count is read as
/// one too, so a frame that ever carries the number instead is not read as none (item 114).
///
/// # Arguments
///
/// * `value` - The field
fn count(value: &Value) -> usize {
    match value {
        // the ids themselves, which is what the server writes
        Value::Array(nodes) => nodes.len(),
        // or a number, read as the count it is
        _ => value.as_u64().unwrap_or(0) as usize,
    }
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
                // a frame from before every root was reported has none
                roots: shoal::serde_json::from_value(member["roots"].clone()).unwrap_or_default(),
                // and before any device was
                devices: shoal::serde_json::from_value(member["devices"].clone()).unwrap_or_default(),
                held_bytes: member["held_bytes"].as_u64(),
                wire_max: member["record"]["wire_max"].as_u64().unwrap_or(0),
                client: member["record"]["client"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                host: member["record"]["failure_domains"]["host"]
                    .as_str()
                    .map(str::to_string),
                rack: member["record"]["failure_domains"]["rack"]
                    .as_str()
                    .map(str::to_string),
                zone: member["record"]["failure_domains"]["zone"]
                    .as_str()
                    .map(str::to_string),
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
            voters: count(&members["voters"]),
            learners: count(&members["learners"]),
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
            starting: replication["starting"].as_u64().unwrap_or(0),
            shards_reporting: replication["shards"]
                .as_array()
                .map_or(0, |shards| shards.len() as u64),
            leading: replication["leading"].as_u64().unwrap_or(0),
            lag_max: replication["lag_max"].as_u64().unwrap_or(0),
            installing: replication["installing"].as_u64().unwrap_or(0),
            quarantined: replication["quarantined"].as_u64().unwrap_or(0),
            plans,
            backups,
            recoveries,
            admin_reads: members["admin_reads"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|read| read.as_str().map(str::to_string))
                .collect(),
            stats: Vec::new(),
        }
    }

    /// Whether the node the model was read from answers an admin read beyond the ones every
    /// cluster build does
    ///
    /// A node that cannot decode a request closes the connection, so a read it does not
    /// advertise is never sent ([F52](../../../docs/src/features/cluster-stats.md)).
    ///
    /// # Arguments
    ///
    /// * `read` - The read's name, as `AdminKind::name` spells it
    #[must_use]
    pub fn answers(&self, read: &str) -> bool {
        self.admin_reads.iter().any(|answered| answered == read)
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

    /// How many hosts the members still in the cluster stand on
    ///
    /// A member whose record names no host stands on one of its own, as placement counts it
    /// ([F81](../../../docs/src/features/failure-domains.md)).
    #[must_use]
    pub fn hosts(&self) -> usize {
        self.spread(HOST)
    }

    /// How many domains the members still in the cluster stand in at a level
    ///
    /// Counted as the planner counts them: a level a member leaves out is the nearest one below
    /// it names, and a member naming nothing stands in a domain of its own
    /// ([F83](../../../docs/src/features/rack-and-zone-domains.md)).
    ///
    /// # Arguments
    ///
    /// * `level` - The level, `host`, `rack` or `zone`
    #[must_use]
    pub fn spread(&self, level: &str) -> usize {
        self.members
            .iter()
            .filter(|member| member.phase != "removed")
            .map(|member| {
                // the level the name came from and the name, or the member alone
                let domains = member.domains();
                domain_at(&domains, level)
                    .map(|(named, name)| (named.to_string(), name.to_string()))
                    .ok_or_else(|| member.node.clone())
            })
            .collect::<std::collections::BTreeSet<Result<(String, String), String>>>()
            .len()
    }

    /// Whether any member names a level, which is when the tab shows it
    ///
    /// # Arguments
    ///
    /// * `level` - The level, `rack` or `zone`
    #[must_use]
    pub fn names(&self, level: &str) -> bool {
        self.members
            .iter()
            .any(|member| member.domains().contains_key(level))
    }

    /// The levels above the host the tab shows, widest last as its columns read
    fn shown_levels(&self) -> Vec<&'static str> {
        // a level nobody names would be a column of dashes, so it is left out
        [RACK, ZONE]
            .into_iter()
            .filter(|level| self.names(level))
            .collect()
    }

    /// Every member's storage roots, one line each, under a header
    ///
    /// A root's figures are its filesystem's, so two roots on one device show the same size and
    /// free bytes and the same device ([F82](../../../docs/src/features/root-capacity.md)).
    #[must_use]
    pub fn root_lines(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "{:<36} {:<28} {:<26} {:<8} {:>10} {:>10} {:>5}",
            "roots", "path", "uses", "device", "size", "free", "used"
        )];
        for member in &self.members {
            for root in &member.roots {
                // the share of the filesystem in use, when its size is known
                let used = root
                    .size_bytes
                    .checked_sub(root.free_bytes)
                    .filter(|_| root.size_bytes > 0)
                    .map_or("-".to_string(), |used| {
                        format!("{}%", used.saturating_mul(100) / root.size_bytes)
                    });
                lines.push(format!(
                    "{:<36} {:<28} {:<26} {:<8} {:>10} {:>10} {:>5}",
                    member.node,
                    if root.path.is_empty() {
                        "-"
                    } else {
                        &root.path
                    },
                    root.uses_label(),
                    if root.device.is_empty() {
                        "-"
                    } else {
                        &root.device
                    },
                    bytes(root.size_bytes),
                    bytes(root.free_bytes),
                    used
                ));
            }
        }
        lines
    }

    /// Every member's devices, one line each, under a header
    ///
    /// A device's figures are its filesystem's, as a root's are; its slices' chunks and staged
    /// bytes are summed, and a failed one says why
    /// ([F85](../../../docs/src/features/devices-and-pools.md)).
    #[must_use]
    pub fn device_lines(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "{:<36} {:<28} {:<6} {:>6} {:<8} {:>10} {:>10} {:>10} {:>8}  {}",
            "devices", "path", "class", "slices", "device", "size", "free", "staged", "chunks", "health"
        )];
        for member in &self.members {
            for device in &member.devices {
                // what its slices hold, summed
                let chunks: u64 = device.slices.iter().map(|slice| slice.chunks).sum();
                let staged: u64 = device.slices.iter().map(|slice| slice.staged_bytes).sum();
                let mut health = match &device.health {
                    shoal::shared::devices::DeviceHealth::Serving if device.short_of_space() => {
                        "serving, short of space".to_string()
                    }
                    shoal::shared::devices::DeviceHealth::Serving => "serving".to_string(),
                    shoal::shared::devices::DeviceHealth::Failed { reason } => format!("failed: {reason}"),
                };
                // what its operator is warned of rides beside it
                for warning in &device.warnings {
                    health.push_str(&format!("; {warning}"));
                }
                lines.push(format!(
                    "{:<36} {:<28} {:<6} {:>6} {:<8} {:>10} {:>10} {:>10} {:>8}  {}",
                    member.node,
                    device.path,
                    device.class,
                    device.slices.len(),
                    if device.device.is_empty() { "-" } else { &device.device },
                    bytes(device.size_bytes),
                    bytes(device.free_bytes),
                    bytes(staged),
                    chunks,
                    health
                ));
            }
        }
        lines
    }

    /// The model as the lines the tab draws
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        lines.push(format!(
            "cluster {} via {} at version {}",
            self.cluster, self.node, self.version
        ));
        // the hosts always, and a rack or zone count only once a member names that level (F83)
        let spread: String = self
            .shown_levels()
            .into_iter()
            .map(|level| format!("  {level}s {}", self.spread(level)))
            .collect();
        lines.push(format!(
            "leader {}  voters {}  learners {}  up {}  hosts {}{spread}  tombstones {}",
            self.leader.as_deref().unwrap_or("none"),
            self.voters,
            self.learners,
            self.up_members,
            self.hosts(),
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
        // a column for each level above the host that a member names, after the host's
        let levels = self.shown_levels();
        let above = |row: Option<&MemberRow>| -> String {
            levels
                .iter()
                .map(|level| {
                    let name = match (row, *level) {
                        (None, level) => Some(level),
                        (Some(member), RACK) => member.rack.as_deref(),
                        (Some(member), _) => member.zone.as_deref(),
                    };
                    format!("{:<12} ", name.unwrap_or("-"))
                })
                .collect()
        };
        lines.push(format!(
            "{:<36} {:<12} {}{:<7} {:<8} {:<9} {:>4} {:>8} {:>10} {:>10} {:>4}  {}",
            "member",
            "host",
            above(None),
            "role",
            "health",
            "phase",
            "inc",
            "grace",
            "free",
            "held",
            "wire",
            "client"
        ));
        for member in &self.members {
            lines.push(format!(
                "{:<36} {:<12} {}{:<7} {:<8} {:<9} {:>4} {:>8} {:>10} {:>10} {:>4}  {}",
                member.node,
                member.host.as_deref().unwrap_or("-"),
                above(Some(member)),
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
        // every member's roots, when any member reports them (F82)
        if self.members.iter().any(|member| !member.roots.is_empty()) {
            lines.push(String::new());
            lines.extend(self.root_lines());
        }
        // and their devices, when any member claimed one (F85)
        if self.members.iter().any(|member| !member.devices.is_empty()) {
            lines.push(String::new());
            lines.extend(self.device_lines());
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
        // the figures the last poll read, if the node answers them
        lines.extend(self.stats.iter().cloned());
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
pub(crate) fn bytes(bytes: u64) -> String {
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
            "cluster": "c1", "node": "n0", "version": 17, "leader": "n0", "voters": ["n0", "n1"], "learners": ["n2"],
            "desired_rf": 3, "active_rf": 2, "up_members": 2, "under_replicated_sets": 4,
            "tombstones": { "n9": { "incarnation": 1, "removed_at": 12 } },
            "wire": { "activated": 5, "floor": 4, "newest": 5, "min_member": 4, "max_member": 5 },
            "members": [
                { "record": { "node": "n0", "incarnation": 2, "wire_max": 5, "client": "127.0.0.1:1", "failure_domains": { "host": "europa" } }, "role": "voter", "health": "up", "phase": "member", "weight": 2, "free_bytes": 2048, "held_bytes": 1024,
                  "roots": [
                      { "path": "/optane/shoal", "device": "259:1", "size_bytes": 8192, "free_bytes": 4096, "uses": ["primary", "latency"] },
                      { "path": "/hdd/shoal", "device": "8:1", "size_bytes": 10240, "free_bytes": 2048, "uses": ["throughput"] }
                  ] },
                { "record": { "node": "n1", "incarnation": 1, "wire_max": 4, "client": "127.0.0.1:2", "failure_domains": { "host": "europa" } }, "role": "voter", "health": "down", "phase": "member", "weight": 1, "grace_remaining_ms": 61000 },
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
        // the host each stands on, and a member naming none stands alone (F81)
        assert_eq!(model.members[0].host.as_deref(), Some("europa"));
        assert_eq!(model.members[2].host, None);
        assert_eq!(model.hosts(), 2);
        // every root a member reported, and none from a member that reported none (F82)
        assert_eq!(model.members[0].roots.len(), 2);
        assert_eq!(model.members[0].roots[1].path, "/hdd/shoal");
        assert!(model.members[1].roots.is_empty());
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
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("n0") && line.contains(" europa ")),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("hosts 2")),
            "{lines:?}"
        );
        // nobody names a rack or a zone, so neither is counted or given a column (F83)
        assert!(!model.names("rack") && !model.names("zone"));
        assert!(
            !lines
                .iter()
                .any(|line| line.contains("racks") || line.contains(" rack ")),
            "{lines:?}"
        );
        // two members in one rack, the third naming nothing: two racks, counted as the planner
        // counts them, a rack column, and still no zone
        let mut racked = model.clone();
        racked.members[0].rack = Some("shelf-1".to_string());
        racked.members[1].rack = Some("shelf-1".to_string());
        assert_eq!((racked.spread("rack"), racked.spread("zone")), (2, 2));
        let racked_lines = racked.render_lines();
        assert!(
            racked_lines
                .iter()
                .any(|line| line.contains("hosts 2  racks 2  tombstones")),
            "{racked_lines:?}"
        );
        assert!(
            racked_lines
                .iter()
                .any(|line| line.starts_with("member") && line.contains(" rack ")),
            "{racked_lines:?}"
        );
        assert!(
            racked_lines
                .iter()
                .any(|line| line.starts_with("n0") && line.contains(" shelf-1 ")),
            "{racked_lines:?}"
        );
        assert!(
            !racked_lines.iter().any(|line| line.contains("zone")),
            "{racked_lines:?}"
        );
        // the record's rack and zone are read from the frame: both members on europa in one hall,
        // and the member naming nothing a zone of its own
        let mut zoned = members.clone();
        zoned["members"][0]["record"]["failure_domains"]["zone"] = json!("hall");
        zoned["members"][1]["record"]["failure_domains"]["zone"] = json!("hall");
        let zoned = ClusterModel::from_frames(
            &zoned,
            &readiness,
            &replication,
            &plans,
            &backups,
            &recoveries,
        );
        assert_eq!(zoned.members[0].zone.as_deref(), Some("hall"));
        assert!(zoned
            .render_lines()
            .iter()
            .any(|line| line.contains("hosts 2  zones 2")));
        // each root on a line of its own, its share used beside its figures
        let roots: Vec<&String> = lines
            .iter()
            .filter(|line| line.starts_with("n0") && line.contains("/"))
            .collect();
        assert_eq!(roots.len(), 2, "{lines:?}");
        assert!(
            roots[0].contains("primary,latency")
                && roots[0].contains("259:1")
                && roots[0].ends_with("50%"),
            "{roots:?}"
        );
        assert!(
            roots[1].contains("throughput")
                && roots[1].contains("8:1")
                && roots[1].ends_with("80%"),
            "{roots:?}"
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
        // and a member from before every root was reported shows no roots section
        let unrooted = ClusterModel::from_frames(
            &json!({ "cluster": "c", "members": [ { "record": { "node": "n0" }, "free_bytes": 7 } ] }),
            &json!({}),
            &json!({}),
            &json!([]),
            &json!([]),
            &json!([]),
        );
        assert_eq!(unrooted.members[0].free_bytes, Some(7));
        assert!(unrooted.members[0].roots.is_empty());
        assert!(
            !unrooted
                .render_lines()
                .iter()
                .any(|line| line.starts_with("roots"))
        );
        assert_eq!(older.headline(), "0 of 0 copies; writes unknown");
        assert!(
            older
                .render_lines()
                .iter()
                .any(|line| line == "no open plans")
        );
    }
}

#[cfg(test)]
mod voter_tests {
    use super::ClusterModel;
    use shoal::serde_json::{json, Value};

    /// The control group's voters and learners are counted from the lists the frame carries
    ///
    /// `TopologyView` serializes `voters` and `learners` as the node ids themselves, not as
    /// counts, so a model that read them as numbers saw a cluster of any size as having no
    /// voters (item 114).
    #[test]
    fn voters_and_learners_are_counted_from_the_lists_the_frame_carries() {
        // the shape `serde_json::to_value(TopologyView)` writes
        let members = json!({
            "voters": ["n0", "n1", "n2"], "learners": ["n3"],
            "members": []
        });
        let model = ClusterModel::from_frames(
            &members,
            &Value::Null,
            &Value::Null,
            &Value::Null,
            &Value::Null,
            &Value::Null,
        );
        assert_eq!((model.voters, model.learners), (3, 1));
    }
}

#[cfg(test)]
mod device_tests {
    use super::ClusterModel;
    use shoal::serde_json::{json, Value};

    /// A member's devices are read from the members frame and drawn a line each under the
    /// roots, a failed one with its reason and a warned one with its warning; a frame without
    /// any draws no devices section (F85)
    #[test]
    fn devices_are_drawn_a_line_each() {
        // two devices as `DeviceReport` serializes them: one serving and warned, one failed
        let members = json!({
            "members": [{
                "record": { "node": "n0" },
                "devices": [
                    {
                        "id": "0199d6f0-0000-7000-8000-000000000001", "path": "/xfs/pool", "class": "ssd",
                        "slices": [
                            { "id": "0199d6f0-0000-7000-8000-000000000002", "index": 0, "chunks": 3, "staged_bytes": 4096 },
                            { "id": "0199d6f0-0000-7000-8000-000000000003", "index": 1, "chunks": 2, "staged_bytes": 0 }
                        ],
                        "size_bytes": 1u64 << 30, "free_bytes": 1u64 << 29, "reserve_bytes": 1u64 << 20,
                        "device": "259:2", "filesystem": "xfs", "rotational": true,
                        "warnings": ["the kernel calls it rotational"],
                        "health": { "state": "serving" }
                    },
                    {
                        "id": "0199d6f0-0000-7000-8000-000000000004", "path": "/hdd/pool", "class": "hdd",
                        "size_bytes": 0, "free_bytes": 0,
                        "health": { "state": "failed", "reason": "missing at start" }
                    }
                ]
            }]
        });
        let model = ClusterModel::from_frames(&members, &Value::Null, &Value::Null, &Value::Null, &Value::Null, &Value::Null);
        assert_eq!(model.members[0].devices.len(), 2);
        let lines = model.render_lines();
        let at = lines.iter().position(|line| line.starts_with("devices")).expect("a devices section");
        // the slices' chunks and staged bytes summed, the warning beside its health
        assert!(lines[at + 1].contains("/xfs/pool") && lines[at + 1].contains(" 2 "), "{}", lines[at + 1]);
        assert!(lines[at + 1].contains("4.0KiB") && lines[at + 1].contains(" 5  serving; the kernel"), "{}", lines[at + 1]);
        assert!(lines[at + 2].contains("failed: missing at start"), "{}", lines[at + 2]);
        // a frame with no devices draws no section
        let none = ClusterModel::from_frames(
            &json!({ "members": [{ "record": { "node": "n0" } }] }),
            &Value::Null,
            &Value::Null,
            &Value::Null,
            &Value::Null,
            &Value::Null,
        );
        assert!(!none.render_lines().iter().any(|line| line.starts_with("devices")));
    }
}
