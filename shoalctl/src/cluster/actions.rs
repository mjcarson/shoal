//! The operations the cluster tab's command line takes, their previews and their follow-ups

use shoal::serde_json::Value;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::AdminKind;
use uuid::Uuid;

/// How a submitted operation is followed once it has been applied
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Follow {
    /// By its plan record
    Plan,
    /// By its repair record
    Repair,
    /// By its backup record
    Backup,
    /// By its restore record
    Restore,
    /// By its move record
    Move,
    /// Nothing to follow: the answer is the whole outcome
    None,
}

/// One operation an operator typed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAction {
    /// Drain a member and take it out of the cluster
    Decommission {
        /// The member
        node: NodeId,
    },
    /// Remove a member the cluster has given up on, onto a replacement if one is named
    Remove {
        /// The member
        node: NodeId,
        /// The member its copies go to, or wherever the planner puts them
        replacement: Option<NodeId>,
    },
    /// Hold or resume a down member's grace
    Maintenance {
        /// The member
        node: NodeId,
        /// Whether to hold it
        suspend: bool,
    },
    /// Spread the sets over the members by weight and bytes
    Rebalance,
    /// Verify or repair a table
    Repair {
        /// The table, by name
        table: String,
        /// `verify` or `repair`
        mode: String,
    },
    /// Back a table, or every table, up under a directory
    Backup {
        /// The table, or none for every table
        table: Option<String>,
        /// The directory on every node that writes
        path: String,
    },
    /// Restore a backup into this cluster
    Restore {
        /// The directory
        path: String,
    },
    /// Activate a wire version
    Activate {
        /// The version
        wire: u8,
    },
    /// Read an operation's record
    Status {
        /// The operation
        op: Uuid,
    },
    /// Read this node's certificate again
    ReloadTls,
}

impl ClusterAction {
    /// Parse a command line
    ///
    /// # Arguments
    ///
    /// * `line` - What was typed
    ///
    /// # Errors
    ///
    /// Says what was wrong with the line, for the error box.
    pub fn parse(line: &str) -> Result<Self, String> {
        let mut words = line.split_whitespace();
        let verb = words.next().ok_or_else(|| "type an operation; `help` lists them".to_string())?;
        let node = |word: Option<&str>| -> Result<NodeId, String> {
            let word = word.ok_or_else(|| format!("{verb} needs a node id"))?;
            word.parse::<Uuid>().map(NodeId).map_err(|_| format!("{word} is not a node id"))
        };
        let action = match verb {
            "decommission" => ClusterAction::Decommission { node: node(words.next())? },
            "remove" => {
                let removed = node(words.next())?;
                let replacement = match words.next() {
                    Some(word) => Some(node(Some(word))?),
                    None => None,
                };
                ClusterAction::Remove { node: removed, replacement }
            }
            "maintenance" => {
                let member = node(words.next())?;
                let suspend = match words.next() {
                    Some("on") => true,
                    Some("off") => false,
                    other => return Err(format!("maintenance takes `on` or `off`, not {other:?}")),
                };
                ClusterAction::Maintenance { node: member, suspend }
            }
            "rebalance" => ClusterAction::Rebalance,
            "repair" => {
                let table = words.next().ok_or_else(|| "repair needs a table name".to_string())?.to_string();
                let mode = words.next().unwrap_or("verify");
                if mode != "verify" && mode != "repair" {
                    return Err(format!("repair takes `verify` or `repair`, not {mode:?}"));
                }
                ClusterAction::Repair { table, mode: mode.to_string() }
            }
            "backup" => {
                let first = words.next().ok_or_else(|| "backup needs a directory".to_string())?.to_string();
                match words.next() {
                    Some(path) => ClusterAction::Backup { table: Some(first), path: path.to_string() },
                    None => ClusterAction::Backup { table: None, path: first },
                }
            }
            "restore" => ClusterAction::Restore {
                path: words.next().ok_or_else(|| "restore needs a directory".to_string())?.to_string(),
            },
            "activate" => {
                let word = words.next().ok_or_else(|| "activate needs a wire version".to_string())?;
                ClusterAction::Activate {
                    wire: word.parse().map_err(|_| format!("{word} is not a wire version"))?,
                }
            }
            "status" => {
                let word = words.next().ok_or_else(|| "status needs an operation id".to_string())?;
                ClusterAction::Status {
                    op: word.parse().map_err(|_| format!("{word} is not an operation id"))?,
                }
            }
            "reload-tls" | "reload_tls" => ClusterAction::ReloadTls,
            other => return Err(format!("{other} is not an operation; `help` lists them")),
        };
        if words.next().is_some() {
            return Err(format!("{verb} takes fewer arguments than that"));
        }
        Ok(action)
    }

    /// The operations the command line takes, for `help`
    #[must_use]
    pub fn help() -> Vec<String> {
        vec![
            "decommission <node>            drain a member and take it out".to_string(),
            "remove <node> [replacement]    remove a member the cluster gave up on".to_string(),
            "maintenance <node> on|off      hold or resume a down member's grace".to_string(),
            "rebalance                      spread the sets by weight and bytes".to_string(),
            "repair <table> [verify|repair] scrub a table, or repair it".to_string(),
            "backup [table] <dir>           back a table or every table up".to_string(),
            "restore <dir>                  restore a backup into this empty cluster".to_string(),
            "activate <wire>                activate a wire version".to_string(),
            "status <op>                    read an operation's record".to_string(),
            "reload-tls                     read this node's certificate again".to_string(),
            String::new(),
            "Enter previews an operation; Enter again submits it. Esc forgets it.".to_string(),
        ]
    }

    /// Whether this action changes the cluster, and so is previewed before it is sent
    #[must_use]
    pub fn is_mutation(&self) -> bool {
        !matches!(self, ClusterAction::Status { .. })
    }

    /// What the operation touches, what will move, and what cannot be undone
    ///
    /// # Arguments
    ///
    /// * `model` - The cluster as the tab last saw it, for the names and figures
    #[must_use]
    pub fn preview(&self, model: &super::ClusterModel) -> Vec<String> {
        let member = |node: &NodeId| {
            let id = node.to_string();
            model
                .members
                .iter()
                .find(|member| member.node == id)
                .map_or_else(
                    || format!("{id} (not a member this node knows)"),
                    |member| format!("{id} ({} {} {}, weight {})", member.role, member.health, member.phase, member.weight),
                )
        };
        match self {
            ClusterAction::Decommission { node } => vec![
                format!("decommission {}", member(node)),
                "moves: every set it holds, to the members the planner picks, one at a time".to_string(),
                "boundary: once every set has moved the identity is tombstoned and never returns".to_string(),
            ],
            ClusterAction::Remove { node, replacement } => vec![
                format!("remove {}", member(node)),
                match replacement {
                    Some(replacement) => format!("moves: every set it holds, onto {}", member(replacement)),
                    None => "moves: every set it holds, to the members the planner picks".to_string(),
                },
                "boundary: the identity is tombstoned and never returns; a directory it left is evidence only".to_string(),
            ],
            ClusterAction::Maintenance { node, suspend } => vec![
                format!("maintenance {} {}", if *suspend { "on" } else { "off" }, member(node)),
                "moves: nothing".to_string(),
                if *suspend {
                    "boundary: none; the grace is held until turned off, and the member is not removed meanwhile".to_string()
                } else {
                    "boundary: the grace resumes from its committed count and expires into a removal".to_string()
                },
            ],
            ClusterAction::Rebalance => vec![
                format!("rebalance over {} members at factor {}", model.members.len(), model.desired_rf),
                "moves: the sets the planner finds over their member's target, one per member at a time".to_string(),
                "boundary: none; a move is a copy replaced, and the record says which".to_string(),
            ],
            ClusterAction::Repair { table, mode } => vec![
                format!("repair {table} in {mode} mode"),
                "moves: nothing; every group's leader scrubs it".to_string(),
                if mode == "repair" {
                    "boundary: a divergent copy is overwritten from the majority's state; the record keeps what it held".to_string()
                } else {
                    "boundary: none; a verify installs nothing".to_string()
                },
            ],
            ClusterAction::Backup { table, path } => vec![
                format!("backup {} under {path}", table.as_deref().unwrap_or("every table")),
                "moves: nothing; every group's leader cuts and copies a file on its own disk".to_string(),
                format!("boundary: none; needs wire version 5 activated (activated {})", model.activated_wire),
            ],
            ClusterAction::Restore { path } => vec![
                format!("restore {path} into cluster {}", model.cluster),
                "moves: every group's members install the files' records under a quarantine".to_string(),
                "boundary: once, into an empty cluster; the source cluster's identities are refused from here on".to_string(),
            ],
            ClusterAction::Activate { wire } => vec![
                format!("activate wire version {wire} (activated {}, members speak {}..={})", model.activated_wire, model.wire_range.0, model.wire_range.1),
                "moves: nothing".to_string(),
                "boundary: no member rolls back below it; a build that cannot speak it is refused at every door".to_string(),
            ],
            ClusterAction::Status { op } => vec![format!("read the record of {op}")],
            ClusterAction::ReloadTls => vec![
                format!("reload the certificate, key and authority of {}", model.node),
                "moves: nothing; every handshake after uses the new material and none is dropped".to_string(),
                "boundary: none; material that does not parse changes nothing".to_string(),
            ],
        }
    }

    /// The request this action sends, and how its outcome is followed
    #[must_use]
    pub fn request(&self) -> (AdminKind, Follow) {
        match self {
            ClusterAction::Decommission { node } => (AdminKind::Decommission { node: *node }, Follow::Plan),
            ClusterAction::Remove { node, replacement } => (
                AdminKind::Remove {
                    node: *node,
                    replacement: *replacement,
                },
                Follow::Plan,
            ),
            ClusterAction::Maintenance { node, suspend } => (
                AdminKind::Maintenance {
                    node: *node,
                    suspend: *suspend,
                },
                Follow::None,
            ),
            ClusterAction::Rebalance => (AdminKind::Rebalance, Follow::Plan),
            ClusterAction::Repair { table, mode } => (
                AdminKind::Repair {
                    table: table.clone(),
                    tablet: None,
                    mode: mode.clone(),
                    source: None,
                    release: false,
                },
                Follow::Repair,
            ),
            ClusterAction::Backup { table, path } => (
                AdminKind::Backup {
                    table: table.clone(),
                    path: path.clone(),
                },
                Follow::Backup,
            ),
            ClusterAction::Restore { path } => (AdminKind::Restore { path: path.clone() }, Follow::Restore),
            ClusterAction::Activate { wire } => (AdminKind::Activate { wire: *wire }, Follow::None),
            ClusterAction::Status { op } => (AdminKind::PlanStatus { op: *op }, Follow::None),
            ClusterAction::ReloadTls => (AdminKind::ReloadTls, Follow::None),
        }
    }
}

impl Follow {
    /// The read that follows an operation by its record
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    #[must_use]
    pub fn status(self, op: Uuid) -> Option<AdminKind> {
        match self {
            Follow::Plan => Some(AdminKind::PlanStatus { op }),
            Follow::Repair => Some(AdminKind::RepairStatus { op }),
            Follow::Backup => Some(AdminKind::BackupStatus { op }),
            Follow::Restore => Some(AdminKind::RestoreStatus { op }),
            Follow::Move => Some(AdminKind::MoveStatus { op }),
            Follow::None => None,
        }
    }

    /// Whether a record says the operation is done
    ///
    /// # Arguments
    ///
    /// * `record` - The record read
    #[must_use]
    pub fn is_done(self, record: &Value) -> bool {
        match self {
            Follow::Plan => !record["outcome"].is_null(),
            Follow::Repair | Follow::Backup | Follow::Restore => record["groups"]
                .as_object()
                .is_some_and(|groups| !groups.is_empty() && groups.values().all(|group| group["phase"] == "Done")),
            Follow::Move => record["phase"] == "Done",
            Follow::None => true,
        }
    }

    /// A record as the lines the tab draws while following it
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `record` - The record read
    #[must_use]
    pub fn render(self, op: Uuid, record: &Value) -> Vec<String> {
        let mut lines = vec![format!("following {op}: {}", if self.is_done(record) { "done" } else { "running" })];
        match self {
            Follow::Plan => {
                lines.push(format!("phase {}", record["phase"].as_str().unwrap_or_default()));
                if let Some(reason) = record["blocked"]["reason"].as_str() {
                    lines.push(format!("blocked: {reason}"));
                }
                for step in record["steps"].as_array().into_iter().flatten() {
                    lines.push(format!(
                        "  tablet {} {} -> {} {}",
                        step["tablet"],
                        step["from"].as_str().unwrap_or_default(),
                        step["to"].as_str().unwrap_or_default(),
                        step["state"].as_str().map_or_else(|| step["state"].to_string(), str::to_string)
                    ));
                }
                if !record["outcome"].is_null() {
                    lines.push(format!("outcome {}", record["outcome"]));
                }
            }
            Follow::Repair | Follow::Backup | Follow::Restore | Follow::Move => {
                for (group, progress) in record["groups"].as_object().into_iter().flatten() {
                    lines.push(format!(
                        "  {group} {} {}",
                        progress["phase"].as_str().map_or_else(|| progress["phase"].to_string(), str::to_string),
                        progress["outcome"]
                    ));
                }
            }
            Follow::None => {}
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterAction, Follow};
    use crate::cluster::model::{ClusterModel, MemberRow};
    use shoal::serde_json::json;
    use shoal::shared::identity::NodeId;
    use shoal::shared::protocol::admin::AdminKind;
    use uuid::Uuid;

    /// Every operation parses from its line and refuses a malformed one by name, previews the
    /// identity it touches, what moves and its boundary, sends the request it names, and is
    /// followed by the record that says when it is done
    #[test]
    fn an_action_previews_its_boundary_and_follows_its_record() {
        let node = NodeId::from(7);
        let other = NodeId::from(9);
        let model = ClusterModel {
            cluster: "c1".to_string(),
            node: "n0".to_string(),
            desired_rf: 3,
            activated_wire: 4,
            wire_range: (4, 5),
            members: vec![MemberRow {
                node: node.to_string(),
                role: "voter".to_string(),
                health: "up".to_string(),
                phase: "member".to_string(),
                incarnation: 1,
                grace_remaining_ms: None,
                weight: 2,
                free_bytes: None,
                held_bytes: None,
                wire_max: 5,
                client: "127.0.0.1:1".to_string(),
            }],
            ..ClusterModel::default()
        };
        // the parses
        let decommission = ClusterAction::parse(&format!("decommission {node}")).expect("parses");
        assert_eq!(decommission, ClusterAction::Decommission { node });
        assert_eq!(
            ClusterAction::parse(&format!("remove {node} {other}")).expect("parses"),
            ClusterAction::Remove { node, replacement: Some(other) }
        );
        assert_eq!(ClusterAction::parse(&format!("maintenance {node} on")).expect("parses"), ClusterAction::Maintenance { node, suspend: true });
        assert_eq!(ClusterAction::parse("rebalance").expect("parses"), ClusterAction::Rebalance);
        assert_eq!(ClusterAction::parse("repair Note").expect("parses"), ClusterAction::Repair { table: "Note".to_string(), mode: "verify".to_string() });
        assert_eq!(ClusterAction::parse("backup /b").expect("parses"), ClusterAction::Backup { table: None, path: "/b".to_string() });
        assert_eq!(ClusterAction::parse("backup Note /b").expect("parses"), ClusterAction::Backup { table: Some("Note".to_string()), path: "/b".to_string() });
        assert_eq!(ClusterAction::parse("restore /b/x").expect("parses"), ClusterAction::Restore { path: "/b/x".to_string() });
        assert_eq!(ClusterAction::parse("activate 5").expect("parses"), ClusterAction::Activate { wire: 5 });
        let op = Uuid::new_v4();
        assert_eq!(ClusterAction::parse(&format!("status {op}")).expect("parses"), ClusterAction::Status { op });
        assert_eq!(ClusterAction::parse("reload-tls").expect("parses"), ClusterAction::ReloadTls);
        // the refusals name what was wrong
        assert!(ClusterAction::parse("").unwrap_err().contains("help"));
        assert!(ClusterAction::parse("decommission nope").unwrap_err().contains("not a node id"));
        assert!(ClusterAction::parse(&format!("maintenance {node} maybe")).unwrap_err().contains("on"));
        assert!(ClusterAction::parse("repair Note sideways").unwrap_err().contains("verify"));
        assert!(ClusterAction::parse("activate five").unwrap_err().contains("wire version"));
        assert!(ClusterAction::parse("frobnicate").unwrap_err().contains("not an operation"));
        assert!(ClusterAction::parse("rebalance now").unwrap_err().contains("fewer"));
        // the previews: the identity as the model knows it, the movement and the boundary
        let preview = decommission.preview(&model);
        assert!(preview[0].contains(&node.to_string()) && preview[0].contains("voter up member, weight 2"), "{preview:?}");
        assert!(preview[1].starts_with("moves:") && preview[2].starts_with("boundary:"), "{preview:?}");
        assert!(preview[2].contains("tombstoned"), "{preview:?}");
        let unknown = ClusterAction::Remove { node: other, replacement: None }.preview(&model);
        assert!(unknown[0].contains("not a member this node knows"), "{unknown:?}");
        let activate = ClusterAction::Activate { wire: 5 }.preview(&model);
        assert!(activate[0].contains("activated 4") && activate[0].contains("4..=5"), "{activate:?}");
        assert!(ClusterAction::Restore { path: "/b".to_string() }.preview(&model)[2].contains("once"));
        assert_eq!(ClusterAction::Status { op }.preview(&model).len(), 1);
        assert!(ClusterAction::Status { op }.is_mutation() == false && decommission.is_mutation());
        // the requests and their follow-ups
        assert_eq!(decommission.request(), (AdminKind::Decommission { node }, Follow::Plan));
        assert_eq!(ClusterAction::Rebalance.request().1, Follow::Plan);
        assert_eq!(ClusterAction::Repair { table: "Note".to_string(), mode: "verify".to_string() }.request().1, Follow::Repair);
        assert_eq!(ClusterAction::Backup { table: None, path: "/b".to_string() }.request().1, Follow::Backup);
        assert_eq!(ClusterAction::Activate { wire: 5 }.request(), (AdminKind::Activate { wire: 5 }, Follow::None));
        assert_eq!(ClusterAction::ReloadTls.request(), (AdminKind::ReloadTls, Follow::None));
        assert_eq!(Follow::Plan.status(op), Some(AdminKind::PlanStatus { op }));
        assert_eq!(Follow::None.status(op), None);
        // a record says when it is done, and renders as lines while it runs
        let running = json!({ "phase": "Running", "outcome": null, "steps": [ { "tablet": 0, "from": "a", "to": "b", "state": "Moving" } ], "blocked": null });
        assert!(!Follow::Plan.is_done(&running));
        let lines = Follow::Plan.render(op, &running);
        assert!(lines[0].contains("running") && lines.iter().any(|line| line.contains("tablet 0 a -> b Moving")), "{lines:?}");
        let done = json!({ "phase": "Done", "outcome": "Completed", "steps": [], "blocked": null });
        assert!(Follow::Plan.is_done(&done));
        assert!(Follow::Plan.render(op, &done).iter().any(|line| line.contains("outcome")));
        let groups = json!({ "groups": { "g1": { "phase": "Done", "outcome": { "Written": {} } }, "g2": { "phase": "Cutting", "outcome": null } } });
        assert!(!Follow::Backup.is_done(&groups));
        assert!(Follow::Backup.render(op, &groups).len() == 3);
        assert!(Follow::Backup.is_done(&json!({ "groups": { "g1": { "phase": "Done" } } })));
        assert!(!Follow::Repair.is_done(&json!({ "groups": {} })));
        assert!(ClusterAction::help().iter().any(|line| line.starts_with("decommission")));
    }
}
