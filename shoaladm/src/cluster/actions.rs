//! The operations the cluster tab's command line takes, their previews and their follow-ups

use shoal::serde_json::Value;
use shoal::shared::grants::{GrantOn, Grants, Privileges, EVERY_BUCKET, EVERY_TABLE};
use shoal::shared::pool_placement::PoolReadiness;
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
    /// By every pool's scrub figures, until no tablet group of a pool asked to scrub is pending
    /// ([F89](../../../docs/src/features/scrub-and-repair.md))
    Scrub(shoal::shared::scrub::ScrubKind),
    /// Nothing to follow: the answer is the whole outcome
    None,
}

/// One operation an operator typed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAction {
    /// Place every tablet over these members, in this order, once
    Initialize {
        /// The members, in the order the tablets are dealt over them
        nodes: Vec<NodeId>,
    },
    /// Drain a member and take it out of the cluster
    Decommission {
        /// The member
        node: NodeId,
    },
    /// Fail a device as an I/O error would, so its placement groups move off it
    /// ([F88](../../../docs/src/features/recovery-and-moves.md))
    FailDevice {
        /// The device
        device: shoal::shared::devices::DeviceId,
    },
    /// Drain a device: its placement groups move off it while it serves their copies, and what
    /// they leave on it is released ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md))
    DrainDevice {
        /// The device
        device: shoal::shared::devices::DeviceId,
    },
    /// Remove a device for good once it holds nothing a group reads, or on the word that it is lost
    RemoveDevice {
        /// The device
        device: shoal::shared::devices::DeviceId,
        /// Whether it is gone, whatever it held
        lost: bool,
    },
    /// Give a device a weight its placement groups move by
    ReweightDevice {
        /// The device
        device: shoal::shared::devices::DeviceId,
        /// Its share of a pool
        weight: u64,
    },
    /// Scrub a pool's placement groups now, light or deep, whatever their schedule
    /// ([F89](../../../docs/src/features/scrub-and-repair.md))
    Scrub {
        /// The pool
        pool: String,
        /// Light or deep
        kind: shoal::shared::scrub::ScrubKind,
        /// A bucket, a device or a slice the pass is narrowed to, or none for the whole pool
        /// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md))
        target: Option<shoal::shared::scrub::ScrubTarget>,
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
    /// Drive a finished restore's failed groups again
    /// ([Resolved #155](../../../docs/src/appendix/resolved/restore-retry.md))
    RetryRestore {
        /// The restore operation
        restore: Uuid,
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
    /// Grant a principal privileges on a table or a bucket
    /// ([F77](../../../docs/src/features/authorization.md),
    /// [F79](../../../docs/src/features/bucket-schema.md))
    Grant {
        /// The principal, by the name it authenticates as
        principal: String,
        /// Whether the grant is on a table or a bucket
        on: GrantOn,
        /// The table or bucket as the schema spells it, or `*` for every one
        name: String,
        /// What is granted
        privileges: Privileges,
    },
    /// Take back privileges a grant on a table or a bucket gave a principal
    Revoke {
        /// The principal, by the name it authenticates as
        principal: String,
        /// Whether the grant was on a table or a bucket
        on: GrantOn,
        /// The name the grant was made on, or `*`
        name: String,
        /// What is taken back
        privileges: Privileges,
    },
    /// Read the grants the cluster holds every principal to
    Grants,
    /// Read every pool's readiness: its devices, failure domains and free bytes against what its
    /// policy needs ([F85](../../../docs/src/features/devices-and-pools.md))
    Pools,
    /// Extend the cluster's committed schema to one that appends tables or buckets to it, which
    /// the node asked has to run ([F93](../../../docs/src/features/operations-and-the-real-cluster.md))
    ExtendSchema {
        /// The schema to extend to, by its structural id
        schema: u64,
    },
    /// An operator's word on a stripe whose row is behind its chunks (F93)
    ResolveStripe {
        /// The bucket
        bucket: String,
        /// The object's path
        path: String,
        /// The stripe
        stripe: u64,
        /// Accept what its holders agree on, or declare it lost
        resolution: shoal::shared::protocol::admin::StripeResolution,
        /// The sequence its row is at, which the refusal named and the resolution is written against
        sequence: u64,
    },
    /// Read what each bucket holds and what its clients were answered (F93)
    Buckets {
        /// One bucket, or every one
        bucket: Option<String>,
    },
    /// Read every member's devices, with their slices and health (F93)
    Devices,
}

/// A device id an operation names, refused by name when missing or malformed
///
/// # Arguments
///
/// * `verb` - The operation, for the refusal
/// * `word` - The word that should be the id
fn device_id(verb: &str, word: Option<&str>) -> Result<shoal::shared::devices::DeviceId, String> {
    let word = word.ok_or_else(|| format!("{verb} needs a device id; `devices` lists them"))?;
    word.parse::<Uuid>()
        .map(shoal::shared::devices::DeviceId)
        .map_err(|_| format!("{word} is not a device id"))
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
        let verb = words
            .next()
            .ok_or_else(|| "type an operation; `help` lists them".to_string())?;
        let node = |word: Option<&str>| -> Result<NodeId, String> {
            let word = word.ok_or_else(|| format!("{verb} needs a node id"))?;
            word.parse::<Uuid>()
                .map(NodeId)
                .map_err(|_| format!("{word} is not a node id"))
        };
        let action = match verb {
            "initialize" => {
                // every remaining word is a member, in the order typed
                let mut nodes = Vec::new();
                for word in words.by_ref() {
                    nodes.push(node(Some(word))?);
                }
                if nodes.is_empty() {
                    return Err("initialize needs at least one node id".to_string());
                }
                ClusterAction::Initialize { nodes }
            }
            "decommission" => ClusterAction::Decommission {
                node: node(words.next())?,
            },
            "fail-device" | "fail_device" => ClusterAction::FailDevice {
                device: device_id(verb, words.next())?,
            },
            "drain-device" | "drain_device" => ClusterAction::DrainDevice {
                device: device_id(verb, words.next())?,
            },
            "remove-device" | "remove_device" => {
                let device = device_id(verb, words.next())?;
                // lost only when said, by name
                let lost = match words.next() {
                    None => false,
                    Some("--lost") => true,
                    Some(other) => return Err(format!("remove-device takes --lost and nothing else, not {other}")),
                };
                ClusterAction::RemoveDevice { device, lost }
            }
            "reweight-device" | "reweight_device" => {
                let device = device_id(verb, words.next())?;
                let word = words
                    .next()
                    .ok_or_else(|| "reweight-device needs a weight after the device".to_string())?;
                let weight = word
                    .parse::<u64>()
                    .map_err(|_| format!("{word} is not a weight; a whole number, the share of a pool"))?;
                ClusterAction::ReweightDevice { device, weight }
            }
            "scrub" => {
                let pool = words
                    .next()
                    .ok_or_else(|| "scrub needs a pool; `pools` lists them".to_string())?
                    .to_string();
                // deep unless light is asked for
                let kind = match words.next() {
                    Some(word) => word.parse::<shoal::shared::scrub::ScrubKind>()?,
                    None => shoal::shared::scrub::ScrubKind::Deep,
                };
                // and what the pass is narrowed to, if anything (F93)
                let target = match words.next() {
                    None => None,
                    Some("bucket") => Some(shoal::shared::scrub::ScrubTarget::Bucket(
                        shoal::shared::identity::BucketId::of(
                            words.next().ok_or_else(|| "scrub ... bucket needs the bucket's name".to_string())?,
                        ),
                    )),
                    Some("device") => Some(shoal::shared::scrub::ScrubTarget::Device(device_id(verb, words.next())?)),
                    Some("slice") => {
                        let device = device_id(verb, words.next())?;
                        let word = words.next().ok_or_else(|| "scrub ... slice needs the slice's index after its device".to_string())?;
                        let index = word.parse::<u16>().map_err(|_| format!("{word} is not a slice index"))?;
                        Some(shoal::shared::scrub::ScrubTarget::Slice(shoal::shared::poolmap::SliceRef { device, index }))
                    }
                    Some(other) => {
                        return Err(format!("a scrub is narrowed to a bucket, a device or a slice, not {other}"))
                    }
                };
                ClusterAction::Scrub { pool, kind, target }
            }
            "resolve-stripe" | "resolve_stripe" => {
                let bucket = words.next().ok_or_else(|| "resolve-stripe needs a bucket".to_string())?.to_string();
                let path = words.next().ok_or_else(|| "resolve-stripe needs the object's path".to_string())?.to_string();
                let word = words.next().ok_or_else(|| "resolve-stripe needs the stripe's index".to_string())?;
                let stripe = word.parse::<u64>().map_err(|_| format!("{word} is not a stripe index"))?;
                let resolution = match words.next() {
                    Some("accept") => shoal::shared::protocol::admin::StripeResolution::Accept,
                    Some("lost") => shoal::shared::protocol::admin::StripeResolution::Lost,
                    other => return Err(format!("resolve-stripe takes `accept` or `lost`, not {other:?}")),
                };
                let word = words
                    .next()
                    .ok_or_else(|| "resolve-stripe needs the sequence its row is at, which its refusal names".to_string())?;
                let sequence = word.parse::<u64>().map_err(|_| format!("{word} is not a sequence"))?;
                ClusterAction::ResolveStripe {
                    bucket,
                    path,
                    stripe,
                    resolution,
                    sequence,
                }
            }
            "buckets" => ClusterAction::Buckets {
                bucket: words.next().map(str::to_string),
            },
            "devices" => ClusterAction::Devices,
            "remove" => {
                let removed = node(words.next())?;
                let replacement = match words.next() {
                    Some(word) => Some(node(Some(word))?),
                    None => None,
                };
                ClusterAction::Remove {
                    node: removed,
                    replacement,
                }
            }
            "maintenance" => {
                let member = node(words.next())?;
                let suspend = match words.next() {
                    Some("on") => true,
                    Some("off") => false,
                    other => return Err(format!("maintenance takes `on` or `off`, not {other:?}")),
                };
                ClusterAction::Maintenance {
                    node: member,
                    suspend,
                }
            }
            "rebalance" => ClusterAction::Rebalance,
            "repair" => {
                let table = words
                    .next()
                    .ok_or_else(|| "repair needs a table name".to_string())?
                    .to_string();
                let mode = words.next().unwrap_or("verify");
                if mode != "verify" && mode != "repair" {
                    return Err(format!("repair takes `verify` or `repair`, not {mode:?}"));
                }
                ClusterAction::Repair {
                    table,
                    mode: mode.to_string(),
                }
            }
            "backup" => {
                let first = words
                    .next()
                    .ok_or_else(|| "backup needs a directory".to_string())?
                    .to_string();
                match words.next() {
                    Some(path) => ClusterAction::Backup {
                        table: Some(first),
                        path: path.to_string(),
                    },
                    None => ClusterAction::Backup {
                        table: None,
                        path: first,
                    },
                }
            }
            "restore" => ClusterAction::Restore {
                path: words
                    .next()
                    .ok_or_else(|| "restore needs a directory".to_string())?
                    .to_string(),
            },
            "restore-retry" | "retry-restore" => {
                let word = words
                    .next()
                    .ok_or_else(|| "restore-retry needs the restore's operation id".to_string())?;
                ClusterAction::RetryRestore {
                    restore: word
                        .parse()
                        .map_err(|_| format!("{word} is not an operation id"))?,
                }
            }
            "extend-schema" => {
                let word = words
                    .next()
                    .ok_or_else(|| "extend-schema needs the schema id to extend to".to_string())?;
                // the id as `status` shows it, in hex, or as a plain number
                let parsed = match word.strip_prefix("0x") {
                    Some(hex) => u64::from_str_radix(hex, 16),
                    None => word.parse(),
                };
                ClusterAction::ExtendSchema {
                    schema: parsed.map_err(|_| format!("{word} is not a schema id"))?,
                }
            }
            "activate" => {
                let word = words
                    .next()
                    .ok_or_else(|| "activate needs a wire version".to_string())?;
                ClusterAction::Activate {
                    wire: word
                        .parse()
                        .map_err(|_| format!("{word} is not a wire version"))?,
                }
            }
            "status" => {
                let word = words
                    .next()
                    .ok_or_else(|| "status needs an operation id".to_string())?;
                ClusterAction::Status {
                    op: word
                        .parse()
                        .map_err(|_| format!("{word} is not an operation id"))?,
                }
            }
            "reload-tls" | "reload_tls" => ClusterAction::ReloadTls,
            "grant" | "revoke" => {
                // a principal, what the grant is on, and the privileges, in that order; a grant
                // is on a table unless the word `bucket` says otherwise
                let usage = format!(
                    "{verb} takes <principal> [bucket] <name|*> <read|write|read,write>"
                );
                let principal = words.next().ok_or_else(|| usage.clone())?.to_string();
                let mut name = words.next().ok_or_else(|| usage.clone())?.to_string();
                let on = match name.as_str() {
                    // the kind is named, so the name follows it
                    "bucket" | "table" => {
                        let on = if name == "bucket" {
                            GrantOn::Bucket
                        } else {
                            GrantOn::Table
                        };
                        name = words.next().ok_or_else(|| usage.clone())?.to_string();
                        on
                    }
                    _ => GrantOn::Table,
                };
                let privileges = Privileges::parse(words.next().ok_or(usage)?)
                    .map_err(|error| error.to_string())?;
                if verb == "grant" {
                    ClusterAction::Grant {
                        principal,
                        on,
                        name,
                        privileges,
                    }
                } else {
                    ClusterAction::Revoke {
                        principal,
                        on,
                        name,
                        privileges,
                    }
                }
            }
            "grants" => ClusterAction::Grants,
            "pools" => ClusterAction::Pools,
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
            "initialize <node> [<node>...]  place every tablet over these members, once"
                .to_string(),
            "decommission <node>            drain a member and take it out".to_string(),
            "fail-device <device>           fail a device; its groups move off it".to_string(),
            "drain-device <device>          move a device's groups off it while it serves".to_string(),
            "remove-device <device> [--lost]  remove a drained or retired device for good".to_string(),
            "reweight-device <device> <w>   give a device a weight its groups move by".to_string(),
            "scrub <pool> [light|deep] [bucket <name> | device <id> | slice <id> <index>]".to_string(),
            "                               scrub a pool now, deep unless light, all of it or part".to_string(),
            "resolve-stripe <bucket> <path> <stripe> accept|lost <sequence>".to_string(),
            "                               settle a stripe whose row is behind its chunks".to_string(),
            "remove <node> [replacement]    remove a member the cluster gave up on".to_string(),
            "maintenance <node> on|off      hold or resume a down member's grace".to_string(),
            "rebalance                      spread the sets by weight and bytes".to_string(),
            "repair <table> [verify|repair] scrub a table, or repair it".to_string(),
            "backup [table] <dir>           back a table or every table up".to_string(),
            "restore <dir>                  restore a backup into this empty cluster".to_string(),
            "restore-retry <op>             drive a finished restore's failed groups again".to_string(),
            "activate <wire>                activate a wire version".to_string(),
            "extend-schema <id>             commit a schema that appends tables or buckets".to_string(),
            "status <op>                    read an operation's record".to_string(),
            "reload-tls                     read this node's certificate again".to_string(),
            "grant <who> <table|*> <privs>  let a principal read, write, or both".to_string(),
            "grant <who> bucket <name|*> <privs>  the same, on a bucket".to_string(),
            "revoke <who> [bucket] <name|*> <privs>  take back what a grant on that name gave"
                .to_string(),
            "grants                         read every principal's grants".to_string(),
            "pools                          read every pool's devices and what it is short of".to_string(),
            "devices                        read every member's devices and their slices".to_string(),
            "buckets [name]                 read what each bucket holds and was answered".to_string(),
            String::new(),
            "Enter previews an operation; Enter again submits it. Esc forgets it.".to_string(),
        ]
    }

    /// Whether this action changes the cluster, and so is previewed before it is sent
    #[must_use]
    pub fn is_mutation(&self) -> bool {
        !matches!(
            self,
            ClusterAction::Status { .. }
                | ClusterAction::Grants
                | ClusterAction::Pools
                | ClusterAction::Buckets { .. }
                | ClusterAction::Devices
        )
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
                    |member| {
                        format!(
                            "{id} ({} {} {}, weight {})",
                            member.role, member.health, member.phase, member.weight
                        )
                    },
                )
        };
        match self {
            ClusterAction::Initialize { nodes } => {
                // the members in the order typed, then what is placed and what cannot be undone
                let mut lines = vec![format!("initialize over {} members in this order:", nodes.len())];
                lines.extend(nodes.iter().map(|node| format!("  {}", member(node))));
                lines.push(format!(
                    "moves: nothing; every tablet of every table is placed over these nodes at factor {}",
                    model.desired_rf
                ));
                lines.push(
                    "boundary: once; a second initialize is refused, and the order is changed by moves, not by another initialize"
                        .to_string(),
                );
                lines
            }
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
            ClusterAction::FailDevice { device } => vec![
                format!("fail device {device}"),
                "moves: every placement group with a chunk on it, once its node reports it failed".to_string(),
                "boundary: it is retired for good once its failure is committed; remove it, wipe it and add it as a new device".to_string(),
            ],
            ClusterAction::DrainDevice { device } => vec![
                format!("drain device {device}"),
                "moves: every placement group with a chunk on it, copied from it while it serves, then released".to_string(),
                "boundary: it is never placed again; remove-device once `devices` shows it empty".to_string(),
            ],
            ClusterAction::RemoveDevice { device, lost } => vec![
                format!("remove device {device}{}", if *lost { ", lost" } else { "" }),
                if *lost {
                    "moves: nothing more; what it held is rebuilt from the other holders".to_string()
                } else {
                    "moves: nothing; refused by name until it was retired, or drained with nothing left".to_string()
                },
                "boundary: its id is never accepted again; its node refuses to serve it by name".to_string(),
            ],
            ClusterAction::ReweightDevice { device, weight } => vec![
                format!("give device {device} weight {weight}"),
                "moves: the placement groups the new weight draws differently, at the next generation".to_string(),
                "boundary: the weight wins over its configuration's at every later start".to_string(),
            ],
            ClusterAction::Scrub { pool, kind, target } => vec![
                format!(
                    "{} scrub of pool {pool} now{}",
                    kind.as_str(),
                    match target {
                        None => String::new(),
                        Some(shoal::shared::scrub::ScrubTarget::Bucket(bucket)) => format!(", bucket {bucket} alone"),
                        Some(shoal::shared::scrub::ScrubTarget::Device(device)) => format!(", the groups on device {device} alone"),
                        Some(shoal::shared::scrub::ScrubTarget::Slice(slice)) => format!(", the groups on slice {slice} alone"),
                    }
                ),
                match kind {
                    shoal::shared::scrub::ScrubKind::Deep => {
                        "moves: nothing; every chunk is read where it lies, in each slice's idle time under its device's ceiling".to_string()
                    }
                    shoal::shared::scrub::ScrubKind::Light => {
                        "moves: nothing; every holder lists what it holds, and the listings are compared with the rows".to_string()
                    }
                },
                format!(
                    "boundary: a corrupt chunk is set aside and rebuilt from a copy that verifies; needs wire version 11 activated (activated {})",
                    model.activated_wire
                ),
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
            ClusterAction::RetryRestore { restore } => vec![
                format!("retry the failed groups of restore {restore}"),
                "moves: each failed group is loaded or installed again from where it failed; a restored group is not touched".to_string(),
                format!("boundary: the same files and operation; needs wire version 6 activated (activated {})", model.activated_wire),
            ],
            ClusterAction::Activate { wire } => vec![
                format!("activate wire version {wire} (activated {}, members speak {}..={})", model.activated_wire, model.wire_range.0, model.wire_range.1),
                "moves: nothing".to_string(),
                "boundary: no member rolls back below it; a build that cannot speak it is refused at every door".to_string(),
            ],
            ClusterAction::ExtendSchema { schema } => vec![
                format!(
                    "extend the cluster's schema from {} to {schema:#018x}",
                    model
                        .schema
                        .map_or_else(|| "none".to_string(), |committed| format!("{committed:#018x}"))
                ),
                "moves: the tables it appends start their groups on every member, and each bucket it appends is bound to its pool".to_string(),
                "boundary: no member rolls back below it; a build that does not hold it is refused at every door".to_string(),
            ],
            ClusterAction::ResolveStripe {
                bucket,
                path,
                stripe,
                resolution,
                sequence,
            } => vec![
                format!("{resolution:?} stripe {stripe} of {bucket}/{path}, its row at sequence {sequence}"),
                match resolution {
                    shoal::shared::protocol::admin::StripeResolution::Accept => "moves: the row names the label k + f holders agree on; the rest are marked and rebuilt".to_string(),
                    shoal::shared::protocol::admin::StripeResolution::Lost => "moves: the row says the stripe is lost; every read and write of it is refused Lost".to_string(),
                },
                "boundary: a lost stripe's bytes are gone for good; an accept trusts what the holders applied over the row".to_string(),
            ],
            ClusterAction::Buckets { bucket } => vec![format!(
                "read what {} holds and was answered",
                bucket.as_deref().unwrap_or("every bucket")
            )],
            ClusterAction::Devices => vec!["read every member's devices".to_string()],
            ClusterAction::Status { op } => vec![format!("read the record of {op}")],
            ClusterAction::ReloadTls => vec![
                format!("reload the certificate, key and authority of {}", model.node),
                "moves: nothing; every handshake after uses the new material and none is dropped".to_string(),
                "boundary: none; material that does not parse changes nothing".to_string(),
            ],
            ClusterAction::Grant {
                principal,
                on,
                name,
                privileges,
            } => vec![
                format!("grant {privileges} on {} to {principal}", on_what(*on, name)),
                "moves: nothing; every node holds the principal to it from the map that carries it, on connections it already holds".to_string(),
                "boundary: none; `revoke` with the same name takes it back".to_string(),
            ],
            ClusterAction::Revoke {
                principal,
                on,
                name,
                privileges,
            } => vec![
                format!("revoke {privileges} on {} from {principal}", on_what(*on, name)),
                "moves: nothing; every node refuses what it took back from the map that carries it".to_string(),
                "boundary: only the grant on this name; a grant on * still gives what it gives".to_string(),
            ],
            ClusterAction::Grants => vec!["read every principal's grants".to_string()],
            ClusterAction::Pools => vec!["read every pool's readiness".to_string()],
        }
    }

    /// The request this action sends, and how its outcome is followed
    #[must_use]
    pub fn request(&self) -> (AdminKind, Follow) {
        match self {
            ClusterAction::Initialize { nodes } => (
                AdminKind::Initialize {
                    nodes: nodes.clone(),
                },
                Follow::None,
            ),
            ClusterAction::Decommission { node } => {
                (AdminKind::Decommission { node: *node }, Follow::Plan)
            }
            ClusterAction::Remove { node, replacement } => (
                AdminKind::Remove {
                    node: *node,
                    replacement: *replacement,
                },
                Follow::Plan,
            ),
            ClusterAction::FailDevice { device } => (AdminKind::FailDevice { device: *device }, Follow::None),
            ClusterAction::DrainDevice { device } => (AdminKind::DrainDevice { device: *device }, Follow::None),
            ClusterAction::RemoveDevice { device, lost } => (
                AdminKind::RemoveDevice {
                    device: *device,
                    lost: *lost,
                },
                Follow::None,
            ),
            ClusterAction::ReweightDevice { device, weight } => (
                AdminKind::ReweightDevice {
                    device: *device,
                    weight: *weight,
                },
                Follow::None,
            ),
            ClusterAction::Scrub { pool, kind, target } => (
                AdminKind::Scrub {
                    pool: pool.clone(),
                    kind: *kind,
                    target: *target,
                },
                Follow::Scrub(*kind),
            ),
            ClusterAction::ResolveStripe {
                bucket,
                path,
                stripe,
                resolution,
                ..
            } => (
                AdminKind::ResolveStripe {
                    bucket: bucket.clone(),
                    path: path.clone(),
                    stripe: *stripe,
                    resolution: *resolution,
                },
                Follow::None,
            ),
            ClusterAction::Buckets { bucket } => (
                AdminKind::BucketStats {
                    bucket: bucket.clone(),
                },
                Follow::None,
            ),
            ClusterAction::Devices => (AdminKind::Devices, Follow::None),
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
            ClusterAction::Restore { path } => {
                (AdminKind::Restore { path: path.clone() }, Follow::Restore)
            }
            ClusterAction::RetryRestore { restore } => (
                AdminKind::RetryRestore { restore: *restore },
                Follow::Restore,
            ),
            ClusterAction::Activate { wire } => (AdminKind::Activate { wire: *wire }, Follow::None),
            ClusterAction::ExtendSchema { schema } => (
                AdminKind::ExtendSchema {
                    schema: *schema,
                    pools: None,
                },
                Follow::None,
            ),
            ClusterAction::Status { op } => (AdminKind::PlanStatus { op: *op }, Follow::None),
            ClusterAction::ReloadTls => (AdminKind::ReloadTls, Follow::None),
            ClusterAction::Grant {
                principal,
                on,
                name,
                privileges,
            } => (
                AdminKind::Grant {
                    principal: principal.clone(),
                    on: *on,
                    name: name.clone(),
                    privileges: *privileges,
                },
                Follow::None,
            ),
            ClusterAction::Revoke {
                principal,
                on,
                name,
                privileges,
            } => (
                AdminKind::Revoke {
                    principal: principal.clone(),
                    on: *on,
                    name: name.clone(),
                    privileges: *privileges,
                },
                Follow::None,
            ),
            ClusterAction::Grants => (AdminKind::Grants, Follow::None),
            ClusterAction::Pools => (AdminKind::PoolStatus, Follow::None),
        }
    }
}

/// How a preview names what a grant is on
///
/// A table is named bare, as it always was; a bucket says it is one, since a bucket and a table
/// can share a name.
///
/// # Arguments
///
/// * `on` - Whether the grant is on a table or a bucket
/// * `name` - The name the grant is on
fn on_what(on: GrantOn, name: &str) -> String {
    match on {
        GrantOn::Table if name == EVERY_TABLE => "every table".to_string(),
        GrantOn::Table => name.to_string(),
        GrantOn::Bucket if name == EVERY_BUCKET => "every bucket".to_string(),
        GrantOn::Bucket => format!("bucket {name}"),
    }
}

/// The grants the cluster answered, as one line a principal and table
///
/// # Arguments
///
/// * `grants` - The `grants` read's answer
#[must_use]
pub fn render_grants(grants: &Value) -> Vec<String> {
    // the answer is the catalog itself; one that does not read as one is shown as it came
    let Ok(grants) = shoal::serde_json::from_value::<Grants>(grants.clone()) else {
        return vec![grants.to_string()];
    };
    if grants.principals.is_empty() {
        return vec!["no principal holds a grant".to_string()];
    }
    // the widest principal, so the tables line up
    let width = grants.principals.keys().map(String::len).max().unwrap_or(0);
    let mut lines = Vec::new();
    for (principal, held) in &grants.principals {
        // its tables, then its buckets
        for on in [GrantOn::Table, GrantOn::Bucket] {
            for (name, privileges) in held.of(on) {
                lines.push(format!(
                    "{principal:<width$}  {:<16}  {privileges}",
                    on_what(on, name)
                ));
            }
        }
    }
    lines
}

/// Every pool's readiness as the cluster answered it, a few lines a pool
///
/// The first line is its policy as placement reads it - the class, the chunks of a stripe and
/// the ones a write needs, the level no two share - and what it has against that; the next say
/// what it falls back on and what it is short of
/// ([F85](../../../docs/src/features/devices-and-pools.md)).
///
/// # Arguments
///
/// * `status` - The `pool_status` read's answer
#[must_use]
pub fn render_pools(status: &Value) -> Vec<String> {
    // the answer is one readiness a pool; one that does not read as that is shown as it came
    let Ok(pools) = shoal::serde_json::from_value::<Vec<PoolReadiness>>(status.clone()) else {
        return vec![status.to_string()];
    };
    if pools.is_empty() {
        return vec!["no pool is configured".to_string()];
    }
    let mut lines = Vec::new();
    for pool in &pools {
        lines.push(format!(
            "{}  {}{} width {} needs {} by {}  devices {}/{}  {}s {}/{}  free {}  usable {}  {}  {}",
            pool.pool,
            pool.class,
            // its layout, copies or a code, from a node that says (F90)
            if pool.redundancy.is_empty() {
                String::new()
            } else {
                format!(" {}", pool.redundancy)
            },
            pool.width,
            pool.needed,
            pool.level,
            pool.serving,
            pool.devices,
            pool.level,
            pool.serving_domains,
            pool.domains,
            super::model::bytes(pool.free_bytes),
            pool.usable_bytes.map_or("-".to_string(), super::model::bytes),
            // where its stripes are placed, once it has any to place (F86)
            pool.pinned.map_or("unpinned".to_string(), |generation| format!("pinned at {generation}")),
            if pool.ready() { "ready" } else { "short" }
        ));
        // where its placement groups are moving and what recovery has to do and did (F88)
        let figures = &pool.recovery;
        lines.push(format!(
            "  recovery: target {}  moves {}  behind {}  stale {}  overflowed {}  rebuilt {}  backfilled {}  switched {}  aborted {}{}",
            pool.target.map_or("-".to_string(), |generation| generation.to_string()),
            pool.moves,
            figures.behind,
            figures.stale,
            figures.overflowed,
            figures.rebuilt,
            figures.backfilled,
            figures.moves_switched,
            figures.moves_aborted,
            if pool.retired.is_empty() {
                String::new()
            } else {
                format!("  retired {}", pool.retired.len())
            }
        ));
        // where its scrubs are and what they found (F89)
        let scrub = &figures.scrub;
        lines.push(format!(
            "  scrub: light {} pending {}  deep {} pending {}  verified {} chunks, {}  stale {}  corrupt {}  unexplained {}  disagreed {}  lost {}{}",
            finished(scrub.light_last_ms),
            scrub.light_pending,
            finished(scrub.deep_last_ms),
            scrub.deep_pending,
            scrub.chunks,
            super::model::bytes(scrub.bytes),
            scrub.stale,
            scrub.corrupt,
            scrub.unexplained,
            scrub.disagreed,
            scrub.lost,
            // an erasure coded pool's parity checked by summaries, and what disagreed (F90)
            if scrub.parity_checked > 0 || scrub.parity_mismatch > 0 {
                format!("  parity checked {} mismatched {}", scrub.parity_checked, scrub.parity_mismatch)
            } else {
                String::new()
            },
        ));
        // what its reclamation removed and has still to, and what is draining (F92)
        let reclaim = &figures.reclaim;
        if !reclaim.is_empty() || !pool.draining.is_empty() {
            lines.push(format!(
                "  reclaim: objects {}  stripes {}  removals {}  floors {}  released {}  unexplained {}  pending {}  deferred {}{}",
                reclaim.objects,
                reclaim.stripes,
                reclaim.chunks,
                reclaim.floors,
                reclaim.released,
                reclaim.unexplained,
                reclaim.pending,
                reclaim.deferred,
                if pool.draining.is_empty() {
                    String::new()
                } else {
                    format!("  draining {}", pool.draining.len())
                }
            ));
        }
        // what its level falls back on, and each thing it lacks
        if let Some(fallback) = &pool.fallback {
            lines.push(format!("  {fallback}"));
        }
        for short in &pool.short {
            lines.push(format!("  short: {short}"));
        }
    }
    lines
}

/// When the oldest pass of a kind finished, as how long ago
///
/// # Arguments
///
/// * `last_ms` - When, in milliseconds since the Unix epoch; zero for never, none for no group
fn finished(last_ms: Option<u64>) -> String {
    match last_ms {
        None => "-".to_string(),
        Some(0) => "never".to_string(),
        Some(last) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX));
            let ago = now.saturating_sub(last) / 1000;
            // the largest unit that is at least one
            match ago {
                0..60 => format!("{ago}s ago"),
                60..3600 => format!("{}m ago", ago / 60),
                3600..86_400 => format!("{}h ago", ago / 3600),
                _ => format!("{}d ago", ago / 86_400),
            }
        }
    }
}

impl ClusterAction {
    /// The operation whose record follows this action, once it was sent as `sent`
    ///
    /// Its own, except for a retry, whose record is the restore's.
    ///
    /// # Arguments
    ///
    /// * `sent` - The operation the request was sent as
    #[must_use]
    pub fn followed(&self, sent: Uuid) -> Uuid {
        match self {
            ClusterAction::RetryRestore { restore } => *restore,
            _ => sent,
        }
    }

    /// The version the request is written against: the topology's, or for a stripe's resolution
    /// the sequence its row is at, which is what the operator judged
    /// ([F93](../../../docs/src/features/operations-and-the-real-cluster.md))
    ///
    /// # Arguments
    ///
    /// * `topology` - The topology version the member serves
    #[must_use]
    pub fn expected_version(&self, topology: u64) -> u64 {
        match self {
            ClusterAction::ResolveStripe { sequence, .. } => *sequence,
            _ => topology,
        }
    }

    /// What a node has to advertise before this action is sent: an admin read's name, or a
    /// mutation's, since a node that cannot decode a kind closes the connection (F93)
    #[must_use]
    pub fn needs(&self) -> (&'static str, bool) {
        let (kind, _) = self.request();
        (kind.name(), !kind.is_mutation())
    }
}

/// Draw an admin read's answer as lines, for the kinds a person reads, or none for another
///
/// # Arguments
///
/// * `kind` - The read asked
/// * `value` - Its answer
#[must_use]
pub fn render_read(kind: &AdminKind, value: &Value) -> Option<Vec<String>> {
    match kind {
        AdminKind::Grants => Some(render_grants(value)),
        AdminKind::PoolStatus => Some(render_pools(value)),
        AdminKind::BucketStats { .. } => Some(render_buckets(value)),
        AdminKind::Devices => Some(render_devices(value)),
        _ => None,
    }
}

/// Draw the `pool_status` read as one line a pool, for the cluster tab's own section (F93)
///
/// Its readiness and what it is short of, its moves and what recovery left stale, and when its
/// scrubs last finished; `pools` draws the whole of it.
///
/// # Arguments
///
/// * `status` - The read's answer
#[must_use]
pub fn pool_summary(status: &Value) -> Vec<String> {
    let Ok(pools) = shoal::serde_json::from_value::<Vec<PoolReadiness>>(status.clone()) else {
        return Vec::new();
    };
    pools
        .iter()
        .map(|pool| {
            let figures = &pool.recovery;
            format!(
                "{} {} {}  devices {}/{}  free {}  moves {} behind {} stale {}  scrub light {} deep {}{}",
                pool.pool,
                pool.class,
                if pool.ready() { "ready" } else { "short" },
                pool.serving,
                pool.devices,
                super::model::bytes(pool.free_bytes),
                pool.moves,
                figures.behind,
                figures.stale,
                finished(figures.scrub.light_last_ms),
                finished(figures.scrub.deep_last_ms),
                if pool.short.is_empty() {
                    String::new()
                } else {
                    format!(": {}", pool.short.join("; "))
                }
            )
        })
        .collect()
}

/// Draw the `bucket_stats` read: each bucket's holdings, then a line an operation kind (F93)
///
/// # Arguments
///
/// * `value` - The read's answer
#[must_use]
pub fn render_buckets(value: &Value) -> Vec<String> {
    let Ok(answer) = shoal::serde_json::from_value::<shoal::shared::protocol::admin::BucketStatsAnswer>(value.clone()) else {
        return vec![value.to_string()];
    };
    if answer.buckets.is_empty() {
        return vec![format!("no bucket has figures yet ({} members heard)", answer.members)];
    }
    let mut lines = vec![format!("{} members' figures, from the {}", answer.members, answer.source)];
    for (bucket, figures) in &answer.buckets {
        lines.push(format!(
            "{bucket}  objects {}  bytes {}",
            figures.objects,
            super::model::bytes(figures.bytes)
        ));
        for (kind, op) in &figures.ops {
            lines.push(format!(
                "  {kind}: {} answered, {} refused  in {}  out {}  mean {}  max {} µs",
                op.count,
                op.refused,
                super::model::bytes(op.bytes_in),
                super::model::bytes(op.bytes_out),
                op.mean_us().map_or("-".to_string(), |mean| format!("{mean} µs")),
                op.max_us
            ));
        }
    }
    lines
}

/// Draw the `devices` read: a line a device of each member, with its health and its slices (F93)
///
/// # Arguments
///
/// * `value` - The read's answer
#[must_use]
pub fn render_devices(value: &Value) -> Vec<String> {
    let Some(members) = value["members"].as_array() else {
        return vec![value.to_string()];
    };
    let mut lines = vec![format!(
        "pool map at generation {}",
        value["generation"].as_u64().unwrap_or(0)
    )];
    for member in members {
        let node = member["node"].as_str().unwrap_or("?");
        let devices: Vec<shoal::shared::devices::DeviceReport> =
            shoal::serde_json::from_value(member["devices"].clone()).unwrap_or_default();
        if devices.is_empty() {
            lines.push(format!("{node}  no devices"));
        }
        for device in devices {
            lines.push(format!(
                "{node}  {}  {} {}  {:?}  free {} of {}  {} slices",
                device.id,
                device.class,
                device.path,
                device.health,
                super::model::bytes(device.free_bytes),
                super::model::bytes(device.size_bytes),
                device.slices.len()
            ));
        }
    }
    lines
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
            // a scrub has no record: every pool's figures say when it is done
            Follow::Scrub(_) => Some(AdminKind::PoolStatus),
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
            Follow::Repair | Follow::Backup | Follow::Restore => {
                record["groups"].as_object().is_some_and(|groups| {
                    !groups.is_empty() && groups.values().all(|group| group["phase"] == "Done")
                })
            }
            Follow::Move => record["phase"] == "Done",
            // every pool an operator asked a scrub of has no tablet group pending, judged against
            // its latest request
            Follow::Scrub(kind) => {
                let Ok(pools) = shoal::serde_json::from_value::<Vec<PoolReadiness>>(record.clone()) else {
                    return false;
                };
                let asked: Vec<&PoolReadiness> = pools
                    .iter()
                    .filter(|pool| pool.scrub_requests.of(kind) > 0)
                    .collect();
                !asked.is_empty()
                    && asked
                        .iter()
                        .all(|pool| pool.recovery.scrub.served(kind, pool.scrub_requests.of(kind)))
            }
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
        let mut lines = vec![format!(
            "following {op}: {}",
            if self.is_done(record) {
                "done"
            } else {
                "running"
            }
        )];
        match self {
            Follow::Plan => {
                lines.push(format!(
                    "phase {}",
                    record["phase"].as_str().unwrap_or_default()
                ));
                if let Some(reason) = record["blocked"]["reason"].as_str() {
                    lines.push(format!("blocked: {reason}"));
                }
                for step in record["steps"].as_array().into_iter().flatten() {
                    lines.push(format!(
                        "  tablet {} {} -> {} {}",
                        step["tablet"],
                        step["from"].as_str().unwrap_or_default(),
                        step["to"].as_str().unwrap_or_default(),
                        step["state"]
                            .as_str()
                            .map_or_else(|| step["state"].to_string(), str::to_string)
                    ));
                }
                if !record["outcome"].is_null() {
                    lines.push(format!("outcome {}", record["outcome"]));
                }
            }
            Follow::Scrub(kind) => {
                let pools = shoal::serde_json::from_value::<Vec<PoolReadiness>>(record.clone()).unwrap_or_default();
                for pool in pools.iter().filter(|pool| pool.scrub_requests.of(kind) > 0) {
                    let scrub = &pool.recovery.scrub;
                    let (pending, against) = match kind {
                        shoal::shared::scrub::ScrubKind::Light => (scrub.light_pending, scrub.light_against),
                        shoal::shared::scrub::ScrubKind::Deep => (scrub.deep_pending, scrub.deep_against),
                    };
                    lines.push(format!(
                        "  {} {} scrub {}: {pending} tablet groups pending; verified {} chunks, stale {}, corrupt {}",
                        pool.pool,
                        kind.as_str(),
                        pool.scrub_requests.of(kind),
                        scrub.chunks,
                        scrub.stale,
                        scrub.corrupt
                    ));
                    if against.is_none_or(|against| against < pool.scrub_requests.of(kind)) {
                        lines.push("  (no member has reported the request yet)".to_string());
                    }
                }
            }
            Follow::Repair | Follow::Backup | Follow::Restore | Follow::Move => {
                for (group, progress) in record["groups"].as_object().into_iter().flatten() {
                    lines.push(format!(
                        "  {group} {} {}",
                        progress["phase"]
                            .as_str()
                            .map_or_else(|| progress["phase"].to_string(), str::to_string),
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
    use shoal::shared::grants::GrantOn;
    use shoal::shared::identity::NodeId;
    use shoal::shared::protocol::admin::AdminKind;
    use uuid::Uuid;

    /// Every operation parses from its line and refuses a malformed one by name, previews the
    /// identity it touches, what moves and its boundary, sends the request it names, and is
    /// followed by the record that says when it is done
    /// `resolve-stripe` names its stripe, what to trust and the sequence its row is at, which the
    /// request is written against in place of the topology version (F93)
    #[test]
    fn a_resolution_is_written_against_its_rows_sequence() {
        use shoal::shared::protocol::admin::StripeResolution;
        let action = ClusterAction::parse("resolve-stripe Posters a/b.png 3 accept 9").expect("parses");
        assert_eq!(
            action,
            ClusterAction::ResolveStripe {
                bucket: "Posters".to_string(),
                path: "a/b.png".to_string(),
                stripe: 3,
                resolution: StripeResolution::Accept,
                sequence: 9,
            }
        );
        assert!(action.is_mutation());
        assert_eq!(action.expected_version(40), 9);
        assert_eq!(ClusterAction::Grants.expected_version(40), 40);
        assert_eq!(action.needs(), ("resolve_stripe", false));
        assert_eq!(ClusterAction::Buckets { bucket: None }.needs(), ("bucket_stats", true));
        assert!(ClusterAction::parse("resolve-stripe Posters a 3 lost").is_err());
        assert!(ClusterAction::parse("resolve-stripe Posters a 3 maybe 9").is_err());
    }

    /// The `bucket_stats` read is drawn a line a bucket and a line an operation kind (F93)
    #[test]
    fn bucket_figures_are_drawn_by_kind() {
        let answer = json!({
            "source": "leader",
            "members": 3,
            "buckets": {
                "Posters": {
                    "objects": 12,
                    "bytes": 2048,
                    "ops": {
                        "put": { "count": 4, "refused": 1, "bytes_in": 4096, "bytes_out": 0, "total_us": 800, "max_us": 500 }
                    }
                }
            }
        });
        let lines = super::render_buckets(&answer);
        assert_eq!(lines[0], "3 members' figures, from the leader");
        assert!(lines[1].starts_with("Posters  objects 12"), "{}", lines[1]);
        assert!(lines[2].contains("put: 4 answered, 1 refused") && lines[2].contains("mean 200 µs"), "{}", lines[2]);
        assert_eq!(super::render_buckets(&json!({"x": 1})), vec![r#"{"x":1}"#]);
    }

    /// `extend-schema` takes the schema id as `status` shows it, in hex, or as a number, and asks
    /// for the extension with the pools of the node it reaches (F93)
    #[test]
    fn an_extension_names_its_schema() {
        use shoal::shared::protocol::admin::AdminKind;
        let action = ClusterAction::parse("extend-schema 0x2a").expect("parses");
        assert_eq!(action, ClusterAction::ExtendSchema { schema: 42 });
        assert_eq!(
            ClusterAction::parse("extend-schema 42").expect("parses"),
            action
        );
        assert!(ClusterAction::parse("extend-schema").is_err());
        assert!(ClusterAction::parse("extend-schema 0xzz").is_err());
        match action.request().0 {
            AdminKind::ExtendSchema { schema, pools } => {
                assert_eq!(schema, 42);
                assert!(pools.is_none());
            }
            other => panic!("{other:?}"),
        }
    }

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
                roots: Vec::new(),
                devices: Vec::new(),
                held_bytes: None,
                wire_max: 5,
                schema_id: 0,
                schema_lineage: Vec::new(),
                client: "127.0.0.1:1".to_string(),
                host: None,
                rack: None,
                zone: None,
            }],
            ..ClusterModel::default()
        };
        // the parses
        let decommission = ClusterAction::parse(&format!("decommission {node}")).expect("parses");
        assert_eq!(decommission, ClusterAction::Decommission { node });
        assert_eq!(
            ClusterAction::parse(&format!("remove {node} {other}")).expect("parses"),
            ClusterAction::Remove {
                node,
                replacement: Some(other)
            }
        );
        assert_eq!(
            ClusterAction::parse(&format!("maintenance {node} on")).expect("parses"),
            ClusterAction::Maintenance {
                node,
                suspend: true
            }
        );
        assert_eq!(
            ClusterAction::parse("rebalance").expect("parses"),
            ClusterAction::Rebalance
        );
        let device = shoal::shared::devices::DeviceId(Uuid::new_v4());
        assert_eq!(
            ClusterAction::parse(&format!("fail-device {device}")).expect("parses"),
            ClusterAction::FailDevice { device }
        );
        assert_eq!(
            ClusterAction::parse(&format!("drain-device {device}")).expect("parses"),
            ClusterAction::DrainDevice { device }
        );
        assert_eq!(
            ClusterAction::parse(&format!("remove-device {device} --lost")).expect("parses"),
            ClusterAction::RemoveDevice { device, lost: true }
        );
        assert_eq!(
            ClusterAction::parse(&format!("remove-device {device}")).expect("parses"),
            ClusterAction::RemoveDevice { device, lost: false }
        );
        assert!(ClusterAction::parse(&format!("remove-device {device} --gone")).is_err());
        assert_eq!(
            ClusterAction::parse(&format!("reweight-device {device} 12")).expect("parses"),
            ClusterAction::ReweightDevice { device, weight: 12 }
        );
        assert!(ClusterAction::parse(&format!("reweight-device {device} heavy")).is_err());
        assert_eq!(
            ClusterAction::parse("repair Note").expect("parses"),
            ClusterAction::Repair {
                table: "Note".to_string(),
                mode: "verify".to_string()
            }
        );
        assert_eq!(
            ClusterAction::parse("backup /b").expect("parses"),
            ClusterAction::Backup {
                table: None,
                path: "/b".to_string()
            }
        );
        assert_eq!(
            ClusterAction::parse("backup Note /b").expect("parses"),
            ClusterAction::Backup {
                table: Some("Note".to_string()),
                path: "/b".to_string()
            }
        );
        assert_eq!(
            ClusterAction::parse("restore /b/x").expect("parses"),
            ClusterAction::Restore {
                path: "/b/x".to_string()
            }
        );
        assert_eq!(
            ClusterAction::parse("activate 5").expect("parses"),
            ClusterAction::Activate { wire: 5 }
        );
        // a retry is followed by the restore's record, not by its own
        let restore = Uuid::new_v4();
        let retry = ClusterAction::parse(&format!("restore-retry {restore}")).expect("parses");
        assert_eq!(retry, ClusterAction::RetryRestore { restore });
        assert_eq!(
            retry.request(),
            (AdminKind::RetryRestore { restore }, Follow::Restore)
        );
        assert_eq!(retry.followed(Uuid::new_v4()), restore);
        assert!(retry.is_mutation());
        let op = Uuid::new_v4();
        assert_eq!(
            ClusterAction::parse(&format!("status {op}")).expect("parses"),
            ClusterAction::Status { op }
        );
        assert_eq!(
            ClusterAction::parse("reload-tls").expect("parses"),
            ClusterAction::ReloadTls
        );
        // the refusals name what was wrong
        assert_eq!(
            ClusterAction::parse(&format!("initialize {node}")).expect("parses"),
            ClusterAction::Initialize { nodes: vec![node] }
        );
        assert!(ClusterAction::parse("").unwrap_err().contains("help"));
        assert!(
            ClusterAction::parse("initialize")
                .unwrap_err()
                .contains("at least one")
        );
        assert!(
            ClusterAction::parse(&format!("initialize {node} nope"))
                .unwrap_err()
                .contains("not a node id")
        );
        assert!(
            ClusterAction::parse("decommission nope")
                .unwrap_err()
                .contains("not a node id")
        );
        assert!(
            ClusterAction::parse(&format!("maintenance {node} maybe"))
                .unwrap_err()
                .contains("on")
        );
        assert!(
            ClusterAction::parse("repair Note sideways")
                .unwrap_err()
                .contains("verify")
        );
        assert!(
            ClusterAction::parse("activate five")
                .unwrap_err()
                .contains("wire version")
        );
        assert!(
            ClusterAction::parse("frobnicate")
                .unwrap_err()
                .contains("not an operation")
        );
        assert!(
            ClusterAction::parse("rebalance now")
                .unwrap_err()
                .contains("fewer")
        );
        // the previews: the identity as the model knows it, the movement and the boundary
        let preview = decommission.preview(&model);
        assert!(
            preview[0].contains(&node.to_string())
                && preview[0].contains("voter up member, weight 2"),
            "{preview:?}"
        );
        assert!(
            preview[1].starts_with("moves:") && preview[2].starts_with("boundary:"),
            "{preview:?}"
        );
        assert!(preview[2].contains("tombstoned"), "{preview:?}");
        let unknown = ClusterAction::Remove {
            node: other,
            replacement: None,
        }
        .preview(&model);
        assert!(
            unknown[0].contains("not a member this node knows"),
            "{unknown:?}"
        );
        let activate = ClusterAction::Activate { wire: 5 }.preview(&model);
        assert!(
            activate[0].contains("activated 4") && activate[0].contains("4..=5"),
            "{activate:?}"
        );
        assert!(
            ClusterAction::Restore {
                path: "/b".to_string()
            }
            .preview(&model)[2]
                .contains("once")
        );
        assert_eq!(ClusterAction::Status { op }.preview(&model).len(), 1);
        assert!(ClusterAction::Status { op }.is_mutation() == false && decommission.is_mutation());
        // the requests and their follow-ups
        assert_eq!(
            decommission.request(),
            (AdminKind::Decommission { node }, Follow::Plan)
        );
        assert_eq!(ClusterAction::Rebalance.request().1, Follow::Plan);
        assert_eq!(
            ClusterAction::Repair {
                table: "Note".to_string(),
                mode: "verify".to_string()
            }
            .request()
            .1,
            Follow::Repair
        );
        assert_eq!(
            ClusterAction::Backup {
                table: None,
                path: "/b".to_string()
            }
            .request()
            .1,
            Follow::Backup
        );
        assert_eq!(
            ClusterAction::Activate { wire: 5 }.request(),
            (AdminKind::Activate { wire: 5 }, Follow::None)
        );
        assert_eq!(
            ClusterAction::ReloadTls.request(),
            (AdminKind::ReloadTls, Follow::None)
        );
        assert_eq!(Follow::Plan.status(op), Some(AdminKind::PlanStatus { op }));
        assert_eq!(Follow::None.status(op), None);
        // a record says when it is done, and renders as lines while it runs
        let running = json!({ "phase": "Running", "outcome": null, "steps": [ { "tablet": 0, "from": "a", "to": "b", "state": "Moving" } ], "blocked": null });
        assert!(!Follow::Plan.is_done(&running));
        let lines = Follow::Plan.render(op, &running);
        assert!(
            lines[0].contains("running")
                && lines
                    .iter()
                    .any(|line| line.contains("tablet 0 a -> b Moving")),
            "{lines:?}"
        );
        let done = json!({ "phase": "Done", "outcome": "Completed", "steps": [], "blocked": null });
        assert!(Follow::Plan.is_done(&done));
        assert!(
            Follow::Plan
                .render(op, &done)
                .iter()
                .any(|line| line.contains("outcome"))
        );
        let groups = json!({ "groups": { "g1": { "phase": "Done", "outcome": { "Written": {} } }, "g2": { "phase": "Cutting", "outcome": null } } });
        assert!(!Follow::Backup.is_done(&groups));
        assert!(Follow::Backup.render(op, &groups).len() == 3);
        assert!(Follow::Backup.is_done(&json!({ "groups": { "g1": { "phase": "Done" } } })));
        assert!(!Follow::Repair.is_done(&json!({ "groups": {} })));
        assert!(
            ClusterAction::help()
                .iter()
                .any(|line| line.starts_with("decommission"))
        );
        assert!(
            ClusterAction::help()
                .iter()
                .any(|line| line.starts_with("initialize"))
        );
    }

    /// An initialize lists its members in the order typed, one line each with what the model
    /// knows of them, names the factor they are placed at and that it happens once, and sends
    /// the members in that order with nothing to follow
    #[test]
    fn initialize_previews_its_order_and_is_sent_once() {
        let first = NodeId::from(3);
        let second = NodeId::from(5);
        let third = NodeId::from(7);
        let model = ClusterModel {
            desired_rf: 2,
            members: vec![MemberRow {
                node: second.to_string(),
                role: "voter".to_string(),
                health: "up".to_string(),
                phase: "member".to_string(),
                incarnation: 1,
                grace_remaining_ms: None,
                weight: 4,
                free_bytes: None,
                roots: Vec::new(),
                devices: Vec::new(),
                held_bytes: None,
                wire_max: 5,
                schema_id: 0,
                schema_lineage: Vec::new(),
                client: "127.0.0.1:1".to_string(),
                host: None,
                rack: None,
                zone: None,
            }],
            ..ClusterModel::default()
        };
        // the order typed is the order kept
        let action =
            ClusterAction::parse(&format!("initialize {third} {second} {first}")).expect("parses");
        assert_eq!(
            action,
            ClusterAction::Initialize {
                nodes: vec![third, second, first]
            }
        );
        assert!(action.is_mutation());
        // the preview: a header, a line per member in that order, the factor and the boundary
        let preview = action.preview(&model);
        assert_eq!(preview.len(), 6, "{preview:?}");
        assert!(preview[0].contains("3 members"), "{preview:?}");
        assert!(
            preview[1].contains(&third.to_string()) && preview[1].contains("not a member"),
            "{preview:?}"
        );
        assert!(
            preview[2].contains(&second.to_string())
                && preview[2].contains("voter up member, weight 4"),
            "{preview:?}"
        );
        assert!(preview[3].contains(&first.to_string()), "{preview:?}");
        assert!(
            preview[4].starts_with("moves:") && preview[4].contains("factor 2"),
            "{preview:?}"
        );
        assert!(
            preview[5].starts_with("boundary:") && preview[5].contains("once"),
            "{preview:?}"
        );
        // the request carries the members in that order and is followed by nothing
        assert_eq!(
            action.request(),
            (
                AdminKind::Initialize {
                    nodes: vec![third, second, first]
                },
                Follow::None
            )
        );
    }

    /// `pools` is a read of every pool's readiness that sends no preview, drawn a line a pool
    /// with its recovery, its fallback and what it is short of beneath (F85, F88)
    #[test]
    fn pools_are_read_and_drawn() {
        let action = ClusterAction::parse("pools").expect("parses");
        assert!(!action.is_mutation());
        assert_eq!(action.request(), (AdminKind::PoolStatus, Follow::None));
        // the answer as `PoolReadiness` serializes it
        let answer = json!([{
            "pool": "fast", "class": "ssd", "level": "rack", "width": 3, "needed": 2,
            "devices": 2, "serving": 2, "domains": 2, "serving_domains": 2,
            "fallback": "rack falls back to host on 2 members",
            "free_bytes": 1u64 << 30, "usable_bytes": 3u64 << 29,
            "short": ["2 racks of 3"]
        }]);
        let lines = super::render_pools(&answer);
        assert!(lines[0].starts_with("fast  ssd width 3 needs 2 by rack  devices 2/2  racks 2/2"), "{}", lines[0]);
        assert!(lines[0].ends_with("unpinned  short"), "{}", lines[0]);
        // a pool its stripes are placed in says at which generation (F86)
        let mut pinned = answer.clone();
        pinned[0]["pinned"] = json!(4);
        pinned[0]["short"] = json!([]);
        assert!(super::render_pools(&pinned)[0].ends_with("pinned at 4  ready"));
        // what recovery has to do and did, a line of its own (F88)
        assert_eq!(
            lines[1],
            "  recovery: target -  moves 0  behind 0  stale 0  overflowed 0  rebuilt 0  backfilled 0  switched 0  aborted 0"
        );
        let mut moving = answer.clone();
        moving[0]["target"] = json!(6);
        moving[0]["moves"] = json!(3);
        moving[0]["retired"] = json!([uuid::Uuid::nil()]);
        moving[0]["recovery"] = json!({"stale": 5, "behind": 40, "rebuilt": 2, "moves_switched": 7});
        assert_eq!(
            super::render_pools(&moving)[1],
            "  recovery: target 6  moves 3  behind 40  stale 5  overflowed 0  rebuilt 2  backfilled 0  switched 7  aborted 0  retired 1"
        );
        // and what its scrubs found, a line of its own beneath (F89)
        assert_eq!(
            lines[2],
            "  scrub: light - pending 0  deep - pending 0  verified 0 chunks, 0B  stale 0  corrupt 0  unexplained 0  disagreed 0  lost 0"
        );
        let mut scrubbed = answer.clone();
        scrubbed[0]["recovery"] = json!({"scrub": {
            "light_last_ms": 0, "deep_last_ms": 0, "deep_pending": 2, "chunks": 9, "bytes": 1u64 << 20,
            "stale": 1, "corrupt": 2, "lost": 1
        }});
        assert_eq!(
            super::render_pools(&scrubbed)[2],
            "  scrub: light never pending 0  deep never pending 2  verified 9 chunks, 1.0MiB  stale 1  corrupt 2  unexplained 0  disagreed 0  lost 1"
        );
        assert_eq!(lines[3], "  rack falls back to host on 2 members");
        assert_eq!(lines[4], "  short: 2 racks of 3");
        // an erasure coded pool names its layout, and its scrubs what its parity check found (F90)
        let mut coded = answer.clone();
        coded[0]["redundancy"] = json!("2+1");
        coded[0]["recovery"] = json!({"scrub": {"parity_checked": 12, "parity_mismatch": 1}});
        let coded = super::render_pools(&coded);
        assert!(coded[0].starts_with("fast  ssd 2+1 width 3 needs 2"), "{}", coded[0]);
        assert!(coded[2].ends_with("lost 0  parity checked 12 mismatched 1"), "{}", coded[2]);
        // none configured, and an answer that does not read is shown as it came
        assert_eq!(super::render_pools(&json!([])), vec!["no pool is configured"]);
        assert_eq!(super::render_pools(&json!({"x": 1})), vec![r#"{"x":1}"#]);
    }

    /// `scrub` parses a pool and a kind, deep unless light is asked for, is a mutation whose
    /// preview names its boundary, and is followed by every pool's figures until no group of a
    /// pool asked is pending, judged against its latest request (F89)
    #[test]
    fn a_scrub_parses_and_is_followed_until_no_group_is_pending() {
        use shoal::shared::scrub::ScrubKind;
        let deep = ClusterAction::parse("scrub fast").expect("parses");
        assert_eq!(
            deep,
            ClusterAction::Scrub {
                pool: "fast".to_string(),
                kind: ScrubKind::Deep,
                target: None,
            }
        );
        assert!(deep.is_mutation());
        let light = ClusterAction::parse("scrub fast light").expect("parses");
        assert_eq!(
            light.request(),
            (
                AdminKind::Scrub {
                    pool: "fast".to_string(),
                    kind: ScrubKind::Light,
                    target: None,
                },
                Follow::Scrub(ScrubKind::Light)
            )
        );
        assert!(ClusterAction::parse("scrub").is_err());
        assert!(ClusterAction::parse("scrub fast weekly").is_err());
        assert!(ClusterAction::parse("scrub fast deep now").is_err());
        // narrowed to a bucket, a device and a slice (F93)
        let bucket = ClusterAction::parse("scrub fast light bucket Posters").expect("parses");
        assert!(matches!(
            bucket,
            ClusterAction::Scrub {
                target: Some(shoal::shared::scrub::ScrubTarget::Bucket(_)),
                ..
            }
        ));
        let device = uuid::Uuid::new_v4();
        assert!(matches!(
            ClusterAction::parse(&format!("scrub fast deep device {device}")).expect("parses"),
            ClusterAction::Scrub {
                target: Some(shoal::shared::scrub::ScrubTarget::Device(_)),
                ..
            }
        ));
        assert!(matches!(
            ClusterAction::parse(&format!("scrub fast deep slice {device} 2")).expect("parses"),
            ClusterAction::Scrub {
                target: Some(shoal::shared::scrub::ScrubTarget::Slice(slice)),
                ..
            } if slice.index == 2
        ));
        assert!(ClusterAction::parse("scrub fast deep bucket").is_err());
        assert!(ClusterAction::parse(&format!("scrub fast deep slice {device}")).is_err());
        // followed by the pools' figures
        let follow = Follow::Scrub(ScrubKind::Deep);
        assert_eq!(follow.status(Uuid::nil()), Some(AdminKind::PoolStatus));
        let pools = |against: u64, pending: u64| {
            json!([
                {
                    "pool": "fast", "class": "ssd", "level": "host", "width": 3, "needed": 2,
                    "devices": 3, "serving": 3, "domains": 3, "serving_domains": 3, "free_bytes": 0,
                    "scrub_requests": {"deep": 2, "light": 0},
                    "recovery": {"scrub": {"deep_against": against, "deep_pending": pending}}
                },
                {
                    "pool": "slow", "class": "hdd", "level": "host", "width": 3, "needed": 2,
                    "devices": 3, "serving": 3, "domains": 3, "serving_domains": 3, "free_bytes": 0,
                    "recovery": {"scrub": {"deep_against": 0, "deep_pending": 3}}
                }
            ])
        };
        // a member that has not seen the request yet says nothing is pending against the last
        assert!(!follow.is_done(&pools(1, 0)));
        assert!(!follow.is_done(&pools(2, 1)));
        // a pool nobody asked is not waited on
        assert!(follow.is_done(&pools(2, 0)));
        assert!(follow.render(Uuid::nil(), &pools(2, 1))[1].contains("fast deep scrub 2: 1 tablet groups pending"));
        // nothing asked of any pool is never done
        assert!(!follow.is_done(&json!([])));
    }

    /// A grant and a revoke parse from a principal, a name and privileges, refuse a privilege
    /// nobody knows by name, preview what they change, and send the request they name; `grants`
    /// is a read that sends no preview, and its answer is one line a principal and table (F77)
    #[test]
    fn a_grant_parses_previews_and_renders() {
        use shoal::shared::grants::Privileges;
        let action = ClusterAction::parse("grant reader Movie read,write").expect("parses");
        assert_eq!(
            action,
            ClusterAction::Grant {
                principal: "reader".to_string(),
                on: GrantOn::Table,
                name: "Movie".to_string(),
                privileges: Privileges::ALL,
            }
        );
        assert!(action.is_mutation());
        let preview = action.preview(&ClusterModel::default());
        assert_eq!(preview[0], "grant read, write on Movie to reader");
        assert!(preview[1].starts_with("moves:") && preview[2].starts_with("boundary:"));
        assert_eq!(
            action.request(),
            (
                AdminKind::Grant {
                    principal: "reader".to_string(),
                    on: GrantOn::Table,
                    name: "Movie".to_string(),
                    privileges: Privileges::ALL,
                },
                Follow::None
            )
        );
        // a grant on a bucket names the kind before the name, and previews it as a bucket (F79)
        let bucket = ClusterAction::parse("grant viewer bucket Posters read").expect("parses");
        assert_eq!(
            bucket.request(),
            (
                AdminKind::Grant {
                    principal: "viewer".to_string(),
                    on: GrantOn::Bucket,
                    name: "Posters".to_string(),
                    privileges: Privileges::READ,
                },
                Follow::None
            )
        );
        let preview = bucket.preview(&ClusterModel::default());
        assert_eq!(preview[0], "grant read on bucket Posters to viewer");
        let every = ClusterAction::parse("revoke viewer bucket * read").expect("parses");
        let preview = every.preview(&ClusterModel::default());
        assert_eq!(preview[0], "revoke read on every bucket from viewer");
        // a kind with no name after it is refused with the usage
        let bare = ClusterAction::parse("grant viewer bucket read").expect_err("parsed");
        assert!(bare.contains("[bucket]"), "{bare}");
        // a revoke on every table says so, and that it is the grant on that name it takes back
        let revoke = ClusterAction::parse("revoke reader * write").expect("parses");
        let preview = revoke.preview(&ClusterModel::default());
        assert_eq!(preview[0], "revoke write on every table from reader");
        // a privilege nobody knows, and a line too short, are refused by name
        let refused = ClusterAction::parse("grant reader Movie delete").expect_err("parsed");
        assert!(refused.contains("delete"), "{refused}");
        let short = ClusterAction::parse("grant reader Movie").expect_err("parsed");
        assert!(short.contains("<principal>"), "{short}");
        // the read previews nothing and sends the read
        let read = ClusterAction::parse("grants").expect("parses");
        assert!(!read.is_mutation());
        assert_eq!(read.request(), (AdminKind::Grants, Follow::None));
        // and its answer is a line a principal and table, the names lined up
        let lines = super::render_grants(&json!({
            "reader": {"tables": {"Movie": ["read"]}, "buckets": {"Posters": ["read"]}},
            "auditor": {"tables": {"*": ["read", "write"]}}
        }));
        assert_eq!(
            lines,
            vec![
                format!("auditor  {:<16}  read, write", "every table"),
                format!("reader   {:<16}  read", "Movie"),
                format!("reader   {:<16}  read", "bucket Posters"),
            ]
        );
        assert_eq!(super::render_grants(&json!({})), vec!["no principal holds a grant"]);
    }
}
