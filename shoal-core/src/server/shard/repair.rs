//! A scrub of one tablet group: the entry proposed, and every member's digest polled
//!
//! The scrub phase of a repair ([F44](../../../../docs/src/features/repair.md)). The leader's
//! shard proposes the scrub entry through its own handle, which fixes the boundary `B` every
//! replica takes its canonical cut at, then asks every member over the replication lane for its
//! report until each has answered or the timeout passes. A member answers `Pending` while its
//! cut's task is still reading, and `Unknown` for a scrub it never applied - a member that was
//! down when the entry committed applies it when it catches up, and is polled until then.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use kanal::AsyncSender;
use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use openraft_rt::WatchReceiver as _;
use tracing::{event, Level};
use uuid::Uuid;

use crate::server::control::plane::ControlRequest;
use crate::server::control::repair::{
    DigestSummary, GroupRepair, Quarantine, QuarantineAction, QuarantineReason, RepairMode,
    RepairOutcome, RepairPhase,
};
use crate::server::control::types::{ControlCommand, ControlResponse};
use crate::server::database::ShoalDatabase;
use crate::server::messages::ServerMsg;
use crate::server::replication::{
    DataConfig, DigestAnswer, DigestIntegrity, DigestReport, GroupMachine, MachineState,
    ShardNetwork, ShardPeer,
};
use crate::shared::identity::{GroupId, NodeId, ShardAddr, TableId};
use crate::shared::protocol::peer::Command;

/// How long to wait between polls of a member whose report is not in yet
const DIGEST_POLL: Duration = Duration::from_millis(100);

/// What an error from a driver that no longer leads begins with
///
/// A driver that lost the lead abandons the group for the new leader rather than failing it.
pub const NOT_LEADER: &str = "this shard does not lead ";

/// What a scrub of one group came to
#[derive(Debug, Clone)]
pub struct ScrubOutcome {
    /// The operation
    pub op: Uuid,
    /// The index the scrub committed at, which every report is of
    pub boundary: u64,
    /// The log id the scrub committed at, whole, for a cut that names it
    /// ([F49](../../../../docs/src/features/backup-and-recovery.md))
    pub log_id: crate::server::wal::WalLogId,
    /// Every member's report, or why there is none from it
    pub reports: BTreeMap<ShardAddr, Result<DigestReport, String>>,
    /// The members whose copies said they stalled on a partition they could not read
    ///
    /// Each is a repair target without a judgement, since the copy said so of itself
    /// ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md)).
    pub stalled: Vec<ShardAddr>,
}

/// Propose a scrub through a group's leader handle and poll every member for its digest
///
/// # Arguments
///
/// * `raft` - The group's handle, which has to lead
/// * `network` - The shard's network
/// * `me` - This shard
/// * `local` - This shard's copy of the group's state, answered without a round trip
/// * `table` - The group's table
/// * `group` - The group
/// * `members` - Every member
/// * `op` - The operation
/// * `timeout` - How long the whole scrub may take
///
/// # Errors
///
/// Fails if the entry could not be proposed - the handle does not lead, or the commit did not
/// land in time. A member that never reports is an error in its own slot, not of the scrub.
#[allow(clippy::too_many_arguments)]
pub async fn scrub_group<D: ShoalDatabase>(
    raft: &Raft<DataConfig, GroupMachine<D>>,
    network: &ShardNetwork,
    me: ShardAddr,
    local: Rc<RefCell<MachineState>>,
    table: TableId,
    group: GroupId,
    members: &[ShardAddr],
    op: Uuid,
    timeout: Duration,
) -> Result<ScrubOutcome, String> {
    let started = Instant::now();
    // the entry: committed at the boundary every replica cuts at
    let written = glommio::timer::timeout(timeout, async {
        Ok(raft.client_write(Command::scrub(table, op)).await)
    })
    .await;
    let log_id = match written {
        Err(_) => {
            return Err(format!(
                "group {group} did not commit the scrub within {timeout:?}"
            ))
        }
        Ok(Ok(response)) => response.log_id,
        Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward)))) => {
            return Err(format!(
                "{NOT_LEADER}group {group}: the leader is {:?}",
                forward.leader_node.or(forward.leader_id)
            ))
        }
        Ok(Err(error)) => return Err(format!("proposing the scrub of group {group}: {error}")),
    };
    let boundary = log_id.index;
    // every member's report, polled until it is in or the time is up
    let mut reports: BTreeMap<ShardAddr, Result<DigestReport, String>> = BTreeMap::new();
    let mut last: BTreeMap<ShardAddr, String> = BTreeMap::new();
    let mut stalled: Vec<ShardAddr> = Vec::new();
    loop {
        for member in members {
            if reports.contains_key(member) {
                continue;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            // this shard's own copy is read here; a peer's over the lane
            let answer = if *member == me {
                Ok(local.borrow().digest_of(op))
            } else {
                let peer = ShardPeer::new(*member, network.clone());
                match peer.digest(group, op, remaining.max(DIGEST_POLL)).await {
                    Ok(bytes) => postcard::from_bytes::<DigestAnswer>(&bytes)
                        .map_err(|error| format!("decoding a digest answer: {error}")),
                    Err(failure) => Err(failure.to_string()),
                }
            };
            match answer {
                // a report of this scrub's boundary; one of another is a cut of an earlier
                // application of the same operation, and this one is still to come
                // ([Resolved #162](../../../../docs/src/appendix/resolved/stale-scrub-digest.md))
                Ok(DigestAnswer::Report(report)) if report.boundary == boundary => {
                    reports.insert(*member, Ok(report));
                }
                Ok(DigestAnswer::Report(report)) => {
                    last.insert(
                        *member,
                        format!(
                            "the member answered a cut at {} for the scrub at {boundary}",
                            report.boundary
                        ),
                    );
                }
                Ok(DigestAnswer::Pending) => {
                    last.insert(*member, "the member's cut is still being read".to_string());
                }
                Ok(DigestAnswer::Unknown) => {
                    last.insert(*member, "the member has not applied the scrub".to_string());
                }
                // a stalled copy never will, so it is not polled until the deadline
                Ok(DigestAnswer::Stalled) => {
                    reports.insert(
                        *member,
                        Err("the member's copy stalled on a partition it could not read".to_string()),
                    );
                    stalled.push(*member);
                }
                Err(error) => {
                    last.insert(*member, error);
                }
            }
        }
        if reports.len() == members.len() {
            break;
        }
        if started.elapsed() >= timeout {
            // whoever is still out answers with the last thing it said
            for member in members {
                if !reports.contains_key(member) {
                    let why = last
                        .get(member)
                        .cloned()
                        .unwrap_or_else(|| "never asked".to_string());
                    reports.insert(*member, Err(format!("no report within {timeout:?}: {why}")));
                }
            }
            break;
        }
        glommio::timer::sleep(DIGEST_POLL).await;
    }
    Ok(ScrubOutcome {
        op,
        boundary,
        log_id,
        reports,
        stalled,
    })
}

/// The least time between two repairs a leader asks for the same stalled member's group
///
/// A repair that failed - the member out of reach, say - is asked for again after this.
pub const STALL_REPAIR_RETRY: Duration = Duration::from_secs(30);

/// The directory under `wal/Shard-N/` a quarantined copy's marker lives in
pub const QUARANTINE_DIR: &str = "quarantine";

/// The marker's name for a group
///
/// # Arguments
///
/// * `group` - The group
fn marker_name(group: GroupId) -> String {
    format!("{group}")
}

/// Persist a copy's quarantine, so a restart finds it still quarantined
///
/// # Arguments
///
/// * `wal_dir` - The shard's WAL directory
/// * `group` - The group
/// * `quarantine` - The quarantine
pub async fn write_quarantine(
    wal_dir: &Path,
    group: GroupId,
    quarantine: &Quarantine,
) -> std::io::Result<()> {
    let dir = wal_dir.join(QUARANTINE_DIR);
    std::fs::create_dir_all(&dir)?;
    let bytes = postcard::to_allocvec(quarantine)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    crate::server::wal::write_atomic(&dir, &marker_name(group), bytes).await
}

/// Remove a copy's quarantine marker, once it is lifted
///
/// # Arguments
///
/// * `wal_dir` - The shard's WAL directory
/// * `group` - The group
pub async fn clear_quarantine(wal_dir: &Path, group: GroupId) -> std::io::Result<()> {
    let path = wal_dir.join(QUARANTINE_DIR).join(marker_name(group));
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    // the removal is made durable like the write was
    let directory = glommio::io::Directory::open(wal_dir.join(QUARANTINE_DIR))
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    directory
        .sync()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    directory
        .close()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(())
}

/// Every quarantine marker under a shard's WAL directory, by group
///
/// # Arguments
///
/// * `wal_dir` - The shard's WAL directory
#[must_use]
pub fn scan_quarantine(wal_dir: &Path) -> HashMap<GroupId, Quarantine> {
    let mut found = HashMap::new();
    let Ok(entries) = std::fs::read_dir(wal_dir.join(QUARANTINE_DIR)) else {
        return found;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        // a staged write that never renamed is not a marker
        let Ok(group) = u64::from_str_radix(&name, 16) else {
            continue;
        };
        let quarantine = std::fs::read(entry.path())
            .ok()
            .and_then(|bytes| postcard::from_bytes::<Quarantine>(&bytes).ok());
        match quarantine {
            Some(quarantine) => {
                found.insert(GroupId(group), quarantine);
            }
            None => {
                tracing::event!(
                    tracing::Level::ERROR,
                    msg =
                        "a quarantine marker does not decode; the copy is held quarantined by name",
                    group = name
                );
                found.insert(
                    GroupId(group),
                    Quarantine {
                        reason: QuarantineReason::Checksum,
                        at: 0,
                        op: Uuid::nil(),
                    },
                );
            }
        }
    }
    found
}

/// What a scrub's reports say about a group's copies
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// What the group came to
    pub outcome: RepairOutcome,
    /// The copies to quarantine, and why
    pub quarantine: Vec<(ShardAddr, QuarantineReason)>,
    /// The digest every trusted copy holds, if one was established
    pub trusted: Option<u64>,
    /// The copies holding the trusted digest, verified
    pub trusted_members: Vec<ShardAddr>,
}

/// Judge a scrub's reports
///
/// A copy whose report is invalid is quarantined for its checksum, whatever else is found. Among
/// the verified copies, a strict majority of the *replica set* agreeing on one digest is the
/// trusted state, and every other verified copy is quarantined as divergent; an operator's
/// source overrides that rule with its own verified digest, and a copy differing from it is
/// quarantined under the operator's word. With no majority and no source - a three way split, or
/// too few verified copies - the outcome is unresolved: nothing more is quarantined, nothing is
/// installed, and the digests are the evidence ([F44](../../../../docs/src/features/repair.md)).
///
/// # Arguments
///
/// * `members` - Every member of the group
/// * `scrub` - What the scrub came to
/// * `source` - The node the operator named as trusted, if one
#[must_use]
pub fn judge(members: &[ShardAddr], scrub: &ScrubOutcome, source: Option<NodeId>) -> Verdict {
    let mut quarantine: Vec<(ShardAddr, QuarantineReason)> = Vec::new();
    let mut invalid: Vec<ShardAddr> = Vec::new();
    let mut verified: BTreeMap<ShardAddr, DigestReport> = BTreeMap::new();
    // the invalid copies are quarantined on the evidence of their own checksums
    for (member, report) in &scrub.reports {
        match report {
            Ok(report) if report.integrity == DigestIntegrity::Verified => {
                verified.insert(*member, *report);
            }
            Ok(_) => {
                invalid.push(*member);
                quarantine.push((*member, QuarantineReason::Checksum));
            }
            Err(_) => {}
        }
    }
    // a copy that stalled on an unreadable partition said so itself, and is repaired as soon
    // as a trusted digest is there to repair it from
    for member in &scrub.stalled {
        quarantine.push((*member, QuarantineReason::Unreadable));
    }
    let summaries: Vec<(ShardAddr, DigestSummary)> = verified
        .iter()
        .map(|(member, report)| {
            (
                *member,
                DigestSummary {
                    digest: report.digest,
                    rows: report.rows,
                    partitions: report.partitions,
                    checksum_failures: 0,
                },
            )
        })
        .collect();
    // the trusted digest: the operator's source, or a strict majority of the replica set
    let trusted: Option<(u64, QuarantineReason)> = match source {
        Some(node) => verified
            .iter()
            .find(|(member, _)| member.node == node)
            .map(|(_, report)| (report.digest, QuarantineReason::Operator)),
        None => {
            let mut tally: BTreeMap<u64, usize> = BTreeMap::new();
            for report in verified.values() {
                *tally.entry(report.digest).or_default() += 1;
            }
            tally
                .into_iter()
                .find(|(_, count)| *count * 2 > members.len())
                .map(|(digest, _)| (digest, QuarantineReason::Divergent))
        }
    };
    match trusted {
        Some((digest, reason)) => {
            let trusted_members: Vec<ShardAddr> = verified
                .iter()
                .filter(|(_, report)| report.digest == digest)
                .map(|(member, _)| *member)
                .collect();
            for (member, report) in &verified {
                if report.digest != digest {
                    quarantine.push((*member, reason));
                }
            }
            // clean is a trusted digest and nothing to quarantine; a member that never
            // reported was not judged, and the outcome says so by name
            let outcome = if quarantine.is_empty() {
                RepairOutcome::Clean {
                    unreported: members
                        .iter()
                        .filter(|member| !verified.contains_key(member))
                        .copied()
                        .collect(),
                }
            } else {
                RepairOutcome::Divergent {
                    quarantined: quarantine.clone(),
                }
            };
            Verdict {
                outcome,
                quarantine,
                trusted: Some(digest),
                trusted_members,
            }
        }
        None => Verdict {
            outcome: RepairOutcome::Unresolved {
                digests: summaries,
                invalid,
            },
            quarantine,
            trusted: None,
            trusted_members: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{judge, DigestIntegrity, DigestReport, RepairOutcome, ScrubOutcome};
    use crate::server::control::repair::QuarantineReason;
    use crate::shared::identity::{NodeId, ShardAddr};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    /// A report at a digest, verified or with failures
    fn report(digest: u64, failures: u64) -> DigestReport {
        DigestReport {
            boundary: 10,
            partitions: 3,
            rows: 3,
            digest,
            integrity: if failures == 0 {
                DigestIntegrity::Verified
            } else {
                DigestIntegrity::Invalid {
                    checksum_failures: failures,
                }
            },
            unverified: 0,
            bytes: 0,
        }
    }

    /// The judge quarantines on evidence and stops without it
    ///
    /// Three verified copies agreeing are clean; two agreeing and one differing quarantine the
    /// one; an invalid copy is quarantined for its checksum whatever the rest say; a three way
    /// split is unresolved with nothing quarantined but the invalid; an operator's source
    /// decides a split, and a copy differing from it is quarantined under the operator's word.
    #[test]
    fn the_judge_needs_a_majority_or_an_operator() {
        // the members in node order, since the verdict lists them so
        let mut nodes: Vec<NodeId> = (0..3).map(|_| NodeId(Uuid::new_v4())).collect();
        nodes.sort();
        let members: Vec<ShardAddr> = nodes.iter().map(|node| ShardAddr::new(*node, 0)).collect();
        let scrub = |digests: [(u64, u64); 3]| ScrubOutcome {
            op: Uuid::new_v4(),
            boundary: 10,
            log_id: {
                use openraft::vote::RaftLeaderId as _;
                openraft::LogId::new(crate::server::wal::LeaderId::new(1, members[0]), 10)
            },
            reports: members
                .iter()
                .zip(digests)
                .map(|(member, (digest, failures))| (*member, Ok(report(digest, failures))))
                .collect::<BTreeMap<_, _>>(),
            stalled: Vec::new(),
        };
        // clean
        let verdict = judge(&members, &scrub([(7, 0), (7, 0), (7, 0)]), None);
        assert_eq!(
            verdict.outcome,
            RepairOutcome::Clean {
                unreported: Vec::new()
            }
        );
        assert!(verdict.quarantine.is_empty());
        assert_eq!(verdict.trusted, Some(7));
        // one divergent
        let verdict = judge(&members, &scrub([(7, 0), (7, 0), (9, 0)]), None);
        assert_eq!(
            verdict.quarantine,
            vec![(members[2], QuarantineReason::Divergent)]
        );
        assert!(matches!(verdict.outcome, RepairOutcome::Divergent { .. }));
        assert_eq!(verdict.trusted_members, vec![members[0], members[1]]);
        // one invalid, the rest agreeing
        let verdict = judge(&members, &scrub([(7, 0), (0, 2), (7, 0)]), None);
        assert_eq!(
            verdict.quarantine,
            vec![(members[1], QuarantineReason::Checksum)]
        );
        assert_eq!(verdict.trusted, Some(7));
        // a three way split: unresolved, nothing quarantined, the digests kept
        let verdict = judge(&members, &scrub([(1, 0), (2, 0), (3, 0)]), None);
        assert!(verdict.quarantine.is_empty());
        assert_eq!(verdict.trusted, None);
        match &verdict.outcome {
            RepairOutcome::Unresolved { digests, invalid } => {
                assert_eq!(digests.len(), 3);
                assert!(invalid.is_empty());
            }
            other => panic!("a split was judged {other:?}"),
        }
        // one invalid and two disagreeing: unresolved, the invalid one still quarantined
        let verdict = judge(&members, &scrub([(1, 0), (2, 0), (0, 1)]), None);
        assert_eq!(
            verdict.quarantine,
            vec![(members[2], QuarantineReason::Checksum)]
        );
        assert!(matches!(verdict.outcome, RepairOutcome::Unresolved { .. }));
        // the operator's source decides a split
        let verdict = judge(&members, &scrub([(1, 0), (2, 0), (3, 0)]), Some(nodes[0]));
        assert_eq!(verdict.trusted, Some(1));
        assert_eq!(
            verdict.quarantine,
            vec![
                (members[1], QuarantineReason::Operator),
                (members[2], QuarantineReason::Operator)
            ]
        );
        // a source whose own copy is invalid establishes nothing
        let verdict = judge(&members, &scrub([(0, 1), (2, 0), (3, 0)]), Some(nodes[0]));
        assert_eq!(verdict.trusted, None);
        assert!(matches!(verdict.outcome, RepairOutcome::Unresolved { .. }));
        // a member that never reported is neither trusted nor quarantined, and two of three
        // agreeing is still a majority of the set
        let mut partial = scrub([(7, 0), (7, 0), (7, 0)]);
        partial
            .reports
            .insert(members[2], Err("no report".to_string()));
        let verdict = judge(&members, &partial, None);
        assert_eq!(verdict.trusted, Some(7));
        assert!(verdict.quarantine.is_empty());
        // a member whose copy stalled on an unreadable partition is a target without a
        // judgement, repaired from the two that agree (item 160)
        let mut stalled = partial.clone();
        stalled.stalled.push(members[2]);
        let stalled_verdict = judge(&members, &stalled, None);
        assert_eq!(stalled_verdict.trusted, Some(7));
        assert_eq!(
            stalled_verdict.quarantine,
            vec![(members[2], QuarantineReason::Unreadable)]
        );
        assert!(matches!(stalled_verdict.outcome, RepairOutcome::Divergent { .. }));
        assert_eq!(stalled_verdict.trusted_members, vec![members[0], members[1]]);
        assert_eq!(
            verdict.outcome,
            RepairOutcome::Clean {
                unreported: vec![members[2]]
            },
            "a missing report is named, not judged"
        );
    }
}

/// How long a driver waits for the control plane to commit its progress
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a driver waits for a member to persist a quarantine
const QUARANTINE_TIMEOUT: Duration = Duration::from_secs(10);

/// Everything a group's driver needs, gathered on the loop before the task starts
pub struct DriverContext<D: ShoalDatabase> {
    /// The group's handle, which leads
    pub raft: Raft<DataConfig, GroupMachine<D>>,
    /// The shard's network
    pub network: ShardNetwork,
    /// This shard
    pub me: ShardAddr,
    /// This node's incarnation
    pub incarnation: u64,
    /// This shard's copy of the group's state
    pub state: Rc<RefCell<MachineState>>,
    /// The group's table
    pub table: TableId,
    /// The group
    pub group: GroupId,
    /// Every member
    pub members: Vec<ShardAddr>,
    /// The record's operation
    pub op: Uuid,
    /// What was asked
    pub mode: RepairMode,
    /// The node the operator named as trusted, if one
    pub source: Option<NodeId>,
    /// Whether the operator asked for a release rather than a judgement
    pub release: bool,
    /// How long one scrub may take
    pub timeout: Duration,
    /// The control thread, which commits the progress
    pub control: kanal::Sender<ControlRequest>,
    /// The loop, which holds this shard's own copy
    pub loop_tx: AsyncSender<ServerMsg<D>>,
    /// Whether the group's log lives in memory alone
    pub volatile: bool,
    /// How long one snapshot transfer may take
    pub snapshot_timeout: Duration,
}

/// How many times a cut is taken again for a receiver whose checkpoint is past it
const CUTS_AT_MOST: usize = 3;

impl<D: ShoalDatabase> DriverContext<D> {
    /// Replace a quarantined durable copy with a snapshot of this shard's, cut past its checkpoint
    ///
    /// The cut is this shard's own, at or past its checkpoint; a receiver whose checkpoint is
    /// at or past the boundary answers where it stands, and the checkpoint here is moved past
    /// it - an entry proposed, the WAL rotated and swept, the compactor's merge waited for -
    /// and the cut taken again, three times at most. Returns the boundary installed
    /// ([F44](../../../../docs/src/features/repair.md)).
    ///
    /// # Arguments
    ///
    /// * `target` - The quarantined copy
    async fn install_on(&self, target: ShardAddr) -> Result<u64, String> {
        let mut peer = crate::server::replication::GroupPeer::for_repair(
            self.group,
            target,
            self.network.clone(),
        );
        // the first cut may be any the leader holds; one the target was past has to be passed
        let mut at_least = 0;
        for attempt in 0..CUTS_AT_MOST {
            let built = self
                .network
                .build(self.group, at_least)
                .await
                .map_err(|error| format!("cutting a snapshot: {error}"))?;
            let vote = self.raft.metrics().borrow_watched().vote.clone();
            event!(Level::INFO, msg = "sending a repair snapshot", op = %self.op, group = %self.group, %target, boundary = built.manifest.boundary.index, attempt);
            match peer
                .repair_snapshot(
                    vote,
                    built.path.clone(),
                    built.manifest.clone(),
                    self.op,
                    self.snapshot_timeout,
                )
                .await?
            {
                crate::server::replication::network::RepairSend::Installed => {
                    return Ok(built.manifest.boundary.index)
                }
                crate::server::replication::network::RepairSend::Behind { checkpoint } => {
                    event!(Level::INFO, msg = "the target's checkpoint is past the cut; moving this shard's past it", op = %self.op, group = %self.group, %target, checkpoint, ours = self.state.borrow().checkpoint_index());
                    self.advance_past(checkpoint).await?;
                    at_least = checkpoint + 1;
                }
            }
        }
        Err(format!("{target}'s checkpoint outran {CUTS_AT_MOST} cuts"))
    }

    /// Move this shard's checkpoint for the group past an index
    ///
    /// An entry is proposed so the group has a frame past the index, the WAL is rotated and
    /// swept so the segment is handed to the compactor, and the checkpoint is polled until it
    /// passes or the scrub deadline does.
    ///
    /// # Arguments
    ///
    /// * `past` - The index to pass
    async fn advance_past(&self, past: u64) -> Result<(), String> {
        let started = Instant::now();
        while self.state.borrow().checkpoint_index() <= past {
            if started.elapsed() > self.timeout {
                return Err(format!(
                    "the checkpoint did not pass {past} within {:?}",
                    self.timeout
                ));
            }
            // an entry past the index: a scrub nobody polls, so no replica cuts it
            let written = glommio::timer::timeout(self.timeout, async {
                Ok(self
                    .raft
                    .client_write(Command::scrub(
                        self.table,
                        crate::server::replication::digest::NUDGE,
                    ))
                    .await)
            })
            .await;
            match written {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => return Err(format!("proposing a nudge: {error}")),
                Err(_) => return Err("the nudge did not commit in time".to_string()),
            }
            let (reply, done) = oneshot::channel();
            self.loop_tx
                .send(ServerMsg::RepairRotate { reply })
                .await
                .map_err(|error| format!("{error:?}"))?;
            let _ = done.await;
            // the compactor's merge moves the checkpoint on its own time
            let waited = Instant::now();
            while self.state.borrow().checkpoint_index() <= past
                && waited.elapsed() < Duration::from_secs(5)
            {
                glommio::timer::sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }

    /// Commit where the group stands
    ///
    /// # Arguments
    ///
    /// * `progress` - Where it stands
    async fn commit(&self, progress: GroupRepair) -> Result<(), String> {
        let started = Instant::now();
        let mut last = String::new();
        // a progress is idempotent, so a proposal that found no control leader - one being
        // elected after a kill - is sent again until the deadline
        while started.elapsed() < PROGRESS_TIMEOUT {
            let (reply, rx) = kanal::bounded(1);
            let command = ControlCommand::RepairProgress {
                op: self.op,
                group: self.group,
                node: self.me.node,
                incarnation: self.incarnation,
                progress: progress.clone(),
            };
            self.control
                .try_send(ControlRequest::Propose { command, reply })
                .map_err(|_| "the control thread is not taking proposals".to_string())?;
            let remaining = PROGRESS_TIMEOUT.saturating_sub(started.elapsed());
            let answered =
                glommio::timer::timeout(remaining, async { Ok(rx.as_async().recv().await) }).await;
            last = match answered {
                Ok(Ok(Ok(ControlResponse::Applied { .. }))) => return Ok(()),
                Ok(Ok(Ok(ControlResponse::Refused { reason, .. }))) => {
                    return Err(format!("the progress was refused: {reason}"))
                }
                Ok(Ok(Ok(other))) => format!("the progress was not applied: {other:?}"),
                Ok(Ok(Err(error))) => error,
                Ok(Err(_)) => "the control thread dropped the proposal".to_string(),
                Err(_) => break,
            };
            glommio::timer::sleep(Duration::from_millis(500)).await;
        }
        Err(format!(
            "the progress was not committed within {PROGRESS_TIMEOUT:?}: {last}"
        ))
    }

    /// Tell a member what to do with its copy's quarantine, and wait until it has
    ///
    /// # Arguments
    ///
    /// * `member` - The member
    /// * `action` - What to do
    async fn quarantine(&self, member: ShardAddr, action: QuarantineAction) -> Result<(), String> {
        if member == self.me {
            // this shard's own copy, through the loop
            let (reply, done) = oneshot::channel();
            self.loop_tx
                .send(ServerMsg::Quarantine {
                    group: self.group,
                    action,
                    reply: Some(reply),
                })
                .await
                .map_err(|error| format!("{error:?}"))?;
            return done
                .await
                .unwrap_or_else(|_| Err("the loop dropped the quarantine".to_string()));
        }
        let peer = ShardPeer::new(member, self.network.clone());
        let payload = postcard::to_allocvec(&action).map_err(|error| error.to_string())?;
        match peer
            .quarantine(self.group, payload, QUARANTINE_TIMEOUT)
            .await
        {
            Ok(_) => Ok(()),
            Err(failure) => Err(format!("{member} did not take the quarantine: {failure}")),
        }
    }
}

/// Drive one group's repair from where the record stands to where this driver can take it
///
/// A release lifts the quarantines the record's groups hold and is done. Otherwise the scrub is
/// proposed and polled, the reports judged, the copies the verdict names quarantined - this
/// shard's own through the loop, a peer's over the lane - and the judgement committed. A verify
/// is done there; a repair goes on to the install
/// ([F44](../../../../docs/src/features/repair.md)). Every step is committed before the next so
/// a driver that dies leaves a phase the next leader resumes from.
///
/// # Arguments
///
/// * `context` - Everything the driver needs
pub async fn drive_group<D: ShoalDatabase>(context: DriverContext<D>) {
    let outcome = drive_group_inner(&context).await;
    let mut phase = match &outcome {
        Ok(phase) => phase.clone(),
        Err(_) => RepairPhase::Done,
    };
    if let Err(error) = outcome {
        // a driver that lost the lead leaves the group for the new leader
        if error.starts_with(NOT_LEADER) {
            phase = RepairPhase::Pending;
            event!(Level::INFO, msg = "a group's repair is left for its new leader", op = %context.op, group = %context.group, error);
            let _ = context
                .commit(GroupRepair {
                    phase: RepairPhase::Pending,
                    driver: None,
                    boundary: None,
                    reports: Vec::new(),
                    outcome: None,
                })
                .await;
        } else {
            event!(Level::ERROR, msg = "a group's repair failed", op = %context.op, group = %context.group, error);
            let _ = context
                .commit(GroupRepair {
                    phase: RepairPhase::Done,
                    driver: Some(context.me.node),
                    boundary: None,
                    reports: Vec::new(),
                    outcome: Some(RepairOutcome::Failed { reason: error }),
                })
                .await;
        }
    }
    let _ = context
        .loop_tx
        .send(ServerMsg::RepairDone {
            op: context.op,
            group: context.group,
            phase,
        })
        .await;
}

/// The phases, each committed before the next
///
/// # Arguments
///
/// * `context` - Everything the driver needs
async fn drive_group_inner<D: ShoalDatabase>(
    context: &DriverContext<D>,
) -> Result<RepairPhase, String> {
    let me = context.me.node;
    // a release: every member lifts, and the group is done
    if context.release {
        for member in &context.members {
            context
                .quarantine(*member, QuarantineAction::Lift { op: None })
                .await?;
        }
        event!(Level::INFO, msg = "released a group's quarantines, as the operator asked", op = %context.op, group = %context.group);
        context
            .commit(GroupRepair {
                phase: RepairPhase::Done,
                driver: Some(me),
                boundary: None,
                reports: Vec::new(),
                outcome: Some(RepairOutcome::Released),
            })
            .await?;
        return Ok(RepairPhase::Done);
    }
    // the scrub: said first, so a driver that dies here is visibly the one that was scrubbing
    context
        .commit(GroupRepair {
            phase: RepairPhase::Scrubbing,
            driver: Some(me),
            boundary: None,
            reports: Vec::new(),
            outcome: None,
        })
        .await?;
    let scrub = scrub_group(
        &context.raft,
        &context.network,
        context.me,
        context.state.clone(),
        context.table,
        context.group,
        &context.members,
        context.op,
        context.timeout,
    )
    .await?;
    // the judgement, and the quarantines it names
    let verdict = judge(&context.members, &scrub, context.source);
    for (member, reason) in &verdict.quarantine {
        event!(Level::WARN, msg = "quarantining a copy", op = %context.op, group = %context.group, member = %member, reason = reason.as_str());
        context
            .quarantine(
                *member,
                QuarantineAction::Set(Quarantine {
                    reason: *reason,
                    at: scrub.boundary,
                    op: context.op,
                }),
            )
            .await?;
    }
    let reports: Vec<(ShardAddr, Result<DigestSummary, String>)> = scrub
        .reports
        .iter()
        .map(|(member, report)| {
            let summary = report.as_ref().map(|report| DigestSummary {
                digest: report.digest,
                rows: report.rows,
                partitions: report.partitions,
                checksum_failures: match report.integrity {
                    DigestIntegrity::Verified => 0,
                    DigestIntegrity::Invalid { checksum_failures } => checksum_failures,
                },
            });
            (*member, summary.map_err(Clone::clone))
        })
        .collect();
    event!(Level::INFO, msg = "judged a group's copies", op = %context.op, group = %context.group, boundary = scrub.boundary, outcome = ?verdict.outcome);
    // a repair lifts the quarantine of a copy that holds the trusted digest, verified: one a
    // crashed install left quarantined by its marker, or one an operator held back. A verify
    // never lifts anything
    if context.mode == RepairMode::Repair && verdict.trusted.is_some() {
        for member in &verdict.trusted_members {
            context
                .quarantine(*member, QuarantineAction::Lift { op: None })
                .await?;
        }
    }
    // a verify is done at the judgement; so is a repair with nothing to repair
    let repairable = context.mode == RepairMode::Repair
        && matches!(verdict.outcome, RepairOutcome::Divergent { .. });
    context
        .commit(GroupRepair {
            phase: if repairable {
                RepairPhase::Judged
            } else {
                RepairPhase::Done
            },
            driver: Some(me),
            boundary: Some(scrub.boundary),
            reports: reports.clone(),
            outcome: Some(verdict.outcome.clone()),
        })
        .await?;
    if !repairable {
        return Ok(RepairPhase::Done);
    }
    // the source is the leader: a leader whose own copy is not trusted hands the lead to a
    // member whose copy is, and that member resumes the record from here
    if !verdict.trusted_members.contains(&context.me) {
        let Some(trusted) = verdict.trusted_members.first().copied() else {
            return Err("no verified member holds the trusted digest".to_string());
        };
        event!(Level::WARN, msg = "this leader's copy is not trusted; transferring the lead to a verified member", op = %context.op, group = %context.group, to = %trusted);
        context
            .commit(GroupRepair {
                phase: RepairPhase::Pending,
                driver: None,
                boundary: Some(scrub.boundary),
                reports,
                outcome: Some(verdict.outcome.clone()),
            })
            .await?;
        context
            .raft
            .trigger()
            .transfer_leader(trusted)
            .await
            .map_err(|error| format!("transferring the lead to {trusted}: {error}"))?;
        return Ok(RepairPhase::Pending);
    }
    // every quarantined copy in turn: a durable one from a snapshot cut past its checkpoint,
    // a volatile one by restarting it empty for the leader to feed
    let targets: Vec<ShardAddr> = verdict
        .quarantine
        .iter()
        .map(|(member, _)| *member)
        .collect();
    let mut boundary = scrub.boundary;
    for target in &targets {
        context
            .commit(GroupRepair {
                phase: RepairPhase::Installing {
                    source: context.me,
                    target: *target,
                },
                driver: Some(me),
                boundary: Some(scrub.boundary),
                reports: reports.clone(),
                outcome: Some(verdict.outcome.clone()),
            })
            .await?;
        if context.volatile {
            context
                .quarantine(*target, QuarantineAction::Rebuild)
                .await?;
            continue;
        }
        boundary = context.install_on(*target).await?;
    }
    // the second round: what was installed has to agree now
    context
        .commit(GroupRepair {
            phase: RepairPhase::Verifying,
            driver: Some(me),
            boundary: Some(boundary),
            reports: reports.clone(),
            outcome: Some(verdict.outcome.clone()),
        })
        .await?;
    let second = scrub_group(
        &context.raft,
        &context.network,
        context.me,
        context.state.clone(),
        context.table,
        context.group,
        &context.members,
        context.op,
        context.timeout,
    )
    .await?;
    let after = judge(&context.members, &second, context.source);
    // a copy that now holds the trusted digest, verified, is no longer divergent
    let mut lifted = Vec::new();
    if after.trusted.is_some() {
        for member in &after.trusted_members {
            context
                .quarantine(*member, QuarantineAction::Lift { op: None })
                .await?;
            lifted.push(*member);
        }
    }
    let repaired = targets
        .iter()
        .all(|target| after.trusted_members.contains(target));
    let outcome = if repaired {
        RepairOutcome::Repaired {
            source: context.me,
            targets: targets.clone(),
            boundary,
            verified: second.boundary,
        }
    } else {
        RepairOutcome::Failed {
            reason: format!(
                "after the install the copies still disagree: {:?}",
                after.outcome
            ),
        }
    };
    event!(Level::INFO, msg = "a group's repair is done", op = %context.op, group = %context.group, ?outcome, lifted = lifted.len());
    let reports: Vec<(ShardAddr, Result<DigestSummary, String>)> = second
        .reports
        .iter()
        .map(|(member, report)| {
            let summary = report.as_ref().map(|report| DigestSummary {
                digest: report.digest,
                rows: report.rows,
                partitions: report.partitions,
                checksum_failures: match report.integrity {
                    DigestIntegrity::Verified => 0,
                    DigestIntegrity::Invalid { checksum_failures } => checksum_failures,
                },
            });
            (*member, summary.map_err(Clone::clone))
        })
        .collect();
    context
        .commit(GroupRepair {
            phase: RepairPhase::Done,
            driver: Some(me),
            boundary: Some(second.boundary),
            reports,
            outcome: Some(outcome),
        })
        .await?;
    Ok(RepairPhase::Done)
}

impl<D: ShoalDatabase> super::Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// The quarantined group a query's keys fall in, if any of them does
    ///
    /// # Arguments
    ///
    /// * `query` - The query
    pub(super) fn quarantined_group(
        &self,
        query: &<D::ClientType as crate::shared::traits::QuerySupport>::QueryKinds,
    ) -> Option<(GroupId, QuarantineReason)> {
        use crate::shared::traits::{QuerySupport as _, ShoalQuerySupport as _, TableNameSupport as _};
        let replication = self.replication.as_ref()?;
        let table = D::ClientType::query_table_name(query).table_id();
        for key in query.partition_keys() {
            // truncation cannot happen: a tablet id is twelve bits
            #[allow(clippy::cast_possible_truncation)]
            let tablet = crate::server::ring::Ring::tablet_of(*key) as u16;
            let Some(group) = replication.tablets.get(&(table, tablet)) else {
                continue;
            };
            let quarantined = replication
                .groups
                .get(group)
                .and_then(|slot| slot.state.borrow().quarantined.map(|quarantine| quarantine.reason));
            if let Some(reason) = quarantined {
                return Some((*group, reason));
            }
        }
        None
    }

    /// Quarantine the copy a record that failed its checksum belongs to
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `partition_id` - The partition whose record failed
    pub(super) async fn quarantine_for_checksum(&mut self, table: D::TableNames, partition_id: u64) {
        use crate::shared::traits::TableNameSupport as _;
        // truncation cannot happen: a tablet id is twelve bits
        #[allow(clippy::cast_possible_truncation)]
        let tablet = crate::server::ring::Ring::tablet_of(partition_id) as u16;
        let group = self
            .replication
            .as_ref()
            .and_then(|replication| replication.tablets.get(&(table.table_id(), tablet)).copied());
        let Some(group) = group else {
            return;
        };
        let at = self
            .replication
            .as_ref()
            .and_then(|replication| replication.groups.get(&group))
            .map_or(0, |slot| slot.state.borrow().applied_index());
        let quarantine = Quarantine {
            reason: QuarantineReason::Checksum,
            at,
            op: Uuid::nil(),
        };
        self.handle_quarantine(group, QuarantineAction::Set(quarantine), None).await;
    }

    /// Quarantine a copy this shard holds, or lift it, and make the marker durable
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `action` - What to do
    /// * `reply` - Where to say it is done, if anybody waits
    pub(super) async fn handle_quarantine(
        &mut self,
        group: GroupId,
        action: QuarantineAction,
        reply: Option<oneshot::Sender<Result<(), String>>>,
    ) {
        let outcome = self.apply_quarantine(group, action).await;
        if let Err(error) = &outcome {
            event!(Level::ERROR, msg = "a quarantine could not be applied", group = %group, ?action, error);
        }
        if let Some(reply) = reply {
            let _ = reply.send(outcome);
        }
    }

    /// The quarantine itself: the state, the marker, the counter and the report
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `action` - What to do
    async fn apply_quarantine(&mut self, group: GroupId, action: QuarantineAction) -> Result<(), String> {
        let Some(replication) = self.replication.as_mut() else {
            return Err("this node hosts no tablet groups".to_string());
        };
        let Some(slot) = replication.groups.get(&group) else {
            return Err(format!("group {group} is not hosted on this shard"));
        };
        let state = slot.state.clone();
        let wal_dir = replication.wal_dir.clone();
        match action {
            QuarantineAction::Set(quarantine) => {
                // already quarantined is already quarantined, and the first reason stands -
                // except that a copy which stalled on an unreadable partition says so over any
                // other reason, since that is the one that has it repaired without an operator
                // ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md))
                let current = state.borrow().quarantined.map(|current| current.reason);
                match current {
                    None => {}
                    Some(QuarantineReason::Unreadable) => return Ok(()),
                    Some(_) if quarantine.reason != QuarantineReason::Unreadable => return Ok(()),
                    Some(_) => {}
                }
                super::repair::write_quarantine(&wal_dir, group, &quarantine)
                    .await
                    .map_err(|error| format!("writing the quarantine marker: {error}"))?;
                state.borrow_mut().quarantined = Some(quarantine);
                // a reason replaced is the same copy quarantined, and is not counted twice
                if current.is_none() {
                    replication.integrity.quarantined += 1;
                }
                event!(Level::ERROR, msg = "quarantined this shard's copy of a group", group = %group, reason = quarantine.reason.as_str(), at = quarantine.at, op = %quarantine.op);
            }
            QuarantineAction::Rebuild => {
                // the copy is rebuilt from the leader; the quarantine goes with it, since what
                // it held is gone
                self.rebuild_group_empty(group)?;
                let replication = self.replication.as_mut().expect("still here");
                super::repair::clear_quarantine(&wal_dir, group)
                    .await
                    .map_err(|error| format!("removing the quarantine marker: {error}"))?;
                replication.last_report = None;
                return Ok(());
            }
            QuarantineAction::Lift { op } => {
                let current = state.borrow().quarantined;
                let Some(current) = current else {
                    return Ok(());
                };
                // a lift names the operation that decided the quarantine, or any
                if op.is_some_and(|op| op != current.op) {
                    return Ok(());
                }
                super::repair::clear_quarantine(&wal_dir, group)
                    .await
                    .map_err(|error| format!("removing the quarantine marker: {error}"))?;
                state.borrow_mut().quarantined = None;
                event!(Level::INFO, msg = "lifted the quarantine on this shard's copy of a group", group = %group, was = current.reason.as_str());
            }
        }
        // the change reaches the control thread on the next tick
        replication.last_report = None;
        Ok(())
    }

    /// Forget a driver that finished, and look for the next group to drive
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `group` - The group
    pub(super) fn handle_repair_done(&mut self, op: Uuid, group: GroupId, phase: RepairPhase) {
        if let Some(replication) = self.replication.as_mut() {
            replication.driving.remove(&(op, group));
            // the map is behind the commit for a moment; what was committed is what counts
            replication.driven.insert((op, group), phase);
        }
        self.drive_repairs();
    }

    /// Start a driver for every pending group of every pending record this shard leads
    ///
    /// Up to `cluster.repair.concurrent` at a time. A group whose phase is pending, or
    /// scrubbing under a driver that is not running here - the previous leader, or this
    /// process before a restart - is this shard's to drive when its handle leads
    /// ([F44](../../../../docs/src/features/repair.md)).
    pub(super) fn drive_repairs(&mut self) {
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let incarnation = self.local.as_ref().map_or(0, |local| local.borrow().incarnation);
        let repair = self.conf.cluster.as_ref().map(|cluster| cluster.repair.clone()).unwrap_or_default();
        let snapshot_timeout = self
            .conf
            .cluster
            .as_ref()
            .map_or(Duration::from_secs(300), |cluster| cluster.replication.snapshot_timeout.duration());
        let loop_tx = self.shard_local_tx.clone();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        // what this shard committed for a record the map no longer carries is forgotten
        replication
            .driven
            .retain(|(op, _), _| map.repairs.iter().any(|record| record.op == *op));
        for record in &map.repairs {
            for (group, progress) in &record.groups {
                if replication.driving.len() >= repair.concurrent as usize {
                    return;
                }
                if replication.driving.contains(&(record.op, *group)) {
                    continue;
                }
                // the phase as this shard last committed it, when the map is behind it
                let phase = match replication.driven.get(&(record.op, *group)) {
                    Some(driven) if driven.rank() > progress.phase.rank() => driven,
                    _ => &progress.phase,
                };
                if !matches!(phase, RepairPhase::Pending | RepairPhase::Scrubbing) {
                    continue;
                }
                let Some(slot) = replication.groups.get(group) else {
                    continue;
                };
                let Some(raft) = slot.raft.clone() else {
                    continue;
                };
                // only the leader drives, and only once per group at a time; this node's
                // member is the slot hosting the group ([F47](../../../../docs/src/features/local-rehome.md))
                let me = slot.spec.me(node);
                let leads = raft.metrics().borrow_watched().current_leader == Some(me);
                if !leads {
                    continue;
                }
                replication.driving.insert((record.op, *group));
                event!(Level::INFO, msg = "driving a group's repair", op = %record.op, group = %group, mode = record.mode.as_str(), release = record.release);
                let context = DriverContext {
                    raft,
                    network: replication.network.clone(),
                    me,
                    incarnation,
                    state: slot.state.clone(),
                    table: slot.spec.table,
                    group: *group,
                    members: slot.spec.members.clone(),
                    op: record.op,
                    mode: record.mode,
                    source: record.source,
                    release: record.release,
                    timeout: repair.timeout.duration(),
                    control: control.clone(),
                    loop_tx: loop_tx.clone(),
                    volatile: slot.store.is_volatile(),
                    snapshot_timeout,
                };
                glommio::spawn_local(drive_group(context)).detach();
            }
        }
    }

    /// Ask for the repair of every group this shard leads whose member stalled on a read
    ///
    /// A copy that could not read a partition for a replicated apply stops applying and is
    /// committed quarantined as unreadable. It needs no judgement, since it said so itself, so
    /// its leader asks for a `Repair` of the group as the process, once a group has no repair
    /// or move open and at most every [`STALL_REPAIR_RETRY`]
    /// ([Resolved #160](../../../../docs/src/appendix/resolved/unreadable-partition-stalls-one-copy.md)).
    pub(super) fn repair_stalled_copies(&mut self) {
        let enabled = self
            .conf
            .cluster
            .as_ref()
            .is_some_and(|cluster| cluster.repair.unreadable);
        if !enabled {
            return;
        }
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let now = Instant::now();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let mut due = Vec::new();
        for (group, slot) in &replication.groups {
            // only a group's leader asks
            let Some(raft) = slot.raft.as_ref() else {
                continue;
            };
            if raft.metrics().borrow_watched().current_leader != Some(slot.spec.me(node)) {
                continue;
            }
            // a member whose copy of this group is committed unreadable
            let stalled = map.members.values().any(|member| {
                member.quarantined.iter().any(|copy| {
                    copy.group == *group && copy.reason == QuarantineReason::Unreadable
                })
            });
            if !stalled {
                continue;
            }
            // not while a repair or a move of the group is open, and not too often
            let pending = map
                .repairs
                .iter()
                .any(|record| record.groups.get(group).is_some_and(|progress| !progress.is_done()));
            let moving = map.moves.iter().any(|record| !record.is_done() && record.groups.contains_key(group));
            let waiting = replication
                .next_stall_repair
                .get(group)
                .is_some_and(|next| now < *next);
            if pending || moving || waiting {
                continue;
            }
            due.push((*group, slot.spec.table, slot.spec.tablets.first().copied().unwrap_or(0)));
        }
        for (group, table, tablet) in due {
            replication.next_stall_repair.insert(group, now + STALL_REPAIR_RETRY);
            let (reply, _rx) = kanal::bounded(1);
            let command = ControlCommand::Repair {
                op: Uuid::new_v4(),
                principal: "stalled-copy".to_string(),
                expected_version: map.version,
                table,
                tablet: Some(tablet),
                mode: RepairMode::Repair,
                source: None,
                release: false,
            };
            event!(Level::WARN, msg = "asking for the repair of a member that stalled on an unreadable partition", group = %group, table = %table);
            let _ = control.try_send(ControlRequest::Propose { command, reply });
        }
    }

    /// Propose a scheduled scrub of every group this shard leads that is due one
    ///
    /// With `cluster.repair.scrub_interval` set, every group is verified on the interval,
    /// staggered by its identity, under an operation recorded like an operator's - as the
    /// process, in verify mode, never installing ([F44](../../../../docs/src/features/repair.md), Q12).
    pub(super) fn schedule_scrubs(&mut self) {
        let Some(interval) = self
            .conf
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.repair.scrub_interval.as_ref())
            .map(|spec| spec.duration())
        else {
            return;
        };
        let map = self.map.get();
        let node = self.node_id();
        let Some(control) = self.control.clone() else {
            return;
        };
        let now = Instant::now();
        let Some(replication) = self.replication.as_mut() else {
            return;
        };
        let mut due = Vec::new();
        for (group, slot) in &replication.groups {
            let Some(raft) = slot.raft.as_ref() else {
                continue;
            };
            if raft.metrics().borrow_watched().current_leader != Some(slot.spec.me(node)) {
                continue;
            }
            // the first pass is staggered across the interval by the group's identity
            let next = *replication.next_scrub.entry(*group).or_insert_with(|| {
                let stagger = interval.mul_f64(f64::from(u32::try_from(group.0 % 997).unwrap_or(0)) / 997.0);
                now + stagger
            });
            if now < next {
                continue;
            }
            // a group already under a pending record waits for it, and so does one whose set
            // is under a move ([F45](../../../../docs/src/features/replica-migration.md))
            let pending = map
                .repairs
                .iter()
                .any(|record| record.groups.get(group).is_some_and(|progress| !progress.is_done()));
            let moving = map.moves.iter().any(|record| !record.is_done() && record.groups.contains_key(group));
            if pending || moving {
                continue;
            }
            due.push((*group, slot.spec.table, slot.spec.tablets.first().copied().unwrap_or(0)));
        }
        for (group, table, tablet) in due {
            replication.next_scrub.insert(group, now + interval);
            let (reply, _rx) = kanal::bounded(1);
            let command = ControlCommand::Repair {
                op: Uuid::new_v4(),
                principal: "scheduler".to_string(),
                expected_version: map.version,
                table,
                tablet: Some(tablet),
                mode: RepairMode::Verify,
                source: None,
                release: false,
            };
            event!(Level::INFO, msg = "proposing a scheduled scrub", group = %group, table = %table);
            let _ = control.try_send(ControlRequest::Propose { command, reply });
        }
    }
}
