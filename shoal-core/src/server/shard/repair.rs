//! A scrub of one tablet group: the entry proposed, and every member's digest polled
//!
//! The scrub phase of a repair ([F44](../../../../docs/src/features/repair.md)). The leader's
//! shard proposes the scrub entry through its own handle, which fixes the boundary `B` every
//! replica takes its canonical cut at, then asks every member over the replication lane for its
//! report until each has answered or the timeout passes. A member answers `Pending` while its
//! cut's task is still reading, and `Unknown` for a scrub it never applied - a member that was
//! down when the entry committed applies it when it catches up, and is polled until then.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use uuid::Uuid;

use crate::server::database::ShoalDatabase;
use crate::server::replication::{DataConfig, DigestAnswer, DigestReport, GroupMachine, MachineState, ShardNetwork, ShardPeer};
use crate::shared::identity::{GroupId, ShardAddr, TableId};
use crate::shared::protocol::peer::Command;

/// How long to wait between polls of a member whose report is not in yet
const DIGEST_POLL: Duration = Duration::from_millis(100);

/// What a scrub of one group came to
#[derive(Debug, Clone)]
pub struct ScrubOutcome {
    /// The operation
    pub op: Uuid,
    /// The index the scrub committed at, which every report is of
    pub boundary: u64,
    /// Every member's report, or why there is none from it
    pub reports: BTreeMap<ShardAddr, Result<DigestReport, String>>,
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
    let written = glommio::timer::timeout(timeout, async { Ok(raft.client_write(Command::scrub(table, op)).await) }).await;
    let boundary = match written {
        Err(_) => return Err(format!("group {group} did not commit the scrub within {timeout:?}")),
        Ok(Ok(response)) => response.log_id.index,
        Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward)))) => {
            return Err(format!(
                "this shard does not lead group {group}: the leader is {:?}",
                forward.leader_node.or(forward.leader_id)
            ))
        }
        Ok(Err(error)) => return Err(format!("proposing the scrub of group {group}: {error}")),
    };
    // every member's report, polled until it is in or the time is up
    let mut reports: BTreeMap<ShardAddr, Result<DigestReport, String>> = BTreeMap::new();
    let mut last: BTreeMap<ShardAddr, String> = BTreeMap::new();
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
                    Ok(bytes) => postcard::from_bytes::<DigestAnswer>(&bytes).map_err(|error| format!("decoding a digest answer: {error}")),
                    Err(failure) => Err(failure.to_string()),
                }
            };
            match answer {
                Ok(DigestAnswer::Report(report)) => {
                    reports.insert(*member, Ok(report));
                }
                Ok(DigestAnswer::Pending) => {
                    last.insert(*member, "the member's cut is still being read".to_string());
                }
                Ok(DigestAnswer::Unknown) => {
                    last.insert(*member, "the member has not applied the scrub".to_string());
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
                    let why = last.get(member).cloned().unwrap_or_else(|| "never asked".to_string());
                    reports.insert(*member, Err(format!("no report within {timeout:?}: {why}")));
                }
            }
            break;
        }
        glommio::timer::sleep(DIGEST_POLL).await;
    }
    Ok(ScrubOutcome { op, boundary, reports })
}
