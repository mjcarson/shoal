//! What proposing a write through a tablet group can come to
//!
//! A write on a cluster node is a [`Command`](crate::shared::protocol::peer::Command) proposed
//! through the group that serves its tablet, and the client is owed exactly one of the outcomes
//! [C5](../../../../docs/src/distributed/replication.md) promises: the result, a definite
//! refusal it may retry at once, or an unknown outcome it may only retry under the same
//! identity ([F40](../../../../docs/src/features/replication.md)). The distinction between the
//! last two is the whole point of this type: a refusal means nothing accepted the command, an
//! unknown means the leader may have.

use serde::{Deserialize, Serialize};

use super::types::ApplyOutcome;

/// What a proposal came to
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalOutcome {
    /// The group applied it, or refused it as a group, and this is what it said
    Answered {
        /// What applying it produced
        outcome: ApplyOutcome,
        /// The log index it was committed at, which the proposer waits to see applied locally
        index: u64,
    },
    /// Admission refused it before it reached the group: a definite refusal
    ///
    /// The group's pending bytes, or a volatile log's bound, would have passed its limit.
    Shed(String),
    /// No leader could take it within the deadline: a definite refusal
    ///
    /// The group is electing, or this node cannot reach the leader. Nothing was appended.
    NotLeader(String),
    /// The leader took it and did not answer within the deadline: the outcome is unknown
    Unknown(String),
    /// The group is stopped or the write failed some other way: a definite refusal
    Failed(String),
}
