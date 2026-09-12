//! What a tablet group carries: its type configuration and what applying a command produces
//!
//! openraft is generic over one type that names everything else, and [`DataConfig`] is that
//! type for every tablet group ([F40](../../../../docs/src/features/replication.md)). The
//! application data is a [`Command`] - a table, a tablet, a request identity and the table's
//! serialized intent - the response is an [`ApplyOutcome`], a member is named by its
//! [`ShardAddr`] and described by nothing more, and the runtime is the glommio one the control
//! group already runs on. The rest are openraft's defaults, the same ones the control group uses.
//!
//! The control group's configuration is not reused because the two groups agree about nothing:
//! a control member is a node and a data member is a shard, a control command is JSON and a
//! data command is bytes forwarded as they were, and a control response is a topology version
//! where a data response is whether an insert, a delete or an update succeeded.

use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};

use crate::server::control::runtime::GlommioRuntime;
use crate::shared::identity::ShardAddr;
use crate::shared::protocol::peer::Command;

declare_raft_types!(
    /// A tablet group's type configuration
    pub DataConfig:
        D = Command,
        R = ApplyOutcome,
        NodeId = ShardAddr,
        Node = ShardAddr,
        AsyncRuntime = GlommioRuntime,
);

/// Which kind of write a command was, which decides the response variant it answers in
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultKind {
    /// A row was inserted
    Insert,
    /// A row was deleted, or was not there to delete
    Delete,
    /// A row was updated, or was not there to update
    Update,
}

/// What applying one command produced, derived in committed order on every replica
///
/// The `bool` is what the table answers today - an insert always succeeds, a delete and an
/// update succeed when the row was there - computed against the state every earlier committed
/// command left, so every replica derives the same answer
/// ([C5](../../../../docs/src/distributed/replication.md), "committed-order results").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult {
    /// What kind of write it was
    pub kind: ResultKind,
    /// Whether it succeeded
    pub ok: bool,
}

/// What the group answers a proposal with
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyOutcome {
    /// The command was applied, and this is its result
    Applied(CommandResult),
    /// The command's identity was seen before, and this is the result it produced then
    ///
    /// The command was not applied a second time; a retry of a committed write is answered as
    /// the first was.
    Duplicate(CommandResult),
    /// The command could not be applied, and this is why
    ///
    /// A payload that does not decode, a table the schema does not have, or an identity reused
    /// with a different payload. A refusal is applied like any other entry - the log moved - and
    /// the client is told; nothing about the table changed.
    Refused(String),
}

impl ApplyOutcome {
    /// The result, if the command was applied or was a repeat of one that was
    #[must_use]
    pub fn result(&self) -> Option<CommandResult> {
        match self {
            ApplyOutcome::Applied(result) | ApplyOutcome::Duplicate(result) => Some(*result),
            ApplyOutcome::Refused(_) => None,
        }
    }
}
