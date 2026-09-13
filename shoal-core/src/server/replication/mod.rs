//! The data plane's replication: tablet groups on the shards
//!
//! Every tablet is served by an embedded Raft group on the data shards
//! ([F40](../../../../docs/src/features/replication.md)): tablets whose replicas land on the
//! same ordered list of shards share one group, a write is a [`Command`](crate::shared::protocol::peer::Command)
//! the group's leader proposes and every member applies in committed order, and the group's log
//! is the shard's WAL ([`wal`](crate::server::wal)). This module holds the type configuration
//! the groups run under, the state machine that hands committed entries to the shard loop, and
//! the network that carries the group's RPCs over the replication lane.

pub mod machine;
pub mod network;
pub mod proposal;
pub mod report;
pub mod types;

pub use machine::{GroupMachine, MachineState, SnapshotData};
pub use network::{GroupNetwork, GroupPeer, ReplicationLink, RpcFailure, ShardNetwork, ShardPeer};
pub use proposal::{BarrierAnswer, ProposalOutcome};
pub use report::{GroupReport, NodeReplication, ReadStats, ReadVerb, ReplicationVerb, ShardReplication};
pub use types::{ApplyOutcome, CommandResult, DataConfig, Remembered, ResultKind};
