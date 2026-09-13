//! The control plane: the thread, the group of one, and the state it keeps
//!
//! A cluster node runs one thread beside its shards, pinned to its own core, that owns the
//! embedded control group ([C3](../../../../docs/src/distributed/membership.md)) and everything
//! that will hang off it - the failure detector, the rebalancer, the admin operations. At
//! [M1](../../../../docs/src/distributed/milestones.md#m1-node-identity-and-the-control-plane-thread)
//! it owns a group of one member, itself, and the state that group commits is the cluster's
//! identity, its member list and the replication policy the bootstrap wrote down.
//!
//! The pieces, each its own module:
//!
//! - [`cores`] decides where the thread runs and what that costs the shards
//! - [`runtime`] is the glommio [`AsyncRuntime`](openraft_rt::AsyncRuntime) openraft drives on
//! - [`types`] declares the Raft type configuration and the commands and state it carries
//! - [`store`] is the durable log and state machine, under the latency sensitive path
//! - [`network`] is the factory that dials the committed member records over the control lane
//! - [`listener`] accepts control peers and hands the membership RPCs to the loop
//! - [`plane`] starts the thread, runs the group, joins, promotes, and answers the pool
//!
//! Standalone mode touches none of this. A server without a `cluster:` block spawns no thread,
//! opens no group and writes no file under `control/`, and the shard path is unchanged.

pub mod cores;
pub mod detector;
pub mod listener;
pub mod network;
pub mod plane;
pub mod repair;
pub mod runtime;
pub mod store;
pub mod types;

pub use cores::ControlPlacement;
pub use plane::{
    AdminCall, ControlEvent, ControlHandle, ControlPlane, ControlRequest, DataReadiness,
    JoinStatus, MapSink, ReadinessView, ShardHealthEvent, TopologyView, VoteProbe,
};
