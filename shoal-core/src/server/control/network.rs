//! The control group's network, which at M1 reaches nobody
//!
//! A group of one never sends: there is no other member to replicate to, vote with or send a
//! snapshot. openraft still asks for a network factory, since a membership change could add a
//! member at any time, and this is the one that says every peer is unreachable. M2 replaces it
//! with the transport ([C2](../../../../docs/src/distributed/transport.md)); until then a
//! membership change that named a second node would find it unreachable, which is the honest
//! answer.

use std::future::Future;

use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2};

use super::store::SnapshotData;
use super::types::{ControlConfig, MemberRecord};
use crate::shared::identity::NodeId;

/// Why no peer can be reached
#[derive(Debug)]
struct NoTransport {
    /// The peer that was asked for
    target: NodeId,
}

impl std::fmt::Display for NoTransport {
    /// Say which peer, and why
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no transport reaches {}: the inter-node transport is M2's",
            self.target
        )
    }
}

impl std::error::Error for NoTransport {}

/// The factory: every client it makes is an [`Unreachable`]
#[derive(Debug, Default, Clone)]
pub struct UnreachableNetwork;

impl RaftNetworkFactory<ControlConfig> for UnreachableNetwork {
    type Network = UnreachablePeer;

    /// A client for a peer, which will refuse every call
    async fn new_client(&mut self, target: NodeId, _node: &MemberRecord) -> Self::Network {
        UnreachablePeer { target }
    }
}

/// A peer nothing can reach
#[derive(Debug)]
pub struct UnreachablePeer {
    /// Who it would be
    target: NodeId,
}

impl UnreachablePeer {
    /// The error every call answers with
    fn unreachable(&self) -> RPCError<ControlConfig> {
        RPCError::Unreachable(Unreachable::new(&NoTransport {
            target: self.target,
        }))
    }
}

impl RaftNetworkV2<ControlConfig> for UnreachablePeer {
    type SnapshotData = SnapshotData;

    /// Refused: nothing carries it
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<ControlConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<ControlConfig>, RPCError<ControlConfig>> {
        Err(self.unreachable())
    }

    /// Refused: nothing carries it
    async fn vote(
        &mut self,
        _rpc: VoteRequest<ControlConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlConfig>, RPCError<ControlConfig>> {
        Err(self.unreachable())
    }

    /// Refused: nothing carries it
    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<ControlConfig>,
        _snapshot: SnapshotOf<ControlConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<ControlConfig>, StreamingError<ControlConfig>> {
        Err(StreamingError::Unreachable(Unreachable::new(&NoTransport {
            target: self.target,
        })))
    }
}
