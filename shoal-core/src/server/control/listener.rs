//! The control listener: accept control peers and drive their RPCs into the group
//!
//! Bound by the control thread on `advertise:control_port` and served on its executor, so a
//! stalled data shard cannot stall a vote ([C2](../../../../docs/src/distributed/transport.md)).
//! Every accepted connection runs its own task, reads `ControlRequest` frames, and answers each
//! under the id it carried. The consensus RPCs and pings are driven straight into this node's
//! `Raft` here; the membership RPCs - a joiner's admission, a member's report, a proposal for
//! the leader - are handed to the control loop as [`Inbound`] events and answered when it
//! answers them ([F39](../../../../docs/src/features/membership.md)), since only the loop
//! knows who leads and what it has promised.
//!
//! A connection the handshake let in as a **joiner** - no cluster, on this lane alone - may ask
//! to join and ping, and nothing else; anything else it sends is answered as an error and ends
//! the connection.

use futures::AsyncReadExt as _;
use futures_channel::oneshot;
use glommio::net::{TcpListener, TcpStream};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::Raft;
use rustls::ServerConfig;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{event, Level};

use super::network::decode_snapshot;
use super::store::ControlStateMachine;
use super::types::{ControlConfig, MemberHealth, MemberPhase};
use crate::server::peer::codec;
use crate::server::peer::handshake::{self, Accepted, Admission, Local, Verdict};
use crate::server::peer::Lane;
use crate::server::ServerError;
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::protocol::peer::{
    self, ControlKind, ControlRequestHead, ControlResponseHead, ControlStatus, CONTROL_HEAD_LEN,
};
use crate::shared::protocol::MessageType;

/// What a ping answers with
#[derive(Serialize)]
struct Pong {
    /// Which run of this node this is
    incarnation: u64,
    /// The topology version this node's group has committed
    topology_version: u64,
}

/// A membership RPC a peer sent, handed to the control loop with a way to answer it
pub struct Inbound {
    /// Which RPC it is
    pub kind: ControlKind,
    /// The peer that sent it
    pub peer: Accepted,
    /// Its serialized request
    pub payload: Vec<u8>,
    /// Where the answer goes, as the status and the payload to frame under the request's id
    pub reply: oneshot::Sender<(ControlStatus, Vec<u8>)>,
}

/// The control thread's judge: the applied state, and this node's own record of its cluster
///
/// A node whose state names no member yet - a joiner between admission and its first log
/// entry - trusts any member of the cluster it adopted, since the leader replicating to it is
/// the only thing that can ever tell it who its members are.
pub struct StateAdmission {
    /// The applied state
    pub machine: ControlStateMachine,
    /// What this node says about itself, whose cluster a joiner learns before its state does
    pub local: Rc<RefCell<Local>>,
}

impl Admission for StateAdmission {
    /// Judge a peer against the applied state
    fn judge(&self, node: NodeId, incarnation: u64, shards: u16) -> Verdict {
        let state = self.machine.state();
        if state.members.is_empty() {
            return Verdict::Member;
        }
        // a removed identity never comes back, by this door or any other
        // ([F49](../../../../docs/src/features/backup-and-recovery.md))
        if state.tombstones.contains_key(&node) || state.members.get(&node).is_some_and(|member| member.phase == MemberPhase::Removed) {
            return Verdict::Removed;
        }
        match state.members.get(&node) {
            None => Verdict::Unknown,
            Some(member) if incarnation < member.record.incarnation => Verdict::Fenced {
                committed: member.record.incarnation,
            },
            // a member that was fenced and is down is still a member at its last incarnation;
            // a shard count is what its record says
            Some(member)
                if member.health != MemberHealth::Joining
                    && usize::from(shards) != member.record.shards =>
            {
                // a node runs fewer shards than a u16 holds; the ring refuses more
                #[allow(clippy::cast_possible_truncation)]
                Verdict::ShardCount {
                    expected: member.record.shards as u16,
                }
            }
            Some(_) => Verdict::Member,
        }
    }

    /// The cluster this node serves, from its state or from what it adopted
    fn cluster(&self) -> Option<ClusterId> {
        self.machine.state().cluster.or(self.local.borrow().cluster)
    }

    /// The wire version the applied state says the cluster activated
    fn activated_wire(&self) -> u8 {
        self.machine.state().activated_wire()
    }

    /// The cluster the applied state says this one was restored from
    fn restored_from(&self) -> Option<ClusterId> {
        self.machine.state().restored_from
    }
}

/// Accept control connections and serve each in a task of its own
///
/// # Arguments
///
/// * `listener` - The bound control socket
/// * `raft` - This node's group, which inbound consensus RPCs are driven into
/// * `machine` - The state machine, for a ping's topology version and the judge
/// * `local` - What this node says about itself
/// * `tls` - What to take the wire with, if the lanes are encrypted
/// * `inbound` - Where the membership RPCs go, for the control loop to answer
pub async fn control_acceptor(
    listener: TcpListener,
    raft: Raft<ControlConfig, ControlStateMachine>,
    machine: ControlStateMachine,
    local: Rc<RefCell<Local>>,
    tls: Option<Arc<ServerConfig>>,
    inbound: kanal::AsyncSender<Inbound>,
) -> Result<(), ServerError> {
    let admission = Rc::new(StateAdmission {
        machine: machine.clone(),
        local: local.clone(),
    });
    loop {
        // a per-connection error must never close the control listener, for the same reason the
        // data listener survives one ([F38](../../../../docs/src/features/inter-node-transport.md))
        let mut stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(error) => {
                event!(Level::WARN, msg = "a control connection could not be accepted", ?error);
                continue;
            }
        };
        let raft = raft.clone();
        let machine = machine.clone();
        let local = local.clone();
        let admission = admission.clone();
        let tls = tls.clone();
        let inbound = inbound.clone();
        glommio::spawn_local(async move {
            // nodelay's error is this connection's alone, not the listener's
            let _ = stream.set_nodelay(true);
            // take the wire, then shake hands on the control lane
            if let Some(config) = &tls {
                if let Err(error) = crate::server::tls::accept(&mut stream, config).await {
                    event!(Level::WARN, msg = "refused a control peer at tls", ?error);
                    return;
                }
            }
            let ours = local.borrow().clone();
            let accepted = match handshake::accept(
                &mut stream,
                &ours,
                &[Lane::Control],
                admission.as_ref(),
            )
            .await
            {
                Ok(accepted) => accepted,
                Err(error) => {
                    event!(Level::WARN, msg = "refused a control peer", ?error);
                    return;
                }
            };
            event!(
                Level::DEBUG,
                msg = "accepted a control peer",
                node = %accepted.node,
                joining = accepted.joining
            );
            let (rx, tx) = stream.split();
            if let Err(error) =
                serve_control(rx, tx, &raft, &machine, &local, &accepted, &inbound).await
            {
                event!(Level::DEBUG, msg = "a control peer ended", node = %accepted.node, ?error);
            }
        })
        .detach();
    }
}

/// Read control requests off one connection and answer each
///
/// # Arguments
///
/// * `rx` - The read half
/// * `tx` - The write half
/// * `raft` - This node's group
/// * `machine` - The state machine, for a ping's topology version
/// * `local` - What this node says about itself, for the frame bound and a pong's incarnation
/// * `peer` - Who is on the other end, and whether it is a joiner
/// * `inbound` - Where the membership RPCs go
async fn serve_control(
    mut rx: futures::io::ReadHalf<TcpStream>,
    mut tx: futures::io::WriteHalf<TcpStream>,
    raft: &Raft<ControlConfig, ControlStateMachine>,
    machine: &ControlStateMachine,
    local: &Rc<RefCell<Local>>,
    peer: &Accepted,
    inbound: &kanal::AsyncSender<Inbound>,
) -> Result<(), ServerError> {
    let max_frame_bytes = local.borrow().max_frame_bytes;
    loop {
        // the header, or a clean end between requests, at the version the hello negotiated
        let Some(header) = codec::read_header(&mut rx, max_frame_bytes, peer.negotiated.version).await? else {
            return Ok(());
        };
        let header = codec::expect(header, MessageType::ControlRequest)?;
        // the fixed head, then the json payload the head's length leaves
        let raw: [u8; CONTROL_HEAD_LEN] = codec::read_array(&mut rx).await?;
        let head = ControlRequestHead::decode(&raw)?;
        let Some(payload_len) = header.body_len().checked_sub(CONTROL_HEAD_LEN) else {
            return Err(crate::shared::protocol::ProtocolError::BodyTooShort {
                need: CONTROL_HEAD_LEN,
                got: header.len,
            }
            .into());
        };
        let payload = codec::read_vec(&mut rx, payload_len).await?;
        // a joiner may ask to join and ping, and nothing else
        let (status, answer) = if peer.joining
            && !matches!(head.kind, ControlKind::Join | ControlKind::Ping)
        {
            err(format!("a joiner may not send {}", head.kind.name()))
        } else {
            match head.kind {
                // the consensus RPCs and pings, driven straight into the group
                ControlKind::AppendEntries
                | ControlKind::Vote
                | ControlKind::Snapshot
                | ControlKind::Ping => {
                    let incarnation = local.borrow().incarnation;
                    dispatch(head.kind, &payload, raft, machine, incarnation).await
                }
                // the membership RPCs, answered by the control loop
                ControlKind::Join | ControlKind::StatusReport | ControlKind::Propose => {
                    let (reply, answer) = oneshot::channel();
                    let sent = inbound
                        .send(Inbound {
                            kind: head.kind,
                            peer: peer.clone(),
                            payload,
                            reply,
                        })
                        .await;
                    match sent {
                        Ok(()) => match answer.await {
                            Ok(answered) => answered,
                            Err(_) => err("the control loop dropped the request".to_string()),
                        },
                        Err(_) => err("the control loop is gone".to_string()),
                    }
                }
            }
        };
        // frame whatever it produced under the same id
        let response_head = ControlResponseHead {
            id: head.id,
            status,
        }
        .encode();
        let frame_header = codec::header_at(
            peer.negotiated.version,
            MessageType::ControlResponse,
            response_head.len() + answer.len(),
            peer.negotiated.max_frame_bytes,
        )?;
        codec::write_frame(&mut tx, &frame_header, &[&response_head, &answer]).await?;
        // a joiner that asked for something else is done here
        if status == ControlStatus::Error && peer.joining {
            return Ok(());
        }
    }
}

/// Drive one consensus request into the group and return its answer, or a failure message
///
/// # Arguments
///
/// * `kind` - Which RPC this is
/// * `payload` - Its serialized request
/// * `raft` - This node's group
/// * `machine` - The state machine, for a ping's topology version
/// * `incarnation` - Which start of this node this is, for a pong
async fn dispatch(
    kind: ControlKind,
    payload: &[u8],
    raft: &Raft<ControlConfig, ControlStateMachine>,
    machine: &ControlStateMachine,
    incarnation: u64,
) -> (ControlStatus, Vec<u8>) {
    match kind {
        ControlKind::AppendEntries => {
            match serde_json::from_slice::<AppendEntriesRequest<ControlConfig>>(payload) {
                Ok(rpc) => match raft.append_entries(rpc).await {
                    Ok(response) => ok(&response),
                    Err(error) => err(format!("append_entries: {error}")),
                },
                Err(error) => err(format!("decoding append_entries: {error}")),
            }
        }
        ControlKind::Vote => match serde_json::from_slice::<VoteRequest<ControlConfig>>(payload) {
            Ok(rpc) => match raft.vote(rpc).await {
                Ok(response) => ok(&response),
                Err(error) => err(format!("vote: {error}")),
            },
            Err(error) => err(format!("decoding vote: {error}")),
        },
        ControlKind::Snapshot => match decode_snapshot(payload) {
            Ok((vote, snapshot)) => match raft.install_full_snapshot(vote, snapshot).await {
                Ok(response) => ok(&response),
                Err(error) => err(format!("install_full_snapshot: {error}")),
            },
            Err(error) => err(format!("decoding snapshot: {error}")),
        },
        ControlKind::Ping => {
            let state = machine.state();
            let pong = Pong {
                incarnation,
                topology_version: state.topology_version,
            };
            ok(&pong)
        }
        // the membership RPCs never reach here
        ControlKind::Join | ControlKind::StatusReport | ControlKind::Propose => {
            err(format!("{} is answered by the control loop", kind.name()))
        }
    }
}

/// An answer, serialized
///
/// # Arguments
///
/// * `value` - The answer
pub fn ok<T: Serialize>(value: &T) -> (ControlStatus, Vec<u8>) {
    match serde_json::to_vec(value) {
        Ok(bytes) => (ControlStatus::Ok, bytes),
        Err(error) => err(format!("encoding an answer: {error}")),
    }
}

/// A failure, as its message
///
/// # Arguments
///
/// * `msg` - What went wrong
pub fn err(msg: String) -> (ControlStatus, Vec<u8>) {
    (ControlStatus::Error, msg.into_bytes())
}

/// The lane name, for a log line
#[allow(dead_code)]
fn lane_name(lane: peer::Lane) -> &'static str {
    lane.name()
}
