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
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
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
use crate::shared::tls::{PeerIdentity, PeerTlsHolder};

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
        if state.tombstones.contains_key(&node)
            || state
                .members
                .get(&node)
                .is_some_and(|member| member.phase == MemberPhase::Removed)
        {
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

    /// The schema the applied state says the cluster committed
    fn committed_schema(&self) -> Option<u64> {
        self.machine.state().schema.as_ref().map(|schema| schema.id)
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
/// * `tls` - What to take the wire with, read at every accept, if the lanes are encrypted
/// * `inbound` - Where the membership RPCs go, for the control loop to answer
/// * `unacked_timeout` - How long sent data may go unacknowledged before a connection is aborted
/// * `rto_min` - The least a connection waits before it sends lost data again
#[allow(clippy::too_many_arguments)]
pub async fn control_acceptor(
    listener: TcpListener,
    raft: Raft<ControlConfig, ControlStateMachine>,
    machine: ControlStateMachine,
    local: Rc<RefCell<Local>>,
    tls: PeerTlsHolder,
    inbound: kanal::AsyncSender<Inbound>,
    unacked_timeout: std::time::Duration,
    keepalive: std::time::Duration,
    rto_min: std::time::Duration,
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
                event!(
                    Level::WARN,
                    msg = "a control connection could not be accepted",
                    ?error
                );
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
            // nodelay's error is this connection's alone, not the listener's, and so is the
            // abort of a connection whose data goes unacknowledged
            // ([Resolved #181](../../../../docs/src/appendix/resolved/partition-retransmit-backoff.md))
            let _ = stream.set_nodelay(true);
            let _ = crate::server::peer::set_unacked_timeout(&stream, unacked_timeout);
            // and one whose peer vanished while it owed nothing is probed and noticed (item 310)
            let _ = crate::server::peer::set_keepalive(&stream, keepalive);
            // and a lost frame, one segment, probed after the transport's minimum RTO, not the
            // kernel's 200 ms ([Resolved #304](../../../../docs/src/appendix/resolved/lone-frame-loss-probe.md))
            let _ = crate::server::peer::set_rto_min(&stream, rto_min);
            // take the wire with the material as it is right now, then shake hands on the
            // control lane with what the certificate said
            let certified = match tls.server() {
                Some(config) => match crate::server::tls::accept(&mut stream, &config).await {
                    Ok(established) => established.peer,
                    Err(error) => {
                        event!(Level::WARN, msg = "refused a control peer at tls", ?error);
                        return;
                    }
                },
                None => PeerIdentity::Plaintext,
            };
            let ours = local.borrow().clone();
            let accepted = match handshake::accept(
                &mut stream,
                &ours,
                &[Lane::Control],
                admission.as_ref(),
                &certified,
                tls.binds_identity(),
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

/// How many membership requests one control connection may have waiting on the control loop at
/// once before the next is answered in turn: a forwarded proposal can wait ten seconds, and a peer
/// that sent a burst of them holds a task each until then
const MEMBERSHIP_IN_FLIGHT: usize = 64;

/// One answer on its way to a control connection's writer: the request's id, and what to frame
type Answer = (u64, ControlStatus, Vec<u8>);

/// Read control requests off one connection and answer each
///
/// The consensus requests and pings are driven straight into the group and answered in turn. A
/// membership request - a join, a status report, a forwarded proposal - is answered by the control
/// loop, a proposal only once it commits, up to ten seconds; answered in turn, it held every append,
/// vote, pre-vote and ping its peer sent after it on this connection past its budget (item 358). So
/// each is waited for on a task of its own, and one writer frames every answer as it is ready, under
/// its request's id, which the asking link matches answers by. A joiner's connection, which carries
/// only its join and its pings and ends on a refusal, is answered in turn as before.
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
    tx: futures::io::WriteHalf<TcpStream>,
    raft: &Raft<ControlConfig, ControlStateMachine>,
    machine: &ControlStateMachine,
    local: &Rc<RefCell<Local>>,
    peer: &Accepted,
    inbound: &kanal::AsyncSender<Inbound>,
) -> Result<(), ServerError> {
    let max_frame_bytes = local.borrow().max_frame_bytes;
    // every answer goes through one writer, in the order it is ready
    let (answers, ready) = futures_channel::mpsc::unbounded::<Answer>();
    let writer = glommio::spawn_local(write_answers(
        tx,
        ready,
        peer.negotiated.version,
        peer.negotiated.max_frame_bytes,
    ));
    // the membership requests waiting on the control loop now
    let waiting = Rc::new(std::cell::Cell::new(0usize));
    let served = loop {
        // the header, or a clean end between requests, at the version the hello negotiated
        let header = match codec::read_header(&mut rx, max_frame_bytes, peer.negotiated.version).await {
            Ok(Some(header)) => header,
            Ok(None) => break Ok(()),
            Err(error) => break Err(error.into()),
        };
        let header = match codec::expect(header, MessageType::ControlRequest) {
            Ok(header) => header,
            Err(error) => break Err(error.into()),
        };
        // the fixed head, then the json payload the head's length leaves
        let raw: [u8; CONTROL_HEAD_LEN] = match codec::read_array(&mut rx).await {
            Ok(raw) => raw,
            Err(error) => break Err(error.into()),
        };
        let head = match ControlRequestHead::decode(&raw) {
            Ok(head) => head,
            Err(error) => break Err(error.into()),
        };
        let Some(payload_len) = header.body_len().checked_sub(CONTROL_HEAD_LEN) else {
            break Err(crate::shared::protocol::ProtocolError::BodyTooShort {
                need: CONTROL_HEAD_LEN,
                got: header.len,
            }
            .into());
        };
        let payload = match codec::read_vec(&mut rx, payload_len).await {
            Ok(payload) => payload,
            Err(error) => break Err(error.into()),
        };
        // a joiner may ask to join and ping, and nothing else
        if peer.joining && !matches!(head.kind, ControlKind::Join | ControlKind::Ping) {
            let (status, answer) = err(format!("a joiner may not send {}", head.kind.name()));
            let _ = answers.unbounded_send((head.id, status, answer));
            break Ok(());
        }
        match head.kind {
            // the consensus RPCs and pings, driven straight into the group, in turn
            ControlKind::AppendEntries
            | ControlKind::Vote
            | ControlKind::PreVote
            | ControlKind::Snapshot
            | ControlKind::Ping => {
                let incarnation = local.borrow().incarnation;
                let (status, answer) = dispatch(head.kind, &payload, raft, machine, incarnation).await;
                let _ = answers.unbounded_send((head.id, status, answer));
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
                let id = head.id;
                if sent.is_err() {
                    let (status, answer) = err("the control loop is gone".to_string());
                    let _ = answers.unbounded_send((id, status, answer));
                    continue;
                }
                // what the loop answers it with, framed whenever it comes
                let answered = async move {
                    match answer.await {
                        Ok(answered) => answered,
                        Err(_) => err("the control loop dropped the request".to_string()),
                    }
                };
                // a joiner's join is answered in turn, and a refusal ends its connection; so is a
                // request past the bound on what one connection may have waiting
                if peer.joining || waiting.get() >= MEMBERSHIP_IN_FLIGHT {
                    let (status, answer) = answered.await;
                    let refused = status == ControlStatus::Error && peer.joining;
                    let _ = answers.unbounded_send((id, status, answer));
                    if refused {
                        break Ok(());
                    }
                    continue;
                }
                // anything else waits on a task of its own, holding nothing read after it
                waiting.set(waiting.get() + 1);
                let (answers, waiting) = (answers.clone(), waiting.clone());
                glommio::spawn_local(async move {
                    let (status, answer) = answered.await;
                    let _ = answers.unbounded_send((id, status, answer));
                    waiting.set(waiting.get() - 1);
                })
                .detach();
            }
        }
    };
    // the writer ends once every answer still waiting is written and nothing can send another
    drop(answers);
    let written = writer.await;
    served.and(written)
}

/// Frame and write every answer a control connection's requests are given, as each is ready
///
/// # Arguments
///
/// * `tx` - The write half
/// * `ready` - The answers, under their requests' ids
/// * `version` - The wire version the hello negotiated
/// * `max_frame_bytes` - The largest frame the peer accepts
async fn write_answers(
    mut tx: futures::io::WriteHalf<TcpStream>,
    mut ready: futures_channel::mpsc::UnboundedReceiver<Answer>,
    version: u8,
    max_frame_bytes: u32,
) -> Result<(), ServerError> {
    use futures::StreamExt as _;
    while let Some((id, status, answer)) = ready.next().await {
        // frame whatever it produced under the same id
        let response_head = ControlResponseHead { id, status }.encode();
        let frame_header = codec::header_at(
            version,
            MessageType::ControlResponse,
            response_head.len() + answer.len(),
            max_frame_bytes,
        )?;
        codec::write_frame(&mut tx, &frame_header, &[&response_head, &answer]).await?;
    }
    Ok(())
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
        // a pre-vote is judged by the same rules as a vote and moves nothing
        // ([Resolved #144](../../../../docs/src/appendix/resolved/post-heal-elections.md))
        ControlKind::PreVote => match serde_json::from_slice::<VoteRequest<ControlConfig>>(payload)
        {
            Ok(rpc) => match raft.pre_vote(rpc).await {
                Ok(response) => ok(&response),
                Err(error) => err(format!("pre_vote: {error}")),
            },
            Err(error) => err(format!("decoding pre_vote: {error}")),
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
