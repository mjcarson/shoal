//! The control listener: peer control RPCs, driven into this node's own group
//!
//! The inbound half of the control lane. It binds `advertise:control_port`, accepts a peer that
//! proves its identity on the [`Lane::Control`] lane, and reads
//! [`ControlRequest`](crate::shared::protocol::peer::ControlRequestHead) frames, driving each
//! into this node's `Raft`: an append into `append_entries`, a vote into `vote`, a snapshot into
//! `install_full_snapshot`, and a ping into a small liveness reply. Every answer carries the
//! request's correlation id back, so one connection can hold many RPCs at once.
//!
//! It runs on the control thread's executor beside the group, so a stalled data shard can never
//! stop it - which is the whole point of the control lane being its own socket on its own thread
//! ([F38](../../../../docs/src/features/inter-node-transport.md)).

use std::rc::Rc;

use futures::io::{ReadHalf, WriteHalf};
use futures::AsyncReadExt;
use glommio::net::{TcpListener, TcpStream};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::Raft;
use rustls::ServerConfig;
use serde::Serialize;
use std::sync::Arc;
use tracing::{event, Level};

use super::network::decode_snapshot;
use super::store::ControlStateMachine;
use super::types::ControlConfig;
use crate::server::conf::cluster::Placement;
use crate::server::peer::codec;
use crate::server::peer::handshake::{self, Local};
use crate::server::peer::Lane;
use crate::server::ServerError;
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

/// Accept control connections and serve each in a task of its own
///
/// # Arguments
///
/// * `listener` - The bound control socket
/// * `raft` - This node's group, which inbound RPCs are driven into
/// * `machine` - The state machine, for a ping's topology version
/// * `local` - What this node says about itself
/// * `placement` - Every node this listener will accept a hello from
/// * `tls` - What to take the wire with, if the lanes are encrypted
pub async fn control_acceptor(
    listener: TcpListener,
    raft: Raft<ControlConfig, ControlStateMachine>,
    machine: ControlStateMachine,
    local: Local,
    placement: Rc<Placement>,
    tls: Option<Arc<ServerConfig>>,
) -> Result<(), ServerError> {
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
        let placement = placement.clone();
        let tls = tls.clone();
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
            let accepted =
                match handshake::accept(&mut stream, &local, &[Lane::Control], &placement).await {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        event!(Level::WARN, msg = "refused a control peer", ?error);
                        return;
                    }
                };
            event!(Level::DEBUG, msg = "accepted a control peer", node = %accepted.node);
            let (rx, tx) = stream.split();
            if let Err(error) =
                serve_control(rx, tx, &raft, &machine, local.max_frame_bytes).await
            {
                event!(Level::DEBUG, msg = "a control peer ended", node = %accepted.node, ?error);
            }
        })
        .detach();
    }
}

/// Read control requests off one connection and answer each into the group
///
/// # Arguments
///
/// * `rx` - The read half
/// * `tx` - The write half
/// * `raft` - This node's group
/// * `machine` - The state machine, for a ping's topology version
/// * `max_frame_bytes` - The largest frame this end accepts
async fn serve_control(
    mut rx: ReadHalf<TcpStream>,
    mut tx: WriteHalf<TcpStream>,
    raft: &Raft<ControlConfig, ControlStateMachine>,
    machine: &ControlStateMachine,
    max_frame_bytes: u32,
) -> Result<(), ServerError> {
    loop {
        // the header, or a clean end between requests
        let Some(header) = codec::read_header(&mut rx, max_frame_bytes).await? else {
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
        // drive it into the group, and frame whatever it produced under the same id
        let (status, answer) = dispatch(head.kind, &payload, raft, machine).await;
        let response_head = ControlResponseHead {
            id: head.id,
            status,
        }
        .encode();
        let frame_header = codec::header(
            MessageType::ControlResponse,
            response_head.len() + answer.len(),
            max_frame_bytes,
        )?;
        codec::write_frame(&mut tx, &frame_header, &[&response_head, &answer]).await?;
    }
}

/// Drive one request into the group and return its answer, or a failure message
///
/// # Arguments
///
/// * `kind` - Which RPC this is
/// * `payload` - Its serialized request
/// * `raft` - This node's group
/// * `machine` - The state machine, for a ping's topology version
async fn dispatch(
    kind: ControlKind,
    payload: &[u8],
    raft: &Raft<ControlConfig, ControlStateMachine>,
    machine: &ControlStateMachine,
) -> (ControlStatus, Vec<u8>) {
    match kind {
        ControlKind::AppendEntries => {
            match serde_json::from_slice::<AppendEntriesRequest<ControlConfig>>(payload) {
                Ok(request) => match raft.append_entries(request).await {
                    Ok(response) => ok(&response),
                    Err(error) => err(format!("append_entries: {error}")),
                },
                Err(error) => err(format!("decoding append_entries: {error}")),
            }
        }
        ControlKind::Vote => match serde_json::from_slice::<VoteRequest<ControlConfig>>(payload) {
            Ok(request) => match raft.vote(request).await {
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
                incarnation: crate::server::peer::incarnation(),
                topology_version: state.topology_version,
            };
            ok(&pong)
        }
    }
}

/// Serialize a successful answer
///
/// # Arguments
///
/// * `value` - The answer to serialize
fn ok<T: Serialize>(value: &T) -> (ControlStatus, Vec<u8>) {
    match serde_json::to_vec(value) {
        Ok(bytes) => (ControlStatus::Ok, bytes),
        Err(error) => err(format!("encoding a control answer: {error}")),
    }
}

/// Build a failure answer
///
/// # Arguments
///
/// * `message` - What went wrong
fn err(message: String) -> (ControlStatus, Vec<u8>) {
    (ControlStatus::Error, message.into_bytes())
}
