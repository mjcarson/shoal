//! The inter-node transport: peer lanes, links and the listener that accepts them
//!
//! This is [F38](../../../../docs/src/features/inter-node-transport.md) inside the engine, the
//! runtime half of the peer protocol the proto crate frames. Three lanes, each a socket of its
//! own: the **data** lane carries forwarded bundles and their answers and is owned by the shard
//! that sends on it; the **bulk** lane carries snapshot streams and is owned by a shard too; the
//! **control** lane carries the control group's RPCs and pings and is owned by the control
//! thread. A lane to a peer is a [`Link`]: a bounded byte queue the owner enqueues frames on and
//! a task that dials, shakes hands, drains the queue and reads what comes back, reconnecting on
//! backoff when the connection drops.
//!
//! # Invariants
//!
//! **A link is owned by one task on one executor and shared with nobody.** The queue is an `Rc`
//! between the owner and the link task, both on the same executor; nothing here is `Send` and
//! nothing needs to be. A shard's links live on that shard, the control thread's on the control
//! thread, and a stalled shard cannot stall a control link because they share no socket, no
//! queue and no executor.
//!
//! **Every queue is bounded in bytes, and admission is judged before anything is recorded.** A
//! frame that would take a queue past its bound is refused to the caller synchronously, so the
//! caller answers the client with a definite refusal and nothing anywhere remembers the frame.
//! Once a frame is queued its fate is one of three events the owner is told about: it was
//! written and answered, it was written and the link dropped (outcome unknown), or the link
//! dropped with it still queued (never written, so definitely not applied).
//!
//! **Nothing ahead of an rkyv payload shares its buffer.** A forwarded answer's preamble is read
//! into its own array and its payload into a fresh aligned allocation, the same rule the client
//! relay follows for a request's trace context.

pub mod codec;
pub mod handshake;
pub mod link;
pub mod listener;
pub mod peers;
pub mod tls;

#[cfg(test)]
mod tests;


use crate::server::conf::cluster::{DialOverride, PeerTls, Transport};
use crate::shared::identity::NodeId;

pub use crate::shared::protocol::peer::Lane;
pub use handshake::Local;

/// Bind a listener that a restart on the same port can bind again at once
///
/// glommio's own bind sets `SO_REUSEPORT` and nothing else, and on Linux a port with a
/// connection still in `TIME_WAIT` refuses a new bind unless `SO_REUSEADDR` is set too. A node
/// restarted on its own ports, or a benchmark arm run twice, would otherwise fail to bind for
/// a minute after the last connection closed ([F41](../../../docs/src/features/read-consistency.md)).
/// Every listener a node opens goes through here.
///
/// # Arguments
///
/// * `addr` - The address to bind
///
/// # Errors
///
/// Fails as the socket calls do.
pub fn bind_reusable(addr: std::net::SocketAddr) -> std::io::Result<glommio::net::TcpListener> {
    use std::os::fd::{FromRawFd, IntoRawFd};
    // the same socket glommio would build, with the address reuse it leaves off
    let domain = if addr.is_ipv6() { socket2::Domain::IPV6 } else { socket2::Domain::IPV4 };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&socket2::SockAddr::from(addr))?;
    socket.listen(1024)?;
    // SAFETY: the descriptor is bound and listening, which is what glommio's conversion asks
    // for, and it is owned by nothing else once it leaves the socket
    Ok(unsafe { glommio::net::TcpListener::from_raw_fd(socket.into_raw_fd()) })
}
pub use link::{Frame, FrameKey, Link, LinkEvent, LinkView};
pub use listener::{peer_acceptor, ListenerContext, ReplicateReply};
pub use peers::{Peers, Pending};

/// Everything a shard needs to talk to its peers, resolved once by the pool
///
/// A standalone node has none of this and builds no peer links, binds no peer listener, and
/// routes against a ring of its own shards. A cluster node gets one of these, shared by every
/// shard - it is `Send` and `Clone` so the pool can hand a copy to each shard thread, which
/// wraps what it needs in an `Rc` and builds its rustls configs on its own executor, the way
/// each shard already builds the client listener's config. Who the peers are is not here: that
/// is the map the control plane pushes ([F39](../../../../docs/src/features/membership.md)).
#[derive(Clone)]
pub struct PeerSetup {
    /// What this node says about itself in every hello
    pub local: Local,
    /// Where particular members are dialled instead of where they advertise
    pub dial: std::collections::BTreeMap<NodeId, DialOverride>,
    /// The map the control plane held when the shards started; later ones are pushed
    pub initial_map: std::sync::Arc<crate::server::map::TabletMap>,
    /// The certificate and authority the lanes use, if they are encrypted
    pub tls: Option<PeerTls>,
    /// The bounds and timers
    pub transport: Transport,
    /// The address the peer listeners bind, `advertise:port`
    pub bind: std::net::SocketAddr,
}

/// What one shard's peer links look like
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardTransportView {
    /// The shard this is
    pub shard: usize,
    /// Every outbound link this shard owns, data and bulk
    pub links: Vec<LinkView>,
    /// Bytes received on bulk lanes accepted by this shard
    pub bulk_received: u64,
}
