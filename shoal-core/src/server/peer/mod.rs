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

use crate::server::conf::cluster::{DialOverride, Transport};
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
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&socket2::SockAddr::from(addr))?;
    socket.listen(1024)?;
    // SAFETY: the descriptor is bound and listening, which is what glommio's conversion asks
    // for, and it is owned by nothing else once it leaves the socket
    Ok(unsafe { glommio::net::TcpListener::from_raw_fd(socket.into_raw_fd()) })
}
/// Abort a peer connection whose sent data goes unacknowledged for this long
///
/// Sets `TCP_USER_TIMEOUT`. The kernel then gives up on the connection rather than doubling its
/// retransmission timer for a quarter of an hour, so a link cut by dropped packets goes down and
/// is dialled again, and is back within a dial of the heal rather than when the timer next
/// fires ([Resolved #181](../../../../docs/src/appendix/resolved/partition-retransmit-backoff.md)).
/// Only unacknowledged data counts, which the kernel acknowledges however busy the process is.
///
/// # Arguments
///
/// * `stream` - The connection
/// * `timeout` - How long its data may go unacknowledged; zero leaves the kernel's default
///
/// # Errors
///
/// Fails as `setsockopt` does.
pub fn set_unacked_timeout<S: std::os::fd::AsRawFd>(
    stream: &S,
    timeout: std::time::Duration,
) -> std::io::Result<()> {
    // zero is the kernel's own default, which is what zero means here too
    let millis = libc::c_uint::try_from(timeout.as_millis()).unwrap_or(libc::c_uint::MAX);
    // SAFETY: the descriptor is an open TCP socket for as long as the stream is borrowed, and the
    // value is a c_uint of the size passed
    let rc = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_USER_TIMEOUT,
            std::ptr::from_ref(&millis).cast(),
            std::mem::size_of::<libc::c_uint>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// How long a connection's sent data must go unacknowledged before the kernel's word counts
///
/// A peer the network has cut off acknowledges nothing, and the kernel says so as soon as its
/// retransmission timer has fired once: a few hundred milliseconds, against the second and a
/// half the replication lane's silence takes at the default failover base. A peer that is only
/// slow (a loaded host, a slow disk, a long queue in the process) still acknowledges every
/// segment, because the kernel does that whatever the process is doing, so this never takes a
/// busy leader for a cut one ([Resolved #143](../../../../docs/src/appendix/resolved/silent-partition-hops.md)).
pub const KERNEL_SILENCE: std::time::Duration = std::time::Duration::from_millis(400);

/// How many times in a row the retransmission timer must fire unanswered for a cut
pub const KERNEL_BACKOFFS: u8 = 2;

/// What the kernel says about a TCP connection's sending side
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpSample {
    /// Segments sent and not yet acknowledged
    pub unacked: u32,
    /// How many times the retransmission timer has backed off since the last acknowledgement
    pub backoff: u8,
    /// Retransmissions by timer not yet recovered
    pub retransmits: u8,
    /// Milliseconds since an acknowledgement was last received
    pub last_ack_recv_ms: u32,
    /// The smoothed round trip, in microseconds
    pub rtt_us: u32,
}

impl TcpSample {
    /// Read a connection's figures from the kernel
    ///
    /// # Arguments
    ///
    /// * `fd` - The connection's socket
    ///
    /// # Errors
    ///
    /// Fails as `getsockopt` does.
    pub fn read(fd: std::os::fd::RawFd) -> std::io::Result<Self> {
        // SAFETY: tcp_info is plain integers, so all zeroes is a valid value to be filled in
        let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
        // SAFETY: the pointer and length describe `info`, which outlives the call, and a closed
        // or reused descriptor fails or answers for another socket rather than writing past it
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                std::ptr::from_mut(&mut info).cast(),
                &raw mut len,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(TcpSample {
            unacked: info.tcpi_unacked,
            backoff: info.tcpi_backoff,
            retransmits: info.tcpi_retransmits,
            last_ack_recv_ms: info.tcpi_last_ack_recv,
            rtt_us: info.tcpi_rtt,
        })
    }

    /// Whether the kernel's figures say the peer is cut off rather than slow
    ///
    /// Data is outstanding, the retransmission timer has fired twice in a row without an
    /// acknowledgement, and none has come for [`KERNEL_SILENCE`] or four round trips, whichever
    /// is longer. Loss the network recovers from by fast retransmit never fires the timer, and a
    /// slow peer's kernel keeps acknowledging, so neither is taken for a cut. One firing is not
    /// enough: at 5% loss a quiet link loses a segment and then its first retransmission several
    /// times a second across a cluster, and the lab refused a few hundred writes a second on
    /// that; losing two retransmissions in a row is a twentieth as likely again, and a cut
    /// reaches it within about 600 ms of its last acknowledgement.
    #[must_use]
    pub fn cut_off(&self) -> bool {
        let quiet = std::time::Duration::from_millis(u64::from(self.last_ack_recv_ms));
        let round_trips = std::time::Duration::from_micros(u64::from(self.rtt_us) * 4);
        self.unacked > 0
            && (self.backoff >= KERNEL_BACKOFFS || self.retransmits >= KERNEL_BACKOFFS)
            && quiet >= KERNEL_SILENCE.max(round_trips)
    }
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
    /// The certificate and authority the lanes use, read at every handshake, if encrypted
    pub tls: crate::shared::tls::PeerTlsHolder,
    /// The bounds and timers
    pub transport: Transport,
    /// The address the peer listeners bind, `advertise:port`
    pub bind: std::net::SocketAddr,
    /// The tablets whose copy on this node is installing a snapshot, which every shard of the
    /// node counts into and routes around
    /// ([#184](../../../../docs/src/appendix/resolved/installing-copy-reads-elsewhere.md))
    pub installing: std::sync::Arc<crate::server::installing::InstallingTablets>,
}

/// What one shard's peer links look like
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardTransportView {
    /// The shard this is
    pub shard: usize,
    /// Every outbound link this shard owns: data, bulk and, since F48, replication
    pub links: Vec<LinkView>,
    /// Bytes received on bulk lanes accepted by this shard
    pub bulk_received: u64,
    /// Queries this shard turned away at the admission bound, `Shedding` by name
    /// ([Resolved #15](../../../docs/src/appendix/resolved/shard-mesh-admission.md))
    #[serde(default)]
    pub shed: u64,
    /// The connections this shard holds a channel for, peer lanes included
    ///
    /// Every connection is announced to every shard, so a connection that went away and is
    /// still counted here is one this shard was never told about
    /// ([Resolved #32](../../../docs/src/appendix/resolved/client-gone-broadcast.md)).
    #[serde(default)]
    pub clients: usize,
}
