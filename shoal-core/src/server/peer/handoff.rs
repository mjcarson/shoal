//! Handing an accepted replication lane to the executor hosting its slot
//!
//! Every shard accepts on the node's peer port, and the kernel picks which one takes a
//! connection. A replication lane's every request is for one slot, which its hello names, so the
//! shard that accepted it gives the connection to the executor hosting that slot rather than
//! reading every append and handing it across as a buffer: the bytes are then read, and under
//! kTLS decrypted, on the core that applies them
//! ([F84](../../../../docs/src/features/replication-lane-handoff.md)). X11 measured why: a
//! connection handed over costs what a direct one does, while bytes crossing executors cost 1.3
//! to 1.7 times the cpu a gibibyte at 1 MiB
//! ([X11](../../../../docs/src/object-storage/streamed-bodies.md)).
//!
//! The connection is given up whole, with no `dup`: the accepting executor takes its stream
//! apart with the fork's `IntoRawFd`, which cancels its poll and timers and closes nothing, and
//! the descriptor travels as an [`OwnedFd`] to the executor that adopts it. kTLS lives on the
//! socket, so a lane under it is still under it on the other side. A request still reaches its
//! shard through that shard's own queue; what goes away is the crossing between cores.
//!
//! # Invariants
//!
//! **Nothing is read past the hello before a lane is given up.** The stream is non-buffered, the
//! TLS handshake reads one record at a time, and the hello is read exactly, so whatever the peer
//! sent after its hello is still in the socket for the executor that adopts it.
//!
//! **One owner of the descriptor at every instant.** It leaves its stream as an `OwnedFd`, rides
//! the message, and becomes a stream again only on the executor that will drive it; a message
//! dropped on the way closes it, and the peer dials again.
//!
//! **A lane is adopted only on the executor that will serve it.** `TcpStream::from` registers the
//! socket with the reactor of the thread it runs on.
//!
//! **Correctness never depends on the handoff.** The lane still dispatches every request by the
//! slot its own head names, so a lane served where it landed, from a peer that names no slot,
//! answers exactly as before.

use std::cell::Cell;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};

use glommio::net::TcpStream;

use super::handshake::Negotiated;
use super::Lane;
use crate::server::hosting::Hosting;
use crate::shared::identity::NodeId;
use crate::shared::protocol::stats::ReplicationLanes;

/// A replication lane on its way from the executor that accepted it to the one hosting its slot
#[derive(Debug)]
pub struct HandedLane {
    /// The connection, given up by the executor that accepted it
    fd: OwnedFd,
    /// The peer it came from
    pub origin: NodeId,
    /// What its hello agreed
    pub negotiated: Negotiated,
    /// The slot its hello named
    pub slot: u16,
}

impl HandedLane {
    /// Give a freshly accepted lane up, so another executor can serve it
    ///
    /// # Arguments
    ///
    /// * `stream` - The connection, whole and with nothing read past its hello
    /// * `origin` - The peer it came from
    /// * `negotiated` - What its hello agreed
    /// * `slot` - The slot its hello named
    #[must_use]
    pub fn release(stream: TcpStream, origin: NodeId, negotiated: Negotiated, slot: u16) -> Self {
        // the stream gives up its descriptor without closing it, its poll and timers cancelled
        let raw = stream.into_raw_fd();
        // SAFETY: `into_raw_fd` handed over the stream's own descriptor and closed nothing, so
        // this is its one owner from here on
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        HandedLane {
            fd,
            origin,
            negotiated,
            slot,
        }
    }

    /// Take the connection back as a stream on the executor that will serve it
    ///
    /// Only ever called on that executor, since the stream registers with the reactor of the
    /// thread it is made on.
    #[must_use]
    pub fn adopt(self) -> TcpStream {
        // the descriptor becomes a socket, and the socket a stream on this executor's reactor
        TcpStream::from(socket2::Socket::from(self.fd))
    }
}

/// Where a lane the listener accepted is served
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    /// By the executor that accepted it
    Here,
    /// By another executor, the one hosting the slot its hello named
    Host(usize),
}

/// Where a lane is served, from what its hello named
///
/// Only a replication lane naming a slot this node has, hosted by another executor, is handed
/// over. Every other lane - data, bulk, a replication lane from a build that names no slot, one
/// naming a slot past the count - is served where it landed, as every lane was before.
///
/// # Arguments
///
/// * `hosting` - Which executor hosts each slot
/// * `executor` - The executor that accepted the lane
/// * `lane` - The lane its hello asked for
/// * `slot` - The slot its hello named, if any
#[must_use]
pub fn destination(
    hosting: &Hosting,
    executor: usize,
    lane: Lane,
    slot: Option<u16>,
) -> Destination {
    // only a replication lane carries one slot's frames
    let (Lane::Replication, Some(slot)) = (lane, slot) else {
        return Destination::Here;
    };
    // a slot this node does not have is served here, where its requests are judged one by one
    if usize::from(slot) >= hosting.slots {
        return Destination::Here;
    }
    // the executor hosting it, which may be this one
    match hosting.host_of_slot(slot) {
        host if host == executor => Destination::Here,
        host => Destination::Host(host),
    }
}

/// Where one shard's replication lanes were read, counted as it happens
#[derive(Debug, Default)]
pub struct LaneCounters {
    /// Lanes this shard accepted and handed to another executor
    handed_out: Cell<u64>,
    /// Lanes this shard took in from the executor that accepted them
    taken_in: Cell<u64>,
    /// Lanes this shard accepted for a slot it hosts
    in_place: Cell<u64>,
    /// Lanes this shard accepted that named no slot, or one this node does not have
    unnamed: Cell<u64>,
    /// Requests this shard read for a slot it hosts
    requests_here: Cell<u64>,
    /// Requests this shard read for a slot another executor hosts
    requests_crossed: Cell<u64>,
    /// Their payload bytes
    bytes_crossed: Cell<u64>,
}

impl LaneCounters {
    /// Add one to a counter
    ///
    /// # Arguments
    ///
    /// * `counter` - The counter
    fn bump(counter: &Cell<u64>) {
        counter.set(counter.get().saturating_add(1));
    }

    /// Count a lane handed to the executor hosting its slot
    pub fn handed_out(&self) {
        Self::bump(&self.handed_out);
    }

    /// Count a lane taken in from the executor that accepted it
    pub fn taken_in(&self) {
        Self::bump(&self.taken_in);
    }

    /// Count a replication lane accepted where it is served
    ///
    /// # Arguments
    ///
    /// * `named` - Whether its hello named a slot this node has
    pub fn served_here(&self, named: bool) {
        if named {
            Self::bump(&self.in_place);
        } else {
            Self::bump(&self.unnamed);
        }
    }

    /// Count a request read off a replication lane
    ///
    /// # Arguments
    ///
    /// * `here` - Whether this executor hosts the slot it names
    /// * `bytes` - Its payload's length
    pub fn request(&self, here: bool, bytes: usize) {
        if here {
            Self::bump(&self.requests_here);
        } else {
            Self::bump(&self.requests_crossed);
            let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
            self.bytes_crossed
                .set(self.bytes_crossed.get().saturating_add(bytes));
        }
    }

    /// What these counters say, for the transport view and the shard's report
    #[must_use]
    pub fn view(&self) -> ReplicationLanes {
        ReplicationLanes {
            handed_out: self.handed_out.get(),
            taken_in: self.taken_in.get(),
            in_place: self.in_place.get(),
            unnamed: self.unnamed.get(),
            requests_here: self.requests_here.get(),
            requests_crossed: self.requests_crossed.get(),
            bytes_crossed: self.bytes_crossed.get(),
        }
    }
}
