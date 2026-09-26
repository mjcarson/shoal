//! An outbound lane to one peer: a bounded queue, and the task that carries it
//!
//! The owner - a shard, or the control thread - enqueues frames and hears events; the task
//! dials, shakes hands, writes what is queued, reads what comes back, and starts again on
//! backoff when the connection drops. The two share the queue through an `Rc` on one executor
//! and nothing else.
//!
//! # What the owner is told
//!
//! [`LinkEvent::Up`] when a handshake completes, with the peer's incarnation.
//! [`LinkEvent::Frame`] for every frame the peer sends back, its head read into a vec of its own
//! and its payload into a fresh aligned allocation. [`LinkEvent::Down`] when the connection is
//! lost, carrying the keys of every frame still queued - those were never written and are
//! definitely not applied - while everything already written and unanswered is the owner's to
//! judge as unknown. The queue is emptied on a drop: a reconnect starts clean, because a frame
//! queued before a drop may carry a deadline that passed while the link was down, and the owner
//! knows that and the link does not.
//!
//! # A wanted link redials at the floor, an idle one backs off
//!
//! The backoff after a failed dial grows from `reconnect_min` toward `reconnect_max`, but only
//! an idle link waits it out: a frame queued while the link is backing off cuts the wait short
//! at `reconnect_min`, so the link dials again, fails again if the peer is still gone, and
//! reports the frame unsent within the floor rather than the whole backoff. A write hopping to
//! a dead leader is refused in a hundred milliseconds that way instead of waiting five seconds
//! for a dial nobody asked for ([F42](../../../../docs/src/features/primary-failover.md)). The
//! dials a wanted link makes at a dead peer are bounded by the floor, one per `reconnect_min`.
//!
//! # Every frame says which version it is written at
//!
//! Since [F48](../../../../docs/src/features/rolling-compatibility.md) a link knows what it
//! negotiated at the hello ([`Link::negotiated`]) and the writer stamps every frame's header
//! with that version as it goes out, unless the frame was built at a version of its own
//! ([`Frame::at`]) - which a body that differs between versions is, encoded at what the link
//! spoke when it was built or at the floor when it was not up. The reader refuses a frame
//! above the negotiated version, so a peer is never misread, only refused.

use bytes::Bytes;
use futures::future::{select, Either};
use futures::io::{ReadHalf, WriteHalf};
use futures::AsyncReadExt;
use glommio::net::TcpStream;
use glommio::task::JoinHandle;
use rkyv::util::AlignedVec;
use rustls::pki_types::ServerName;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tracing::{event, Level};
use uuid::Uuid;

use super::codec;
use super::handshake::{self, Local, Negotiated, PeerAddr};
use super::Lane;
use crate::server::conf::cluster::Transport;
use crate::server::ServerError;
use crate::shared::identity::NodeId;
use crate::shared::protocol::peer::{
    CONTROL_HEAD_LEN, FORWARDED_PREAMBLE_LEN, REPLICATE_RESPONSE_HEAD_LEN,
};
use crate::shared::protocol::{Header, MessageType, HEADER_LEN};
use crate::shared::tls::{PeerIdentity, PeerTlsHolder};

/// What a queued frame carries, so the owner can answer for it if it is never written
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameKey {
    /// A forward carrying these queries, by bundle and index
    Forward(Vec<(Uuid, u64)>),
    /// A control request with this correlation id
    Control(u64),
    /// A snapshot frame of this many payload bytes
    Bulk(usize),
    /// A replication request with this correlation id
    Replication(u64),
}

/// A frame waiting to be written
#[derive(Debug)]
pub struct Frame {
    /// The encoded header
    header: [u8; HEADER_LEN],
    /// The body, in the order it goes on the wire
    ///
    /// `Bytes` so that a forwarded bundle is the client's buffer shared, never copied.
    parts: Vec<Bytes>,
    /// What this frame carries
    key: FrameKey,
    /// How many bytes it takes, header included
    len: usize,
    /// The version the body is encoded at, if it was encoded at one of its own
    ///
    /// None for a body that reads the same at every version, which the writer stamps with
    /// the link's negotiated version as it goes out
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    version: Option<u8>,
}

impl Frame {
    /// Build a frame whose body reads the same at every version this build speaks
    ///
    /// # Arguments
    ///
    /// * `kind` - What the frame carries
    /// * `parts` - The body, in the order it goes on the wire
    /// * `key` - What the owner needs back if it is never written
    /// * `max_frame_bytes` - The largest frame the peer accepts
    pub fn new(
        kind: MessageType,
        parts: Vec<Bytes>,
        key: FrameKey,
        max_frame_bytes: u32,
    ) -> Result<Self, ServerError> {
        let body_len = parts.iter().map(Bytes::len).sum::<usize>();
        let header = codec::header(kind, body_len, max_frame_bytes)?;
        Ok(Frame {
            header,
            parts,
            key,
            len: HEADER_LEN + body_len,
            version: None,
        })
    }

    /// Build a frame whose body was encoded at a named version
    ///
    /// The header carries that version whatever the link negotiates later, since the receiver
    /// decodes the body at the version the header names
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    ///
    /// # Arguments
    ///
    /// * `version` - The version the body is encoded at
    /// * `kind` - What the frame carries
    /// * `parts` - The body, in the order it goes on the wire
    /// * `key` - What the owner needs back if it is never written
    /// * `max_frame_bytes` - The largest frame the peer accepts
    pub fn at(
        version: u8,
        kind: MessageType,
        parts: Vec<Bytes>,
        key: FrameKey,
        max_frame_bytes: u32,
    ) -> Result<Self, ServerError> {
        let mut frame = Self::new(kind, parts, key, max_frame_bytes)?;
        frame.header[0] = version;
        frame.version = Some(version);
        Ok(frame)
    }

    /// How many bytes this frame takes on the wire
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether this frame is empty, which no frame is
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// What a link tells its owner
#[derive(Debug)]
pub enum LinkEvent {
    /// The handshake completed
    Up {
        /// The peer
        node: NodeId,
        /// The lane
        lane: Lane,
        /// Which run of the peer answered
        incarnation: u64,
        /// What the hello agreed ([F48](../../../../docs/src/features/rolling-compatibility.md))
        negotiated: Negotiated,
    },
    /// The connection was lost, or never made
    Down {
        /// The peer
        node: NodeId,
        /// The lane
        lane: Lane,
        /// Every frame still queued, which was never written
        unsent: Vec<FrameKey>,
        /// Why, for the log
        reason: String,
        /// The refusal the peer answered the hello with, if the dial got that far
        ///
        /// A `Removed` here is a verdict on this node's identity, not on the connection, and
        /// the owner stops on it ([F49](../../../../docs/src/features/backup-and-recovery.md)).
        refused: Option<crate::shared::protocol::peer::PeerRefusal>,
    },
    /// The peer sent a frame back
    Frame {
        /// The peer
        node: NodeId,
        /// The lane
        lane: Lane,
        /// The frame's header
        header: Header,
        /// The frame's fixed head, whose length the kind decides
        head: Vec<u8>,
        /// The frame's payload, in an allocation of its own
        payload: AlignedVec,
    },
}

/// Where a link is in its life
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// Never dialled, or between a drop and the next dial
    Idle,
    /// Dialling and shaking hands
    Connecting,
    /// Connected and carrying frames
    Up,
    /// Waiting out a backoff after a drop
    Backoff,
}

impl LinkState {
    /// Get the name this state is reported under
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LinkState::Idle => "idle",
            LinkState::Connecting => "connecting",
            LinkState::Up => "up",
            LinkState::Backoff => "backoff",
        }
    }
}

/// What a link looks like from outside, for the transport view and the tests
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LinkView {
    /// The peer
    pub node: NodeId,
    /// The lane
    pub lane: String,
    /// Where the link is in its life
    pub state: String,
    /// Bytes queued and not yet written
    pub queued_bytes: usize,
    /// The most bytes the queue holds
    pub bound: usize,
    /// Frames written since the link was made
    pub sent_frames: u64,
    /// Bytes written since the link was made
    pub sent_bytes: u64,
    /// Frames refused at the queue since the link was made
    pub shed_frames: u64,
    /// Frames dropped unwritten by a lost connection
    pub dropped_frames: u64,
    /// How many times the link has been dialled
    pub dials: u64,
    /// The incarnation of the peer the link is up with, if it is up
    pub peer_incarnation: Option<u64>,
    /// The wire version the link negotiated, if it is up
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md))
    #[serde(default)]
    pub wire_version: Option<u8>,
    /// The capabilities both ends act on, if it is up
    #[serde(default)]
    pub capabilities: Option<u64>,
    /// Why the last dial failed, if one has, kept until a dial succeeds
    /// ([F50](../../../../docs/src/features/cluster-operations.md))
    #[serde(default)]
    pub last_failure: Option<String>,
}

/// The queue and counters both halves of a link share
struct Queue {
    /// Frames waiting to be written
    frames: VecDeque<Frame>,
    /// Bytes across those frames
    queued_bytes: usize,
    /// The most bytes the queue holds
    bound: usize,
    /// Who to wake when a frame is queued
    waker: Option<Waker>,
    /// Where the link is in its life
    state: LinkState,
    /// The peer's incarnation while the link is up
    peer_incarnation: Option<u64>,
    /// What the hello agreed, while the link is up
    negotiated: Option<Negotiated>,
    /// Why the last dial failed, if one has, cleared by a dial that succeeds
    last_failure: Option<String>,
    /// Frames written
    sent_frames: u64,
    /// Bytes written
    sent_bytes: u64,
    /// Frames refused at the queue
    shed_frames: u64,
    /// Frames dropped unwritten
    dropped_frames: u64,
    /// Dials made
    dials: u64,
    /// Whether the owner has gone away
    closed: bool,
}

impl Queue {
    /// Take every queued frame's key, emptying the queue
    fn drain_keys(&mut self) -> Vec<FrameKey> {
        let keys = self
            .frames
            .drain(..)
            .map(|frame| frame.key)
            .collect::<Vec<_>>();
        self.dropped_frames += keys.len() as u64;
        self.queued_bytes = 0;
        keys
    }
}

/// A lane to one peer, from the owner's side
pub struct Link {
    /// The peer, or the nil id for a seed
    node: NodeId,
    /// The lane
    lane: Lane,
    /// Where it dials, and who it expects there
    target: PeerAddr,
    /// The queue shared with the task
    queue: Rc<RefCell<Queue>>,
    /// The task, held so dropping the link stops it
    task: Option<JoinHandle<()>>,
}

impl Link {
    /// Open a lane to a peer, dialling lazily on the first frame
    ///
    /// # Arguments
    ///
    /// * `lane` - Which lane this is
    /// * `entry` - Where to dial and who to expect there
    /// * `local` - What this node says about itself, read at every dial
    /// * `transport` - The bounds and timers
    /// * `tls` - What to dial with, read at every dial so a reload reaches the next one
    /// * `on_event` - Where to deliver what the link learns
    pub fn spawn<F: Fn(LinkEvent) + 'static>(
        lane: Lane,
        entry: PeerAddr,
        local: Rc<RefCell<Local>>,
        transport: &Transport,
        tls: PeerTlsHolder,
        on_event: F,
    ) -> Self {
        let bound = match lane {
            Lane::Data => transport.data_queue_bytes,
            Lane::Control => transport.control_queue_bytes,
            Lane::Bulk => transport.bulk_queue_bytes,
            Lane::Replication => transport.replication_queue_bytes,
        };
        let queue = Rc::new(RefCell::new(Queue {
            frames: VecDeque::new(),
            queued_bytes: 0,
            bound,
            waker: None,
            state: LinkState::Idle,
            peer_incarnation: None,
            negotiated: None,
            last_failure: None,
            sent_frames: 0,
            sent_bytes: 0,
            shed_frames: 0,
            dropped_frames: 0,
            dials: 0,
            closed: false,
        }));
        let node = entry.node_or_nil();
        let target = entry.clone();
        let settings = Settings {
            lane,
            entry,
            local,
            tls,
            handshake_timeout: transport.handshake_timeout.duration(),
            reconnect_min: transport.reconnect_min.duration(),
            reconnect_max: transport.reconnect_max.duration(),
        };
        // the task waits for the first frame before it dials, so a peer nothing is sent to is
        // a peer nothing connects to
        let task = glommio::spawn_local(run(settings, queue.clone(), on_event)).detach();
        Link {
            node,
            lane,
            target,
            queue,
            task: Some(task),
        }
    }

    /// Where this link dials, and who it expects there
    #[must_use]
    pub fn target(&self) -> &PeerAddr {
        &self.target
    }

    /// Queue a frame, or refuse it because the queue is at its bound
    ///
    /// Synchronous and judged before anything else happens to the frame: a refusal means the
    /// peer will never see it, so the owner answers with a definite refusal.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to queue
    ///
    /// # Errors
    ///
    /// Hands the frame back with the bound it would have passed.
    pub fn enqueue(&self, frame: Frame) -> Result<(), (Frame, usize)> {
        let mut queue = self.queue.borrow_mut();
        // the bound is on bytes, and a frame that would pass it is shed whole
        if queue.queued_bytes + frame.len() > queue.bound {
            queue.shed_frames += 1;
            let bound = queue.bound;
            return Err((frame, bound));
        }
        queue.queued_bytes += frame.len();
        queue.frames.push_back(frame);
        // wake the task, which may be waiting for exactly this
        if let Some(waker) = queue.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// Whether the link is up right now
    #[must_use]
    pub fn is_up(&self) -> bool {
        self.queue.borrow().state == LinkState::Up
    }

    /// What the link negotiated at its hello, or the floor while it is not up
    ///
    /// A body that differs between versions is encoded at this and framed with [`Frame::at`],
    /// so the header says what the body is whatever the link speaks by the time it is written
    /// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    #[must_use]
    pub fn negotiated(&self) -> Negotiated {
        let queue = self.queue.borrow();
        queue
            .negotiated
            .unwrap_or_else(|| Negotiated::floor(protocol_default_max()))
    }

    /// What the link negotiated at its hello, or nothing while it is not up
    ///
    /// Unlike [`Link::negotiated`], which assumes the floor for a link that is down, this tells
    /// a link that is down apart from one up with a peer that lacks a capability.
    #[must_use]
    pub fn negotiated_if_up(&self) -> Option<Negotiated> {
        self.queue.borrow().negotiated
    }

    /// Whether the link's owner let it go, after which nothing queued is ever written
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.borrow().closed
    }

    /// The peer this link reaches
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// What this link looks like from outside
    #[must_use]
    pub fn view(&self) -> LinkView {
        let queue = self.queue.borrow();
        LinkView {
            node: self.node,
            lane: self.lane.name().to_string(),
            state: queue.state.as_str().to_string(),
            queued_bytes: queue.queued_bytes,
            bound: queue.bound,
            sent_frames: queue.sent_frames,
            sent_bytes: queue.sent_bytes,
            shed_frames: queue.shed_frames,
            dropped_frames: queue.dropped_frames,
            dials: queue.dials,
            peer_incarnation: queue.peer_incarnation,
            wire_version: queue.negotiated.map(|negotiated| negotiated.version),
            capabilities: queue.negotiated.map(|negotiated| negotiated.capabilities),
            last_failure: queue.last_failure.clone(),
        }
    }
}

/// The frame bound a link assumes of a peer it has not spoken to
fn protocol_default_max() -> u32 {
    crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES
}

impl Drop for Link {
    /// Stop the task and drop whatever was queued
    fn drop(&mut self) {
        // tell the task the owner is gone, in case it is between awaits
        self.queue.borrow_mut().closed = true;
        if let Some(task) = self.task.take() {
            task.cancel();
        }
    }
}

/// What the task needs to dial and shake hands
struct Settings {
    /// Which lane this is
    lane: Lane,
    /// Where to dial and who to expect there
    entry: PeerAddr,
    /// What this node says about itself, read at every dial since a joiner's cluster changes
    local: Rc<RefCell<Local>>,
    /// What to dial with, read at every dial ([F50](../../../../docs/src/features/cluster-operations.md))
    tls: PeerTlsHolder,
    /// How long a dial and handshake may take
    handshake_timeout: Duration,
    /// The shortest backoff
    reconnect_min: Duration,
    /// The longest backoff
    reconnect_max: Duration,
}

/// A future that resolves when the queue has a frame, or the owner has gone
struct NextFrame {
    /// The queue to watch
    queue: Rc<RefCell<Queue>>,
}

impl Future for NextFrame {
    type Output = Option<Frame>;

    /// Take the next frame if there is one, else park until one is queued
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut queue = self.queue.borrow_mut();
        if let Some(frame) = queue.frames.pop_front() {
            queue.queued_bytes -= frame.len();
            return Poll::Ready(Some(frame));
        }
        if queue.closed {
            return Poll::Ready(None);
        }
        queue.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// A future that resolves once the queue is not empty, without taking anything
struct Wanted {
    /// The queue to watch
    queue: Rc<RefCell<Queue>>,
}

impl Future for Wanted {
    type Output = bool;

    /// Resolve when a frame is queued (`true`) or the owner has gone (`false`)
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut queue = self.queue.borrow_mut();
        if !queue.frames.is_empty() {
            return Poll::Ready(true);
        }
        if queue.closed {
            return Poll::Ready(false);
        }
        queue.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// Whether a dial failed on a verdict about who the peer is, which no queued frame changes
///
/// The certificate named another node, the hello named another identity, or the peer refused
/// ours as mismatched, removed or of another cluster. A refused connection or a timeout is not
/// one: the node may be starting again at the same address. Nor is a peer that does not know us
/// yet, which a joiner meets until the map reaches it; a frame that wants to go should find
/// either at once.
///
/// # Arguments
///
/// * `error` - Why the dial failed
pub(super) fn is_identity_verdict(error: &ServerError) -> bool {
    use crate::server::errors::ShoalError;
    use crate::shared::protocol::peer::PeerRefusal;
    match error {
        ServerError::Shoal(ShoalError::CertificateIdentity { .. } | ShoalError::PeerIdentity { .. }) => true,
        ServerError::Shoal(ShoalError::PeerRefused { reason, .. }) => matches!(
            reason,
            PeerRefusal::IdentityMismatch | PeerRefusal::Removed | PeerRefusal::WrongCluster
        ),
        _ => false,
    }
}

/// The link's life: wait to be wanted, dial, carry, drop, back off, again
///
/// # Arguments
///
/// * `settings` - How to dial and shake hands
/// * `queue` - The queue shared with the owner
/// * `on_event` - Where to deliver what the link learns
async fn run<F: Fn(LinkEvent) + 'static>(
    settings: Settings,
    queue: Rc<RefCell<Queue>>,
    on_event: F,
) {
    let node = settings.entry.node_or_nil();
    let lane = settings.lane;
    let mut backoff = settings.reconnect_min;
    let mut attempt = 0u64;
    loop {
        // nothing to send is nothing to connect for
        if !(Wanted {
            queue: queue.clone(),
        })
        .await
        {
            return;
        }
        // dial and shake hands, under one deadline
        {
            let mut q = queue.borrow_mut();
            q.state = LinkState::Connecting;
            q.dials += 1;
        }
        let connected = glommio::timer::timeout(settings.handshake_timeout, async {
            Ok(connect(&settings).await)
        })
        .await
        // flatten the deadline error and the dial error into one
        .map_err(ServerError::from)
        .and_then(|inner| inner);
        let (stream, peer_incarnation, negotiated) = match connected {
            Ok(established) => established,
            Err(error) => {
                // never connected, so everything queued was never written; the reason is
                // kept for the transport view until a dial succeeds
                let unsent = {
                    let mut q = queue.borrow_mut();
                    q.state = LinkState::Backoff;
                    q.last_failure = Some(format!("{error}"));
                    q.drain_keys()
                };
                event!(Level::WARN, msg = "a peer link could not be made", %node, %lane, ?error);
                // a refusal at the hello is the peer's verdict, carried apart from the text
                let refused = match &error {
                    ServerError::Shoal(crate::server::errors::ShoalError::PeerRefused {
                        reason,
                        ..
                    }) => Some(*reason),
                    _ => None,
                };
                on_event(LinkEvent::Down {
                    node,
                    lane,
                    unsent,
                    reason: format!("{error:?}"),
                    refused,
                });
                // wait out the backoff, growing it with jitter, and try again if still wanted:
                // never sooner than the floor, and no later than the first frame that wants
                // to go, since a frame waiting out a backoff is a caller waiting out a backoff.
                // a verdict on who the peer is is not changed by a frame, so it waits the whole
                // backoff: a rebuilt node's old address answered its every heartbeat with a
                // handshake that could only fail, ten times a second a link
                // ([Resolved #172](../../../../docs/src/appendix/resolved/identity-refusal-redials.md))
                let wait = jittered(backoff, attempt, settings.local.borrow().incarnation);
                attempt = attempt.wrapping_add(1);
                backoff = (backoff * 2).min(settings.reconnect_max);
                let floor = if is_identity_verdict(&error) {
                    wait
                } else {
                    wait.min(settings.reconnect_min)
                };
                glommio::timer::sleep(floor).await;
                let rest = wait.saturating_sub(floor);
                if !rest.is_zero() {
                    let wanted = Wanted {
                        queue: queue.clone(),
                    };
                    let _ = glommio::timer::timeout(rest, async { Ok(wanted.await) }).await;
                }
                if queue.borrow().closed {
                    return;
                }
                continue;
            }
        };
        // up, so the backoff starts over
        backoff = settings.reconnect_min;
        {
            let mut q = queue.borrow_mut();
            q.state = LinkState::Up;
            q.peer_incarnation = Some(peer_incarnation);
            q.negotiated = Some(negotiated);
            q.last_failure = None;
        }
        on_event(LinkEvent::Up {
            node,
            lane,
            incarnation: peer_incarnation,
            negotiated,
        });
        // carry frames both ways until either direction fails
        let (rx, tx) = stream.split();
        let max_frame_bytes = settings.local.borrow().max_frame_bytes;
        let outcome = carry(
            rx,
            tx,
            &queue,
            node,
            lane,
            max_frame_bytes,
            negotiated.version,
            &on_event,
        )
        .await;
        // whatever was still queued was never written
        let unsent = {
            let mut q = queue.borrow_mut();
            q.state = LinkState::Backoff;
            q.peer_incarnation = None;
            q.negotiated = None;
            q.drain_keys()
        };
        event!(Level::WARN, msg = "a peer link dropped", %node, %lane, reason = ?outcome);
        on_event(LinkEvent::Down {
            node,
            lane,
            unsent,
            reason: format!("{outcome:?}"),
            refused: None,
        });
        if queue.borrow().closed {
            return;
        }
        // a link that dropped waits the shortest backoff before dialling again
        let incarnation = settings.local.borrow().incarnation;
        glommio::timer::sleep(jittered(settings.reconnect_min, attempt, incarnation)).await;
        attempt = attempt.wrapping_add(1);
    }
}

/// Dial the peer, take the wire and shake hands
///
/// # Arguments
///
/// * `settings` - How to dial and shake hands
async fn connect(settings: &Settings) -> Result<(TcpStream, u64, Negotiated), ServerError> {
    // the member's address for this lane, which the bulk lane shares with data
    let addr = match settings.lane {
        Lane::Control => &settings.entry.control,
        Lane::Data | Lane::Bulk | Lane::Replication => &settings.entry.data,
    };
    let addr: SocketAddr = addr.parse().map_err(|_| {
        ServerError::Shoal(crate::server::errors::ShoalError::InvalidConfig(format!(
            "peer address {addr} is not an address"
        )))
    })?;
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    // take the wire first, if the lanes are encrypted, with the material as it is right now
    let certified = match settings.tls.client() {
        Some(config) => {
            let name = ServerName::from(addr.ip());
            super::tls::connect(&mut stream, config, name).await?.peer
        }
        None => PeerIdentity::Plaintext,
    };
    // then say who we are and check who answered, as we are right now
    let local = settings.local.borrow().clone();
    let (peer, negotiated) = handshake::dial(
        &mut stream,
        &local,
        settings.lane,
        &settings.entry,
        &certified,
        settings.tls.binds_identity(),
    )
    .await?;
    Ok((stream, peer.incarnation, negotiated))
}

/// Write queued frames and read answered ones until either direction fails
///
/// # Arguments
///
/// * `rx` - The read half of the connection
/// * `tx` - The write half of the connection
/// * `queue` - The queue shared with the owner
/// * `node` - The peer
/// * `lane` - The lane
/// * `max_frame_bytes` - The largest frame this end accepts
/// * `version` - The wire version the hello negotiated
/// * `on_event` - Where to deliver what the link reads
#[allow(clippy::too_many_arguments)]
async fn carry<F: Fn(LinkEvent) + 'static>(
    mut rx: ReadHalf<TcpStream>,
    mut tx: WriteHalf<TcpStream>,
    queue: &Rc<RefCell<Queue>>,
    node: NodeId,
    lane: Lane,
    max_frame_bytes: u32,
    version: u8,
    on_event: &F,
) -> Result<(), ServerError> {
    // the writer drains the queue
    let writer = async {
        loop {
            let Some(frame) = (NextFrame {
                queue: queue.clone(),
            })
            .await
            else {
                return Ok(());
            };
            // a frame built at no version of its own goes out at the negotiated one; one built
            // at a version of its own keeps it, which is what its body is encoded at
            let mut header = frame.header;
            if frame.version.is_none() {
                header[0] = version;
            }
            let parts: Vec<&[u8]> = frame.parts.iter().map(|part| &part[..]).collect();
            codec::write_frame(&mut tx, &header, &parts).await?;
            let mut q = queue.borrow_mut();
            q.sent_frames += 1;
            q.sent_bytes += frame.len() as u64;
        }
    };
    // the reader hands every answer to the owner
    let reader = async {
        loop {
            let Some(header) = codec::read_header(&mut rx, max_frame_bytes, version).await? else {
                return Err::<(), ServerError>(
                    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "the peer closed")
                        .into(),
                );
            };
            // the head's length is the kind's to say, and only answers come back on a link
            let head_len = match header.kind {
                MessageType::Forwarded => FORWARDED_PREAMBLE_LEN,
                MessageType::ControlResponse => CONTROL_HEAD_LEN,
                MessageType::ReplicateResponse => REPLICATE_RESPONSE_HEAD_LEN,
                other => {
                    return Err(
                        crate::shared::protocol::ProtocolError::UnexpectedMessageType {
                            expected: MessageType::Forwarded,
                            got: other,
                        }
                        .into(),
                    );
                }
            };
            // a frame that cannot hold its own head is not one
            let Some(payload_len) = header.body_len().checked_sub(head_len) else {
                return Err(crate::shared::protocol::ProtocolError::BodyTooShort {
                    need: head_len,
                    got: header.len,
                }
                .into());
            };
            let head = codec::read_vec(&mut rx, head_len).await?;
            let payload = codec::read_body(&mut rx, payload_len).await?;
            on_event(LinkEvent::Frame {
                node,
                lane,
                header,
                head,
                payload,
            });
        }
    };
    // whichever direction ends first ends the connection
    match select(pin!(writer), pin!(reader)).await {
        Either::Left((outcome, _)) => outcome,
        Either::Right((outcome, _)) => outcome,
    }
}

/// A backoff with jitter, so a cluster's links do not all redial in step
///
/// # Arguments
///
/// * `base` - The backoff to jitter
/// * `attempt` - Which attempt this is, which seeds the jitter
/// * `incarnation` - Which start of this node this is, so two nodes' links differ too
fn jittered(base: Duration, attempt: u64, incarnation: u64) -> Duration {
    // a small hash of the attempt and the start, spread over plus or minus a quarter
    let seed = attempt
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(incarnation);
    let unit = (seed >> 11) as f64 / (1u64 << 53) as f64;
    let factor = 0.75 + unit * 0.5;
    base.mul_f64(factor)
}

/// Give the reader half of a split stream back its bytes, for a listener that needs one
///
/// Not used by the link itself; here so that the two halves of the peer module read the same
/// way.
pub async fn read_exact_into(
    rx: &mut ReadHalf<TcpStream>,
    buffer: &mut [u8],
) -> Result<(), ServerError> {
    rx.read_exact(buffer).await?;
    Ok(())
}
