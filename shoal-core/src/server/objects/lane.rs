//! The object lane: a slice's chunk requests between nodes, served on the slice's own executor
//!
//! Bytes between nodes travel on a fifth lane, a socket of their own for every slice a peer talks
//! to, so a mebibyte of chunk queued on one never sits ahead of a vote, a forward or a snapshot
//! chunk ([S13](../../../../docs/src/object-storage/isolation.md)). Both ends run on object
//! executors, never on a table shard ([F86](../../../../docs/src/features/replicated-pools.md)):
//!
//! - **The serving end.** A shard's listener accepts the connection, reads its hello - which names
//!   the slice its requests are for - and hands it, whole and kTLS included, to the object executor
//!   that owns that slice, the way [F84](../../../../docs/src/features/replication-lane-handoff.md)
//!   hands a replication lane to its slot's executor. That executor answers every request on the
//!   lane itself, so no frame's bytes hop between executors
//!   ([X11](../../../../docs/src/object-storage/streamed-bodies.md)).
//! - **The dialling end.** Each object executor keeps one [`Link`] a remote slice it sends to,
//!   dialled on first use with the shard links' backoff, hello and TLS, whose queue is bounded by
//!   `transport.object_queue_bytes`. A request sent from an object executor - a put's pipeline -
//!   goes out on that executor's own link; one sent from a table shard is handed to an object
//!   executor chosen by the holder, so one caller's requests to one holder keep their order. A
//!   request that finds its link's queue at its bound waits for room while the link goes on
//!   writing, so a sender is held to the rate its holder takes bytes at
//!   ([Resolved #238](../../../../docs/src/appendix/resolved/object-lane-backpressure.md)).
//!
//! # Invariants
//!
//! **Every request is answered once**, under its correlation id: done, refused with a store code,
//! a label, or bytes. A link that drops answers every request it carried unavailable, which its
//! sender counts as a holder that did not answer. A reader's read whose answer has not come within
//! `transport.object_read_timeout` is refused unavailable by its link's sweep, and its answer let
//! go if it comes after; nothing else is bounded there
//! ([item 316](../../../../docs/src/appendix/resolved/unanswered-read-bound.md)).
//!
//! **A stalled holder holds only its lane**: its link's queue is bounded, and a request that does
//! not fit waits only while the link writes; one that waits [`STALL_AFTER`] with nothing written
//! is refused, and nothing but the callers that sent to that holder waits for it. A served lane
//! reads no more than [`MAX_SERVED`] requests ahead of its answers, so a sender that stops reading
//! its answers stops its own requests and nobody else's.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Instant;

use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use glommio::net::TcpStream;
use glommio::sync::{Semaphore, StaticPermit};
use tracing::{event, Level};

use super::budget::{Budget, DRAW_WAIT};
use super::executor::{Answer, Holder};
use super::inflight::{Gauge, Held, InFlight, Throttle, WARN_EVERY};
use super::serve::{self, Answered, Replier, Streams};
use super::store::{Slice, StoreError};
use crate::server::conf::cluster::{DialOverride, Transport};
use crate::server::map::TabletMap;
use crate::server::peer::codec;
use crate::server::peer::handoff::HandedLane;
use crate::server::peer::handshake::{Local, Negotiated, PeerAddr};
use crate::server::peer::link::{Frame, FrameKey, Link, LinkEvent};
use crate::server::request_body::RequestBody;
use crate::server::ServerError;
use crate::shared::identity::NodeId;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, StoreCode};
use crate::shared::protocol::{MessageType, ProtocolError, OBJECT_WIRE_VERSION};
use crate::shared::tls::PeerTlsHolder;

/// The most requests a served lane reads ahead of its answers
pub const MAX_SERVED: u64 = 256;

/// How long a request waits for room at a link's queue while the link writes nothing, before the
/// holder is taken to have stalled and the request is refused
pub const STALL_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

/// How often a request waiting for room looks at its link's queue again
const ROOM_POLL: std::time::Duration = std::time::Duration::from_millis(1);

/// The warnings that a lane wrote nothing for [`STALL_AFTER`] and refused a request, one logged
/// every [`WARN_EVERY`] with the count of the rest (F108)
static STALLED: Throttle = Throttle::new();

/// The warnings that a served lane owes [`MAX_SERVED`] answers and reads no more until one is
/// written, one logged every [`WARN_EVERY`] (F108)
static SERVED_FULL: Throttle = Throttle::new();

/// The warnings that a read had no answer within its bound and was refused, one logged every
/// [`WARN_EVERY`] with the count of the rest (item 316)
static UNANSWERED: Throttle = Throttle::new();

/// The lines that an answer came for a read refused at its bound, one logged every [`WARN_EVERY`]
/// (item 316)
static LATE: Throttle = Throttle::new();

/// The most reads refused at their bound a dialled lane remembers, so an answer that comes after
/// is said to be late; past it the oldest is forgotten (item 316)
const REFUSED_KEPT: usize = 1024;

/// What a node's object executors need to dial an object lane, on a cluster node
#[derive(Debug, Clone)]
pub struct LanePlan {
    /// What this node says about itself in every hello
    pub local: Local,
    /// Where particular members are dialled instead of where they advertise
    pub dial: BTreeMap<NodeId, DialOverride>,
    /// What the lanes are encrypted with, if they are
    pub tls: PeerTlsHolder,
    /// The bounds and timers every lane keeps
    pub transport: Transport,
}

/// Where a request's answer goes, on the channel its sender is waiting on
pub type Waiter = kanal::AsyncSender<Result<Answer, StoreError>>;

/// How a request a lane serves is answered: once, a read's bytes where its store left them
/// included ([F106](../../../../docs/src/features/lane-copies.md))
type LaneReplier = Box<dyn FnOnce(Answered)>;

/// An answer a served lane owes, with what it holds until it is written
struct Owed {
    /// The correlation id of the request it answers
    id: u64,
    /// The answer, a read's buffers and their draw with it
    answer: Answered,
    /// Its place in the executor's count of answers owed, given back once it is written (F108)
    _owed: Held,
    /// The lane's room its request took, given back once the answer is written, so a peer that
    /// stops reading its answers stops its own requests
    _room: StaticPermit,
}

/// A lane's replier as one of whole answers, for what answers a request on its own
///
/// # Arguments
///
/// * `replier` - The lane's replier
fn whole(replier: LaneReplier) -> Replier {
    Box::new(move |reply| replier(Answered::Reply(reply)))
}

/// What one executor's lanes have done, for the transport view and the tests
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LaneFigures {
    /// Lanes this executor dialled, one a remote slice
    pub links: u64,
    /// Requests it sent over them
    pub sent: u64,
    /// Requests refused at a link's queue, its holder not keeping up
    pub shed: u64,
    /// Requests that waited for room at a link's queue before it took them
    #[serde(default)]
    pub waited: u64,
    /// Lanes it was handed to serve
    pub adopted: u64,
    /// Requests it answered on them
    pub served: u64,
    /// Answers its served lanes wrote
    #[serde(default)]
    pub answers_written: u64,
    /// Calls those answers went in, several ready together in one (F111)
    #[serde(default)]
    pub answer_writes: u64,
    /// Pieces of whole chunks whose units its served lanes read straight into a buffer their
    /// device writes from (F111)
    #[serde(default)]
    pub aligned_pieces: u64,
}

impl LaneFigures {
    /// Two executors' figures added together
    ///
    /// # Arguments
    ///
    /// * `other` - The other's
    #[must_use]
    pub fn plus(self, other: LaneFigures) -> LaneFigures {
        LaneFigures {
            links: self.links + other.links,
            sent: self.sent + other.sent,
            shed: self.shed + other.shed,
            waited: self.waited + other.waited,
            adopted: self.adopted + other.adopted,
            served: self.served + other.served,
            answers_written: self.answers_written + other.answers_written,
            answer_writes: self.answer_writes + other.answer_writes,
            aligned_pieces: self.aligned_pieces + other.aligned_pieces,
        }
    }
}

/// One dialled lane to one remote slice, and the requests it carries unanswered
struct ChunkLink {
    /// The link
    link: Link,
    /// Each request's waiter, by correlation id
    waiters: Rc<RefCell<HashMap<u64, Pending>>>,
    /// The next correlation id
    next: Cell<u64>,
    /// The peer's frame bound, once a hello agreed one
    max_frame: Rc<Cell<u32>>,
    /// What the link read and the reads it refused unanswered, shared with what it learns
    unanswered: Rc<Unanswered>,
    /// Whether a task refuses the reads it carries past their bound, which runs while any waits
    sweeping: Cell<bool>,
}

/// A request a dialled lane carries unanswered: where its answer goes, and its place in the
/// executor's count of requests waiting, given back however the request ends (F108)
struct Pending {
    /// Where its answer goes
    waiter: Waiter,
    /// Its place in the count
    _held: Held,
    /// When a reader's read is refused if its answer has not come, and what is said of it then
    /// (item 316); none for any other request
    bounded: Option<Bounded>,
}

/// A reader's read a dialled lane carries, as it was when queued, which is refused if its answer
/// has not come by its bound (item 316)
struct Bounded {
    /// When it is refused
    until: Instant,
    /// When it was queued
    queued: Instant,
    /// The chunk it reads
    chunk: crate::shared::stripes::ChunkId,
    /// Where in the chunk it starts
    offset: u64,
    /// How many bytes it asks for
    len: u64,
    /// The frames the link had written when it was queued
    written: u64,
    /// The frames queued ahead of it on the link, itself included
    place: u64,
    /// The answers the link had read when it was queued
    answered: u64,
}

/// What a dialled lane read, and the reads it refused unanswered at their bound (item 316)
#[derive(Default)]
struct Unanswered {
    /// The answers the link has read
    answered: Cell<u64>,
    /// Each read refused at its bound by correlation id, with when it was queued, so an answer
    /// that comes for it after is said to be late; forgotten when the link drops
    refused: RefCell<BTreeMap<u64, Instant>>,
}

thread_local! {
    /// The lanes of the object executor this thread is, if it is one
    static CURRENT: RefCell<Option<Rc<Lanes>>> = const { RefCell::new(None) };
}

/// The lanes of the object executor this thread is, if it is one
#[must_use]
pub fn current() -> Option<Rc<Lanes>> {
    CURRENT.with(|current| current.borrow().clone())
}

/// What one object executor keeps for its lanes
pub struct Lanes {
    /// How to dial, on a cluster node
    plan: Option<LanePlan>,
    /// This node's hello, which a link reads at every dial
    local: Option<Rc<RefCell<Local>>>,
    /// The map the node last installed: members' addresses and the activated wire
    map: RefCell<Arc<TabletMap>>,
    /// The links this executor dialled, by holder
    links: RefCell<HashMap<Holder, Rc<ChunkLink>>>,
    /// The streams of whole chunks this executor is writing, shared with its mailbox
    streams: Streams,
    /// The executor's budget, which a served request's buffers are drawn from
    budget: Arc<Budget>,
    /// What the executor's lanes have in flight, which the store reads without it (F108)
    inflight: Arc<InFlight>,
    /// The executor's background queue, which a served background or scrub's request runs on
    background: Option<glommio::TaskQueueHandle>,
    /// The executor's apply queue, which a served apply or discard runs on, below the foreground
    /// and never counted as it (S13, F91)
    applies: Option<glommio::TaskQueueHandle>,
    /// The id the next served connection is told apart by, never zero, which is this node's own
    next_origin: Cell<u64>,
    /// What the lanes have done
    figures: Cell<LaneFigures>,
}

impl Lanes {
    /// One executor's lanes, before any is dialled or served, made this thread's
    ///
    /// # Arguments
    ///
    /// * `plan` - How to dial, on a cluster node
    /// * `streams` - The executor's streams of whole chunks
    /// * `budget` - The executor's budget
    /// * `inflight` - What the executor's lanes have in flight, read without it (F108)
    /// * `background` - The executor's background queue
    /// * `applies` - The executor's apply queue
    #[must_use]
    pub fn start(
        plan: Option<LanePlan>,
        streams: Streams,
        budget: Arc<Budget>,
        inflight: Arc<InFlight>,
        background: Option<glommio::TaskQueueHandle>,
        applies: Option<glommio::TaskQueueHandle>,
    ) -> Rc<Self> {
        // the hello a link writes, which a joiner's cluster is filled into later
        let local = plan
            .as_ref()
            .map(|plan| Rc::new(RefCell::new(plan.local.clone())));
        let lanes = Rc::new(Lanes {
            plan,
            local,
            map: RefCell::new(Arc::new(TabletMap::default())),
            links: RefCell::new(HashMap::new()),
            streams,
            budget,
            inflight,
            background,
            applies,
            next_origin: Cell::new(1),
            figures: Cell::new(LaneFigures::default()),
        });
        // a request sent from this thread goes out on this executor's own links
        CURRENT.with(|current| *current.borrow_mut() = Some(lanes.clone()));
        lanes
    }

    /// Let this thread's lanes go, as its executor stops, closing every link
    pub fn stop(&self) {
        self.links.borrow_mut().clear();
        CURRENT.with(|current| *current.borrow_mut() = None);
    }

    /// Change the figures
    ///
    /// # Arguments
    ///
    /// * `change` - What to change
    fn count(&self, change: impl FnOnce(&mut LaneFigures)) {
        let mut figures = self.figures.get();
        change(&mut figures);
        self.figures.set(figures);
    }

    /// What the lanes have done
    #[must_use]
    pub fn figures(&self) -> LaneFigures {
        self.figures.get()
    }

    /// Whether a whole chunk's file is sized to the class of units it holds: once the cluster has
    /// activated the wire every member sizes files at, and at once on a node of no cluster (F96)
    ///
    /// A cluster node's executor holds a map with no members and the floor's activation until the
    /// node installs its first, so it never sizes a file by class before it knows the cluster has.
    #[must_use]
    pub fn classed(&self) -> bool {
        // a node of no cluster has no plan to dial by, and no activation to wait for
        self.plan.is_none() || self.map.borrow().activated_wire >= crate::shared::protocol::CLASS_WIRE_VERSION
    }

    /// Take a map the node installed: the members a link dials, and the activated wire
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    pub fn install(&self, map: Arc<TabletMap>) {
        // a map older than the one held is old news
        if map.version < self.map.borrow().version {
            return;
        }
        // a joiner learns its cluster from the first map that names one
        if let Some(local) = &self.local {
            let mut local = local.borrow_mut();
            if local.cluster.is_none() {
                local.cluster = map.cluster;
            }
        }
        *self.map.borrow_mut() = map;
    }

    /// Send a request to a remote holder over its lane, its answer to the waiter given
    ///
    /// Returns once the request is on the lane's queue, or with the refusal the holder would have
    /// sent when it cannot be: the cluster has not activated the object wire, no lane reaches the
    /// holder's node, or the lane stalled. A queue at its bound is a holder behind, not gone: the
    /// request waits for room while the link goes on writing, which holds its sender to the rate
    /// the holder takes bytes at. A link that writes nothing for [`STALL_AFTER`] while the request
    /// waits is a stalled holder's, and the request is refused then
    /// ([Resolved #238](../../../../docs/src/appendix/resolved/object-lane-backpressure.md)).
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - The request
    /// * `waiter` - Where its answer goes, once it is queued
    ///
    /// # Errors
    ///
    /// The refusal, as `Unavailable`; the waiter is sent nothing then.
    pub async fn send(&self, holder: Holder, request: &ChunkRequest, waiter: Waiter) -> Result<(), ChunkReply> {
        // a refusal is the holder's not answering
        let unavailable = |message: String| ChunkReply::Refused {
            code: StoreCode::Unavailable,
            message,
        };
        let map = self.map.borrow().clone();
        // a lane is dialled only once every member speaks the version that serves one
        if map.activated_wire < OBJECT_WIRE_VERSION {
            return Err(unavailable(format!(
                "the cluster has activated wire {} and an object lane needs {OBJECT_WIRE_VERSION}",
                map.activated_wire
            )));
        }
        let Some(link) = self.link(holder, &map) else {
            return Err(unavailable(format!("no object lane reaches node {}", holder.node)));
        };
        // the request under an id of its own
        let id = link.next.get();
        link.next.set(id.wrapping_add(1));
        // its body in parts, a piece's or a unit's bytes shared with the request rather than copied
        // into a body for each holder it goes to (F106)
        let frame = Frame::new(
            MessageType::ChunkRequest,
            request.encode_parts(id),
            FrameKey::Chunk(id),
            link.max_frame.get(),
        )
        .map_err(|error| unavailable(format!("a chunk request did not frame: {error}")))?;
        // the queue's room, waited for while the link writes, and a stall refused
        let mut waited = false;
        let mut written = link.link.written();
        let mut since = std::time::Instant::now();
        loop {
            // a frame larger than the bound never fits, and one that fits is queued now
            if frame.len() > link.link.bound() || link.link.has_room(frame.len()) {
                // a reader's read is refused if its answer has not come by the lane's bound
                let bounded = self.bound_of(request, &link);
                let watched = bounded.is_some();
                // registered before it is queued, so its answer always finds its waiter
                link.waiters.borrow_mut().insert(
                    id,
                    Pending {
                        waiter: waiter.clone(),
                        _held: self.inflight.hold(Gauge::Waiting),
                        bounded,
                    },
                );
                match link.link.enqueue(frame) {
                    Ok(()) => {
                        self.count(|figures| {
                            figures.sent += 1;
                            figures.waited += u64::from(waited);
                        });
                        self.inflight.count_sent();
                        // and watched by the link's sweep, started if none runs (item 316)
                        if watched && !link.sweeping.replace(true) {
                            glommio::spawn_local(sweep(Rc::downgrade(&link), holder, self.inflight.clone())).detach();
                        }
                        return Ok(());
                    }
                    Err((_, bound)) => {
                        link.waiters.borrow_mut().remove(&id);
                        self.count(|figures| figures.shed += 1);
                        self.inflight.count_refused();
                        return Err(unavailable(format!(
                            "a request of more than the object lane's bound of {bound} bytes to slice {} on node {}",
                            holder.slice, holder.node
                        )));
                    }
                }
            }
            // the link writing anything is a holder taking bytes, however slowly
            let now = link.link.written();
            if now != written {
                written = now;
                since = std::time::Instant::now();
            }
            // and one that wrote nothing for the stall bound is stalled
            if since.elapsed() >= STALL_AFTER {
                self.count(|figures| figures.shed += 1);
                self.inflight.count_refused();
                // said, though a stalled holder refuses every request after, once a while (F108)
                if let Some(besides) = STALLED.pass(WARN_EVERY) {
                    event!(
                        Level::WARN,
                        msg = "an object lane wrote nothing for its stall bound and refused a request",
                        slice = %holder.slice,
                        node = %holder.node,
                        bound = link.link.bound(),
                        refused_besides = besides,
                    );
                }
                return Err(unavailable(format!(
                    "the object lane to slice {} on node {} holds its bound of {} bytes and wrote nothing for {:?}; its holder is not keeping up",
                    holder.slice,
                    holder.node,
                    link.link.bound(),
                    STALL_AFTER
                )));
            }
            waited = true;
            glommio::timer::sleep(ROOM_POLL).await;
        }
    }

    /// When a request queued now is refused if its answer has not come, and what is said of it
    /// then: a reader's read alone, by the lane's bound on one (item 316)
    ///
    /// A rebuild's, a move's and a scrub's reads come wrapped in their class and wait behind their
    /// holder's foreground and their device's ceiling, and a write is decided by its row, so none
    /// of them is bounded here.
    ///
    /// # Arguments
    ///
    /// * `request` - The request
    /// * `link` - The link it is queued on
    fn bound_of(&self, request: &ChunkRequest, link: &ChunkLink) -> Option<Bounded> {
        // a bare read is a reader's
        let ChunkRequest::Read { chunk, offset, len, .. } = request else {
            return None;
        };
        let bound = self.plan.as_ref()?.transport.object_read_timeout.duration();
        let queued = Instant::now();
        Some(Bounded {
            until: queued + bound,
            queued,
            chunk: *chunk,
            offset: *offset,
            len: *len,
            written: link.link.written(),
            // queued behind every frame the link holds now
            place: link.link.queued_frames() as u64 + 1,
            answered: link.unanswered.answered.get(),
        })
    }

    /// The link to a holder, dialled lazily on first use
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `map` - The map, whose member the holder is on
    fn link(&self, holder: Holder, map: &TabletMap) -> Option<Rc<ChunkLink>> {
        let plan = self.plan.as_ref()?;
        let local = self.local.clone()?;
        // where the member is dialled: its advertised address, or this node's override
        let mut entry: PeerAddr = map.peer_addr(holder.node)?;
        if let Some(data) = plan.dial.get(&holder.node).and_then(|target| target.data.clone()) {
            entry.data = data;
        }
        // a link to an address the member no longer advertises is dialled afresh
        if let Some(link) = self.links.borrow().get(&holder) {
            if link.link.target().data == entry.data {
                return Some(link.clone());
            }
        }
        let waiters: Rc<RefCell<HashMap<u64, Pending>>> = Rc::new(RefCell::new(HashMap::new()));
        let max_frame = Rc::new(Cell::new(local.borrow().max_frame_bytes));
        // whether the link is up, held in the executor's count while it is (F108)
        let up: Rc<RefCell<Option<Held>>> = Rc::new(RefCell::new(None));
        // what the link reads, and the reads it refused at their bound (item 316)
        let unanswered = Rc::new(Unanswered::default());
        let link = Link::spawn_for_slice(
            holder.slice,
            entry,
            local,
            &plan.transport,
            plan.tls.clone(),
            {
                let waiters = waiters.clone();
                let max_frame = max_frame.clone();
                let inflight = self.inflight.clone();
                let unanswered = unanswered.clone();
                move |event| on_event(holder, &waiters, &max_frame, &up, &inflight, &unanswered, event)
            },
        );
        let link = Rc::new(ChunkLink {
            link,
            waiters,
            next: Cell::new(1),
            max_frame,
            unanswered,
            sweeping: Cell::new(false),
        });
        self.links.borrow_mut().insert(holder, link.clone());
        self.count(|figures| figures.links += 1);
        Some(link)
    }

    /// Serve a lane a shard accepted and handed to this executor, on the slice it named
    ///
    /// # Arguments
    ///
    /// * `lane` - The lane, as the accepting shard gave it up
    /// * `slice` - The slice, if this executor owns it
    pub fn adopt(self: &Rc<Self>, lane: HandedLane, slice: Option<Rc<Slice>>) {
        let negotiated = lane.negotiated;
        let node = lane.origin;
        // the connection becomes a stream on this executor's reactor
        let stream = lane.adopt();
        // a slice this executor does not own closes the lane, and the peer dials again
        let Some(slice) = slice else {
            event!(Level::WARN, msg = "an object lane named a slice this executor does not own", %node);
            return;
        };
        self.count(|figures| figures.adopted += 1);
        let origin = self.next_origin.get();
        self.next_origin.set(origin.wrapping_add(1).max(1));
        let lanes = self.clone();
        glommio::spawn_local(async move {
            let (rx, tx) = stream.split();
            if let Err(error) = lanes.serve(slice, origin, negotiated, rx, tx).await {
                event!(Level::DEBUG, msg = "an object lane ended", %node, ?error);
            }
        })
        .detach();
    }

    /// Answer every request one lane carries, on its slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `origin` - The connection, as this executor tells it apart
    /// * `negotiated` - What its hello agreed
    /// * `rx` - Its read half
    /// * `tx` - Its write half
    async fn serve(
        self: &Rc<Self>,
        slice: Rc<Slice>,
        origin: u64,
        negotiated: Negotiated,
        mut rx: ReadHalf<TcpStream>,
        tx: WriteHalf<TcpStream>,
    ) -> Result<(), ServerError> {
        // counted as served while it is, however it ends (F108)
        let _serving = self.inflight.hold(Gauge::ServedLane);
        // the answers go out through one writer, in the order they are ready, which says when it
        // stopped so this stops reading too (item 307)
        let (answers, ready) = futures::channel::mpsc::unbounded::<Owed>();
        let stopped = Rc::new(Cell::new(false));
        let writer = glommio::spawn_local(write_answers(
            self.clone(),
            slice.state().id,
            tx,
            ready,
            negotiated,
            stopped.clone(),
        ))
        .detach();
        // how far reading may run ahead of the answers
        let room = Rc::new(Semaphore::new(MAX_SERVED));
        // where the lane's request bodies are read, their room given back as each is dropped (F111)
        let mut arena = bytes::BytesMut::new();
        let own_frame = self
            .local
            .as_ref()
            .map_or(crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES, |local| {
                local.borrow().max_frame_bytes
            });
        let outcome = loop {
            // a lane whose writer stopped is ended, never read on with its answers dropped: its
            // asker reads the end of the lane and refuses every request it carried (item 307)
            if stopped.get() {
                break Ok(());
            }
            // a test can stall this node's lanes, as a holder that stops reading would
            while super::faults::lanes_stalled() {
                glommio::timer::sleep(std::time::Duration::from_millis(20)).await;
            }
            // a lane that owes as many answers as it reads ahead of waits for one to be written,
            // and says so once a while: its peer's requests wait behind it (F108)
            if room.available() == 0 {
                if let Some(besides) = SERVED_FULL.pass(WARN_EVERY) {
                    event!(
                        Level::WARN,
                        msg = "a served object lane owes as many answers as it reads ahead of, and reads no more until one is written",
                        slice = %slice.state().id,
                        owed = MAX_SERVED,
                        warned_besides = besides,
                    );
                }
            }
            // a request read is one the lane has room to answer
            let Ok(permit) = room.acquire_static_permit(1).await else {
                break Ok(());
            };
            let header = match codec::read_header(&mut rx, own_frame, negotiated.version).await {
                Ok(Some(header)) => header,
                Ok(None) => break Ok(()),
                Err(error) => break Err(error),
            };
            // only requests come this way, and a frame that cannot be right ends the lane
            if header.kind != MessageType::ChunkRequest {
                break Err(ProtocolError::UnexpectedMessageType {
                    expected: MessageType::ChunkRequest,
                    got: header.kind,
                }
                .into());
            }
            // the request: a piece of a whole chunk's units read straight into a buffer its
            // device writes from, every other body unzeroed into the lane's arena with a unit's
            // bytes taken as slices of it, never copied out (F106, F111)
            let read = read_request(&mut rx, header.body_len(), &mut arena).await;
            let (id, request, aligned) = match read {
                Ok(read) => read,
                Err(error) => break Err(error),
            };
            // and a request read while the stall began is held unserved, as the next one would be
            while super::faults::lanes_stalled() {
                glommio::timer::sleep(std::time::Duration::from_millis(20)).await;
            }
            self.count(|figures| {
                figures.served += 1;
                figures.aligned_pieces += u64::from(aligned.is_some());
            });
            self.inflight.count_served();
            // owed from the moment it is read until its answer is written, or dropped unanswered
            let owed = self.inflight.hold(Gauge::Owed);
            // every request is answered once, under its id, and frees its room once its answer
            // is written (F106)
            let replier: LaneReplier = {
                let answers = answers.clone();
                Box::new(move |answer| {
                    let _ = answers.unbounded_send(Owed {
                        id,
                        answer,
                        _owed: owed,
                        _room: permit,
                    });
                })
            };
            self.dispatch(&slice, origin, request, aligned, replier);
        };
        // the writer ends once every answer still owed is written
        drop(answers);
        let _ = writer.await;
        outcome
    }

    /// Hand one request to whatever answers it on a slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `origin` - The connection it arrived on
    /// * `request` - The request
    /// * `aligned` - A piece's units, where they were read apart for its device (F111)
    /// * `replier` - How it is answered
    fn dispatch(
        &self,
        slice: &Rc<Slice>,
        origin: u64,
        request: ChunkRequest,
        aligned: Option<glommio::io::DmaBuffer>,
        replier: LaneReplier,
    ) {
        match request {
            // a piece goes to its stream's task now, before the next is read, so pieces keep
            // order; it is in flight on its slice until it is answered
            ChunkRequest::Write(piece) => {
                let replier = foreground(slice, whole(replier));
                self.streams.take(slice.clone(), origin, true, piece, aligned, replier);
            }
            ChunkRequest::Stage(piece) => {
                let replier = foreground(slice, whole(replier));
                self.streams.take(slice.clone(), origin, false, piece, aligned, replier);
            }
            // a background or a scrub's request waits for its slice's gate on a task of its own,
            // on the executor's background queue, then runs in its class, counted under it until
            // it is answered, and a piece of a whole chunk by its stream; a rebuild sends a chunk's
            // pieces one at a time (F88, F89, item 299)
            request @ (ChunkRequest::Background(_) | ChunkRequest::Scrub(_)) => {
                let streams = self.streams.clone();
                let budget = self.budget.clone();
                let slice = slice.clone();
                let task = async move {
                    let (inner, held) = super::executor::gate(&slice, request).await;
                    let replier: LaneReplier = Box::new(move |answer| {
                        replier(answer);
                        drop(held);
                    });
                    match inner {
                        ChunkRequest::Write(piece) => {
                            streams.take(slice, origin, true, piece, aligned, whole(replier));
                        }
                        ChunkRequest::Stage(piece) => {
                            streams.take(slice, origin, false, piece, aligned, whole(replier));
                        }
                        other => serve_drawn(&budget, &slice, other, replier).await,
                    }
                };
                let spawned = match self.background {
                    Some(queue) => glommio::spawn_local_into(task, queue).map(glommio::Task::detach),
                    None => Ok::<_, glommio::GlommioError<()>>(glommio::spawn_local(task).detach()),
                };
                if let Err(error) = spawned {
                    event!(Level::WARN, msg = "a background request could not be spawned", error = %error);
                }
            }
            // an apply or a discard runs on the apply queue, below the foreground and never
            // counted as it; the slice counts an apply while it runs (S13, F91)
            request @ (ChunkRequest::Apply { .. } | ChunkRequest::Discard { .. }) => {
                let budget = self.budget.clone();
                let slice = slice.clone();
                let task = async move { serve_drawn(&budget, &slice, request, replier).await };
                let spawned = match self.applies {
                    Some(queue) => glommio::spawn_local_into(task, queue).map(glommio::Task::detach),
                    None => Ok::<_, glommio::GlommioError<()>>(glommio::spawn_local(task).detach()),
                };
                if let Err(error) = spawned {
                    event!(Level::WARN, msg = "an apply could not be spawned", error = %error);
                }
            }
            // anything else runs on its own task, its buffers drawn from the budget first, in
            // flight on its slice while it runs
            other => {
                let budget = self.budget.clone();
                let slice = slice.clone();
                glommio::spawn_local(async move {
                    let _foreground = slice.state().foreground();
                    serve_drawn(&budget, &slice, other, replier).await;
                })
                .detach();
            }
        }
    }
}

/// A replier that counts its request in flight on its slice until it answers
///
/// # Arguments
///
/// * `slice` - The slice
/// * `replier` - How the request is answered
fn foreground(slice: &Rc<Slice>, replier: Replier) -> Replier {
    let held = slice.state().foreground();
    Box::new(move |reply| {
        replier(reply);
        drop(held);
    })
}

/// Serve a request on a slice, its buffers drawn from the budget first, and answer it
///
/// # Arguments
///
/// * `budget` - The executor's budget
/// * `slice` - The slice
/// * `request` - The request
/// * `replier` - How it is answered
async fn serve_drawn(budget: &Arc<Budget>, slice: &Rc<Slice>, request: ChunkRequest, replier: LaneReplier) {
    let draw = served_draws(&request);
    let permit = if draw > 0 {
        match budget.draw(draw, DRAW_WAIT).await {
            Ok(permit) => Some(permit),
            Err(error) => {
                replier(Answered::Reply(serve::refused(&error)));
                return;
            }
        }
    } else {
        None
    };
    // a read's bytes stay where the store read them, and go out from there (F106)
    replier(serve::answer(slice, request).await);
    drop(permit);
}

/// The bytes a request a lane serves draws before it runs: a read's none, since its store draws
/// what it reads and the span it answers holds that draw until its answer is written, where a read
/// was drawn here and again in the store (F106)
///
/// # Arguments
///
/// * `request` - The request
fn served_draws(request: &ChunkRequest) -> u64 {
    match request {
        ChunkRequest::Read { .. } => 0,
        ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => served_draws(inner),
        other => draws(other),
    }
}

/// The bytes a request's buffers hold on the slice that answers it, drawn from its budget
///
/// # Arguments
///
/// * `request` - The request
#[must_use]
pub fn draws(request: &ChunkRequest) -> u64 {
    match request {
        ChunkRequest::StageUnits { units, .. } | ChunkRequest::Fold { units, .. } => {
            units.iter().map(|unit| unit.bytes.len() as u64).sum()
        }
        // a parity stage holds its change, and the old units it reads to fold it into (F90)
        ChunkRequest::StageParity { changes, .. } => {
            2 * changes
                .iter()
                .flat_map(|change| change.units.iter())
                .map(|unit| unit.bytes.len() as u64)
                .sum::<u64>()
        }
        ChunkRequest::Read { len, .. } => *len,
        ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => draws(inner),
        _ => 0,
    }
}

/// Write a served lane's answers as they are ready, until every one owed is written
///
/// A read's answer is its head, then its bytes from the buffers the read left them in, one frame
/// and never copied into a body; each answer holds its buffers, their draw and its room until it
/// is written ([F106](../../../../docs/src/features/lane-copies.md)).
///
/// # Arguments
///
/// * `tx` - The lane's write half
/// * `ready` - The answers, each under its request's id
/// * `negotiated` - What the lane's hello agreed
/// * `stopped` - Set once an answer could not be written, which ends the lane's reading
async fn write_answers(
    lanes: Rc<Lanes>,
    slice: crate::shared::devices::SliceId,
    mut tx: WriteHalf<TcpStream>,
    mut ready: futures::channel::mpsc::UnboundedReceiver<Owed>,
    negotiated: Negotiated,
    stopped: Rc<Cell<bool>>,
) {
    while let Some(first) = ready.next().await {
        // a test can hold the answers back, so many are ready at once
        while super::faults::answers_held(slice) {
            glommio::timer::sleep(std::time::Duration::from_millis(20)).await;
        }
        // every answer ready now, in the order they were owed, within a call's bounds (F111)
        let mut batch = vec![Batched::of(first)];
        let mut bytes = batch[0].len();
        while batch.len() < BATCH_ANSWERS && bytes < BATCH_BYTES {
            let Ok(Some(owed)) = ready.try_next() else {
                break;
            };
            let next = Batched::of(owed);
            bytes += next.len();
            batch.push(next);
        }
        let written = write_batch(&mut tx, &negotiated, &batch).await;
        if written.is_ok() {
            lanes.count(|figures| {
                figures.answers_written += batch.len() as u64;
                figures.answer_writes += 1;
            });
        }
        // an answer that cannot be framed or written ends the lane: its write half is closed, so its
        // asker's link reads the end, refuses every request it carried and dials again, and its
        // reading stops. A writer that stopped alone left the lane reading and every answer after
        // dropped, its asker waiting on each with no bound (item 307)
        if let Err(error) = written {
            event!(
                Level::WARN,
                msg = "an object lane's answer could not be written, and the lane ends",
                ?error
            );
            stopped.set(true);
            let _ = tx.close().await;
            break;
        }
        // their buffers, their draws and their rooms go once they are written
        drop(batch);
    }
}

/// Read one request from a served lane: a piece of a whole chunk's units into a buffer its device
/// writes from, and every other body into the lane's arena (F111)
///
/// The head is read first to tell them apart. A piece's fixed fields and checksums follow it, which
/// say how long its units are, and those are read into an aligned buffer the slice writes as it
/// is, where they were read into a body and copied into one.
///
/// # Arguments
///
/// * `rx` - The lane's read half
/// * `len` - The body's length, from its checked frame header
/// * `arena` - The lane's arena
///
/// # Errors
///
/// A failed read, or a body that cannot be right, which ends the lane.
async fn read_request(
    rx: &mut ReadHalf<TcpStream>,
    len: usize,
    arena: &mut bytes::BytesMut,
) -> Result<(u64, ChunkRequest, Option<glommio::io::DmaBuffer>), ServerError> {
    use crate::shared::protocol::peer::chunk::{
        is_piece, piece_crcs, CHUNK_HEAD_LEN, PIECE_FIXED_LEN,
    };
    // the head, which every request has
    if len < CHUNK_HEAD_LEN {
        let short = ProtocolError::MalformedObject("an object lane request shorter than its head");
        return Err(short.into());
    }
    let mut head = vec![0u8; CHUNK_HEAD_LEN];
    rx.read_exact(&mut head).await?;
    // anything but a piece: the rest behind the head, in the arena
    if !is_piece(&head) || len < CHUNK_HEAD_LEN + PIECE_FIXED_LEN {
        let body = RequestBody::read_rest_from_arena(rx, &head, len, arena).await?.freeze();
        let (id, request) = ChunkRequest::decode_shared(&body)?;
        return Ok((id, request, None));
    }
    // a piece's fixed fields and its checksums, which say how long its units are
    head.resize(CHUNK_HEAD_LEN + PIECE_FIXED_LEN, 0);
    rx.read_exact(&mut head[CHUNK_HEAD_LEN..]).await?;
    let crcs = piece_crcs(&head[CHUNK_HEAD_LEN..]);
    let prefix = CHUNK_HEAD_LEN + PIECE_FIXED_LEN + crcs.saturating_mul(8);
    if prefix > len {
        let malformed = "an object lane piece names more checksums than it carries";
        return Err(ProtocolError::MalformedObject(malformed).into());
    }
    head.resize(prefix, 0);
    rx.read_exact(&mut head[CHUNK_HEAD_LEN + PIECE_FIXED_LEN..]).await?;
    // and its units, straight into a buffer its device writes from
    let units = len - prefix;
    let mut aligned = glommio::allocate_dma_buffer(units.max(1));
    aligned.trim_to_size(units);
    rx.read_exact(aligned.as_bytes_mut()).await?;
    let (id, request) = ChunkRequest::decode_piece_head(&head, units)?;
    Ok((id, request, Some(aligned)))
}

/// The most answers a served lane writes in one call: an answer is a header, a head and a few
/// buffers of bytes, and the fork's vectored write takes at most 1,024 a call (F111)
const BATCH_ANSWERS: usize = 64;

/// The most bytes of answers a served lane gathers for one call, so a call behind a slow peer
/// never holds every answer's room until its last byte is written (F111)
const BATCH_BYTES: usize = 4 << 20;

/// An answer as a batch writes it: its body's head encoded, and a read's bytes where its store
/// left them
struct Batched {
    /// The answer, its buffers, draw and room with it
    owed: Owed,
    /// The answer's whole body for one that is not a read's bytes; a read's head for one that is
    head: Vec<u8>,
}

impl Batched {
    /// An answer encoded for its batch
    ///
    /// # Arguments
    ///
    /// * `owed` - The answer
    fn of(owed: Owed) -> Self {
        let head = match &owed.answer {
            Answered::Reply(reply) => reply.encode_body(owed.id),
            Answered::Bytes { label, .. } => ChunkReply::bytes_head(owed.id, *label),
        };
        Batched { owed, head }
    }

    /// How many bytes its frame's body is
    fn len(&self) -> usize {
        match &self.owed.answer {
            Answered::Reply(_) => self.head.len(),
            Answered::Bytes { span, .. } => self.head.len() + span.len(),
        }
    }
}

/// Write a batch of answers' frames in one vectored call, in order
///
/// # Arguments
///
/// * `tx` - The lane's write half
/// * `negotiated` - What the lane's hello agreed
/// * `batch` - The answers
async fn write_batch(
    tx: &mut WriteHalf<TcpStream>,
    negotiated: &Negotiated,
    batch: &[Batched],
) -> Result<(), ServerError> {
    // each frame's header, which a body past the peer's frame refuses
    let headers = batch
        .iter()
        .map(|answer| {
            codec::header_at(
                negotiated.version,
                MessageType::ChunkResponse,
                answer.len(),
                negotiated.max_frame_bytes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    // every frame's slices, in order: its header, its head, then a read's bytes where they lie
    let mut slices = Vec::with_capacity(batch.len() * 4);
    for (answer, header) in batch.iter().zip(&headers) {
        slices.push(std::io::IoSlice::new(header));
        slices.push(std::io::IoSlice::new(&answer.head));
        if let Answered::Bytes { span, .. } = &answer.owed.answer {
            for part in span.parts() {
                if !part.is_empty() {
                    slices.push(std::io::IoSlice::new(part));
                }
            }
        }
    }
    codec::write_slices(tx, &mut slices).await
}

/// Refuse every reader's read a dialled lane carries whose answer has not come by its bound, for as
/// long as any read it bounds waits (item 316)
///
/// A read whose answer did not come left its coordinator waiting until the run that sent it
/// ended. Refused by name, it goes to another holder, or its stripe decodes without this one.
///
/// # Arguments
///
/// * `link` - The link, which the sweep never keeps
/// * `holder` - The holder it reaches
/// * `inflight` - What its executor's lanes have in flight
async fn sweep(link: Weak<ChunkLink>, holder: Holder, inflight: Arc<InFlight>) {
    loop {
        // the soonest bound of any read still waiting, and the sweep done once none waits
        let soonest = {
            let Some(chunk_link) = link.upgrade() else {
                return;
            };
            let soonest = chunk_link
                .waiters
                .borrow()
                .values()
                .filter_map(|pending| pending.bounded.as_ref().map(|bounded| bounded.until))
                .min();
            match soonest {
                Some(until) => until,
                None => {
                    chunk_link.sweeping.set(false);
                    return;
                }
            }
        };
        glommio::timer::sleep(soonest.saturating_duration_since(Instant::now())).await;
        let Some(chunk_link) = link.upgrade() else {
            return;
        };
        // every read past its bound, taken from the waiters at once
        let now = Instant::now();
        let expired: Vec<(u64, Pending)> = {
            let mut waiters = chunk_link.waiters.borrow_mut();
            let ids: Vec<u64> = waiters
                .iter()
                .filter(|(_, pending)| pending.bounded.as_ref().is_some_and(|bounded| bounded.until <= now))
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| waiters.remove(&id).map(|pending| (id, pending)))
                .collect()
        };
        // each refused by name
        for (id, pending) in expired {
            refuse_unanswered(&chunk_link, holder, id, pending, &inflight);
        }
    }
}

/// Refuse a reader's read whose answer has not come by its bound, and say what its link and the
/// link's socket saw, which tells the end that held it (item 316)
///
/// # Arguments
///
/// * `link` - The link that carried it
/// * `holder` - The holder it was sent to
/// * `id` - Its correlation id
/// * `pending` - The read, taken from the link's waiters
/// * `inflight` - What its executor's lanes have in flight
fn refuse_unanswered(link: &ChunkLink, holder: Holder, id: u64, pending: Pending, inflight: &InFlight) {
    let Pending {
        waiter,
        _held: held,
        bounded,
    } = pending;
    let Some(bounded) = bounded else {
        return;
    };
    let waited = bounded.queued.elapsed();
    // no longer waited on, before its reader hears so
    drop(held);
    inflight.count_refused();
    // remembered, so an answer that comes for it after is said to be late
    {
        let mut refused = link.unanswered.refused.borrow_mut();
        if refused.len() >= REFUSED_KEPT {
            refused.pop_first();
        }
        refused.insert(id, bounded.queued);
    }
    // what the link saw since it was queued, and the kernel's queues each way, once a while
    if let Some(besides) = UNANSWERED.pass(WARN_EVERY) {
        let view = link.link.view();
        let written_since = view.sent_frames.saturating_sub(bounded.written);
        event!(
            Level::WARN,
            msg = "a read sent over an object lane had no answer within its bound and was refused, so its reader asks another holder",
            node = %holder.node,
            slice = %holder.slice,
            id,
            chunk = ?bounded.chunk,
            offset = bounded.offset,
            len = bounded.len,
            waited_ms = waited.as_millis() as u64,
            // the link wrote it once the frames written since it was queued reach its place
            written = written_since >= bounded.place,
            frames_written_since = written_since,
            answers_read_since = link.unanswered.answered.get().saturating_sub(bounded.answered),
            still_waiting = link.waiters.borrow().len(),
            state = %view.state,
            queued_bytes = view.queued_bytes,
            socket = ?link.link.socket(),
            refused_besides = besides,
        );
    }
    let bound = bounded.until.saturating_duration_since(bounded.queued);
    let _ = waiter.try_send(Ok(Answer::Chunk(ChunkReply::Refused {
        code: StoreCode::Unavailable,
        message: format!(
            "no answer to a read came from slice {} on node {} within {bound:?}, the object lane's bound on one",
            holder.slice, holder.node
        ),
    })));
}

/// Hand what a dialled lane learns to the requests waiting on it
///
/// # Arguments
///
/// * `holder` - The holder the lane reaches
/// * `waiters` - The requests waiting, by correlation id
/// * `max_frame` - The peer's frame bound, which the hello agreed
/// * `up` - The link's place in its executor's count of links up, while it is (F108)
/// * `inflight` - What its executor's lanes have in flight
/// * `unanswered` - What the link read, and the reads it refused at their bound (item 316)
/// * `event` - What the link learned
fn on_event(
    holder: Holder,
    waiters: &Rc<RefCell<HashMap<u64, Pending>>>,
    max_frame: &Rc<Cell<u32>>,
    up: &Rc<RefCell<Option<Held>>>,
    inflight: &Arc<InFlight>,
    unanswered: &Rc<Unanswered>,
    event: LinkEvent,
) {
    match event {
        LinkEvent::Up { negotiated, .. } => {
            max_frame.set(negotiated.max_frame_bytes);
            // counted up once, however often it says so, and down when it goes
            let mut held = up.borrow_mut();
            if held.is_none() {
                *held = Some(inflight.hold(Gauge::Link));
            }
        }
        // every request the lane carried is unanswered: its holder did not answer
        LinkEvent::Down { reason, .. } => {
            up.borrow_mut().take();
            // and no answer comes for a read it refused at its bound on another connection
            unanswered.refused.borrow_mut().clear();
            let owed: Vec<Pending> = waiters
                .borrow_mut()
                .drain()
                .map(|(_, pending)| pending)
                .collect();
            for pending in owed {
                let _ = pending.waiter.try_send(Ok(Answer::Chunk(ChunkReply::Refused {
                    code: StoreCode::Unavailable,
                    message: format!("the object lane dropped: {reason}"),
                })));
            }
        }
        LinkEvent::Frame {
            header,
            head,
            payload,
            ..
        } => {
            if header.kind != MessageType::ChunkResponse {
                return;
            }
            // a read's bytes stay in the body the link read them into (F106)
            match ChunkReply::decode_owned(&head, payload) {
                Ok((id, reply)) => {
                    // counted, so a read refused at its bound says whether answers after it came
                    unanswered.answered.set(unanswered.answered.get() + 1);
                    let pending = waiters.borrow_mut().remove(&id);
                    match pending {
                        Some(pending) => {
                            let _ = pending.waiter.try_send(Ok(Answer::Chunk(reply)));
                        }
                        // the answer to a read refused at its bound says it was held, not lost
                        None => {
                            let refused = unanswered.refused.borrow_mut().remove(&id);
                            if let Some(queued) = refused {
                                // at WARN, so a node logging at WARN keeps when the held answer
                                // came, which tells a hold from a loss (item 316)
                                if let Some(besides) = LATE.pass(WARN_EVERY) {
                                    event!(
                                        Level::WARN,
                                        msg = "an answer came for a read an object lane refused at its bound",
                                        node = %holder.node,
                                        slice = %holder.slice,
                                        id,
                                        after_ms = queued.elapsed().as_millis() as u64,
                                        late_besides = besides,
                                    );
                                }
                            }
                        }
                    }
                }
                Err(error) => event!(Level::WARN, msg = "an object lane's answer did not decode", ?error),
            }
        }
    }
}

#[cfg(test)]
mod tests;
