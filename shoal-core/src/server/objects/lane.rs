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
//! sender counts as a holder that did not answer.
//!
//! **A stalled holder holds only its lane**: its link's queue is bounded, and a request that does
//! not fit waits only while the link writes; one that waits [`STALL_AFTER`] with nothing written
//! is refused, and nothing but the callers that sent to that holder waits for it. A served lane
//! reads no more than [`MAX_SERVED`] requests ahead of its answers, so a sender that stops reading
//! its answers stops its own requests and nobody else's.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncReadExt, StreamExt};
use glommio::net::TcpStream;
use glommio::sync::Semaphore;
use tracing::{event, Level};

use super::budget::{Budget, DRAW_WAIT};
use super::executor::{Answer, Holder};
use super::serve::{self, Replier, Streams};
use super::store::{Slice, StoreError};
use crate::server::conf::cluster::{DialOverride, Transport};
use crate::server::map::TabletMap;
use crate::server::peer::codec;
use crate::server::peer::handoff::HandedLane;
use crate::server::peer::handshake::{Local, Negotiated, PeerAddr};
use crate::server::peer::link::{Frame, FrameKey, Link, LinkEvent};
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
        }
    }
}

/// One dialled lane to one remote slice, and the requests it carries unanswered
struct ChunkLink {
    /// The link
    link: Link,
    /// Each request's waiter, by correlation id
    waiters: Rc<RefCell<HashMap<u64, Waiter>>>,
    /// The next correlation id
    next: Cell<u64>,
    /// The peer's frame bound, once a hello agreed one
    max_frame: Rc<Cell<u32>>,
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
    #[must_use]
    pub fn start(plan: Option<LanePlan>, streams: Streams, budget: Arc<Budget>) -> Rc<Self> {
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
        let frame = Frame::new(
            MessageType::ChunkRequest,
            vec![Bytes::from(request.encode_body(id))],
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
                // registered before it is queued, so its answer always finds its waiter
                link.waiters.borrow_mut().insert(id, waiter.clone());
                match link.link.enqueue(frame) {
                    Ok(()) => {
                        self.count(|figures| {
                            figures.sent += 1;
                            figures.waited += u64::from(waited);
                        });
                        return Ok(());
                    }
                    Err((_, bound)) => {
                        link.waiters.borrow_mut().remove(&id);
                        self.count(|figures| figures.shed += 1);
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
        let waiters: Rc<RefCell<HashMap<u64, Waiter>>> = Rc::new(RefCell::new(HashMap::new()));
        let max_frame = Rc::new(Cell::new(local.borrow().max_frame_bytes));
        let link = Link::spawn_for_slice(
            holder.slice,
            entry,
            local,
            &plan.transport,
            plan.tls.clone(),
            {
                let waiters = waiters.clone();
                let max_frame = max_frame.clone();
                move |event| on_event(&waiters, &max_frame, event)
            },
        );
        let link = Rc::new(ChunkLink {
            link,
            waiters,
            next: Cell::new(1),
            max_frame,
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
        // the answers go out through one writer, in the order they are ready
        let (answers, ready) = futures::channel::mpsc::unbounded::<(u64, ChunkReply)>();
        let writer = glommio::spawn_local(write_answers(tx, ready, negotiated)).detach();
        // how far reading may run ahead of the answers
        let room = Rc::new(Semaphore::new(MAX_SERVED));
        let own_frame = self
            .local
            .as_ref()
            .map_or(crate::shared::protocol::DEFAULT_MAX_FRAME_BYTES, |local| {
                local.borrow().max_frame_bytes
            });
        let outcome = loop {
            // a test can stall this node's lanes, as a holder that stops reading would
            while super::faults::lanes_stalled() {
                glommio::timer::sleep(std::time::Duration::from_millis(20)).await;
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
            let body = match codec::read_vec(&mut rx, header.body_len()).await {
                Ok(body) => body,
                Err(error) => break Err(error),
            };
            let (id, request) = match ChunkRequest::decode(&body) {
                Ok(decoded) => decoded,
                Err(error) => break Err(error.into()),
            };
            drop(body);
            // and a request read while the stall began is held unserved, as the next one would be
            while super::faults::lanes_stalled() {
                glommio::timer::sleep(std::time::Duration::from_millis(20)).await;
            }
            self.count(|figures| figures.served += 1);
            // every request is answered once, under its id, and frees its room as it is
            let replier: Replier = {
                let answers = answers.clone();
                Box::new(move |reply| {
                    let _ = answers.unbounded_send((id, reply));
                    drop(permit);
                })
            };
            self.dispatch(&slice, origin, request, replier);
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
    /// * `replier` - How it is answered
    fn dispatch(&self, slice: &Rc<Slice>, origin: u64, request: ChunkRequest, replier: Replier) {
        match request {
            // a piece goes to its stream's task now, before the next is read, so pieces keep order
            ChunkRequest::Write(piece) => self.streams.take(slice.clone(), origin, true, piece, replier),
            ChunkRequest::Stage(piece) => self.streams.take(slice.clone(), origin, false, piece, replier),
            // anything else runs on its own task, its buffers drawn from the budget first
            other => {
                let draw = draws(&other);
                let budget = self.budget.clone();
                let slice = slice.clone();
                glommio::spawn_local(async move {
                    let permit = if draw > 0 {
                        match budget.draw(draw, DRAW_WAIT).await {
                            Ok(permit) => Some(permit),
                            Err(error) => {
                                replier(serve::refused(&error));
                                return;
                            }
                        }
                    } else {
                        None
                    };
                    replier(serve::serve(&slice, other).await);
                    drop(permit);
                })
                .detach();
            }
        }
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
        ChunkRequest::Read { len, .. } => *len,
        _ => 0,
    }
}

/// Write a served lane's answers as they are ready, until every one owed is written
///
/// # Arguments
///
/// * `tx` - The lane's write half
/// * `ready` - The answers, each under its request's id
/// * `negotiated` - What the lane's hello agreed
async fn write_answers(
    mut tx: WriteHalf<TcpStream>,
    mut ready: futures::channel::mpsc::UnboundedReceiver<(u64, ChunkReply)>,
    negotiated: Negotiated,
) {
    while let Some((id, reply)) = ready.next().await {
        let body = reply.encode_body(id);
        // an answer that cannot be framed or written ends the lane, and the sender dials again
        let Ok(header) = codec::header_at(
            negotiated.version,
            MessageType::ChunkResponse,
            body.len(),
            negotiated.max_frame_bytes,
        ) else {
            break;
        };
        if codec::write_frame(&mut tx, &header, &[&body]).await.is_err() {
            break;
        }
    }
}

/// Hand what a dialled lane learns to the requests waiting on it
///
/// # Arguments
///
/// * `waiters` - The requests waiting, by correlation id
/// * `max_frame` - The peer's frame bound, which the hello agreed
/// * `event` - What the link learned
fn on_event(waiters: &Rc<RefCell<HashMap<u64, Waiter>>>, max_frame: &Rc<Cell<u32>>, event: LinkEvent) {
    match event {
        LinkEvent::Up { negotiated, .. } => max_frame.set(negotiated.max_frame_bytes),
        // every request the lane carried is unanswered: its holder did not answer
        LinkEvent::Down { reason, .. } => {
            let owed: Vec<Waiter> = waiters.borrow_mut().drain().map(|(_, waiter)| waiter).collect();
            for waiter in owed {
                let _ = waiter.try_send(Ok(Answer::Chunk(ChunkReply::Refused {
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
            match ChunkReply::decode_parts(&head, &payload) {
                Ok((id, reply)) => {
                    let waiter = waiters.borrow_mut().remove(&id);
                    if let Some(waiter) = waiter {
                        let _ = waiter.try_send(Ok(Answer::Chunk(reply)));
                    }
                }
                Err(error) => event!(Level::WARN, msg = "an object lane's answer did not decode", ?error),
            }
        }
    }
}

#[cfg(test)]
mod tests;
