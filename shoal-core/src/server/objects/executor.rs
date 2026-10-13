//! A node's object executors and the handle that reaches the slices they own
//!
//! Object work runs on executors of its own, one a cpu named in `resources.object_cpus`, each
//! owning some of the node's device slices and doing all of their I/O
//! ([S13](../../../../docs/src/object-storage/isolation.md#who-owns-a-slice)). A caller - a test,
//! the lab's harness, and from M15 a shard staging a client's bytes - reaches a slice through
//! [`ObjectStore`], which hands each request to the executor that owns it, as a peer's frame naming
//! a slot is handed to the executor hosting it.
//!
//! # Invariants
//!
//! **A slice is served by one executor**: it is opened on its owner and every request for it goes
//! there, so its reads and applies take turns on one executor's lock and nothing else touches its
//! files. Which executor owns which slice is the node's own business, held here and never sent to
//! a peer.
//!
//! **Every queue has a latency goal**, so a loop's `yield_if_needed` is taken: foreground reads and
//! stages first, applies below them, and the pool's refill and every later background last
//! ([X9](../../../../docs/src/object-storage/table-latency.md#recommendation),
//! [S13](../../../../docs/src/object-storage/isolation.md#io-on-a-slice)).
//!
//! **Every buffer is drawn from the executor's budget**, and a draw that does not fit is shed by
//! name, never grown past it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use glommio::{Latency, LocalExecutorBuilder, Placement, PoolPlacement, Shares};
use tracing::{event, Level};

use super::budget::{Budget, Permit, DRAW_WAIT};
use super::cpus::ObjectCpu;
use super::lane::{self, LaneFigures, LanePlan, Lanes};
use super::serve::{self, Streams};
use super::store::{ChunkId, ReadAnswer, Slice, SliceOptions, StageUnits, StageWhole, StoreError};
use crate::server::devices::{DeviceState, SliceState};
use crate::server::errors::ShoalError;
use crate::server::map::TabletMap;
use crate::server::peer::handoff::HandedLane;
use crate::server::ServerError;
use crate::shared::devices::SliceId;
use crate::shared::identity::NodeId;
use crate::shared::objects::Label;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, StoreCode};

/// The latency goal of every object queue: X9's, which held a table beside 64 KiB steps
pub const QUEUE_GOAL: Duration = Duration::from_micros(100);

/// A slice an executor serves: its device, its own state, and how it is kept
pub type OwnedSlice = (Arc<DeviceState>, Arc<SliceState>, SliceOptions);

/// What a node's object executors are to be
#[derive(Debug, Clone)]
pub struct ObjectPlan {
    /// Where each executor runs
    pub cpus: Vec<ObjectCpu>,
    /// The memory object buffers may hold, across every executor
    pub budget: u64,
    /// The registered buffer memory each executor's ring is given
    pub io_memory: usize,
    /// Every slice to serve, with its device and how it is kept
    pub slices: Vec<OwnedSlice>,
    /// The node the executors run on, which a holder on it is local to
    pub node: NodeId,
    /// How the executors dial an object lane to another node's slices, on a cluster node
    pub lanes: Option<LanePlan>,
}

/// A stripe chunk's holder: a slice, and the node it is on
///
/// The one thing a coordinator names a holder by. A holder on this node is reached through its
/// owning executor's mailbox, one on another node over the object lane
/// ([F86](../../../../docs/src/features/replicated-pools.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Holder {
    /// The node the slice is on
    pub node: NodeId,
    /// The slice
    pub slice: SliceId,
}

/// Work run on an object executor with the store in hand: a put's pipeline, a lane's link
pub type Job = Box<dyn FnOnce(ObjectStore) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

/// One operation on a slice
#[derive(Debug)]
enum Op {
    /// Stage a whole chunk
    StageWhole(StageWhole),
    /// Stage part of a chunk
    StageUnits(StageUnits),
    /// Apply a committed write
    Apply(ChunkId, Label),
    /// Discard an excluded one
    Discard(ChunkId, Label),
    /// Read a range at a label
    Read {
        /// The chunk
        id: ChunkId,
        /// Where the range starts
        offset: u64,
        /// How long it is
        len: u64,
        /// The label wanted
        label: Label,
    },
    /// The label a chunk carries on the device
    ChunkLabel(ChunkId),
    /// The label and length a chunk's header carries on the device
    ChunkHeader(ChunkId),
    /// The records no row has decided, staged at least this long ago or found by a start
    Undecided(Duration),
    /// A chunk request, as the object lane carries it, from this node or another
    Chunk {
        /// The request
        request: ChunkRequest,
        /// Who sent it: zero for this node, else the connection it arrived on
        origin: u64,
    },
}

/// What an operation answers
#[derive(Debug)]
pub enum Answer {
    /// Done
    Done,
    /// A read's answer
    Read(ReadAnswer),
    /// A chunk's label
    Label(Option<Label>),
    /// A chunk request's answer
    Chunk(ChunkReply),
    /// A chunk header's label and length
    Header(Option<(Label, u64)>),
    /// The records no row has decided
    Undecided(Vec<(ChunkId, Label)>),
}

/// Wait for a chunk request's answer on the channel it was sent with
///
/// # Arguments
///
/// * `answer` - Where it arrives
pub async fn answer_of(answer: kanal::AsyncReceiver<Result<Answer, StoreError>>) -> ChunkReply {
    // an executor that stopped before answering is unavailable, which a caller may try again
    match answer.recv().await {
        Ok(Ok(Answer::Chunk(reply))) => reply,
        Ok(Ok(other)) => ChunkReply::Refused {
            code: StoreCode::Invalid,
            message: format!("a chunk request was answered {other:?}"),
        },
        Ok(Err(error)) => serve::refused(&error),
        Err(_) => ChunkReply::Refused {
            code: StoreCode::Unavailable,
            message: "the object executor stopped before it answered".to_string(),
        },
    }
}

/// A request for the executor that owns a slice
struct Request {
    /// The slice
    slice: SliceId,
    /// What to do
    op: Op,
    /// The bytes its buffers hold, drawn on the executor before it runs and held until it is
    /// answered: the draw's wait sleeps on the executor's timer, so the handle works on any runtime
    draw: u64,
    /// Where to answer
    reply: kanal::AsyncSender<Result<Answer, StoreError>>,
}

/// What an executor is told
enum Message {
    /// A request
    Request(Request),
    /// Work to run on it, with the store in hand
    Run(Job),
    /// A chunk request for a holder on another node, sent over this executor's lane to it
    Remote {
        /// The holder
        holder: Holder,
        /// The request
        request: ChunkRequest,
        /// Where its answer goes
        reply: kanal::AsyncSender<Result<Answer, StoreError>>,
    },
    /// An object lane a shard accepted for a slice this executor owns
    Adopt(HandedLane),
    /// The map the node installed, which the lanes dial members by
    Map(Arc<TabletMap>),
    /// What this executor's lanes have done
    Figures(kanal::Sender<LaneFigures>),
    /// Stop, once every slice is closed
    Stop,
}

/// Which executor owns a slice, and the cpu it runs on
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Owner {
    /// The executor's index
    pub executor: usize,
    /// Its cpu
    pub cpu: usize,
}

/// What every executor shares with the handle
struct Inner {
    /// The node the executors run on
    node: NodeId,
    /// Each executor's mailbox
    senders: Vec<kanal::Sender<Message>>,
    /// The executor the next job runs on, dealt round
    next_job: AtomicUsize,
    /// Which executor owns each slice
    owners: BTreeMap<SliceId, Owner>,
    /// Each executor's budget
    budgets: Vec<Arc<Budget>>,
    /// Each slice's figures, by id
    slices: BTreeMap<SliceId, Arc<SliceState>>,
    /// The executors' threads, joined at the stop
    threads: Mutex<Vec<glommio::ExecutorJoinHandle<()>>>,
}

/// The handle every caller reaches a node's slices through
#[derive(Clone)]
pub struct ObjectStore {
    /// What the executors share
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ObjectStore {
    /// Name the store by what it serves
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStore")
            .field("executors", &self.inner.senders.len())
            .field("owners", &self.inner.owners)
            .finish()
    }
}

/// Run one operation on a slice its executor owns
///
/// # Arguments
///
/// * `slice` - The slice
/// * `op` - The operation
async fn run(slice: &Rc<Slice>, op: Op) -> Result<Answer, StoreError> {
    match op {
        Op::StageWhole(stage) => slice.stage_whole(stage).await.map(|()| Answer::Done),
        Op::StageUnits(stage) => slice.stage_units(stage).await.map(|()| Answer::Done),
        Op::Apply(id, label) => slice.apply(id, label).await.map(|()| Answer::Done),
        Op::Discard(id, label) => slice.discard(id, label).await.map(|()| Answer::Done),
        Op::Read { id, offset, len, label } => slice.read(id, offset, len, label).await.map(Answer::Read),
        Op::ChunkLabel(id) => slice.chunk_label(&id).await.map(Answer::Label),
        Op::ChunkHeader(id) => slice.chunk_header(&id).await.map(Answer::Header),
        Op::Undecided(older) => Ok(Answer::Undecided(slice.undecided(older))),
        Op::Chunk { request, .. } => Ok(Answer::Chunk(serve::serve(slice, request).await)),
    }
}

/// Hand a piece of a whole chunk to its stream on the slice it names, or refuse it
///
/// # Arguments
///
/// * `streams` - The executor's streams
/// * `slice` - The slice, if this executor owns it
/// * `id` - The slice's id
/// * `origin` - Who sent it
/// * `place` - Whether the chunk goes straight into place
/// * `piece` - The piece
/// * `reply` - Where its sender is answered
fn take_piece(
    streams: &Streams,
    slice: Option<Rc<Slice>>,
    id: SliceId,
    origin: u64,
    place: bool,
    piece: crate::shared::protocol::peer::chunk::Piece,
    reply: kanal::AsyncSender<Result<Answer, StoreError>>,
) {
    // the answer goes back on the request's own channel, which has room for the one answer
    let replier: serve::Replier = Box::new(move |answer| {
        let _ = reply.try_send(Ok(Answer::Chunk(answer)));
    });
    match slice {
        Some(slice) => streams.take(slice, origin, place, piece, replier),
        None => replier(serve::refused(&StoreError::UnknownSlice(id))),
    }
}

/// One executor's life: open its slices, serve them, close them
///
/// # Arguments
///
/// * `index` - The executor's index
/// * `mine` - The slices it owns
/// * `budget` - Its budget
/// * `mailbox` - Where its requests arrive
/// * `opened` - Where it says its slices are open
/// * `store` - The handle its jobs run with, set once every executor started
/// * `lanes` - How it dials an object lane, on a cluster node
#[allow(clippy::too_many_arguments)]
async fn serve(
    index: usize,
    mine: Vec<(Arc<DeviceState>, Arc<SliceState>, SliceOptions)>,
    budget: Arc<Budget>,
    mailbox: kanal::Receiver<Message>,
    opened: std::sync::mpsc::Sender<()>,
    store: Arc<OnceLock<ObjectStore>>,
    lanes: Option<LanePlan>,
) {
    let executor = glommio::executor();
    // three queues, each with a goal, so every step's yield is taken
    let queue = |shares: usize, name: &str| {
        executor.create_task_queue(
            Shares::Static(shares),
            Latency::Matters(QUEUE_GOAL),
            &format!("{name}:{index}"),
        )
    };
    let foreground = queue(1000, "ObjectForeground");
    let applies = queue(500, "ObjectApply");
    let background = queue(100, "ObjectBackground");
    // every slice it owns, a failed device's left closed and refusing
    let mut slices: HashMap<SliceId, Rc<Slice>> = HashMap::new();
    for (device, state, mut options) in mine {
        options.budget = Some(budget.clone());
        let id = state.id;
        match Slice::open(device.clone(), state, options, Some(background)).await {
            Ok(slice) => {
                slices.insert(id, slice);
            }
            Err(error) => event!(
                Level::ERROR,
                device = %device.path().display(),
                slice = %id,
                msg = "a slice did not open; its device is failed and the node goes on",
                error = %error,
            ),
        }
    }
    let _ = opened.send(());
    // the streams of whole chunks this executor is writing, a task each, from this node or a lane
    let streams = Streams::default();
    // the object lanes it dials and the ones it is handed to serve (F86)
    let lanes = Lanes::start(lanes, streams.clone(), budget.clone());
    // every request on the queue its kind belongs to, answered when it is done
    let mailbox = mailbox.to_async();
    while let Ok(message) = mailbox.recv().await {
        let request = match message {
            Message::Request(request) => request,
            // a job runs on the foreground queue with the store in hand
            Message::Run(job) => {
                if let Some(store) = store.get().cloned() {
                    if let Ok(spawned) = glommio::spawn_local_into(job(store), foreground) {
                        spawned.detach();
                    }
                }
                continue;
            }
            // a remote holder's request goes out on this executor's lane to it
            Message::Remote { holder, request, reply } => {
                lanes.send(holder, &request, reply);
                continue;
            }
            // a lane for a slice this executor owns is served here, on the slice's own executor
            Message::Adopt(handed) => {
                let slice = handed.slice.and_then(|id| slices.get(&id).cloned());
                lanes.adopt(handed, slice);
                continue;
            }
            Message::Map(map) => {
                lanes.install(map);
                continue;
            }
            Message::Figures(reply) => {
                let _ = reply.send(lanes.figures());
                continue;
            }
            Message::Stop => break,
        };
        // a piece of a whole chunk goes to its stream's task now, before the next request is
        // read, so the pieces of one stream are written in the order they were sent
        let request = match request {
            Request {
                slice: id,
                op:
                    Op::Chunk {
                        request: ChunkRequest::Write(piece),
                        origin,
                    },
                reply,
                ..
            } => {
                take_piece(&streams, slices.get(&id).cloned(), id, origin, true, piece, reply);
                continue;
            }
            Request {
                slice: id,
                op:
                    Op::Chunk {
                        request: ChunkRequest::Stage(piece),
                        origin,
                    },
                reply,
                ..
            } => {
                take_piece(&streams, slices.get(&id).cloned(), id, origin, false, piece, reply);
                continue;
            }
            request => request,
        };
        let slice = slices.get(&request.slice).cloned();
        let lane = match request.op {
            Op::Apply(..) | Op::Discard(..) => applies,
            _ => foreground,
        };
        let budget = budget.clone();
        let task = async move {
            let Request {
                slice: id,
                op,
                draw,
                reply,
            } = request;
            // the operation's buffers from the budget, or shed by name
            let permit: Option<Permit> = if draw > 0 {
                match budget.draw(draw, DRAW_WAIT).await {
                    Ok(permit) => Some(permit),
                    Err(error) => {
                        let _ = reply.send(Err(error)).await;
                        return;
                    }
                }
            } else {
                None
            };
            let answer = match slice {
                Some(slice) => {
                    // the cpu it ran on, which is its owner's by construction
                    // SAFETY: sched_getcpu takes nothing and reads the calling thread's cpu
                    if let Ok(cpu) = usize::try_from(unsafe { libc::sched_getcpu() }) {
                        slice
                            .state()
                            .served_on
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(cpu);
                    }
                    run(&slice, op).await
                }
                None => Err(StoreError::UnknownSlice(id)),
            };
            let _ = reply.send(answer).await;
            drop(permit);
        };
        if let Ok(spawned) = glommio::spawn_local_into(task, lane) {
            spawned.detach();
        }
    }
    // every link closes before the slices do
    lanes.stop();
    for slice in slices.values() {
        slice.close().await;
    }
}

impl ObjectStore {
    /// Start a node's object executors, each with the slices it owns opened
    ///
    /// Slices are dealt round-robin over the executors in device order, so a device's several
    /// slices land on different executors and drive it from several cores.
    ///
    /// # Arguments
    ///
    /// * `plan` - What the executors are to be
    ///
    /// # Errors
    ///
    /// An executor that cannot be started.
    pub fn start(plan: ObjectPlan) -> Result<ObjectStore, ServerError> {
        let executors = plan.cpus.len().max(1);
        let each = plan.budget / executors as u64;
        let mut senders = Vec::with_capacity(executors);
        let mut budgets = Vec::with_capacity(executors);
        let mut owners = BTreeMap::new();
        let mut slices = BTreeMap::new();
        let mut owned: Vec<Vec<OwnedSlice>> = vec![Vec::new(); executors];
        for (index, (device, state, options)) in plan.slices.into_iter().enumerate() {
            let executor = index % executors;
            owners.insert(
                state.id,
                Owner {
                    executor,
                    cpu: plan.cpus.get(executor).map_or(0, |cpu| cpu.cpu),
                },
            );
            slices.insert(state.id, state.clone());
            owned[executor].push((device, state, options));
        }
        let mut threads = Vec::with_capacity(executors);
        let (opened, opened_rx) = std::sync::mpsc::channel();
        // the handle every executor runs its jobs with, set once they have all started
        let handle_cell: Arc<OnceLock<ObjectStore>> = Arc::new(OnceLock::new());
        for (index, (place, mine)) in plan.cpus.iter().zip(owned).enumerate() {
            let (sender, receiver) = kanal::bounded(4096);
            let budget = Budget::new(each);
            senders.push(sender);
            budgets.push(budget.clone());
            let opened = opened.clone();
            let cell = handle_cell.clone();
            let lanes = plan.lanes.clone();
            // its blocking thread, where every rename and unlink goes, on the cpu chosen for it
            let blocking = glommio::CpuSet::online()?.filter(|location| location.cpu == place.blocking);
            let handle = LocalExecutorBuilder::new(Placement::Fixed(place.cpu))
                .name(&format!("shoal-objects-{index}"))
                .io_memory(plan.io_memory)
                .ring_depth(256)
                .blocking_thread_pool_placement(PoolPlacement::Custom(vec![blocking]))
                .spawn(move || serve(index, mine, budget, receiver, opened, cell, lanes))?;
            threads.push(handle);
        }
        drop(opened);
        // every executor has its slices open before a request can reach one; one that never says
        // so died first, most often pinning itself to a cpu a cgroup forbids, and is named
        let mut answered = 0;
        while answered < threads.len() && opened_rx.recv().is_ok() {
            answered += 1;
        }
        if answered < threads.len() {
            let cpus: Vec<usize> = plan.cpus.iter().map(|place| place.cpu).collect();
            let mut why = Vec::new();
            for handle in threads {
                match handle.join() {
                    Ok(()) => {}
                    Err(error) => why.push(format!("{error}")),
                }
            }
            return Err(ServerError::Shoal(ShoalError::ObjectCpuRefused {
                cpu: cpus.first().copied().unwrap_or(0),
                why: format!(
                    "{} of the object executors on cpus {cpus:?} did not start: {}",
                    cpus.len() - answered,
                    why.join("; ")
                ),
            }));
        }
        let store = ObjectStore {
            inner: Arc::new(Inner {
                node: plan.node,
                senders,
                next_job: AtomicUsize::new(0),
                owners,
                budgets,
                slices,
                threads: Mutex::new(threads),
            }),
        };
        let _ = handle_cell.set(store.clone());
        Ok(store)
    }

    /// The node the store's executors run on
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.inner.node
    }

    /// Whether a holder is a slice of this node
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    #[must_use]
    pub fn is_local(&self, holder: &Holder) -> bool {
        holder.node == self.inner.node
    }

    /// Run work on one of the executors, dealt round, with the store in hand
    ///
    /// # Arguments
    ///
    /// * `job` - The work
    ///
    /// # Errors
    ///
    /// Every executor has stopped.
    pub fn run(&self, job: Job) -> Result<(), StoreError> {
        let executor = self.inner.next_job.fetch_add(1, Ordering::Relaxed) % self.inner.senders.len().max(1);
        self.inner
            .senders
            .get(executor)
            .ok_or_else(|| StoreError::Invalid("this node runs no object executor".to_string()))?
            .send(Message::Run(job))
            .map_err(|_| StoreError::Invalid("the object executors have stopped".to_string()))
    }

    /// Send a chunk request to a holder, and hand back where its answer will arrive
    ///
    /// The request is queued in order behind every earlier one to the same holder from this
    /// caller, so the pieces of a whole chunk arrive in the order they were sent. Every request is
    /// answered once; a piece before its chunk's last may have its answer let go unread, since a
    /// failure of it ends its stream, which the last piece's answer then says.
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - The request
    ///
    /// # Errors
    ///
    /// The holder is on another node and no object lane reaches it, or this node's executors have
    /// stopped: either as the refusal the holder would have sent.
    pub async fn send(&self, holder: Holder, request: ChunkRequest) -> Result<kanal::AsyncReceiver<Result<Answer, StoreError>>, ChunkReply> {
        // a holder on another node is reached over the object lane
        if !self.is_local(&holder) {
            return self.send_remote(holder, request).await;
        }
        // the bytes a request carries are drawn on the owner before it runs
        let draw = lane::draws(&request);
        let owner = self
            .inner
            .owners
            .get(&holder.slice)
            .ok_or_else(|| serve::refused(&StoreError::UnknownSlice(holder.slice)))?;
        let (reply, answer) = kanal::bounded_async(1);
        self.inner.senders[owner.executor]
            .as_async()
            .send(Message::Request(Request {
                slice: holder.slice,
                op: Op::Chunk { request, origin: 0 },
                draw,
                reply,
            }))
            .await
            .map_err(|_| ChunkReply::Refused {
                code: StoreCode::Unavailable,
                message: "the object executor has stopped".to_string(),
            })?;
        Ok(answer)
    }

    /// Send a chunk request to a holder on another node, over the object lane to its slice
    ///
    /// From an object executor the request goes out on that executor's own lane; from anywhere
    /// else it is handed to the executor the holder is dealt to, so every request one caller sends
    /// one holder goes out on one lane, in the order it was sent.
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - The request
    ///
    /// # Errors
    ///
    /// This node's executors have stopped, as the refusal the holder would have sent.
    async fn send_remote(
        &self,
        holder: Holder,
        request: ChunkRequest,
    ) -> Result<kanal::AsyncReceiver<Result<Answer, StoreError>>, ChunkReply> {
        let (reply, answer) = kanal::bounded_async(1);
        // an object executor sends on its own lanes, with no hop between threads
        if let Some(lanes) = lane::current() {
            lanes.send(holder, &request, reply);
            return Ok(answer);
        }
        // anything else hands it to the executor the holder is dealt to
        let (high, low) = holder.slice.0.as_u64_pair();
        let dealt = crate::shared::rendezvous::mix64(high ^ low.rotate_left(17));
        let executor = (dealt % self.inner.senders.len().max(1) as u64) as usize;
        let Some(sender) = self.inner.senders.get(executor) else {
            return Err(ChunkReply::Refused {
                code: StoreCode::Unavailable,
                message: "this node runs no object executor to reach another node's slices".to_string(),
            });
        };
        sender
            .as_async()
            .send(Message::Remote { holder, request, reply })
            .await
            .map_err(|_| ChunkReply::Refused {
                code: StoreCode::Unavailable,
                message: "the object executor has stopped".to_string(),
            })?;
        Ok(answer)
    }

    /// Hand an object lane a shard accepted to the executor that owns the slice its hello named
    ///
    /// Returns whether it was handed; a lane that was not is dropped here, which closes it, and
    /// its peer dials again.
    ///
    /// # Arguments
    ///
    /// * `lane` - The lane, given up by the shard that accepted it
    #[must_use]
    pub fn adopt(&self, lane: HandedLane) -> bool {
        // a slice this node does not serve has no executor to take it
        let Some(owner) = lane.slice.and_then(|slice| self.inner.owners.get(&slice)) else {
            return false;
        };
        self.inner.senders[owner.executor]
            .send(Message::Adopt(lane))
            .is_ok()
    }

    /// Hand every executor the map the node installed, which its lanes dial members by
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    pub fn install_map(&self, map: &Arc<TabletMap>) {
        for sender in &self.inner.senders {
            let _ = sender.try_send(Message::Map(map.clone()));
        }
    }

    /// What every executor's lanes have done, added together
    pub async fn lane_figures(&self) -> LaneFigures {
        let mut total = LaneFigures::default();
        for sender in &self.inner.senders {
            let (reply, figures) = kanal::bounded(1);
            if sender.as_async().send(Message::Figures(reply)).await.is_ok() {
                if let Ok(figures) = figures.to_async().recv().await {
                    total = total.plus(figures);
                }
            }
        }
        total
    }

    /// Ask a holder a chunk request, and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - The request
    pub async fn ask_holder(&self, holder: Holder, request: ChunkRequest) -> ChunkReply {
        match self.send(holder, request).await {
            Ok(answer) => answer_of(answer).await,
            Err(refused) => refused,
        }
    }

    /// Hand a request to the executor that owns its slice, and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `op` - The operation
    /// * `draw` - The bytes its buffers hold, drawn here from the owner's budget
    async fn ask(&self, slice: SliceId, op: Op, draw: u64) -> Result<Answer, StoreError> {
        let owner = self
            .inner
            .owners
            .get(&slice)
            .ok_or(StoreError::UnknownSlice(slice))?;
        let (reply, answer) = kanal::bounded_async(1);
        self.inner.senders[owner.executor]
            .as_async()
            .send(Message::Request(Request {
                slice,
                op,
                draw,
                reply,
            }))
            .await
            .map_err(|_| StoreError::Invalid("the object executor has stopped".to_string()))?;
        answer
            .recv()
            .await
            .map_err(|_| StoreError::Invalid("the object executor stopped before it answered".to_string()))?
    }

    /// Stage a whole chunk on a slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `stage` - The chunk and its bytes
    ///
    /// # Errors
    ///
    /// The slice's refusal, a budget with no room, or a slice this node does not serve.
    pub async fn stage_whole(&self, slice: SliceId, stage: StageWhole) -> Result<(), StoreError> {
        let bytes = stage.bytes.len() as u64;
        self.ask(slice, Op::StageWhole(stage), bytes).await.map(|_| ())
    }

    /// Stage part of a chunk on a slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `stage` - The units and their labels
    ///
    /// # Errors
    ///
    /// The slice's refusal, a budget with no room, or a slice this node does not serve.
    pub async fn stage_units(&self, slice: SliceId, stage: StageUnits) -> Result<(), StoreError> {
        let bytes = stage.units.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        self.ask(slice, Op::StageUnits(stage), bytes).await.map(|_| ())
    }

    /// Apply a committed write on a slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `id` - The chunk
    /// * `label` - The label the write makes
    ///
    /// # Errors
    ///
    /// The slice's refusal, or a slice this node does not serve.
    pub async fn apply(&self, slice: SliceId, id: ChunkId, label: Label) -> Result<(), StoreError> {
        self.ask(slice, Op::Apply(id, label), 0).await.map(|_| ())
    }

    /// Discard an excluded write on a slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `id` - The chunk
    /// * `label` - The label the write makes
    ///
    /// # Errors
    ///
    /// The slice's refusal, or a slice this node does not serve.
    pub async fn discard(&self, slice: SliceId, id: ChunkId, label: Label) -> Result<(), StoreError> {
        self.ask(slice, Op::Discard(id, label), 0).await.map(|_| ())
    }

    /// Read a range of a chunk at a label on a slice
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `id` - The chunk
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    /// * `label` - The label wanted
    ///
    /// # Errors
    ///
    /// The slice's refusal, a budget with no room, or a slice this node does not serve.
    pub async fn read(
        &self,
        slice: SliceId,
        id: ChunkId,
        offset: u64,
        len: u64,
        label: Label,
    ) -> Result<ReadAnswer, StoreError> {
        match self.ask(slice, Op::Read { id, offset, len, label }, 0).await? {
            Answer::Read(answer) => Ok(answer),
            other => Err(StoreError::Invalid(format!("a read was answered {other:?}"))),
        }
    }

    /// The label a chunk carries on a slice's device
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `id` - The chunk
    ///
    /// # Errors
    ///
    /// A failed device, or a slice this node does not serve.
    pub async fn chunk_label(&self, slice: SliceId, id: ChunkId) -> Result<Option<Label>, StoreError> {
        match self.ask(slice, Op::ChunkLabel(id), 0).await? {
            Answer::Label(label) => Ok(label),
            other => Err(StoreError::Invalid(format!("a label was asked and answered {other:?}"))),
        }
    }

    /// The label and length a chunk's header carries on a slice's device
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `id` - The chunk
    ///
    /// # Errors
    ///
    /// A failed device, or a slice this node does not serve.
    pub async fn chunk_header(&self, slice: SliceId, id: ChunkId) -> Result<Option<(Label, u64)>, StoreError> {
        match self.ask(slice, Op::ChunkHeader(id), 0).await? {
            Answer::Header(header) => Ok(header),
            other => Err(StoreError::Invalid(format!("a header was asked and answered {other:?}"))),
        }
    }

    /// Every record on this node's slices no row has decided yet: staged at least `older` ago, or
    /// found by a start
    ///
    /// # Arguments
    ///
    /// * `older` - How long a stage is left to its stager
    pub async fn undecided(&self, older: Duration) -> Vec<(SliceId, ChunkId, Label)> {
        let mut found = Vec::new();
        // every slice this node serves, asked on its owner
        for slice in self.inner.owners.keys() {
            if let Ok(Answer::Undecided(records)) = self.ask(*slice, Op::Undecided(older), 0).await {
                found.extend(records.into_iter().map(|(chunk, label)| (*slice, chunk, label)));
            }
        }
        found
    }

    /// Which executor owns each slice, and its cpu: the node's own business, never a peer's
    #[must_use]
    pub fn owners(&self) -> BTreeMap<SliceId, Owner> {
        self.inner.owners.clone()
    }

    /// Every cpu an operation on each slice ran on
    #[must_use]
    pub fn served_on(&self) -> BTreeMap<SliceId, BTreeSet<usize>> {
        self.inner
            .slices
            .iter()
            .map(|(id, state)| {
                (
                    *id,
                    state
                        .served_on
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .clone(),
                )
            })
            .collect()
    }

    /// The bytes every slice's reads have answered, across the node's slices
    #[must_use]
    pub fn read_bytes(&self) -> u64 {
        self.inner
            .slices
            .values()
            .map(|state| state.read_bytes.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }

    /// Each executor's budget: drawn, its whole, and the draws it shed
    #[must_use]
    pub fn budgets(&self) -> Vec<(u64, u64, u64)> {
        self.inner
            .budgets
            .iter()
            .map(|budget| (budget.used(), budget.limit(), budget.shed()))
            .collect()
    }

    /// The slices this node serves, by id, and where each is
    #[must_use]
    pub fn slices(&self) -> BTreeMap<SliceId, PathBuf> {
        self.inner
            .slices
            .iter()
            .map(|(id, state)| (*id, state.path.clone()))
            .collect()
    }

    /// Stop every executor once its slices are closed
    pub fn stop(&self) {
        for sender in &self.inner.senders {
            let _ = sender.send(Message::Stop);
        }
        let threads: Vec<_> = self
            .inner
            .threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect();
        for thread in threads {
            let _ = thread.join();
        }
    }
}
