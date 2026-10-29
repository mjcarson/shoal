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
//! stages first, applies below them, and ~~the pool's refill and~~ every later background last
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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use glommio::{Latency, LocalExecutorBuilder, Placement, PoolPlacement, Shares};
use tracing::{event, Level};

use super::budget::{Budget, Permit, DRAW_WAIT};
use super::cpus::ObjectCpu;
use super::inflight::InFlight;
use super::lane::{self, LaneFigures, LanePlan, Lanes};
use super::serve::{self, Streams};
use super::store::cache::{CacheFigures, CacheReport, UnitCache};
use super::store::{ChunkId, ReadAnswer, Slice, SliceOptions, StageUnits, StageWhole, StoreError};
use crate::server::devices::{DeviceState, SliceState};
use crate::server::errors::ShoalError;
use crate::server::map::TabletMap;
use crate::server::peer::handoff::HandedLane;
use crate::server::ServerError;
use crate::shared::devices::SliceId;
use crate::shared::identity::NodeId;
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, StoreCode};
use crate::shared::protocol::stats::ObjectWork;

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
    /// The memory the units reads verified may be held in, across every executor that owns a
    /// slice; none keeps no cache (F102)
    pub cache: u64,
    /// The registered buffer memory each executor's ring is given
    pub io_memory: usize,
    /// Every slice to serve, with its device and how it is kept
    pub slices: Vec<OwnedSlice>,
    /// The node the executors run on, which a holder on it is local to
    pub node: NodeId,
    /// How the executors dial an object lane to another node's slices, on a cluster node
    pub lanes: Option<LanePlan>,
}

/// What a node's slices' scrubs and checks came to, summed over its slices (F89)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct ScrubSlices {
    /// The bytes deep scrubs read and verified
    pub scrub_bytes: u64,
    /// The checksum failures found, by reads, applies and scrubs
    pub checksum_failures: u64,
    /// The chunks set aside
    pub quarantined: u64,
    /// The most scrub requests any one slice served at once, which is never more than one
    pub scrub_peak: u64,
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
    /// Every placement group the slice holds anything of (M20)
    Groups,
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
    Header(Option<super::store::chunk::ChunkHeader>),
    /// The records no row has decided
    Undecided(Vec<(ChunkId, Label)>),
    /// The placement groups a slice holds anything of, by consumer
    Groups(Vec<(u64, PlacementGroup)>),
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
        /// Told once the request is on the lane's queue, or why it cannot be, so its sender
        /// waits for room as one on this executor does (item 238)
        queued: kanal::AsyncSender<Result<(), ChunkReply>>,
    },
    /// An object lane a shard accepted for a slice this executor owns
    Adopt(HandedLane),
    /// The map the node installed, which the lanes dial members by
    Map(Arc<TabletMap>),
    /// What this executor's lanes have done
    Figures(kanal::Sender<LaneFigures>),
    /// Give the cache's units back down to its bound, which a node past its memory lowered (F107)
    ShrinkCache,
    /// Stop, once every slice is closed
    Stop,
}

/// Ask the allocator to give the memory freed back to the system, so a cache that gave units back
/// lowers the process's resident memory, which is what a node's memory is judged by (F107)
fn trim() {
    // glibc keeps freed memory in its arenas until asked; another allocator has nothing to say
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: malloc_trim takes no pointer and only releases pages the allocator holds free
    unsafe {
        libc::malloc_trim(0);
    }
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
    /// The executors a job and a request for another node's slice are dealt over: every one that
    /// owns no rotational slice, or every one when all do (F91)
    workers: Vec<usize>,
    /// Which executor owns each slice
    owners: BTreeMap<SliceId, Owner>,
    /// Each executor's budget
    budgets: Vec<Arc<Budget>>,
    /// What each executor's cache of read units did (F102)
    caches: Vec<Arc<CacheFigures>>,
    /// What each executor has in flight on its lanes, read without its mailbox (F108)
    inflight: Vec<Arc<InFlight>>,
    /// Each slice's figures, by id
    slices: BTreeMap<SliceId, Arc<SliceState>>,
    /// What the executors did with the erasure code (F90)
    codec: Arc<super::codec::Counters>,
    /// The reads this node has outstanding to each of another node's slices, which a read of a
    /// pool of copies on disks chooses its holder by (F97)
    remote_reads: Mutex<BTreeMap<SliceId, Arc<AtomicU64>>>,
    /// The turn the next choice among holders that tie starts at (F97)
    next_turn: AtomicUsize,
    /// The executors' threads, joined at the stop
    threads: Mutex<Vec<glommio::ExecutorJoinHandle<()>>>,
}

/// A read counted against a remote slice while it is outstanding, uncounted when it ends however
/// it ends, cancelled included (F97)
struct Outstanding(Arc<AtomicU64>);

impl Drop for Outstanding {
    /// The read is no longer outstanding
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
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
    // an operation on a chunk a reclaimer sealed waits for the seal to end (M20); a chunk
    // request waits in its own serve
    let sealed = match &op {
        Op::StageWhole(stage) => Some(stage.id),
        Op::StageUnits(stage) => Some(stage.id),
        Op::Apply(id, _) | Op::Discard(id, _) | Op::ChunkLabel(id) | Op::ChunkHeader(id) => Some(*id),
        Op::Read { id, .. } => Some(*id),
        Op::Undecided(_) | Op::Groups | Op::Chunk { .. } => None,
    };
    if let Some(id) = sealed {
        slice.until_unsealed(&id).await;
    }
    // a stage of what a reclamation just removed is refused, so a late one does not bring it back
    // (item 253)
    let staged = match &op {
        Op::StageWhole(stage) => Some(stage.id),
        Op::StageUnits(stage) => Some(stage.id),
        _ => None,
    };
    if let Some(id) = staged {
        slice.refuse_removed(&id)?;
    }
    match op {
        Op::StageWhole(stage) => slice.stage_whole(stage).await.map(|()| Answer::Done),
        Op::StageUnits(stage) => slice.stage_units(stage).await.map(|()| Answer::Done),
        Op::Apply(id, label) => slice.apply(id, label).await.map(|()| Answer::Done),
        Op::Discard(id, label) => slice.discard(id, label).await.map(|()| Answer::Done),
        Op::Read { id, offset, len, label } => slice.read(id, offset, len, label).await.map(Answer::Read),
        Op::ChunkLabel(id) => slice.chunk_label(&id).await.map(Answer::Label),
        Op::ChunkHeader(id) => slice.chunk_header(&id).await.map(Answer::Header),
        Op::Undecided(older) => Ok(Answer::Undecided(slice.undecided(older))),
        Op::Groups => slice.groups().map(Answer::Groups),
        Op::Chunk { request, .. } => Ok(Answer::Chunk(serve::serve(slice, request).await)),
    }
}

/// Whether an operation is an apply or a discard of a committed write, which runs on the apply
/// queue below the foreground and is never counted as foreground (S13, F91)
///
/// # Arguments
///
/// * `op` - The operation
pub(crate) fn is_apply(op: &Op) -> bool {
    matches!(
        op,
        Op::Apply(..)
            | Op::Discard(..)
            | Op::Chunk {
                request: ChunkRequest::Apply { .. }
                    | ChunkRequest::Discard { .. }
                    | ChunkRequest::Reclaim { .. }
                    | ChunkRequest::DiscardThrough { .. },
                ..
            }
    )
}

/// How often a background request looks again at a slice that was busy
const IDLE_POLL: Duration = Duration::from_millis(1);

/// The bytes a request moves on its slice, which a background one draws from its device's
/// recovery ceiling
///
/// # Arguments
///
/// * `request` - The request
pub(crate) fn moved_bytes(request: &ChunkRequest) -> u64 {
    match request {
        ChunkRequest::Write(piece) | ChunkRequest::Stage(piece) => piece.bytes.len() as u64,
        ChunkRequest::StageUnits { units, .. } | ChunkRequest::Fold { units, .. } => {
            units.iter().map(|unit| unit.bytes.len() as u64).sum()
        }
        ChunkRequest::Read { len, .. } | ChunkRequest::Verify { len, .. } | ChunkRequest::VerifySummary { len, .. } => *len,
        // a parity stage reads its old units and journals its new ones: twice what changed (F90)
        ChunkRequest::StageParity { changes, .. } => {
            2 * changes
                .iter()
                .flat_map(|change| change.units.iter())
                .map(|unit| unit.bytes.len() as u64)
                .sum::<u64>()
        }
        ChunkRequest::Background(inner) => moved_bytes(inner),
        // a scrub's listing draws a header block a chunk as it reads each, on its slice (F89)
        ChunkRequest::Scrub(inner) => moved_bytes(inner),
        ChunkRequest::Apply { .. }
        | ChunkRequest::Discard { .. }
        | ChunkRequest::Confirm { .. }
        | ChunkRequest::List { .. }
        | ChunkRequest::Seal { .. }
        | ChunkRequest::Unseal { .. }
        | ChunkRequest::Reclaim { .. }
        | ChunkRequest::DiscardThrough { .. } => 0,
    }
}

/// Whether a background request waits for its slice's idle time: everything but a confirmation
/// and a listing, which read a chunk's header or a page of a directory and move no bytes
///
/// X12 paced the bytes a rebuild moves. A move's listings and a rebuild's confirmation held to the
/// same gate made a failed device's two thousand mostly empty placement groups wait on busy slices
/// for every one of them, a move a second on the lab under eight 1 MiB operations in flight.
///
/// # Arguments
///
/// * `request` - The request it wraps
pub(crate) fn waits_for_idle(request: &ChunkRequest) -> bool {
    match request {
        ChunkRequest::Confirm { .. } | ChunkRequest::List { .. } => false,
        ChunkRequest::Background(inner) => waits_for_idle(inner),
        // a scrub's request always waits, its listings too: a light scrub lists every chunk of a
        // device, which no foreground pays for
        ChunkRequest::Scrub(_) => true,
        _ => true,
    }
}

/// Wait until a background request may run on its slice: no foreground request in flight there,
/// and its bytes drawn from its device's recovery ceiling (F88)
///
/// A slice that is never idle runs none, which is the rule X12 measured holds a foreground's tail;
/// the ceiling bounds what an idle slice's recovery takes of its device beside its other slices.
/// A request that moves no bytes and does not wait for idle time (`waits_for_idle`) is not gated.
///
/// # Arguments
///
/// * `slice` - The slice
/// * `bytes` - What the request moves
pub(crate) async fn background_gate(slice: &Rc<Slice>, bytes: u64) {
    // the bytes a background request moves, drawn once the foreground is out of the way
    loop {
        // the foreground and the applies first, every time either is in flight (S13, F91)
        if !slice.state().idle_for_background() {
            glommio::timer::sleep(IDLE_POLL).await;
            continue;
        }
        match slice.device().ceiling.take(bytes) {
            Ok(()) => break,
            Err(wait) => glommio::timer::sleep(wait.max(IDLE_POLL)).await,
        }
    }
    slice
        .state()
        .background_bytes
        .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Wait until a scrub's request may run on its slice: no foreground request and no rebuild's or
/// move's in flight there, no other scrub request either, and its bytes drawn from its device's
/// ceiling, the budget recovery draws on too (F89)
///
/// Scrubs yield to recovery and recovery to the foreground, and a slice serves one scrub piece at a
/// time: what X12 measured holds a foreground's tail while a deep scrub reads 385 MiB/s of an SSD
/// ([X12](../../../../docs/src/object-storage/recovery-scrub-rates.md#the-comparison)). Answers the
/// slice's one scrub permit, held until the request is answered.
///
/// # Arguments
///
/// * `slice` - The slice
/// * `bytes` - What the request reads
pub(crate) async fn scrub_gate(slice: &Rc<Slice>, bytes: u64) -> crate::server::devices::Scrubbing {
    loop {
        // the foreground and recovery first, every time either is in flight
        if !slice.state().idle_for_scrub() {
            glommio::timer::sleep(IDLE_POLL).await;
            continue;
        }
        // one scrub piece at a time
        let Some(permit) = slice.state().try_scrub() else {
            glommio::timer::sleep(IDLE_POLL).await;
            continue;
        };
        match slice.device().ceiling.take(bytes) {
            Ok(()) => return permit,
            Err(wait) => {
                drop(permit);
                glommio::timer::sleep(wait.max(IDLE_POLL)).await;
            }
        }
    }
}

/// What a background or a scrub's request is counted under while it is served on its slice,
/// given back when this drops
pub(crate) struct Gated {
    /// A rebuild's or a move's count, which a scrub waits out
    _recovering: Option<crate::server::devices::Recovering>,
    /// A scrub's, the slice's one permit
    _scrubbing: Option<crate::server::devices::Scrubbing>,
}

/// Wait for a background or a scrub's request's gate, and answer the request as it is served with
/// what it is counted under until it is answered
///
/// The request keeps its class, which [`serve`](serve::serve) answers it by: a rebuild's or a
/// move's read neither takes from the cache of read units nor fills it (F102), and a scrub's listing
/// draws a block of its device's ceiling for each header it reads (F89). A piece of a whole chunk
/// is answered bare, since its stream writes it whatever its class. Until item 299 every request
/// was answered bare, and served as a reader's. A request of neither class passes at once, counted
/// under nothing.
///
/// # Arguments
///
/// * `slice` - The slice
/// * `request` - The request, a background or a scrub's
pub(crate) async fn gate(slice: &Rc<Slice>, request: ChunkRequest) -> (ChunkRequest, Option<Gated>) {
    match request {
        ChunkRequest::Background(inner) => {
            // in flight as recovery from before its wait, so a scrub never starts beside it
            let held = slice.state().recovering();
            if waits_for_idle(&inner) {
                background_gate(slice, moved_bytes(&inner)).await;
            }
            (
                served_as(ChunkRequest::Background(inner)),
                Some(Gated {
                    _recovering: Some(held),
                    _scrubbing: None,
                }),
            )
        }
        ChunkRequest::Scrub(inner) => {
            let permit = scrub_gate(slice, moved_bytes(&inner)).await;
            (
                served_as(ChunkRequest::Scrub(inner)),
                Some(Gated {
                    _recovering: None,
                    _scrubbing: Some(permit),
                }),
            )
        }
        other => (other, None),
    }
}

/// A gated request as it is served: a piece of a whole chunk bare, for its stream to write, and
/// anything else in the class it came in, which `serve` answers it by (item 299)
///
/// # Arguments
///
/// * `request` - A background or a scrub's request
fn served_as(request: ChunkRequest) -> ChunkRequest {
    match request {
        ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner)
            if matches!(*inner, ChunkRequest::Write(_) | ChunkRequest::Stage(_)) =>
        {
            *inner
        }
        other => other,
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
/// * `foreground` - Whether it counts as foreground on its slice until it is answered
/// * `gated` - What a background piece is counted under until it is answered
#[allow(clippy::too_many_arguments)]
fn take_piece(
    streams: &Streams,
    slice: Option<Rc<Slice>>,
    id: SliceId,
    origin: u64,
    place: bool,
    piece: crate::shared::protocol::peer::chunk::Piece,
    reply: kanal::AsyncSender<Result<Answer, StoreError>>,
    foreground: bool,
    gated: Option<Gated>,
) {
    // a foreground piece is in flight on its slice until it is answered
    let held = slice
        .as_ref()
        .filter(|_| foreground)
        .map(|slice| slice.state().foreground());
    // the answer goes back on the request's own channel, which has room for the one answer
    let replier: serve::Replier = Box::new(move |answer| {
        let _ = reply.try_send(Ok(Answer::Chunk(answer)));
        drop(held);
        drop(gated);
    });
    match slice {
        Some(slice) => streams.take(slice, origin, place, piece, None, replier),
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
/// * `cache` - Its share of the node's cache of read units, and where it counts what that does (F102)
/// * `inflight` - What its lanes have in flight, read without its mailbox (F108)
/// * `mailbox` - Where its requests arrive
/// * `opened` - Where it says its slices are open
/// * `store` - The handle its jobs run with, set once every executor started
/// * `lanes` - How it dials an object lane, on a cluster node
#[allow(clippy::too_many_arguments)]
async fn serve(
    index: usize,
    mine: Vec<(Arc<DeviceState>, Arc<SliceState>, SliceOptions)>,
    budget: Arc<Budget>,
    cache: (u64, Arc<CacheFigures>),
    inflight: Arc<InFlight>,
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
    // the units its slices' reads verify, kept for the next read of them, in its share of the
    // node's cache; a node that keeps none, or an executor that owns no slice, has none (F102)
    let (share, figures) = cache;
    let cache = (share > 0).then(|| Rc::new(UnitCache::new(share, figures)));
    // every slice it owns, a failed device's left closed and refusing
    let mut slices: HashMap<SliceId, Rc<Slice>> = HashMap::new();
    for (device, state, mut options) in mine {
        options.budget = Some(budget.clone());
        options.apply_queue = Some(applies);
        let id = state.id;
        match Slice::open(device.clone(), state, options, Some(background)).await {
            Ok(slice) => {
                if let Some(cache) = &cache {
                    slice.set_cache(cache.clone());
                }
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
    let lanes = Lanes::start(
        lanes,
        streams.clone(),
        budget.clone(),
        inflight,
        Some(background),
        Some(applies),
    );
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
            // a remote holder's request goes out on this executor's lane to it, on a task of its
            // own so one waiting for room never holds the mailbox
            Message::Remote {
                holder,
                request,
                reply,
                queued,
            } => {
                let lanes = lanes.clone();
                if let Ok(spawned) = glommio::spawn_local_into(
                    async move {
                        let sent = lanes.send(holder, &request, reply).await;
                        let _ = queued.send(sent).await;
                    },
                    foreground,
                ) {
                    spawned.detach();
                }
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
            // the units past the bound a node past its memory lowered, given back now, and the
            // allocator asked to return what they held to the system (F107)
            Message::ShrinkCache => {
                if cache.as_ref().is_some_and(|cache| cache.shrink() > 0) {
                    trim();
                }
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
                take_piece(&streams, slices.get(&id).cloned(), id, origin, true, piece, reply, true, None);
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
                take_piece(&streams, slices.get(&id).cloned(), id, origin, false, piece, reply, true, None);
                continue;
            }
            // a background or a scrub's request waits on its own task for its slice's gate, then
            // runs in its class, counted under it until it is answered, and a piece of a whole
            // chunk by its stream; a rebuild sends a whole chunk's pieces one at a time, so a
            // stream's order is its sender's (F88, F89, item 299)
            Request {
                slice: id,
                op:
                    Op::Chunk {
                        request: request @ (ChunkRequest::Background(_) | ChunkRequest::Scrub(_)),
                        origin,
                    },
                reply,
                ..
            } => {
                let Some(slice) = slices.get(&id).cloned() else {
                    let _ = reply.try_send(Ok(Answer::Chunk(serve::refused(&StoreError::UnknownSlice(id)))));
                    continue;
                };
                let streams = streams.clone();
                let task = async move {
                    let (inner, held) = gate(&slice, request).await;
                    match inner {
                        ChunkRequest::Write(piece) => {
                            take_piece(&streams, Some(slice), id, origin, true, piece, reply, false, held);
                        }
                        ChunkRequest::Stage(piece) => {
                            take_piece(&streams, Some(slice), id, origin, false, piece, reply, false, held);
                        }
                        request => {
                            let answer = run(&slice, Op::Chunk { request, origin }).await;
                            let _ = reply.send(answer).await;
                            drop(held);
                        }
                    }
                };
                if let Ok(spawned) = glommio::spawn_local_into(task, background) {
                    spawned.detach();
                }
                continue;
            }
            request => request,
        };
        let slice = slices.get(&request.slice).cloned();
        // an apply or a discard runs below the foreground and is never counted as it, so a read
        // waits for no apply's queue and a rebuild for every apply in flight (S13, F91)
        let applying = is_apply(&request.op);
        let lane = if applying { applies } else { foreground };
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
                    // in flight on its slice, which a background request waits out; an apply is
                    // counted by the slice as it runs
                    let _foreground = (!applying).then(|| slice.state().foreground());
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
        let mut inflight = Vec::with_capacity(executors);
        let mut owners = BTreeMap::new();
        let mut slices = BTreeMap::new();
        let mut owned: Vec<Vec<OwnedSlice>> = vec![Vec::new(); executors];
        // a rotational device's slice never shares an executor with a solid state one's: every
        // disk's slice on the last executor, the rest dealt round the others (S13, X7, F91)
        let disks = plan.slices.iter().filter(|(device, _, _)| device.rotational()).count();
        let split = disks > 0 && disks < plan.slices.len();
        if split && executors < 2 {
            return Err(ServerError::Shoal(ShoalError::ObjectCpuRefused {
                cpu: plan.cpus.first().map_or(0, |cpu| cpu.cpu),
                why: "the node serves a rotational device and a solid state one, and a disk's slice \
                      never shares an executor with an SSD's; name a second cpu in \
                      resources.object_cpus"
                    .to_string(),
            }));
        }
        let mut dealt_solid = 0usize;
        for (index, (device, state, options)) in plan.slices.into_iter().enumerate() {
            let executor = if !split {
                index % executors
            } else if device.rotational() {
                executors - 1
            } else {
                dealt_solid += 1;
                (dealt_solid - 1) % (executors - 1)
            };
            if split {
                event!(
                    Level::INFO,
                    device = %device.path().display(),
                    slice = %state.id,
                    rotational = device.rotational(),
                    msg = if device.rotational() {
                        "a rotational device's slice is served on an executor no solid state slice shares"
                    } else {
                        "a solid state device's slice is served on an executor no rotational slice shares"
                    },
                );
            }
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
        // jobs and requests for other nodes' slices go to executors no disk's slice is on, which a
        // put's pipeline and a code's arithmetic would otherwise hold up (F91)
        let mut workers: Vec<usize> = (0..executors)
            .filter(|executor| owned[*executor].iter().all(|(device, _, _)| !device.rotational()))
            .collect();
        if workers.is_empty() {
            workers = (0..executors).collect();
        }
        // the cache of read units is shared out by the slices each executor owns, so one that
        // owns none, a worker beside a disk's executor, holds none (F102)
        let owned_slices = owned.iter().map(Vec::len).sum::<usize>() as u64;
        let shares: Vec<u64> = owned
            .iter()
            .map(|mine| match owned_slices {
                0 => 0,
                all => plan.cache.saturating_mul(mine.len() as u64) / all,
            })
            .collect();
        let mut caches = Vec::with_capacity(executors);
        let mut threads = Vec::with_capacity(executors);
        let (opened, opened_rx) = std::sync::mpsc::channel();
        // what every executor's calls of the erasure code count into, this node's alone (F90)
        let codec = Arc::new(super::codec::Counters::default());
        // the handle every executor runs its jobs with, set once they have all started
        let handle_cell: Arc<OnceLock<ObjectStore>> = Arc::new(OnceLock::new());
        for (index, (place, mine)) in plan.cpus.iter().zip(owned).enumerate() {
            let (sender, receiver) = kanal::bounded(4096);
            let budget = Budget::new(each);
            senders.push(sender);
            budgets.push(budget.clone());
            // what its lanes have in flight, which the store reads without its mailbox (F108)
            let flight = Arc::new(InFlight::default());
            inflight.push(flight.clone());
            // its share of the cache, and the figures the store reads it by
            let figures = Arc::new(CacheFigures::default());
            caches.push(figures.clone());
            let cache = (shares.get(index).copied().unwrap_or(0), figures);
            let opened = opened.clone();
            let cell = handle_cell.clone();
            let lanes = plan.lanes.clone();
            let counters = codec.clone();
            // its blocking thread, where every rename and unlink goes, on the cpu chosen for it
            let blocking = glommio::CpuSet::online()?.filter(|location| location.cpu == place.blocking);
            let handle = LocalExecutorBuilder::new(Placement::Fixed(place.cpu))
                .name(&format!("shoal-objects-{index}"))
                .io_memory(plan.io_memory)
                .ring_depth(256)
                .blocking_thread_pool_placement(PoolPlacement::Custom(vec![blocking]))
                .spawn(move || {
                    // this thread's calls of the code count as this node's
                    super::codec::install(counters);
                    serve(
                        index, mine, budget, cache, flight, receiver, opened, cell, lanes,
                    )
                })?;
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
                workers,
                owners,
                budgets,
                caches,
                inflight,
                slices,
                codec,
                remote_reads: Mutex::new(BTreeMap::new()),
                next_turn: AtomicUsize::new(0),
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
        let workers = &self.inner.workers;
        let dealt = self.inner.next_job.fetch_add(1, Ordering::Relaxed) % workers.len().max(1);
        let executor = workers.get(dealt).copied().unwrap_or(0);
        self.inner
            .senders
            .get(executor)
            .ok_or_else(|| StoreError::Invalid("this node runs no object executor".to_string()))?
            .send(Message::Run(job))
            .map_err(|_| StoreError::Invalid("the object executors have stopped".to_string()))
    }

    /// Run a computation on one of the executors and hand back what it came to
    ///
    /// Erasure arithmetic is object work, and never runs on a table shard (X9): a coordinating
    /// shard hands an encode or a decode here and waits for its answer, while the executor takes
    /// it in steps its latency goal can cut ([F90](../../../../docs/src/features/erasure-coded-pools.md)).
    ///
    /// # Arguments
    ///
    /// * `work` - What to compute: a future made on the executor
    ///
    /// # Errors
    ///
    /// Every executor has stopped, or the one it ran on stopped before it answered.
    pub async fn compute<T, F>(&self, work: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce() -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
    {
        let (answer, answered) = kanal::bounded_async(1);
        self.run(Box::new(move |_store| {
            Box::pin(async move {
                let _ = answer.send(work().await).await;
            })
        }))?;
        answered
            .recv()
            .await
            .map_err(|_| StoreError::Invalid("an object executor stopped before its computation answered".to_string()))
    }

    /// What this node's object executors did with the erasure code since it started (F90)
    #[must_use]
    pub fn codec_figures(&self) -> crate::shared::protocol::stats::CodecFigures {
        self.inner.codec.figures()
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
    /// one holder goes out on one lane, in the order it was sent. Either way this returns once the
    /// request is on the lane's queue: a queue at its bound holds its sender until the lane has
    /// written enough to take it (item 238).
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - The request
    ///
    /// # Errors
    ///
    /// This node's executors have stopped, or the lane refused the request - the object wire not
    /// activated, no lane to the node, or a holder that stalled - as the refusal the holder would
    /// have sent.
    async fn send_remote(
        &self,
        holder: Holder,
        request: ChunkRequest,
    ) -> Result<kanal::AsyncReceiver<Result<Answer, StoreError>>, ChunkReply> {
        let (reply, answer) = kanal::bounded_async(1);
        // an object executor sends on its own lanes, with no hop between threads, once the
        // lane has room
        if let Some(lanes) = lane::current() {
            lanes.send(holder, &request, reply).await?;
            return Ok(answer);
        }
        // anything else hands it to the executor the holder is dealt to
        let (high, low) = holder.slice.0.as_u64_pair();
        let dealt = crate::shared::rendezvous::mix64(high ^ low.rotate_left(17));
        let workers = &self.inner.workers;
        let executor = workers
            .get((dealt % workers.len().max(1) as u64) as usize)
            .copied()
            .unwrap_or(0);
        let Some(sender) = self.inner.senders.get(executor) else {
            return Err(ChunkReply::Refused {
                code: StoreCode::Unavailable,
                message: "this node runs no object executor to reach another node's slices".to_string(),
            });
        };
        let stopped = || ChunkReply::Refused {
            code: StoreCode::Unavailable,
            message: "the object executor has stopped".to_string(),
        };
        let (queued, on_queue) = kanal::bounded_async(1);
        sender
            .as_async()
            .send(Message::Remote {
                holder,
                request,
                reply,
                queued,
            })
            .await
            .map_err(|_| stopped())?;
        // and the sender waits until the lane took it, as one on that executor would
        on_queue.recv().await.map_err(|_| stopped())??;
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

    /// The reads queued on a holder's slice as this node sees them: one of this node's slices
    /// counts every request in flight on it, from any node, and never fewer than the reads this
    /// node chose it for that are not answered yet; another node's, the reads this node has
    /// outstanding to it (F97)
    ///
    /// A request is in flight on a slice only once its executor takes it from its mailbox, which a
    /// loaded host does late: reads that chose the slice before then each saw an empty queue, and
    /// one disk served every one of them (item 314).
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    #[must_use]
    pub fn queue_of(&self, holder: &Holder) -> u64 {
        // every request in flight on this node's own slice, at least the reads it chose
        if self.is_local(holder) {
            return self.inner.slices.get(&holder.slice).map_or(0, |state| state.queue());
        }
        self.inner
            .remote_reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&holder.slice)
            .map_or(0, |count| count.load(Ordering::Relaxed))
    }

    /// The turn a choice among holders that tie starts at, moved one on for every choice (F97)
    #[must_use]
    pub fn next_turn(&self) -> usize {
        self.inner.next_turn.fetch_add(1, Ordering::Relaxed)
    }

    /// Ask a holder to read, counting the read against its slice from the moment it is chosen
    /// until it is answered, so the next read's choice of holder sees it whichever node the slice
    /// is on (F97, item 314)
    ///
    /// # Arguments
    ///
    /// * `holder` - The holder
    /// * `request` - The read
    pub async fn read_from(&self, holder: Holder, request: ChunkRequest) -> ChunkReply {
        // a read of this node's own slice counted by the slice, with no lock taken, from its choice
        // until it is answered however it ends (item 314)
        let _chosen = self
            .is_local(&holder)
            .then(|| self.inner.slices.get(&holder.slice).map(SliceState::chosen))
            .flatten();
        // and one of another node's slice counted while it is outstanding, and uncounted however it
        // ends
        let _outstanding = (!self.is_local(&holder)).then(|| {
            let count = self
                .inner
                .remote_reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(holder.slice)
                .or_default()
                .clone();
            count.fetch_add(1, Ordering::Relaxed);
            Outstanding(count)
        });
        self.ask_holder(holder, request).await
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
    pub async fn chunk_header(&self, slice: SliceId, id: ChunkId) -> Result<Option<super::store::chunk::ChunkHeader>, StoreError> {
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

    /// Every placement group one of this node's slices holds anything of, by consumer (M20)
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    ///
    /// # Errors
    ///
    /// A failed device, or a slice this node does not serve.
    pub async fn groups(&self, slice: SliceId) -> Result<Vec<(u64, PlacementGroup)>, StoreError> {
        match self.ask(slice, Op::Groups, 0).await? {
            Answer::Groups(groups) => Ok(groups),
            other => Err(StoreError::Invalid(format!("a slice's groups were asked and answered {other:?}"))),
        }
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

    /// The bytes each slice's reads have answered, by slice (F90)
    #[must_use]
    pub fn read_bytes_by_slice(&self) -> BTreeMap<SliceId, u64> {
        self.inner
            .slices
            .iter()
            .map(|(id, state)| (*id, state.read_bytes.load(std::sync::atomic::Ordering::Relaxed)))
            .collect()
    }

    /// The bytes background requests - rebuilds and moves - have moved on every slice (F88)
    #[must_use]
    pub fn background_bytes(&self) -> u64 {
        self.inner
            .slices
            .values()
            .map(|state| state.background_bytes.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }

    /// What every slice's scrubs and checks came to: the bytes deep scrubs verified, the checksum
    /// failures found, the chunks set aside, and the most scrub requests any slice served at once
    /// (F89)
    #[must_use]
    pub fn scrub_figures(&self) -> ScrubSlices {
        let load = |counter: &std::sync::atomic::AtomicU64| counter.load(std::sync::atomic::Ordering::Relaxed);
        let mut figures = ScrubSlices::default();
        for state in self.inner.slices.values() {
            figures.scrub_bytes += load(&state.scrub_bytes);
            figures.checksum_failures += load(&state.checksum_failures);
            figures.quarantined += load(&state.quarantined);
            figures.scrub_peak = figures.scrub_peak.max(load(&state.scrub_peak));
        }
        figures
    }

    /// What every executor's cache of read units did, summed over them (F102)
    #[must_use]
    pub fn cache_figures(&self) -> CacheReport {
        // each executor's own, added up
        let mut report = CacheReport::default();
        for figures in &self.inner.caches {
            report.add(&figures.report());
        }
        report
    }

    /// Have the node's caches of read units give units back, the process past the node's memory by
    /// `excess`: whether any held units to give ([F107](../../../../docs/src/features/cache-default-share.md))
    ///
    /// A node past its memory gives up what its caches hold before any shard evicts a row. Each
    /// cache's bound falls below what it holds by its share of the excess and an eighth of what it
    /// holds more, and is never raised again, and its executor gives the units past it back. Every
    /// shard of a node may ask at once: a bound only falls, so the same excess lowers it once.
    ///
    /// # Arguments
    ///
    /// * `excess` - The bytes the process holds past the node's memory
    pub fn yield_caches(&self, excess: u64) -> bool {
        // what the caches hold together, which shares the excess out
        let held: u64 = self.inner.caches.iter().map(|figures| figures.held.load(Ordering::Relaxed)).sum();
        if held == 0 {
            return false;
        }
        for (executor, figures) in self.inner.caches.iter().enumerate() {
            let own = figures.held.load(Ordering::Relaxed);
            if own == 0 {
                continue;
            }
            // its share of the excess by what it holds, and an eighth of what it holds besides
            let share = u64::try_from(u128::from(excess) * u128::from(own) / u128::from(held)).unwrap_or(own);
            let bound = own.saturating_sub(share.saturating_add(own / 8));
            figures.limit.fetch_min(bound, Ordering::Relaxed);
            // and its executor told to give back what is past it now
            if let Some(sender) = self.inner.senders.get(executor) {
                let _ = sender.try_send(Message::ShrinkCache);
            }
        }
        true
    }

    /// The object work this node's executors and slices have in flight, read from atomics alone,
    /// never through an executor's mailbox, so a wedged executor reports what it holds too
    /// ([F108](../../../../docs/src/features/object-work-in-flight.md))
    ///
    /// The shards' own figures, `tasks` and what they refused or gave up, are each shard's to add.
    #[must_use]
    pub fn work(&self) -> ObjectWork {
        let mut work = ObjectWork::default();
        // every executor's budget, together
        for budget in &self.inner.budgets {
            work.budget_used += budget.used();
            work.budget_limit += budget.limit();
            work.budget_shed += budget.shed();
        }
        // what each executor's lanes have in flight
        for flight in &self.inner.inflight {
            flight.add_to(&mut work);
        }
        // and what each slice has in flight
        for slice in self.inner.slices.values() {
            work.foreground += slice.foreground.load(Ordering::Relaxed);
            work.staged_bytes += slice.staged_bytes.load(Ordering::Relaxed);
            work.recovery += slice.recovery.load(Ordering::Relaxed);
            work.scrubs += slice.scrub_inflight.load(Ordering::Relaxed);
            work.applying += slice.applying.load(Ordering::Relaxed);
        }
        work
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
