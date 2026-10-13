//! Object operations on a shard ([F80](../../../../../docs/src/features/object-wire.md),
//! [F86](../../../../../docs/src/features/replicated-pools.md))
//!
//! An object's metadata is rows of its bucket's two generated tables: its path's entry in
//! `ObjectMeta`, and a `StripeMeta` row for each stripe written in place
//! ([S3](../../../../../docs/src/object-storage/objects.md)). An object at or under its pool's
//! inline threshold is held in its entry; a larger one is stripes, whose chunks its pool's holders
//! keep on their devices. Every operation is a task on the shard the client's connection reached:
//!
//! - **The metadata client.** Each shard registers one virtual client of its own with every
//!   shard, as a peer's data lane does, and a demultiplexer hands each answer it is sent to the
//!   task waiting on that bundle. A task's queries are routed, replicated, forwarded and gathered
//!   exactly as a client's are - through `ServerMsg::Internal`, which enters routing past the front
//!   door that refuses every client's query of a generated table.
//! - **The object task.** One an operation, holding no borrow of its shard: the metadata client,
//!   the shard's own channel, the connection's channel, the bucket's `ObjectOps`, the map it
//!   places by and the node's object store, through which every stripe chunk is asked of its
//!   holder. It answers the operation once, on the connection's channel.
//! - **Its parts**: [`put`] writes a whole object, [`read`] reads a range or says what an object
//!   is, [`write`] writes in place or appends, [`truncate`] cuts or grows, [`decide`] settles a
//!   stage its stager left, and [`place`] says where a stripe's chunks are.
//!
//! # Invariants
//!
//! **An internal bundle's identity is fresh, except a stripe commit's.** A gather is keyed by its
//! bundle and index, and a tablet group remembers a write's result by its bundle and index: a
//! conditional write retried after a refusal under the same identity would be answered the first
//! refusal forever. A stripe commit's identity is derived from its write, its stripe and its try
//! ([`crate::shared::stripes::commit_bundle`]), so the same try resent after an unknown outcome is
//! answered as it was, and a retry of the write on another coordinator learns which of its tries
//! committed.
//!
//! **A put commits nothing it has not checked.** Its bytes are held to the CRC-64/NVME its head
//! named before its entry is committed.
//!
//! **A retried put is the same write.** Its object id is minted from its identity and its path
//! (`objects::object_id`), so a retry that finds its own id in the entry answers as the first try
//! did and commits nothing.
//!
//! **An answer read from the metadata client is validated** (`ObjectOps::answer_bytes`,
//! `stripe_answer_bytes`): a forwarded share's bytes reach it as a peer wrote them.

pub(crate) mod decide;
pub(crate) mod place;
pub(crate) mod put;
pub(crate) mod read;
pub(crate) mod truncate;
pub(crate) mod write;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::Either;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::util::AlignedVec;
use tracing::{event, Level, Span};
use uuid::Uuid;

use super::Reservation;
use crate::server::conf::ObjectTimings;
use crate::server::database::ShoalDatabase;
use crate::server::map::TabletMap;
use crate::server::messages::{Reply, ReplyKind, ServerMsg};
use crate::server::objects::ObjectStore;
use crate::server::stage_profile::{StageStamps, Stamp};
use crate::shared::objects::{MetaAnswer, ObjectEntry, ObjectOps, ObjectRow, StripeAnswer};
use crate::shared::pools::PoolSpec;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::{ObjectAnswer, ObjectOp, ObjectOpKind, ObjectOutcome};
use crate::shared::protocol::read::{ReadLevel, ReadOptions};
use crate::shared::queries::Queries;
use crate::shared::stripes::StripeRow;
use crate::shared::traits::{QuerySupport, RkyvSupport};

/// The most object operations one shard runs at once
///
/// A put's bytes are held against the shard's assembly budget, or its stream's window, until its
/// task ends, which bounds what puts hold; this bounds the rest, so a client pipelining reads
/// cannot start more than this.
pub(crate) const MAX_OBJECT_TASKS: usize = 1024;

/// How many times a write reads its row again after its commit was refused before it gives up
///
/// A refusal means another write to a path of the same row, or to the same stripe, committed in
/// between; a write that loses this many races in a row is answered `Busy`, which a client retries.
const MAX_TRIES: u32 = 16;

/// The read level an object's entry is read at unless the operation names one
///
/// A row read at `One` can be a replica behind its leader, and a reader that trusted one could be
/// shown an object older than an acknowledged write ([S9](../../../../../docs/src/object-storage/read-path.md)).
const DEFAULT_LEVEL: ReadLevel = ReadLevel::Quorum;

/// The bytes every metadata client's id begins with, which no client's id can
///
/// A metadata client's id is a version 8 UUID of these bytes and its shard's number, so a shard
/// can tell one in its client map from a connection; a connection's id is a version 4 UUID, which
/// never is version 8.
const META_CLIENT_MARKER: [u8; 6] = *b"OBJLNE";

/// The id of a shard's metadata client, the same on every start
///
/// # Arguments
///
/// * `shard` - The shard whose client it is
#[must_use]
pub(crate) fn meta_client_id(shard: usize) -> Uuid {
    // the marker, then the shard's number, then the version and variant bits a v8 UUID carries
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&META_CLIENT_MARKER);
    bytes[8..16].copy_from_slice(&(shard as u64).to_be_bytes());
    bytes[6] = 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Whether a client id is a metadata client's rather than a connection's
///
/// # Arguments
///
/// * `id` - The client id
#[must_use]
pub(crate) fn is_meta_client(id: &Uuid) -> bool {
    // a version 8 id that begins with the marker
    id.get_version_num() == 8 && id.as_bytes()[..6] == META_CLIENT_MARKER
}

/// A shard's virtual client: the answers to every bundle its object tasks send
#[derive(Clone)]
pub(crate) struct MetaClient {
    /// The client id every shard knows the client by
    pub id: Uuid,
    /// The channel answers are sent down, which every shard holds a clone of
    pub tx: AsyncSender<Reply>,
    /// The task waiting on each bundle, by its id
    waiters: Rc<RefCell<HashMap<Uuid, oneshot::Sender<Reply>>>>,
    /// How many object operations this shard is running
    inflight: Rc<Cell<usize>>,
    /// The stripes whose pending bytes a task of this shard is clearing, so one clear runs at once
    clearing: Rc<RefCell<HashSet<(u64, u128, u64)>>>,
    /// The stages this shard is deciding, so one decide of each runs at once
    pub(crate) deciding: Rc<RefCell<HashSet<(crate::shared::devices::SliceId, crate::shared::stripes::ChunkId)>>>,
    /// When this shard first met each stripe's pending bytes, by the label they make
    ///
    /// Its own clock, never a time a row carries: a writer that died before its clear leaves its
    /// bytes in the row, and whoever meets them there for `clear_wait` folds and clears them.
    pending_seen: Rc<RefCell<HashMap<StripeOf, (crate::shared::objects::Label, std::time::Instant)>>>,
}

/// A stripe as its row is keyed: its consumer, its object and its index
type StripeOf = (u64, u128, u64);

impl MetaClient {
    /// A metadata client, and the receiving end its demultiplexer reads
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard whose client it is
    pub fn new(shard: usize) -> (Self, AsyncReceiver<Reply>) {
        // the channel every shard answers the client on
        let (tx, rx) = kanal::unbounded_async();
        let client = MetaClient {
            id: meta_client_id(shard),
            tx,
            waiters: Rc::new(RefCell::new(HashMap::new())),
            inflight: Rc::new(Cell::new(0)),
            clearing: Rc::new(RefCell::new(HashSet::new())),
            deciding: Rc::new(RefCell::new(HashSet::new())),
            pending_seen: Rc::new(RefCell::new(HashMap::new())),
        };
        (client, rx)
    }

    /// Hand every answer the client is sent to the task waiting on its bundle, until it closes
    ///
    /// # Arguments
    ///
    /// * `rx` - The client's receiving end
    pub async fn demux(self, rx: AsyncReceiver<Reply>) {
        // one answer a bundle, each to its waiter
        while let Ok(reply) = rx.recv().await {
            let waiter = self.waiters.borrow_mut().remove(&reply.id);
            match waiter {
                Some(waiter) => {
                    // a waiter that gave up has dropped its end, and the answer goes with it
                    let _ = waiter.send(reply);
                }
                None => {
                    event!(Level::DEBUG, msg = "a metadata answer nobody waits for", id = %reply.id);
                }
            }
        }
    }

    /// Wait for the answer to a bundle about to be sent
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle's id
    fn register(&self, bundle: Uuid) -> oneshot::Receiver<Reply> {
        let (tx, rx) = oneshot::channel();
        self.waiters.borrow_mut().insert(bundle, tx);
        rx
    }

    /// Stop waiting for a bundle's answer
    ///
    /// # Arguments
    ///
    /// * `bundle` - The bundle's id
    fn forget(&self, bundle: &Uuid) {
        self.waiters.borrow_mut().remove(bundle);
    }

    /// How many object operations this shard is running
    pub fn running(&self) -> usize {
        self.inflight.get()
    }

    /// Count one more operation running, until the guard is dropped
    fn start(&self) -> Running {
        self.inflight.set(self.inflight.get() + 1);
        Running {
            inflight: self.inflight.clone(),
        }
    }

    /// Take a stripe for clearing, if no task of this shard is clearing it
    ///
    /// # Arguments
    ///
    /// * `key` - The stripe
    fn begin_clear(&self, key: (u64, u128, u64)) -> bool {
        self.clearing.borrow_mut().insert(key)
    }

    /// Give a stripe back once its clear is done
    ///
    /// # Arguments
    ///
    /// * `key` - The stripe
    fn end_clear(&self, key: &(u64, u128, u64)) {
        self.clearing.borrow_mut().remove(key);
        self.pending_seen.borrow_mut().remove(key);
    }

    /// Whether a stripe's pending bytes have stood in its row since this shard first met them
    /// for at least `wait`, noting them the first time
    ///
    /// # Arguments
    ///
    /// * `key` - The stripe
    /// * `label` - The label its pending bytes make
    /// * `wait` - How long a writer is given to clear them itself
    fn pending_overdue(&self, key: (u64, u128, u64), label: crate::shared::objects::Label, wait: std::time::Duration) -> bool {
        let mut seen = self.pending_seen.borrow_mut();
        // the shard forgets what it met before it remembers past a bound
        if seen.len() >= PENDING_SEEN && !seen.contains_key(&key) {
            seen.clear();
        }
        match seen.get(&key) {
            // the same bytes, met long enough ago
            Some((met, at)) if *met == label => at.elapsed() >= wait,
            // new to this shard, or newer bytes than it met
            _ => {
                seen.insert(key, (label, std::time::Instant::now()));
                false
            }
        }
    }
}

/// The most stripes a shard remembers meeting pending bytes in
const PENDING_SEEN: usize = 4096;

/// One object operation counted as running, until it is dropped
struct Running {
    /// The count it is in
    inflight: Rc<Cell<usize>>,
}

impl Drop for Running {
    /// Count the operation as done
    fn drop(&mut self) {
        self.inflight.set(self.inflight.get().saturating_sub(1));
    }
}

/// What a query of the metadata came back as, before it is read as a row or a stripe
enum RawAnswer {
    /// The archived answer of the table's own response type
    Whole(AlignedVec),
    /// The machinery refused it, and why
    Failed(ErrorCode, String),
}

/// What every object task reads and commits metadata through, holding nothing of its shard
pub(crate) struct Meta<D: ShoalDatabase> {
    /// The bucket's queries
    pub ops: &'static dyn ObjectOps<D::ClientType>,
    /// The shard's metadata client
    pub client: MetaClient,
    /// The shard's own channel, which internal bundles are routed through
    pub shard_tx: AsyncSender<ServerMsg<D>>,
    /// The operation's span
    pub span: Span,
    /// How long one internal query may take before it is given up
    pub wait: Duration,
}

impl<D: ShoalDatabase> Clone for Meta<D> {
    /// The same bucket, client, channel, span and wait, for another task
    fn clone(&self) -> Self {
        Meta {
            ops: self.ops,
            client: self.client.clone(),
            shard_tx: self.shard_tx.clone(),
            span: self.span.clone(),
            wait: self.wait,
        }
    }
}

impl<D: ShoalDatabase> Meta<D> {
    /// Route one query through this shard as a bundle of its own and wait for its raw answer
    ///
    /// # Arguments
    ///
    /// * `query` - The query
    /// * `level` - The read level it is served at, if it reads
    /// * `bundle` - The bundle's identity, or a fresh one
    async fn ask_raw(
        &self,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        level: Option<ReadLevel>,
        bundle: Option<Uuid>,
    ) -> RawAnswer {
        // a bundle of one query, under the identity given or a fresh one of its own
        let mut queries = Queries::<D::ClientType>::default();
        if let Some(bundle) = bundle {
            queries.id = bundle;
        }
        let id = queries.id;
        queries.add_mut(query);
        let archived = match queries.serialize() {
            Ok(archived) => archived,
            Err(error) => {
                return RawAnswer::Failed(
                    ErrorCode::Internal,
                    format!("an object's query did not archive: {error}"),
                )
            }
        };
        // waited on before it is sent, so its answer cannot arrive first
        let answer = self.client.register(id);
        let options = level.map(|level| ReadOptions {
            level: Some(level),
            deadline_ms: 0,
            tokens: Vec::new(),
        });
        let msg = ServerMsg::Internal {
            client: self.client.id,
            span: self.span.clone(),
            body: Bytes::from_owner(archived),
            options,
        };
        if self.shard_tx.send(msg).await.is_err() {
            self.client.forget(&id);
            return RawAnswer::Failed(ErrorCode::Unavailable, "the shard is going away".to_string());
        }
        // a stripe commit, the one bundle sent under a derived identity, is in flight (F86)
        if bundle.is_some() {
            crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Proposed);
        }
        // the answer, or the end of the time one query may take
        let sleep = glommio::timer::sleep(self.wait);
        futures::pin_mut!(sleep);
        let reply = match futures::future::select(answer, sleep).await {
            Either::Left((Ok(reply), _)) => reply,
            Either::Left((Err(_), _)) => {
                return RawAnswer::Failed(
                    ErrorCode::Unavailable,
                    "the metadata client closed".to_string(),
                )
            }
            Either::Right(_) => {
                self.client.forget(&id);
                return RawAnswer::Failed(
                    ErrorCode::Timeout,
                    format!("an object's query took longer than {:?}", self.wait),
                );
            }
        };
        // a whole answer is the table's; a refusal is the machinery's
        match reply.kind {
            ReplyKind::Whole => RawAnswer::Whole(reply.archived),
            ReplyKind::Refused { code } => {
                RawAnswer::Failed(code, String::from_utf8_lossy(&reply.archived).into_owned())
            }
            _ => RawAnswer::Failed(ErrorCode::Internal, "an answer of another kind".to_string()),
        }
    }

    /// Route a query of the bucket's `ObjectMeta` table and read its answer
    ///
    /// # Arguments
    ///
    /// * `query` - The query
    /// * `level` - The read level it is served at, if it reads
    async fn ask(
        &self,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        level: Option<ReadLevel>,
    ) -> MetaAnswer {
        match self.ask_raw(query, level, None).await {
            RawAnswer::Whole(archived) => self.ops.answer_bytes(&archived),
            RawAnswer::Failed(code, why) => MetaAnswer::Failed(code, why),
        }
    }

    /// Route a query of the bucket's `StripeMeta` table and read its answer
    ///
    /// # Arguments
    ///
    /// * `query` - The query
    /// * `level` - The read level it is served at, if it reads
    /// * `bundle` - The bundle's identity: a stripe commit's derived one, or a fresh one
    async fn ask_stripes(
        &self,
        query: <D::ClientType as QuerySupport>::QueryKinds,
        level: Option<ReadLevel>,
        bundle: Option<Uuid>,
    ) -> StripeAnswer {
        match self.ask_raw(query, level, bundle).await {
            RawAnswer::Whole(archived) => self.ops.stripe_answer_bytes(&archived),
            RawAnswer::Failed(code, why) => StripeAnswer::Failed(code, why),
        }
    }

    /// Read the `ObjectMeta` row under a key, which holds at most one
    ///
    /// # Arguments
    ///
    /// * `key` - The row's key
    /// * `level` - The read level
    async fn row(
        &self,
        key: u64,
        level: Option<ReadLevel>,
    ) -> Result<Option<ObjectRow>, (ErrorCode, String)> {
        match self.ask(self.ops.get(key), level).await {
            MetaAnswer::Rows(mut rows) => Ok(rows.pop()),
            MetaAnswer::Failed(code, why) => Err((code, why)),
            MetaAnswer::Refused(_) | MetaAnswer::Written | MetaAnswer::Unexpected(_) => Err((
                ErrorCode::Internal,
                "a get of an object's row answered as a write".to_string(),
            )),
        }
    }

    /// Read the `StripeMeta` rows of some stripes at `Quorum`, by stripe
    ///
    /// Read through each row's group's leader, so a commit that follows finds its row resident
    /// ([X10](../../../../../docs/src/object-storage/stripe-row-costs.md)), and at least as new as
    /// the entry read before them ([S9](../../../../../docs/src/object-storage/read-path.md)).
    ///
    /// # Arguments
    ///
    /// * `consumer` - The bucket's consumer
    /// * `object` - The object
    /// * `stripes` - The stripes
    async fn stripe_rows(
        &self,
        consumer: u64,
        object: u128,
        stripes: &[u64],
    ) -> Result<BTreeMap<u64, StripeRow>, (ErrorCode, String)> {
        // no stripe, no query
        if stripes.is_empty() {
            return Ok(BTreeMap::new());
        }
        let keys = stripes.iter().map(|stripe| (consumer, object, *stripe)).collect();
        match self
            .ask_stripes(self.ops.stripe_get(keys), Some(ReadLevel::Quorum), None)
            .await
        {
            StripeAnswer::Rows(rows) => Ok(rows
                .into_iter()
                .filter(|row| row.consumer == consumer && row.object == object)
                .map(|row| (row.stripe, row))
                .collect()),
            StripeAnswer::Failed(code, why) => Err((code, why)),
            StripeAnswer::Refused(_) | StripeAnswer::Written | StripeAnswer::Unexpected(_) => Err((
                ErrorCode::Internal,
                "a get of an object's stripes answered as a write".to_string(),
            )),
        }
    }

    /// Commit a stripe's next row: over the row read if there was one, else where none is
    ///
    /// # Arguments
    ///
    /// * `next` - The row after the change
    /// * `read` - The sequence of the row read, or none when it had none
    /// * `bundle` - The commit's identity: a write's derived one, or a fresh one
    async fn commit_stripe(&self, next: StripeRow, read: Option<u64>, bundle: Option<Uuid>) -> StripeAnswer {
        let query = match read {
            Some(sequence) => self.ops.stripe_commit(next, sequence),
            None => self.ops.stripe_create(next),
        };
        self.ask_stripes(query, None, bundle).await
    }
}

/// Everything one object operation's task holds, and nothing of its shard
pub(crate) struct ObjectTask<D: ShoalDatabase>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// How it reads and commits metadata
    pub meta: Meta<D>,
    /// The operation
    pub op: ObjectOp,
    /// The bytes of a write assembled whole: an inline put's, a write in place's, an append's
    pub data: Option<Bytes>,
    /// A put past its pool's inline threshold, its bytes arriving as the client sends them
    pub stream: Option<AsyncReceiver<Vec<u8>>>,
    /// The assembly budget assembled bytes hold, given back when the task ends and this drops
    pub _hold: Option<Reservation>,
    /// The pool the bucket is bound to, by name
    pub pool_name: String,
    /// The pool
    pub pool: PoolSpec,
    /// The consumer the bucket is bound as
    pub consumer: u64,
    /// The map the operation places its stripes by
    pub map: Arc<TabletMap>,
    /// The node's object store, through which every stripe chunk is asked of its holder
    pub store: Option<ObjectStore>,
    /// How its waits and bounds are set
    pub timings: ObjectTimings,
    /// The connection's channel, which the answer goes down
    pub reply_tx: AsyncSender<Reply>,
    /// The shard's attempt at the operation, which a cancel is bounded by
    pub attempt: u64,
}

/// What an operation came to: an answer and a read's range, or a failure of the machinery
type Outcome = Result<(ObjectAnswer, Option<Vec<u8>>), (ErrorCode, String)>;

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Run the operation and answer it once
    pub async fn run(self) {
        // counted as running until this returns
        let _running = self.meta.client.start();
        // the operation
        let outcome = match self.op.kind {
            ObjectOpKind::Put => self.put().await,
            ObjectOpKind::Read => self.read(true).await,
            ObjectOpKind::Stat => self.read(false).await,
            ObjectOpKind::Delete => self.delete().await,
            ObjectOpKind::WriteAt | ObjectOpKind::Append => self.write().await,
            ObjectOpKind::Truncate => self.truncate().await,
        };
        // its answer, framed by the connection's writer
        let reply = match outcome {
            Ok((answer, range)) => self.answer(&answer, range.as_deref()),
            Err((code, message)) => self.failure(code, &message),
        };
        // a connection that has gone has nobody to answer
        let _ = self.reply_tx.send(reply).await;
    }

    /// The key of the operation's path's `ObjectMeta` row
    fn key(&self) -> u64 {
        crate::server::faults::path_key(&self.op.path)
    }

    /// The object store, or the refusal of an operation this node cannot do without one
    fn store(&self) -> Result<&ObjectStore, ObjectAnswer> {
        self.store.as_ref().ok_or_else(|| {
            ObjectAnswer::refused(
                self.op.id,
                self.op.kind,
                ObjectOutcome::TooLarge,
                crate::shared::protocol::object::too_large(self.op.length, self.pool.inline_threshold()),
            )
        })
    }

    /// Delete the object: drop its entry, and its row with it when it was the last one
    ///
    /// A delete of a path with no object is done: a retry of a delete whose answer was lost is
    /// the same delete. A deleted object's stripes are reclaimed by M20; its id is retired here.
    async fn delete(&self) -> Outcome {
        let op = &self.op;
        let key = self.key();
        for _ in 0..MAX_TRIES {
            // what the row holds now
            let Some(row) = self.meta.row(key, Some(DEFAULT_LEVEL)).await? else {
                return Ok((gone(op, None), None));
            };
            let Some(at) = row.position(&op.path) else {
                return Ok((gone(op, None), None));
            };
            let removed = row.entries[at].clone();
            // an entry whose objects have stripes stays, retired, until reclamation removes them;
            // an inline one goes with its row
            let has_stripes = removed.inline.is_none() && removed.object != 0
                || !removed.state.retired.is_empty()
                || removed.state.replacing.is_some();
            let query = if has_stripes {
                if !removed.state.live && removed.state.replacing.is_none() {
                    return Ok((gone(op, None), None));
                }
                let mut entries = row.entries.clone();
                let entry = &mut entries[at];
                if entry.state.live && entry.object != 0 {
                    entry.state.retired.push(entry.object);
                }
                if let Some(replacing) = entry.state.replacing.take() {
                    entry.state.retired.push(replacing);
                }
                entry.state.live = false;
                entry.object = 0;
                entry.content_version += 1;
                entry.modified_ms = now_ms();
                self.meta.ops.replace(
                    ObjectRow {
                        key,
                        version: row.version + 1,
                        entries,
                    },
                    row.version,
                )
            } else if row.entries.len() == 1 {
                self.meta.ops.remove(key, row.version)
            } else {
                let mut entries = row.entries.clone();
                entries.remove(at);
                self.meta.ops.replace(
                    ObjectRow {
                        key,
                        version: row.version + 1,
                        entries,
                    },
                    row.version,
                )
            };
            match self.meta.ask(query, None).await {
                MetaAnswer::Written => {
                    let removed = removed.state.live.then_some(&removed);
                    return Ok((gone(op, removed), None));
                }
                MetaAnswer::Refused(_) => continue,
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((
                        ErrorCode::Internal,
                        "a delete answered as a read".to_string(),
                    ))
                }
            }
        }
        Err(contended(&op.path))
    }

    /// The answer of an operation that is done, from the entry it leaves
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry
    /// * `checksum` - The checksum to answer with
    fn done(&self, entry: &ObjectEntry, checksum: u64) -> ObjectAnswer {
        ObjectAnswer {
            id: self.op.id,
            outcome: ObjectOutcome::Done,
            kind: self.op.kind,
            object: entry.object,
            content_version: entry.content_version,
            size: entry.size,
            offset: 0,
            length: 0,
            checksum,
            created_ms: entry.created_ms,
            modified_ms: entry.modified_ms,
            message: String::new(),
            attributes: None,
        }
    }

    /// An operation refused by name
    ///
    /// # Arguments
    ///
    /// * `outcome` - Why
    /// * `message` - What to say
    fn refused(&self, outcome: ObjectOutcome, message: String) -> ObjectAnswer {
        ObjectAnswer::refused(self.op.id, self.op.kind, outcome, message)
    }

    /// An answer as the connection's writer frames it: its body, then a read's range
    ///
    /// # Arguments
    ///
    /// * `answer` - The answer
    /// * `range` - A read's bytes
    fn answer(&self, answer: &ObjectAnswer, range: Option<&[u8]>) -> Reply {
        answer_reply(answer, range, self.attempt, &self.meta.span)
    }

    /// A failure of the machinery under the operation, which the writer frames as an error
    ///
    /// # Arguments
    ///
    /// * `code` - What class of failure
    /// * `message` - What to say
    fn failure(&self, code: ErrorCode, message: &str) -> Reply {
        failure_reply(self.op.id, code, message, self.attempt, &self.meta.span)
    }
}

/// An object answer as a connection's writer frames it: its body, then a read's range
///
/// # Arguments
///
/// * `answer` - The answer
/// * `range` - A read's bytes
/// * `attempt` - The shard's attempt at the operation
/// * `span` - The operation's span
pub(crate) fn answer_reply(
    answer: &ObjectAnswer,
    range: Option<&[u8]>,
    attempt: u64,
    span: &Span,
) -> Reply {
    // the body after the id, which the writer puts behind the frame's header and the id
    let body = match answer.encode_body() {
        Ok((_, body)) => body,
        Err(error) => {
            let message = format!("an answer did not encode: {error}");
            return failure_reply(answer.id, ErrorCode::Internal, &message, attempt, span);
        }
    };
    let range = range.unwrap_or(&[]);
    let mut archived = AlignedVec::with_capacity(body.len() + range.len());
    archived.extend_from_slice(&body);
    archived.extend_from_slice(range);
    reply(
        answer.id,
        ReplyKind::Object {
            head: body.len() as u32,
        },
        archived,
        attempt,
        span,
    )
}

/// A failure of the machinery under an operation, which a connection's writer frames as an error
///
/// # Arguments
///
/// * `id` - The operation's id
/// * `code` - What class of failure
/// * `message` - What to say
/// * `attempt` - The shard's attempt at the operation
/// * `span` - The operation's span
pub(crate) fn failure_reply(
    id: Uuid,
    code: ErrorCode,
    message: &str,
    attempt: u64,
    span: &Span,
) -> Reply {
    let mut archived = AlignedVec::with_capacity(message.len());
    archived.extend_from_slice(message.as_bytes());
    reply(id, ReplyKind::Refused { code }, archived, attempt, span)
}

/// A reply to an operation, under its id and the shard's attempt at it
///
/// # Arguments
///
/// * `id` - The operation's id
/// * `kind` - What it is
/// * `archived` - Its bytes
/// * `attempt` - The shard's attempt at the operation
/// * `span` - The operation's span
fn reply(id: Uuid, kind: ReplyKind, archived: AlignedVec, attempt: u64, span: &Span) -> Reply {
    Reply {
        id,
        index: 0,
        end: true,
        kind,
        span: span.clone(),
        stamps: StageStamps::new(Stamp::now()),
        archived,
        attempt,
        slot: 0,
        token: None,
        leader: None,
        op: None,
    }
}

/// An operation's answer when no object is at its path
///
/// # Arguments
///
/// * `op` - The operation
fn not_found(op: &ObjectOp) -> ObjectAnswer {
    ObjectAnswer::refused(
        op.id,
        op.kind,
        ObjectOutcome::NotFound,
        format!("no object is at {}", op.path),
    )
}

/// A delete's answer: done, naming the object it removed if there was one
///
/// # Arguments
///
/// * `op` - The delete
/// * `removed` - The entry it removed
fn gone(op: &ObjectOp, removed: Option<&ObjectEntry>) -> ObjectAnswer {
    let mut answer = ObjectAnswer::refused(op.id, op.kind, ObjectOutcome::Done, String::new());
    if let Some(removed) = removed {
        answer.object = removed.object;
        answer.content_version = removed.content_version;
        answer.size = removed.size;
        answer.created_ms = removed.created_ms;
        answer.modified_ms = removed.modified_ms;
    }
    answer
}

/// The failure of a write whose row moved under it every time it tried
///
/// # Arguments
///
/// * `path` - The object's path
fn contended(path: &str) -> (ErrorCode, String) {
    (
        ErrorCode::Shedding,
        format!("the row of {path} moved under this write {MAX_TRIES} times; try it again"),
    )
}

/// Milliseconds since the Unix epoch, for people only
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::{is_meta_client, meta_client_id};
    use std::collections::HashSet;
    use uuid::Uuid;

    /// A metadata client's id is told from a connection's, and every shard's has its own
    ///
    /// The transport view counts connections by leaving these out, so a connection read as one
    /// would go uncounted and one read as a connection would be one forever.
    #[test]
    fn a_meta_clients_id_is_never_a_connections() {
        // every shard's client is one, and no two share an id
        let clients: HashSet<Uuid> = (0..256).map(meta_client_id).collect();
        assert_eq!(clients.len(), 256);
        assert!(clients.iter().all(is_meta_client));
        // the same shard's client has the same id on every start
        assert_eq!(meta_client_id(3), meta_client_id(3));
        // and no id a connection or a bundle is given is one
        for _ in 0..10_000 {
            assert!(!is_meta_client(&Uuid::new_v4()));
            assert!(!is_meta_client(&Uuid::now_v7()));
        }
        assert!(!is_meta_client(&Uuid::nil()));
    }
}
