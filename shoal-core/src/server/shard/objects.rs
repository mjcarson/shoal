//! Object operations on a shard ([F80](../../../../docs/src/features/object-wire.md))
//!
//! At M13 every object a bucket holds is held inline, in its path's entry of the bucket's
//! `ObjectMeta` row ([S3](../../../../docs/src/object-storage/objects.md#small-objects-stay-inline)),
//! so an object operation is a few ordinary queries of that generated table: a get of the row,
//! then a conditional insert or delete of the whole row, judged at apply in committed order
//! ([F68](../../../../docs/src/features/conditional-writes.md)). This module runs them.
//!
//! - **The object lane.** Each shard registers one virtual client of its own with every shard,
//!   as a peer's data lane does, and a demultiplexer hands each answer it is sent to the task
//!   waiting on that bundle. An object task's queries are routed, replicated, forwarded and gathered
//!   exactly as a client's are - through `ServerMsg::Internal`, which enters routing past the front
//!   door that refuses every client's query of a generated table.
//! - **The object task.** One a operation, spawned on the shard that coordinates the connection,
//!   holding no borrow of it: the lane, the shard's own channel, the connection's channel and the
//!   bucket's `ObjectOps`. It answers the operation once, on the connection's channel.
//!
//! # Invariants
//!
//! **Every internal bundle has a fresh v7 identity, never the operation's.** A gather is keyed by
//! its bundle and index, and a tablet group remembers a write's result by its bundle and index: a
//! conditional write retried after a refusal under the same identity would be answered the first
//! refusal forever.
//!
//! **A put commits nothing it has not checked.** Its bytes are held to the CRC-64/NVME its head
//! named before the first query is sent.
//!
//! **A retried put is the same write.** Its object id is minted from its identity and its path
//! (`objects::object_id`), so a retry that finds its own id in the entry answers as the first try
//! did and commits nothing.
//!
//! **An answer read from the lane is validated** (`ObjectOps::answer_bytes`): a forwarded share's
//! bytes reach the lane as a peer wrote them.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::Either;
use kanal::{AsyncReceiver, AsyncSender};
use rkyv::util::AlignedVec;
use tracing::{event, Level, Span};
use uuid::Uuid;

use super::Reservation;
use crate::server::database::ShoalDatabase;
use crate::server::messages::{Reply, ReplyKind, ServerMsg};
use crate::server::stage_profile::{StageStamps, Stamp};
use crate::shared::checksum;
use crate::shared::objects::{
    object_id, MetaAnswer, ObjectEntry, ObjectOps, ObjectRow, ObjectState,
};
use crate::shared::pools::PoolSpec;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::object::{ObjectAnswer, ObjectOp, ObjectOpKind, ObjectOutcome};
use crate::shared::protocol::read::{ReadLevel, ReadOptions};
use crate::shared::queries::Queries;
use crate::shared::traits::{QuerySupport, RkyvSupport};

/// The most object operations one shard runs at once
///
/// A put's bytes are held against the shard's assembly budget until its task ends, which bounds
/// what puts hold; this bounds the rest, so a client pipelining reads cannot start more than this.
pub(crate) const MAX_OBJECT_TASKS: usize = 1024;

/// How many times a write reads its row again after its commit was refused before it gives up
///
/// A refusal means another write to a path of the same row committed in between; a write that
/// loses this many races in a row is answered `Shedding`, which a client retries.
const MAX_TRIES: usize = 16;

/// The read level an object's row is read at unless the operation names one
///
/// A row read at `One` can be a replica behind its leader, and a reader that trusted one could be
/// shown an object older than an acknowledged write ([S9](../../../../docs/src/object-storage/read-path.md)).
const DEFAULT_LEVEL: ReadLevel = ReadLevel::Quorum;

/// The bytes every object lane's id begins with, which no client's id can
///
/// A lane's id is a version 8 UUID of these bytes and its shard's number, so a shard can tell a
/// lane in its client map from a connection; a connection's id is a version 4 UUID, which never
/// is version 8.
const LANE_MARKER: [u8; 6] = *b"OBJLNE";

/// The id of a shard's object lane, the same on every start
///
/// # Arguments
///
/// * `shard` - The shard whose lane it is
#[must_use]
pub(crate) fn lane_id(shard: usize) -> Uuid {
    // the marker, then the shard's number, then the version and variant bits a v8 UUID carries
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&LANE_MARKER);
    bytes[8..16].copy_from_slice(&(shard as u64).to_be_bytes());
    bytes[6] = 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Whether a client id is an object lane's rather than a connection's
///
/// # Arguments
///
/// * `id` - The client id
#[must_use]
pub(crate) fn is_lane(id: &Uuid) -> bool {
    // a version 8 id that begins with the marker
    id.get_version_num() == 8 && id.as_bytes()[..6] == LANE_MARKER
}

/// A shard's virtual client: the answers to every bundle its object tasks send
#[derive(Clone)]
pub(crate) struct ObjectLane {
    /// The client id every shard knows the lane by
    pub id: Uuid,
    /// The channel answers are sent down, which every shard holds a clone of
    pub tx: AsyncSender<Reply>,
    /// The task waiting on each bundle, by its id
    waiters: Rc<RefCell<HashMap<Uuid, oneshot::Sender<Reply>>>>,
    /// How many object operations this shard is running
    inflight: Rc<Cell<usize>>,
}

impl ObjectLane {
    /// A lane, and the receiving end its demultiplexer reads
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard whose lane it is
    pub fn new(shard: usize) -> (Self, AsyncReceiver<Reply>) {
        // the channel every shard answers the lane on
        let (tx, rx) = kanal::unbounded_async();
        let lane = ObjectLane {
            id: lane_id(shard),
            tx,
            waiters: Rc::new(RefCell::new(HashMap::new())),
            inflight: Rc::new(Cell::new(0)),
        };
        (lane, rx)
    }

    /// Hand every answer the lane is sent to the task waiting on its bundle, until the lane closes
    ///
    /// # Arguments
    ///
    /// * `rx` - The lane's receiving end
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
                    event!(Level::DEBUG, msg = "an object lane answer nobody waits for", id = %reply.id);
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
}

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

/// Everything one object operation's task holds, and nothing of its shard
pub(crate) struct ObjectTask<D: ShoalDatabase>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// The bucket's queries
    pub ops: &'static dyn ObjectOps<D::ClientType>,
    /// The operation
    pub op: ObjectOp,
    /// A put's bytes
    pub data: Option<Bytes>,
    /// The assembly budget a put's bytes hold, given back when the task ends and this drops
    pub _hold: Option<Reservation>,
    /// The pool the bucket is bound to
    pub pool: PoolSpec,
    /// The shard's object lane
    pub lane: ObjectLane,
    /// The shard's own channel, which internal bundles are routed through
    pub shard_tx: AsyncSender<ServerMsg<D>>,
    /// The connection's channel, which the answer goes down
    pub reply_tx: AsyncSender<Reply>,
    /// The shard's attempt at the operation, which a cancel is bounded by
    pub attempt: u64,
    /// The operation's span
    pub span: Span,
    /// How long one internal query may take before it is given up
    pub wait: Duration,
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
        let _running = self.lane.start();
        // the operation
        let outcome = match self.op.kind {
            ObjectOpKind::Put => self.put().await,
            ObjectOpKind::Read => self.read(true).await,
            ObjectOpKind::Stat => self.read(false).await,
            ObjectOpKind::Delete => self.delete().await,
        };
        // its answer, framed by the connection's writer
        let reply = match outcome {
            Ok((answer, range)) => self.answer(&answer, range.as_deref()),
            Err((code, message)) => self.failure(code, &message),
        };
        // a connection that has gone has nobody to answer
        let _ = self.reply_tx.send(reply).await;
    }

    /// Put the whole object: check its bytes, then commit its entry, conditional on the row read
    async fn put(&self) -> Outcome {
        let op = &self.op;
        // the bytes, held to the checksum the head named before anything is sent
        let bytes = self.data.as_deref().unwrap_or(&[]);
        let crc = checksum::crc64(bytes);
        if crc != op.checksum {
            return Ok((
                ObjectAnswer::refused(
                    op.id,
                    op.kind,
                    ObjectOutcome::ChecksumMismatch,
                    format!(
                        "the put's {} bytes have CRC-64/NVME {crc:#018x}, not the {:#018x} its head named; nothing was committed",
                        bytes.len(),
                        op.checksum
                    ),
                ),
                None,
            ));
        }
        // the row its entry lives in, and the object this put makes, minted from the put itself
        let key = crate::server::faults::path_key(&op.path);
        let object = object_id(&op.id, &op.path);
        for _ in 0..MAX_TRIES {
            // what the row holds now
            let row = self.row(key, Some(DEFAULT_LEVEL)).await?;
            // a retry of a put that committed finds its own object and commits nothing
            if let Some(entry) = row.as_ref().and_then(|row| row.entry(&op.path)) {
                if entry.object == object && entry.state.live {
                    return Ok((self.done(entry, crc), None));
                }
            }
            // the entry this put makes, the row's other entries untouched
            let now = now_ms();
            let entry = ObjectEntry {
                path: op.path.clone(),
                object,
                content_version: 1,
                size: bytes.len() as u64,
                put_length: bytes.len() as u64,
                geometry: self.pool.geometry(),
                epoch: 0,
                floors: Vec::new(),
                state: ObjectState {
                    live: true,
                    replacing: None,
                    retired: Vec::new(),
                },
                created_ms: now,
                modified_ms: now,
                attributes: op.attributes.clone().unwrap_or_default(),
                inline: Some(bytes.to_vec()),
            };
            // commit the row with it, conditional on the row this put read
            let query = match &row {
                Some(row) => {
                    let mut next = ObjectRow {
                        key,
                        version: row.version + 1,
                        entries: row.entries.clone(),
                    };
                    match next.position(&op.path) {
                        Some(at) => next.entries[at] = entry.clone(),
                        None => next.entries.push(entry.clone()),
                    }
                    self.ops.replace(next, row.version)
                }
                None => self.ops.create(ObjectRow {
                    key,
                    version: 1,
                    entries: vec![entry.clone()],
                }),
            };
            match self.ask(query, None).await {
                MetaAnswer::Written => return Ok((self.done(&entry, crc), None)),
                // the row moved under this put: read it again
                MetaAnswer::Refused(_) => continue,
                // a commit whose outcome is unknown may have applied: the row says whether
                MetaAnswer::Failed(ErrorCode::OutcomeUnknown | ErrorCode::Timeout, why) => {
                    if let Some(row) = self.row(key, Some(DEFAULT_LEVEL)).await? {
                        if let Some(entry) = row.entry(&op.path) {
                            if entry.object == object && entry.state.live {
                                return Ok((self.done(entry, crc), None));
                            }
                        }
                    }
                    return Err((ErrorCode::OutcomeUnknown, why));
                }
                MetaAnswer::Failed(code, why) => return Err((code, why)),
                MetaAnswer::Rows(_) | MetaAnswer::Unexpected(_) => {
                    return Err((
                        ErrorCode::Internal,
                        "a write answered as a read".to_string(),
                    ))
                }
            }
        }
        Err(contended(&op.path))
    }

    /// Read a range of the object, or stat it
    ///
    /// # Arguments
    ///
    /// * `bytes` - Whether to answer with the range's bytes, which a stat does not
    async fn read(&self, bytes: bool) -> Outcome {
        let op = &self.op;
        // the path's entry, at the level the operation named or a strong read
        let key = crate::server::faults::path_key(&op.path);
        let row = self
            .row(key, Some(op.level.unwrap_or(DEFAULT_LEVEL)))
            .await?;
        let Some(entry) = row
            .as_ref()
            .and_then(|row| row.entry(&op.path))
            .filter(|entry| entry.state.live)
        else {
            return Ok((not_found(op), None));
        };
        // every object is held inline until a write reaches a pool's devices (M15); M14 gave them
        // devices, which no client byte reaches yet
        let Some(inline) = &entry.inline else {
            return Err((
                ErrorCode::Internal,
                format!(
                    "{} names no inline bytes and no pool holds stripes yet",
                    op.path
                ),
            ));
        };
        let mut answer = self.done(entry, 0);
        answer.kind = op.kind;
        if !bytes {
            // a stat says what the object is, and nothing of its bytes
            answer.attributes = Some(entry.attributes.clone());
            answer.checksum = checksum::crc64(inline);
            return Ok((answer, None));
        }
        // the range asked for, cut to the object: past the end reads nothing
        let start = op.offset.min(entry.size) as usize;
        let end = op.offset.saturating_add(op.length).min(entry.size) as usize;
        let range = &inline[start..end];
        answer.offset = start as u64;
        answer.length = range.len() as u64;
        answer.checksum = checksum::crc64(range);
        Ok((answer, Some(range.to_vec())))
    }

    /// Delete the object: drop its entry, and its row with it when it was the last one
    ///
    /// A delete of a path with no object is done: a retry of a delete whose answer was lost is
    /// the same delete.
    async fn delete(&self) -> Outcome {
        let op = &self.op;
        let key = crate::server::faults::path_key(&op.path);
        for _ in 0..MAX_TRIES {
            // what the row holds now
            let Some(row) = self.row(key, Some(DEFAULT_LEVEL)).await? else {
                return Ok((gone(op, None), None));
            };
            let Some(at) = row.position(&op.path) else {
                return Ok((gone(op, None), None));
            };
            let removed = row.entries[at].clone();
            // the row without it, or no row when it was the last entry
            let query = if row.entries.len() == 1 {
                self.ops.remove(key, row.version)
            } else {
                let mut entries = row.entries.clone();
                entries.remove(at);
                self.ops.replace(
                    ObjectRow {
                        key,
                        version: row.version + 1,
                        entries,
                    },
                    row.version,
                )
            };
            match self.ask(query, None).await {
                MetaAnswer::Written => return Ok((gone(op, Some(&removed)), None)),
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

    /// Read the row under a key, which holds at most one
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

    /// Route one query through this shard as a bundle of its own and wait for its answer
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
        // a bundle of one query, under a fresh identity of its own
        let mut bundle = Queries::<D::ClientType>::default();
        let id = bundle.id;
        bundle.add_mut(query);
        let archived = match bundle.serialize() {
            Ok(archived) => archived,
            Err(error) => {
                return MetaAnswer::Failed(
                    ErrorCode::Internal,
                    format!("an object's query did not archive: {error}"),
                )
            }
        };
        // waited on before it is sent, so its answer cannot arrive first
        let answer = self.lane.register(id);
        let options = level.map(|level| ReadOptions {
            level: Some(level),
            deadline_ms: 0,
            tokens: Vec::new(),
        });
        let msg = ServerMsg::Internal {
            client: self.lane.id,
            span: self.span.clone(),
            body: Bytes::from_owner(archived),
            options,
        };
        if self.shard_tx.send(msg).await.is_err() {
            self.lane.forget(&id);
            return MetaAnswer::Failed(
                ErrorCode::Unavailable,
                "the shard is going away".to_string(),
            );
        }
        // the answer, or the end of the time one query may take
        let sleep = glommio::timer::sleep(self.wait);
        futures::pin_mut!(sleep);
        let reply = match futures::future::select(answer, sleep).await {
            Either::Left((Ok(reply), _)) => reply,
            Either::Left((Err(_), _)) => {
                return MetaAnswer::Failed(
                    ErrorCode::Unavailable,
                    "the object lane closed".to_string(),
                )
            }
            Either::Right(_) => {
                self.lane.forget(&id);
                return MetaAnswer::Failed(
                    ErrorCode::Timeout,
                    format!("an object's query took longer than {:?}", self.wait),
                );
            }
        };
        // a whole answer is read, validated; a refusal is the machinery's
        match reply.kind {
            ReplyKind::Whole => self.ops.answer_bytes(&reply.archived),
            ReplyKind::Refused { code } => {
                MetaAnswer::Failed(code, String::from_utf8_lossy(&reply.archived).into_owned())
            }
            _ => MetaAnswer::Unexpected("an answer of another kind"),
        }
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

    /// An answer as the connection's writer frames it: its body, then a read's range
    ///
    /// # Arguments
    ///
    /// * `answer` - The answer
    /// * `range` - A read's bytes
    fn answer(&self, answer: &ObjectAnswer, range: Option<&[u8]>) -> Reply {
        answer_reply(answer, range, self.attempt, &self.span)
    }

    /// A failure of the machinery under the operation, which the writer frames as an error
    ///
    /// # Arguments
    ///
    /// * `code` - What class of failure
    /// * `message` - What to say
    fn failure(&self, code: ErrorCode, message: &str) -> Reply {
        failure_reply(self.op.id, code, message, self.attempt, &self.span)
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
    use super::{is_lane, lane_id};
    use std::collections::HashSet;
    use uuid::Uuid;

    /// A lane's id is told from a connection's, and every shard's lane has its own
    ///
    /// The transport view counts connections by leaving lanes out, so a connection read as a lane
    /// would go uncounted and a lane read as a connection would be one forever.
    #[test]
    fn a_lanes_id_is_never_a_connections() {
        // every shard's lane is a lane, and no two share an id
        let lanes: HashSet<Uuid> = (0..256).map(lane_id).collect();
        assert_eq!(lanes.len(), 256);
        assert!(lanes.iter().all(is_lane));
        // the same shard's lane has the same id on every start
        assert_eq!(lane_id(3), lane_id(3));
        // and no id a connection or a bundle is given is one
        for _ in 0..10_000 {
            assert!(!is_lane(&Uuid::new_v4()));
            assert!(!is_lane(&Uuid::now_v7()));
        }
        assert!(!is_lane(&Uuid::nil()));
    }
}
