//! A bucket's objects, from the client ([F80](../../../../docs/src/features/object-wire.md))
//!
//! A caller reaches a bucket by its marker - `client.bucket::<Posters>()` - and an object in it by
//! its path, and every operation is one `ObjectOp` frame, a put's bytes in the data frames after
//! it, and one `ObjectAnswer` back, a read's range in the data frames after that
//! ([S12](../../../../docs/src/object-storage/wire-and-client.md)).
//!
//! - **Connections set apart.** An object operation travels on a connection set apart for long
//!   streams, which a client keeps by default, and not on the pool small queries' answers share,
//!   since X11 found a small answer behind a 1 MiB frame waits for it
//!   ([F73](../../../../docs/src/features/bodies-across-frames.md)); only a client that keeps none
//!   apart uses that pool. On a client that routes, it goes on one to the node leading the row of
//!   its path's entry ([F74](../../../../docs/src/features/client-routing.md)).
//! - **Checksums.** A put names the CRC-64/NVME of its bytes in its head, and a read's answer names
//!   the CRC-64/NVME of its range, which is checked here as the bytes come in; a range that does
//!   not match is an error, never a value.
//! - **Retries.** An operation that fails in a way that may be retried is sent again under the same
//!   identity, which a put mints its object's id from, so a retried put is the same write.
//!
//! # Invariants
//!
//! **No object frame is written to a connection that was not granted objects.** A server that
//! receives one closes the connection, so this refuses first, by name.
//!
//! **A read hands back nothing it has not checked.**

use std::future::Future;
use std::io::{IoSlice, SeekFrom};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use shoal_proto::client::Errors;
use shoal_proto::shared::checksum;
use shoal_proto::shared::identity::{BucketId, TableId};
use shoal_proto::shared::objects::{path_key, Attributes, BucketRows, ObjectOps};
use shoal_proto::shared::protocol::error::ErrorCode;
use shoal_proto::shared::protocol::object::{
    ObjectAnswer, ObjectOp, ObjectOpKind, ObjectOutcome, CLIENT_CAP_OBJECTS,
};
use shoal_proto::shared::protocol::read::ReadLevel;
use shoal_proto::shared::protocol::stream;
use shoal_proto::shared::traits::QuerySupport;
use uuid::Uuid;

use super::messages::ClientMsg;
use super::{
    node_conn, outcome_unknown, receive_failed, retriable, routing, settle, Checked, Shoal,
    RETRY_BACKOFF_MAX, RETRY_BACKOFF_MIN,
};

/// How long an object operation is retried under its identity unless a caller says otherwise
///
/// Every object operation is the same operation when it is sent again under its identity, so it
/// is always safe to retry; this bounds how long a caller waits on a cluster that cannot answer.
pub const DEFAULT_OBJECT_RETRY: Duration = Duration::from_secs(30);

/// How many bytes an open file asks for at a time when its caller asks for fewer
const READ_AHEAD: u64 = 1 << 20;

/// What an object is, as the server answered for it
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectInfo {
    /// The object's id, minted by the put that created it and never reused
    pub object: u128,
    /// Moved by every change to the object's content; an ETag is derived from it and the id
    pub content_version: u64,
    /// The object's length in bytes
    pub size: u64,
    /// When the object was created, in milliseconds since the Unix epoch, for people only
    pub created_ms: u64,
    /// When the object was last changed, in milliseconds since the Unix epoch, for people only
    pub modified_ms: u64,
    /// The CRC-64/NVME of what the operation answered: a put's bytes, a read's range, a stat's
    /// whole object
    pub checksum: u64,
    /// What the object says about itself, on a stat
    pub attributes: Option<Attributes>,
}

impl ObjectInfo {
    /// What an answer says the object is
    ///
    /// # Arguments
    ///
    /// * `answer` - The answer
    fn of(answer: &ObjectAnswer) -> Self {
        ObjectInfo {
            object: answer.object,
            content_version: answer.content_version,
            size: answer.size,
            created_ms: answer.created_ms,
            modified_ms: answer.modified_ms,
            checksum: answer.checksum,
            attributes: answer.attributes.clone(),
        }
    }
}

/// What a put says beyond its bytes
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// What the object says about itself
    pub attributes: Option<Attributes>,
    /// The put's identity, a v7 UUID, so a caller retrying it itself sends the same write
    pub identity: Option<Uuid>,
    /// How long to retry the put under its identity, or the client's default
    pub retry: Option<Duration>,
}

impl PutOptions {
    /// Set what the object says about itself
    ///
    /// # Arguments
    ///
    /// * `attributes` - The attributes
    #[must_use]
    pub fn attributes(mut self, attributes: Attributes) -> Self {
        self.attributes = Some(attributes);
        self
    }

    /// Set the put's identity
    ///
    /// # Arguments
    ///
    /// * `identity` - A v7 UUID
    #[must_use]
    pub fn identity(mut self, identity: Uuid) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Set how long the put is retried
    ///
    /// # Arguments
    ///
    /// * `retry` - The budget; zero sends it once
    #[must_use]
    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = Some(retry);
        self
    }
}

/// The objects of one bucket, by its identity
///
/// What [`BucketHandle`] wraps, for a caller - a benchmark's driver - that holds a bucket's
/// identity and not its marker.
pub struct Objects<'a, S: QuerySupport> {
    /// The client the bucket is reached through
    client: &'a Shoal<S>,
    /// The bucket
    bucket: BucketId,
    /// The bucket's queries, which name the table and the hash its operations are routed by
    ops: Option<&'static dyn ObjectOps<S>>,
    /// How long an operation is retried under its identity
    retry: Duration,
}

impl<S: QuerySupport> Clone for Objects<'_, S> {
    /// The same bucket through the same client
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: QuerySupport> Copy for Objects<'_, S> {}

/// A handle to one bucket of a database, named by its marker
pub struct BucketHandle<'a, S: QuerySupport, M: BucketRows<Client = S>> {
    /// The bucket's objects
    objects: Objects<'a, S>,
    /// The bucket this handle names
    marker: PhantomData<M>,
}

impl<'a, S: QuerySupport, M: BucketRows<Client = S>> Deref for BucketHandle<'a, S, M> {
    type Target = Objects<'a, S>;

    /// The bucket's objects
    fn deref(&self) -> &Self::Target {
        &self.objects
    }
}

impl<S: QuerySupport> Shoal<S> {
    /// Reach a bucket the schema declares, by its marker
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let posters = client.bucket::<Posters>();
    /// posters.put("alien/one-sheet.png", &bytes).await?;
    /// let (info, bytes) = posters.get("alien/one-sheet.png").await?;
    /// ```
    #[must_use]
    pub fn bucket<M: BucketRows<Client = S>>(&self) -> BucketHandle<'_, S, M> {
        BucketHandle {
            objects: self.objects(M::id()),
            marker: PhantomData,
        }
    }

    /// Reach a bucket by its identity
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket's identity
    #[must_use]
    pub fn objects(&self, bucket: BucketId) -> Objects<'_, S> {
        Objects {
            client: self,
            bucket,
            ops: S::object_queries(bucket),
            retry: DEFAULT_OBJECT_RETRY,
        }
    }
}

impl<'a, S: QuerySupport> Objects<'a, S> {
    /// The bucket's identity
    #[must_use]
    pub fn id(&self) -> BucketId {
        self.bucket
    }

    /// Retry every operation through this handle for this long under its identity
    ///
    /// # Arguments
    ///
    /// * `retry` - The budget; zero sends each operation once
    #[must_use]
    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = retry;
        self
    }

    /// Create or replace an object from its bytes
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    /// * `bytes` - Every byte of it
    ///
    /// # Errors
    ///
    /// [`Errors::Object`] when the bucket refuses it by name - past its pool's inline threshold, a
    /// bad path - and a server's error when the machinery under it failed past every retry.
    pub async fn put(&self, path: &str, bytes: &[u8]) -> Result<ObjectInfo, Errors> {
        self.put_with(path, bytes, &PutOptions::default()).await
    }

    /// Create or replace an object from its bytes, with what it says about itself
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    /// * `bytes` - Every byte of it
    /// * `options` - Its attributes, its identity and its retry budget
    ///
    /// # Errors
    ///
    /// As [`Objects::put`].
    pub async fn put_with(
        &self,
        path: &str,
        bytes: &[u8],
        options: &PutOptions,
    ) -> Result<ObjectInfo, Errors> {
        // the put names the checksum of every byte it carries, so nothing changed on the way is
        // committed
        let identity = options.identity.unwrap_or_else(Uuid::now_v7);
        let mut op = ObjectOp::put(
            identity,
            self.bucket,
            path,
            bytes.len() as u64,
            checksum::crc64(bytes),
        );
        op.attributes = options.attributes.clone();
        let retry = options.retry.unwrap_or(self.retry);
        let (answer, _) = self.run(&op, bytes, retry).await?;
        Ok(ObjectInfo::of(&answer))
    }

    /// Read a whole object
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    ///
    /// # Errors
    ///
    /// [`Errors::Object`] with [`ObjectOutcome::NotFound`] when no object is at the path, or with
    /// [`ObjectOutcome::ChecksumMismatch`] when its bytes do not match the server's checksum.
    pub async fn get(&self, path: &str) -> Result<(ObjectInfo, Vec<u8>), Errors> {
        self.read_range(path, 0, u64::MAX, None).await
    }

    /// Read up to `length` bytes of an object from `offset`, at a read level
    ///
    /// A range past the object's end is cut to it; one that starts past it reads nothing.
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    /// * `offset` - Where the range starts
    /// * `length` - The most bytes wanted
    /// * `level` - The read level, or the server's default, a strong read
    ///
    /// # Errors
    ///
    /// As [`Objects::get`].
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        level: Option<ReadLevel>,
    ) -> Result<(ObjectInfo, Vec<u8>), Errors> {
        // a read of nothing asks the server nothing
        if length == 0 {
            let info = self.stat(path).await?;
            return Ok((info, Vec::new()));
        }
        let mut op = ObjectOp::read(Uuid::now_v7(), self.bucket, path, offset, length);
        op.level = level;
        let (answer, bytes) = self.run(&op, &[], self.retry).await?;
        Ok((ObjectInfo::of(&answer), bytes))
    }

    /// Read an object's bytes from an offset into a buffer
    ///
    /// Returns how many bytes were read, fewer than the buffer holds at the object's end.
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    /// * `offset` - Where to start
    /// * `buf` - Where the bytes go
    ///
    /// # Errors
    ///
    /// As [`Objects::get`].
    pub async fn read_at(&self, path: &str, offset: u64, buf: &mut [u8]) -> Result<usize, Errors> {
        let (_, bytes) = self
            .read_range(path, offset, buf.len() as u64, None)
            .await?;
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    /// Say what an object is, without its bytes
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    ///
    /// # Errors
    ///
    /// [`Errors::Object`] with [`ObjectOutcome::NotFound`] when no object is at the path.
    pub async fn stat(&self, path: &str) -> Result<ObjectInfo, Errors> {
        let op = ObjectOp::bare(Uuid::now_v7(), self.bucket, ObjectOpKind::Stat, path);
        let (answer, _) = self.run(&op, &[], self.retry).await?;
        Ok(ObjectInfo::of(&answer))
    }

    /// Remove an object
    ///
    /// Returns whether an object was at the path. A delete of a path with nothing at it is done:
    /// a retry of a delete whose answer was lost is the same delete.
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    ///
    /// # Errors
    ///
    /// A server's error when the machinery under it failed past every retry.
    pub async fn delete(&self, path: &str) -> Result<bool, Errors> {
        let op = ObjectOp::bare(Uuid::now_v7(), self.bucket, ObjectOpKind::Delete, path);
        let (answer, _) = self.run(&op, &[], self.retry).await?;
        Ok(answer.object != 0)
    }

    /// Open an object as a file that reads and seeks
    ///
    /// Its size is read once, here. A seek is arithmetic and sends nothing; a read asks for the
    /// range it wants, and a megabyte ahead when it wants less.
    ///
    /// # Arguments
    ///
    /// * `path` - The object's path
    ///
    /// # Errors
    ///
    /// As [`Objects::stat`].
    pub async fn open(&self, path: &str) -> Result<ObjectFile<'a, S>, Errors> {
        let info = self.stat(path).await?;
        Ok(ObjectFile {
            objects: *self,
            path: path.to_string(),
            info,
            position: 0,
            ahead: Vec::new(),
            ahead_at: 0,
            pending: None,
        })
    }

    /// Run an operation, retrying it under its identity while what failed it may be retried
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `body` - A put's bytes
    /// * `retry` - How long it may be retried
    async fn run(
        &self,
        op: &ObjectOp,
        body: &[u8],
        retry: Duration,
    ) -> Result<(ObjectAnswer, Vec<u8>), Errors> {
        let started = Instant::now();
        let mut pause = RETRY_BACKOFF_MIN;
        let mut unknown = false;
        loop {
            match self.once(op, body).await {
                Ok(done) => return judge(done),
                Err(error) => {
                    // the same operation again, while what failed it says to try again
                    if !retriable(&error) || started.elapsed() + pause >= retry {
                        return Err(settle(error, unknown));
                    }
                    unknown |= outcome_unknown(&error);
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(RETRY_BACKOFF_MAX);
                }
            }
        }
    }

    /// Send an operation once and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `body` - A put's bytes
    async fn once(&self, op: &ObjectOp, body: &[u8]) -> Result<(ObjectAnswer, Vec<u8>), Errors> {
        let client = self.client;
        // the answer comes back under the operation's own id, which a retry keeps
        let mut id = op.id;
        let (tx, rx) = client.track_response(&mut id, true)?;
        let outcome = async {
            let conn = self.connection(op).await?;
            // a connection not granted objects is sent no object frame: its server would close it
            if conn.caps & CLIENT_CAP_OBJECTS == 0 {
                return Err(Errors::Config(
                    "the server did not grant object operations on this connection: it predates \
                     them, or this client does not take streams"
                        .to_string(),
                ));
            }
            let max = client.peer_max_frame_bytes();
            // the operation's frame, then a put's bytes in data frames, all on this connection
            let head = op.encode(max)?;
            conn.shared.write(&mut [IoSlice::new(&head)]).await?;
            client.owe(&id, conn.id, 1, &tx);
            if op.streams() {
                let frame = stream::data_body(client.streams.request_frame_bytes, max);
                let mut pieces = stream::Splitter::new(op.length, frame);
                while let Some(piece) = pieces.next_piece() {
                    let preamble =
                        stream::data_preamble(&id, piece.offset, piece.len, piece.last, max)?;
                    let start = piece.offset as usize;
                    let payload = &body[start..start + piece.len];
                    conn.shared
                        .write(&mut [IoSlice::new(&preamble), IoSlice::new(payload)])
                        .await?;
                }
            }
            // a connection whose reader already stopped will never answer
            if client.dead_conns.pin().contains_key(&conn.id) {
                return Err(Errors::Server {
                    query_id: Some(id),
                    index: None,
                    code: ErrorCode::ConnectionLost,
                    msg: "the connection this operation was written to had already stopped"
                        .to_string(),
                });
            }
            match rx.recv().await.map_err(receive_failed)? {
                ClientMsg::Object(answer, bytes) => Ok((*answer, bytes)),
                ClientMsg::ServerError(code, msg, _) => Err(Errors::Server {
                    query_id: Some(id),
                    index: None,
                    code,
                    msg,
                }),
                ClientMsg::Response(..) | ClientMsg::Admin(_) | ClientMsg::End(_) => {
                    Err(Errors::Config(
                        "an object operation was answered with another kind".to_string(),
                    ))
                }
            }
        }
        .await;
        // whatever happened, this id is done and its channel pair goes back to the queue
        client.channel_map.pin().remove(&id);
        let _ = client.channel_queue_tx.send((tx, rx)).await;
        outcome
    }

    /// The connection an operation goes on: one set apart, to the node leading its row's group
    /// when the client routes, else through the endpoints
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    async fn connection(&self, op: &ObjectOp) -> Result<Checked, Errors> {
        let client = self.client;
        // the node its path's row is led from, when the client routes and knows the bucket
        if let (Some(router), Some(ops)) = (&client.router, self.ops) {
            let table = TableId::of(ops.object_table());
            let key = ops.route_key(path_key(&op.path));
            let leader_bound = op.kind.writes() || op.level != Some(ReadLevel::One);
            if let Some(node) = router.node_for(table, key, leader_bound) {
                let pool = &client.pool;
                if let Some(conn) =
                    node_conn(router, node, true, pool, client.bulk_pool.as_ref()).await
                {
                    return Ok(conn);
                }
            }
        }
        // else a connection set apart through the endpoints, or the shared pool if none is
        let chosen = client.bulk_pool.as_ref().unwrap_or(&client.pool);
        chosen.get_owned().await.map_err(routing::pool_failed)
    }
}

/// An answer as a caller sees it: done, or refused by name; a read's range checked
///
/// # Arguments
///
/// * `done` - The answer and a read's range
fn judge(done: (ObjectAnswer, Vec<u8>)) -> Result<(ObjectAnswer, Vec<u8>), Errors> {
    let (answer, bytes) = done;
    // refused by name
    if answer.outcome != ObjectOutcome::Done {
        return Err(Errors::Object {
            id: answer.id,
            outcome: answer.outcome,
            msg: answer.message,
        });
    }
    // a read's bytes hold the checksum the server answered with, or they are not handed back
    if answer.kind == ObjectOpKind::Read {
        let crc = checksum::crc64(&bytes);
        if crc != answer.checksum || bytes.len() as u64 != answer.length {
            return Err(Errors::Object {
                id: answer.id,
                outcome: ObjectOutcome::ChecksumMismatch,
                msg: format!(
                    "a read of {} bytes has CRC-64/NVME {crc:#018x}, not the {:#018x} of the {} bytes the server answered",
                    bytes.len(),
                    answer.checksum,
                    answer.length
                ),
            });
        }
    }
    Ok((answer, bytes))
}

/// A read of an object's range in flight, for an open file
type Pending<'a> = Pin<Box<dyn Future<Output = Result<(ObjectInfo, Vec<u8>), Errors>> + 'a>>;

/// An object opened as a file: it reads from where it is, and seeks without sending anything
pub struct ObjectFile<'a, S: QuerySupport> {
    /// The bucket's objects
    objects: Objects<'a, S>,
    /// The object's path
    path: String,
    /// What the object was when it was opened
    info: ObjectInfo,
    /// Where the next read starts
    position: u64,
    /// Bytes read ahead of the position
    ahead: Vec<u8>,
    /// Where those bytes start
    ahead_at: u64,
    /// A read in flight
    pending: Option<Pending<'a>>,
}

impl<S: QuerySupport> ObjectFile<'_, S> {
    /// What the object was when it was opened
    #[must_use]
    pub fn info(&self) -> &ObjectInfo {
        &self.info
    }

    /// Where the next read starts
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }
}

impl<S: QuerySupport> tokio::io::AsyncRead for ObjectFile<'_, S> {
    /// Read from the position: what was read ahead, else the next range
    ///
    /// # Arguments
    ///
    /// * `cx` - The task's context
    /// * `buf` - Where the bytes go
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            // what was read ahead and covers the position
            let end = this.ahead_at + this.ahead.len() as u64;
            if this.position >= this.ahead_at && this.position < end {
                let from = (this.position - this.ahead_at) as usize;
                let take = buf.remaining().min(this.ahead.len() - from);
                buf.put_slice(&this.ahead[from..from + take]);
                this.position += take as u64;
                return Poll::Ready(Ok(()));
            }
            // the end of the object reads nothing
            if this.position >= this.info.size || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            // the next range, a megabyte at least, asked for once
            if this.pending.is_none() {
                let objects = this.objects;
                let path = this.path.clone();
                let at = this.position;
                let len = (buf.remaining() as u64).max(READ_AHEAD);
                this.pending = Some(Box::pin(async move {
                    objects.read_range(&path, at, len, None).await
                }));
                this.ahead_at = at;
            }
            let pending = this.pending.as_mut().expect("a read in flight");
            match pending.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    this.pending = None;
                    match result {
                        Ok((_, bytes)) => {
                            // a range that came back empty is the object's end
                            if bytes.is_empty() {
                                this.ahead.clear();
                                return Poll::Ready(Ok(()));
                            }
                            this.ahead = bytes;
                        }
                        Err(error) => {
                            return Poll::Ready(Err(std::io::Error::other(error.to_string())))
                        }
                    }
                }
            }
        }
    }
}

impl<S: QuerySupport> tokio::io::AsyncSeek for ObjectFile<'_, S> {
    /// Move the position, which is arithmetic and sends nothing
    ///
    /// # Arguments
    ///
    /// * `position` - Where to
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        // from the start, the end as it was opened, or here
        let target = match position {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(delta) => this.info.size.checked_add_signed(delta),
            SeekFrom::Current(delta) => this.position.checked_add_signed(delta),
        };
        match target {
            Some(at) => {
                this.position = at;
                Ok(())
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a seek before the start of the object",
            )),
        }
    }

    /// The position the seek moved to
    ///
    /// # Arguments
    ///
    /// * `_cx` - The task's context
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
