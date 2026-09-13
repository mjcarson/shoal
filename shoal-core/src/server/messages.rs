//! The different messages that can be sent in shoal

use bytes::Bytes;
use glommio::io::ReadResult;
use kanal::AsyncSender;
use rkyv::util::AlignedVec;
use tracing::Span;
use uuid::Uuid;

use std::rc::Rc;

use super::request_body::RequestBody;
use super::shard::ShardContact;
use super::stage_profile::{StageStamps, Stamp};
use crate::shared::protocol::read::{ReadLevel, SessionToken};
use crate::shared::responses::{Response, ResponseError};
use crate::shared::row_ref::RowRef;
use crate::server::database::ShoalDatabase;
use crate::shared::traits::{QuerySupport};

/// How one query, or one share of it, is to be served as a read
///
/// Resolved once on the coordinator and carried unchanged to the shard that executes the
/// query, whether over the mesh or over a forward
/// ([F41](../../../docs/src/features/read-consistency.md)). A write carries one too, since the
/// deadline, the attempt and the slot are the bundle's and not the read's; its level and tokens
/// are ignored.
#[derive(Debug, Clone)]
pub struct ReadPlan {
    /// The level this read is served at
    pub level: ReadLevel,
    /// When the bundle stops waiting for an answer
    pub deadline: Stamp,
    /// The committed lower bounds this read has to be served past, on the tablets it names
    ///
    /// Shared rather than cloned: every share of a bundle's query holds the same tokens.
    pub tokens: Rc<[SessionToken]>,
    /// Which slot of the coordinator's gather this share fills, if the query was split
    pub slot: u16,
    /// Which attempt at the bundle this is, so a late share is told apart by identity
    pub attempt: u64,
    /// Whether the barrier and the token waits have already been served for this read
    ///
    /// Set once the wait task has posted its outcome, so a read parked on a disk load after
    /// its barrier never waits twice.
    pub ready: bool,
}

impl ReadPlan {
    /// A plan that waits on nothing: a `One` read with no tokens, at a deadline
    ///
    /// # Arguments
    ///
    /// * `deadline` - When the bundle stops waiting
    #[must_use]
    pub fn one(deadline: Stamp) -> Self {
        ReadPlan {
            level: ReadLevel::One,
            deadline,
            tokens: Rc::from(Vec::new()),
            slot: 0,
            attempt: 0,
            ready: false,
        }
    }

    /// Whether this read has a barrier or a token wait ahead of it
    #[must_use]
    pub fn needs_wait(&self) -> bool {
        self.level == ReadLevel::Quorum || !self.tokens.is_empty()
    }
}

/// The metadata about a query from a client
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// The id of the client this query came from
    pub client: Uuid,
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// Whether this is the last query in a query bundle
    pub end: bool,
    /// The shard collecting this queries responses if it was split across shards
    ///
    /// A query naming partitions on several shards is answered in pieces, and the
    /// client is owed exactly one response for it, so those pieces go back to the
    /// shard that split it instead of straight to the client. `None` means this query
    /// is answered by one shard alone and needs no collecting.
    pub gather: Option<ShardContact>,
    /// The span this query and everything it causes hangs off
    ///
    /// **This must never be an empty span.** `#[instrument(parent = &meta.span, ...)]` with an
    /// empty parent does not produce an orphan - `tracing` turns a `None` parent into
    /// `Attributes::new_root`, so every span downstream of it silently starts a *new trace*
    /// ([Resolved #89](../../../docs/src/appendix/resolved/fragmented-query-traces.md)). It is
    /// opened per query in `Coordinator::send_to_shard`, under the request span the socket read
    /// opened, and carried unchanged across every channel hop from there.
    pub span: Span,
    /// When this query reached each stage of its journey through shoal
    ///
    /// A zero sized type unless the `stage-profile` feature is on, which is what lets this
    /// ride along in the response tuples without a `#[cfg]` at every site that builds one.
    pub stamps: StageStamps,
    /// A partition this execution must answer without reading from disk
    ///
    /// Set only when a load for that partition failed, so the replay that failure releases
    /// answers from what is resident instead of asking for the same read that just failed.
    /// It has to travel with the query rather than sit on the table, because a failed load
    /// releases several queries at once and each of them consumes the exemption separately.
    ///
    /// This is server side state on a server side struct - it never reaches a client.
    pub skip_disk: Option<u64>,
    /// The failure this query must answer with instead of the rows it could not read
    ///
    /// Set when a read this query was parked on gave up. It travels with the query rather than
    /// being answered on the spot because the query may still be parked on *another* partition:
    /// answering here would put a second response at an index that already has one. It is
    /// applied where this query finally produces a response, which happens exactly once.
    pub failed: Option<ResponseError>,
    /// How this query is served as a read, and which attempt and slot it answers under
    pub read: ReadPlan,
}

impl QueryMetadata {
    /// Create a new query metadata object
    ///
    /// # Arguments
    ///
    /// * `client` - The id for this client
    /// * `id` - The id of this query
    /// * `index` - The index for this query in a bundle of queries
    /// * `end` - Whether this is the last query in a bundle or not
    /// * `gather` - The shard collecting this queries responses if it was split
    /// * `span` - The span this query and everything it causes hangs off
    /// * `stamps` - The stage timings for the bundle this query arrived in
    pub fn new(
        client: Uuid,
        id: Uuid,
        index: usize,
        end: bool,
        gather: Option<ShardContact>,
        span: Span,
        stamps: StageStamps,
    ) -> Self {
        QueryMetadata {
            client,
            id,
            index,
            end,
            gather,
            span,
            stamps,
            // a query starts out with no reason to skip a read, since only a load that has
            // already failed can give it one
            skip_disk: None,
            // and with nothing to report, since nothing has failed yet
            failed: None,
            // a plan that waits on nothing and never expires, until the coordinator sets one
            read: ReadPlan::one(Stamp::now().plus_nanos(u64::MAX / 4)),
        }
    }

    /// Create query metadata that is not being profiled
    ///
    /// Every query the server routes carries the stamps of the bundle it arrived in, so this
    /// is only for callers that build metadata outside that path — tests, and the replay of a
    /// query that was parked before the profile existed. The stamps it makes are based at
    /// now, so a record built from them measures from here rather than from the socket read.
    ///
    /// # Arguments
    ///
    /// * `client` - The id for this client
    /// * `id` - The id of this query
    /// * `index` - The index for this query in a bundle of queries
    /// * `end` - Whether this is the last query in a bundle or not
    /// * `gather` - The shard collecting this queries responses if it was split
    /// * `span` - The span this query and everything it causes hangs off
    pub fn untimed(
        client: Uuid,
        id: Uuid,
        index: usize,
        end: bool,
        gather: Option<ShardContact>,
        span: Span,
    ) -> Self {
        QueryMetadata::new(
            client,
            id,
            index,
            end,
            gather,
            span,
            StageStamps::new(Stamp::now()),
        )
    }
}
/// What a strong or session read waited on, for the shard's counters and the read's stamps
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadWaits {
    /// How many barriers were obtained, one per group the read touched
    pub barriers: u64,
    /// How many of them were asked of a leader elsewhere
    pub barrier_hops: u64,
    /// Nanoseconds spent obtaining barriers
    pub barrier_ns: u64,
    /// Nanoseconds spent waiting for this replica to apply through them, or past a token
    pub apply_ns: u64,
    /// Whether a token's lower bound was waited past
    pub session: bool,
}

/// How to turn a borrowed reply into the bytes a client is sent
///
/// A reply built out of rows the table is still holding has to be serialized before that borrow
/// ends, and only the generated response enum knows which variant the rows belong in. So the
/// derive hands one of these down into the table, which calls it while the rows are still where
/// it found them.
/// The lifetime is universal on purpose: the rows a reply borrows live only as long as the scan
/// that found them, and a sealer bound to one particular borrow could not be called with the one
/// the scan actually produces.
pub type SealReply<P> = for<'row> fn(
    Response<RowRef<'row, P>>,
) -> Result<AlignedVec<16>, rkyv::rancor::Error>;

/// What a query produced, and whether it is still a value or already bytes
///
/// A get whose partitions are all resident is answered out of the rows the shard already holds
/// rather than out of copies of them ([O2](../../../docs/src/appendix/optimizations.md)). Those
/// rows cannot outlive the scan that found them, so the table serializes the reply itself and
/// hands back the bytes. Every other answer is still a value, because it has somewhere else to
/// be first: a share has to be merged, and a parked get has to be picked back up.
#[derive(Debug)]
pub enum Answer<R> {
    /// Serialized where the rows lay, and never copied out of it
    ///
    /// The stamps that come with this already carry `exec_done` and `replied`, because the
    /// serialize those bracket happened inside the table rather than in `Shard::reply`.
    Sealed(AlignedVec<16>),
    /// An answer that is still a value, for a share to merge or a client to be answered with
    Open(R),
}


/// Whether an answer is whole or one shard's share of a split query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyKind {
    /// A whole answer, which a client relay frames as a response
    Whole,
    /// One shard's share of a query another node split, which a peer relay frames as a share
    Share,
    /// A topology frame for a subscribed client, at this map version
    ///
    /// The bytes are the frame's JSON and the id is nil, since a push answers no query. A client
    /// relay writes only the newest of the ones queued to it, since an older topology is worth
    /// nothing once a newer one exists ([F39](../../../docs/src/features/membership.md)).
    Topology {
        /// The map version the frame carries
        version: u64,
    },
    /// The answer to an admin request, under the id the client sent it with
    ///
    /// The bytes are the response's JSON.
    Admin,
}

/// An answer on its way to the relay that writes it
///
/// Every shard holds a channel of these to every connection's write relay, and whichever shard
/// answers a query hands its bytes down that channel. The index and the end flag travel beside
/// the bytes rather than inside them: a client relay never needs them, since a response frame
/// carries only the query id, but a peer relay frames an answer by bundle and index and cannot
/// read the index out of an archive it did not write without validating it
/// ([F38](../../../docs/src/features/inter-node-transport.md)).
#[derive(Debug)]
pub struct Reply {
    /// The bundle the answered query arrived in
    pub id: Uuid,
    /// The index the answer is owed under
    pub index: usize,
    /// Whether the answered query was the last of its stream
    pub end: bool,
    /// Whether this is a whole answer or a share
    pub kind: ReplyKind,
    /// The span the answer is written under
    pub span: Span,
    /// When the query reached each stage, and its index
    pub stamps: StageStamps,
    /// The answer, sealed
    pub archived: AlignedVec,
    /// The attempt at the bundle this answers, which a peer relay echoes on the answer head
    pub attempt: u64,
    /// The slot of the origin's gather a share fills, echoed the same way
    pub slot: u16,
    /// The session token a committed write minted, for a client that asked for one
    ///
    /// Written ahead of the payload by a relay whose connection negotiated the section, and
    /// dropped by one that did not ([F41](../../../docs/src/features/read-consistency.md)).
    pub token: Option<crate::shared::protocol::read::SessionToken>,
}

/// What a peer link learned, delivered into the shard that owns it
///
/// Built by the link task on the shard's own executor and sent over the shard's own channel,
/// so the shard handles it between two of its other messages like anything else.
#[derive(Debug)]
pub enum PeerEvent {
    /// A link event: up, down, or a frame the peer sent back
    Link(crate::server::peer::LinkEvent),
    /// The deadline sweep, run by a timer task on the shard
    Tick,
}

/// The messages that can be sent over of node local mesh
///
/// # Safety
///
/// The Partition variant must never be sent across threads. In order to
/// prevent that only loaders should ever create Partition variant. Loaders
/// should also refrain from have any channel other then to their local shard.
///
/// The same holds for every variant a tablet group sends its own shard - `Apply`, `Proposed`,
/// `Replication`, `GroupUp`, `GroupsDown`, `WalSealed`, `SegmentCompacted`,
/// `CheckpointWritten`, `ReplicationView` and `ReplicationVerb`: they carry responders, oneshots
/// and `Raft` handles that belong to one executor, and only a task on that shard's executor
/// ever builds one ([F40](../../../docs/src/features/replication.md)). `Comms::broadcast`
/// never sees them.
pub enum ServerMsg<D: ShoalDatabase>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Install a newer tablet map, pushed whole by the control plane
    ///
    /// A shard swaps its map and rebuilds its ring between two messages, so nothing ever routes
    /// against half a map; a version at or below the installed one is ignored
    /// ([F39](../../../docs/src/features/membership.md)).
    Map(std::sync::Arc<crate::server::map::TabletMap>),
    /// Tell this shard about a new client
    NewClient {
        /// This clients id
        client: Uuid,
        /// The channel to send responses for this client on
        client_tx: AsyncSender<Reply>,
    },
    /// Tell this shard a client has gone away, so its channel can be dropped
    ///
    /// Broadcast by the acceptor once a connection's read relay has ended, whether the client
    /// closed cleanly or sent something the relay refused. Before this existed every shard held
    /// every connection's channel for as long as the process ran
    /// ([Resolved #32](../../../docs/src/appendix/resolved/disconnected-client-cleanup.md)).
    ClientGone(Uuid),
    /// A client asked for the topology and every change to it
    ///
    /// Sent to the shard that accepted the connection alone, which answers with the current map's
    /// frame and pushes every newer one it installs. A peer relay never sends this, so a peer
    /// connection is never subscribed ([F39](../../../docs/src/features/membership.md)).
    Subscribe {
        /// The client
        client: Uuid,
    },
    /// A client sent an admin request over its connection
    ///
    /// Sent to the shard that accepted the connection alone. A read is answered by the control
    /// thread's applied state; a mutation is refused here unless the principal is one the
    /// committed policy names, and proposed through the control thread otherwise
    /// ([F39](../../../docs/src/features/membership.md)).
    Admin {
        /// The client
        client: Uuid,
        /// The id the client sent it under, which its answer carries
        id: Uuid,
        /// Who the connection authenticated as, if it did
        principal: Option<String>,
        /// What is asked
        request: crate::shared::protocol::admin::AdminRequest,
    },
    /// A bundle forwarded by another node, still in the buffer it arrived in
    ///
    /// The peer listener read it in three pieces and judged every length; what it could not
    /// judge - offsets against the bundle, the bundle's own bytes - the shard that receives this
    /// validates before anything is routed, since a process boundary trusts nothing it did not
    /// check itself ([F38](../../../docs/src/features/inter-node-transport.md)).
    Forward {
        /// The peer connection this arrived on, which is the client every answer goes to
        conn: Uuid,
        /// The node that forwarded it
        origin: crate::shared::identity::NodeId,
        /// The bundle's fixed fields
        preamble: crate::shared::protocol::peer::ForwardPreamble,
        /// Which queries of it this node answers, and on which shards
        entries: Vec<crate::shared::protocol::peer::ForwardEntry>,
        /// The bundle's bytes
        data: RequestBody,
        /// When the last byte of it came off the socket
        base: Stamp,
    },
    /// Something a peer link owned by this shard learned
    Peer(PeerEvent),
    /// Drive a snapshot stream of this many bytes at a peer, for the bounded-bytes test
    BulkProbe {
        /// The peer to stream at
        node: crate::shared::identity::NodeId,
        /// How many payload bytes to stream
        bytes: u64,
    },
    /// Report what this shard's peer links look like
    Transport(std::sync::mpsc::Sender<crate::server::peer::ShardTransportView>),
    /// A message from a client
    Client {
        /// This peers id
        peer: Uuid,
        /// The root span every span this bundle produces hangs off
        ///
        /// Opened by the relay when the last byte of the frame came off the socket, rather than
        /// here, so the trace starts where the request does. It is carried rather than entered:
        /// the relay task and the shard that handles this are different tasks on different
        /// threads, and `tracing`'s ambient span does not cross either boundary.
        span: Span,
        /// The raw data for our request
        ///
        /// This is a [`RequestBody`] rather than a bare buffer because the bytes in it are
        /// never written twice: the read that fills it is the only way one can be built, and
        /// that is what lets the relay hand a reader memory it has not zeroed first.
        data: RequestBody,
        /// When the last byte of this request came off the socket
        ///
        /// Every stage offset a query in this bundle records is measured from here, since
        /// this is the first moment the server knows the bundle exists.
        base: Stamp,
        /// What the bundle said about how its reads are served, if it said anything
        /// ([F41](../../../docs/src/features/read-consistency.md))
        options: Option<crate::shared::protocol::read::ReadOptions>,
    },
    /// A query to execute, still in the buffer it arrived in
    ///
    /// This carries bytes rather than a query because the coordinator never builds one: it
    /// validates the bundle, routes every query in it by the scalars in its archive, and hands
    /// each shard the buffer plus enough to find its own query inside it
    /// ([F26](../../../docs/src/features/archive-routed-requests.md)). The shard that executes
    /// the query is the shard that pays for deserializing it, which is what keeps a bundle of
    /// wide rows from being copied twice on the one core every request passes through.
    Query {
        /// The metadata about a query
        meta: QueryMetadata,
        /// The bundle this query arrived in, shared with every shard it was routed to
        ///
        /// A [`Bytes`] rather than a [`crate::server::request_body::RequestBody`] because a
        /// bundle naming partitions on several shards is held by all of them at once, and
        /// cloning one of these is a refcount rather than a copy of the bundle.
        body: Bytes,
        /// Which query in that bundle this is
        offset: usize,
        /// The partition keys this shard owns, when the query was narrowed to a subset
        ///
        /// `None` means answer the query as it stands, which is what every write needs - a
        /// write names its partition in a field the narrowing does not reach, so narrowing one
        /// would be both wrong and unnecessary.
        keys: Option<Vec<u64>>,
    },
    /// A query being run again, after the partition it was parked on was read
    ///
    /// This is the other way a query reaches [`crate::server::shard::Shard::handle_query`], and
    /// it carries a query rather than bytes because there is no longer a bundle to read one out
    /// of: the query was decoded when it first arrived, narrowed to the partition it blocked
    /// on, and parked there. Decoding it a second time is neither possible nor wanted, which is
    /// why this is a variant of its own rather than an arm of [`ServerMsg::Query`] - the
    /// difference between the two is exactly which costs have already been paid.
    ///
    /// A released query never leaves the shard that parked it.
    Released {
        /// The metadata about the query being run again
        meta: QueryMetadata,
        /// The query, as it was when it was parked
        query: <D::ClientType as QuerySupport>::QueryKinds,
    },
    /// One shards share of a query that was split across several shards
    ///
    /// This travels from the shard that executed a piece of a query back to the shard
    /// that split it, which merges the pieces and answers the client once.
    Gathered {
        /// The metadata for the query this is part of the answer to
        meta: QueryMetadata,
        /// This shards share of the answer
        response: <D::ClientType as QuerySupport>::ResponseKinds,
        /// Whether the share is a failure, which fails its slot at once
        /// ([F41](../../../docs/src/features/read-consistency.md))
        failed: bool,
    },
    /// A partition loaded from disk. This can never be sent across threads!
    Partition(LoadedPartitionKinds<D>),
    /// A partition that could not be loaded from disk
    ///
    /// A load completing is the only thing that drains a tables `blocked` map, so a load that
    /// fails has to say so rather than simply not arriving - otherwise every query parked on
    /// that partition waits for a message that will never be sent, and so does its client.
    ///
    /// The failure it carries is what the *client* is told, which is deliberately less than
    /// what is logged: the loader is where the context is richest and it logs the path and the
    /// errno, while this carries only a class and a line naming the table and partition. A
    /// pruned partition carries no failure at all, because a partition that really is gone is
    /// answered correctly by finding nothing.
    PartitionLoadFailed {
        /// The span of the read that gave up
        ///
        /// The queries this releases are linked to it rather than parented to it: a read serves
        /// every query parked on its partition, so it belongs to none of them alone.
        span: Span,
        /// The table the partition that could not be read belongs to
        table: D::TableNames,
        /// The partition that could not be read
        partition_id: u64,
        /// What the queries parked on it should answer with, if this was a failure at all
        error: Option<ResponseError>,
    },
    /// Some data has been flushed to storage
    ///
    /// This carries no position. The writer tracks its own durable watermark in
    /// state shared with its detached IO tasks, which is correct the instant an IO
    /// completes rather than whenever the shard happens to drain its channel. This
    /// message exists purely to wake the shard up so it runs `handle_flushed`.
    ///
    /// Its *arrival* is load-bearing: the shard only sweeps its tables for newly
    /// durable responses when one of these has landed, so every advance of a durable
    /// watermark has to be followed by one of these. See the invariants on
    /// `docs/src/features/flushed-sweep-gate.md` before adding a path that moves a
    /// watermark.
    DataFlushed,
    /// Mark some partitions as evictable
    MarkEvictable {
        generation: u64,
        table: D::TableNames,
        partitions: Vec<u64>,
    },
    /// Make this shard fail, for a test of what the cluster does about a dead shard
    ///
    /// The shard's loop returns an error, which the pool and the control plane both hear of
    /// exactly as they would for any other death ([F39](../../../docs/src/features/membership.md)).
    Fail,
    /// A batch of committed entries a tablet group's state machine hands the loop to apply
    ///
    /// Sent by the group's `GroupMachine` from openraft's state machine worker and never by
    /// anything else; the loop applies every entry in order to the table it names, answers its
    /// responder, and fires `done` once the batch is through
    /// ([F40](../../../docs/src/features/replication.md)). Never crosses a thread: it carries
    /// responders and a oneshot that belong to this shard's executor.
    Apply {
        /// The group
        group: crate::shared::identity::GroupId,
        /// The entries, each with the responder a proposer is waiting on, if one is
        entries: Vec<openraft::storage::EntryResponder<crate::server::replication::DataConfig>>,
        /// Fired once the batch is through
        done: futures_channel::oneshot::Sender<()>,
    },
    /// A proposal this shard made resolved, and this is what the group answered
    ///
    /// Posted by the task that awaited the group's `client_write`, so the loop answers the
    /// client between two of its other messages ([F40](../../../docs/src/features/replication.md)).
    Proposed {
        /// The metadata of the write
        meta: QueryMetadata,
        /// The table it named
        table: D::TableNames,
        /// The tablet its key hashed to, which its token names
        tablet: u16,
        /// The group it went through, if admission let it that far
        group: Option<crate::shared::identity::GroupId>,
        /// What the group answered, or why it could not
        outcome: crate::server::replication::proposal::ProposalOutcome,
        /// How many bytes were held pending for it
        bytes: usize,
    },
    /// A replication request a peer sent this shard over the replication lane
    ///
    /// The listener read the frame and judged its lengths; the shard hands it to the group the
    /// head names, or runs the proposal it carries, and writes the answer back through `reply`
    /// ([F40](../../../docs/src/features/replication.md)).
    Replication {
        /// The peer that sent it
        origin: crate::shared::identity::NodeId,
        /// The request's fixed fields
        head: crate::shared::protocol::peer::ReplicateRequestHead,
        /// Its payload
        payload: Vec<u8>,
        /// Where the answer goes: the connection's write relay
        reply: AsyncSender<crate::server::peer::ReplicateReply>,
    },
    /// A tablet group's `Raft` was built, from the task that built it
    ///
    /// Building one re-applies the checkpoint up to the committed log id on the building task,
    /// which posts `Apply` batches to this loop; so it is built on a task of its own and handed
    /// over here rather than awaited on the loop ([F40](../../../docs/src/features/replication.md)).
    GroupUp {
        /// The group
        group: crate::shared::identity::GroupId,
        /// Its handle, or why it could not be built
        raft: Result<
            openraft::Raft<
                crate::server::replication::DataConfig,
                crate::server::replication::GroupMachine<D>,
            >,
            String,
        >,
    },
    /// A read's barrier and token waits are done, from the task that waited on them
    ///
    /// Posted by the task `await_read_barrier` spawned, so the loop runs the read between two
    /// of its other messages and never awaits a `Raft` method itself
    /// ([F41](../../../docs/src/features/read-consistency.md)). Never crosses a thread.
    ReadReady {
        /// The read's metadata, its plan now marked ready
        meta: QueryMetadata,
        /// The read
        query: <D::ClientType as QuerySupport>::QueryKinds,
        /// The span to reply under
        span: Span,
        /// The metadata to answer with, if this is a share of a split query
        gathered_meta: Option<QueryMetadata>,
        /// What the waits came to: how long they took, or why the read cannot be served
        outcome: Result<ReadWaits, ResponseError>,
    },
    /// Every tablet group this shard hosts has shut down, from the task that stopped them
    GroupsDown,
    /// The WAL sealed a segment, so the loop can judge whether it is resolved
    WalSealed {
        /// The segment's generation
        generation: u64,
    },
    /// A table's compactor finished a WAL segment, so its groups' checkpoints can advance
    SegmentCompacted {
        /// The table
        table: D::TableNames,
        /// The segment's generation
        generation: u64,
    },
    /// A transfer wants a snapshot file of a group at or past a boundary
    ///
    /// Posted by a group's network from openraft's snapshot transmitter, which cannot touch the
    /// loop's state; the loop answers with the file it holds, or cuts one and answers once it
    /// is built ([F43](../../../docs/src/features/node-recovery.md)). Never crosses a thread.
    BuildSnapshot {
        /// The group
        group: crate::shared::identity::GroupId,
        /// Where the file goes
        reply: futures_channel::oneshot::Sender<Result<std::rc::Rc<crate::server::replication::BuiltSnapshot>, String>>,
    },
    /// A snapshot file was cut, by the compactor or by the loop's own task
    SnapshotBuilt {
        /// The group
        group: crate::shared::identity::GroupId,
        /// The file and its manifest, or why there is none
        outcome: Result<(std::path::PathBuf, crate::server::replication::SnapshotManifest), String>,
    },
    /// A received snapshot is to be installed, from the group's state machine
    ///
    /// Posted by `GroupMachine::install_snapshot` on openraft's worker task, or on the task
    /// building the group at open when a pending marker is past the checkpoint, and answered
    /// through `done` once the install is durable ([F43](../../../docs/src/features/node-recovery.md)).
    /// Never crosses a thread.
    InstallSnapshot {
        /// The group
        group: crate::shared::identity::GroupId,
        /// The verified file
        path: std::path::PathBuf,
        /// What it is
        manifest: crate::server::replication::SnapshotManifest,
        /// What openraft was told the snapshot is
        meta: openraft::type_config::alias::SnapshotMetaOf<crate::server::replication::DataConfig>,
        /// Fired once the install is durable, or with why it is not
        done: futures_channel::oneshot::Sender<Result<(), String>>,
    },
    /// The checkpoint file was written, so the groups it covers may snapshot at it
    CheckpointWritten {
        /// Which write this was, so a stale completion is ignored
        version: u64,
        /// Whether it landed
        outcome: Result<(), String>,
    },
    /// Report what this shard's tablet groups look like, for readiness and the fixture
    ReplicationView(std::sync::mpsc::Sender<crate::server::replication::report::ShardReplication>),
    /// Drive a replication verb, for the fixture
    ReplicationVerb {
        /// What to do
        verb: crate::server::replication::report::ReplicationVerb,
        /// Where the answer goes
        reply: std::sync::mpsc::Sender<Result<serde_json::Value, String>>,
    },
    /// Drive a read verb, for the fixture, on a standalone node or a cluster one
    /// ([F41](../../../docs/src/features/read-consistency.md))
    ReadVerb {
        /// What to do
        verb: crate::server::replication::report::ReadVerb,
        /// Where the answer goes
        reply: std::sync::mpsc::Sender<Result<serde_json::Value, String>>,
    },
    /// The hold on this shard's shares has run out, so send what it kept
    ReleaseHeld,
    /// Tell this shard to shutdown
    Shutdown,
}

impl<D: ShoalDatabase> Clone for ServerMsg<D> {
    fn clone(&self) -> Self {
        match self {
            ServerMsg::Map(map) => ServerMsg::Map(map.clone()),
            ServerMsg::Client {
                peer,
                span,
                data,
                base,
                options,
            } => ServerMsg::Client {
                peer: *peer,
                span: span.clone(),
                data: data.clone(),
                base: *base,
                options: options.clone(),
            },
            ServerMsg::NewClient { client, client_tx } => ServerMsg::NewClient {
                client: client.clone(),
                client_tx: client_tx.clone(),
            },
            ServerMsg::ClientGone(client) => ServerMsg::ClientGone(*client),
            // a forward is handed to the shard that accepted it and is never broadcast
            ServerMsg::Forward { .. } => {
                panic!("A forwarded bundle is only ever handed to the shard that accepted it")
            }
            // a link's events go to the shard that owns the link and nowhere else
            ServerMsg::Peer(_) => panic!("A peer event is only ever sent to the shard that owns the link"),
            ServerMsg::BulkProbe { node, bytes } => ServerMsg::BulkProbe {
                node: *node,
                bytes: *bytes,
            },
            // a view is asked of one shard, on a channel that answers once
            ServerMsg::Transport(_) => panic!("A transport view is asked of one shard"),
            ServerMsg::Query {
                meta,
                body,
                offset,
                keys,
            } => ServerMsg::Query {
                meta: meta.clone(),
                // a refcount on the bundle rather than a copy of it
                body: body.clone(),
                offset: *offset,
                keys: keys.clone(),
            },
            // a released query is replayed on the shard that parked it and is never
            // broadcast, so there is nothing that would ever ask us to duplicate one
            ServerMsg::Released { .. } => {
                panic!("A released query is only ever replayed on the shard that parked it")
            }
            // a gathered response travels to exactly one shard and is never broadcast,
            // so there is nothing that would ever ask us to duplicate one
            ServerMsg::Gathered { .. } => {
                panic!("A gathered response is only ever sent to one shard")
            }
            ServerMsg::Partition(loaded) => ServerMsg::Partition(loaded.clone()),
            ServerMsg::PartitionLoadFailed {
                span,
                table,
                partition_id,
                error,
            } => ServerMsg::PartitionLoadFailed {
                span: span.clone(),
                error: error.clone(),
                table: *table,
                partition_id: *partition_id,
            },
            ServerMsg::DataFlushed => ServerMsg::DataFlushed,
            ServerMsg::MarkEvictable {
                generation,
                table,
                partitions,
            } => ServerMsg::MarkEvictable {
                generation: *generation,
                table: *table,
                partitions: partitions.clone(),
            },
            // a failure is asked of one shard
            ServerMsg::Fail => panic!("A failure is asked of one shard"),
            // everything a tablet group sends its own shard stays on that shard
            ServerMsg::Apply { .. } => panic!("An apply batch is for the shard hosting the group"),
            ServerMsg::Proposed { .. } => panic!("A proposal's outcome is for the shard that proposed it"),
            ServerMsg::Replication { .. } => panic!("A replication request is for the shard the head names"),
            ServerMsg::GroupUp { .. } => panic!("A group handle is for the shard that built it"),
            ServerMsg::ReadReady { .. } => panic!("A ready read is for the shard that waited on it"),
            ServerMsg::GroupsDown => panic!("A groups-down notice is for one shard"),
            ServerMsg::WalSealed { .. } => panic!("A sealed segment is the writing shard's"),
            ServerMsg::SegmentCompacted { .. } => panic!("A compacted segment is the writing shard's"),
            ServerMsg::CheckpointWritten { .. } => panic!("A checkpoint write is the writing shard's"),
            ServerMsg::BuildSnapshot { .. } => panic!("A snapshot is built for the shard hosting the group"),
            ServerMsg::InstallSnapshot { .. } => panic!("A snapshot is installed on the shard hosting the group"),
            ServerMsg::SnapshotBuilt { .. } => panic!("A built snapshot is the cutting shard's"),
            ServerMsg::ReplicationView(_) => panic!("A replication view is asked of one shard"),
            ServerMsg::ReplicationVerb { .. } => panic!("A replication verb is for one shard"),
            ServerMsg::ReadVerb { .. } => panic!("A read verb is for one shard"),
            ServerMsg::ReleaseHeld => panic!("A release is for the shard that held"),
            // a subscription and an admin request go to the accepting shard alone
            ServerMsg::Subscribe { .. } => panic!("A subscription is for one shard"),
            ServerMsg::Admin { .. } => panic!("An admin request is for one shard"),
            ServerMsg::Shutdown => ServerMsg::Shutdown,
        }
    }
}

/// # Safety
///
/// The Partition variant should not be sent across threads ever, and neither should any of the
/// tablet group variants named above; every one of them is built on the shard it is sent to.
unsafe impl<D: ShoalDatabase> Send for ServerMsg<D>
where
    D: Send,
    D::TableNames: Send,
    <D::ClientType as QuerySupport>::QueryKinds: Send,
{
}

#[derive(Clone)]
pub struct LoadedPartition {
    /// The partition that is being read from disk
    pub partition_id: u64,
    /// The result for this read
    pub data: ReadResult,
    /// The span of the read that produced this
    ///
    /// A load releases every query parked on its partition, so the queries it unblocks are
    /// *linked* to this rather than parented to it - only the query that asked for the read is
    /// its child. See `PersistentTable::load_partition`.
    pub span: Span,
}

pub struct LoadedPartitionKinds<D: ShoalDatabase> {
    /// The table this partition is for
    pub table: D::TableNames,
    /// The partition that was loaded from storage
    pub loaded: LoadedPartition,
}

impl<D: ShoalDatabase> Clone for LoadedPartitionKinds<D> {
    fn clone(&self) -> Self {
        LoadedPartitionKinds {
            table: self.table.clone(),
            loaded: self.loaded.clone(),
        }
    }
}
