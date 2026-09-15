//! The different messages that are used client side

use rkyv::util::AlignedVec;

use std::time::Duration;

use shoal_proto::shared::protocol::error::ErrorCode;
use shoal_proto::shared::protocol::read::{ReadLevel, ReadOptions, SessionToken};
use uuid::Uuid;

/// What a caller says about how the reads in a bundle are served
///
/// Every field is optional and the default says nothing: a bundle sent with the default is
/// framed exactly as one sent before options existed, and inherits each table's policy and
/// the server's deadline. Set on one send with [`Shoal::send_with`], or on every send with
/// [`ShoalBuilder::read_options`] ([F41](../../../../docs/src/features/read-consistency.md)).
///
/// [`Shoal::send_with`]: crate::client::Shoal::send_with
/// [`ShoalBuilder::read_options`]: crate::client::ShoalBuilder::read_options
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SendOptions {
    /// The level every read in the bundle is served at, or none to inherit each table's policy
    pub read: Option<ReadLevel>,
    /// How long the whole bundle may take, if shorter than the server's own budget
    ///
    /// A server never grants a longer budget than its `networking.query_deadline`, and the
    /// wire carries whole milliseconds, so anything finer is rounded down.
    pub deadline: Option<Duration>,
    /// The tokens earlier writes handed back, so these reads are served past them
    ///
    /// At most sixteen; a send with more is refused before anything is written.
    pub tokens: Vec<SessionToken>,
    /// The bundle id to send under, so a re-send is the same request to the server
    ///
    /// A write's identity on the server is its bundle id and its index in the bundle, and a
    /// group answers a repeat of one it applied with the result it produced the first time.
    /// Pinning the id is what makes a re-send after an unknown outcome safe: the write happens
    /// once whatever the client saw ([F42](../../../../docs/src/features/primary-failover.md)).
    /// Never on the wire as a section; it is the bundle's own id.
    pub identity: Option<Uuid>,
    /// How long to keep re-sending the bundle while the server's answer says to try again
    ///
    /// `NotLeader`, `Unavailable`, `QuorumUnavailable`, `ConnectionLost`, `OutcomeUnknown` and
    /// `Timeout` are tried again under the same identity with a growing pause between tries,
    /// until one succeeds, another code comes back, or this budget runs out - and then the
    /// last answer is the caller's. Only a collected send retries; a stream never does.
    pub retry: Option<Duration>,
}

impl SendOptions {
    /// Options that say nothing, which is what every send without them uses
    #[must_use]
    pub fn new() -> Self {
        SendOptions::default()
    }

    /// Serve every read in the bundle at this level
    ///
    /// # Arguments
    ///
    /// * `level` - The level
    #[must_use]
    pub fn read(mut self, level: ReadLevel) -> Self {
        self.read = Some(level);
        self
    }

    /// Give the whole bundle this long
    ///
    /// # Arguments
    ///
    /// * `deadline` - The budget
    #[must_use]
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Serve the reads past this token's lower bound
    ///
    /// # Arguments
    ///
    /// * `token` - The token an earlier write handed back
    #[must_use]
    pub fn token(mut self, token: SessionToken) -> Self {
        self.tokens.push(token);
        self
    }

    /// Send the bundle under this id, so a re-send is the same request
    ///
    /// # Arguments
    ///
    /// * `identity` - The bundle id
    #[must_use]
    pub fn identity(mut self, identity: Uuid) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Keep re-sending the bundle under one identity for this long while the answer says to
    ///
    /// # Arguments
    ///
    /// * `within` - The budget for every try together
    #[must_use]
    pub fn retry(mut self, within: Duration) -> Self {
        self.retry = Some(within);
        self
    }

    /// Whether these options say anything at all
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read.is_none() && self.deadline.is_none() && self.tokens.is_empty()
    }

    /// The section these options go on the wire as
    #[must_use]
    pub fn to_wire(&self) -> ReadOptions {
        ReadOptions {
            level: self.read,
            // the wire carries milliseconds; a deadline under one is sent as one rather than
            // as zero, which would mean the server's default
            deadline_ms: self.deadline.map_or(0, |deadline| {
                u32::try_from(deadline.as_millis())
                    .unwrap_or(u32::MAX)
                    .max(1)
            }),
            tokens: self.tokens.clone(),
        }
    }
}

#[cfg(feature = "stage-profile")]
use shoal_proto::stamps::Stamp;

/// When a response arrived on the client side
///
/// The server's own record ends when it hands the response bytes to the socket. These are the
/// two stamps that close the loop past that point, and they are what separate "the server was
/// slow" from "the response sat in a channel waiting for a worker to pick it up".
///
/// A zero sized type unless the `stage-profile` feature is on, for the same reason
/// [`StageStamps`] is — this rides on every response, so it has to be free when nobody is
/// profiling.
#[cfg(feature = "stage-profile")]
#[derive(Debug, Clone, Copy)]
pub struct ClientStamps {
    /// When the last byte of this response came off the socket
    pub arrived: Stamp,
}

/// When a response arrived on the client side, which is not recorded in this build
#[cfg(not(feature = "stage-profile"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct ClientStamps;

// this rides on every response, so it is only acceptable if it really is free when off
#[cfg(not(feature = "stage-profile"))]
const _: () = assert!(
    std::mem::size_of::<ClientStamps>() == 0,
    "ClientStamps must be zero sized when the stage-profile feature is off"
);

impl ClientStamps {
    /// Record that a response has just arrived
    #[cfg(feature = "stage-profile")]
    #[must_use]
    pub fn arrived_now() -> Self {
        ClientStamps {
            arrived: Stamp::now(),
        }
    }

    /// Record that a response has arrived, which is not kept in this build
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    #[must_use]
    pub fn arrived_now() -> Self {
        ClientStamps
    }

    /// Get when this response came off the socket
    #[cfg(feature = "stage-profile")]
    #[must_use]
    pub fn arrived(&self) -> Stamp {
        self.arrived
    }
}

/// What sending one bundle of queries cost on the client side
///
/// Every stage in here is paid **once per batch** and shared by every query in it. A report
/// that charged them per query would be inventing per query costs out of a batch level one, so
/// they are labelled as batch level where they are reported.
///
/// A zero sized type unless the `stage-profile` feature is on, which is what lets
/// [`crate::client::ShoalQueryStream::send`] return one without changing what a caller that
/// ignores it pays.
#[cfg(feature = "stage-profile")]
#[derive(Debug, Clone, Copy)]
pub struct BatchStamps {
    /// When this bundle was handed to `send`
    pub entered: Stamp,
    /// When it finished being serialized
    pub serialized: Stamp,
    /// When a pooled connection was acquired for it
    ///
    /// The gap between this and `serialized` is time spent waiting on the connection pool,
    /// which is where client side backpressure shows up when queries are in flight.
    pub pooled: Stamp,
    /// When its last byte was handed to the socket
    pub written: Stamp,
}

/// What sending one bundle cost, which is not recorded in this build
#[cfg(not(feature = "stage-profile"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchStamps;

impl BatchStamps {
    /// Start timing a bundle that is about to be sent
    #[cfg(feature = "stage-profile")]
    #[must_use]
    pub fn entered_now() -> Self {
        // every later stage of this bundle is stamped over the top of this one
        let now = Stamp::now();
        BatchStamps {
            entered: now,
            serialized: now,
            pooled: now,
            written: now,
        }
    }

    /// Start timing a bundle, which is not recorded in this build
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    #[must_use]
    pub fn entered_now() -> Self {
        BatchStamps
    }

    /// Record that this bundle finished being serialized
    #[cfg(feature = "stage-profile")]
    pub fn mark_serialized(&mut self) {
        self.serialized = Stamp::now();
    }

    /// Record that this bundle finished being serialized, which does nothing in this build
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    pub fn mark_serialized(&mut self) {}

    /// Record that a pooled connection was acquired for this bundle
    #[cfg(feature = "stage-profile")]
    pub fn mark_pooled(&mut self) {
        self.pooled = Stamp::now();
    }

    /// Record that a connection was acquired, which does nothing in this build
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    pub fn mark_pooled(&mut self) {}

    /// Record that this bundle's last byte was handed to the socket
    #[cfg(feature = "stage-profile")]
    pub fn mark_written(&mut self) {
        self.written = Stamp::now();
    }

    /// Record that this bundle was written, which does nothing in this build
    #[cfg(not(feature = "stage-profile"))]
    #[inline(always)]
    pub fn mark_written(&mut self) {}
}

#[derive(Debug)]
pub enum ClientMsg {
    /// A response from the server
    ///
    /// The stamps travel with the bytes rather than beside them so that every path a response
    /// takes — ordered, unordered, and the reorder buffer's re-wrap — carries them without
    /// having to remember to. So does the session token a committed write's answer carries
    /// ([F41](../../../../docs/src/features/read-consistency.md)).
    Response(AlignedVec, ClientStamps, Option<SessionToken>),
    /// A failure the server sent for this query instead of a response
    ///
    /// This carries no index, unlike a response. It arrives on a frame attached to a query id,
    /// and a query id names a whole bundle rather than one query in it, so there is no position
    /// in the stream to place it at — it ends the stream wherever it lands.
    ServerError(ErrorCode, String, ClientStamps),
    /// The answer to an admin request, as the JSON the server wrote
    /// ([F39](../../../../docs/src/features/membership.md))
    Admin(Vec<u8>),
    /// A client side message to mark the end of a stream
    End(usize),
}
