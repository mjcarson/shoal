//! The different messages that are used client side

use rkyv::util::AlignedVec;

use crate::shared::protocol::error::ErrorCode;

#[cfg(feature = "stage-profile")]
use crate::server::stage_profile::Stamp;

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
    /// having to remember to.
    Response(AlignedVec, ClientStamps),
    /// A failure the server sent for this query instead of a response
    ///
    /// This carries no index, unlike a response. It arrives on a frame attached to a query id,
    /// and a query id names a whole bundle rather than one query in it, so there is no position
    /// in the stream to place it at — it ends the stream wherever it lands.
    ServerError(ErrorCode, String, ClientStamps),
    /// A client side message to mark the end of a stream
    End(usize),
}
