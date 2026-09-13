//! The wire framing shared between the client and the server in Shoal
//!
//! Every frame in either direction starts with the same 8 byte header:
//!
//! ```text
//!  ┌─────────┬─────────┬──────────┬────────────────────┐
//!  │ version │  type   │  flags   │   length (u32 LE)  │
//!  │  (1 B)  │  (1 B)  │  (2 B)   │       (4 B)        │
//!  └─────────┴─────────┴──────────┴────────────────────┘
//!  request  : [header][trace context 26 B]?[read options 16 B + tokens]?[rkyv Queries]
//!  response : [header][query id 16 B][session token 48 B]?[rkyv ResponseKinds]
//! ```
//!
//! A request frame's trace context is present only when [`Flags::TRACE_CONTEXT`] is set, which is
//! what makes the request preamble the one variable length preamble here
//! ([F35](../../../docs/src/features/wire-trace-context.md)). A read options section follows it
//! under [`Flags::READ_OPTIONS`], and a response frame carries a session token under
//! [`Flags::SESSION_TOKEN`] - both only between peers that negotiated them, since a section a
//! peer cannot size is not a section it can skip
//! ([F41](../../../docs/src/features/read-consistency.md)).
//!
//! # Invariants
//!
//! **The eight header bytes are fixed for all protocol versions.** The version byte is at offset
//! 0 and the length is a little endian `u32` at offsets 4..8, and neither ever moves. That is what
//! lets a peer read a frame of a version it does not speak, say which version it saw, and drain
//! exactly the right number of body bytes before replying. Without it the version byte is
//! decorative, because a peer that cannot parse the rest of the header cannot resynchronize.
//!
//! **`length` counts every byte after the header**, including a response frame's 16 byte query id
//! and a request frame's trace context. It is not the payload length. That is what lets a peer
//! skip a frame whose type it does not know, which is the only reason a type byte is worth
//! carrying.
//!
//! **Anything ahead of a payload is read separately from it.** A request frame's trace context and
//! a response frame's query id are read into their own buffers rather than into the front of the
//! payload's, because the payload is an rkyv archive accessed in place: a reader whose archive
//! starts 26 bytes into its allocation has every pointer in it misaligned. That is why
//! [`decode_request`] hands back a header and stops, and the trace context is a second read.
//!
//! **This module knows nothing about any async runtime.** The server reads with glommio and the
//! client reads with tokio, and the two share every decision here and none of the I/O, because
//! there is no I/O left to share — each call site is a `read_exact` of a fixed size array, one
//! pure call into this module, and a `read_exact` of the body. Its whole dependency list is
//! `core` and `uuid`, so it can move into a client only crate later without changing.

use uuid::Uuid;

pub mod admin;
pub mod auth;
pub mod error;
pub mod fingerprint;
pub mod handshake;
pub mod peer;
pub mod read;
pub mod trace;

#[cfg(test)]
mod tests;

use handshake::RefusalReason;
use trace::{TraceContext, TRACE_CONTEXT_LEN};

/// The version of the wire protocol this build speaks
///
/// Went to 2 when a get's answer started carrying the index of the partitions its rows came
/// from ([F27](../../../docs/src/features/grouped-responses.md)). That is a change to the
/// payload of one response variant rather than to any header field, so nothing about framing
/// moved — but a peer built before it reads `ArchivedGetRows` as `ArchivedVec`, which is a
/// pointer into the wrong place rather than an error, so the two must never speak. The version
/// byte is refused in [`RawHeader::validate`] before a frame is read, and this constant is also
/// mixed into every schema fingerprint, so a mismatch is a refused connection naming both sides
/// twice over.
///
/// Went to 3 when a request frame started being able to carry a W3C trace context between its
/// header and its payload ([F35](../../../docs/src/features/wire-trace-context.md)). Unlike the
/// 1 → 2 bump this *is* a framing change: a peer built before it reads the 26 context bytes as
/// the first 26 bytes of an rkyv archive. The flag bit is what makes every change **after** this
/// one cheaper - a peer that does not know a bit still round trips it - but the bit itself had to
/// be introduced to a peer that would understand a frame carrying it.
///
/// Went to 4 when a tablet group's log gained the scrub entry and the replication lane the
/// digest request ([F44](../../../docs/src/features/repair.md)). Neither is a framing change -
/// a scrub is a command whose tablet is the one no tablet can be, and a digest is a new
/// request kind - but a peer built before it would apply the entry as a write with no payload
/// and refuse the request as unknown, and the hello's exact match is what keeps such a peer
/// out of a group rather than in it, half understanding what it is sent.
pub const PROTOCOL_VERSION: u8 = 4;

/// The size of the frame header in bytes
pub const HEADER_LEN: usize = 8;

/// The size of the query id a response frame carries after its header
pub const QUERY_ID_LEN: usize = 16;

/// The number of bytes a server reads before it knows what else a request frame carries
///
/// This is the header alone, and it is deliberately *not* the whole preamble any more: a frame
/// with [`Flags::TRACE_CONTEXT`] set carries [`TRACE_CONTEXT_LEN`] more bytes after it, which are
/// read once the flags say they are there.
pub const REQUEST_PREAMBLE_LEN: usize = HEADER_LEN;

/// The largest a request preamble can be, with a trace context on it
pub const MAX_REQUEST_PREAMBLE_LEN: usize = HEADER_LEN + TRACE_CONTEXT_LEN;

/// The number of bytes a client reads before the payload of a response frame
pub const RESPONSE_PREAMBLE_LEN: usize = HEADER_LEN + QUERY_ID_LEN;

/// The largest frame either peer will accept unless it is told otherwise
///
/// This bound exists to be enforced before a length is used as an allocation size. A bundle of
/// 100 queries is a few kibibytes, so 64 mebibytes is several orders of magnitude of headroom
/// while still closing the hole where a peer could ask for a `u32::MAX` sized allocation.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

/// The kind of message a frame carries
///
/// The discriminants are explicit and start at 1. Two reasons, both of which a future change has
/// to keep: a zeroed buffer must never decode as a valid message type, and inserting a variant
/// must never silently renumber the wire. `every_message_type_round_trips_through_its_discriminant`
/// is the test that turns an insertion into a failure instead of a compatibility break.
///
/// `Hello`, `HelloAck`, `Auth`, `AuthResponse`, `Queries`, `Response` and `Error` are what a client
/// and a server exchange. Types 13 and above are the peer protocol
/// ([F38](../../../docs/src/features/inter-node-transport.md)), spoken only between nodes of one
/// cluster, and `Ping`/`Pong` gained a body there. `Topology` and the two `Admin` types are the
/// membership milestone's ([F39](../../../docs/src/features/membership.md)); `Replicate` and its
/// response are the replication milestone's ([F40](../../../docs/src/features/replication.md));
/// `GoAway`, `Cancel`
/// and `StatusReport` stay reserved so that the features that need them are a call site rather
/// than another flag day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    /// A client opening a connection, naming its version and its schema
    Hello = 1,
    /// A server accepting or refusing a `Hello`
    HelloAck = 2,
    /// A client proving who it is - reserved for authentication
    Auth = 3,
    /// A server's half of an authentication exchange - reserved
    AuthResponse = 4,
    /// A bundle of queries from a client
    Queries = 5,
    /// A single response to one query in a bundle
    Response = 6,
    /// A liveness check - reserved for the connection pool's health check
    Ping = 7,
    /// The answer to a `Ping` - reserved
    Pong = 8,
    /// The cluster's members, placement and policy
    ///
    /// From a client it is a subscription: an empty body under a query id, answered with the
    /// current topology under that id and followed by every later version under the nil id
    /// ([F39](../../../docs/src/features/membership.md)).
    Topology = 9,
    /// A failure with no query to attach it to - reserved for the error channel
    Error = 10,
    /// A server draining a connection before it closes it - reserved
    GoAway = 11,
    /// A client abandoning a query it will never read - reserved
    Cancel = 12,
    /// A node opening a peer connection, naming its cluster, its identity and the lane
    PeerHello = 13,
    /// A node accepting or refusing a `PeerHello`
    PeerHelloAck = 14,
    /// A bundle of queries forwarded to the node that owns some of its partitions
    Forward = 15,
    /// One query's answer, or one shard's share of it, going back to the node that forwarded it
    Forwarded = 16,
    /// A control plane request - a consensus RPC or a ping - with a correlation id
    ControlRequest = 17,
    /// The answer to a `ControlRequest`, under the same correlation id
    ControlResponse = 18,
    /// A node's bounded status report - reserved for the failure detector
    StatusReport = 19,
    /// The start of a snapshot stream on the bulk lane
    SnapshotBegin = 20,
    /// One checksummed chunk of a snapshot stream
    SnapshotChunk = 21,
    /// The end of a snapshot stream, with what the whole of it hashed to
    SnapshotEnd = 22,
    /// A client's administrative request - a topology read or a versioned cluster mutation
    Admin = 23,
    /// The answer to an `Admin` request, under the same id
    AdminResponse = 24,
    /// A data-plane consensus RPC or a proposal, on the replication lane, with a correlation id
    ///
    /// The frames a tablet group's members exchange
    /// ([F40](../../../docs/src/features/replication.md)): an append, a vote, a proposal to
    /// the group's leader, each under a group identity and the shard on the peer that hosts it.
    Replicate = 25,
    /// The answer to a `Replicate` request, under the same correlation id
    ReplicateResponse = 26,
}

impl MessageType {
    /// Get the byte this message type is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a message type from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a message type from
    #[inline]
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        // map each known discriminant back to its variant
        match raw {
            1 => Ok(MessageType::Hello),
            2 => Ok(MessageType::HelloAck),
            3 => Ok(MessageType::Auth),
            4 => Ok(MessageType::AuthResponse),
            5 => Ok(MessageType::Queries),
            6 => Ok(MessageType::Response),
            7 => Ok(MessageType::Ping),
            8 => Ok(MessageType::Pong),
            9 => Ok(MessageType::Topology),
            10 => Ok(MessageType::Error),
            11 => Ok(MessageType::GoAway),
            12 => Ok(MessageType::Cancel),
            13 => Ok(MessageType::PeerHello),
            14 => Ok(MessageType::PeerHelloAck),
            15 => Ok(MessageType::Forward),
            16 => Ok(MessageType::Forwarded),
            17 => Ok(MessageType::ControlRequest),
            18 => Ok(MessageType::ControlResponse),
            19 => Ok(MessageType::StatusReport),
            20 => Ok(MessageType::SnapshotBegin),
            21 => Ok(MessageType::SnapshotChunk),
            22 => Ok(MessageType::SnapshotEnd),
            23 => Ok(MessageType::Admin),
            24 => Ok(MessageType::AdminResponse),
            25 => Ok(MessageType::Replicate),
            26 => Ok(MessageType::ReplicateResponse),
            // anything else was written by a peer we do not understand, including a zeroed buffer
            unknown => Err(ProtocolError::UnknownMessageType(unknown)),
        }
    }

    /// Get the name of this message type
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            MessageType::Hello => "Hello",
            MessageType::HelloAck => "HelloAck",
            MessageType::Auth => "Auth",
            MessageType::AuthResponse => "AuthResponse",
            MessageType::Queries => "Queries",
            MessageType::Response => "Response",
            MessageType::Ping => "Ping",
            MessageType::Pong => "Pong",
            MessageType::Topology => "Topology",
            MessageType::Error => "Error",
            MessageType::GoAway => "GoAway",
            MessageType::Cancel => "Cancel",
            MessageType::PeerHello => "PeerHello",
            MessageType::PeerHelloAck => "PeerHelloAck",
            MessageType::Forward => "Forward",
            MessageType::Forwarded => "Forwarded",
            MessageType::ControlRequest => "ControlRequest",
            MessageType::ControlResponse => "ControlResponse",
            MessageType::StatusReport => "StatusReport",
            MessageType::SnapshotBegin => "SnapshotBegin",
            MessageType::SnapshotChunk => "SnapshotChunk",
            MessageType::SnapshotEnd => "SnapshotEnd",
            MessageType::Admin => "Admin",
            MessageType::AdminResponse => "AdminResponse",
            MessageType::Replicate => "Replicate",
            MessageType::ReplicateResponse => "ReplicateResponse",
        }
    }
}

impl std::fmt::Display for MessageType {
    /// Write this message type's name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// The two flag bytes a frame header carries
///
/// # Invariants
///
/// **Unknown bits are preserved, never rejected.** A peer that does not know what a bit means
/// must still round trip it, because that is the entire mechanism by which a bit can be spent
/// without bumping the version byte. A decoder that masked unknown bits off would turn the first
/// use of bit 4 into a compatibility break.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Flags(u16);

impl Flags {
    /// No flags are set
    pub const NONE: Flags = Flags(0);

    /// This frame's payload is an error rather than a result - reserved for the error channel
    pub const IS_ERROR: Flags = Flags(1 << 0);

    /// The client's view of the cluster topology is stale - reserved for shard aware routing
    pub const STALE_TOPOLOGY: Flags = Flags(1 << 1);

    /// This frame is the last one for its query - reserved
    pub const LAST: Flags = Flags(1 << 2);

    /// This frame refuses what the peer asked for, and the body says why
    pub const REFUSED: Flags = Flags(1 << 3);

    /// A W3C trace context sits between this request frame's header and its payload
    ///
    /// The first of the twelve free bits to be spent, and the reason the other eleven are worth
    /// having: a peer that does not know a bit round trips it rather than refusing it, so the
    /// *next* optional block costs no version byte.
    pub const TRACE_CONTEXT: Flags = Flags(1 << 4);

    /// A read options section sits between this request frame's trace context and its payload
    ///
    /// Unlike the trace context this section is not fixed length, so a peer that round trips
    /// the bit still cannot skip the bytes behind it. That is why it is sent only to a server
    /// whose hello ack granted [`read::CLIENT_CAP_READ_OPTIONS`]
    /// ([F41](../../../docs/src/features/read-consistency.md)).
    pub const READ_OPTIONS: Flags = Flags(1 << 5);

    /// A session token sits between this response frame's query id and its payload
    ///
    /// Written only to a connection whose hello asked for it, for the same reason.
    pub const SESSION_TOKEN: Flags = Flags(1 << 6);

    /// Build a flag set from its raw bits
    ///
    /// Unknown bits are kept as they are, since a bit this build does not know about is a bit a
    /// newer peer is using.
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw bits to build a flag set from
    #[inline]
    pub const fn from_bits(raw: u16) -> Self {
        Flags(raw)
    }

    /// Get the raw bits of this flag set
    #[inline]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Check if every bit in another flag set is set in this one
    ///
    /// # Arguments
    ///
    /// * `other` - The flags to check for
    #[inline]
    pub const fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }

    /// Combine two flag sets
    ///
    /// # Arguments
    ///
    /// * `other` - The flags to add to this set
    #[inline]
    pub const fn union(self, other: Flags) -> Self {
        Flags(self.0 | other.0)
    }
}

/// The things that can be wrong with a frame
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolError {
    /// The peer speaks a version of the protocol this build does not
    UnsupportedVersion {
        /// The version the peer sent
        got: u8,
        /// The version this build speaks
        ours: u8,
    },
    /// The peer sent a message type this build does not know
    UnknownMessageType(u8),
    /// The peer named an authentication mechanism this build does not know
    UnknownAuthMechanism(u8),
    /// The peer named an authentication status this build does not know
    UnknownAuthStatus(u8),
    /// The peer wrote a trace context in a version this build does not read
    UnknownTraceContextVersion(u8),
    /// The peer said a trace context followed and then wrote one that names no parent
    InvalidTraceContext,
    /// The peer wrote a read options section in a version this build does not read
    UnknownReadOptionsVersion(u8),
    /// The peer named a read level this build does not know
    UnknownReadLevel(u8),
    /// The peer wrote a session token in a version this build does not read
    UnknownSessionTokenVersion(u8),
    /// The peer put more tokens in one bundle than a section may carry
    TooManySessionTokens(usize),
    /// A read options section's fixed fields do not describe its bytes
    MalformedReadOptions(&'static str),
    /// The peer sent a valid message type, but not the one this frame had to be
    UnexpectedMessageType {
        /// The message type that had to be here
        expected: MessageType,
        /// The message type that was actually here
        got: MessageType,
    },
    /// The peer claimed a frame larger than we are willing to allocate for
    FrameTooLarge {
        /// The length the peer claimed
        len: u32,
        /// The largest frame we will accept
        max: u32,
    },
    /// A response frame claimed fewer bytes than its own query id takes
    BodyTooShort {
        /// The number of bytes this frame kind has to carry at a minimum
        need: usize,
        /// The number of bytes the peer claimed
        got: u32,
    },
    /// We tried to write a payload that will not fit in a frame
    PayloadTooLarge {
        /// The number of bytes we tried to write
        len: usize,
        /// The largest frame the peer will accept
        max: u32,
    },
    /// The peer was built from a different schema than this build
    SchemaMismatch {
        /// This build's schema fingerprint
        ours: u64,
        /// The peer's schema fingerprint
        theirs: u64,
    },
    /// The peer refused our handshake
    Refused {
        /// The reason the peer gave
        reason: RefusalReason,
    },
    /// A node refused our peer handshake
    PeerRefused {
        /// The reason it gave
        reason: peer::PeerRefusal,
    },
    /// A peer named a lane this build does not know
    UnknownLane(u8),
    /// A peer named a control request kind this build does not know
    UnknownControlKind(u8),
    /// A peer named a forwarded answer kind this build does not know
    UnknownForwardedKind(u8),
    /// A peer named a replication request kind this build does not know
    UnknownReplicateKind(u8),
    /// A replicated command's fixed fields do not describe its bytes
    ///
    /// Like [`ProtocolError::MalformedForward`], a sentence for a person: a command that does
    /// not decode is refused at the group and logged once.
    MalformedCommand(&'static str),
    /// A forwarded bundle's fixed fields do not describe its bytes
    ///
    /// Carries what was wrong in a sentence rather than a code, because every one of these ends
    /// the connection it arrived on and is logged once, and a person is the only reader.
    MalformedForward(&'static str),
    /// A snapshot chunk's bytes do not hash to what its header says
    SnapshotChecksum {
        /// What the chunk said it hashed to
        claimed: u32,
        /// What it hashed to
        computed: u32,
    },
}

impl std::fmt::Display for ProtocolError {
    /// Write a legible description of this protocol error
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::UnsupportedVersion { got, ours } => write!(
                f,
                "the peer speaks protocol version {got} but this build speaks {ours}"
            ),
            ProtocolError::UnknownMessageType(raw) => {
                write!(f, "the peer sent an unknown message type: {raw}")
            }
            ProtocolError::UnknownAuthMechanism(raw) => {
                write!(f, "the peer named an unknown authentication mechanism: {raw}")
            }
            ProtocolError::UnknownAuthStatus(raw) => {
                write!(f, "the peer named an unknown authentication status: {raw}")
            }
            ProtocolError::UnknownTraceContextVersion(raw) => {
                write!(f, "the peer wrote an unknown trace context version: {raw}")
            }
            ProtocolError::InvalidTraceContext => {
                write!(f, "the peer sent a trace context that names no parent")
            }
            ProtocolError::UnknownReadOptionsVersion(raw) => {
                write!(f, "the peer wrote an unknown read options version: {raw}")
            }
            ProtocolError::UnknownReadLevel(raw) => {
                write!(f, "the peer named an unknown read level: {raw}")
            }
            ProtocolError::UnknownSessionTokenVersion(raw) => {
                write!(f, "the peer wrote an unknown session token version: {raw}")
            }
            ProtocolError::TooManySessionTokens(count) => write!(
                f,
                "the peer put {count} session tokens in one bundle; at most {} are carried",
                read::MAX_SESSION_TOKENS
            ),
            ProtocolError::MalformedReadOptions(what) => {
                write!(f, "the peer sent a malformed read options section: {what}")
            }
            ProtocolError::UnexpectedMessageType { expected, got } => {
                write!(f, "expected a {expected} frame but got a {got} frame")
            }
            ProtocolError::FrameTooLarge { len, max } => write!(
                f,
                "the peer claimed a frame of {len} bytes but the largest we accept is {max}"
            ),
            ProtocolError::BodyTooShort { need, got } => write!(
                f,
                "this frame kind carries at least {need} bytes but the peer claimed {got}"
            ),
            ProtocolError::PayloadTooLarge { len, max } => write!(
                f,
                "tried to write {len} bytes but the peer accepts at most {max} per frame"
            ),
            ProtocolError::SchemaMismatch { ours, theirs } => write!(
                f,
                "the peer was built from a different schema: ours is {ours:#018x} and theirs is {theirs:#018x}"
            ),
            ProtocolError::Refused { reason } => {
                write!(f, "the peer refused our handshake: {reason}")
            }
            ProtocolError::PeerRefused { reason } => {
                write!(f, "the node refused our peer handshake: {reason}")
            }
            ProtocolError::UnknownLane(raw) => write!(f, "the peer named an unknown lane: {raw}"),
            ProtocolError::UnknownControlKind(raw) => {
                write!(f, "the peer named an unknown control request kind: {raw}")
            }
            ProtocolError::UnknownForwardedKind(raw) => {
                write!(f, "the peer named an unknown forwarded answer kind: {raw}")
            }
            ProtocolError::UnknownReplicateKind(raw) => {
                write!(f, "the peer named an unknown replication request kind: {raw}")
            }
            ProtocolError::MalformedCommand(what) => {
                write!(f, "a replicated command is malformed: {what}")
            }
            ProtocolError::MalformedForward(what) => {
                write!(f, "the peer sent a malformed forward: {what}")
            }
            ProtocolError::SnapshotChecksum { claimed, computed } => write!(
                f,
                "a snapshot chunk claimed checksum {claimed:#010x} but hashed to {computed:#010x}"
            ),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// A frame header exactly as it came off the wire, before any of it is judged
///
/// This exists so that a version we do not speak is still a header we can read. The layout of
/// these eight bytes is fixed for every protocol version, so a peer can always learn which
/// version it is talking to and how many body bytes to drain before it replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawHeader {
    /// The protocol version the peer wrote this frame with
    pub version: u8,
    /// The message type byte, which may not name a type this build knows
    pub kind: u8,
    /// The flags on this frame
    pub flags: Flags,
    /// The number of bytes after the header, which may be larger than we will accept
    pub len: u32,
}

impl RawHeader {
    /// Read the eight header bytes without judging any of them
    ///
    /// # Arguments
    ///
    /// * `raw` - The eight header bytes to read
    #[inline]
    pub const fn decode(raw: &[u8; HEADER_LEN]) -> Self {
        RawHeader {
            version: raw[0],
            kind: raw[1],
            flags: Flags::from_bits(u16::from_le_bytes([raw[2], raw[3]])),
            len: u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]),
        }
    }

    /// Check that this header is one we can act on
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The largest frame we are willing to allocate for
    #[inline]
    pub const fn validate(self, max_frame_bytes: u32) -> Result<Header, ProtocolError> {
        // refuse a version we do not speak before we try to make sense of anything else
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                got: self.version,
                ours: PROTOCOL_VERSION,
            });
        }
        // refuse a length we would not be willing to allocate for, before anything allocates
        if self.len > max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge {
                len: self.len,
                max: max_frame_bytes,
            });
        }
        // parse the message type, which is the last thing that can be wrong with a header
        match MessageType::from_byte(self.kind) {
            Ok(kind) => Ok(Header {
                version: self.version,
                kind,
                flags: self.flags,
                len: self.len,
            }),
            Err(error) => Err(error),
        }
    }
}

/// A frame header that this build knows how to act on
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The protocol version this frame was written with
    pub version: u8,
    /// The kind of message this frame carries
    pub kind: MessageType,
    /// The flags on this frame
    pub flags: Flags,
    /// The number of bytes after this header, including a response frame's query id
    pub len: u32,
}

impl Header {
    /// Build a header for a frame we are about to write
    ///
    /// This is fallible because a body length is a `usize` and the length field is a `u32`, so an
    /// unchecked cast would silently truncate on a 64 bit target and write a frame whose header
    /// disagrees with its own bytes. Every encode path has to go through here.
    ///
    /// # Arguments
    ///
    /// * `kind` - The kind of message this frame carries
    /// * `flags` - The flags to set on this frame
    /// * `body_len` - The number of bytes that will follow this header
    /// * `max_frame_bytes` - The largest frame the peer will accept
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    pub const fn new(
        kind: MessageType,
        flags: Flags,
        body_len: usize,
        max_frame_bytes: u32,
    ) -> Result<Self, ProtocolError> {
        // refuse a body the peer would not accept, which also catches anything past a u32
        if body_len > max_frame_bytes as usize {
            return Err(ProtocolError::PayloadTooLarge {
                len: body_len,
                max: max_frame_bytes,
            });
        }
        // the bound above is a u32, so a body that passed it cannot truncate here
        let len = body_len as u32;
        Ok(Header {
            version: PROTOCOL_VERSION,
            kind,
            flags,
            len,
        })
    }

    /// Write this header out as the eight bytes that go on the wire
    #[inline]
    pub const fn encode(&self) -> [u8; HEADER_LEN] {
        // split our two multi byte fields into their little endian bytes
        let flags = self.flags.bits().to_le_bytes();
        let len = self.len.to_le_bytes();
        [
            self.version,
            self.kind.as_byte(),
            flags[0],
            flags[1],
            len[0],
            len[1],
            len[2],
            len[3],
        ]
    }

    /// Read and check a header in one step
    ///
    /// # Arguments
    ///
    /// * `raw` - The eight header bytes to read
    /// * `max_frame_bytes` - The largest frame we are willing to allocate for
    #[inline]
    pub const fn decode(
        raw: &[u8; HEADER_LEN],
        max_frame_bytes: u32,
    ) -> Result<Self, ProtocolError> {
        RawHeader::decode(raw).validate(max_frame_bytes)
    }

    /// Check that this frame is the kind of message we were expecting
    ///
    /// # Arguments
    ///
    /// * `expected` - The message type this frame had to be
    #[inline]
    pub const fn expect(self, expected: MessageType) -> Result<Self, ProtocolError> {
        // a frame of the wrong type is a peer that is out of step with us, not a corrupt frame
        if self.kind as u8 != expected as u8 {
            return Err(ProtocolError::UnexpectedMessageType {
                expected,
                got: self.kind,
            });
        }
        Ok(self)
    }

    /// Get the number of bytes that follow this header
    #[inline]
    pub const fn body_len(&self) -> usize {
        self.len as usize
    }

    /// Get the number of bytes of this request frame's body that are a trace context
    ///
    /// Zero unless [`Flags::TRACE_CONTEXT`] is set, which is the only thing that puts anything
    /// between a request frame's header and its payload.
    #[inline]
    pub const fn trace_len(&self) -> usize {
        // the flag is the only thing that says a context is there
        if self.flags.contains(Flags::TRACE_CONTEXT) {
            TRACE_CONTEXT_LEN
        } else {
            0
        }
    }

    /// Whether a read options section follows this request frame's trace context
    ///
    /// The section is not fixed length, so unlike [`Header::trace_len`] this can only say that
    /// one is there: its head says how long it is, and is read separately
    /// ([F41](../../../docs/src/features/read-consistency.md)).
    #[inline]
    pub const fn has_read_options(&self) -> bool {
        self.flags.contains(Flags::READ_OPTIONS)
    }

    /// Get the number of payload bytes this request frame carries after its trace context
    ///
    /// This is only meaningful for a request frame, since it is the one frame kind whose preamble
    /// is variable length. A response frame's fixed fields are subtracted by
    /// [`decode_server_frame`] instead. A frame carrying a read options section has that taken
    /// off too, by [`Header::payload_len_after`], once its head has said how long it is.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::BodyTooShort`] when the peer set the trace flag and then claimed a
    /// body that cannot hold a trace context. This check is only possible because the length
    /// counts everything after the header rather than just the payload.
    #[inline]
    pub const fn request_payload_len(&self) -> Result<usize, ProtocolError> {
        self.payload_len_after(0)
    }

    /// Get the number of payload bytes after the trace context and this many more bytes
    ///
    /// # Arguments
    ///
    /// * `ahead` - How many bytes sit between the trace context and the payload
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::BodyTooShort`] when the body cannot hold what sits ahead of the
    /// payload.
    #[inline]
    pub const fn payload_len_after(&self, ahead: usize) -> Result<usize, ProtocolError> {
        // take off whatever sits between the header and the payload
        let ahead = self.trace_len() + ahead;
        match self.body_len().checked_sub(ahead) {
            Some(payload_len) => Ok(payload_len),
            None => Err(ProtocolError::BodyTooShort {
                need: ahead,
                got: self.len,
            }),
        }
    }
}

/// The routing fields every frame a server sends a client carries, whatever its type
///
/// A server writes two kinds of frame down a connection — a response and an error — and both put
/// their query id in the same place, so a client can read one fixed size preamble and only then
/// decide which it is holding. This is what that read decodes to, before its type is judged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerFrame {
    /// The header of this frame
    pub header: Header,
    /// The query this frame belongs to, or nil if it is about the connection itself
    pub query_id: Uuid,
    /// The number of body bytes after the query id
    pub rest_len: usize,
}

impl ServerFrame {
    /// How many of the bytes after the query id are a session token
    ///
    /// Zero unless [`Flags::SESSION_TOKEN`] is set, which only a response frame carries. The
    /// token is read into its own buffer ahead of the payload, for the reason the query id is
    /// ([F41](../../../docs/src/features/read-consistency.md)).
    #[inline]
    #[must_use]
    pub const fn token_len(&self) -> usize {
        if self.header.flags.contains(Flags::SESSION_TOKEN) {
            read::SESSION_TOKEN_LEN
        } else {
            0
        }
    }

    /// How many payload bytes follow the token, if there is one
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::BodyTooShort`] when the flag is set on a frame too short to hold
    /// a token.
    #[inline]
    pub const fn payload_len(&self) -> Result<usize, ProtocolError> {
        match self.rest_len.checked_sub(self.token_len()) {
            Some(payload_len) => Ok(payload_len),
            None => Err(ProtocolError::BodyTooShort {
                need: QUERY_ID_LEN + read::SESSION_TOKEN_LEN,
                got: self.header.len,
            }),
        }
    }
}

/// A response frame's header and the routing fields that follow it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseFrame {
    /// The header of this frame
    pub header: Header,
    /// The query this response belongs to
    pub query_id: Uuid,
    /// The number of payload bytes after the query id
    pub payload_len: usize,
}

/// Build the bytes that go ahead of a bundle of queries
///
/// The preamble is returned as a stack array rather than joined onto the payload so that the
/// header and the payload stay two separate buffers. That is what keeps the write vectored and
/// the peer's read of the payload landing at the start of its own allocation.
///
/// # Arguments
///
/// * `payload_len` - The number of archived bytes that will follow this preamble
/// * `max_frame_bytes` - The largest frame the server will accept
#[inline]
pub const fn request_preamble(
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; REQUEST_PREAMBLE_LEN], ProtocolError> {
    // a request frame carries nothing between its header and its payload
    match Header::new(
        MessageType::Queries,
        Flags::NONE,
        payload_len,
        max_frame_bytes,
    ) {
        Ok(header) => Ok(header.encode()),
        Err(error) => Err(error),
    }
}

/// Build the bytes that go ahead of a single response payload
///
/// # Arguments
///
/// * `query_id` - The query this response belongs to
/// * `payload_len` - The number of archived bytes that will follow this preamble
/// * `max_frame_bytes` - The largest frame the client will accept
#[inline]
pub fn response_preamble(
    query_id: &Uuid,
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; RESPONSE_PREAMBLE_LEN], ProtocolError> {
    server_preamble(MessageType::Response, Flags::NONE, query_id, payload_len, max_frame_bytes)
}

/// Build the header and query id that go ahead of any frame a server writes under a query id
///
/// A response, a topology push and an admin answer share one shape on the wire: the header,
/// sixteen bytes of id, then the payload. Only the kind differs, so the three are one function
/// ([F39](../../../../docs/src/features/membership.md)).
///
/// # Arguments
///
/// * `kind` - What kind of frame this is
/// * `flags` - The flags to set, which say what sits between the id and the payload
/// * `query_id` - The query this frame answers, or the nil id for a push nobody asked for
/// * `payload_len` - How many bytes follow the id, any session token included
/// * `max_frame_bytes` - The largest frame the receiver will accept
///
/// # Errors
///
/// Fails if the frame would be larger than the receiver accepts.
pub fn server_preamble(
    kind: MessageType,
    flags: Flags,
    query_id: &Uuid,
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; RESPONSE_PREAMBLE_LEN], ProtocolError> {
    // the query id is part of the frame body, so it counts towards the length
    let body_len = QUERY_ID_LEN.saturating_add(payload_len);
    let header = Header::new(kind, flags, body_len, max_frame_bytes)?;
    // lay the header down first and the query id after it
    let mut preamble = [0u8; RESPONSE_PREAMBLE_LEN];
    preamble[..HEADER_LEN].copy_from_slice(&header.encode());
    preamble[HEADER_LEN..].copy_from_slice(query_id.as_bytes());
    Ok(preamble)
}

/// Build the header that goes ahead of a client's topology subscription or admin request
///
/// The body it announces is a query id followed by the request's JSON, which
/// [`admin::encode_body`] produces.
///
/// # Arguments
///
/// * `kind` - `Topology` for a subscription, `Admin` for an operation
/// * `body_len` - How many bytes follow the header, id included
/// * `max_frame_bytes` - The largest frame the server will accept
///
/// # Errors
///
/// Fails if the frame would be larger than the server accepts.
pub const fn client_preamble(
    kind: MessageType,
    body_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; REQUEST_PREAMBLE_LEN], ProtocolError> {
    match Header::new(kind, Flags::NONE, body_len, max_frame_bytes) {
        Ok(header) => Ok(header.encode()),
        Err(error) => Err(error),
    }
}

/// The bytes that go ahead of a bundle of queries, however many of them there are
///
/// A request preamble is either eight bytes or thirty four, and this is one buffer rather than two
/// so that a caller's write stays two `IoSlice`s: the preamble, and the payload in an allocation
/// of its own. Building it as a stack array with a length rather than a `Vec` keeps the framing of
/// a bundle allocation free, which is what it has always been.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPreamble {
    /// The preamble bytes, of which only the first `len` are written
    bytes: [u8; MAX_REQUEST_PREAMBLE_LEN],
    /// How many of those bytes this preamble actually is
    len: usize,
}

impl RequestPreamble {
    /// Get the bytes to write ahead of this bundle's payload
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        // only the front of the buffer was filled, and the rest is never written
        self.bytes.split_at(self.len).0
    }

    /// Get the number of bytes this preamble is
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this preamble carries no bytes, which it never does
    ///
    /// Here because clippy asks for it beside a `len`. A preamble always carries at least a
    /// header, so this is always false.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Build the bytes that go ahead of a bundle of queries, with a trace context on them
///
/// The context is what joins the caller's spans to the ones the server opens answering this
/// bundle. Passing `None` writes exactly the eight bytes [`request_preamble`] writes, which is the
/// case for every caller that is not tracing - so a build that could set the flag and has nothing
/// to put in it costs nothing on the wire.
///
/// # Arguments
///
/// * `trace` - The trace context to carry, if this caller is in a trace
/// * `payload_len` - The number of archived bytes that will follow this preamble
/// * `max_frame_bytes` - The largest frame the server will accept
#[inline]
pub const fn request_preamble_traced(
    trace: Option<&TraceContext>,
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<RequestPreamble, ProtocolError> {
    // a bundle with no context to carry is framed exactly as it was before this existed
    let Some(trace) = trace else {
        return match request_preamble(payload_len, max_frame_bytes) {
            Ok(header) => Ok(RequestPreamble {
                bytes: pad_preamble(header),
                len: HEADER_LEN,
            }),
            Err(error) => Err(error),
        };
    };
    // the context is part of the frame body, so it counts towards the length
    let Some(body_len) = payload_len.checked_add(TRACE_CONTEXT_LEN) else {
        return Err(ProtocolError::PayloadTooLarge {
            len: payload_len,
            max: max_frame_bytes,
        });
    };
    // build the header, saying that a context follows it
    let header = match Header::new(
        MessageType::Queries,
        Flags::TRACE_CONTEXT,
        body_len,
        max_frame_bytes,
    ) {
        Ok(header) => header,
        Err(error) => return Err(error),
    };
    // lay the header down first and the context after it
    let mut bytes = pad_preamble(header.encode());
    let context = trace.encode();
    let mut index = 0;
    while index < TRACE_CONTEXT_LEN {
        bytes[HEADER_LEN + index] = context[index];
        index += 1;
    }
    Ok(RequestPreamble {
        bytes,
        len: MAX_REQUEST_PREAMBLE_LEN,
    })
}

/// The bytes ahead of a bundle's payload, with or without a read options section
///
/// A bundle with nothing to say about its reads is framed by [`RequestPreamble`] exactly as it
/// was before options existed, allocation free. One that carries a level, a deadline or tokens
/// has a section of variable length behind its header, which is what the second shape holds
/// ([F41](../../../docs/src/features/read-consistency.md)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestHead {
    /// The fixed preamble alone
    Fixed(RequestPreamble),
    /// The preamble with a read options section after it
    Extended(Vec<u8>),
}

impl RequestHead {
    /// Get the bytes to write ahead of this bundle's payload
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RequestHead::Fixed(preamble) => preamble.as_bytes(),
            RequestHead::Extended(bytes) => bytes,
        }
    }
}

/// Build the bytes that go ahead of a bundle of queries, with a trace context and read options
///
/// Passing no options, or empty ones, writes exactly what [`request_preamble_traced`] writes -
/// so a caller that has nothing to say about its reads pays nothing on the wire, and a peer
/// that never negotiated the section is never sent one.
///
/// # Arguments
///
/// * `trace` - The trace context to carry, if this caller is in a trace
/// * `options` - What the bundle says about its reads, if anything
/// * `payload_len` - The number of archived bytes that will follow this preamble
/// * `max_frame_bytes` - The largest frame the server will accept
///
/// # Errors
///
/// Fails if the frame would be larger than the server accepts, or the options carry more
/// tokens than a section may.
pub fn request_preamble_with(
    trace: Option<&TraceContext>,
    options: Option<&read::ReadOptions>,
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<RequestHead, ProtocolError> {
    // a bundle with no options is framed exactly as it was before they existed
    let Some(options) = options.filter(|options| !options.is_empty()) else {
        return request_preamble_traced(trace, payload_len, max_frame_bytes).map(RequestHead::Fixed);
    };
    // the section and the context are both part of the body, so both count towards the length
    let section = options.encode()?;
    let trace_len = if trace.is_some() { TRACE_CONTEXT_LEN } else { 0 };
    let body_len = payload_len
        .checked_add(trace_len)
        .and_then(|len| len.checked_add(section.len()))
        .ok_or(ProtocolError::PayloadTooLarge {
            len: payload_len,
            max: max_frame_bytes,
        })?;
    // build the header, saying what follows it
    let mut flags = Flags::READ_OPTIONS;
    if trace.is_some() {
        flags = flags.union(Flags::TRACE_CONTEXT);
    }
    let header = Header::new(MessageType::Queries, flags, body_len, max_frame_bytes)?;
    // lay the header down, then the context, then the section, in flag bit order
    let mut bytes = Vec::with_capacity(HEADER_LEN + trace_len + section.len());
    bytes.extend_from_slice(&header.encode());
    if let Some(trace) = trace {
        bytes.extend_from_slice(&trace.encode());
    }
    bytes.extend_from_slice(&section);
    Ok(RequestHead::Extended(bytes))
}

/// Widen an encoded header into the buffer a request preamble is held in
///
/// `RequestPreamble` is one fixed size array whichever shape it holds, so an untraced preamble is
/// a header followed by bytes that are never written. This exists because neither `copy_from_slice`
/// nor array concatenation is const.
///
/// # Arguments
///
/// * `header` - The encoded header to widen
#[inline]
const fn pad_preamble(header: [u8; HEADER_LEN]) -> [u8; MAX_REQUEST_PREAMBLE_LEN] {
    // copy the header into the front and leave the rest alone
    let mut bytes = [0u8; MAX_REQUEST_PREAMBLE_LEN];
    let mut index = 0;
    while index < HEADER_LEN {
        bytes[index] = header[index];
        index += 1;
    }
    bytes
}

/// Read the preamble of a bundle of queries
///
/// This stops at the header rather than going on to decode a trace context, because the two are
/// separate reads: the header is what says whether a context is there at all, and the payload
/// after it has to land at the start of its own allocation to be accessed in place. A caller reads
/// this, checks [`Header::trace_len`], and reads a [`TraceContext`] if there is one.
///
/// # Arguments
///
/// * `raw` - The preamble bytes to read
/// * `max_frame_bytes` - The largest frame we are willing to allocate for
#[inline]
pub const fn decode_request(
    raw: &[u8; REQUEST_PREAMBLE_LEN],
    max_frame_bytes: u32,
) -> Result<Header, ProtocolError> {
    // check the header, then check that this frame is a bundle of queries and not something else
    match Header::decode(raw, max_frame_bytes) {
        Ok(header) => header.expect(MessageType::Queries),
        Err(error) => Err(error),
    }
}

/// Decode the header of any frame a client may send once it is connected
///
/// A bundle of queries, a topology subscription or an admin request; anything else is refused
/// naming `Queries`, since that is what a connection is for
/// ([F39](../../../../docs/src/features/membership.md)).
///
/// # Arguments
///
/// * `raw` - The header bytes
/// * `max_frame_bytes` - The largest frame this server accepts
///
/// # Errors
///
/// Fails if the header is malformed, the frame too large, or the kind not one a client sends.
pub const fn decode_client_request(
    raw: &[u8; REQUEST_PREAMBLE_LEN],
    max_frame_bytes: u32,
) -> Result<Header, ProtocolError> {
    // check the header, then that the kind is one of the three a client sends
    match Header::decode(raw, max_frame_bytes) {
        Ok(header) => match header.kind {
            MessageType::Queries | MessageType::Topology | MessageType::Admin => Ok(header),
            _ => header.expect(MessageType::Queries),
        },
        Err(error) => Err(error),
    }
}

/// Read the preamble of any frame a server sends a client, without judging its type
///
/// A client reads the same fixed preamble for every frame that arrives and dispatches on the type
/// afterwards, so this stops one byte short of deciding what the frame is. Everything that is
/// wrong with a *header* is still an error here — a version we do not speak, a length past our
/// bound, a type byte that names nothing — because none of those depend on which type it turned
/// out to be.
///
/// # Arguments
///
/// * `raw` - The preamble bytes to read
/// * `max_frame_bytes` - The largest frame we are willing to allocate for
#[inline]
pub fn decode_server_frame(
    raw: &[u8; RESPONSE_PREAMBLE_LEN],
    max_frame_bytes: u32,
) -> Result<ServerFrame, ProtocolError> {
    // pull the header out of the front of the preamble and check it
    let mut header_bytes = [0u8; HEADER_LEN];
    header_bytes.copy_from_slice(&raw[..HEADER_LEN]);
    let header = Header::decode(&header_bytes, max_frame_bytes)?;
    // every frame a server sends carries a query id, so a shorter one cannot be one
    //
    // this check is only possible because the length counts everything after the header rather
    // than just the payload
    let rest_len = match header.body_len().checked_sub(QUERY_ID_LEN) {
        Some(rest_len) => rest_len,
        None => {
            return Err(ProtocolError::BodyTooShort {
                need: QUERY_ID_LEN,
                got: header.len,
            })
        }
    };
    // the query id sits between the header and whatever the rest of the body is
    let mut id_bytes = [0u8; QUERY_ID_LEN];
    id_bytes.copy_from_slice(&raw[HEADER_LEN..]);
    Ok(ServerFrame {
        header,
        query_id: Uuid::from_bytes(id_bytes),
        rest_len,
    })
}

/// Read the preamble of a single response
///
/// # Arguments
///
/// * `raw` - The preamble bytes to read
/// * `max_frame_bytes` - The largest frame we are willing to allocate for
#[inline]
pub fn decode_response(
    raw: &[u8; RESPONSE_PREAMBLE_LEN],
    max_frame_bytes: u32,
) -> Result<ResponseFrame, ProtocolError> {
    // read the fields every server frame has, then check that this one is a response
    let frame = decode_server_frame(raw, max_frame_bytes)?;
    let header = frame.header.expect(MessageType::Response)?;
    Ok(ResponseFrame {
        header,
        query_id: frame.query_id,
        // everything after a response frame's query id is its payload
        payload_len: frame.rest_len,
    })
}
