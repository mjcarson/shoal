//! The wire framing shared between the client and the server in Shoal
//!
//! Every frame in either direction starts with the same 8 byte header:
//!
//! ```text
//!  ┌─────────┬─────────┬──────────┬────────────────────┐
//!  │ version │  type   │  flags   │   length (u32 LE)  │
//!  │  (1 B)  │  (1 B)  │  (2 B)   │       (4 B)        │
//!  └─────────┴─────────┴──────────┴────────────────────┘
//!  request  : [header][rkyv Queries]
//!  response : [header][query id 16 B][rkyv ResponseKinds]
//! ```
//!
//! # Invariants
//!
//! **The eight header bytes are fixed for all protocol versions.** The version byte is at offset
//! 0 and the length is a little endian `u32` at offsets 4..8, and neither ever moves. That is what
//! lets a peer read a frame of a version it does not speak, say which version it saw, and drain
//! exactly the right number of body bytes before replying. Without it the version byte is
//! decorative, because a peer that cannot parse the rest of the header cannot resynchronize.
//!
//! **`length` counts every byte after the header**, including a response frame's 16 byte query id.
//! It is not the payload length. That is what lets a peer skip a frame whose type it does not
//! know, which is the only reason a type byte is worth carrying.
//!
//! **This module knows nothing about any async runtime.** The server reads with glommio and the
//! client reads with tokio, and the two share every decision here and none of the I/O, because
//! there is no I/O left to share — each call site is a `read_exact` of a fixed size array, one
//! pure call into this module, and a `read_exact` of the body. Its whole dependency list is
//! `core` and `uuid`, so it can move into a client only crate later without changing.

use uuid::Uuid;

pub mod fingerprint;
pub mod handshake;

#[cfg(test)]
mod tests;

use handshake::RefusalReason;

/// The version of the wire protocol this build speaks
pub const PROTOCOL_VERSION: u8 = 1;

/// The size of the frame header in bytes
pub const HEADER_LEN: usize = 8;

/// The size of the query id a response frame carries after its header
pub const QUERY_ID_LEN: usize = 16;

/// The number of bytes a client reads before the body of a request frame
pub const REQUEST_PREAMBLE_LEN: usize = HEADER_LEN;

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
/// Only `Hello`, `HelloAck`, `Queries` and `Response` are constructed today. The other eight are
/// reserved so that the features that need them are a call site rather than another flag day.
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
    /// The shards in this cluster and what each one owns - reserved for shard aware routing
    Topology = 9,
    /// A failure with no query to attach it to - reserved for the error channel
    Error = 10,
    /// A server draining a connection before it closes it - reserved
    GoAway = 11,
    /// A client abandoning a query it will never read - reserved
    Cancel = 12,
}

impl MessageType {
    /// Get the byte this message type is written as
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a message type from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a message type from
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
            // anything else was written by a peer we do not understand, including a zeroed buffer
            unknown => Err(ProtocolError::UnknownMessageType(unknown)),
        }
    }

    /// Get the name of this message type
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

    /// Build a flag set from its raw bits
    ///
    /// Unknown bits are kept as they are, since a bit this build does not know about is a bit a
    /// newer peer is using.
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw bits to build a flag set from
    pub const fn from_bits(raw: u16) -> Self {
        Flags(raw)
    }

    /// Get the raw bits of this flag set
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Check if every bit in another flag set is set in this one
    ///
    /// # Arguments
    ///
    /// * `other` - The flags to check for
    pub const fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }

    /// Combine two flag sets
    ///
    /// # Arguments
    ///
    /// * `other` - The flags to add to this set
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
    pub const fn body_len(&self) -> usize {
        self.len as usize
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
pub fn response_preamble(
    query_id: &Uuid,
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; RESPONSE_PREAMBLE_LEN], ProtocolError> {
    // the query id is part of the frame body, so it counts towards the length
    let body_len = QUERY_ID_LEN.saturating_add(payload_len);
    let header = Header::new(
        MessageType::Response,
        Flags::NONE,
        body_len,
        max_frame_bytes,
    )?;
    // lay the header down first and the query id after it
    let mut preamble = [0u8; RESPONSE_PREAMBLE_LEN];
    preamble[..HEADER_LEN].copy_from_slice(&header.encode());
    preamble[HEADER_LEN..].copy_from_slice(query_id.as_bytes());
    Ok(preamble)
}

/// Read the preamble of a bundle of queries
///
/// # Arguments
///
/// * `raw` - The preamble bytes to read
/// * `max_frame_bytes` - The largest frame we are willing to allocate for
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

/// Read the preamble of a single response
///
/// # Arguments
///
/// * `raw` - The preamble bytes to read
/// * `max_frame_bytes` - The largest frame we are willing to allocate for
pub fn decode_response(
    raw: &[u8; RESPONSE_PREAMBLE_LEN],
    max_frame_bytes: u32,
) -> Result<ResponseFrame, ProtocolError> {
    // pull the header out of the front of the preamble and check it
    let mut header_bytes = [0u8; HEADER_LEN];
    header_bytes.copy_from_slice(&raw[..HEADER_LEN]);
    let header = Header::decode(&header_bytes, max_frame_bytes)?.expect(MessageType::Response)?;
    // a response frame always carries a query id, so a shorter one cannot be a response
    //
    // this check is only possible because the length counts everything after the header rather
    // than just the payload
    let payload_len = match header.body_len().checked_sub(QUERY_ID_LEN) {
        Some(payload_len) => payload_len,
        None => {
            return Err(ProtocolError::BodyTooShort {
                need: QUERY_ID_LEN,
                got: header.len,
            })
        }
    };
    // the query id sits between the header and the payload
    let mut id_bytes = [0u8; QUERY_ID_LEN];
    id_bytes.copy_from_slice(&raw[HEADER_LEN..]);
    Ok(ResponseFrame {
        header,
        query_id: Uuid::from_bytes(id_bytes),
        payload_len,
    })
}
