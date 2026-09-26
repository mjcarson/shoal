//! The frame a server sends when a query failed, and the vocabulary of failures it names
//!
//! ```text
//!  ┌──────────────────┬──────────┬───────────┬─────────────────────┐
//!  │ query id (16 B)  │ code     │ reserved  │ message (UTF-8)     │
//!  │                  │ (u16 LE) │  (2 B)    │  len = rest of body │
//!  └──────────────────┴──────────┴───────────┴─────────────────────┘
//! ```
//!
//! # Invariants
//!
//! **The query id sits at the same offset a response frame's does.** That is what lets a client
//! read one fixed [`RESPONSE_PREAMBLE_LEN`] preamble for both kinds of frame and only then decide
//! which it is holding. Moving it would either cost the response path a third read or force the
//! preamble read to be per type, and the response path's two read structure is what keeps it zero
//! copy. Stated generally: **every frame a server sends a client carries a 16 byte query id after
//! its header, and a nil id means the frame is about the connection rather than about a query.**
//!
//! **The message type is authoritative and [`Flags::IS_ERROR`] is never the sole test.** The flag
//! is set on every frame built here, and it is redundant against the type byte on purpose — it
//! makes "is this a failure" one bit test that will still work when a `Response` frame carrying an
//! error payload sets it too. A reader that branched on the flag alone would decode an error body
//! as a response payload the moment that happens.
//!
//! **The discriminants of [`ErrorCode`] are never renumbered and never reused**, the same rule
//! [`MessageType`] follows, and for the same reason: they are on the wire.
//!
//! **This module holds no rkyv.** Like the handshake it is fixed bytes, which is what lets
//! [`ErrorCode`] be shared with the archived [`ResponseError`] payload without the protocol module
//! taking a dependency on the serialization format it exists to frame.
//!
//! [`RESPONSE_PREAMBLE_LEN`]: super::RESPONSE_PREAMBLE_LEN
//! [`ResponseError`]: crate::shared::responses::ResponseError

use std::borrow::Cow;

use uuid::Uuid;

use super::{Flags, Header, MessageType, ProtocolError, HEADER_LEN, QUERY_ID_LEN};

/// The fixed part of an error frame's body: its query id, its code, and two reserved bytes
pub const ERROR_BODY_MIN: usize = QUERY_ID_LEN + 4;

/// The number of bytes a peer reads before an error frame's message
pub const ERROR_PREAMBLE_LEN: usize = HEADER_LEN + ERROR_BODY_MIN;

/// The longest message an error frame will carry, in bytes
///
/// A failure is diagnostic text, not a payload. Bounding it separately from the frame bound is
/// what stops the error channel being an allocation channel: a peer that named a 60 mebibyte
/// message would be under `max_frame_bytes` and still have picked our allocation size for us.
pub const MAX_ERROR_MSG_LEN: usize = 4096;

/// What kind of failure a frame or a response is reporting
///
/// The discriminants are explicit, banded by decade with gaps so that a new code can be appended
/// inside its own family, and are never renumbered or reused.
/// `every_error_code_round_trips_through_its_discriminant` is the test that turns an insertion into
/// a failure rather than into a compatibility break.
///
/// Unlike [`MessageType`], zero is a legal value. It is `Unknown`, which is what a code this build
/// does not recognize reads back as, so a newer peer's failure is still legible as a message even
/// when its class is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    /// A failure whose class this build does not recognize
    Unknown = 0,
    /// A failure the server could not attribute to anything more specific
    Internal = 1,
    /// The copy of this data on disk could not be read
    StorageRead = 10,
    /// The archive holding this data is not on disk
    ArchiveMissing = 11,
    /// The archive holding this data is on disk and is not readable as an archive
    CorruptArchive = 12,
    /// A write could not be committed to this table's log, and was not applied
    ///
    /// A definite refusal: a commit fails before it stages a byte, and a table that is refused
    /// one leaves every row as it found it, so nothing of this write is in memory or on disk
    /// ([Resolved #16](../../../../docs/src/appendix/resolved/hot-path-panics.md)). A table whose
    /// intent log failed refuses every write this way until the server is restarted
    /// ([Resolved #122](../../../../docs/src/appendix/resolved/intent-log-failure.md)).
    StorageWrite = 13,
    /// This response is larger than the frame bound the connection agreed on
    ResponseTooLarge = 20,
    /// This request is larger than the frame bound the connection agreed on - reserved
    RequestTooLarge = 21,
    /// The request's identity is older than the group promises to remember
    ///
    /// A definite refusal, judged before anything is proposed: a time-ordered identity
    /// minted before the retry window, or before the newest identity the group has
    /// forgotten, is answered by name rather than applied as new - a retry that late might be
    /// the write's second effect ([F45](../../../../docs/src/features/replica-migration.md)).
    IdentityExpired = 22,
    /// The server is over capacity and did not run this query
    ///
    /// A definite refusal: nothing accepted the query, so a write behind it did not apply. Sent
    /// when a forward to another node would take its queue past its byte bound
    /// ([F38](../../../../docs/src/features/inter-node-transport.md)).
    Shedding = 30,
    /// This query did not complete within its deadline
    ///
    /// A gather that was still owed shares when the bundle's budget ran out, or a strong read
    /// whose barrier or application wait did not finish in time. Whether a write behind it
    /// applied is not what this says: a read has no write behind it, and a write is answered
    /// [`ErrorCode::OutcomeUnknown`] instead ([F41](../../../../docs/src/features/read-consistency.md)).
    Timeout = 31,
    /// This write was accepted and whether it applied is not known
    ///
    /// The forwarding node accepted the query and then lost the peer, or the peer never answered
    /// within the deadline. A write may or may not have applied. This is deliberately not
    /// [`ErrorCode::Shedding`], which says the query was refused before anything accepted it
    /// ([F38](../../../../docs/src/features/inter-node-transport.md)). A table also answers it
    /// for a write committed to an intent log that then failed before the write was durable,
    /// whose bytes may or may not have landed
    /// ([Resolved #122](../../../../docs/src/appendix/resolved/intent-log-failure.md)).
    OutcomeUnknown = 32,
    /// The connection this query was sent on ended before it was answered
    ConnectionLost = 40,
    /// The server is draining this connection - reserved for `GoAway`
    GoingAway = 41,
    /// No shard that could answer this query is reachable
    ///
    /// A definite refusal, like [`ErrorCode::Shedding`]: the frame carrying this query was never
    /// written to the peer that owns it, because the link went down with the frame still queued.
    Unavailable = 50,
    /// Fewer members are up than the write's consistency needs
    ///
    /// A definite refusal, judged at admission on the coordinator against the installed
    /// topology: a `Quorum` write needs a majority of the desired replicas to be up and an `All`
    /// write every one of them ([F39](../../../../docs/src/features/membership.md)). The message
    /// names how many are up and how many are needed.
    QuorumUnavailable = 51,
    /// A session token names another cluster, or was sent to a node in no cluster
    ///
    /// Refused by name rather than ignored: a lower bound from another history bounds nothing
    /// here ([F41](../../../../docs/src/features/read-consistency.md)).
    WrongCluster = 52,
    /// A session token names a group that does not serve its tablet on the replica asked
    ///
    /// The token's lineage is not the one this replica holds, so its index means nothing to
    /// the replica's log ([F41](../../../../docs/src/features/read-consistency.md)).
    UnknownLineage = 53,
    /// The bundle asked for a read level this server does not serve
    UnsupportedReadLevel = 54,
    /// The node asked no longer serves this tablet, and answered from a newer map
    ///
    /// A definite refusal: the query was routed by a map older than the configuration the
    /// tablet now lives under - to a source whose copy retired, or to a node that never held
    /// it - and nothing accepted it. The coordinator sends it once to another holder; a
    /// client that meets it retries under the same identity
    /// ([F45](../../../../docs/src/features/replica-migration.md)).
    StaleTopology = 55,
    /// The connection's principal may not perform this administrative request
    Unauthorized = 60,
    /// The request named a topology version other than the current one
    StaleVersion = 61,
    /// The node asked cannot commit this request and knows no leader that can
    NotLeader = 62,
    /// This node has no placement to route by, so no data query can be answered by it
    ///
    /// A joiner before an operator initialized the placement. A member the placement does not
    /// name is not refused: it forwards every query to the tablets' holders
    /// ([Resolved #169](../../../../docs/src/appendix/resolved/unplaced-member-forwards.md)).
    NotInitialized = 63,
    /// The copy this read would be served from is quarantined
    ///
    /// A checksum failed on it, or a scrub found it divergent, and it serves nothing until a
    /// verified repair or an operator lifts it ([F44](../../../../docs/src/features/repair.md)).
    Quarantined = 64,
    /// The administrative request named a node that is not a member of this cluster
    ///
    /// This and the codes after it are the kinds an admin refusal carries beside its sentence,
    /// so a client acts on the code rather than the wording
    /// ([Resolved #98](../../../../docs/src/appendix/resolved/admin-refusal-kinds.md)).
    NotMember = 65,
    /// The administrative request named a member that is not up, and needed an up one
    NotUp = 66,
    /// The administrative request named a node, a set or a plan twice, or one already holding
    /// what it would be given
    Duplicate = 67,
    /// The cluster, the placement or the restore the request would do is already done
    AlreadyInitialized = 68,
    /// The control voter count named is not one the policy allows
    BadVoterCount = 69,
    /// The administrative request is malformed as stated, whatever the cluster's state
    InvalidRequest = 70,
    /// The member named is in the wrong phase, or under the wrong grace, for the request
    WrongPhase = 71,
    /// The operation, group, table or tablet named is not one the cluster records
    UnknownOperation = 72,
    /// The wire version named cannot be activated, or is below what the request needs
    WireVersion = 73,
}

impl ErrorCode {
    /// Get the number this code is written as
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Parse a code from the number it was written as
    ///
    /// This fails *open*, to `Unknown`, where [`RefusalReason::from_byte`] fails closed. The two
    /// are answering different questions: a refusal reason decides whether to trust a connection,
    /// so an unrecognized one has to be treated as a refusal, while a code only classifies a
    /// failure that has already happened. Refusing to decode it would throw away a message that is
    /// still perfectly readable by a person.
    ///
    /// # Arguments
    ///
    /// * `raw` - The number to parse a code from
    ///
    /// [`RefusalReason::from_byte`]: super::handshake::RefusalReason::from_byte
    pub const fn from_u16(raw: u16) -> Self {
        // map each known discriminant back to its variant
        match raw {
            1 => ErrorCode::Internal,
            10 => ErrorCode::StorageRead,
            11 => ErrorCode::ArchiveMissing,
            12 => ErrorCode::CorruptArchive,
            13 => ErrorCode::StorageWrite,
            20 => ErrorCode::ResponseTooLarge,
            21 => ErrorCode::RequestTooLarge,
            22 => ErrorCode::IdentityExpired,
            30 => ErrorCode::Shedding,
            31 => ErrorCode::Timeout,
            32 => ErrorCode::OutcomeUnknown,
            40 => ErrorCode::ConnectionLost,
            41 => ErrorCode::GoingAway,
            50 => ErrorCode::Unavailable,
            51 => ErrorCode::QuorumUnavailable,
            52 => ErrorCode::WrongCluster,
            53 => ErrorCode::UnknownLineage,
            54 => ErrorCode::UnsupportedReadLevel,
            55 => ErrorCode::StaleTopology,
            60 => ErrorCode::Unauthorized,
            61 => ErrorCode::StaleVersion,
            62 => ErrorCode::NotLeader,
            63 => ErrorCode::NotInitialized,
            64 => ErrorCode::Quarantined,
            65 => ErrorCode::NotMember,
            66 => ErrorCode::NotUp,
            67 => ErrorCode::Duplicate,
            68 => ErrorCode::AlreadyInitialized,
            69 => ErrorCode::BadVoterCount,
            70 => ErrorCode::InvalidRequest,
            71 => ErrorCode::WrongPhase,
            72 => ErrorCode::UnknownOperation,
            73 => ErrorCode::WireVersion,
            // zero, and anything a newer peer knows about that we do not
            _ => ErrorCode::Unknown,
        }
    }

    /// Get the name of this code
    pub const fn name(self) -> &'static str {
        match self {
            ErrorCode::Unknown => "Unknown",
            ErrorCode::Internal => "Internal",
            ErrorCode::StorageRead => "StorageRead",
            ErrorCode::ArchiveMissing => "ArchiveMissing",
            ErrorCode::CorruptArchive => "CorruptArchive",
            ErrorCode::StorageWrite => "StorageWrite",
            ErrorCode::ResponseTooLarge => "ResponseTooLarge",
            ErrorCode::RequestTooLarge => "RequestTooLarge",
            ErrorCode::IdentityExpired => "IdentityExpired",
            ErrorCode::Shedding => "Shedding",
            ErrorCode::Timeout => "Timeout",
            ErrorCode::OutcomeUnknown => "OutcomeUnknown",
            ErrorCode::ConnectionLost => "ConnectionLost",
            ErrorCode::GoingAway => "GoingAway",
            ErrorCode::Unavailable => "Unavailable",
            ErrorCode::QuorumUnavailable => "QuorumUnavailable",
            ErrorCode::WrongCluster => "WrongCluster",
            ErrorCode::UnknownLineage => "UnknownLineage",
            ErrorCode::UnsupportedReadLevel => "UnsupportedReadLevel",
            ErrorCode::StaleTopology => "StaleTopology",
            ErrorCode::Unauthorized => "Unauthorized",
            ErrorCode::StaleVersion => "StaleVersion",
            ErrorCode::NotLeader => "NotLeader",
            ErrorCode::NotInitialized => "NotInitialized",
            ErrorCode::Quarantined => "Quarantined",
            ErrorCode::NotMember => "NotMember",
            ErrorCode::NotUp => "NotUp",
            ErrorCode::Duplicate => "Duplicate",
            ErrorCode::AlreadyInitialized => "AlreadyInitialized",
            ErrorCode::BadVoterCount => "BadVoterCount",
            ErrorCode::InvalidRequest => "InvalidRequest",
            ErrorCode::WrongPhase => "WrongPhase",
            ErrorCode::UnknownOperation => "UnknownOperation",
            ErrorCode::WireVersion => "WireVersion",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    /// Write this code's name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// An error frame's header and the fields that follow it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorFrame {
    /// The header of this frame
    pub header: Header,
    /// The query this failure belongs to, or nil if it is about the connection itself
    pub query_id: Uuid,
    /// What class of failure this is
    pub code: ErrorCode,
    /// The number of message bytes after the preamble
    pub msg_len: usize,
}

/// Cut a message down to what an error frame will carry
///
/// The cut lands on a character boundary, so a truncated message is still a `&str` and still
/// prints. A message that already fits is returned untouched.
///
/// # Arguments
///
/// * `msg` - The message to cut down
pub fn truncate_msg(msg: &str) -> &str {
    // a message that fits needs nothing done to it
    if msg.len() <= MAX_ERROR_MSG_LEN {
        return msg;
    }
    // walk back from the bound until we are on a character boundary, which is at most three bytes
    let mut end = MAX_ERROR_MSG_LEN;
    while end > 0 && !msg.is_char_boundary(end) {
        end -= 1;
    }
    &msg[..end]
}

/// Build the bytes that go ahead of an error frame's message
///
/// The preamble is returned as a stack array rather than joined onto the message for the same
/// reason a response's is: the two stay separate buffers so the write can be vectored.
///
/// # Arguments
///
/// * `query_id` - The query this failure belongs to, or nil for the connection itself
/// * `code` - What class of failure this is
/// * `msg_len` - The number of message bytes that will follow this preamble
/// * `max_frame_bytes` - The largest frame the peer will accept
pub fn error_preamble(
    query_id: &Uuid,
    code: ErrorCode,
    msg_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; ERROR_PREAMBLE_LEN], ProtocolError> {
    // refuse a message past our own bound before the peer's, since ours is the tighter of the two
    if msg_len > MAX_ERROR_MSG_LEN {
        return Err(ProtocolError::PayloadTooLarge {
            len: msg_len,
            max: MAX_ERROR_MSG_LEN as u32,
        });
    }
    // the query id and the code are part of the frame body, so they count towards the length
    let body_len = ERROR_BODY_MIN.saturating_add(msg_len);
    let header = Header::new(
        MessageType::Error,
        Flags::IS_ERROR,
        body_len,
        max_frame_bytes,
    )?;
    // lay the header down, then the query id, then the code and its two reserved bytes
    let mut preamble = [0u8; ERROR_PREAMBLE_LEN];
    preamble[..HEADER_LEN].copy_from_slice(&header.encode());
    preamble[HEADER_LEN..HEADER_LEN + QUERY_ID_LEN].copy_from_slice(query_id.as_bytes());
    preamble[HEADER_LEN + QUERY_ID_LEN..HEADER_LEN + QUERY_ID_LEN + 2]
        .copy_from_slice(&code.as_u16().to_le_bytes());
    Ok(preamble)
}

/// Work out how many message bytes an error frame carries, before anything allocates for them
///
/// This is split out from [`decode_error`] because a client reads the same
/// [`RESPONSE_PREAMBLE_LEN`] preamble for every frame a server sends and only learns the type
/// afterwards, so it holds a [`Header`] rather than a whole error preamble at the point it has to
/// size its read.
///
/// # Arguments
///
/// * `header` - The header of the error frame to size
///
/// [`RESPONSE_PREAMBLE_LEN`]: super::RESPONSE_PREAMBLE_LEN
pub const fn msg_len(header: Header) -> Result<usize, ProtocolError> {
    // an error frame always carries a query id and a code, so a shorter one cannot be one
    let msg_len = match header.body_len().checked_sub(ERROR_BODY_MIN) {
        Some(msg_len) => msg_len,
        None => {
            return Err(ProtocolError::BodyTooShort {
                need: ERROR_BODY_MIN,
                got: header.len,
            })
        }
    };
    // refuse a message past our own bound, which is tighter than the frame bound that let it in
    if msg_len > MAX_ERROR_MSG_LEN {
        return Err(ProtocolError::FrameTooLarge {
            len: header.len,
            max: MAX_ERROR_MSG_LEN as u32,
        });
    }
    Ok(msg_len)
}

/// Read the code and the message out of the body bytes that follow an error frame's query id
///
/// A message that is not valid UTF-8 is read lossily rather than refused. Refusing it would
/// replace a legible failure with an obscure one, and the code — which is the part a caller
/// branches on — is in the bytes ahead of the message and is unaffected either way.
///
/// # Arguments
///
/// * `rest` - The body bytes after the query id: the code, its reserved bytes, and the message
pub fn decode_error_tail(rest: &[u8]) -> Result<(ErrorCode, Cow<'_, str>), ProtocolError> {
    // the code and its reserved bytes have to be here before there can be a message at all
    if rest.len() < 4 {
        return Err(ProtocolError::BodyTooShort {
            need: 4,
            got: rest.len() as u32,
        });
    }
    // pull the code out of the front, ignoring the two reserved bytes after it
    let code = ErrorCode::from_u16(u16::from_le_bytes([rest[0], rest[1]]));
    // everything left is the message
    Ok((code, String::from_utf8_lossy(&rest[4..])))
}

/// Read a whole error frame preamble
///
/// # Arguments
///
/// * `raw` - The preamble bytes to read
/// * `max_frame_bytes` - The largest frame we are willing to allocate for
pub fn decode_error(
    raw: &[u8; ERROR_PREAMBLE_LEN],
    max_frame_bytes: u32,
) -> Result<ErrorFrame, ProtocolError> {
    // pull the header out of the front of the preamble and check it
    let mut header_bytes = [0u8; HEADER_LEN];
    header_bytes.copy_from_slice(&raw[..HEADER_LEN]);
    let header = Header::decode(&header_bytes, max_frame_bytes)?.expect(MessageType::Error)?;
    // size the message this frame carries before anything allocates for it
    let msg_len = msg_len(header)?;
    // the query id sits between the header and the code
    let mut id_bytes = [0u8; QUERY_ID_LEN];
    id_bytes.copy_from_slice(&raw[HEADER_LEN..HEADER_LEN + QUERY_ID_LEN]);
    // the code sits after the query id, with two reserved bytes behind it
    let code = ErrorCode::from_u16(u16::from_le_bytes([
        raw[HEADER_LEN + QUERY_ID_LEN],
        raw[HEADER_LEN + QUERY_ID_LEN + 1],
    ]));
    Ok(ErrorFrame {
        header,
        query_id: Uuid::from_bytes(id_bytes),
        code,
        msg_len,
    })
}
