//! Tests for the wire framing
//!
//! These are the tests that hold the format itself still. Everything else in the protocol is
//! exercised by the integration suite, but a discriminant that quietly changed, or a flag bit that
//! moved, would still pass every one of those tests while breaking every peer built from an older
//! commit.

use uuid::Uuid;

use super::auth::{
    decode_auth_body, decode_auth_response_body, encode_auth, encode_auth_response, payload_len,
    AuthMechanism, AuthMechanisms, AuthStatus, AUTH_BODY_MIN, MAX_AUTH_FRAME_BODY,
    MAX_AUTH_PAYLOAD_LEN,
};
use super::error::{
    self, decode_error, decode_error_tail, error_preamble, ErrorCode, ERROR_BODY_MIN,
    ERROR_PREAMBLE_LEN, MAX_ERROR_MSG_LEN,
};
use super::fingerprint::{self, ROLE_FILTER, ROLE_PARTITION, ROLE_SORT, ROLE_UPDATE};
use super::handshake::{Hello, HelloAck, RefusalReason, HANDSHAKE_BODY_LEN, HANDSHAKE_FRAME_LEN};
use super::trace::{TraceContext, TRACE_CONTEXT_LEN, TRACE_CONTEXT_VERSION};
use super::{
    decode_request, decode_response, request_preamble, request_preamble_traced, response_preamble,
    Flags, Header, MessageType, ProtocolError, RawHeader, HEADER_LEN, MAX_REQUEST_PREAMBLE_LEN,
    PROTOCOL_VERSION, QUERY_ID_LEN, REQUEST_PREAMBLE_LEN, RESPONSE_PREAMBLE_LEN,
};

/// Every message type this build knows, so a test can walk all of them
const ALL_TYPES: [MessageType; 12] = [
    MessageType::Hello,
    MessageType::HelloAck,
    MessageType::Auth,
    MessageType::AuthResponse,
    MessageType::Queries,
    MessageType::Response,
    MessageType::Ping,
    MessageType::Pong,
    MessageType::Topology,
    MessageType::Error,
    MessageType::GoAway,
    MessageType::Cancel,
];

/// Every error code this build knows, so a test can walk all of them
const ALL_CODES: [ErrorCode; 12] = [
    ErrorCode::Unknown,
    ErrorCode::Internal,
    ErrorCode::StorageRead,
    ErrorCode::ArchiveMissing,
    ErrorCode::CorruptArchive,
    ErrorCode::ResponseTooLarge,
    ErrorCode::RequestTooLarge,
    ErrorCode::Shedding,
    ErrorCode::Timeout,
    ErrorCode::ConnectionLost,
    ErrorCode::GoingAway,
    ErrorCode::Unavailable,
];

/// A frame bound big enough that no test trips it by accident
const ROOMY: u32 = 1024 * 1024;

/// A header round trips through its eight bytes for every message type
#[test]
fn a_header_round_trips() {
    // build, encode and decode a header for every message type we know
    for kind in ALL_TYPES {
        let header = Header::new(kind, Flags::LAST, 4096, ROOMY).unwrap();
        let decoded = Header::decode(&header.encode(), ROOMY).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(decoded.kind, kind);
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.body_len(), 4096);
    }
}

/// A request preamble round trips
#[test]
fn a_request_preamble_round_trips() {
    // a request frame carries nothing between its header and its payload
    let preamble = request_preamble(9001, ROOMY).unwrap();
    let header = decode_request(&preamble, ROOMY).unwrap();
    assert_eq!(header.kind, MessageType::Queries);
    assert_eq!(header.body_len(), 9001);
}

/// A response preamble round trips, and the query id survives byte for byte
#[test]
fn a_response_preamble_round_trips() {
    // use a fixed uuid so a byte order slip shows up as a different id rather than as a flake
    let query_id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
    let preamble = response_preamble(&query_id, 512, ROOMY).unwrap();
    let frame = decode_response(&preamble, ROOMY).unwrap();
    assert_eq!(frame.query_id, query_id);
    assert_eq!(frame.payload_len, 512);
    // the length counts the query id, so it is sixteen more than the payload
    assert_eq!(frame.header.body_len(), 512 + QUERY_ID_LEN);
    // and the id is written exactly as uuid writes it, so the two peers cannot disagree
    assert_eq!(&preamble[HEADER_LEN..], query_id.as_bytes());
}

/// The preambles are the same size they were before the header existed
///
/// The old request frame was an eight byte length, and the old response preamble was a sixteen
/// byte query id plus an eight byte length. Narrowing the length to a `u32` pays for the version,
/// type and flag bytes exactly, so this change costs zero bytes per frame. If a future change
/// makes a preamble bigger, that is a real cost and this test is where it should be argued.
#[test]
fn the_preamble_sizes_are_unchanged() {
    assert_eq!(REQUEST_PREAMBLE_LEN, 8);
    assert_eq!(RESPONSE_PREAMBLE_LEN, 24);
    // and a request frame carrying a trace context is those eight bytes plus a fixed block, which
    // is the one preamble here that is not the same size every time
    assert_eq!(TRACE_CONTEXT_LEN, 26);
    assert_eq!(MAX_REQUEST_PREAMBLE_LEN, 34);
}

/// Every message type is written as the byte it has always been written as
///
/// This is the most load bearing test in the file. Inserting a variant into `MessageType` without
/// an explicit discriminant would renumber every variant after it and break every peer built from
/// an older commit, and nothing else in the suite would notice.
#[test]
fn every_message_type_round_trips_through_its_discriminant() {
    // the discriminant each message type is pinned to, which may never change
    let pinned = [
        (MessageType::Hello, 1u8),
        (MessageType::HelloAck, 2),
        (MessageType::Auth, 3),
        (MessageType::AuthResponse, 4),
        (MessageType::Queries, 5),
        (MessageType::Response, 6),
        (MessageType::Ping, 7),
        (MessageType::Pong, 8),
        (MessageType::Topology, 9),
        (MessageType::Error, 10),
        (MessageType::GoAway, 11),
        (MessageType::Cancel, 12),
    ];
    // check both directions for each one
    for (kind, byte) in pinned {
        assert_eq!(kind.as_byte(), byte, "{kind} moved off byte {byte}");
        assert_eq!(MessageType::from_byte(byte).unwrap(), kind);
    }
    // and check that the list above did not fall behind the enum
    assert_eq!(pinned.len(), ALL_TYPES.len());
}

/// Every flag bit is the bit it has always been
#[test]
fn flag_bits_are_stable() {
    assert_eq!(Flags::NONE.bits(), 0);
    assert_eq!(Flags::IS_ERROR.bits(), 1);
    assert_eq!(Flags::STALE_TOPOLOGY.bits(), 2);
    assert_eq!(Flags::LAST.bits(), 4);
    assert_eq!(Flags::REFUSED.bits(), 8);
}

/// A flag bit this build does not know is carried through untouched
///
/// This is what makes the sixteen flag bits spendable one at a time. A decoder that masked
/// unknown bits off would turn the first use of a new bit into a compatibility break.
#[test]
fn unknown_flag_bits_are_preserved() {
    // set a bit no constant claims yet
    let exotic = Flags::from_bits(1 << 15);
    let header = Header::new(MessageType::Response, exotic, 64, ROOMY).unwrap();
    let decoded = Header::decode(&header.encode(), ROOMY).unwrap();
    assert_eq!(decoded.flags, exotic);
    assert!(decoded.flags.contains(exotic));
    // and one we do know is still readable next to one we do not
    let mixed = exotic.union(Flags::LAST);
    let header = Header::new(MessageType::Response, mixed, 64, ROOMY).unwrap();
    let decoded = Header::decode(&header.encode(), ROOMY).unwrap();
    assert!(decoded.flags.contains(Flags::LAST));
    assert!(decoded.flags.contains(exotic));
}

/// A version this build does not speak is refused, and named in the error
#[test]
fn an_unknown_version_is_refused() {
    // try a few versions either side of the one we speak, including zero
    for version in [0u8, PROTOCOL_VERSION + 1, 255] {
        let mut raw = Header::new(MessageType::Queries, Flags::NONE, 8, ROOMY)
            .unwrap()
            .encode();
        raw[0] = version;
        let error = Header::decode(&raw, ROOMY).unwrap_err();
        assert_eq!(
            error,
            ProtocolError::UnsupportedVersion {
                got: version,
                ours: PROTOCOL_VERSION,
            }
        );
    }
}

/// A version we do not speak is still a header we can read
///
/// This is what lets a server refuse a client legibly instead of dropping it: it can say which
/// version it saw and drain exactly the right number of body bytes before replying.
#[test]
fn a_header_of_an_unknown_version_is_still_readable() {
    // write a header with a version from the future
    let mut raw = Header::new(MessageType::Hello, Flags::NONE, 16, ROOMY)
        .unwrap()
        .encode();
    raw[0] = 99;
    // the raw header still tells us everything we need to answer the peer
    let raw_header = RawHeader::decode(&raw);
    assert_eq!(raw_header.version, 99);
    assert_eq!(raw_header.len, 16);
    assert_eq!(raw_header.kind, MessageType::Hello.as_byte());
}

/// A message type this build does not know is refused
#[test]
fn an_unknown_message_type_is_refused() {
    // zero in particular, so that a zeroed buffer is never mistaken for a hello
    for kind in [0u8, 13, 255] {
        let mut raw = Header::new(MessageType::Queries, Flags::NONE, 8, ROOMY)
            .unwrap()
            .encode();
        raw[1] = kind;
        assert_eq!(
            Header::decode(&raw, ROOMY).unwrap_err(),
            ProtocolError::UnknownMessageType(kind)
        );
    }
}

/// A frame of the right shape but the wrong type is refused
#[test]
fn a_frame_of_the_wrong_type_is_refused() {
    // a response frame arriving where a bundle of queries had to be
    let preamble = response_preamble(&Uuid::new_v4(), 32, ROOMY).unwrap();
    let mut raw = [0u8; REQUEST_PREAMBLE_LEN];
    raw.copy_from_slice(&preamble[..REQUEST_PREAMBLE_LEN]);
    assert_eq!(
        decode_request(&raw, ROOMY).unwrap_err(),
        ProtocolError::UnexpectedMessageType {
            expected: MessageType::Queries,
            got: MessageType::Response,
        }
    );
}

/// A length past the bound is refused before anything allocates
///
/// The decoder is a pure function over a fixed size array, so it cannot allocate even if it wanted
/// to. That is the property this test is protecting: the check has to happen here, in front of the
/// call site that would have used the length as an allocation size.
#[test]
fn a_length_over_the_bound_is_refused() {
    // claim the largest frame a u32 can spell against a tiny bound
    let mut raw = Header::new(MessageType::Queries, Flags::NONE, 8, ROOMY)
        .unwrap()
        .encode();
    raw[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        Header::decode(&raw, 1024).unwrap_err(),
        ProtocolError::FrameTooLarge {
            len: u32::MAX,
            max: 1024,
        }
    );
    // and a frame exactly on the bound is still accepted
    let mut raw = Header::new(MessageType::Queries, Flags::NONE, 8, ROOMY)
        .unwrap()
        .encode();
    raw[4..8].copy_from_slice(&1024u32.to_le_bytes());
    assert_eq!(Header::decode(&raw, 1024).unwrap().body_len(), 1024);
}

/// A response frame shorter than its own query id is refused
#[test]
fn a_response_shorter_than_its_query_id_is_refused() {
    // claim a body that cannot even hold the routing field every response carries
    for len in 0u32..QUERY_ID_LEN as u32 {
        let mut raw = response_preamble(&Uuid::new_v4(), 0, ROOMY).unwrap();
        raw[4..8].copy_from_slice(&len.to_le_bytes());
        assert_eq!(
            decode_response(&raw, ROOMY).unwrap_err(),
            ProtocolError::BodyTooShort {
                need: QUERY_ID_LEN,
                got: len,
            }
        );
    }
}

/// A payload too large to frame is refused on the way out, not silently truncated
#[test]
fn a_payload_that_does_not_fit_is_refused_on_encode() {
    // a request payload past the peer's bound
    assert_eq!(
        request_preamble(2048, 1024).unwrap_err(),
        ProtocolError::PayloadTooLarge {
            len: 2048,
            max: 1024,
        }
    );
    // a response payload whose query id is what pushes it over
    assert_eq!(
        response_preamble(&Uuid::new_v4(), 1024, 1024).unwrap_err(),
        ProtocolError::PayloadTooLarge {
            len: 1024 + QUERY_ID_LEN,
            max: 1024,
        }
    );
    // and a payload that would truncate rather than merely exceed the bound
    assert!(matches!(
        request_preamble(u32::MAX as usize + 1, u32::MAX).unwrap_err(),
        ProtocolError::PayloadTooLarge { .. }
    ));
}

/// A hello round trips through its sixteen bytes
#[test]
fn a_hello_round_trips() {
    let hello = Hello {
        schema_fingerprint: 0xdead_beef_cafe_f00d,
        max_frame_bytes: 4096,
        mechanisms: AuthMechanisms::SCRAM_SHA_256,
    };
    assert_eq!(Hello::decode(&hello.encode()), hello);
    // and the whole frame is a header this build can read
    let frame = hello.frame(ROOMY).unwrap();
    assert_eq!(frame.len(), HANDSHAKE_FRAME_LEN);
    let mut header_bytes = [0u8; HEADER_LEN];
    header_bytes.copy_from_slice(&frame[..HEADER_LEN]);
    let header = Header::decode(&header_bytes, ROOMY).unwrap();
    assert_eq!(header.kind, MessageType::Hello);
    assert_eq!(header.body_len(), HANDSHAKE_BODY_LEN);
}

/// An ack round trips through its sixteen bytes, accepted or refused
#[test]
fn a_hello_ack_round_trips() {
    // walk every reason, since the reason byte is the one field an ack has and a hello does not
    for reason in [
        RefusalReason::Accepted,
        RefusalReason::UnsupportedVersion,
        RefusalReason::SchemaMismatch,
        RefusalReason::NoCommonAuthMechanism,
    ] {
        let ack = HelloAck {
            schema_fingerprint: 0x0102_0304_0506_0708,
            max_frame_bytes: 8192,
            reason,
            mechanism: Some(AuthMechanism::ScramSha256),
        };
        assert_eq!(HelloAck::decode(&ack.encode()), ack);
        // a refusal is flagged in the header too, so a peer can tell without reading the body
        let frame = ack.frame(ROOMY).unwrap();
        let mut header_bytes = [0u8; HEADER_LEN];
        header_bytes.copy_from_slice(&frame[..HEADER_LEN]);
        let header = Header::decode(&header_bytes, ROOMY).unwrap();
        assert_eq!(header.kind, MessageType::HelloAck);
        assert_eq!(header.flags.contains(Flags::REFUSED), !reason.is_accepted());
    }
}

/// A refusal reason this build does not know is read as a refusal, not as an acceptance
#[test]
fn an_unknown_refusal_reason_fails_closed() {
    // a server from the future refusing us for a reason we have never heard of
    for raw in [4u8, 42, 255] {
        assert!(!RefusalReason::from_byte(raw).is_accepted());
    }
    // and only a literal zero means accepted
    assert!(RefusalReason::from_byte(0).is_accepted());
}

/// Two fields in a different order fingerprint differently
#[test]
fn reordering_two_fields_changes_the_fingerprint() {
    // the same two fields, swapped
    let first = fingerprint::mix_field(fingerprint::SEED, "id", "u64", 8, 8, 0, ROLE_PARTITION);
    let first = fingerprint::mix_field(first, "name", "String", 8, 4, 1, ROLE_FILTER);
    let second = fingerprint::mix_field(fingerprint::SEED, "name", "String", 8, 4, 0, ROLE_FILTER);
    let second = fingerprint::mix_field(second, "id", "u64", 8, 8, 1, ROLE_PARTITION);
    assert_ne!(first, second);
}

/// A renamed field fingerprints differently
#[test]
fn renaming_a_field_changes_the_fingerprint() {
    let before = fingerprint::mix_field(fingerprint::SEED, "title", "String", 8, 4, 0, ROLE_SORT);
    let after = fingerprint::mix_field(fingerprint::SEED, "name", "String", 8, 4, 0, ROLE_SORT);
    assert_ne!(before, after);
}

/// A type that widened behind an alias fingerprints differently
///
/// This is the one case the spelling of a type cannot catch on its own — `type Id = u32` becoming
/// `type Id = u64` leaves every declaration reading `Id`. Mixing the archived size is what closes
/// it, and this test is what says so.
#[test]
fn widening_a_type_behind_an_alias_changes_the_fingerprint() {
    let narrow = fingerprint::mix_field(fingerprint::SEED, "id", "Id", 4, 4, 0, ROLE_PARTITION);
    let wide = fingerprint::mix_field(fingerprint::SEED, "id", "Id", 8, 8, 0, ROLE_PARTITION);
    assert_ne!(narrow, wide);
}

/// A field that gained a role fingerprints differently
#[test]
fn changing_what_a_field_can_do_changes_the_fingerprint() {
    let before = fingerprint::mix_field(fingerprint::SEED, "views", "u64", 8, 8, 0, ROLE_FILTER);
    let after = fingerprint::mix_field(
        fingerprint::SEED,
        "views",
        "u64",
        8,
        8,
        0,
        ROLE_FILTER | ROLE_UPDATE,
    );
    assert_ne!(before, after);
}

/// A table list in a different order fingerprints differently
#[test]
fn reordering_the_tables_changes_the_fingerprint() {
    let first = fingerprint::mix_str(
        fingerprint::mix_str(fingerprint::SEED, "movies"),
        "keywords",
    );
    let second = fingerprint::mix_str(
        fingerprint::mix_str(fingerprint::SEED, "keywords"),
        "movies",
    );
    assert_ne!(first, second);
}

/// The separator keeps two values from running together
///
/// Without a separator between mixed values, `("ab", "c")` and `("a", "bc")` hash identically, and
/// renaming a pair of adjacent fields would sail through the handshake.
#[test]
fn the_separator_stops_concatenation_colliding() {
    let split_late = fingerprint::mix_str(fingerprint::mix_str(fingerprint::SEED, "ab"), "c");
    let split_early = fingerprint::mix_str(fingerprint::mix_str(fingerprint::SEED, "a"), "bc");
    assert_ne!(split_late, split_early);
}

/// Every error code is written as the number it has always been written as
///
/// This is `every_message_type_round_trips_through_its_discriminant` for the other enum that is on
/// the wire. Inserting a variant without an explicit discriminant would renumber every code after
/// it, and because a code this build does not recognize decodes as `Unknown` rather than as an
/// error, nothing else in the suite would notice — a peer would simply start reporting the wrong
/// class of failure.
#[test]
fn every_error_code_round_trips_through_its_discriminant() {
    // the number each code is pinned to, which may never change
    let pinned = [
        (ErrorCode::Unknown, 0u16),
        (ErrorCode::Internal, 1),
        (ErrorCode::StorageRead, 10),
        (ErrorCode::ArchiveMissing, 11),
        (ErrorCode::CorruptArchive, 12),
        (ErrorCode::ResponseTooLarge, 20),
        (ErrorCode::RequestTooLarge, 21),
        (ErrorCode::Shedding, 30),
        (ErrorCode::Timeout, 31),
        (ErrorCode::ConnectionLost, 40),
        (ErrorCode::GoingAway, 41),
        (ErrorCode::Unavailable, 50),
    ];
    // check both directions for each one
    for (code, raw) in pinned {
        assert_eq!(code.as_u16(), raw, "{code} moved off number {raw}");
        assert_eq!(ErrorCode::from_u16(raw), code);
    }
    // and check that the list above did not fall behind the enum
    assert_eq!(pinned.len(), ALL_CODES.len());
}

/// A code this build does not know reads as unknown rather than as a decode failure
///
/// This is the one place the error channel deliberately fails open. A newer server naming a class
/// of failure we have never heard of still has a message we can print, and refusing the frame
/// would throw that message away to make a point.
#[test]
fn an_unknown_error_code_reads_as_unknown() {
    // walk some numbers no variant claims, including the gaps inside the bands
    for raw in [2u16, 13, 22, 42, 51, 9000, u16::MAX] {
        assert_eq!(ErrorCode::from_u16(raw), ErrorCode::Unknown);
    }
}

/// An error frame round trips, and every field survives byte for byte
#[test]
fn an_error_frame_round_trips() {
    // use a fixed uuid so a byte order slip shows up as a different id rather than as a flake
    let query_id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
    let msg = "the copy of this partition on disk could not be read";
    let preamble = error_preamble(&query_id, ErrorCode::StorageRead, msg.len(), ROOMY).unwrap();
    let frame = decode_error(&preamble, ROOMY).unwrap();
    assert_eq!(frame.query_id, query_id);
    assert_eq!(frame.code, ErrorCode::StorageRead);
    assert_eq!(frame.msg_len, msg.len());
    // the type byte says what this is, and the flag says it too
    assert_eq!(frame.header.kind, MessageType::Error);
    assert!(frame.header.flags.contains(Flags::IS_ERROR));
    // the length counts the query id and the code, so it is twenty more than the message
    assert_eq!(frame.header.body_len(), msg.len() + ERROR_BODY_MIN);
    // and the tail parses back to the code and the message it was built from
    let mut rest = Vec::from(&preamble[HEADER_LEN + QUERY_ID_LEN..]);
    rest.extend_from_slice(msg.as_bytes());
    let (code, decoded) = decode_error_tail(&rest).unwrap();
    assert_eq!(code, ErrorCode::StorageRead);
    assert_eq!(decoded, msg);
}

/// An error frame's query id sits exactly where a response frame's does
///
/// This is what lets the client read one fixed preamble for every frame a server sends and only
/// then dispatch on the type. If these two ever disagreed, the client would read part of an error
/// code as the tail of a uuid, route the frame to a query that does not exist, and report a
/// missing stream channel for a frame that was perfectly well formed.
#[test]
fn an_error_frames_query_id_sits_where_a_responses_does() {
    // build one of each for the same query
    let query_id = Uuid::from_u128(0xdead_beef_dead_beef_dead_beef_dead_beef);
    let response = response_preamble(&query_id, 64, ROOMY).unwrap();
    let error = error_preamble(&query_id, ErrorCode::Internal, 8, ROOMY).unwrap();
    // both put the id in the sixteen bytes after the header, and both write it the same way
    assert_eq!(&response[HEADER_LEN..HEADER_LEN + QUERY_ID_LEN], query_id.as_bytes());
    assert_eq!(&error[HEADER_LEN..HEADER_LEN + QUERY_ID_LEN], query_id.as_bytes());
    // so the preamble a client reads is the same size whichever one arrived
    assert_eq!(RESPONSE_PREAMBLE_LEN, HEADER_LEN + QUERY_ID_LEN);
    assert!(ERROR_PREAMBLE_LEN > RESPONSE_PREAMBLE_LEN);
}

/// An error frame that cannot hold its own fixed fields is refused
#[test]
fn an_error_frame_shorter_than_its_own_fields_is_refused() {
    // claim a body that cannot hold the query id and the code every error frame carries
    for len in 0u32..ERROR_BODY_MIN as u32 {
        let mut raw = error_preamble(&Uuid::new_v4(), ErrorCode::Internal, 0, ROOMY).unwrap();
        raw[4..8].copy_from_slice(&len.to_le_bytes());
        assert!(matches!(
            decode_error(&raw, ROOMY).unwrap_err(),
            ProtocolError::BodyTooShort { .. }
        ));
    }
    // and a tail with no room for the code is refused on its own
    assert!(matches!(
        decode_error_tail(&[0u8; 3]).unwrap_err(),
        ProtocolError::BodyTooShort { need: 4, got: 3 }
    ));
}

/// A message past the message bound is refused in both directions
///
/// The frame bound is 64 mebibytes and the message bound is four kibibytes, so a message can be
/// far too large while the frame carrying it is comfortably legal. Without this check the error
/// channel would be an allocation channel that never trips the frame bound at all.
#[test]
fn a_message_over_the_message_bound_is_refused() {
    // a message we are about to write that is over the bound
    assert_eq!(
        error_preamble(
            &Uuid::new_v4(),
            ErrorCode::Internal,
            MAX_ERROR_MSG_LEN + 1,
            ROOMY
        )
        .unwrap_err(),
        ProtocolError::PayloadTooLarge {
            len: MAX_ERROR_MSG_LEN + 1,
            max: MAX_ERROR_MSG_LEN as u32,
        }
    );
    // a message a peer claims that is over the bound, sized before anything allocates for it
    let header = Header::new(
        MessageType::Error,
        Flags::IS_ERROR,
        ERROR_BODY_MIN + MAX_ERROR_MSG_LEN + 1,
        ROOMY,
    )
    .unwrap();
    assert!(matches!(
        error::msg_len(header).unwrap_err(),
        ProtocolError::FrameTooLarge { .. }
    ));
    // and a message exactly on the bound is still accepted, in both directions
    let preamble =
        error_preamble(&Uuid::new_v4(), ErrorCode::Internal, MAX_ERROR_MSG_LEN, ROOMY).unwrap();
    assert_eq!(
        decode_error(&preamble, ROOMY).unwrap().msg_len,
        MAX_ERROR_MSG_LEN
    );
    // a message truncated to the bound lands on a character boundary rather than mid character
    let wide = "\u{1f420}".repeat(MAX_ERROR_MSG_LEN);
    let cut = error::truncate_msg(&wide);
    assert!(cut.len() <= MAX_ERROR_MSG_LEN);
    assert!(wide.starts_with(cut));
}

/// A message that is not valid UTF-8 still delivers the code it came with
///
/// The code is what a caller branches on and it sits ahead of the message, so a garbled message
/// must not be allowed to swallow it. Reading lossily also means the readable part of a message
/// that was truncated mid character still prints.
#[test]
fn a_message_that_is_not_utf8_still_delivers_its_code() {
    // a tail whose code is fine and whose message is a lone continuation byte
    let mut rest = Vec::from(ErrorCode::CorruptArchive.as_u16().to_le_bytes());
    rest.extend_from_slice(&[0, 0]);
    rest.extend_from_slice(b"partition ");
    rest.push(0xff);
    let (code, msg) = decode_error_tail(&rest).unwrap();
    assert_eq!(code, ErrorCode::CorruptArchive);
    assert!(msg.starts_with("partition "));
    // the invalid byte became the replacement character rather than ending the decode
    assert!(msg.contains('\u{fffd}'));
}

/// Every mechanism this build knows round trips through the byte it is written as
///
/// The same test [`MessageType`] and [`ErrorCode`] have, for the same reason: these are on the
/// wire, so a variant inserted in the middle has to fail here rather than silently renumber a
/// mechanism a deployed client is already naming.
#[test]
fn every_auth_mechanism_round_trips_through_its_discriminant() {
    // pin the numbers themselves, since a round trip alone would survive renumbering all of them
    for (mechanism, raw) in [
        (AuthMechanism::ScramSha256, 1u8),
        (AuthMechanism::MutualTls, 2),
    ] {
        assert_eq!(mechanism.as_byte(), raw);
        assert_eq!(AuthMechanism::from_byte(raw).unwrap(), mechanism);
        // and the SASL name a config file spells it with maps back to the same variant
        assert_eq!(AuthMechanism::from_name(mechanism.name()), Some(mechanism));
    }
    // zero is not a mechanism, which is what makes a zeroed buffer decode as none rather than one
    assert!(matches!(
        AuthMechanism::from_byte(0).unwrap_err(),
        ProtocolError::UnknownAuthMechanism(0)
    ));
    // and a name nothing knows is not silently dropped
    assert_eq!(AuthMechanism::from_name("PLAIN"), None);
}

/// Every status this build knows round trips through the byte it is written as
#[test]
fn every_auth_status_round_trips_through_its_discriminant() {
    for (status, raw) in [
        (AuthStatus::Challenge, 1u8),
        (AuthStatus::Success, 2),
        (AuthStatus::Failed, 3),
    ] {
        assert_eq!(status.as_byte(), raw);
        assert_eq!(AuthStatus::from_byte(raw).unwrap(), status);
    }
    // a zeroed buffer must never read as a successful login
    assert!(matches!(
        AuthStatus::from_byte(0).unwrap_err(),
        ProtocolError::UnknownAuthStatus(0)
    ));
}

/// A mechanism set answers what it contains, and keeps bits it cannot name
#[test]
fn a_mechanism_set_keeps_bits_it_cannot_name() {
    // bit 9 is a mechanism from a build that does not exist yet
    let offered = AuthMechanisms::SCRAM_SHA_256.union(AuthMechanisms::from_bits(1 << 9));
    assert!(offered.contains(AuthMechanisms::SCRAM_SHA_256));
    assert!(!offered.contains(AuthMechanisms::MUTUAL_TLS));
    // the unknown bit survived, which is what lets a newer peer spend one without a version bump
    assert_eq!(offered.bits() & (1 << 9), 1 << 9);
    // and selection walks the server's preference order rather than the client's bits
    let both = AuthMechanisms::SCRAM_SHA_256.union(AuthMechanisms::MUTUAL_TLS);
    assert_eq!(
        both.first_supported(&[AuthMechanism::MutualTls, AuthMechanism::ScramSha256]),
        Some(AuthMechanism::MutualTls)
    );
    assert_eq!(
        both.first_supported(&[AuthMechanism::ScramSha256, AuthMechanism::MutualTls]),
        Some(AuthMechanism::ScramSha256)
    );
    // a server that accepts nothing selects nothing, whatever was offered
    assert_eq!(both.first_supported(&[]), None);
    assert_eq!(
        AuthMechanisms::NONE.first_supported(&[AuthMechanism::ScramSha256]),
        None
    );
}

/// An auth frame round trips through the bytes it is written as
#[test]
fn an_auth_frame_round_trips() {
    // a payload with a zero byte in it, since these are opaque bytes rather than text to the codec
    let payload = b"n,,n=user,r=\0nonce";
    let frame = encode_auth(AuthMechanism::ScramSha256, payload, ROOMY).unwrap();
    // the header says what it is and how much follows it
    let mut header_bytes = [0u8; HEADER_LEN];
    header_bytes.copy_from_slice(&frame[..HEADER_LEN]);
    let header = Header::decode(&header_bytes, ROOMY).unwrap();
    assert_eq!(header.kind, MessageType::Auth);
    assert_eq!(header.body_len(), AUTH_BODY_MIN + payload.len());
    assert_eq!(payload_len(header).unwrap(), payload.len());
    // and the body gives both fields back unchanged
    let (mechanism, read) = decode_auth_body(&frame[HEADER_LEN..]).unwrap();
    assert_eq!(mechanism, AuthMechanism::ScramSha256);
    assert_eq!(read, payload);
}

/// An auth response round trips, and a refusal is flagged in its header as well as its body
#[test]
fn an_auth_response_round_trips() {
    for status in [AuthStatus::Challenge, AuthStatus::Success, AuthStatus::Failed] {
        let payload = b"r=abc,s=def,i=4096";
        let frame = encode_auth_response(status, payload, ROOMY).unwrap();
        let mut header_bytes = [0u8; HEADER_LEN];
        header_bytes.copy_from_slice(&frame[..HEADER_LEN]);
        let header = Header::decode(&header_bytes, ROOMY).unwrap();
        assert_eq!(header.kind, MessageType::AuthResponse);
        // a refusal is readable from the header alone, the way a refused handshake is
        assert_eq!(
            header.flags.contains(Flags::REFUSED),
            status == AuthStatus::Failed
        );
        let (read_status, read) = decode_auth_response_body(&frame[HEADER_LEN..]).unwrap();
        assert_eq!(read_status, status);
        assert_eq!(read, payload);
    }
}

/// A payload past the auth bound is refused, on the way out and on the way in
///
/// These frames are read from a peer that has proved nothing yet, so the bound that applies is
/// this one and not the connection's much larger frame bound.
#[test]
fn an_auth_payload_past_the_bound_is_refused() {
    // one byte past what an auth frame will carry, with a frame bound that would allow it
    let payload = vec![0u8; MAX_AUTH_PAYLOAD_LEN + 1];
    assert!(matches!(
        encode_auth(AuthMechanism::ScramSha256, &payload, ROOMY).unwrap_err(),
        ProtocolError::PayloadTooLarge { .. }
    ));
    assert!(matches!(
        encode_auth_response(AuthStatus::Challenge, &payload, ROOMY).unwrap_err(),
        ProtocolError::PayloadTooLarge { .. }
    ));
    // and a peer that claims one is refused before anything is allocated for it
    let header = Header::new(
        MessageType::Auth,
        Flags::NONE,
        AUTH_BODY_MIN + MAX_AUTH_PAYLOAD_LEN + 1,
        ROOMY,
    )
    .unwrap();
    assert!(matches!(
        payload_len(header).unwrap_err(),
        ProtocolError::FrameTooLarge { .. }
    ));
    // a payload of exactly the bound still fits, which is what `MAX_AUTH_FRAME_BODY` is sized for
    let payload = vec![0u8; MAX_AUTH_PAYLOAD_LEN];
    let frame = encode_auth(AuthMechanism::ScramSha256, &payload, ROOMY).unwrap();
    let mut header_bytes = [0u8; HEADER_LEN];
    header_bytes.copy_from_slice(&frame[..HEADER_LEN]);
    let header = Header::decode(&header_bytes, MAX_AUTH_FRAME_BODY).unwrap();
    assert_eq!(payload_len(header).unwrap(), MAX_AUTH_PAYLOAD_LEN);
}

/// A body too short to hold an auth frame's fixed part is refused rather than indexed into
#[test]
fn an_auth_body_that_is_too_short_is_refused() {
    for short in 0..AUTH_BODY_MIN {
        let body = vec![1u8; short];
        assert!(matches!(
            decode_auth_body(&body).unwrap_err(),
            ProtocolError::BodyTooShort { .. }
        ));
        assert!(matches!(
            decode_auth_response_body(&body).unwrap_err(),
            ProtocolError::BodyTooShort { .. }
        ));
    }
    // a body with the fixed part and nothing else is an empty payload, not an error
    let (mechanism, payload) = decode_auth_body(&[1, 0, 0, 0]).unwrap();
    assert_eq!(mechanism, AuthMechanism::ScramSha256);
    assert!(payload.is_empty());
}

/// A hello ack naming a mechanism this build cannot do reads as none rather than as that one
///
/// This is safe in exactly one direction. The client is the peer that has to *do* the mechanism,
/// so a name it cannot read leaves it with nothing to send and it refuses the connection itself.
#[test]
fn an_unknown_mechanism_in_an_ack_reads_as_none() {
    let ack = HelloAck {
        schema_fingerprint: 7,
        max_frame_bytes: 4096,
        reason: RefusalReason::Accepted,
        mechanism: None,
    };
    // hand-write a mechanism byte from a build that does not exist yet
    let mut body = ack.encode();
    body[13] = 99;
    assert_eq!(HelloAck::decode(&body).mechanism, None);
    // and a zero, which is what every server wrote before there was authentication
    body[13] = 0;
    assert_eq!(HelloAck::decode(&body).mechanism, None);
}

/// A trace context that names a parent survives the wire
///
/// Both ids and the flags byte come back exactly as they went out, because the receiver builds a
/// `SpanContext` straight out of them - an id that shifted by a byte is a parent that resolves to
/// nothing, which is a new trace rather than an error.
#[test]
fn a_trace_context_round_trips() {
    // a context with every byte of both ids distinguishable from its neighbours
    let trace_id = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    let span_id = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let context = TraceContext::new(trace_id, span_id, 1).expect("a valid context was refused");
    // it goes out as the version byte, both ids in order, and the flags
    let raw = context.encode();
    assert_eq!(raw.len(), TRACE_CONTEXT_LEN);
    assert_eq!(raw[0], TRACE_CONTEXT_VERSION);
    assert_eq!(&raw[1..17], &trace_id);
    assert_eq!(&raw[17..25], &span_id);
    assert_eq!(raw[25], 1);
    // and comes back as what went in
    let decoded = TraceContext::decode(&raw).expect("a context we wrote was refused");
    assert_eq!(decoded, context);
    assert_eq!(decoded.trace_id(), trace_id);
    assert_eq!(decoded.span_id(), span_id);
    assert!(decoded.is_sampled());
}

/// A trace context whose ids name no parent is never built, and never accepted
///
/// This is the load bearing refusal of the whole feature. `tracing` turns a parent it cannot
/// resolve into `Attributes::new_root`, so a zero id does not produce an orphan somebody would
/// notice - it silently starts a new trace, which is exactly what the context exists to stop.
#[test]
fn a_trace_context_that_names_no_parent_is_refused() {
    // neither id may be all zeroes, on the way in
    assert!(TraceContext::new([0; 16], [1; 8], 1).is_none());
    assert!(TraceContext::new([1; 16], [0; 8], 1).is_none());
    assert!(TraceContext::new([1; 16], [1; 8], 1).is_some());
    // or on the way out, for a peer that set the flag and then wrote one anyway
    let mut raw = TraceContext::new([1; 16], [1; 8], 1)
        .expect("a valid context was refused")
        .encode();
    raw[1..17].fill(0);
    assert_eq!(
        TraceContext::decode(&raw).unwrap_err(),
        ProtocolError::InvalidTraceContext
    );
}

/// A trace context in a version this build does not write is refused rather than guessed at
///
/// The flag bit says a context follows and nothing more, so this is the only thing that can tell a
/// decoder these 26 bytes are not the ones it knows how to read.
#[test]
fn an_unknown_trace_context_version_is_refused() {
    let mut raw = TraceContext::new([7; 16], [3; 8], 0)
        .expect("a valid context was refused")
        .encode();
    raw[0] = TRACE_CONTEXT_VERSION.wrapping_add(1);
    assert_eq!(
        TraceContext::decode(&raw).unwrap_err(),
        ProtocolError::UnknownTraceContextVersion(TRACE_CONTEXT_VERSION.wrapping_add(1))
    );
}

/// A traced request preamble says a context follows, and counts it in its own length
///
/// The length counting the context rather than just the payload is what lets a peer that does not
/// want the context drain the frame anyway, which is the standing rule for every fixed field here.
#[test]
fn a_traced_request_preamble_round_trips() {
    let context = TraceContext::new([9; 16], [4; 8], 1).expect("a valid context was refused");
    // frame a bundle of 4096 payload bytes, carrying the context
    let preamble = request_preamble_traced(Some(&context), 4096, ROOMY).unwrap();
    assert_eq!(preamble.len(), MAX_REQUEST_PREAMBLE_LEN);
    assert!(!preamble.is_empty());
    // the header says a context follows, and counts it
    let mut header_bytes = [0u8; REQUEST_PREAMBLE_LEN];
    header_bytes.copy_from_slice(&preamble.as_bytes()[..REQUEST_PREAMBLE_LEN]);
    let header = decode_request(&header_bytes, ROOMY).unwrap();
    assert!(header.flags.contains(Flags::TRACE_CONTEXT));
    assert_eq!(header.body_len(), 4096 + TRACE_CONTEXT_LEN);
    assert_eq!(header.trace_len(), TRACE_CONTEXT_LEN);
    // and the payload after the context is the length the caller asked to frame
    assert_eq!(header.request_payload_len().unwrap(), 4096);
    // the context itself sits between the two, and is what went in
    let mut context_bytes = [0u8; TRACE_CONTEXT_LEN];
    context_bytes.copy_from_slice(&preamble.as_bytes()[REQUEST_PREAMBLE_LEN..]);
    assert_eq!(TraceContext::decode(&context_bytes).unwrap(), context);
}

/// A bundle with no context to carry is framed exactly as it was before contexts existed
///
/// Most callers are in this arm: a client with no OpenTelemetry layer installed has no context to
/// put on the wire, and must not pay 26 bytes a frame to say so.
#[test]
fn an_untraced_preamble_is_byte_identical() {
    // both routes to a preamble, given the same bundle
    let plain = request_preamble(4096, ROOMY).unwrap();
    let traced = request_preamble_traced(None, 4096, ROOMY).unwrap();
    // the same eight bytes, and no more of them
    assert_eq!(traced.len(), REQUEST_PREAMBLE_LEN);
    assert_eq!(traced.as_bytes(), &plain);
    // which means no flag bit, and a length that is the payload alone
    let header = decode_request(&plain, ROOMY).unwrap();
    assert!(!header.flags.contains(Flags::TRACE_CONTEXT));
    assert_eq!(header.trace_len(), 0);
    assert_eq!(header.request_payload_len().unwrap(), 4096);
}

/// A frame claiming a trace context it is too short to hold is refused before anything reads it
///
/// The subtraction that finds the payload length would otherwise underflow, and a peer would go on
/// to read a body of nearly `usize::MAX` bytes.
#[test]
fn a_traced_frame_too_short_for_its_context_is_refused() {
    // a header that says a context follows and then claims fewer bytes than one takes
    let header = Header::new(
        MessageType::Queries,
        Flags::TRACE_CONTEXT,
        TRACE_CONTEXT_LEN - 1,
        ROOMY,
    )
    .unwrap();
    assert_eq!(
        header.request_payload_len().unwrap_err(),
        ProtocolError::BodyTooShort {
            need: TRACE_CONTEXT_LEN,
            got: (TRACE_CONTEXT_LEN - 1) as u32,
        }
    );
    // a frame carrying nothing but a context is legal, since an empty bundle is a legal bundle
    let empty = Header::new(
        MessageType::Queries,
        Flags::TRACE_CONTEXT,
        TRACE_CONTEXT_LEN,
        ROOMY,
    )
    .unwrap();
    assert_eq!(empty.request_payload_len().unwrap(), 0);
}

/// The trace context flag is bit 4, and does not collide with the four bits that came before it
///
/// A flag bit that moved would be read as a different flag by every peer built from an older
/// commit, which is the same class of break as a renumbered message type.
#[test]
fn the_trace_context_flag_is_its_own_bit() {
    assert_eq!(Flags::TRACE_CONTEXT.bits(), 1 << 4);
    // and none of the bits already spent are set by it
    for other in [
        Flags::IS_ERROR,
        Flags::STALE_TOPOLOGY,
        Flags::LAST,
        Flags::REFUSED,
    ] {
        assert!(!Flags::TRACE_CONTEXT.contains(other));
        assert!(!other.contains(Flags::TRACE_CONTEXT));
    }
}
