//! Tests for the wire framing
//!
//! These are the tests that hold the format itself still. Everything else in the protocol is
//! exercised by the integration suite, but a discriminant that quietly changed, or a flag bit that
//! moved, would still pass every one of those tests while breaking every peer built from an older
//! commit.

use uuid::Uuid;

use super::fingerprint::{self, ROLE_FILTER, ROLE_PARTITION, ROLE_SORT, ROLE_UPDATE};
use super::handshake::{Hello, HelloAck, RefusalReason, HANDSHAKE_BODY_LEN, HANDSHAKE_FRAME_LEN};
use super::{
    decode_request, decode_response, request_preamble, response_preamble, Flags, Header,
    MessageType, ProtocolError, RawHeader, HEADER_LEN, PROTOCOL_VERSION, QUERY_ID_LEN,
    REQUEST_PREAMBLE_LEN, RESPONSE_PREAMBLE_LEN,
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
    ] {
        let ack = HelloAck {
            schema_fingerprint: 0x0102_0304_0506_0708,
            max_frame_bytes: 8192,
            reason,
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
    for raw in [3u8, 42, 255] {
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
