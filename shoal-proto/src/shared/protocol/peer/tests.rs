//! The peer protocol's codecs, and the refusals that keep a length from sizing anything

use super::super::read::{ReadLevel, SessionToken};
use super::super::trace::TraceContext;
use super::super::{
    Header, MessageType, ProtocolError, HEADER_LEN, MIN_PEER_VERSION, PROTOCOL_VERSION,
};
use super::*;

/// A hello with every field set to something distinguishable
fn a_hello(lane: Lane) -> PeerHello {
    PeerHello {
        cluster: [1; 16],
        node: [2; 16],
        incarnation: 0x0102_0304_0506_0708,
        lane,
        wire_min: PROTOCOL_VERSION,
        wire_max: PROTOCOL_VERSION,
        capabilities: CAPABILITIES,
        schema_id: 0xdead_beef_cafe_f00d,
        shards: 12,
        max_frame_bytes: 1 << 20,
    }
}

/// A hello and an ack each round trip through their bodies and their frames
#[test]
fn a_peer_hello_and_its_ack_round_trip() {
    for lane in [Lane::Data, Lane::Control, Lane::Bulk, Lane::Replication] {
        let hello = a_hello(lane);
        assert_eq!(PeerHello::decode(&hello.encode()).unwrap(), hello);
        // the frame is a header naming the type and the body length, then the body
        let frame = hello.frame(1 << 20).unwrap();
        let header = Header::decode(frame[..HEADER_LEN].try_into().unwrap(), 1 << 20).unwrap();
        assert_eq!(header.kind, MessageType::PeerHello);
        assert_eq!(header.body_len(), PEER_HELLO_BODY_LEN);
        assert_eq!(PEER_HELLO_FRAME_LEN, HEADER_LEN + PEER_HELLO_BODY_LEN);
        // an ack carries the reason and everything else
        for reason in [
            PeerRefusal::Accepted,
            PeerRefusal::WrongCluster,
            PeerRefusal::LaneRefused,
        ] {
            let ack = PeerHelloAck { hello, reason };
            assert_eq!(PeerHelloAck::decode(&ack.encode()).unwrap(), ack);
            let frame = ack.frame(1 << 20).unwrap();
            let header = Header::decode(frame[..HEADER_LEN].try_into().unwrap(), 1 << 20).unwrap();
            assert_eq!(header.kind, MessageType::PeerHelloAck);
            // a refusal is visible from the header alone
            assert_eq!(
                header.flags.contains(super::super::Flags::REFUSED),
                !reason.is_accepted()
            );
        }
    }
    assert_eq!(
        a_hello(Lane::Data).negotiate(&a_hello(Lane::Data)),
        Some(PROTOCOL_VERSION)
    );
    // the hello frame is written at the floor, so a peer anywhere in the range reads it
    let frame = a_hello(Lane::Data).frame(1 << 20).unwrap();
    assert_eq!(frame[0], MIN_PEER_VERSION);
}

/// A version range negotiates to the highest both read, and the capabilities intersect
///
/// The Q10 contract as [F48](../../../../../docs/src/features/rolling-compatibility.md)
/// delivers it: the wire version is a range on each side and the two speak the highest
/// version both hold; disjoint ranges share nothing; a peer above us is spoken to at our
/// newest; and what both can act on is the intersection of the capability words.
#[test]
fn a_version_range_negotiates_to_the_highest_shared() {
    // a hello with a range of its own
    let ranged = |min: u8, max: u8| PeerHello {
        wire_min: min,
        wire_max: max,
        ..a_hello(Lane::Data)
    };
    // the table: (ours, theirs, spoken)
    let table = [
        ((4, 5), (4, 5), Some(5)),
        ((4, 5), (4, 4), Some(4)),
        ((4, 4), (4, 5), Some(4)),
        ((4, 5), (5, 5), Some(5)),
        ((4, 5), (5, 6), Some(5)),
        ((4, 5), (6, 7), None),
        ((6, 7), (4, 5), None),
        ((4, 4), (5, 5), None),
        ((5, 5), (4, 4), None),
        ((4, 6), (5, 9), Some(6)),
    ];
    for ((our_min, our_max), (their_min, their_max), spoken) in table {
        let ours = ranged(our_min, our_max);
        let theirs = ranged(their_min, their_max);
        assert_eq!(
            theirs.negotiate(&ours),
            spoken,
            "ours {our_min}..={our_max} theirs {their_min}..={their_max}"
        );
        // and the same from the other side
        assert_eq!(ours.negotiate(&theirs), spoken);
    }
    // the range this build advertises is the floor to the newest, and a pin lowers the top
    assert_eq!(PeerHello::range(None), (MIN_PEER_VERSION, PROTOCOL_VERSION));
    assert_eq!(
        PeerHello::range(Some(MIN_PEER_VERSION)),
        (MIN_PEER_VERSION, MIN_PEER_VERSION)
    );
    assert_eq!(
        PeerHello::range(Some(PROTOCOL_VERSION + 3)),
        (MIN_PEER_VERSION, PROTOCOL_VERSION)
    );
    assert_eq!(
        PeerHello::range(Some(0)),
        (MIN_PEER_VERSION, MIN_PEER_VERSION)
    );
    // the capabilities both act on are the intersection
    let ours = PeerHello {
        capabilities: CAP_FORWARD_V1 | CAP_REPLICATION_V1,
        ..a_hello(Lane::Data)
    };
    let theirs = PeerHello {
        capabilities: CAP_REPLICATION_V1 | CAP_MEMBERSHIP_V1,
        ..a_hello(Lane::Data)
    };
    assert_eq!(theirs.common_capabilities(&ours), CAP_REPLICATION_V1);
    assert_eq!(
        a_hello(Lane::Data).common_capabilities(&a_hello(Lane::Data)),
        CAPABILITIES
    );
    // every required capability is one this build acts on
    assert_eq!(REQUIRED_CAPABILITIES & CAPABILITIES, REQUIRED_CAPABILITIES);
}

/// A pre-vote is its own kind, and its capability is advertised but never required (item 144)
///
/// A build without pre-vote is still a member: the bit being optional is what lets a rolling
/// upgrade reach a cluster of builds that all answer it.
#[test]
fn pre_vote_is_an_optional_capability() {
    // the kind is eleven and round trips
    assert_eq!(ReplicateKind::from_byte(11).unwrap(), ReplicateKind::PreVote);
    assert_eq!(ReplicateKind::PreVote.as_byte(), 11);
    assert_eq!(ReplicateKind::PreVote.name(), "pre_vote");
    // this build acts on it
    assert_eq!(CAP_PRE_VOTE_V1, 1 << 6);
    assert_ne!(CAPABILITIES & CAP_PRE_VOTE_V1, 0);
    // the control lane's kind is eight
    assert_eq!(ControlKind::from_byte(8).unwrap(), ControlKind::PreVote);
    assert_eq!(ControlKind::PreVote.as_byte(), 8);
    // and a peer that does not is not refused for it
    assert_eq!(REQUIRED_CAPABILITIES & CAP_PRE_VOTE_V1, 0);
}

/// Every refusal is pinned to its byte, and an unknown byte is still a refusal
#[test]
fn every_peer_refusal_round_trips_and_unknown_fails_closed() {
    let pinned = [
        (PeerRefusal::Accepted, 0u8),
        (PeerRefusal::WrongCluster, 1),
        (PeerRefusal::UnknownNode, 2),
        (PeerRefusal::IdentityMismatch, 3),
        (PeerRefusal::NoCommonVersion, 4),
        (PeerRefusal::SchemaMismatch, 5),
        (PeerRefusal::ShardCountMismatch, 6),
        (PeerRefusal::LaneRefused, 7),
        (PeerRefusal::Unauthorized, 8),
        (PeerRefusal::Fenced, 10),
        (PeerRefusal::DuplicateIdentity, 11),
        (PeerRefusal::NotJoinable, 12),
        (PeerRefusal::BelowActivatedWire, 13),
        (PeerRefusal::Removed, 14),
        (PeerRefusal::CapabilityMissing, 15),
    ];
    for (reason, byte) in pinned {
        assert_eq!(reason.as_byte(), byte);
        assert_eq!(PeerRefusal::from_byte(byte), reason);
    }
    for raw in [9u8, 42, 254, 255] {
        let read = PeerRefusal::from_byte(raw);
        assert!(!read.is_accepted(), "byte {raw} read as an acceptance");
        assert_eq!(read, PeerRefusal::Unrecognized);
    }
    // an unknown lane byte is refused rather than defaulted, zero included
    for raw in [0u8, 5, 255] {
        assert_eq!(Lane::from_byte(raw), Err(ProtocolError::UnknownLane(raw)));
    }
    // and a hello naming one does not decode
    let mut body = a_hello(Lane::Data).encode();
    body[40] = 9;
    assert!(PeerHello::decode(&body).is_err());
}

/// Entries of every shape round trip through their bytes
fn some_entries() -> Vec<ForwardEntry> {
    vec![
        ForwardEntry {
            offset: 0,
            index: 7,
            end: false,
            shard: 3,
            origin_shard: 1,
            gather: true,
            trace: TraceContext::new([9; 16], [8; 8], 1),
            read: None,
            keys: vec![1, u64::MAX, 42],
        },
        ForwardEntry {
            offset: 5,
            index: 12,
            end: true,
            shard: 0,
            origin_shard: 0,
            gather: false,
            trace: None,
            read: None,
            keys: Vec::new(),
        },
    ]
}

/// A forward's preamble and entries round trip, and its bundle length is what is left
#[test]
fn a_forward_round_trips() {
    let entries = some_entries();
    let bytes = encode_entries(&entries).unwrap();
    assert_eq!(
        bytes.len(),
        entries.iter().map(ForwardEntry::encoded_len).sum::<usize>()
    );
    assert_eq!(decode_entries(&bytes, 2).unwrap(), entries);
    let preamble = ForwardPreamble {
        bundle: [3; 16],
        attempt: 99,
        base_index: 7,
        hops: 0,
        remaining_ms: 5000,
        entries: 2,
        entries_len: bytes.len() as u32,
    };
    // a frame holding the preamble, the entries and a hundred bundle bytes
    let body_len = FORWARD_PREAMBLE_LEN + bytes.len() + 100;
    let decoded = ForwardPreamble::decode(&preamble.encode(), body_len).unwrap();
    assert_eq!(decoded, preamble);
    assert_eq!(decoded.bundle_len(body_len), 100);
}

/// Every way a forward can lie about its lengths is refused before anything is sized by it
#[test]
fn a_malformed_forward_is_refused_before_allocation() {
    let entries = some_entries();
    let bytes = encode_entries(&entries).unwrap();
    let good = ForwardPreamble {
        bundle: [3; 16],
        attempt: 0,
        base_index: 0,
        hops: 0,
        remaining_ms: 1,
        entries: 2,
        entries_len: bytes.len() as u32,
    };
    let body_len = FORWARD_PREAMBLE_LEN + bytes.len() + 8;
    // a hop count is a loop
    let looped = ForwardPreamble { hops: 1, ..good };
    assert!(matches!(
        ForwardPreamble::decode(&looped.encode(), body_len),
        Err(ProtocolError::MalformedForward(_))
    ));
    // no entries, too many entries, entries past their byte bound
    for bad in [
        ForwardPreamble { entries: 0, ..good },
        ForwardPreamble {
            entries: MAX_FORWARD_ENTRIES + 1,
            ..good
        },
        ForwardPreamble {
            entries_len: MAX_FORWARD_ENTRIES_BYTES + 1,
            ..good
        },
    ] {
        assert!(matches!(
            ForwardPreamble::decode(&bad.encode(), usize::MAX / 2),
            Err(ProtocolError::MalformedForward(_))
        ));
    }
    // a frame too short to hold a bundle after its entries
    assert!(ForwardPreamble::decode(&good.encode(), FORWARD_PREAMBLE_LEN + bytes.len()).is_err());
    // entries cut short, entries with trailing bytes, an unknown flag, too many keys
    assert!(decode_entries(&bytes[..bytes.len() - 1], 2).is_err());
    let mut longer = bytes.clone();
    longer.push(0);
    assert!(decode_entries(&longer, 2).is_err());
    assert!(
        decode_entries(&bytes, 1).is_err(),
        "a count below the bytes is trailing bytes"
    );
    let mut flagged = bytes.clone();
    flagged[16] |= 1 << 7;
    assert!(decode_entries(&flagged, 2).is_err());
    let mut keys = bytes.clone();
    keys[18..22].copy_from_slice(&(MAX_FORWARD_KEYS + 1).to_le_bytes());
    assert!(decode_entries(&keys, 2).is_err());
    // the encoder refuses what the decoder would
    let too_many_keys = vec![ForwardEntry {
        keys: vec![0; MAX_FORWARD_KEYS as usize + 1],
        ..entries[1].clone()
    }];
    assert!(encode_entries(&too_many_keys).is_err());
    assert!(encode_entries(&[]).is_err());
}

/// A forwarded answer's preamble round trips, and its error payload does too
#[test]
fn a_forwarded_answer_round_trips() {
    for kind in [
        ForwardedKind::Whole,
        ForwardedKind::Share,
        ForwardedKind::Error,
    ] {
        let preamble = ForwardedPreamble {
            bundle: [4; 16],
            index: u64::MAX - 1,
            kind,
            // an opaque classification byte travels as it is
            served: 0x21,
            attempt: 0,
            slot: 0,
            token: None,
        };
        assert_eq!(
            ForwardedPreamble::decode(&preamble.encode()).unwrap(),
            preamble
        );
    }
    let mut raw = ForwardedPreamble {
        bundle: [0; 16],
        index: 0,
        kind: ForwardedKind::Whole,
        served: 0,
        attempt: 0,
        slot: 0,
        token: None,
    }
    .encode();
    raw[24] = 4;
    assert_eq!(
        ForwardedPreamble::decode(&raw),
        Err(ProtocolError::UnknownForwardedKind(4))
    );
    let payload = encode_error_payload(32, "the peer went away");
    assert_eq!(
        decode_error_payload(&payload).unwrap(),
        (32, "the peer went away".to_string())
    );
    assert!(decode_error_payload(&[1]).is_err());
}

/// Control heads round trip and an unknown kind is refused
#[test]
fn control_heads_round_trip() {
    for kind in [
        ControlKind::AppendEntries,
        ControlKind::Vote,
        ControlKind::Snapshot,
        ControlKind::Ping,
    ] {
        let head = ControlRequestHead {
            id: 0xfeed,
            kind,
            deadline_ms: 1500,
        };
        assert_eq!(ControlRequestHead::decode(&head.encode()).unwrap(), head);
    }
    let mut raw = ControlRequestHead {
        id: 1,
        kind: ControlKind::Ping,
        deadline_ms: 0,
    }
    .encode();
    raw[8] = 0;
    assert_eq!(
        ControlRequestHead::decode(&raw),
        Err(ProtocolError::UnknownControlKind(0))
    );
    for status in [ControlStatus::Ok, ControlStatus::Error] {
        let head = ControlResponseHead { id: 77, status };
        assert_eq!(ControlResponseHead::decode(&head.encode()), head);
    }
    // an unknown status is a failure, never an answer
    let mut raw = ControlResponseHead {
        id: 1,
        status: ControlStatus::Ok,
    }
    .encode();
    raw[8] = 200;
    assert_eq!(
        ControlResponseHead::decode(&raw).status,
        ControlStatus::Error
    );
}

/// Snapshot heads round trip, fit their frames, and a chunk checks its bytes
#[test]
fn snapshot_frames_round_trip_and_checksum() {
    let begin = SnapshotBegin {
        stream: [5; 16],
        transition: [6; 16],
        boundary: 1000,
        total: 1 << 30,
        manifest_len: 10,
    };
    assert_eq!(
        SnapshotBegin::decode(&begin.encode(), SNAPSHOT_BEGIN_LEN + 10).unwrap(),
        begin
    );
    assert!(SnapshotBegin::decode(&begin.encode(), SNAPSHOT_BEGIN_LEN + 9).is_err());
    let bytes = b"sixteen bytes!!!";
    let chunk = SnapshotChunk {
        stream: [5; 16],
        offset: 4096,
        len: bytes.len() as u32,
        checksum: checksum(bytes),
    };
    let decoded = SnapshotChunk::decode(&chunk.encode(), SNAPSHOT_CHUNK_LEN + bytes.len()).unwrap();
    assert_eq!(decoded, chunk);
    decoded.verify(bytes).unwrap();
    assert!(matches!(
        decoded.verify(b"sixteen bytes!!?"),
        Err(ProtocolError::SnapshotChecksum { .. })
    ));
    assert!(SnapshotChunk::decode(&chunk.encode(), SNAPSHOT_CHUNK_LEN).is_err());
    for status in [SnapshotStatus::Complete, SnapshotStatus::Aborted] {
        let end = SnapshotEnd {
            stream: [5; 16],
            total: 1 << 30,
            checksum: 7,
            status,
            resume_from: 4096,
        };
        assert_eq!(SnapshotEnd::decode(&end.encode()), end);
    }
}

/// A token for the entry and answer tests
///
/// # Arguments
///
/// * `index` - The index to put in it
fn a_token(index: u64) -> SessionToken {
    SessionToken {
        cluster: crate::shared::identity::ClusterId(uuid::Uuid::from_bytes([7; 16])),
        table: crate::shared::identity::TableId(11),
        tablet: 300,
        group: crate::shared::identity::GroupId(0xabcd),
        index,
    }
}

/// An entry's read plan and an answer's attempt, slot and token round trip; the refused shapes
/// are refused (F41)
#[test]
fn forward_entries_carry_read_plans_and_answers_carry_attempts() {
    // an entry with a plan, between a context and its keys
    let planned = ForwardEntry {
        offset: 2,
        index: 9,
        end: false,
        shard: 1,
        origin_shard: 2,
        gather: true,
        trace: TraceContext::new([3; 16], [2; 8], 1),
        read: Some(EntryRead {
            level: ReadLevel::Quorum,
            slot: 5,
            tokens: vec![a_token(1), a_token(2)],
        }),
        keys: vec![10, 20],
    };
    let bare = ForwardEntry {
        read: Some(EntryRead {
            level: ReadLevel::One,
            slot: 0,
            tokens: Vec::new(),
        }),
        trace: None,
        keys: Vec::new(),
        ..planned.clone()
    };
    let entries = vec![planned.clone(), bare.clone()];
    let bytes = encode_entries(&entries).unwrap();
    assert_eq!(bytes.len(), planned.encoded_len() + bare.encoded_len());
    assert_eq!(decode_entries(&bytes, 2).unwrap(), entries);
    // a plan that inherits its level is not a plan: resolution happened on the coordinator
    let mut inherit = encode_entries(&[bare.clone()]).unwrap();
    inherit[22] = 0;
    assert!(decode_entries(&inherit, 1).is_err());
    // a plan with too many tokens is refused on the way out
    let over = ForwardEntry {
        read: Some(EntryRead {
            level: ReadLevel::One,
            slot: 0,
            tokens: (0..17).map(a_token).collect(),
        }),
        ..bare.clone()
    };
    assert!(encode_entries(&[over]).is_err());
    // the answer head is ninety six bytes, with and without a token
    for token in [None, Some(a_token(77))] {
        let preamble = ForwardedPreamble {
            bundle: [1; 16],
            index: 3,
            kind: ForwardedKind::Whole,
            served: 0x10,
            attempt: u64::MAX - 5,
            slot: 4,
            token,
        };
        let raw = preamble.encode();
        assert_eq!(raw.len(), FORWARDED_PREAMBLE_LEN);
        assert_eq!(ForwardedPreamble::decode(&raw).unwrap(), preamble);
    }
    // an unknown answer flag is refused
    let mut flagged = ForwardedPreamble {
        bundle: [0; 16],
        index: 0,
        kind: ForwardedKind::Share,
        served: 0,
        attempt: 0,
        slot: 0,
        token: None,
    }
    .encode();
    flagged[26] = 1 << 4;
    assert!(ForwardedPreamble::decode(&flagged).is_err());
    // the read barrier kind is five
    assert_eq!(
        ReplicateKind::from_byte(5).unwrap(),
        ReplicateKind::ReadBarrier
    );
    assert_eq!(ReplicateKind::ReadBarrier.as_byte(), 5);
    // and the capability bit is its own
    assert_eq!(CAP_READ_CONSISTENCY_V1, 1 << 5);
    assert_ne!(CAPABILITIES & CAP_READ_CONSISTENCY_V1, 0);
}
