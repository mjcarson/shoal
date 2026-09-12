//! The peer protocol's codecs, and the refusals that keep a length from sizing anything

use super::super::trace::TraceContext;
use super::super::{Header, MessageType, ProtocolError, HEADER_LEN, PROTOCOL_VERSION};
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
    for lane in [Lane::Data, Lane::Control, Lane::Bulk] {
        let hello = a_hello(lane);
        assert_eq!(PeerHello::decode(&hello.encode()).unwrap(), hello);
        // the frame is a header naming the type and the body length, then the body
        let frame = hello.frame(1 << 20).unwrap();
        let header = Header::decode(frame[..HEADER_LEN].try_into().unwrap(), 1 << 20).unwrap();
        assert_eq!(header.kind, MessageType::PeerHello);
        assert_eq!(header.body_len(), PEER_HELLO_BODY_LEN);
        assert_eq!(PEER_HELLO_FRAME_LEN, HEADER_LEN + PEER_HELLO_BODY_LEN);
        // an ack carries the reason and everything else
        for reason in [PeerRefusal::Accepted, PeerRefusal::WrongCluster, PeerRefusal::LaneRefused] {
            let ack = PeerHelloAck { hello, reason };
            assert_eq!(PeerHelloAck::decode(&ack.encode()).unwrap(), ack);
            let frame = ack.frame(1 << 20).unwrap();
            let header = Header::decode(frame[..HEADER_LEN].try_into().unwrap(), 1 << 20).unwrap();
            assert_eq!(header.kind, MessageType::PeerHelloAck);
            // a refusal is visible from the header alone
            assert_eq!(header.flags.contains(super::super::Flags::REFUSED), !reason.is_accepted());
        }
    }
    assert!(a_hello(Lane::Data).speaks_our_version());
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
    for raw in [0u8, 4, 255] {
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
            keys: Vec::new(),
        },
    ]
}

/// A forward's preamble and entries round trip, and its bundle length is what is left
#[test]
fn a_forward_round_trips() {
    let entries = some_entries();
    let bytes = encode_entries(&entries).unwrap();
    assert_eq!(bytes.len(), entries.iter().map(ForwardEntry::encoded_len).sum::<usize>());
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
        ForwardPreamble { entries: MAX_FORWARD_ENTRIES + 1, ..good },
        ForwardPreamble { entries_len: MAX_FORWARD_ENTRIES_BYTES + 1, ..good },
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
    assert!(decode_entries(&bytes, 1).is_err(), "a count below the bytes is trailing bytes");
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
    for kind in [ForwardedKind::Whole, ForwardedKind::Share, ForwardedKind::Error] {
        let preamble = ForwardedPreamble {
            bundle: [4; 16],
            index: u64::MAX - 1,
            kind,
            // an opaque classification byte travels as it is
            served: 0x21,
        };
        assert_eq!(ForwardedPreamble::decode(&preamble.encode()).unwrap(), preamble);
    }
    let mut raw = ForwardedPreamble {
        bundle: [0; 16],
        index: 0,
        kind: ForwardedKind::Whole,
        served: 0,
    }
    .encode();
    raw[24] = 4;
    assert_eq!(
        ForwardedPreamble::decode(&raw),
        Err(ProtocolError::UnknownForwardedKind(4))
    );
    let payload = encode_error_payload(32, "the peer went away");
    assert_eq!(decode_error_payload(&payload).unwrap(), (32, "the peer went away".to_string()));
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
    assert_eq!(ControlRequestHead::decode(&raw), Err(ProtocolError::UnknownControlKind(0)));
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
    assert_eq!(ControlResponseHead::decode(&raw).status, ControlStatus::Error);
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
    assert_eq!(SnapshotBegin::decode(&begin.encode(), SNAPSHOT_BEGIN_LEN + 10).unwrap(), begin);
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
