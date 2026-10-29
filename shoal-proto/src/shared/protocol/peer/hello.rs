//! The two frames a peer connection opens with
//!
//! ```text
//!  ┌────────────┬────────────┬─────────────┬──────┬─────────┬─────────┬────────┬──────────────┬───────────┬────────┬──────────┬─────────────────┐
//!  │ cluster id │  node id   │ incarnation │ lane │ wire min│ wire max│ reason │ capabilities │ schema id │ shards │ slot + 1 │ max frame bytes │
//!  │   (16 B)   │   (16 B)   │  (u64, 8 B) │ (1 B)│  (1 B)  │  (1 B)  │ (1 B)  │  (u64, 8 B)  │(u64, 8 B) │(u16,2B)│(u16, 2 B)│   (u32, 4 B)    │
//!  └────────────┴────────────┴─────────────┴──────┴─────────┴─────────┴────────┴──────────────┴───────────┴────────┴──────────┴─────────────────┘
//! ```
//!
//! A hello and its ack have one layout. The dialler writes its own record with `reason` zero;
//! the acceptor answers with *its* record and the reason it accepted or refused, so both ends
//! learn the other's identity, incarnation, shard count and capabilities from one frame each.
//!
//! **Schema identity and wire version are separate fields**, which is the Q10 contract
//! ([C13](../../../../../docs/src/distributed/protocol.md)): `schema_id` is the structural
//! fingerprint of the schema alone, `wire_min..=wire_max` the versions of this framing the peer
//! reads, and `capabilities` the peer features it can act on. A client hello folds the protocol
//! version into its fingerprint and that stays as it is; a peer keeps the three apart so that a
//! rolling upgrade can tell "different schema" from "newer transport" from "same everything".
//! ~~At M2 all three are required to match exactly - the contract is defined here, and the
//! codecs for an older version are what M10 owes before n−1 is accepted.~~ Since
//! [F48](../../../../../docs/src/features/rolling-compatibility.md) the three are judged
//! apart: the schema id exactly, the wire version as the highest both ranges hold
//! ([`PeerHello::negotiate`]), and the capabilities as the intersection
//! ([`PeerHello::common_capabilities`]). The hello frame itself is written at
//! [`MIN_PEER_VERSION`] so that any peer in the range reads it.
//!
//! **A reserved pair of bytes was spent, and not moved,** the way the client hello spends its
//! own. Since [F84](../../../../../docs/src/features/replication-lane-handoff.md) the two bytes
//! after `shards` name the slot a lane's frames are for, as the slot plus one, zero for none: a
//! peer built before it wrote zeroes there and read nothing, which is "no slot" both ways. An
//! acceptor acts on it for the replication lane alone, by handing the connection to the executor
//! hosting that slot; the ack always names none. No capability bit says so, because nothing about
//! a frame changes and the handoff is the acceptor's choice: an acceptor that ignores the slot
//! serves the lane where it landed, with the hop every lane took before.
//!
//! **An object lane's hello is longer by a slice.** Since
//! [F86](../../../../../docs/src/features/replicated-pools.md) the hello of a [`Lane::Object`]
//! connection carries, after the record above, the sixteen bytes of the slice its frames are for,
//! which the acceptor hands the connection to the owner of. Every other lane's hello is the record
//! alone, so a peer of any build reads it; a build from before F86 drains the longer hello's last
//! sixteen bytes as growth, and refuses the lane it does not serve by name.
//!
//! **Every hello and every ack ends with its sender's lineage.** Since
//! [F93](../../../../../docs/src/features/operations-and-the-real-cluster.md) the bytes after the
//! record (and after an object lane's slice) are the lineage of the schema the sender serves: a
//! count byte, then that many ids. A peer is admitted where one lineage is a prefix of the other,
//! which is an append of tables or buckets in either direction, and refused where it does not hold
//! the schema the cluster committed. `schema_id` stays in the record as the lineage's last element,
//! and a hello that carries no lineage is read as the lineage of that id alone.

use super::super::{
    Flags, Header, MessageType, ProtocolError, HEADER_LEN, MIN_PEER_VERSION, PROTOCOL_VERSION,
};
use super::{bytes16_at, u16_at, u32_at, u64_at, Lane, PeerRefusal};

/// The size of a peer hello body in bytes
pub const PEER_HELLO_BODY_LEN: usize = 68;

/// The size of a whole peer hello frame, header included
pub const PEER_HELLO_FRAME_LEN: usize = HEADER_LEN + PEER_HELLO_BODY_LEN;

/// The bytes an object lane's hello carries after the record: the slice its frames are for
pub const OBJECT_HELLO_EXTENSION: usize = 16;

/// The most ids a hello's lineage carries, one a field of its sender's schema
pub const MAX_LINEAGE: usize = 255;

/// Write a lineage after a hello's or an ack's record: a count byte, then that many ids
///
/// A lineage longer than [`MAX_LINEAGE`] is cut to its last [`MAX_LINEAGE`] ids, which still end
/// at the schema's own id; a peer judges it where it overlaps its own, so a cut lineage is the
/// same schema as the whole one it was cut from
/// ([Resolved #271](../../../../../docs/src/appendix/resolved/lineage-judged-where-it-overlaps.md)).
///
/// # Arguments
///
/// * `lineage` - The sender's lineage
/// * `into` - The body the lineage goes after
pub fn write_lineage(lineage: &[u64], into: &mut Vec<u8>) {
    // the last ids, which end at the schema's own
    let kept = &lineage[lineage.len().saturating_sub(MAX_LINEAGE)..];
    // the count fits a byte, since it is at most MAX_LINEAGE
    #[allow(clippy::cast_possible_truncation)]
    into.push(kept.len() as u8);
    for id in kept {
        into.extend_from_slice(&id.to_le_bytes());
    }
}

/// Read the lineage a hello's or an ack's growth carries
///
/// The lineage of `schema_id` alone when the growth carries none, or carries one that does not
/// end at it, so a peer is never judged by a lineage that contradicts its own record.
///
/// # Arguments
///
/// * `rest` - The bytes after the record, an object lane hello's slice already taken off
/// * `schema_id` - The schema id the record names
#[must_use]
pub fn read_lineage(rest: &[u8], schema_id: u64) -> Vec<u64> {
    // a count byte, then that many ids
    let Some((count, ids)) = rest.split_first() else {
        return vec![schema_id];
    };
    let count = usize::from(*count);
    if count == 0 || ids.len() < count * 8 {
        return vec![schema_id];
    }
    let lineage: Vec<u64> = ids[..count * 8]
        .chunks_exact(8)
        .map(|word| u64::from_le_bytes(word.try_into().unwrap_or_default()))
        .collect();
    // a lineage is believed only when it ends where the record says the schema is
    if lineage.last() == Some(&schema_id) {
        lineage
    } else {
        vec![schema_id]
    }
}

/// This peer forwards bundles and answers them
pub const CAP_FORWARD_V1: u64 = 1 << 0;

/// This peer carries the control group's consensus RPCs on its control lane
pub const CAP_CONTROL_RAFT_V1: u64 = 1 << 1;

/// This peer carries snapshot streams on its bulk lane
pub const CAP_BULK_SNAPSHOT_V1: u64 = 1 << 2;

/// This peer admits joiners over its control lane and carries membership RPCs and status reports
pub const CAP_MEMBERSHIP_V1: u64 = 1 << 3;

/// This peer hosts tablet groups and carries their consensus RPCs on its replication lane
pub const CAP_REPLICATION_V1: u64 = 1 << 4;

/// This peer carries read plans on its forward entries, attempts and tokens on its answers, and
/// read barriers on its replication lane ([F41](../../../../../docs/src/features/read-consistency.md))
pub const CAP_READ_CONSISTENCY_V1: u64 = 1 << 5;

/// This peer answers the consensus library's pre-vote on its replication lane
///
/// Optional: a peer without it is still a member, and a pre-vote to it is granted locally, the
/// library's own default, rather than sent as a kind it would refuse to decode
/// ([Resolved #144](../../../../../docs/src/appendix/resolved/post-heal-elections.md)).
pub const CAP_PRE_VOTE_V1: u64 = 1 << 6;

/// This peer serves the object lane: stripe chunks staged, written, applied, folded and read on
/// the executor of the slice each connection names
///
/// Optional: a peer without it is still a member, and no object lane is dialled to it - a pool
/// whose holder is on such a peer counts that holder missed - and nothing proposes a stripe write
/// until [`super::super::OBJECT_WIRE_VERSION`] is activated
/// ([F86](../../../../../docs/src/features/replicated-pools.md)).
pub const CAP_OBJECT_LANE_V1: u64 = 1 << 8;

/// This peer acts on a `Cancel` on its data lane, naming a forwarded bundle and the attempts
/// before a bound
///
/// Optional: a peer without it is still a member, and is sent no cancel - its shares run to
/// the end and their answers are dropped at the origin, which is all a cancel did before
/// [F75](../../../../../docs/src/features/client-cancel.md). A data lane's reader refuses any
/// other frame than a forward by ending the lane, so the bit is what keeps a cancel off a lane
/// whose reader was built before it.
pub const CAP_CANCEL_V1: u64 = 1 << 7;

/// Everything this build can act on
pub const CAPABILITIES: u64 = CAP_FORWARD_V1
    | CAP_CONTROL_RAFT_V1
    | CAP_BULK_SNAPSHOT_V1
    | CAP_MEMBERSHIP_V1
    | CAP_REPLICATION_V1
    | CAP_READ_CONSISTENCY_V1
    | CAP_PRE_VOTE_V1
    | CAP_CANCEL_V1
    | CAP_OBJECT_LANE_V1;

/// The capabilities a peer has to act on to be a member at all
///
/// Every bit above but [`CAP_PRE_VOTE_V1`], [`CAP_CANCEL_V1`] and [`CAP_OBJECT_LANE_V1`]: each is what some version in [`MIN_PEER_VERSION`]`..=`[`PROTOCOL_VERSION`]
/// carries, so a peer in the range without one is a build this one does not know how to
/// half serve, and is refused ([`PeerRefusal::CapabilityMissing`]). A capability a future
/// build adds as optional is left out of this set and gated by `Negotiated::has` at the one
/// place it is acted on, the way `CLIENT_CAP_READ_OPTIONS` gates a client
/// ([F48](../../../../../docs/src/features/rolling-compatibility.md)).
pub const REQUIRED_CAPABILITIES: u64 = CAP_FORWARD_V1
    | CAP_CONTROL_RAFT_V1
    | CAP_BULK_SNAPSHOT_V1
    | CAP_MEMBERSHIP_V1
    | CAP_REPLICATION_V1
    | CAP_READ_CONSISTENCY_V1;

/// Where each field sits in the body
const CLUSTER_AT: usize = 0;
const NODE_AT: usize = 16;
const INCARNATION_AT: usize = 32;
const LANE_AT: usize = 40;
const WIRE_MIN_AT: usize = 41;
const WIRE_MAX_AT: usize = 42;
const REASON_AT: usize = 43;
const CAPABILITIES_AT: usize = 44;
const SCHEMA_AT: usize = 52;
const SHARDS_AT: usize = 60;
const SLOT_AT: usize = 62;
const MAX_FRAME_AT: usize = 64;

/// What a node says about itself when it opens, or answers, a peer connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHello {
    /// The cluster this node belongs to, as the uuid's bytes
    pub cluster: [u8; 16],
    /// This node's identity, as the uuid's bytes
    pub node: [u8; 16],
    /// Which run of this node this is
    ///
    /// Strictly greater for a later start of the same node, so a peer can tell a restart from a
    /// duplicate. At M2 it is the process start time in nanoseconds since the epoch, which is
    /// recorded as provisional under Q11.
    pub incarnation: u64,
    /// Which lane this connection carries
    pub lane: Lane,
    /// The oldest wire version this build reads
    pub wire_min: u8,
    /// The newest wire version this build reads
    pub wire_max: u8,
    /// What this build can act on, as [`CAPABILITIES`] bits
    pub capabilities: u64,
    /// The structural fingerprint of the schema this node serves
    pub schema_id: u64,
    /// How many shards this node runs, which is what the placement has to agree with
    pub shards: u16,
    /// The slot on the acceptor this connection's frames are for, if it carries one slot's
    ///
    /// Written as the slot plus one in what were two reserved bytes, zero for none, so a build
    /// before [F84](../../../../../docs/src/features/replication-lane-handoff.md) writes and reads
    /// none. A replication lane names its member's slot, and the acceptor hands the connection to
    /// the executor hosting it; every other lane, and every ack, names none.
    pub slot: Option<u16>,
    /// The slice on the acceptor this object lane's frames are for, as the slice id's bytes
    ///
    /// Written after the record, on an object lane's hello alone; every other lane, and every
    /// ack, names none ([F86](../../../../../docs/src/features/replicated-pools.md)).
    pub slice: Option<[u8; 16]>,
    /// The largest frame this node will accept on this connection
    pub max_frame_bytes: u32,
}

impl PeerHello {
    /// Write this hello's body, with a reason byte of zero
    ///
    /// # Arguments
    ///
    /// * `reason` - The reason byte to write, which is zero for a hello
    fn encode_with(&self, reason: PeerRefusal) -> [u8; PEER_HELLO_BODY_LEN] {
        let mut body = [0u8; PEER_HELLO_BODY_LEN];
        body[CLUSTER_AT..CLUSTER_AT + 16].copy_from_slice(&self.cluster);
        body[NODE_AT..NODE_AT + 16].copy_from_slice(&self.node);
        body[INCARNATION_AT..INCARNATION_AT + 8].copy_from_slice(&self.incarnation.to_le_bytes());
        body[LANE_AT] = self.lane.as_byte();
        body[WIRE_MIN_AT] = self.wire_min;
        body[WIRE_MAX_AT] = self.wire_max;
        body[REASON_AT] = reason.as_byte();
        body[CAPABILITIES_AT..CAPABILITIES_AT + 8]
            .copy_from_slice(&self.capabilities.to_le_bytes());
        body[SCHEMA_AT..SCHEMA_AT + 8].copy_from_slice(&self.schema_id.to_le_bytes());
        body[SHARDS_AT..SHARDS_AT + 2].copy_from_slice(&self.shards.to_le_bytes());
        // the slot plus one, zero for none; a slot with no successor names none and is served
        // where it lands, which costs a hop and never an error
        let slot = self.slot.and_then(|slot| slot.checked_add(1)).unwrap_or(0);
        body[SLOT_AT..SLOT_AT + 2].copy_from_slice(&slot.to_le_bytes());
        body[MAX_FRAME_AT..MAX_FRAME_AT + 4].copy_from_slice(&self.max_frame_bytes.to_le_bytes());
        body
    }

    /// Write this hello's body
    #[must_use]
    pub fn encode(&self) -> [u8; PEER_HELLO_BODY_LEN] {
        self.encode_with(PeerRefusal::Accepted)
    }

    /// Read a hello's fields out of a body, leaving the reason byte to the caller
    ///
    /// # Arguments
    ///
    /// * `body` - The body to read
    fn decode_fields(body: &[u8; PEER_HELLO_BODY_LEN]) -> Result<Self, ProtocolError> {
        Ok(PeerHello {
            cluster: bytes16_at(body, CLUSTER_AT),
            node: bytes16_at(body, NODE_AT),
            incarnation: u64_at(body, INCARNATION_AT),
            lane: Lane::from_byte(body[LANE_AT])?,
            wire_min: body[WIRE_MIN_AT],
            wire_max: body[WIRE_MAX_AT],
            capabilities: u64_at(body, CAPABILITIES_AT),
            schema_id: u64_at(body, SCHEMA_AT),
            shards: u16_at(body, SHARDS_AT),
            // zero is none, which is what a build before the slot wrote
            slot: u16_at(body, SLOT_AT).checked_sub(1),
            // a slice follows the record, and is read from what follows it
            slice: None,
            max_frame_bytes: u32_at(body, MAX_FRAME_AT),
        })
    }

    /// Read the slice an object lane's hello names and the lineage after it, from the bytes after
    /// its record
    ///
    /// Returns the sender's lineage ([`read_lineage`]).
    ///
    /// # Arguments
    ///
    /// * `rest` - The bytes the hello carried after its record
    #[must_use]
    pub fn read_extension(&mut self, rest: &[u8]) -> Vec<u64> {
        // only an object lane names a slice, in the sixteen bytes after its record
        let mut lineage_at = 0;
        if self.lane == Lane::Object && rest.len() >= OBJECT_HELLO_EXTENSION {
            self.slice = Some(bytes16_at(rest, 0));
            lineage_at = OBJECT_HELLO_EXTENSION;
        }
        // and every hello's lineage after it
        read_lineage(&rest[lineage_at..], self.schema_id)
    }

    /// Read a hello out of its body
    ///
    /// # Arguments
    ///
    /// * `body` - The body to read
    pub fn decode(body: &[u8; PEER_HELLO_BODY_LEN]) -> Result<Self, ProtocolError> {
        Self::decode_fields(body)
    }

    /// Build the whole frame for this hello, an object lane's slice and the lineage after its
    /// record
    ///
    /// # Arguments
    ///
    /// * `lineage` - The lineage of the schema this node serves
    /// * `max_frame_bytes` - The largest frame the peer will accept, which a hello always fits
    pub fn frame(&self, lineage: &[u64], max_frame_bytes: u32) -> Result<Vec<u8>, ProtocolError> {
        // the record, then an object lane's slice, then the lineage
        let slice = self.slice.filter(|_| self.lane == Lane::Object);
        let mut body = self.encode().to_vec();
        if let Some(slice) = slice {
            body.extend_from_slice(&slice);
        }
        write_lineage(lineage, &mut body);
        // the hello is written at the floor, so a peer anywhere in the range reads it
        let header = Header::at(
            MIN_PEER_VERSION,
            MessageType::PeerHello,
            Flags::NONE,
            body.len(),
            max_frame_bytes,
        )?;
        let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
        frame.extend_from_slice(&header.encode());
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    /// The version two peers speak, if their ranges share one
    ///
    /// The highest version both read: the smaller of the two maxima, provided it is at or
    /// above both minima. Every frame after the hello is written at this version or below it
    /// ([F48](../../../../../docs/src/features/rolling-compatibility.md)).
    ///
    /// # Arguments
    ///
    /// * `ours` - This end's hello, whose range is what it advertised
    #[must_use]
    pub const fn negotiate(&self, ours: &PeerHello) -> Option<u8> {
        // the highest version both read
        let version = if self.wire_max < ours.wire_max {
            self.wire_max
        } else {
            ours.wire_max
        };
        // which has to be one both read
        if version >= self.wire_min && version >= ours.wire_min {
            Some(version)
        } else {
            None
        }
    }

    /// The capabilities both peers can act on
    ///
    /// # Arguments
    ///
    /// * `ours` - This end's hello
    #[must_use]
    pub const fn common_capabilities(&self, ours: &PeerHello) -> u64 {
        self.capabilities & ours.capabilities
    }

    /// The range this build advertises, bounded above by a pin
    ///
    /// # Arguments
    ///
    /// * `pin` - The newest version to advertise, or none for [`PROTOCOL_VERSION`]
    #[must_use]
    pub const fn range(pin: Option<u8>) -> (u8, u8) {
        let max = match pin {
            Some(pin) if pin < PROTOCOL_VERSION => pin,
            _ => PROTOCOL_VERSION,
        };
        let max = if max < MIN_PEER_VERSION {
            MIN_PEER_VERSION
        } else {
            max
        };
        (MIN_PEER_VERSION, max)
    }
}

/// A node accepting or refusing a peer hello, with its own record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHelloAck {
    /// The accepting node's own record
    pub hello: PeerHello,
    /// Whether it accepted, and if not why
    pub reason: PeerRefusal,
}

impl PeerHelloAck {
    /// Write this ack's body
    #[must_use]
    pub fn encode(&self) -> [u8; PEER_HELLO_BODY_LEN] {
        self.hello.encode_with(self.reason)
    }

    /// Read an ack out of its body
    ///
    /// # Arguments
    ///
    /// * `body` - The body to read
    pub fn decode(body: &[u8; PEER_HELLO_BODY_LEN]) -> Result<Self, ProtocolError> {
        Ok(PeerHelloAck {
            hello: PeerHello::decode_fields(body)?,
            reason: PeerRefusal::from_byte(body[REASON_AT]),
        })
    }

    /// Build the whole frame for this ack, the acceptor's lineage after its record
    ///
    /// A refusal carries [`Flags::REFUSED`] so a peer reading only the header knows the answer.
    ///
    /// # Arguments
    ///
    /// * `lineage` - The lineage of the schema the acceptor serves
    /// * `max_frame_bytes` - The largest frame the peer will accept
    pub fn frame(&self, lineage: &[u64], max_frame_bytes: u32) -> Result<Vec<u8>, ProtocolError> {
        // the record and then the lineage, under one header
        let record = frame_of(MessageType::PeerHelloAck, self.encode(), max_frame_bytes)?;
        let mut body = record[HEADER_LEN..].to_vec();
        write_lineage(lineage, &mut body);
        let header = Header::at(
            MIN_PEER_VERSION,
            MessageType::PeerHelloAck,
            if self.reason.is_accepted() {
                Flags::NONE
            } else {
                Flags::REFUSED
            },
            body.len(),
            max_frame_bytes,
        )?;
        let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
        frame.extend_from_slice(&header.encode());
        frame.extend_from_slice(&body);
        Ok(frame)
    }
}

/// Put a header in front of a hello body
///
/// # Arguments
///
/// * `kind` - Which of the two frames this is
/// * `body` - The body to frame
/// * `max_frame_bytes` - The largest frame the peer will accept
fn frame_of(
    kind: MessageType,
    body: [u8; PEER_HELLO_BODY_LEN],
    max_frame_bytes: u32,
) -> Result<[u8; PEER_HELLO_FRAME_LEN], ProtocolError> {
    // a refused ack says so in its header too
    let flags = if kind == MessageType::PeerHelloAck
        && !PeerRefusal::from_byte(body[REASON_AT]).is_accepted()
    {
        Flags::REFUSED
    } else {
        Flags::NONE
    };
    // the hello is written at the floor, so a peer anywhere in the range reads it
    let header = Header::at(
        MIN_PEER_VERSION,
        kind,
        flags,
        PEER_HELLO_BODY_LEN,
        max_frame_bytes,
    )?;
    let mut frame = [0u8; PEER_HELLO_FRAME_LEN];
    frame[..HEADER_LEN].copy_from_slice(&header.encode());
    frame[HEADER_LEN..].copy_from_slice(&body);
    Ok(frame)
}
