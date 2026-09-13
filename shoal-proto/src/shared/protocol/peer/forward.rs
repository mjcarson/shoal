//! A bundle forwarded to the node that owns some of its partitions, and the answers coming back
//!
//! ```text
//!  forward   : [header][preamble 48 B][entries][rkyv Queries]
//!  forwarded : [header][preamble 96 B][rkyv ResponseKinds | error]
//! ```
//!
//! The coordinator that received a bundle from a client routes every query in it by the scalars
//! in its archive ([F26](../../../../../docs/src/features/archive-routed-requests.md)); the ones
//! whose partitions live on another node are sent there as **one frame per node**, carrying the
//! whole bundle once and one entry per query in it that the node owns. The receiving node
//! validates the bundle again - a process boundary re-establishes every checked-decoding
//! invariant - and hands each entry to the shard it names, exactly as its own coordinator would.
//!
//! An answer comes back as a `Forwarded` frame naming the bundle and the query index, so the
//! origin can find what it is waiting on. It carries no return address: the origin's pending
//! record holds the client, the span and the stamps, and a peer never forwards a forward - the
//! hop count on arrival must be zero.
//!
//! Since [F41](../../../../../docs/src/features/read-consistency.md) an entry may carry a read
//! plan - the level the coordinator resolved, the slot of the gather it fills and the tokens
//! bounding it - and an answer carries the attempt and slot it fills and the token a write
//! minted. The widened answer head is what `CAP_READ_CONSISTENCY_V1` names: a peer without the
//! bit is refused at the hello, since the M2 rule is an exact match.

use super::super::read::{ReadLevel, SessionToken, MAX_SESSION_TOKENS, SESSION_TOKEN_LEN};
use super::super::trace::{TraceContext, TRACE_CONTEXT_LEN};
use super::super::ProtocolError;
use super::{bytes16_at, u16_at, u32_at, u64_at};

/// The size of a forward preamble in bytes
pub const FORWARD_PREAMBLE_LEN: usize = 48;

/// The size of a forwarded preamble in bytes
///
/// Was 32 through M4; the attempt, the slot and a token do not fit six reserved bytes, so it
/// widened under the read consistency capability.
pub const FORWARDED_PREAMBLE_LEN: usize = 96;

/// The most entries one forward may carry
///
/// A bundle of more queries than this for one node is sent as several forwards.
pub const MAX_FORWARD_ENTRIES: u16 = 4096;

/// The most bytes the entries of one forward may take
///
/// Judged before the entries are read, so a preamble cannot name an allocation this big times
/// anything: at most this many bytes are ever read for entries.
pub const MAX_FORWARD_ENTRIES_BYTES: u32 = 4 * 1024 * 1024;

/// The most partition keys one entry may narrow its query to
pub const MAX_FORWARD_KEYS: u32 = 1 << 16;

/// The fixed size of one entry before its trace context and keys
const ENTRY_FIXED_LEN: usize = 22;

/// Where the preamble's fields sit
const BUNDLE_AT: usize = 0;
const ATTEMPT_AT: usize = 16;
const BASE_INDEX_AT: usize = 24;
const HOPS_AT: usize = 32;
const REMAINING_AT: usize = 36;
const ENTRIES_AT: usize = 40;
const ENTRIES_LEN_AT: usize = 44;

/// Entry flag bits
const FLAG_END: u8 = 1 << 0;
const FLAG_GATHER: u8 = 1 << 1;
const FLAG_TRACE: u8 = 1 << 2;
const FLAG_READ: u8 = 1 << 3;

/// The fixed size of an entry's read plan, ahead of its tokens
const ENTRY_READ_FIXED_LEN: usize = 4;

/// Answer head flag bits
const FORWARDED_FLAG_TOKEN: u8 = 1 << 0;

/// How the coordinator asked one share of a read to be served
///
/// ```text
///  level u8 | tokens u8 | slot u16 | tokens × 48 B
/// ```
///
/// The level is resolved once on the coordinator - the bundle's override, then the table's
/// policy, then the cluster's - and forwarded resolved; the serving node validates the byte and
/// never re-resolves it. The slot is where this share lands in the origin's gather, and the
/// tokens are the ones bounding this share's tablets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRead {
    /// The level this share is served at
    pub level: ReadLevel,
    /// Which slot of the origin's gather this share fills
    pub slot: u16,
    /// The committed lower bounds this share has to be served past
    pub tokens: Vec<SessionToken>,
}

impl EntryRead {
    /// How many bytes this plan takes on the wire
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        ENTRY_READ_FIXED_LEN + self.tokens.len() * SESSION_TOKEN_LEN
    }
}

/// What is fixed about a forward, ahead of its entries and its bundle
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardPreamble {
    /// The bundle these queries arrived in, as the uuid's bytes
    pub bundle: [u8; 16],
    /// Which attempt at this bundle this is
    ///
    /// Minted per forward, so a retry after a reconnect is told apart from the original. Nothing
    /// retries at M2; the field exists so that C2's operation identity has somewhere to go
    /// without a wire change.
    pub attempt: u64,
    /// The bundle's base index, so every index below is absolute in the client's stream
    pub base_index: u64,
    /// How many nodes have forwarded this already, which must be zero on arrival
    pub hops: u8,
    /// How many milliseconds the origin will still wait for an answer
    pub remaining_ms: u32,
    /// How many entries follow
    pub entries: u16,
    /// How many bytes those entries take
    pub entries_len: u32,
}

impl ForwardPreamble {
    /// Write this preamble
    #[must_use]
    pub fn encode(&self) -> [u8; FORWARD_PREAMBLE_LEN] {
        let mut body = [0u8; FORWARD_PREAMBLE_LEN];
        body[BUNDLE_AT..BUNDLE_AT + 16].copy_from_slice(&self.bundle);
        body[ATTEMPT_AT..ATTEMPT_AT + 8].copy_from_slice(&self.attempt.to_le_bytes());
        body[BASE_INDEX_AT..BASE_INDEX_AT + 8].copy_from_slice(&self.base_index.to_le_bytes());
        body[HOPS_AT] = self.hops;
        body[REMAINING_AT..REMAINING_AT + 4].copy_from_slice(&self.remaining_ms.to_le_bytes());
        body[ENTRIES_AT..ENTRIES_AT + 2].copy_from_slice(&self.entries.to_le_bytes());
        body[ENTRIES_LEN_AT..ENTRIES_LEN_AT + 4].copy_from_slice(&self.entries_len.to_le_bytes());
        body
    }

    /// Read a preamble, judging every length in it against the frame it came in
    ///
    /// # Arguments
    ///
    /// * `raw` - The preamble bytes
    /// * `body_len` - The number of bytes the frame's header said follow it
    pub fn decode(raw: &[u8; FORWARD_PREAMBLE_LEN], body_len: usize) -> Result<Self, ProtocolError> {
        let preamble = ForwardPreamble {
            bundle: bytes16_at(raw, BUNDLE_AT),
            attempt: u64_at(raw, ATTEMPT_AT),
            base_index: u64_at(raw, BASE_INDEX_AT),
            hops: raw[HOPS_AT],
            remaining_ms: u32_at(raw, REMAINING_AT),
            entries: u16_at(raw, ENTRIES_AT),
            entries_len: u32_at(raw, ENTRIES_LEN_AT),
        };
        // a forward that has already been forwarded is a routing loop, not a query
        if preamble.hops != 0 {
            return Err(ProtocolError::MalformedForward("a forward arrived with a hop count"));
        }
        // no entries is a frame with nothing to do, which no node ever writes
        if preamble.entries == 0 {
            return Err(ProtocolError::MalformedForward("a forward names no entries"));
        }
        if preamble.entries > MAX_FORWARD_ENTRIES {
            return Err(ProtocolError::MalformedForward("a forward names too many entries"));
        }
        // the entries have to fit their bound before anything is sized by them
        if preamble.entries_len > MAX_FORWARD_ENTRIES_BYTES {
            return Err(ProtocolError::MalformedForward("a forward's entries pass their bound"));
        }
        // and the frame has to hold the preamble, the entries and at least a bundle's worth
        let fixed = FORWARD_PREAMBLE_LEN + preamble.entries_len as usize;
        if body_len <= fixed {
            return Err(ProtocolError::MalformedForward("a forward's frame cannot hold its bundle"));
        }
        Ok(preamble)
    }

    /// How many bundle bytes follow the entries in a frame of this length
    ///
    /// # Arguments
    ///
    /// * `body_len` - The number of bytes the frame's header said follow it
    #[must_use]
    pub const fn bundle_len(&self, body_len: usize) -> usize {
        body_len - FORWARD_PREAMBLE_LEN - self.entries_len as usize
    }
}

/// One query of a forwarded bundle, and which shard of the receiving node answers it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardEntry {
    /// Which query in the bundle this is
    pub offset: u32,
    /// The absolute index the answer is owed under
    pub index: u64,
    /// Whether this is the last query of the client's stream
    pub end: bool,
    /// The shard of the receiving node that owns this query's partitions
    pub shard: u16,
    /// The shard of the origin node collecting the shares of this query, if it was split
    pub origin_shard: u16,
    /// Whether the answer is a share to be merged on the origin rather than a whole answer
    pub gather: bool,
    /// The trace this query belongs to, if the origin was tracing it
    pub trace: Option<TraceContext>,
    /// How this share of a read is to be served, if the origin planned it
    ///
    /// `None` for a write, and for a read a node built before plans existed would have sent;
    /// the serving node treats it as `One` with no slot and no tokens.
    pub read: Option<EntryRead>,
    /// The partition keys this shard owns, when the query was narrowed to a subset
    ///
    /// Empty means answer the query as it stands, which is what every write needs.
    pub keys: Vec<u64>,
}

impl ForwardEntry {
    /// How many bytes this entry takes on the wire
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        // the fixed fields, the context and the plan if there are any, and eight bytes a key
        ENTRY_FIXED_LEN
            + if self.trace.is_some() { TRACE_CONTEXT_LEN } else { 0 }
            + self.read.as_ref().map_or(0, EntryRead::encoded_len)
            + self.keys.len() * 8
    }

    /// Append this entry to a buffer of entries
    ///
    /// # Arguments
    ///
    /// * `out` - The buffer to append to
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(&self.shard.to_le_bytes());
        out.extend_from_slice(&self.origin_shard.to_le_bytes());
        // pack the three booleans into one byte
        let mut flags = 0u8;
        if self.end {
            flags |= FLAG_END;
        }
        if self.gather {
            flags |= FLAG_GATHER;
        }
        if self.trace.is_some() {
            flags |= FLAG_TRACE;
        }
        if self.read.is_some() {
            flags |= FLAG_READ;
        }
        out.push(flags);
        out.push(0);
        // truncation cannot happen: a caller building more keys than the bound is refused below
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());
        if let Some(trace) = &self.trace {
            out.extend_from_slice(&trace.encode());
        }
        // the plan sits after the context and before the keys
        if let Some(read) = &self.read {
            out.push(read.level.as_byte());
            // the bound is sixteen, so the count fits its byte; a caller past it is refused
            #[allow(clippy::cast_possible_truncation)]
            out.push(read.tokens.len() as u8);
            out.extend_from_slice(&read.slot.to_le_bytes());
            for token in &read.tokens {
                out.extend_from_slice(&token.encode());
            }
        }
        for key in &self.keys {
            out.extend_from_slice(&key.to_le_bytes());
        }
    }
}

/// Encode a set of entries into the bytes a forward carries between its preamble and its bundle
///
/// # Arguments
///
/// * `entries` - The entries to encode
///
/// # Errors
///
/// Refuses a set that would not decode: too many entries, or an entry with too many keys.
pub fn encode_entries(entries: &[ForwardEntry]) -> Result<Vec<u8>, ProtocolError> {
    if entries.is_empty() || entries.len() > MAX_FORWARD_ENTRIES as usize {
        return Err(ProtocolError::MalformedForward("an entry count outside its bound"));
    }
    let mut out = Vec::with_capacity(entries.iter().map(ForwardEntry::encoded_len).sum());
    for entry in entries {
        if entry.keys.len() > MAX_FORWARD_KEYS as usize {
            return Err(ProtocolError::MalformedForward("an entry with too many keys"));
        }
        if entry.read.as_ref().is_some_and(|read| read.tokens.len() > MAX_SESSION_TOKENS) {
            return Err(ProtocolError::MalformedForward("an entry with too many session tokens"));
        }
        entry.encode_into(&mut out);
    }
    if out.len() > MAX_FORWARD_ENTRIES_BYTES as usize {
        return Err(ProtocolError::MalformedForward("entries past their byte bound"));
    }
    Ok(out)
}

/// Decode the entries a forward carries, which must use exactly the bytes given
///
/// Every field is bounds checked before it is read, and the key count of every entry against
/// [`MAX_FORWARD_KEYS`] before its keys are read. The receiver still has to check every
/// `offset` against the bundle it validated and every `shard` against its own shard count,
/// since neither is knowable here.
///
/// # Arguments
///
/// * `raw` - The entry bytes, exactly `entries_len` of them
/// * `count` - How many entries the preamble said there are
pub fn decode_entries(raw: &[u8], count: u16) -> Result<Vec<ForwardEntry>, ProtocolError> {
    let mut entries = Vec::with_capacity(count as usize);
    let mut at = 0usize;
    for _ in 0..count {
        // the fixed fields have to be there before any of them is read
        let Some(fixed) = raw.get(at..at + ENTRY_FIXED_LEN) else {
            return Err(ProtocolError::MalformedForward("an entry is cut short"));
        };
        let offset = u32_at(fixed, 0);
        let index = u64_at(fixed, 4);
        let shard = u16_at(fixed, 12);
        let origin_shard = u16_at(fixed, 14);
        let flags = fixed[16];
        let keys_len = u32_at(fixed, 18);
        at += ENTRY_FIXED_LEN;
        // a flag this build does not know is an entry it cannot act on
        if flags & !(FLAG_END | FLAG_GATHER | FLAG_TRACE | FLAG_READ) != 0 {
            return Err(ProtocolError::MalformedForward("an entry sets an unknown flag"));
        }
        if keys_len > MAX_FORWARD_KEYS {
            return Err(ProtocolError::MalformedForward("an entry names too many keys"));
        }
        // the context, if the flags say one is there
        let trace = if flags & FLAG_TRACE != 0 {
            let Some(bytes) = raw.get(at..at + TRACE_CONTEXT_LEN) else {
                return Err(ProtocolError::MalformedForward("a trace context is cut short"));
            };
            let mut fixed = [0u8; TRACE_CONTEXT_LEN];
            fixed.copy_from_slice(bytes);
            at += TRACE_CONTEXT_LEN;
            Some(TraceContext::decode(&fixed)?)
        } else {
            None
        };
        // the read plan, if the flags say one is there
        let read = if flags & FLAG_READ != 0 {
            let Some(fixed) = raw.get(at..at + ENTRY_READ_FIXED_LEN) else {
                return Err(ProtocolError::MalformedForward("a read plan is cut short"));
            };
            // a plan names a level, never inherits one: resolution happened on the coordinator
            let Some(level) = ReadLevel::from_byte(fixed[0])? else {
                return Err(ProtocolError::MalformedForward("a read plan names no level"));
            };
            let tokens = usize::from(fixed[1]);
            if tokens > MAX_SESSION_TOKENS {
                return Err(ProtocolError::MalformedForward("a read plan names too many tokens"));
            }
            let slot = u16_at(fixed, 2);
            at += ENTRY_READ_FIXED_LEN;
            let token_bytes = tokens * SESSION_TOKEN_LEN;
            let Some(bytes) = raw.get(at..at + token_bytes) else {
                return Err(ProtocolError::MalformedForward("a read plan's tokens are cut short"));
            };
            let mut decoded = Vec::with_capacity(tokens);
            for chunk in bytes.chunks_exact(SESSION_TOKEN_LEN) {
                let mut fixed = [0u8; SESSION_TOKEN_LEN];
                fixed.copy_from_slice(chunk);
                decoded.push(SessionToken::decode(&fixed)?);
            }
            at += token_bytes;
            Some(EntryRead {
                level,
                slot,
                tokens: decoded,
            })
        } else {
            None
        };
        // and the keys, each of which is a scalar read out in this host's endianness
        let keys_bytes = keys_len as usize * 8;
        let Some(key_bytes) = raw.get(at..at + keys_bytes) else {
            return Err(ProtocolError::MalformedForward("an entry's keys are cut short"));
        };
        let keys = key_bytes
            .chunks_exact(8)
            .map(|chunk| u64_at(chunk, 0))
            .collect();
        at += keys_bytes;
        entries.push(ForwardEntry {
            offset,
            index,
            end: flags & FLAG_END != 0,
            shard,
            origin_shard,
            gather: flags & FLAG_GATHER != 0,
            trace,
            read,
            keys,
        });
    }
    // trailing bytes are a preamble that lied about its entries
    if at != raw.len() {
        return Err(ProtocolError::MalformedForward("entries do not fill their bytes"));
    }
    Ok(entries)
}

/// What kind of answer a forwarded frame carries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ForwardedKind {
    /// A whole answer, sealed bytes the origin hands straight to its client
    Whole = 1,
    /// One shard's share of a split query, which the origin merges
    Share = 2,
    /// A failure the receiving node is answering with instead of either
    Error = 3,
}

impl ForwardedKind {
    /// Get the byte this kind is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a kind from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse
    #[inline]
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            1 => Ok(ForwardedKind::Whole),
            2 => Ok(ForwardedKind::Share),
            3 => Ok(ForwardedKind::Error),
            unknown => Err(ProtocolError::UnknownForwardedKind(unknown)),
        }
    }
}

/// What is fixed about an answer coming back, ahead of its payload
///
/// ```text
///  bundle 16 | index u64 | kind u8 | served u8 | flags u8 | reserved 5 | attempt u64 | slot u16 | reserved 6 | token 48
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardedPreamble {
    /// The bundle the answered query arrived in
    pub bundle: [u8; 16],
    /// The absolute index the answer is owed under
    pub index: u64,
    /// What follows
    pub kind: ForwardedKind,
    /// How the serving node classified the query as it ran it, for the origin's stage record
    ///
    /// Opaque to the protocol: the serving node's profiling stamps know what kind of query it
    /// was and what durability it waited on, and the origin - which forwarded bytes it never
    /// decoded - does not. A build that records no stages sends zero, and zero is read as
    /// "unclassified" rather than as any kind of query. Nothing about the answer's meaning
    /// depends on it.
    pub served: u8,
    /// The attempt at the bundle this answers, echoed from the forward
    ///
    /// An answer to an attempt the origin has moved past is late and dropped by identity
    /// rather than by guesswork ([F41](../../../../../docs/src/features/read-consistency.md)).
    pub attempt: u64,
    /// The slot of the origin's gather this share fills, echoed from the entry's plan
    pub slot: u16,
    /// The token a write minted, if this answers a write that committed
    pub token: Option<SessionToken>,
}

impl ForwardedPreamble {
    /// Write this preamble
    #[must_use]
    pub fn encode(&self) -> [u8; FORWARDED_PREAMBLE_LEN] {
        let mut body = [0u8; FORWARDED_PREAMBLE_LEN];
        body[..16].copy_from_slice(&self.bundle);
        body[16..24].copy_from_slice(&self.index.to_le_bytes());
        body[24] = self.kind.as_byte();
        body[25] = self.served;
        body[26] = if self.token.is_some() { FORWARDED_FLAG_TOKEN } else { 0 };
        // five reserved bytes stay zero
        body[32..40].copy_from_slice(&self.attempt.to_le_bytes());
        body[40..42].copy_from_slice(&self.slot.to_le_bytes());
        // six reserved bytes stay zero
        if let Some(token) = &self.token {
            body[48..96].copy_from_slice(&token.encode());
        }
        body
    }

    /// Read a preamble
    ///
    /// # Arguments
    ///
    /// * `raw` - The preamble bytes
    ///
    /// # Errors
    ///
    /// Refuses a kind or a flag this build does not know, and a token it cannot read.
    pub fn decode(raw: &[u8; FORWARDED_PREAMBLE_LEN]) -> Result<Self, ProtocolError> {
        let flags = raw[26];
        // a flag this build does not know is an answer it cannot act on
        if flags & !FORWARDED_FLAG_TOKEN != 0 {
            return Err(ProtocolError::MalformedForward("an answer sets an unknown flag"));
        }
        let token = if flags & FORWARDED_FLAG_TOKEN != 0 {
            let mut fixed = [0u8; SESSION_TOKEN_LEN];
            fixed.copy_from_slice(&raw[48..96]);
            Some(SessionToken::decode(&fixed)?)
        } else {
            None
        };
        Ok(ForwardedPreamble {
            bundle: bytes16_at(raw, 0),
            index: u64_at(raw, 16),
            kind: ForwardedKind::from_byte(raw[24])?,
            served: raw[25],
            attempt: u64_at(raw, 32),
            slot: u16_at(raw, 40),
            token,
        })
    }
}

/// Encode the payload of an error answer: a code and a message
///
/// # Arguments
///
/// * `code` - The error code, as the number it is written as
/// * `msg` - What to say about it
#[must_use]
pub fn encode_error_payload(code: u16, msg: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + msg.len());
    out.extend_from_slice(&code.to_le_bytes());
    out.extend_from_slice(msg.as_bytes());
    out
}

/// Decode the payload of an error answer
///
/// A message that is not UTF-8 is replaced rather than refused, since the code is what a
/// program acts on and the message is for a person.
///
/// # Arguments
///
/// * `raw` - The payload bytes
pub fn decode_error_payload(raw: &[u8]) -> Result<(u16, String), ProtocolError> {
    if raw.len() < 2 {
        return Err(ProtocolError::MalformedForward("an error answer has no code"));
    }
    let code = u16_at(raw, 0);
    let msg = String::from_utf8_lossy(&raw[2..]).into_owned();
    Ok((code, msg))
}
