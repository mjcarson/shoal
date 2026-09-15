//! What a client says about how a read is to be served, and what a write hands back for it
//!
//! ```text
//!  request  : [header][trace context 26 B]?[read options 16 B + tokens × 48 B]?[rkyv Queries]
//!  response : [header][query id 16 B][session token 48 B]?[rkyv ResponseKinds]
//! ```
//!
//! A bundle may name the level every read in it is served at, a deadline for the whole bundle,
//! and up to [`MAX_SESSION_TOKENS`] tokens that earlier writes handed back, each a committed
//! lower bound on one tablet's history ([F41](../../../../docs/src/features/read-consistency.md),
//! [C6](../../../../docs/src/distributed/reads.md)). Both sections sit *outside* the rkyv
//! archive: the payload's layout, and with it the schema fingerprint, is exactly what it was
//! before either existed, and the sections are read into their own buffers so that the archive
//! still lands at the start of its own allocation.
//!
//! # Invariants
//!
//! **A section is sent only to a peer that said it reads it.** [`Flags::READ_OPTIONS`] is a
//! header bit a peer that does not know it round trips, but the section behind it has a length
//! a peer that does not know it cannot compute, so it would read the section as the first bytes
//! of an archive. The [`CLIENT_CAP_READ_OPTIONS`] byte in the hello is what closes that: a client
//! sends the section only once the ack granted the bit, and a server writes a token section only
//! to a connection whose hello asked for one. The bit is negotiated, the codec follows the bit,
//! and the protocol version does not move - which is C2's selected-version contract rather than
//! a flag day.
//!
//! **A token is an index, never a term.** A committed index is never lost, so a lower bound on
//! one is enough to ask a replica to wait until it has applied that far; a term would only be
//! needed to tell one *uncommitted* history from another, which is what the barrier's own
//! read log id carries and a token never does.
//!
//! [`Flags::READ_OPTIONS`]: super::Flags::READ_OPTIONS

use super::ProtocolError;
use crate::shared::identity::{ClusterId, GroupId, TableId};

/// The size of a session token on the wire, in bytes
pub const SESSION_TOKEN_LEN: usize = 48;

/// The most tokens one bundle may carry
///
/// A bundle needing several tablet lower bounds carries several tokens; this bounds what a
/// server allocates for them before it has read any.
pub const MAX_SESSION_TOKENS: usize = 16;

/// The size of the fixed head of a read options section, ahead of its tokens
pub const READ_OPTIONS_HEAD_LEN: usize = 16;

/// The most bytes a read options section can be
pub const MAX_READ_OPTIONS_LEN: usize =
    READ_OPTIONS_HEAD_LEN + MAX_SESSION_TOKENS * SESSION_TOKEN_LEN;

/// The version of the read options head this build writes and reads
const READ_OPTIONS_VERSION: u8 = 1;

/// The version of the session token this build writes and reads
const SESSION_TOKEN_VERSION: u8 = 1;

/// The client reads and writes the read options and session token sections
///
/// Spent from the client hello's reserved bytes; the ack's copy is the subset the server
/// granted.
pub const CLIENT_CAP_READ_OPTIONS: u8 = 1 << 0;

/// The level a read is served at
///
/// One name for the strong level rather than two: `Primary` and `Quorum` were drafted as
/// separate routing policies over one freshness promise, and the decision under Q5 was that
/// one implementation earns one name ([C13](../../../../docs/src/distributed/protocol.md)).
/// Zero on the wire is "inherit", which is the table's policy and then the cluster's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ReadLevel {
    /// One eligible replica's committed applied state, possibly stale
    One = 1,
    /// A data-quorum barrier, then the replica's state applied through it
    Quorum = 2,
}

impl ReadLevel {
    /// Get the byte this level is written as
    #[inline]
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a level from the byte it was written as, where zero is "inherit"
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse
    ///
    /// # Errors
    ///
    /// A byte naming no level this build knows is refused rather than read as any level.
    #[inline]
    pub const fn from_byte(raw: u8) -> Result<Option<Self>, ProtocolError> {
        match raw {
            0 => Ok(None),
            1 => Ok(Some(ReadLevel::One)),
            2 => Ok(Some(ReadLevel::Quorum)),
            unknown => Err(ProtocolError::UnknownReadLevel(unknown)),
        }
    }

    /// Get the lowercase name of this level
    #[inline]
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            ReadLevel::One => "one",
            ReadLevel::Quorum => "quorum",
        }
    }
}

impl std::fmt::Display for ReadLevel {
    /// Write this level's name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A committed lower bound on one tablet's history, handed back by a write
///
/// ```text
///  ver u8 | flags u8 | tablet u16 | reserved 4 | cluster 16 | table u64 | group u64 | index u64
/// ```
///
/// A read that carries one is served by a replica only once that replica has applied at least
/// this far in this group's log, so a client reads its own writes without every read paying
/// for a barrier. It names the cluster so that a token from another cluster is refused by name,
/// and the group so that a replica serving the tablet under another lineage - after a move or
/// a split, neither of which exists yet - refuses it rather than waiting on an index that means
/// nothing to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionToken {
    /// The cluster the write was committed in
    pub cluster: ClusterId,
    /// The table the write named
    pub table: TableId,
    /// The tablet the write's key hashed to
    pub tablet: u16,
    /// The group that served the tablet when the write committed
    pub group: GroupId,
    /// The log index the write was committed at
    pub index: u64,
}

impl SessionToken {
    /// Write this token as the bytes it goes on the wire as
    #[must_use]
    pub fn encode(&self) -> [u8; SESSION_TOKEN_LEN] {
        let mut raw = [0u8; SESSION_TOKEN_LEN];
        raw[0] = SESSION_TOKEN_VERSION;
        raw[1] = 0;
        raw[2..4].copy_from_slice(&self.tablet.to_le_bytes());
        // four reserved bytes stay zero
        raw[8..24].copy_from_slice(self.cluster.0.as_bytes());
        raw[24..32].copy_from_slice(&self.table.0.to_le_bytes());
        raw[32..40].copy_from_slice(&self.group.0.to_le_bytes());
        raw[40..48].copy_from_slice(&self.index.to_le_bytes());
        raw
    }

    /// Read a token from the bytes it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The token bytes
    ///
    /// # Errors
    ///
    /// A token of a version this build does not read is refused rather than misread.
    pub fn decode(raw: &[u8; SESSION_TOKEN_LEN]) -> Result<Self, ProtocolError> {
        if raw[0] != SESSION_TOKEN_VERSION {
            return Err(ProtocolError::UnknownSessionTokenVersion(raw[0]));
        }
        let mut cluster = [0u8; 16];
        cluster.copy_from_slice(&raw[8..24]);
        Ok(SessionToken {
            cluster: ClusterId(uuid::Uuid::from_bytes(cluster)),
            table: TableId(u64::from_le_bytes(
                raw[24..32].try_into().expect("eight bytes"),
            )),
            tablet: u16::from_le_bytes([raw[2], raw[3]]),
            group: GroupId(u64::from_le_bytes(
                raw[32..40].try_into().expect("eight bytes"),
            )),
            index: u64::from_le_bytes(raw[40..48].try_into().expect("eight bytes")),
        })
    }
}

/// What a bundle says about how its reads are served
///
/// ```text
///  ver u8 | level u8 | tokens u8 | reserved u8 | deadline ms u32 | reserved 8 | tokens × 48 B
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadOptions {
    /// The level every read in the bundle is served at, or none to inherit each table's policy
    pub level: Option<ReadLevel>,
    /// How many milliseconds the bundle may take in all, or zero for the server's default
    pub deadline_ms: u32,
    /// The committed lower bounds the bundle's reads have to be served past
    pub tokens: Vec<SessionToken>,
}

impl ReadOptions {
    /// Whether this is the empty option set, which is not worth a section on the wire
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.level.is_none() && self.deadline_ms == 0 && self.tokens.is_empty()
    }

    /// How many bytes this section takes on the wire
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        READ_OPTIONS_HEAD_LEN + self.tokens.len() * SESSION_TOKEN_LEN
    }

    /// Write this section
    ///
    /// # Errors
    ///
    /// Refuses more tokens than a section may carry, since the count is one byte and the bound
    /// is what a receiver allocates against.
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        if self.tokens.len() > MAX_SESSION_TOKENS {
            return Err(ProtocolError::TooManySessionTokens(self.tokens.len()));
        }
        let mut out = Vec::with_capacity(self.encoded_len());
        out.push(READ_OPTIONS_VERSION);
        out.push(self.level.map_or(0, ReadLevel::as_byte));
        // the bound above is sixteen, so the count fits its byte
        #[allow(clippy::cast_possible_truncation)]
        out.push(self.tokens.len() as u8);
        out.push(0);
        out.extend_from_slice(&self.deadline_ms.to_le_bytes());
        out.extend_from_slice(&[0u8; 8]);
        for token in &self.tokens {
            out.extend_from_slice(&token.encode());
        }
        Ok(out)
    }

    /// Read the fixed head of a section, saying how many token bytes follow it
    ///
    /// The head is read on its own so that a receiver knows how many more bytes to read before
    /// it allocates for them; [`ReadOptions::decode_tokens`] finishes the job.
    ///
    /// # Arguments
    ///
    /// * `raw` - The head bytes
    ///
    /// # Errors
    ///
    /// Refuses a head of a version this build does not read, a level byte it does not know, or
    /// a token count past the bound.
    pub fn decode_head(raw: &[u8; READ_OPTIONS_HEAD_LEN]) -> Result<(Self, usize), ProtocolError> {
        if raw[0] != READ_OPTIONS_VERSION {
            return Err(ProtocolError::UnknownReadOptionsVersion(raw[0]));
        }
        let level = ReadLevel::from_byte(raw[1])?;
        let tokens = usize::from(raw[2]);
        if tokens > MAX_SESSION_TOKENS {
            return Err(ProtocolError::TooManySessionTokens(tokens));
        }
        let deadline_ms = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
        let options = ReadOptions {
            level,
            deadline_ms,
            tokens: Vec::with_capacity(tokens),
        };
        Ok((options, tokens * SESSION_TOKEN_LEN))
    }

    /// Read the tokens that follow a head, which must be exactly the bytes the head said
    ///
    /// # Arguments
    ///
    /// * `raw` - The token bytes
    ///
    /// # Errors
    ///
    /// Refuses bytes that are not a whole number of tokens, or a token this build cannot read.
    pub fn decode_tokens(&mut self, raw: &[u8]) -> Result<(), ProtocolError> {
        if raw.len() % SESSION_TOKEN_LEN != 0 {
            return Err(ProtocolError::MalformedReadOptions(
                "tokens do not fill their bytes",
            ));
        }
        for chunk in raw.chunks_exact(SESSION_TOKEN_LEN) {
            let mut fixed = [0u8; SESSION_TOKEN_LEN];
            fixed.copy_from_slice(chunk);
            self.tokens.push(SessionToken::decode(&fixed)?);
        }
        if self.tokens.len() > MAX_SESSION_TOKENS {
            return Err(ProtocolError::TooManySessionTokens(self.tokens.len()));
        }
        Ok(())
    }

    /// Read a whole section from one buffer
    ///
    /// # Arguments
    ///
    /// * `raw` - The section bytes, head and tokens
    ///
    /// # Errors
    ///
    /// Refuses a buffer that is not exactly one section.
    pub fn decode(raw: &[u8]) -> Result<Self, ProtocolError> {
        let Some(head) = raw.get(..READ_OPTIONS_HEAD_LEN) else {
            return Err(ProtocolError::MalformedReadOptions(
                "a read options head is cut short",
            ));
        };
        let mut fixed = [0u8; READ_OPTIONS_HEAD_LEN];
        fixed.copy_from_slice(head);
        let (mut options, token_bytes) = Self::decode_head(&fixed)?;
        let rest = &raw[READ_OPTIONS_HEAD_LEN..];
        if rest.len() != token_bytes {
            return Err(ProtocolError::MalformedReadOptions(
                "a read options head does not describe its tokens",
            ));
        }
        options.decode_tokens(rest)?;
        Ok(options)
    }
}
