//! The two frames a connection opens with
//!
//! # Invariants
//!
//! **The client always speaks first.** It writes a `Hello` and then reads; the server reads a
//! `Hello` and then writes. If both peers waited to read, every connection would deadlock, and
//! nothing in the frame layout would show it.
//!
//! **The bodies are fixed bytes, not rkyv archives.** The whole purpose of this exchange is to
//! detect that the peer's schema — and with it, potentially, its rkyv layout — does not match
//! ours. Decoding it with rkyv would make the detector depend on the thing it detects: a peer
//! built differently would report "corrupt archive" rather than "your schema is different", which
//! is exactly the confusion the fingerprint exists to remove. Fixed bytes also mean the handshake
//! has no alignment requirement and a known size before it is read.
//!
//! **The version is not in the body.** It is in the header of every frame, which is why the
//! version byte is per frame rather than per connection.
//!
//! **A reserved byte may be spent, but never moved.** The authentication mechanism fields were cut
//! out of the reserved tails of both bodies rather than appended to them, which is what let
//! authentication land without touching the version byte: an older peer wrote those bytes as
//! zeroes and read them as nothing, and zero is "no mechanism" in both directions. Anything that
//! needs a field the reserved bytes cannot hold needs a new protocol version, not a longer body.

use super::auth::{AuthMechanism, AuthMechanisms};
use super::{Flags, Header, MessageType, ProtocolError, HEADER_LEN};

/// The size of a handshake body in bytes
pub const HANDSHAKE_BODY_LEN: usize = 16;

/// The size of a whole handshake frame, header included
pub const HANDSHAKE_FRAME_LEN: usize = HEADER_LEN + HANDSHAKE_BODY_LEN;

/// Why a server refused a connection, or that it did not
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RefusalReason {
    /// The server accepted this connection
    Accepted = 0,
    /// The client speaks a protocol version the server does not
    UnsupportedVersion = 1,
    /// The client was built from a different schema than the server
    SchemaMismatch = 2,
    /// The server requires authentication and the client offered nothing it accepts
    ///
    /// This is a refusal of the *handshake*, not a failed login. A client that offered a mechanism
    /// the server accepts gets an acceptance and then proves itself, and a client that cannot prove
    /// itself is refused on an [`AuthResponse`](super::MessageType::AuthResponse) instead.
    NoCommonAuthMechanism = 3,
}

impl RefusalReason {
    /// Get the byte this reason is written as
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a refusal reason from the byte it was written as
    ///
    /// An unknown reason is read as a refusal rather than as an acceptance, because a server that
    /// gave a reason this build does not know still refused us.
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a reason from
    pub const fn from_byte(raw: u8) -> Self {
        match raw {
            0 => RefusalReason::Accepted,
            1 => RefusalReason::UnsupportedVersion,
            3 => RefusalReason::NoCommonAuthMechanism,
            // fail closed on anything we do not recognize
            _ => RefusalReason::SchemaMismatch,
        }
    }

    /// Check whether this reason means the connection was accepted
    pub const fn is_accepted(self) -> bool {
        matches!(self, RefusalReason::Accepted)
    }
}

impl std::fmt::Display for RefusalReason {
    /// Write a legible description of this refusal reason
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefusalReason::Accepted => write!(f, "accepted"),
            RefusalReason::UnsupportedVersion => write!(f, "unsupported protocol version"),
            RefusalReason::SchemaMismatch => write!(f, "schema fingerprint mismatch"),
            RefusalReason::NoCommonAuthMechanism => {
                write!(f, "no authentication mechanism in common")
            }
        }
    }
}

/// The frame a client opens a connection with
///
/// ```text
///  ┌──────────────────────┬───────────────────┬────────────┬──────────┐
///  │ schema fingerprint   │ max frame bytes   │ mechanisms │ reserved │
///  │     (u64 LE, 8 B)    │   (u32 LE, 4 B)   │(u16 LE, 2B)│  (2 B)   │
///  └──────────────────────┴───────────────────┴────────────┴──────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hello {
    /// The fingerprint of the schema this client was built from
    pub schema_fingerprint: u64,
    /// The largest frame this client will accept
    pub max_frame_bytes: u32,
    /// The authentication mechanisms this client can do
    ///
    /// An empty set is a client with no credentials, which is every client built before there was
    /// authentication and every client that has not been given any. It is not an error — a server
    /// that requires nothing lets it straight in.
    pub mechanisms: AuthMechanisms,
}

impl Hello {
    /// Write this hello out as the sixteen bytes that go on the wire
    pub const fn encode(&self) -> [u8; HANDSHAKE_BODY_LEN] {
        // split each field into its little endian bytes
        let fingerprint = self.schema_fingerprint.to_le_bytes();
        let max = self.max_frame_bytes.to_le_bytes();
        let mechanisms = self.mechanisms.bits().to_le_bytes();
        // the last two bytes are reserved and are written as zeroes
        [
            fingerprint[0],
            fingerprint[1],
            fingerprint[2],
            fingerprint[3],
            fingerprint[4],
            fingerprint[5],
            fingerprint[6],
            fingerprint[7],
            max[0],
            max[1],
            max[2],
            max[3],
            mechanisms[0],
            mechanisms[1],
            0,
            0,
        ]
    }

    /// Read a hello from the sixteen bytes it was written as
    ///
    /// The reserved bytes are ignored rather than checked, so a newer client that fills them can
    /// still open a connection to this build. The mechanism bits are read the same way and are not
    /// masked, for the reason [`AuthMechanisms`] gives: a bit this build cannot name is a mechanism
    /// a newer client can do, and a server only ever asks this set about mechanisms it wants.
    ///
    /// # Arguments
    ///
    /// * `raw` - The sixteen body bytes to read
    pub const fn decode(raw: &[u8; HANDSHAKE_BODY_LEN]) -> Self {
        Hello {
            schema_fingerprint: u64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]),
            max_frame_bytes: u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]),
            mechanisms: AuthMechanisms::from_bits(u16::from_le_bytes([raw[12], raw[13]])),
        }
    }

    /// Build the whole frame, header included, ready for a single write
    ///
    /// # Errors
    ///
    /// This cannot fail in practice — a sixteen byte body fits in any frame bound worth having —
    /// but it goes through the same fallible header constructor as everything else rather than
    /// growing a second way to build a header.
    pub const fn frame(
        &self,
        max_frame_bytes: u32,
    ) -> Result<[u8; HANDSHAKE_FRAME_LEN], ProtocolError> {
        // build the header for a body of exactly one handshake
        let header = match Header::new(
            MessageType::Hello,
            Flags::NONE,
            HANDSHAKE_BODY_LEN,
            max_frame_bytes,
        ) {
            Ok(header) => header,
            Err(error) => return Err(error),
        };
        Ok(join(header.encode(), self.encode()))
    }
}

/// The frame a server answers a [`Hello`] with, whether it accepts or refuses
///
/// ```text
///  ┌──────────────────────┬───────────────────┬─────────┬───────────┬──────────┐
///  │ schema fingerprint   │ max frame bytes   │ reason  │ mechanism │ reserved │
///  │     (u64 LE, 8 B)    │   (u32 LE, 4 B)   │  (1 B)  │   (1 B)   │  (2 B)   │
///  └──────────────────────┴───────────────────┴─────────┴───────────┴──────────┘
/// ```
///
/// A refusal is a `HelloAck` too. The server always answers before it closes, so that the client
/// learns *why* it was refused instead of seeing a reset, and it can always do so because the
/// header layout is the same in every protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloAck {
    /// The fingerprint of the schema this server was built from
    pub schema_fingerprint: u64,
    /// The largest frame this server will accept
    pub max_frame_bytes: u32,
    /// Whether this server accepted the connection, and if not why
    pub reason: RefusalReason,
    /// The one mechanism this client has to prove itself with, if it has to prove itself at all
    ///
    /// The server picks, from what the client offered. `None` means this server requires nothing
    /// and the connection is usable as soon as this frame is read, which is what every server
    /// with no `auth` section answers and what every server answered before there was one.
    pub mechanism: Option<AuthMechanism>,
}

impl HelloAck {
    /// Write this ack out as the sixteen bytes that go on the wire
    pub const fn encode(&self) -> [u8; HANDSHAKE_BODY_LEN] {
        // split each field into its little endian bytes
        let fingerprint = self.schema_fingerprint.to_le_bytes();
        let max = self.max_frame_bytes.to_le_bytes();
        // a server that requires nothing writes a zero here, which is what an older one wrote too
        let mechanism = match self.mechanism {
            Some(mechanism) => mechanism.as_byte(),
            None => 0,
        };
        // the last two bytes are reserved and are written as zeroes
        [
            fingerprint[0],
            fingerprint[1],
            fingerprint[2],
            fingerprint[3],
            fingerprint[4],
            fingerprint[5],
            fingerprint[6],
            fingerprint[7],
            max[0],
            max[1],
            max[2],
            max[3],
            self.reason.as_byte(),
            mechanism,
            0,
            0,
        ]
    }

    /// Read an ack from the sixteen bytes it was written as
    ///
    /// A mechanism byte this build cannot name is read as `None`, which is safe in exactly one
    /// direction and it is this one: the client is the peer that has to *do* the mechanism, so a
    /// name it does not know leaves it with nothing to send, and it refuses the connection itself
    /// rather than proceeding unauthenticated. A server never reads this field.
    ///
    /// # Arguments
    ///
    /// * `raw` - The sixteen body bytes to read
    pub const fn decode(raw: &[u8; HANDSHAKE_BODY_LEN]) -> Self {
        // a zero here is no mechanism, and anything we cannot name is treated as one we cannot do
        let mechanism = match AuthMechanism::from_byte(raw[13]) {
            Ok(mechanism) => Some(mechanism),
            Err(_) => None,
        };
        HelloAck {
            schema_fingerprint: u64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]),
            max_frame_bytes: u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]),
            reason: RefusalReason::from_byte(raw[12]),
            mechanism,
        }
    }

    /// Build the whole frame, header included, ready for a single write
    ///
    /// A refusal sets [`Flags::REFUSED`] so that a peer can tell an acceptance from a refusal
    /// without reading the body at all.
    pub const fn frame(
        &self,
        max_frame_bytes: u32,
    ) -> Result<[u8; HANDSHAKE_FRAME_LEN], ProtocolError> {
        // flag a refusal in the header as well as in the body
        let flags = if self.reason.is_accepted() {
            Flags::NONE
        } else {
            Flags::REFUSED
        };
        // build the header for a body of exactly one handshake
        let header = match Header::new(
            MessageType::HelloAck,
            flags,
            HANDSHAKE_BODY_LEN,
            max_frame_bytes,
        ) {
            Ok(header) => header,
            Err(error) => return Err(error),
        };
        Ok(join(header.encode(), self.encode()))
    }
}

/// Lay a handshake header and body down into one buffer
///
/// # Arguments
///
/// * `header` - The encoded header to write first
/// * `body` - The encoded body to write after it
const fn join(
    header: [u8; HEADER_LEN],
    body: [u8; HANDSHAKE_BODY_LEN],
) -> [u8; HANDSHAKE_FRAME_LEN] {
    // copy each half into place by index, since slices cannot be copied in a const fn
    let mut frame = [0u8; HANDSHAKE_FRAME_LEN];
    let mut i = 0;
    while i < HEADER_LEN {
        frame[i] = header[i];
        i += 1;
    }
    let mut j = 0;
    while j < HANDSHAKE_BODY_LEN {
        frame[HEADER_LEN + j] = body[j];
        j += 1;
    }
    frame
}
