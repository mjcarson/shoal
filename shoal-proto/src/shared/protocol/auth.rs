//! The two frames an authentication exchange is carried on
//!
//! ```text
//!  Auth          ┌──────────────┬───────────┬──────────────────────┐
//!  client→server │ mechanism    │ reserved  │ payload              │
//!                │   (1 B)      │  (3 B)    │  len = rest of body  │
//!                └──────────────┴───────────┴──────────────────────┘
//!  AuthResponse  ┌──────────────┬───────────┬──────────────────────┐
//!  server→client │ status       │ reserved  │ payload              │
//!                │   (1 B)      │  (3 B)    │  len = rest of body  │
//!                └──────────────┴───────────┴──────────────────────┘
//! ```
//!
//! # Invariants
//!
//! **These frames carry no query id.** They belong to the part of a connection that happens before
//! it is split, alongside [`Hello`] and [`HelloAck`], which is why the rule that every frame a
//! server sends a client carries a query id ([`error`]) does not reach them — the client reading
//! them is `ShoalConnectionManager::connect`, not the response proxy, and it reads one frame at a
//! time knowing exactly which one it asked for.
//!
//! **This module holds no cryptography.** The payloads are opaque bytes here and are given meaning
//! by [`shared::auth`], which is a separate module for the same reason [`super`] has no rkyv in it:
//! this one has to stay reachable from `core` and `uuid` alone so it can move to a client only
//! crate later.
//!
//! **The discriminants of [`AuthMechanism`] and [`AuthStatus`] are never renumbered and never
//! reused**, the same rule [`MessageType`] and [`ErrorCode`] follow, and for the same reason: they
//! are on the wire. Both start at 1 so that a zeroed buffer decodes as neither.
//!
//! **A mechanism is selected by the server, from what the client offered.** The client puts a
//! bitmap of what it can do in its [`Hello`] and the server names exactly one in its [`HelloAck`],
//! which is the shape Cassandra's `AUTHENTICATE` uses. A client never picks, so a client that
//! prefers the weaker of two mechanisms cannot talk a server into it.
//!
//! [`Hello`]: super::handshake::Hello
//! [`HelloAck`]: super::handshake::HelloAck
//! [`error`]: super::error
//! [`shared::auth`]: crate::shared::auth
//! [`ErrorCode`]: super::error::ErrorCode

use super::{Flags, Header, MessageType, ProtocolError, HEADER_LEN};

/// The fixed part of an auth frame's body: its mechanism or status, and three reserved bytes
pub const AUTH_BODY_MIN: usize = 4;

/// The longest payload either auth frame will carry, in bytes
///
/// A SASL message is a few hundred bytes of text. Bounding it separately from the frame bound is
/// what stops the handshake being an allocation channel for a peer that has not authenticated yet:
/// without this, anything that can reach the port can name a 64 mebibyte auth frame and have it
/// allocated before it has proved anything at all. This is the same argument [`MAX_ERROR_MSG_LEN`]
/// makes, against a peer that has fewer reasons to be trusted.
///
/// [`MAX_ERROR_MSG_LEN`]: super::error::MAX_ERROR_MSG_LEN
pub const MAX_AUTH_PAYLOAD_LEN: usize = 4096;

/// The largest whole body either auth frame will carry, in bytes
///
/// This is what a reader hands [`Header::decode`] as its frame bound, rather than the connection's
/// `max_frame_bytes`, so that a peer which has not authenticated yet cannot name an allocation
/// four orders of magnitude larger than any message it could legitimately be sending. It is the
/// payload bound plus the fixed part, so a payload of exactly [`MAX_AUTH_PAYLOAD_LEN`] still fits.
pub const MAX_AUTH_FRAME_BODY: u32 = (AUTH_BODY_MIN + MAX_AUTH_PAYLOAD_LEN) as u32;

/// How a peer proves who it is
///
/// Only [`AuthMechanism::ScramSha256`] can be selected today. [`AuthMechanism::MutualTls`] is
/// defined and refused, so that the mechanism it names is a new arm in one match rather than a new
/// handshake, once there is a TLS layer to read a certificate subject off of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AuthMechanism {
    /// A username and a password, proved without the password crossing the wire
    ScramSha256 = 1,
    /// The subject of the peer's certificate - reserved, and not selectable until there is TLS
    MutualTls = 2,
}

impl AuthMechanism {
    /// Get the byte this mechanism is written as
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a mechanism from the byte it was written as
    ///
    /// Zero is not a mechanism. It is what an old peer and an anonymous connection both write, and
    /// the callers that need to tell "no mechanism" apart from "a mechanism I do not know" check
    /// for it before they come here.
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a mechanism from
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        // map each known discriminant back to its variant
        match raw {
            1 => Ok(AuthMechanism::ScramSha256),
            2 => Ok(AuthMechanism::MutualTls),
            // zero included, since a frame that names no mechanism is not an auth frame
            unknown => Err(ProtocolError::UnknownAuthMechanism(unknown)),
        }
    }

    /// Get the name of this mechanism
    ///
    /// These are the SASL names, so that a log line and an RFC agree with each other.
    pub const fn name(self) -> &'static str {
        match self {
            AuthMechanism::ScramSha256 => "SCRAM-SHA-256",
            AuthMechanism::MutualTls => "MUTUAL-TLS",
        }
    }

    /// Parse a mechanism from the SASL name a config file spells it with
    ///
    /// The comparison is case insensitive because a config file is typed by a person, and the
    /// names are the SASL ones for the reason [`AuthMechanism::name`] gives.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to parse a mechanism from
    pub fn from_name(name: &str) -> Option<Self> {
        // walk every mechanism rather than matching on strings, so a new one cannot be forgotten
        [AuthMechanism::ScramSha256, AuthMechanism::MutualTls]
            .into_iter()
            .find(|mechanism| mechanism.name().eq_ignore_ascii_case(name))
    }

    /// Get this mechanism as a set containing only itself
    pub const fn bit(self) -> AuthMechanisms {
        AuthMechanisms(1 << (self.as_byte() - 1))
    }
}

impl std::fmt::Display for AuthMechanism {
    /// Write this mechanism's name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// The set of mechanisms a client says it can do
///
/// # Invariants
///
/// **Unknown bits are preserved, never rejected**, for the reason [`Flags`] gives: a bit this
/// build does not know about is a bit a newer peer is using, and a server that masked them off
/// would make the first new mechanism a compatibility break rather than an addition. A server only
/// ever asks this set whether it contains a mechanism *the server* wants, so a bit it cannot name
/// costs it nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct AuthMechanisms(u16);

impl AuthMechanisms {
    /// This peer offers no mechanism at all
    pub const NONE: AuthMechanisms = AuthMechanisms(0);

    /// This peer can do SCRAM-SHA-256
    pub const SCRAM_SHA_256: AuthMechanisms = AuthMechanism::ScramSha256.bit();

    /// This peer can do mutual TLS - reserved
    pub const MUTUAL_TLS: AuthMechanisms = AuthMechanism::MutualTls.bit();

    /// Build a mechanism set from its raw bits
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw bits to build a mechanism set from
    pub const fn from_bits(raw: u16) -> Self {
        AuthMechanisms(raw)
    }

    /// Get the raw bits of this mechanism set
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Check if this set offers nothing
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Check if every mechanism in another set is in this one
    ///
    /// # Arguments
    ///
    /// * `other` - The mechanisms to check for
    pub const fn contains(self, other: AuthMechanisms) -> bool {
        self.0 & other.0 == other.0
    }

    /// Combine two mechanism sets
    ///
    /// # Arguments
    ///
    /// * `other` - The mechanisms to add to this set
    pub const fn union(self, other: AuthMechanisms) -> Self {
        AuthMechanisms(self.0 | other.0)
    }

    /// Pick the first mechanism in a preference order that this set also offers
    ///
    /// The preference order is the *server's*, and this is the only place a mechanism is chosen.
    /// Walking the server's list rather than the client's bits is what makes the server's ordering
    /// authoritative instead of incidental on which bit happens to be lowest.
    ///
    /// # Arguments
    ///
    /// * `preference` - The mechanisms the server will accept, strongest first
    pub fn first_supported(self, preference: &[AuthMechanism]) -> Option<AuthMechanism> {
        // take the first thing we want that they can also do
        preference
            .iter()
            .copied()
            .find(|mechanism| self.contains(mechanism.bit()))
    }
}

/// Where a server says an exchange has got to
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AuthStatus {
    /// The server wants another round, and the payload is what it wants answered
    Challenge = 1,
    /// The peer is authenticated, and the payload is the last thing the mechanism had to say
    Success = 2,
    /// The peer is not authenticated, and the payload says why in prose
    Failed = 3,
}

impl AuthStatus {
    /// Get the byte this status is written as
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a status from the byte it was written as
    ///
    /// This fails rather than falling back to [`AuthStatus::Failed`], because a status this build
    /// cannot read is a server that is out of step with us and not a server that refused us, and a
    /// client that reported the second would send someone to check a password that is fine.
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse a status from
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        // map each known discriminant back to its variant
        match raw {
            1 => Ok(AuthStatus::Challenge),
            2 => Ok(AuthStatus::Success),
            3 => Ok(AuthStatus::Failed),
            // zero included, so that a zeroed buffer is never read as a successful login
            unknown => Err(ProtocolError::UnknownAuthStatus(unknown)),
        }
    }

    /// Get the name of this status
    pub const fn name(self) -> &'static str {
        match self {
            AuthStatus::Challenge => "Challenge",
            AuthStatus::Success => "Success",
            AuthStatus::Failed => "Failed",
        }
    }
}

impl std::fmt::Display for AuthStatus {
    /// Write this status' name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Build a whole auth frame, header included, ready for a single write
///
/// These are built into one buffer rather than returned as a preamble and left to be written
/// beside their payload, which is what the response and error paths do. Those are the hot path and
/// this is two round trips per connection, so the thing worth optimizing here is the number of
/// syscalls a stalled handshake is holding open rather than the number of copies it makes.
///
/// # Arguments
///
/// * `mechanism` - The mechanism this peer is proving itself with
/// * `payload` - The mechanism's own bytes, which mean nothing to this module
/// * `max_frame_bytes` - The largest frame the peer will accept
pub fn encode_auth(
    mechanism: AuthMechanism,
    payload: &[u8],
    max_frame_bytes: u32,
) -> Result<Vec<u8>, ProtocolError> {
    // build the frame with the mechanism in the byte a status would otherwise be in
    encode_auth_frame(
        MessageType::Auth,
        Flags::NONE,
        mechanism.as_byte(),
        payload,
        max_frame_bytes,
    )
}

/// Build a whole auth response frame, header included, ready for a single write
///
/// A refusal sets [`Flags::REFUSED`] as well as saying so in its status byte, so that a peer can
/// tell a refusal from a challenge without reading the body — the same redundancy [`HelloAck`]
/// already has, and for the same reason.
///
/// # Arguments
///
/// * `status` - Where this exchange has got to
/// * `payload` - The mechanism's own bytes, or the reason in prose when this is a refusal
/// * `max_frame_bytes` - The largest frame the peer will accept
///
/// [`HelloAck`]: super::handshake::HelloAck
pub fn encode_auth_response(
    status: AuthStatus,
    payload: &[u8],
    max_frame_bytes: u32,
) -> Result<Vec<u8>, ProtocolError> {
    // flag a refusal in the header as well as in the body
    let flags = match status {
        AuthStatus::Failed => Flags::REFUSED,
        _ => Flags::NONE,
    };
    encode_auth_frame(
        MessageType::AuthResponse,
        flags,
        status.as_byte(),
        payload,
        max_frame_bytes,
    )
}

/// Lay an auth frame's header, its leading byte and its payload down into one buffer
///
/// # Arguments
///
/// * `kind` - The kind of message this frame carries
/// * `flags` - The flags to set on this frame
/// * `leading` - The mechanism or status byte this frame's body opens with
/// * `payload` - The mechanism's own bytes
/// * `max_frame_bytes` - The largest frame the peer will accept
fn encode_auth_frame(
    kind: MessageType,
    flags: Flags,
    leading: u8,
    payload: &[u8],
    max_frame_bytes: u32,
) -> Result<Vec<u8>, ProtocolError> {
    // refuse a payload past our own bound before the peer's, since ours is the tighter of the two
    if payload.len() > MAX_AUTH_PAYLOAD_LEN {
        return Err(ProtocolError::PayloadTooLarge {
            len: payload.len(),
            max: MAX_AUTH_PAYLOAD_LEN as u32,
        });
    }
    // the leading byte and its reserved bytes are part of the body, so they count towards the length
    let body_len = AUTH_BODY_MIN.saturating_add(payload.len());
    let header = Header::new(kind, flags, body_len, max_frame_bytes)?;
    // lay the header down, then the leading byte, then three reserved zeroes, then the payload
    let mut frame = Vec::with_capacity(HEADER_LEN + body_len);
    frame.extend_from_slice(&header.encode());
    frame.push(leading);
    frame.extend_from_slice(&[0, 0, 0]);
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Work out how many payload bytes an auth frame carries, before anything allocates for them
///
/// # Arguments
///
/// * `header` - The header of the auth frame to size
pub const fn payload_len(header: Header) -> Result<usize, ProtocolError> {
    // an auth frame always carries a leading byte and its reserved bytes, so a shorter one cannot
    // be one
    let payload_len = match header.body_len().checked_sub(AUTH_BODY_MIN) {
        Some(payload_len) => payload_len,
        None => {
            return Err(ProtocolError::BodyTooShort {
                need: AUTH_BODY_MIN,
                got: header.len,
            })
        }
    };
    // refuse a payload past our own bound, which is tighter than the frame bound that let it in
    if payload_len > MAX_AUTH_PAYLOAD_LEN {
        return Err(ProtocolError::FrameTooLarge {
            len: header.len,
            max: MAX_AUTH_PAYLOAD_LEN as u32,
        });
    }
    Ok(payload_len)
}

/// Read the mechanism and the payload out of an auth frame's body
///
/// # Arguments
///
/// * `body` - The whole body of the frame: the mechanism, its reserved bytes, and the payload
pub fn decode_auth_body(body: &[u8]) -> Result<(AuthMechanism, &[u8]), ProtocolError> {
    // the leading byte and its reserved bytes have to be here before there can be a payload at all
    let (leading, payload) = split_auth_body(body)?;
    Ok((AuthMechanism::from_byte(leading)?, payload))
}

/// Read the status and the payload out of an auth response frame's body
///
/// # Arguments
///
/// * `body` - The whole body of the frame: the status, its reserved bytes, and the payload
pub fn decode_auth_response_body(body: &[u8]) -> Result<(AuthStatus, &[u8]), ProtocolError> {
    // the leading byte and its reserved bytes have to be here before there can be a payload at all
    let (leading, payload) = split_auth_body(body)?;
    Ok((AuthStatus::from_byte(leading)?, payload))
}

/// Split an auth frame's body into its leading byte and its payload
///
/// # Arguments
///
/// * `body` - The whole body of the frame
#[allow(clippy::cast_possible_truncation)]
fn split_auth_body(body: &[u8]) -> Result<(u8, &[u8]), ProtocolError> {
    // the fixed part of the body has to be here before there can be a payload at all
    if body.len() < AUTH_BODY_MIN {
        return Err(ProtocolError::BodyTooShort {
            need: AUTH_BODY_MIN,
            // a body this short cannot overflow a u32
            got: body.len() as u32,
        });
    }
    // everything after the leading byte and its three reserved bytes is the payload
    Ok((body[0], &body[AUTH_BODY_MIN..]))
}
