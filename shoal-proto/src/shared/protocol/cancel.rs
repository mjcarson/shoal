//! A client abandoning a bundle it will never read: the `Cancel` frame
//! ([F75](../../../../docs/src/features/client-cancel.md))
//!
//! ```text
//!  cancel : [header, type 12, no flags][bundle id 16 B]
//! ```
//!
//! `Cancel` was reserved as message type 12 at [F10](../../../../docs/src/features/framing-and-protocol-evolution.md)
//! and unwired until F75. A client sends one when a caller stops reading a bundle's answers before
//! they are all in - it dropped the stream, or the stream ended early on its deadline or an
//! error - and only on a connection whose hello was granted [`CLIENT_CAP_CANCEL`]: a server that
//! did not grant it refuses the frame the way it always refused one, by ending the connection.
//!
//! **A cancel names a bundle, and applies to every arrival of that bundle on its connection that
//! came before it.** A retry, or any bundle sent again under the same id afterwards, is untouched:
//! the frames of one connection are read in order, so "before" is a fact both ends agree on. The
//! server answers every cancel it reads with exactly one `Error` frame of code
//! [`ErrorCode::Cancelled`](super::error::ErrorCode::Cancelled) under the bundle's id, the last
//! frame the cancelled arrivals produce on that connection; a client never delivers it to a
//! caller, since a retry may hold the id by then.
//!
//! The header's flags are kept clear. A later per-query cancel would name an index after the id
//! behind a flag and a capability of its own, so this decoder refuses any flag and any length
//! but its own rather than reading past what it knows.

use uuid::Uuid;

use super::{Flags, Header, MessageType, ProtocolError, HEADER_LEN, QUERY_ID_LEN};

/// The client sends `Cancel` frames, and the server acts on them
///
/// Spent from the hello's capability byte after [`CLIENT_CAP_LEADER_HINTS`](super::read::CLIENT_CAP_LEADER_HINTS);
/// the ack's copy is what the server granted. A server built before F75 grants nothing here,
/// and is sent no cancel.
pub const CLIENT_CAP_CANCEL: u8 = 1 << 3;

/// The bytes after a cancel's header: the bundle's id
pub const CANCEL_BODY_LEN: usize = QUERY_ID_LEN;

/// A whole cancel frame, header and body
pub const CANCEL_FRAME_LEN: usize = HEADER_LEN + CANCEL_BODY_LEN;

/// Build the frame that cancels a bundle
///
/// # Arguments
///
/// * `id` - The bundle to cancel
/// * `max_frame_bytes` - The server's frame bound
///
/// # Errors
///
/// Fails only if the server's frame bound is smaller than a cancel, which no handshake allows.
pub fn cancel_frame(
    id: &Uuid,
    max_frame_bytes: u32,
) -> Result<[u8; CANCEL_FRAME_LEN], ProtocolError> {
    // no flags: the bits are kept for a later per-query cancel
    let header = Header::new(
        MessageType::Cancel,
        Flags::NONE,
        CANCEL_BODY_LEN,
        max_frame_bytes,
    )?;
    // the header, then the id
    let mut frame = [0u8; CANCEL_FRAME_LEN];
    frame[..HEADER_LEN].copy_from_slice(&header.encode());
    frame[HEADER_LEN..].copy_from_slice(id.as_bytes());
    Ok(frame)
}

/// Check a cancel's header before its body is read
///
/// # Arguments
///
/// * `header` - The cancel's header, already decoded
///
/// # Errors
///
/// Fails if the frame carries a flag or a length other than a cancel's, which is a client this
/// build does not understand and is never guessed at.
pub const fn check_cancel(header: &Header) -> Result<(), ProtocolError> {
    // a flag is a section this build does not know how to read
    if header.flags.bits() != Flags::NONE.bits() {
        return Err(ProtocolError::MalformedCancel("a cancel carries no flags"));
    }
    // exactly an id, never more or less
    if header.body_len() != CANCEL_BODY_LEN {
        return Err(ProtocolError::MalformedCancel(
            "a cancel's body is the sixteen bytes of a bundle id",
        ));
    }
    Ok(())
}

/// Read the bundle a cancel names
///
/// # Arguments
///
/// * `body` - The cancel's body, after its header
#[must_use]
pub fn decode_cancel(body: &[u8; CANCEL_BODY_LEN]) -> Uuid {
    // the id is the whole body
    Uuid::from_bytes(*body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::protocol::{RawHeader, DEFAULT_MAX_FRAME_BYTES};

    /// A cancel is a header of type 12 and the id, and reads back as the id it was built from
    #[test]
    fn a_cancel_frame_round_trips() {
        // build one for a fresh id
        let id = Uuid::now_v7();
        let frame = cancel_frame(&id, DEFAULT_MAX_FRAME_BYTES).expect("a cancel frame");
        // the header names the type, no flags and the id's length
        let mut raw = [0u8; HEADER_LEN];
        raw.copy_from_slice(&frame[..HEADER_LEN]);
        let header = RawHeader::decode(&raw)
            .validate(DEFAULT_MAX_FRAME_BYTES)
            .expect("a valid header");
        assert_eq!(header.kind, MessageType::Cancel);
        assert_eq!(header.kind.as_byte(), 12);
        assert_eq!(header.flags, Flags::NONE);
        assert_eq!(header.body_len(), CANCEL_BODY_LEN);
        check_cancel(&header).expect("a cancel this build reads");
        // the body is the id
        let mut body = [0u8; CANCEL_BODY_LEN];
        body.copy_from_slice(&frame[HEADER_LEN..]);
        assert_eq!(decode_cancel(&body), id);
    }

    /// A cancel with a flag or a length other than an id's is refused rather than guessed at
    #[test]
    fn a_cancel_of_the_wrong_shape_is_refused() {
        // one byte longer than an id
        let long = Header::new(
            MessageType::Cancel,
            Flags::NONE,
            CANCEL_BODY_LEN + 1,
            1 << 20,
        )
        .expect("a header");
        assert!(matches!(
            check_cancel(&long),
            Err(ProtocolError::MalformedCancel(_))
        ));
        // one byte shorter
        let short = Header::new(
            MessageType::Cancel,
            Flags::NONE,
            CANCEL_BODY_LEN - 1,
            1 << 20,
        )
        .expect("a header");
        assert!(matches!(
            check_cancel(&short),
            Err(ProtocolError::MalformedCancel(_))
        ));
        // the right length under a flag this build does not know
        let flagged = Header::new(MessageType::Cancel, Flags::LAST, CANCEL_BODY_LEN, 1 << 20)
            .expect("a header");
        assert!(matches!(
            check_cancel(&flagged),
            Err(ProtocolError::MalformedCancel(_))
        ));
    }
}
