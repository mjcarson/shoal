//! Reading and writing peer frames over a glommio socket
//!
//! The proto crate decides what every byte means and this decides nothing: it is `read_exact`
//! of a fixed size array, one pure call into the codec, and `read_exact` of the body, the same
//! shape the client relays have. What it adds is the one rule the relays also follow - a body
//! that will be accessed in place as an archive is read into a fresh aligned allocation at
//! offset zero, never into the tail of the buffer its head was read into.

use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rkyv::util::AlignedVec;
use std::io::IoSlice;

use crate::server::ServerError;
use crate::shared::protocol::{self, Header, ProtocolError, HEADER_LEN};

/// Read the next frame's header, or learn that the peer closed cleanly between frames
///
/// # Arguments
///
/// * `rx` - The read half of the connection
/// * `max_frame_bytes` - The largest frame this end accepts
pub async fn read_header(
    rx: &mut ReadHalf<TcpStream>,
    max_frame_bytes: u32,
) -> Result<Option<Header>, ServerError> {
    // the header is the one read that may find nothing at all
    let mut raw = [0u8; HEADER_LEN];
    match rx.read_exact(&mut raw).await {
        Ok(()) => (),
        // a clean end of stream between frames is the peer going away, not a failure
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    // judge it before its length is used for anything
    Ok(Some(Header::decode(&raw, max_frame_bytes)?))
}

/// Read a fixed size head into an array of its own
///
/// # Arguments
///
/// * `rx` - The read half of the connection
pub async fn read_array<const N: usize>(
    rx: &mut ReadHalf<TcpStream>,
) -> Result<[u8; N], ServerError> {
    let mut raw = [0u8; N];
    rx.read_exact(&mut raw).await?;
    Ok(raw)
}

/// Read a body of a known length into a fresh aligned allocation
///
/// Aligned because a forwarded answer and a forwarded bundle are both rkyv archives accessed
/// in place; a plain `Vec` would work almost every time, which is the worst kind of working.
///
/// # Arguments
///
/// * `rx` - The read half of the connection
/// * `len` - How many bytes to read, which the header has already bounded
pub async fn read_body(rx: &mut ReadHalf<TcpStream>, len: usize) -> Result<AlignedVec, ServerError> {
    let mut body = AlignedVec::with_capacity(len);
    // the read overwrites every byte, so the buffer is not zeroed first
    body.resize(len, 0);
    rx.read_exact(&mut body[..]).await?;
    Ok(body)
}

/// Read a body of a known length into a plain vec
///
/// For heads and JSON, which are never accessed in place.
///
/// # Arguments
///
/// * `rx` - The read half of the connection
/// * `len` - How many bytes to read, which the header has already bounded
pub async fn read_vec(rx: &mut ReadHalf<TcpStream>, len: usize) -> Result<Vec<u8>, ServerError> {
    let mut body = vec![0u8; len];
    rx.read_exact(&mut body).await?;
    Ok(body)
}

/// Check a header names the frame kind a lane expects here
///
/// # Arguments
///
/// * `header` - The header read
/// * `expected` - The one kind acceptable at this point
pub fn expect(header: Header, expected: protocol::MessageType) -> Result<Header, ServerError> {
    header.expect(expected).map_err(ServerError::from)
}

/// Write one frame: a header and every part of its body, in order
///
/// Vectored so a forward's preamble, entries and bundle go out in one call without being
/// copied into one buffer first; the bundle in particular is the client's bytes, shared with
/// every shard that answers part of it, and is never copied to be sent.
///
/// # Arguments
///
/// * `tx` - The write half of the connection
/// * `header` - The encoded header
/// * `parts` - The body, in the order it goes on the wire
pub async fn write_frame(
    tx: &mut WriteHalf<TcpStream>,
    header: &[u8; HEADER_LEN],
    parts: &[&[u8]],
) -> Result<(), ServerError> {
    // one slice per part, header first
    let mut slices = Vec::with_capacity(parts.len() + 1);
    slices.push(IoSlice::new(header));
    for part in parts {
        if !part.is_empty() {
            slices.push(IoSlice::new(part));
        }
    }
    let mut bufs = &mut slices[..];
    // keep writing until every byte of every part is the socket's problem
    while !bufs.is_empty() {
        match tx.write_vectored(bufs).await {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "wrote no bytes of a peer frame",
                )
                .into());
            }
            Ok(n) => IoSlice::advance_slices(&mut bufs, n),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Build the header for a frame about to be written
///
/// # Arguments
///
/// * `kind` - What the frame carries
/// * `body_len` - How many bytes follow the header
/// * `max_frame_bytes` - The largest frame the peer accepts
pub fn header(
    kind: protocol::MessageType,
    body_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; HEADER_LEN], ProtocolError> {
    Ok(Header::new(kind, protocol::Flags::NONE, body_len, max_frame_bytes)?.encode())
}
