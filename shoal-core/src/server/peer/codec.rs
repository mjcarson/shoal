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
/// * `version` - The newest wire version this connection negotiated, which a frame above is
///   refused by ([F48](../../../../docs/src/features/rolling-compatibility.md))
pub async fn read_header(
    rx: &mut ReadHalf<TcpStream>,
    max_frame_bytes: u32,
    version: u8,
) -> Result<Option<Header>, ServerError> {
    // the header is the one read that may find nothing at all
    let mut raw = [0u8; HEADER_LEN];
    match rx.read_exact(&mut raw).await {
        Ok(()) => (),
        // a clean end of stream between frames is the peer going away, not a failure
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    // judge it before its length is used for anything, at the version this connection speaks
    Ok(Some(
        protocol::RawHeader::decode(&raw).validate_at(version, max_frame_bytes)?,
    ))
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
pub async fn read_body(
    rx: &mut ReadHalf<TcpStream>,
    len: usize,
) -> Result<AlignedVec, ServerError> {
    let mut body = AlignedVec::with_capacity(len);
    // the read overwrites every byte, so the buffer is not zeroed first: the allocation is claimed
    // as the body as it is (F106)
    //
    // SAFETY: `with_capacity` reserved at least `len` bytes, and nothing reads `body` before the
    // `read_exact` below fills every one of them; a read that fails drops it, never handed out. So
    // a body this returns is one a read filled to its length: F25's argument for a request body
    // (`RequestBody::read_from`), whose read is the same `read_exact` of the same socket.
    unsafe { body.set_len(len) };
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
    write_slices(tx, &mut slices).await
}

/// Write slices of one frame or several, in order, every byte landing
///
/// The fork's vectored write takes at most 1,024 slices a call, and this calls it again for the
/// rest until none is left ([F111](../../../../docs/src/features/lane-buffers.md)).
///
/// # Arguments
///
/// * `tx` - The connection's write half
/// * `slices` - The bytes, in the order they go on the wire
pub async fn write_slices(
    tx: &mut WriteHalf<TcpStream>,
    slices: &mut [IoSlice<'_>],
) -> Result<(), ServerError> {
    let mut bufs = slices;
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

/// Build the header for a frame about to be written, at the floor
///
/// A link stamps its negotiated version over this as the frame goes out; a listener's relay
/// builds its answers with [`header_at`] instead.
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
    header_at(protocol::MIN_PEER_VERSION, kind, body_len, max_frame_bytes)
}

/// Build the header for a frame about to be written at a named version
///
/// # Arguments
///
/// * `version` - The version the body is encoded at, or the one the connection negotiated
/// * `kind` - What the frame carries
/// * `body_len` - How many bytes follow the header
/// * `max_frame_bytes` - The largest frame the peer accepts
pub fn header_at(
    version: u8,
    kind: protocol::MessageType,
    body_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; HEADER_LEN], ProtocolError> {
    Ok(Header::at(
        version,
        kind,
        protocol::Flags::NONE,
        body_len,
        max_frame_bytes,
    )?
    .encode())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{AsyncReadExt, AsyncWriteExt};
    use glommio::net::{TcpListener, TcpStream};

    /// A body read into a buffer that is never zeroed is every byte its peer sent, however the
    /// peer cut them, and one the peer ends short of its length is an error, never a body (F106)
    #[test]
    fn a_body_is_every_byte_its_peer_sent() {
        glommio::LocalExecutorBuilder::default()
            .spawn(|| async {
                let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
                let addr = listener.local_addr().expect("an address");
                let sent: Vec<u8> = (0..200_000u32).map(|at| (at % 253) as u8).collect();
                let writer = glommio::spawn_local({
                    let sent = sent.clone();
                    async move {
                        let mut stream = TcpStream::connect(addr).await.expect("connect");
                        // the body in uneven pieces, a pause between them
                        for piece in sent.chunks(7_919) {
                            stream.write_all(piece).await.expect("written");
                            stream.flush().await.expect("flushed");
                            glommio::timer::sleep(Duration::from_micros(200)).await;
                        }
                        // then a thousand bytes of a body of two thousand, and gone
                        stream.write_all(&sent[..1000]).await.expect("written");
                        stream.flush().await.expect("flushed");
                    }
                })
                .detach();
                let stream = listener.accept().await.expect("accept");
                let (mut rx, _tx) = stream.split();
                let body = super::read_body(&mut rx, sent.len()).await.expect("a body");
                assert!(body[..] == sent[..], "the body is not the bytes its peer sent");
                writer.await;
                assert!(
                    super::read_body(&mut rx, 2_000).await.is_err(),
                    "a body its peer ended short was handed out"
                );
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }
}
