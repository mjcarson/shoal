//! Driving a TLS handshake over glommio, then getting out of the way
//!
//! [`shared::tls`] holds every decision this makes and none of the I/O. What is here is the twenty
//! lines that pump it over a glommio socket, and the `setsockopt` that hands the finished session
//! to the kernel.
//!
//! # Invariants
//!
//! **Nothing is written to the socket between the handshake finishing and kTLS being enabled.**
//! The kernel takes over at a record boundary and has no way to be told about bytes already in
//! flight, so [`accept`] does both and returns only once the socket carries plaintext.
//!
//! **After this returns, the relays do not know TLS exists.** `client_rx_relay`'s two `read_exact`
//! calls and `client_tx_relay`'s `write_vectored` are unchanged from the plaintext path, because
//! the kernel is doing the record layer. That is the whole point of
//! [F14](../../../docs/src/features/encryption-in-transit.md) and the property to protect if this
//! is ever revisited.
//!
//! [`shared::tls`]: crate::shared::tls

use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rustls::server::ServerConnectionData;
use rustls::ServerConfig;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use super::ServerError;
use crate::shared::tls::{
    ktls, record_body_len, Established, TlsError, TlsServerHandshake, TlsStep, RECORD_HEADER_LEN,
};

/// Take the wire on a freshly accepted connection
///
/// Returns what rustls kept of the session, which the caller holds for the life of the connection
/// so that a key update has somewhere to be handled.
///
/// # Arguments
///
/// * `stream` - The connection to take, before it has been split
/// * `config` - What this server proves itself with
pub async fn accept(
    stream: &mut TcpStream,
    config: &Arc<ServerConfig>,
) -> Result<Established<ServerConnectionData>, ServerError> {
    // drive the handshake until rustls says the session is established
    let mut handshake = TlsServerHandshake::server(config.clone())?;
    loop {
        match handshake.step()? {
            // rustls encoded a flight, so put it on the wire before asking it anything else
            TlsStep::Transmit => flush(stream, &mut handshake).await?,
            // rustls wants more from the peer than it has been given
            TlsStep::NeedRead => {
                // anything already encoded goes out first, or a peer waiting on it deadlocks
                flush(stream, &mut handshake).await?;
                read_record(stream, &mut handshake).await?;
            }
            // the session is established, so flush the last flight and hand it to the kernel
            TlsStep::Done => {
                flush(stream, &mut handshake).await?;
                break;
            }
        }
    }
    // take the keys out and give them to the kernel, which owns the record layer from here
    //
    // nothing has been read past the last handshake record, so the socket is idle at this moment
    // and the kernel can take it over cleanly - see `record_body_len` for why that matters
    let established = handshake.finish()?;
    ktls::enable(stream.as_raw_fd(), &established.secrets)?;
    Ok(established)
}

/// Write whatever this handshake has encoded, if anything
///
/// # Arguments
///
/// * `stream` - The connection to write to
/// * `handshake` - The handshake that may have a flight waiting
async fn flush(
    stream: &mut TcpStream,
    handshake: &mut TlsServerHandshake,
) -> Result<(), ServerError> {
    // a handshake with nothing to say is the ordinary case, so this is cheap
    if !handshake.has_outgoing() {
        return Ok(());
    }
    let outgoing = handshake.take_outgoing();
    stream.write_all(&outgoing).await?;
    stream.flush().await?;
    Ok(())
}

/// Read exactly one TLS record and give it to the handshake
///
/// # Invariants
///
/// **Exactly one record, never more.** See [`record_body_len`] — reading past the peer's last
/// handshake record would eat the first bytes it sends afterwards, and those bytes cannot be
/// handed to the kernel once it owns the socket.
///
/// # Arguments
///
/// * `stream` - The connection to read from
/// * `handshake` - The handshake to feed
async fn read_record(
    stream: &mut TcpStream,
    handshake: &mut TlsServerHandshake,
) -> Result<(), ServerError> {
    // read the header first, so the body's size is known before anything allocates for it
    let mut header = [0u8; RECORD_HEADER_LEN];
    if let Err(error) = stream.read_exact(&mut header).await {
        // a peer that closed mid handshake is not one this connection can be had with
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(TlsError::Closed.into());
        }
        return Err(error.into());
    }
    // now that the length has been judged, read the body it named
    let mut body = vec![0u8; record_body_len(&header)?];
    stream.read_exact(&mut body).await?;
    handshake.feed(&header);
    handshake.feed(&body);
    Ok(())
}
