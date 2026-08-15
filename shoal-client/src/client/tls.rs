//! Driving a TLS handshake over tokio, then getting out of the way
//!
//! The mirror of [`server::tls`], over the other runtime. Every decision either of them makes is
//! in [`shared::tls`]; what is here is the pump and the `setsockopt`.
//!
//! # Invariants
//!
//! **This runs before the stream is split**, for the same reason the Shoal handshake and the SCRAM
//! exchange do: everything it reads would otherwise be handed to the proxy, which would decode a
//! handshake flight as a response to a query nobody sent.
//!
//! **After this returns, `TcpProxy` does not know TLS exists.** Its two `read_exact` calls and the
//! `AlignedVec<16>` between them are unchanged from the plaintext path, because the kernel is
//! doing the record layer — a response still lands, once, in memory the client allocated and
//! aligned. That is the entire property
//! [F14](../../../docs/src/features/encryption-in-transit.md) exists to preserve, and the reason
//! this file is forty lines rather than four hundred.
//!
//! [`server::tls`]: crate::server::tls
//! [`shared::tls`]: crate::shared::tls

use rustls::client::ClientConnectionData;
use rustls::ClientConfig;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ConnectError;
use shoal_proto::shared::tls::{
    ktls, record_body_len, server_name, Established, TlsClientHandshake, TlsClientOptions,
    TlsError, TlsStep, RECORD_HEADER_LEN,
};

/// Take the wire on a freshly opened connection
///
/// Returns what rustls kept of the session, which the caller holds for the life of the connection
/// so that a key update has somewhere to be handled.
///
/// # Arguments
///
/// * `stream` - The connection to take, before it has been split
/// * `config` - What this client trusts
/// * `options` - Which name to ask the server for
/// * `addr` - The address being connected to, used when no name was configured
pub async fn connect(
    stream: &mut TcpStream,
    config: &Arc<ClientConfig>,
    options: &TlsClientOptions,
    addr: &std::net::SocketAddr,
) -> Result<Established<ClientConnectionData>, ConnectError> {
    // work out what name this server's certificate has to carry before talking to it
    let name = server_name(options, addr)?;
    let mut handshake = TlsClientHandshake::client(config.clone(), name)?;
    loop {
        match handshake.step().map_err(ConnectError::Tls)? {
            // rustls encoded a flight, so put it on the wire before asking it anything else
            TlsStep::Transmit => flush(stream, &mut handshake).await?,
            // rustls wants more from the server than it has been given
            TlsStep::NeedRead => {
                // anything already encoded goes out first, or both peers wait on each other
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
    // and the kernel can take it over cleanly
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
    handshake: &mut TlsClientHandshake,
) -> Result<(), ConnectError> {
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
/// **Exactly one record, never more.** Reading past the server's last handshake record would eat
/// the first bytes of whatever it sends afterwards, and those bytes cannot be handed to the kernel
/// once it owns the socket. See [`record_body_len`].
///
/// # Arguments
///
/// * `stream` - The connection to read from
/// * `handshake` - The handshake to feed
async fn read_record(
    stream: &mut TcpStream,
    handshake: &mut TlsClientHandshake,
) -> Result<(), ConnectError> {
    // read the header first, so the body's size is known before anything allocates for it
    let mut header = [0u8; RECORD_HEADER_LEN];
    if let Err(error) = stream.read_exact(&mut header).await {
        // a server that closed mid handshake is not one this client can talk to
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(TlsError::Closed.into());
        }
        return Err(error.into());
    }
    // now that the length has been judged, read the body it named
    let mut body = vec![0u8; record_body_len(&header).map_err(ConnectError::Tls)?];
    stream.read_exact(&mut body).await?;
    handshake.feed(&header);
    handshake.feed(&body);
    Ok(())
}
