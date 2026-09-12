//! Taking the wire on a peer connection, in either direction
//!
//! The accepting side is exactly what a client connection does
//! ([`server::tls::accept`](crate::server::tls::accept)) with the mutual config from
//! [`peer_server_config`](crate::shared::tls::peer_server_config). The dialling side is new to
//! the engine: a client handshake driven over a glommio socket, one record per read, then the
//! keys handed to the kernel the same way. Every invariant of the server side holds here -
//! nothing is written between the handshake finishing and kTLS taking the socket, and after
//! this returns the link's reads and writes do not know TLS exists.

use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rustls::client::ClientConnectionData;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use crate::server::ServerError;
use crate::shared::tls::{
    ktls, record_body_len, Established, TlsClientHandshake, TlsError, TlsStep, RECORD_HEADER_LEN,
};

/// Take the wire on a connection this node dialled
///
/// Returns what rustls kept of the session, which the link holds for the life of the
/// connection so that a key update has somewhere to be handled.
///
/// # Arguments
///
/// * `stream` - The connection, before anything has been written on it
/// * `config` - What this node proves itself with and checks the peer against
/// * `name` - The name the peer's certificate has to carry
pub async fn connect(
    stream: &mut TcpStream,
    config: Arc<ClientConfig>,
    name: ServerName<'static>,
) -> Result<Established<ClientConnectionData>, ServerError> {
    // drive the handshake until rustls says the session is established
    let mut handshake = TlsClientHandshake::client(config, name)?;
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
/// Exactly one, never more: reading past the peer's last handshake record would eat the first
/// bytes it sends afterwards, and those cannot be handed to the kernel once it owns the socket.
///
/// # Arguments
///
/// * `stream` - The connection to read from
/// * `handshake` - The handshake to feed
async fn read_record(
    stream: &mut TcpStream,
    handshake: &mut TlsClientHandshake,
) -> Result<(), ServerError> {
    // read the header first, so the body's size is known before anything allocates for it
    let mut header = [0u8; RECORD_HEADER_LEN];
    if let Err(error) = stream.read_exact(&mut header).await {
        // a peer that closed mid handshake refused us, most likely for the certificate we sent
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
