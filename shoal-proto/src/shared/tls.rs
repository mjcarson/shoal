//! Taking the wire before either peer speaks Shoal over it
//!
//! [`ktls`] is the kernel side and this is everything above it: what a certificate and a key are
//! loaded from, what a TLS 1.3 session is allowed to negotiate, and a handshake that both peers
//! can drive over two different async runtimes.
//!
//! # Invariants
//!
//! **rustls performs the handshake and nothing else.** Once [`ktls::enable`] has run, rustls is out
//! of the data path — the kernel owns the record layer, `read` returns plaintext into whatever
//! buffer the caller names, and the client's response read lands in an `AlignedVec<16>` exactly as
//! it did before there was any encryption. That is the whole reason
//! [D4](../../../docs/src/direction/encryption.md) ended up here rather than on a userspace record
//! layer, and it is why nothing in `client.rs` or `shard.rs` had to learn what a TLS record is.
//!
//! **This module knows nothing about any async runtime.** The server reads with glommio and the
//! client reads with tokio, and the two share every decision here and none of the I/O, the same
//! way [`protocol`] already works. [`TlsClientHandshake`] and [`TlsServerHandshake`] are pumped by
//! their caller: they say *transmit this*, *read more*, or *done*, and each call site does its own
//! `read`/`write`.
//!
//! **Session tickets are off and they have to stay off.** See [`server_config`].
//!
//! **The crypto is not reachable from [`protocol`].** That module stays on `core` and `uuid` so
//! D5's crate split is a move rather than a rewrite, which is the same rule
//! [`auth`](super::auth) follows for the same reason.
//!
//! [`protocol`]: super::protocol

use rustls::client::danger::ServerCertVerifier;
use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::crypto::CryptoProvider;
use rustls::kernel::KernelConnection;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{ConnectionState, EncodeError, UnbufferedStatus};
use rustls::{ClientConfig, ExtractedSecrets, ServerConfig, SupportedCipherSuite};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod ktls;

#[cfg(test)]
mod tests;

/// The cipher suites this build will negotiate, strongest first
///
/// Only the two AES-GCM suites, because they are the two the kernel's TLS module can be given keys
/// for in the form [`ktls`] builds. ChaCha20-Poly1305 has a kernel spelling as well and is left out
/// deliberately: every machine this runs on has AES-NI, so the suite that exists for machines
/// without it would only ever be negotiated by accident.
pub const TLS_CIPHER_SUITES: &[SupportedCipherSuite] = &[
    rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
    rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256,
];

/// The size of a TLS record header, which is the same in every version
pub const RECORD_HEADER_LEN: usize = 5;

/// The largest record either peer will read during a handshake
///
/// TLS bounds a record's payload at 2^14 and allows a little over for expansion, so this is that
/// plus room. It exists for the same reason `max_frame_bytes` does: a length off the wire is used
/// as an allocation size before a byte of the body has arrived.
pub const MAX_RECORD_LEN: usize = (1 << 14) + 2048;

/// How many bytes follow the header of the record it belongs to
///
/// # Invariants
///
/// **A handshake reads exactly one record at a time, and never more.** This is not an
/// optimization — it is what keeps the socket idle at the moment the kernel takes it over. A read
/// that pulled in the peer's last handshake record *and* the first bytes it sent afterwards would
/// leave those bytes in a userspace buffer that is dropped when the handshake ends, and the kernel
/// has no way to be told about them. The symptom is a connection that completes its TLS handshake
/// and then hangs until the deadline, because the `Hello` that follows it was silently eaten.
///
/// # Arguments
///
/// * `header` - The five header bytes of a record
#[inline]
pub fn record_body_len(header: &[u8; RECORD_HEADER_LEN]) -> Result<usize, TlsError> {
    // the length is a big endian u16 at offsets 3..5, in every version of the protocol
    let len = u16::from_be_bytes([header[3], header[4]]) as usize;
    // judge it before it is used as an allocation size
    if len > MAX_RECORD_LEN {
        return Err(TlsError::RecordTooLarge(len));
    }
    Ok(len)
}

/// How large a buffer a handshake flight is encoded into before it is written
///
/// A TLS 1.3 flight carrying a certificate chain is a few kilobytes. This is generous rather than
/// tuned, since it is allocated once per connection and freed as soon as the handshake ends.
const HANDSHAKE_BUFFER: usize = 16 * 1024;

/// The things that can go wrong taking the wire
#[derive(Debug)]
pub enum TlsError {
    /// A certificate or key file could not be read
    Io {
        /// What was being read
        path: PathBuf,
        /// Why it could not be
        source: std::io::Error,
    },
    /// A certificate file held no certificates
    NoCertificates(PathBuf),
    /// A key file held no private key
    NoPrivateKey(PathBuf),
    /// A configuration rustls would not accept
    Config(rustls::Error),
    /// The name this client is asking for is not a name a certificate can carry
    InvalidServerName(String),
    /// The handshake itself failed
    Handshake(rustls::Error),
    /// The peer went away in the middle of the handshake
    Closed,
    /// The peer named a record larger than TLS allows
    RecordTooLarge(usize),
    /// The kernel would not attach its TLS module to this socket
    ///
    /// Almost always because `modprobe tls` has never run on this machine — `setsockopt` does not
    /// autoload it, so this is the error a first deployment sees.
    UlpUnavailable(std::io::Error),
    /// The kernel refused the keys for one direction of this connection
    KeysRefused {
        /// Whether this was the transmit direction
        transmit: bool,
        /// What the kernel said
        source: std::io::Error,
    },
    /// The session negotiated a cipher the kernel cannot be given keys for
    UnsupportedCipher,
    /// A key was not the length its cipher calls for
    KeyLength {
        /// How long it should have been
        want: usize,
        /// How long it was
        got: usize,
    },
}

impl std::fmt::Display for TlsError {
    /// Write a legible description of this TLS error
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io { path, source } => {
                write!(f, "failed to read {}: {source}", path.display())
            }
            TlsError::NoCertificates(path) => {
                write!(f, "{} held no certificates", path.display())
            }
            TlsError::NoPrivateKey(path) => {
                write!(f, "{} held no private key", path.display())
            }
            TlsError::Config(error) => write!(f, "this TLS configuration was refused: {error}"),
            TlsError::InvalidServerName(name) => {
                write!(f, "'{name}' is not a valid server name")
            }
            TlsError::Handshake(error) => write!(f, "the TLS handshake failed: {error}"),
            TlsError::Closed => write!(f, "the peer closed the connection during the TLS handshake"),
            TlsError::RecordTooLarge(len) => write!(
                f,
                "the peer named a {len} byte TLS record, larger than the {MAX_RECORD_LEN} byte bound"
            ),
            TlsError::UlpUnavailable(source) => write!(
                f,
                "the kernel would not enable TLS on this socket ({source}) - the 'tls' module is \
                 not loaded, and setsockopt does not autoload it. run 'modprobe tls'"
            ),
            TlsError::KeysRefused { transmit, source } => {
                let which = if *transmit { "transmit" } else { "receive" };
                write!(f, "the kernel refused the {which} keys: {source}")
            }
            TlsError::UnsupportedCipher => write!(
                f,
                "this session negotiated a cipher the kernel cannot be given keys for"
            ),
            TlsError::KeyLength { want, got } => {
                write!(f, "expected a {want} byte key and got a {got} byte one")
            }
        }
    }
}

impl std::error::Error for TlsError {}

impl From<rustls::Error> for TlsError {
    /// Treat a bare rustls error as a handshake failure
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: rustls::Error) -> Self {
        TlsError::Handshake(error)
    }
}

/// Where a server's certificate and key are read from
///
/// This is the shape the `networking.tls` config section deserializes into, and it is here rather
/// than in `server::conf` so that the client's own options can be described beside it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsServerOptions {
    /// The PEM file holding this server's certificate chain, leaf first
    pub cert: PathBuf,
    /// The PEM file holding the private key for that chain
    pub key: PathBuf,
}

/// What a client needs to recognise the server it is calling
///
/// There is deliberately no root store here beyond `ca`. A datacenter store talks to a private CA,
/// so a bundle of public roots would be a dependency for a case that does not arise — and a client
/// that trusted the public roots as well would accept any certificate on the internet for the name
/// it asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsClientOptions {
    /// The PEM file holding the certificate authority that signed the server's certificate
    pub ca: PathBuf,
    /// The name to ask for, if it is not the address being connected to
    pub server_name: Option<String>,
}

impl TlsClientOptions {
    /// Build client options that trust one certificate authority
    ///
    /// # Arguments
    ///
    /// * `ca` - The PEM file holding the authority that signed the server's certificate
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_proto::shared::tls::TlsClientOptions;
    ///
    /// let tls = TlsClientOptions::new("/etc/shoal/ca.pem").server_name("shoal.internal");
    /// ```
    pub fn new<P: Into<PathBuf>>(ca: P) -> Self {
        TlsClientOptions {
            ca: ca.into(),
            server_name: None,
        }
    }

    /// Ask for a name other than the address being connected to
    ///
    /// # Arguments
    ///
    /// * `server_name` - The name the server's certificate should carry
    pub fn server_name<N: Into<String>>(mut self, server_name: N) -> Self {
        self.server_name = Some(server_name.into());
        self
    }
}

/// The provider every config in this module is built on
///
/// `aws-lc-rs` rather than `ring` because it is rustls' own default and because its AES-GCM
/// assembly is what a userspace fallback would want. Under kTLS this decides handshake cost and
/// nothing else, since the kernel does the bulk work.
fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Read a certificate chain out of a PEM file
///
/// # Arguments
///
/// * `path` - The PEM file to read
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    // read the whole file, since a certificate chain is kilobytes and this happens once
    let pem = std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let certs = rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    // a file that parsed but held nothing is a misconfiguration rather than an empty chain
    if certs.is_empty() {
        return Err(TlsError::NoCertificates(path.to_path_buf()));
    }
    Ok(certs)
}

/// Read a private key out of a PEM file
///
/// # Arguments
///
/// * `path` - The PEM file to read
pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    // the same read as above, and the same reason
    let pem = std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|source| TlsError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| TlsError::NoPrivateKey(path.to_path_buf()))
}

/// Build the config a server accepts connections with
///
/// # Invariants
///
/// **`enable_secret_extraction` is on.** Without it the keys cannot be taken out of the session at
/// all and there is nothing to give the kernel.
///
/// **`send_tls13_tickets` is zero, and has to stay zero.** A TLS 1.3 server sends
/// `NewSessionTicket` *after* the handshake completes. On a socket the kernel has taken over that
/// is a non application record, and a plain `read` cannot return one — it fails the read with
/// `EIO`. Turning tickets back on would break every connection this server accepts, some
/// milliseconds after each one appeared to succeed.
///
/// The cost is session resumption, which is the first of the three mitigations
/// [D3](../../../docs/src/direction/authentication.md) proposes for the fifty handshakes a client
/// pool opens. kTLS and that mitigation are not compatible in this form.
///
/// # Arguments
///
/// * `options` - Where this server's certificate and key are
pub fn server_config(options: &TlsServerOptions) -> Result<Arc<ServerConfig>, TlsError> {
    // load what this server proves itself with before building anything around it
    let certs = load_certs(&options.cert)?;
    let key = load_key(&options.key)?;
    let mut config = ServerConfig::builder_with_provider(suite_limited_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsError::Config)?;
    // the keys have to be reachable, or there is nothing to hand the kernel
    config.enable_secret_extraction = true;
    // see this function's invariants - a ticket after the handshake breaks every read
    config.send_tls13_tickets = 0;
    Ok(Arc::new(config))
}

/// Build the config a client connects with
///
/// # Arguments
///
/// * `options` - Which authority this client trusts, and what name it asks for
pub fn client_config(options: &TlsClientOptions) -> Result<Arc<ClientConfig>, TlsError> {
    // trust exactly the authority this deployment named and nothing else
    let mut roots = rustls::RootCertStore::empty();
    for cert in load_certs(&options.ca)? {
        roots.add(cert).map_err(TlsError::Config)?;
    }
    let mut config = ClientConfig::builder_with_provider(suite_limited_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    // the client's own keys have to be reachable for the same reason the server's do
    config.enable_secret_extraction = true;
    Ok(Arc::new(config))
}

/// Build a client config that trusts a verifier of the caller's choosing
///
/// This exists for tests and for the benchmark harness, which generate a certificate per run and
/// have nothing to check it against. It is not reachable from any configuration file.
///
/// # Arguments
///
/// * `verifier` - What to check the server's certificate with
pub fn client_config_with_verifier(
    verifier: Arc<dyn ServerCertVerifier>,
) -> Result<Arc<ClientConfig>, TlsError> {
    // the same config as above with its trust decision replaced
    let mut config = ClientConfig::builder_with_provider(suite_limited_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    config.enable_secret_extraction = true;
    Ok(Arc::new(config))
}

/// The crypto provider, cut down to the suites the kernel can be given keys for
fn suite_limited_provider() -> Arc<CryptoProvider> {
    // start from the default and replace its suite list, so every other decision stays upstream's
    let mut provider = (*provider()).clone();
    provider.cipher_suites = TLS_CIPHER_SUITES.to_vec();
    Arc::new(provider)
}

/// Turn an address or a configured name into the name a certificate is checked against
///
/// # Arguments
///
/// * `options` - The client's TLS options, which may name a server explicitly
/// * `addr` - The address being connected to, used when they do not
pub fn server_name(
    options: &TlsClientOptions,
    addr: &std::net::SocketAddr,
) -> Result<ServerName<'static>, TlsError> {
    // an explicit name wins, since an address is only a stand in for one
    match &options.server_name {
        Some(name) => ServerName::try_from(name.clone())
            .map_err(|_| TlsError::InvalidServerName(name.clone())),
        None => Ok(ServerName::from(addr.ip())),
    }
}

/// What a finished handshake leaves behind
///
/// Both halves are kept. The secrets are what the kernel is given; the [`KernelConnection`] is what
/// a key update would be handled through, and it is held for the life of the connection rather
/// than dropped so that handling one later is a call site rather than a redesign.
///
/// [`KernelConnection`]: rustls::kernel::KernelConnection
pub struct Established<Data> {
    /// The keys and sequence numbers to give the kernel
    pub secrets: ExtractedSecrets,
    /// What rustls keeps of the session once the kernel owns the record layer
    pub kernel: rustls::kernel::KernelConnection<Data>,
}

/// What a handshake wants its caller to do next
pub enum TlsStep {
    /// Write what [`take_outgoing`] returns to the peer, then step again
    ///
    /// [`take_outgoing`]: TlsHandshake::take_outgoing
    Transmit,
    /// Read more bytes off the peer, [`feed`] them, then step again
    ///
    /// [`feed`]: TlsHandshake::feed
    NeedRead,
    /// The handshake is finished and the kernel can take the socket
    Done,
}

/// The two rustls connections this module can drive
///
/// This exists so that [`TlsHandshake`] can be one type rather than two near-identical ones.
/// rustls defines `process_tls_records` on two separate inherent impls, one per connection data
/// type, so a function generic over that type cannot reach either — a trait with one method per
/// side is what bridges them.
///
/// It is not meant to be implemented outside this module, and there is nothing else it would be
/// correct to implement it for.
pub trait Handshaker: Sized {
    /// Which side of a connection this is, in rustls' terms
    type Data;

    /// Hand this connection whatever ciphertext has arrived
    ///
    /// # Arguments
    ///
    /// * `incoming` - The ciphertext read off the socket so far
    fn process<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data>;

    /// Give up this connection's session in favour of the kernel's record layer
    fn into_kernel(
        self,
    ) -> Result<(ExtractedSecrets, KernelConnection<Self::Data>), rustls::Error>;
}

impl Handshaker for UnbufferedClientConnection {
    type Data = ClientConnectionData;

    /// Hand this client connection whatever ciphertext has arrived
    ///
    /// # Arguments
    ///
    /// * `incoming` - The ciphertext read off the socket so far
    fn process<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        // this lives on `UnbufferedConnectionCommon`, which the connection derefs to
        (**self).process_tls_records(incoming)
    }

    /// Give up this session in favour of the kernel's record layer
    ///
    /// `dangerous_into_kernel_connection` rather than `dangerous_extract_secrets`, which is
    /// deprecated precisely because it drops key update and session ticket handling.
    fn into_kernel(
        self,
    ) -> Result<(ExtractedSecrets, KernelConnection<Self::Data>), rustls::Error> {
        self.dangerous_into_kernel_connection()
    }
}

impl Handshaker for UnbufferedServerConnection {
    type Data = ServerConnectionData;

    /// Hand this server connection whatever ciphertext has arrived
    ///
    /// # Arguments
    ///
    /// * `incoming` - The ciphertext read off the socket so far
    fn process<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        // this lives on `UnbufferedConnectionCommon`, which the connection derefs to
        (**self).process_tls_records(incoming)
    }

    /// Give up this session in favour of the kernel's record layer
    fn into_kernel(
        self,
    ) -> Result<(ExtractedSecrets, KernelConnection<Self::Data>), rustls::Error> {
        self.dangerous_into_kernel_connection()
    }
}

/// The state a handshake in progress carries, whichever side of it this is
pub struct TlsHandshake<C: Handshaker> {
    /// The rustls connection being driven
    conn: C,
    /// Ciphertext that has arrived and not been consumed yet
    incoming: Vec<u8>,
    /// Ciphertext that has been encoded and not written yet
    outgoing: Vec<u8>,
}

impl<C: Handshaker> TlsHandshake<C> {
    /// Wrap a connection that has not started its handshake
    ///
    /// # Arguments
    ///
    /// * `conn` - The rustls connection to drive
    fn new(conn: C) -> Self {
        TlsHandshake {
            conn,
            incoming: Vec::with_capacity(HANDSHAKE_BUFFER),
            outgoing: Vec::with_capacity(HANDSHAKE_BUFFER),
        }
    }

    /// Give this handshake bytes that arrived from the peer
    ///
    /// # Arguments
    ///
    /// * `bytes` - What was read off the socket
    pub fn feed(&mut self, bytes: &[u8]) {
        // append rather than replace, since a flight can span several reads
        self.incoming.extend_from_slice(bytes);
    }

    /// Take the bytes this handshake wants written to the peer
    ///
    /// The buffer is emptied, so a caller that takes and then fails to write has dropped a flight
    /// and the handshake will stall rather than repeat it.
    pub fn take_outgoing(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.outgoing)
    }

    /// Whether this handshake has anything waiting to be written
    pub fn has_outgoing(&self) -> bool {
        !self.outgoing.is_empty()
    }

    /// Advance this handshake as far as the bytes it holds allow
    ///
    /// # Invariants
    ///
    /// **Every rustls state falls through to the discard at the bottom of the loop.** rustls
    /// requires the bytes it reported consumed to be removed from the front of the incoming buffer
    /// before the next call, so an early return from inside the match would leave them there and
    /// the handshake would re-read the same flight forever.
    pub fn step(&mut self) -> Result<TlsStep, TlsError> {
        // destructured so the borrow checker can see that the three buffers are disjoint
        let TlsHandshake {
            conn,
            incoming,
            outgoing,
        } = self;
        loop {
            // hand rustls whatever ciphertext has arrived and see what it wants next
            let UnbufferedStatus {
                discard,
                state: status,
            } = conn.process(incoming);
            let turn = match status.map_err(TlsError::Handshake)? {
                // a flight is ready, so encode it and go round again - the transmit state that
                // follows is what tells the caller to write it
                ConnectionState::EncodeTlsData(mut encoder) => {
                    encode_flight(outgoing, &mut encoder)?;
                    Turn::Again
                }
                // what was encoded has to reach the peer before anything else happens
                ConnectionState::TransmitTlsData(data) => {
                    data.done();
                    Turn::Step(TlsStep::Transmit)
                }
                // rustls needs more from the peer than it has been given
                ConnectionState::BlockedHandshake => Turn::Step(TlsStep::NeedRead),
                // the handshake is over and the kernel can have the socket
                ConnectionState::WriteTraffic(_) => Turn::Step(TlsStep::Done),
                // a peer that closed mid handshake is not one this connection can be had with
                ConnectionState::Closed | ConnectionState::PeerClosed => Turn::Closed,
                // neither side offers early data, so reaching it means a config moved under this
                _ => {
                    return Err(TlsError::Handshake(rustls::Error::General(
                        "the TLS handshake reached a state this build does not drive".to_owned(),
                    )))
                }
            };
            // consume what rustls read, which it requires before the next call
            if discard > 0 {
                incoming.drain(..discard);
            }
            match turn {
                Turn::Again => {}
                Turn::Step(step) => return Ok(step),
                Turn::Closed => return Err(TlsError::Closed),
            }
        }
    }

    /// Take what this handshake negotiated, once it says it is done
    pub fn finish(self) -> Result<Established<C::Data>, TlsError> {
        // this is where rustls stops being in the data path and the kernel starts
        let (secrets, kernel) = self.conn.into_kernel().map_err(TlsError::Handshake)?;
        Ok(Established { secrets, kernel })
    }
}

/// One side of a handshake, from the client
pub type TlsClientHandshake = TlsHandshake<UnbufferedClientConnection>;

/// One side of a handshake, from the server
pub type TlsServerHandshake = TlsHandshake<UnbufferedServerConnection>;

impl TlsClientHandshake {
    /// Start a handshake against a server
    ///
    /// # Arguments
    ///
    /// * `config` - What this client trusts and will negotiate
    /// * `name` - The name to ask that server for
    pub fn client(config: Arc<ClientConfig>, name: ServerName<'static>) -> Result<Self, TlsError> {
        // the unbuffered shape, because this has to be driven over two different async runtimes
        let conn =
            UnbufferedClientConnection::new(config, name).map_err(TlsError::Handshake)?;
        Ok(TlsHandshake::new(conn))
    }
}

impl TlsServerHandshake {
    /// Start a handshake against a client
    ///
    /// # Arguments
    ///
    /// * `config` - What this server proves itself with and will negotiate
    pub fn server(config: Arc<ServerConfig>) -> Result<Self, TlsError> {
        // the unbuffered shape, for the reason the client's constructor gives
        let conn = UnbufferedServerConnection::new(config).map_err(TlsError::Handshake)?;
        Ok(TlsHandshake::new(conn))
    }
}

/// What one turn of the loop below decided
///
/// This exists so that every rustls state is handled by an arm that falls through to the discard
/// underneath it. An early `return` from inside the match would skip that discard, and rustls
/// requires it to have happened before the next call — the failure mode is a handshake that
/// re-reads the same flight forever.
enum Turn {
    /// rustls is ready to be asked again immediately, with nothing for the caller to do
    Again,
    /// hand this step back to the caller
    Step(TlsStep),
    /// the peer closed in the middle of the handshake
    Closed,
}

/// Encode one handshake flight onto the end of the outgoing buffer
///
/// # Arguments
///
/// * `outgoing` - The buffer to append this flight to
/// * `encoder` - What rustls wants encoded
fn encode_flight<Data>(
    outgoing: &mut Vec<u8>,
    encoder: &mut rustls::unbuffered::EncodeTlsData<'_, Data>,
) -> Result<(), TlsError> {
    // append rather than overwrite, since several flights can be encoded before one is written
    let at = outgoing.len();
    outgoing.resize(at + HANDSHAKE_BUFFER, 0);
    let written = match encoder.encode(&mut outgoing[at..]) {
        Ok(written) => written,
        // the guess above was too small, so grow to exactly what was asked for and encode again
        Err(EncodeError::InsufficientSize(needed)) => {
            outgoing.resize(at + needed.required_size, 0);
            encoder.encode(&mut outgoing[at..]).map_err(encode_failed)?
        }
        Err(error) => return Err(encode_failed(error)),
    };
    outgoing.truncate(at + written);
    Ok(())
}

/// Describe a flight that could not be encoded
///
/// # Arguments
///
/// * `error` - What rustls said about it
fn encode_failed(error: EncodeError) -> TlsError {
    TlsError::Handshake(rustls::Error::General(format!(
        "could not encode a handshake flight: {error:?}"
    )))
}
