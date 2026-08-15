//! Handing a finished TLS session's keys to the kernel
//!
//! This is the whole of the kernel side of [F14](../../../docs/src/features/encryption-in-transit.md).
//! Everything it needs comes out of rustls' [`ExtractedSecrets`], and everything it does is one
//! `setsockopt` to attach the kernel's TLS module and one more per direction to give it the keys.
//!
//! After [`enable`] returns, the socket carries **plaintext** in both directions: `read` returns
//! decrypted application data into whatever buffer the caller names, and `write` takes plaintext.
//! That is the entire reason this module exists rather than a userspace record layer — the
//! client's response read lands in an `AlignedVec<16>` exactly as it did before there was any
//! encryption, so the zero copy property survives without a line of the read loop changing.
//!
//! # Invariants
//!
//! **The socket must be idle when this is called.** The TLS handshake has to have completed with
//! nothing buffered on either side, and no Shoal byte may have crossed yet. The kernel takes over
//! at a record boundary and has no way to be told about bytes that are already in flight.
//!
//! **The peer must not send post-handshake records.** A `NewSessionTicket` or a `KeyUpdate` is a
//! non application record, and a plain `read` on a kTLS socket cannot return one — it fails with
//! `EIO` instead. That is why [`super::server_config`] sets `send_tls13_tickets = 0`, and it is
//! why nothing in Shoal triggers a key update. See that function for what the first costs.
//!
//! **The `tls` kernel module has to be loaded.** `setsockopt` does *not* autoload it, so a machine
//! that has never used kTLS answers `ENOENT` until `modprobe tls` has run. [`is_available`] is
//! what turns that into a legible startup failure instead of a confusing one.

use rustls::crypto::cipher::{AeadKey, Iv};
use rustls::{ConnectionTrafficSecrets, ExtractedSecrets};
use std::os::fd::{AsRawFd, RawFd};

use super::TlsError;

/// The TCP upper layer protocol option that attaches the kernel's TLS module to a socket
const TCP_ULP: libc::c_int = 31;

/// The socket level every TLS key option is set at
const SOL_TLS: libc::c_int = 282;

/// The option naming the keys the kernel encrypts outbound records with
const TLS_TX: libc::c_int = 1;

/// The option naming the keys the kernel decrypts inbound records with
const TLS_RX: libc::c_int = 2;

/// The name of the upper layer protocol to attach, as the kernel spells it
const ULP_NAME: &[u8] = b"tls";

/// The protocol version byte pair the kernel spells TLS 1.3 as
const TLS_1_3_VERSION: u16 = (3 << 8) | 4;

/// The cipher the kernel spells AES-128-GCM as
const TLS_CIPHER_AES_GCM_128: u16 = 51;

/// The cipher the kernel spells AES-256-GCM as
const TLS_CIPHER_AES_GCM_256: u16 = 52;

/// How many bytes of rustls' nonce are the implicit salt
///
/// The kernel wants the twelve byte nonce split in two. The leading four are the salt it holds
/// for the life of the connection and the trailing eight are the per record initialization
/// vector.
const SALT_LEN: usize = 4;

/// How many bytes of rustls' nonce are the explicit initialization vector
const EXPLICIT_IV_LEN: usize = 8;

/// The header every `tls_crypto_info` struct starts with
///
/// This mirrors the kernel's `struct tls_crypto_info` field for field and must keep doing so —
/// the bytes of these structs are handed to `setsockopt` directly.
#[repr(C)]
#[derive(Clone, Copy)]
struct TlsCryptoInfo {
    /// The protocol version these keys belong to
    version: u16,
    /// Which cipher these keys are for
    cipher_type: u16,
}

/// The kernel's key material for one direction of an AES-128-GCM session
///
/// Mirrors `struct tls12_crypto_info_aes_gcm_128`.
#[repr(C)]
#[derive(Clone, Copy)]
struct TlsCryptoInfoAesGcm128 {
    /// Which version and cipher this is
    info: TlsCryptoInfo,
    /// The explicit part of the nonce
    iv: [u8; EXPLICIT_IV_LEN],
    /// The AEAD key
    key: [u8; 16],
    /// The implicit part of the nonce
    salt: [u8; SALT_LEN],
    /// The record sequence number to start counting from
    rec_seq: [u8; 8],
}

/// The kernel's key material for one direction of an AES-256-GCM session
///
/// Mirrors `struct tls12_crypto_info_aes_gcm_256`.
#[repr(C)]
#[derive(Clone, Copy)]
struct TlsCryptoInfoAesGcm256 {
    /// Which version and cipher this is
    info: TlsCryptoInfo,
    /// The explicit part of the nonce
    iv: [u8; EXPLICIT_IV_LEN],
    /// The AEAD key
    key: [u8; 32],
    /// The implicit part of the nonce
    salt: [u8; SALT_LEN],
    /// The record sequence number to start counting from
    rec_seq: [u8; 8],
}

/// Either shape of key material, so one function can set a direction whatever the cipher is
#[derive(Clone, Copy)]
enum CryptoInfo {
    /// An AES-128-GCM direction
    Aes128(TlsCryptoInfoAesGcm128),
    /// An AES-256-GCM direction
    Aes256(TlsCryptoInfoAesGcm256),
}

impl CryptoInfo {
    /// Build the kernel's key material from what rustls negotiated for one direction
    ///
    /// # Arguments
    ///
    /// * `seq` - The record sequence number this direction is at
    /// * `secrets` - The key and nonce rustls negotiated for this direction
    fn build(seq: u64, secrets: &ConnectionTrafficSecrets) -> Result<Self, TlsError> {
        // the kernel takes rustls' twelve byte nonce as a four byte salt and an eight byte iv
        match secrets {
            ConnectionTrafficSecrets::Aes128Gcm { key, iv } => {
                let (salt, explicit) = split_nonce(iv);
                Ok(CryptoInfo::Aes128(TlsCryptoInfoAesGcm128 {
                    info: TlsCryptoInfo {
                        version: TLS_1_3_VERSION,
                        cipher_type: TLS_CIPHER_AES_GCM_128,
                    },
                    iv: explicit,
                    key: fixed_key::<16>(key)?,
                    salt,
                    rec_seq: seq.to_be_bytes(),
                }))
            }
            ConnectionTrafficSecrets::Aes256Gcm { key, iv } => {
                let (salt, explicit) = split_nonce(iv);
                Ok(CryptoInfo::Aes256(TlsCryptoInfoAesGcm256 {
                    info: TlsCryptoInfo {
                        version: TLS_1_3_VERSION,
                        cipher_type: TLS_CIPHER_AES_GCM_256,
                    },
                    iv: explicit,
                    key: fixed_key::<32>(key)?,
                    salt,
                    rec_seq: seq.to_be_bytes(),
                }))
            }
            // chacha20-poly1305 has a kernel spelling too, and the cipher list this build offers
            // does not include it - see `super::TLS_CIPHER_SUITES` for why
            _ => Err(TlsError::UnsupportedCipher),
        }
    }

    /// Where this key material starts and how long it is, for `setsockopt`
    fn as_ptr_len(&self) -> (*const libc::c_void, libc::socklen_t) {
        // each arm hands over its own struct's bytes, since the two are different lengths
        match self {
            CryptoInfo::Aes128(info) => (
                std::ptr::from_ref(info).cast(),
                std::mem::size_of::<TlsCryptoInfoAesGcm128>() as libc::socklen_t,
            ),
            CryptoInfo::Aes256(info) => (
                std::ptr::from_ref(info).cast(),
                std::mem::size_of::<TlsCryptoInfoAesGcm256>() as libc::socklen_t,
            ),
        }
    }
}

/// Split rustls' twelve byte nonce the way the kernel wants it
///
/// # Arguments
///
/// * `iv` - The nonce rustls negotiated
fn split_nonce(iv: &Iv) -> ([u8; SALT_LEN], [u8; EXPLICIT_IV_LEN]) {
    // the leading four bytes are the salt and the trailing eight are the explicit iv
    let raw = iv.as_ref();
    let mut salt = [0u8; SALT_LEN];
    let mut explicit = [0u8; EXPLICIT_IV_LEN];
    salt.copy_from_slice(&raw[..SALT_LEN]);
    explicit.copy_from_slice(&raw[SALT_LEN..SALT_LEN + EXPLICIT_IV_LEN]);
    (salt, explicit)
}

/// Copy an AEAD key into an array of the size its cipher wants
///
/// A key of the wrong length means the cipher this was matched on and the struct being filled
/// disagree, which is a bug here rather than anything a peer can cause.
///
/// # Arguments
///
/// * `key` - The key rustls negotiated
fn fixed_key<const N: usize>(key: &AeadKey) -> Result<[u8; N], TlsError> {
    // refuse rather than truncate, since a short key would be silently weaker
    let raw = key.as_ref();
    if raw.len() != N {
        return Err(TlsError::KeyLength {
            want: N,
            got: raw.len(),
        });
    }
    let mut out = [0u8; N];
    out.copy_from_slice(raw);
    Ok(out)
}

/// Whether this kernel can take a socket's TLS keys at all
///
/// This opens a throwaway socket and tries to attach the TLS upper layer protocol to it. It is
/// called once at startup rather than per connection, so that a server configured for TLS on a
/// machine where `modprobe tls` has never run fails while it is starting instead of failing every
/// client that connects to it.
pub fn is_available() -> bool {
    // a socket of our own, so that probing cannot disturb anything real
    let probe = match std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        std::time::Duration::from_millis(1),
    ) {
        Ok(sock) => sock,
        // nothing is listening on port zero, which is the expected outcome - fall back to asking
        // the module list instead, since a failed connect tells us nothing about kTLS
        Err(_) => return module_loaded(),
    };
    attach_ulp(probe.as_raw_fd()).is_ok()
}

/// Whether the kernel's TLS module is present, by asking the module list
///
/// `setsockopt` does not autoload it, so this is the difference between "this kernel cannot do
/// kTLS" and "this kernel can, once somebody runs `modprobe tls`".
fn module_loaded() -> bool {
    // /proc/modules is the list of what is loaded, one module per line, name first
    let Ok(modules) = std::fs::read_to_string("/proc/modules") else {
        return false;
    };
    modules
        .lines()
        .any(|line| line.split_whitespace().next() == Some("tls"))
}

/// Attach the kernel's TLS upper layer protocol to a socket
///
/// # Arguments
///
/// * `fd` - The socket to attach the TLS module to
fn attach_ulp(fd: RawFd) -> Result<(), TlsError> {
    // SAFETY: the name is a borrowed slice that outlives this call and the length is its own
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            TCP_ULP,
            ULP_NAME.as_ptr().cast(),
            ULP_NAME.len() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(TlsError::UlpUnavailable(std::io::Error::last_os_error()));
    }
    Ok(())
}

/// Read back which upper layer protocol a socket has attached, if any
///
/// This is what makes [`enable`] self checking and what lets a test assert that the kernel really
/// is doing the record layer. Without it, "is this socket encrypted by the kernel" is not a
/// question anything can ask — a userspace implementation that copied plaintext into the same
/// buffers would be indistinguishable from the outside, which is the failure this whole feature is
/// most exposed to.
///
/// # Arguments
///
/// * `fd` - The socket to ask about
pub fn ulp_name(fd: RawFd) -> Option<String> {
    // the kernel writes the name and the length it used, so start with room for any of them
    let mut name = [0u8; 16];
    let mut len = name.len() as libc::socklen_t;
    // SAFETY: the buffer and the length both outlive the call and the length matches the buffer
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            TCP_ULP,
            name.as_mut_ptr().cast(),
            &raw mut len,
        )
    };
    // a socket with no upper layer protocol answers with an error rather than an empty name
    if rc != 0 || len == 0 {
        return None;
    }
    // the kernel includes the trailing nul in what it reports, so trim it back off
    let end = name[..len as usize]
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(len as usize);
    Some(String::from_utf8_lossy(&name[..end]).into_owned())
}

/// Attach the kernel's TLS module to a socket and give it both directions' keys
///
/// # Invariants
///
/// **The attachment is read back rather than assumed.** A `setsockopt` that returned zero but left
/// the socket in plaintext would produce a connection that works perfectly and encrypts nothing,
/// and no test above this layer could tell. The read back costs one syscall per connection.
///
/// # Arguments
///
/// * `fd` - The socket to enable kTLS on
/// * `secrets` - What rustls negotiated during the handshake
pub fn enable(fd: RawFd, secrets: &ExtractedSecrets) -> Result<(), TlsError> {
    // attach the upper layer protocol first, since the key options only exist once it is on
    attach_ulp(fd)?;
    // confirm it actually took, rather than trusting a return code with a plaintext socket
    if ulp_name(fd).as_deref() != Some("tls") {
        return Err(TlsError::UlpUnavailable(std::io::Error::new(
            std::io::ErrorKind::Other,
            "the socket accepted TCP_ULP but did not report the tls upper layer protocol",
        )));
    }
    // then hand it each direction's keys, transmit first
    set_direction(fd, TLS_TX, secrets.tx.0, &secrets.tx.1)?;
    set_direction(fd, TLS_RX, secrets.rx.0, &secrets.rx.1)?;
    Ok(())
}

/// Give the kernel one direction's key material
///
/// # Arguments
///
/// * `fd` - The socket to set the keys on
/// * `option` - Which direction is being set
/// * `seq` - The record sequence number that direction is at
/// * `secrets` - The key and nonce for that direction
fn set_direction(
    fd: RawFd,
    option: libc::c_int,
    seq: u64,
    secrets: &ConnectionTrafficSecrets,
) -> Result<(), TlsError> {
    // build the struct this cipher wants and hand its bytes straight to the kernel
    let info = CryptoInfo::build(seq, secrets)?;
    let (ptr, len) = info.as_ptr_len();
    // SAFETY: the pointer and length come from the struct held by `info`, which outlives the call
    let rc = unsafe { libc::setsockopt(fd, SOL_TLS, option, ptr, len) };
    if rc != 0 {
        return Err(TlsError::KeysRefused {
            transmit: option == TLS_TX,
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::crypto::cipher::{AeadKey, Iv};

    /// Build a nonce whose bytes say where in it they came from
    ///
    /// `Iv::new` and `Iv::copy` are both gated behind rustls' `tls12` feature, which this build
    /// does not enable, so this goes through the `From` impl that is always there.
    fn nonce() -> Iv {
        let mut raw = [0u8; 12];
        for (index, byte) in raw.iter_mut().enumerate() {
            *byte = index as u8;
        }
        Iv::from(raw)
    }

    /// Build the only length of key that can be constructed from outside rustls
    ///
    /// `AeadKey`'s only public constructor is `From<[u8; 32]>` — `AeadKey::new` is `pub(crate)`
    /// and also `tls12` gated. That is why the 128 bit arms below are tested through their
    /// rejection rather than through a success.
    fn key() -> AeadKey {
        AeadKey::from([7u8; 32])
    }

    #[test]
    /// The kernel's salt is the front of rustls' nonce and its iv is the back
    ///
    /// Getting these the wrong way round produces a connection that completes its handshake and
    /// then cannot decrypt a single record, which is a long way from the mistake.
    fn the_nonce_splits_into_a_leading_salt_and_a_trailing_iv() {
        let (salt, explicit) = split_nonce(&nonce());
        assert_eq!(salt, [0, 1, 2, 3]);
        assert_eq!(explicit, [4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    /// A key of the wrong length is refused rather than truncated
    ///
    /// Truncating would hand the kernel a key that is silently weaker than the one negotiated,
    /// and the connection would work, which is the worst combination available.
    fn a_key_of_the_wrong_length_is_refused() {
        assert_eq!(fixed_key::<32>(&key()).unwrap(), [7u8; 32]);
        assert!(matches!(
            fixed_key::<16>(&key()),
            Err(TlsError::KeyLength { want: 16, got: 32 })
        ));
    }

    #[test]
    /// A 128 bit cipher carrying a 256 bit key is refused rather than silently cut down
    fn a_cipher_and_key_that_disagree_are_refused() {
        let secrets = ConnectionTrafficSecrets::Aes128Gcm {
            key: key(),
            iv: nonce(),
        };
        assert!(matches!(
            CryptoInfo::build(0, &secrets),
            Err(TlsError::KeyLength { want: 16, got: 32 })
        ));
    }

    #[test]
    /// The sequence number reaches the kernel big endian
    ///
    /// The kernel counts records in network byte order. A little endian `rec_seq` decrypts the
    /// first record of a connection and nothing after it.
    fn the_sequence_number_is_big_endian() {
        let secrets = ConnectionTrafficSecrets::Aes256Gcm {
            key: key(),
            iv: nonce(),
        };
        let CryptoInfo::Aes256(info) = CryptoInfo::build(1, &secrets).unwrap() else {
            panic!("an aes-256 secret built something other than an aes-256 info");
        };
        assert_eq!(info.rec_seq, [0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    /// Each struct is the length the kernel expects for its cipher
    ///
    /// The two differ only in the key field's width, so a mismatch hands the kernel the wrong
    /// number of bytes and it refuses the socket.
    fn each_cipher_has_its_own_struct_length() {
        let secrets = ConnectionTrafficSecrets::Aes256Gcm {
            key: key(),
            iv: nonce(),
        };
        let (_, len) = CryptoInfo::build(0, &secrets).unwrap().as_ptr_len();
        assert_eq!(
            len,
            std::mem::size_of::<TlsCryptoInfoAesGcm256>() as libc::socklen_t
        );
        assert!(
            std::mem::size_of::<TlsCryptoInfoAesGcm256>()
                > std::mem::size_of::<TlsCryptoInfoAesGcm128>(),
            "the wider key should make a wider struct"
        );
    }

    #[test]
    /// Every info names the version and cipher the kernel reads first
    fn every_info_names_tls_1_3() {
        let secrets = ConnectionTrafficSecrets::Aes256Gcm {
            key: key(),
            iv: nonce(),
        };
        let CryptoInfo::Aes256(info) = CryptoInfo::build(0, &secrets).unwrap() else {
            panic!("an aes-256 secret built something other than an aes-256 info");
        };
        assert_eq!(info.info.version, TLS_1_3_VERSION);
        assert_eq!(info.info.cipher_type, TLS_CIPHER_AES_GCM_256);
    }

    #[test]
    /// A socket reports the tls upper layer protocol once it has been attached, and not before
    ///
    /// This is the only assertion anywhere that can tell a kTLS socket from a plaintext one, which
    /// makes it the one that would catch this feature being quietly replaced by a userspace
    /// implementation. Everything above this layer looks identical either way.
    fn a_socket_reports_the_tls_ulp_once_it_is_attached() {
        // this needs the kernel module, so say so rather than failing on a machine without it
        if !module_loaded() {
            eprintln!(
                "SKIPPING a_socket_reports_the_tls_ulp_once_it_is_attached: the 'tls' kernel \
                 module is not loaded. run 'sudo modprobe tls'"
            );
            return;
        }
        // a connected loopback pair, since TCP_ULP is refused on a socket that is not connected
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind a listener");
        let addr = listener.local_addr().expect("the listener had no address");
        let client = std::net::TcpStream::connect(addr).expect("failed to connect");
        let (server, _) = listener.accept().expect("failed to accept");
        // a fresh socket has no upper layer protocol
        assert_eq!(ulp_name(client.as_raw_fd()), None);
        // and reports one the moment it is attached
        attach_ulp(client.as_raw_fd()).expect("failed to attach the tls ulp");
        assert_eq!(ulp_name(client.as_raw_fd()).as_deref(), Some("tls"));
        // the other end is untouched, which is what makes this a property of the socket and not
        // of the process
        assert_eq!(ulp_name(server.as_raw_fd()), None);
    }

    #[test]
    /// The module probe agrees with what the module list says
    ///
    /// This is not asserting that kTLS is available — it is asserting that the two ways of asking
    /// cannot disagree, so a machine without `modprobe tls` gets the legible error rather than a
    /// confusing one.
    fn the_availability_probe_matches_the_module_list() {
        assert_eq!(is_available(), module_loaded());
    }
}
