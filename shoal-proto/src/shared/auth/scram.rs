//! SCRAM-SHA-256, as RFC 5802 defines it and RFC 7677 names the hash for
//!
//! ```text
//!  client                                                          server
//!    │  Auth          n,,n=<user>,r=<cnonce>                          │
//!    ├───────────────────────────────────────────────────────────────►│
//!    │  AuthResponse  Challenge  r=<cnonce||snonce>,s=<salt>,i=<iters> │
//!    │◄───────────────────────────────────────────────────────────────┤
//!    │  Auth          c=biws,r=<cnonce||snonce>,p=<proof>              │
//!    ├───────────────────────────────────────────────────────────────►│
//!    │  AuthResponse  Success    v=<server signature>                  │
//!    │◄───────────────────────────────────────────────────────────────┤
//! ```
//!
//! # Invariants
//!
//! **The messages are RFC 5802's text, byte for byte.** They are not a bespoke binary encoding of
//! the same construction, even though both peers are built from this repository and nothing else
//! speaks this protocol. The proof is computed over the concatenation of the message strings, so
//! the encoding *is* part of the cryptography — changing it changes what is signed, and matching
//! the RFC is what lets the RFC's own test vector be a test here.
//!
//! **The client verifies the server too.** SCRAM is mutual, and the final `v=` is not decoration:
//! a peer that captured a previous exchange can produce a plausible challenge but cannot produce
//! this signature without the credential. A client that skipped this check would authenticate
//! itself to anything that answered.
//!
//! **`Hi` is PBKDF2-HMAC-SHA-256 with the credential's own iteration count**, taken from the
//! server's challenge rather than from this build's default, so that raising the default does not
//! invalidate credentials derived under the old one.
//!
//! **No SASLprep.** RFC 5802 says to normalize the password with SASLprep before deriving from it
//! and this does not, so a password whose Unicode normalization differs between two clients would
//! derive different keys on each. Written down in the feature page as a limitation rather than
//! discovered: it costs a dependency, and every password this database has ever been given is
//! ASCII, where SASLprep is the identity function.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::{AuthError, CredentialStore, Principal, StoredCredential, DEFAULT_ITERATIONS, SALT_LEN};
use crate::shared::protocol::auth::AuthMechanism;

/// The number of random bytes a nonce is drawn from
///
/// Base64 of eighteen bytes is twenty four characters with no padding, which is comfortably past
/// the RFC's advice and lands in the printable, comma free alphabet the grammar requires without
/// any further filtering.
const NONCE_BYTES: usize = 18;

/// The length of a SHA-256 digest in bytes
const KEY_LEN: usize = 32;

/// The gs2 header this implementation sends, which says it is not using channel binding
///
/// Channel binding needs a TLS layer to bind *to*, so it is `n` — "the client does not support
/// it" — rather than `y`. Saying `y` without support is the downgrade `y` exists to detect.
const GS2_HEADER: &str = "n,,";

/// The base64 of [`GS2_HEADER`], which is what a client final message repeats back in `c=`
const GS2_HEADER_B64: &str = "biws";

/// A HMAC-SHA-256, which every derivation here is built out of
type HmacSha256 = Hmac<Sha256>;

/// Fill a buffer with cryptographically random bytes
///
/// # Arguments
///
/// * `buf` - The buffer to fill
pub fn fill_random(buf: &mut [u8]) {
    // draw from the operating system rather than a seeded generator, since these are secrets
    OsRng.fill_bytes(buf);
}

/// Compute a HMAC-SHA-256
///
/// # Arguments
///
/// * `key` - The key to sign with
/// * `msg` - The message to sign
fn hmac(key: &[u8], msg: &[u8]) -> [u8; KEY_LEN] {
    // a hmac accepts a key of any length, so this cannot fail whatever we are given
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts keys of any length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Compute a SHA-256 digest
///
/// # Arguments
///
/// * `msg` - The message to digest
fn sha256(msg: &[u8]) -> [u8; KEY_LEN] {
    Sha256::digest(msg).into()
}

/// Derive the salted password RFC 5802 calls `Hi`
///
/// # Arguments
///
/// * `password` - The password to derive from
/// * `salt` - The salt to derive with
/// * `iterations` - The number of PBKDF2 rounds to run
fn salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; KEY_LEN] {
    // Hi() is PBKDF2-HMAC-SHA-256 with a one block output
    let mut out = [0u8; KEY_LEN];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut out);
    out
}

/// Exclusive-or two keys of the same length into a new one
///
/// # Arguments
///
/// * `left` - The first key
/// * `right` - The second key
fn xor(left: &[u8; KEY_LEN], right: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
    // walk both keys together, since they are the same fixed length
    let mut out = [0u8; KEY_LEN];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = left[index] ^ right[index];
    }
    out
}

/// Draw a fresh nonce
fn nonce() -> String {
    // draw the raw bytes, then spell them in an alphabet the grammar allows
    let mut raw = [0u8; NONCE_BYTES];
    fill_random(&mut raw);
    STANDARD.encode(raw)
}

/// Escape a username the way RFC 5802's `saslname` requires
///
/// A comma would end the field and an equals would start an escape, so both are spelled out. This
/// is not cosmetic: without it a username containing a comma could inject a second field into the
/// message the proof is computed over.
///
/// # Arguments
///
/// * `username` - The username to escape
fn escape_username(username: &str) -> String {
    // the equals has to be replaced first, or it would escape the escapes the comma introduces
    username.replace('=', "=3D").replace(',', "=2C")
}

/// Unescape a username the way RFC 5802's `saslname` requires
///
/// # Arguments
///
/// * `username` - The escaped username to read
fn unescape_username(username: &str) -> Result<String, AuthError> {
    // walk the string looking for the two escapes, since anything else after an equals is invalid
    let mut out = String::with_capacity(username.len());
    let mut rest = username;
    while let Some(at) = rest.find('=') {
        // everything before the escape is literal
        out.push_str(&rest[..at]);
        // an escape is exactly three bytes, and only two of them are legal
        match rest.get(at..at + 3) {
            Some("=3D") => out.push('='),
            Some("=2C") => out.push(','),
            _ => return Err(AuthError::Malformed("invalid escape in username")),
        }
        rest = &rest[at + 3..];
    }
    // everything after the last escape is literal too
    out.push_str(rest);
    Ok(out)
}

/// Pull the value of a single letter attribute out of a comma separated message
///
/// # Arguments
///
/// * `msg` - The message to read
/// * `key` - The attribute letter to find
fn attribute<'a>(msg: &'a str, key: char) -> Option<&'a str> {
    // find the field that opens with this letter and an equals, then take everything after it
    msg.split(',')
        .find(|field| field.starts_with(key) && field.as_bytes().get(1) == Some(&b'='))
        .map(|field| &field[2..])
}

/// Build a credential for a user that does not exist
///
/// The salt is derived from the username so that probing the same name twice gets the same
/// challenge, the way a real user would, and from a per process key so that it cannot be computed
/// by anyone who has not already got into the server. The two keys are drawn from the same
/// derivation and correspond to no password at all, so the proof check fails exactly where it
/// would for a wrong password.
///
/// # Arguments
///
/// * `fake_salt_key` - The per process key to derive this decoy from
/// * `username` - The name that was asked for
pub fn decoy_credential(fake_salt_key: &[u8; 32], username: &str) -> StoredCredential {
    // derive a salt that is stable per name and unguessable off this machine
    let salt = hmac(fake_salt_key, username.as_bytes());
    // derive two keys from the same place, which no password will ever reproduce
    let stored_key = hmac(fake_salt_key, b"decoy stored key");
    let server_key = hmac(fake_salt_key, b"decoy server key");
    StoredCredential {
        salt: salt[..SALT_LEN].to_vec(),
        iterations: DEFAULT_ITERATIONS,
        stored_key: stored_key.to_vec(),
        server_key: server_key.to_vec(),
    }
}

impl StoredCredential {
    /// Derive a stored credential from a password
    ///
    /// This is the only place a password is ever seen by the server side of this module, and it is
    /// used at startup by a config that named one and by whatever generates a credential to put in
    /// a config file. What it returns cannot be turned back into the password.
    ///
    /// # Arguments
    ///
    /// * `password` - The password to derive from
    /// * `iterations` - The number of PBKDF2 rounds to derive with
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_proto::shared::auth::{StoredCredential, DEFAULT_ITERATIONS};
    ///
    /// let stored = StoredCredential::from_password("hunter2", DEFAULT_ITERATIONS);
    /// assert_eq!(stored.iterations, DEFAULT_ITERATIONS);
    /// ```
    pub fn from_password(password: &str, iterations: u32) -> Self {
        // draw a fresh salt, since two users with the same password must not derive the same keys
        let mut salt = [0u8; SALT_LEN];
        fill_random(&mut salt);
        StoredCredential::from_password_and_salt(password, &salt, iterations)
    }

    /// Derive a stored credential from a password and a salt that has already been chosen
    ///
    /// Split out from [`StoredCredential::from_password`] so that a test can pin the whole
    /// derivation against a known vector, which is impossible while the salt is drawn inside.
    ///
    /// # Arguments
    ///
    /// * `password` - The password to derive from
    /// * `salt` - The salt to derive with
    /// * `iterations` - The number of PBKDF2 rounds to derive with
    pub fn from_password_and_salt(password: &str, salt: &[u8], iterations: u32) -> Self {
        // derive the salted password once, since both keys come out of it
        let salted = salted_password(password, salt, iterations);
        // the client key is hashed before it is stored, so this file cannot be replayed as a proof
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac(&salted, b"Server Key");
        StoredCredential {
            salt: salt.to_vec(),
            iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        }
    }
}

/// What a client wants done with a server's challenge
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStep {
    /// Write these bytes back in an `Auth` frame
    Send(Vec<u8>),
}

/// What a server wants done with a client's message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerStep {
    /// Answer with these bytes and wait for another `Auth` frame
    Challenge(Vec<u8>),
    /// This peer is who it says it is, and these bytes prove this server is too
    Success {
        /// The final payload, which the client checks this server with
        payload: Vec<u8>,
        /// Who this connection now belongs to
        principal: Principal,
    },
}

/// The client half of a SCRAM-SHA-256 exchange
///
/// # Invariants
///
/// **`first` is called exactly once and before `step`.** The nonce and the bare first message it
/// produces are both signed later, so a second call would leave the two halves of the exchange
/// computing the proof over different bytes.
pub struct ScramClient {
    /// The name being authenticated
    username: String,
    /// The password to prove it with
    password: String,
    /// The nonce this client drew
    client_nonce: String,
    /// The first message this client sent, without its gs2 header
    first_bare: String,
    /// The signature this client expects the server to answer with, once it knows it
    expected_server_signature: Option<[u8; KEY_LEN]>,
}

impl ScramClient {
    /// Start a SCRAM exchange as a client
    ///
    /// # Arguments
    ///
    /// * `username` - The name to authenticate as
    /// * `password` - The password to prove it with
    pub fn new(username: &str, password: &str) -> Self {
        ScramClient {
            username: username.to_owned(),
            password: password.to_owned(),
            client_nonce: nonce(),
            first_bare: String::new(),
            expected_server_signature: None,
        }
    }

    /// Start a SCRAM exchange with a nonce that was chosen rather than drawn
    ///
    /// This exists so that RFC 7677's test vector can be driven end to end. The vector fixes both
    /// nonces, and a client that draws its own can only ever be checked against itself — which
    /// would pin this implementation to its own bugs rather than to the standard.
    ///
    /// # Arguments
    ///
    /// * `username` - The name to authenticate as
    /// * `password` - The password to prove it with
    /// * `client_nonce` - The nonce to use instead of drawing one
    #[cfg(test)]
    pub(crate) fn with_nonce(username: &str, password: &str, client_nonce: &str) -> Self {
        ScramClient {
            username: username.to_owned(),
            password: password.to_owned(),
            client_nonce: client_nonce.to_owned(),
            first_bare: String::new(),
            expected_server_signature: None,
        }
    }

    /// Build the first message, which opens the exchange
    ///
    /// # Errors
    ///
    /// This is infallible today and returns a `Result` so that a mechanism whose first message
    /// depends on something that can fail — a certificate that will not load, for mutual TLS —
    /// fits the same call site.
    pub fn first(&mut self) -> Result<Vec<u8>, AuthError> {
        // the bare message is everything after the gs2 header, and it is what gets signed later
        self.first_bare = format!(
            "n={},r={}",
            escape_username(&self.username),
            self.client_nonce
        );
        Ok(format!("{GS2_HEADER}{}", self.first_bare).into_bytes())
    }

    /// Answer a server's challenge
    ///
    /// # Arguments
    ///
    /// * `input` - The server's challenge payload
    pub fn step(&mut self, input: &[u8]) -> Result<ClientStep, AuthError> {
        // a challenge before we have sent anything is a server that is out of step with us
        if self.first_bare.is_empty() {
            return Err(AuthError::OutOfOrder);
        }
        // the challenge is text, and a challenge that is not is not one we can read
        let server_first =
            std::str::from_utf8(input).map_err(|_| AuthError::Malformed("challenge is not utf-8"))?;
        // pull the three fields a server first message has to carry
        let combined_nonce =
            attribute(server_first, 'r').ok_or(AuthError::Malformed("challenge has no nonce"))?;
        let salt_b64 =
            attribute(server_first, 's').ok_or(AuthError::Malformed("challenge has no salt"))?;
        let iterations = attribute(server_first, 'i')
            .ok_or(AuthError::Malformed("challenge has no iteration count"))?
            .parse::<u32>()
            .map_err(|_| AuthError::Malformed("iteration count is not a number"))?;
        // the server has to have extended *our* nonce rather than replaced it
        //
        // this is the check that stops a peer replaying a challenge it captured from someone else,
        // since our nonce is fresh and a captured one cannot contain it
        if !combined_nonce.starts_with(&self.client_nonce) || combined_nonce == self.client_nonce {
            return Err(AuthError::NonceMismatch);
        }
        // an iteration count of zero would make the derivation free, so it is not a count we run
        if iterations == 0 {
            return Err(AuthError::Malformed("iteration count is zero"));
        }
        let salt = STANDARD
            .decode(salt_b64)
            .map_err(|_| AuthError::Malformed("salt is not base64"))?;
        // derive everything this exchange needs from the password
        let salted = salted_password(&self.password, &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac(&salted, b"Server Key");
        // the message the proof is computed over is all three messages, joined
        let final_without_proof = format!("c={GS2_HEADER_B64},r={combined_nonce}");
        let auth_message = format!("{},{server_first},{final_without_proof}", self.first_bare);
        // the proof is the client key masked by a signature only the real server can reproduce
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof = xor(&client_key, &client_signature);
        // remember what the server will have to answer with, so `finish` can check it
        self.expected_server_signature = Some(hmac(&server_key, auth_message.as_bytes()));
        Ok(ClientStep::Send(
            format!("{final_without_proof},p={}", STANDARD.encode(proof)).into_bytes(),
        ))
    }

    /// Check the payload a server sent with its success
    ///
    /// This is the half of SCRAM that authenticates the *server*, and skipping it would leave this
    /// client proving itself to anything that answered the port.
    ///
    /// # Arguments
    ///
    /// * `input` - The server's final payload
    pub fn finish(&mut self, input: &[u8]) -> Result<(), AuthError> {
        // a success before we sent a proof is a server letting us in without asking
        let expected = self
            .expected_server_signature
            .ok_or(AuthError::OutOfOrder)?;
        let server_final = std::str::from_utf8(input)
            .map_err(|_| AuthError::Malformed("server final message is not utf-8"))?;
        // an `e=` here is a server refusing us in the mechanism's own vocabulary rather than in a
        // failed status, which a peer built from a different SCRAM implementation may well do
        if attribute(server_final, 'e').is_some() {
            return Err(AuthError::BadCredentials);
        }
        let signature = attribute(server_final, 'v')
            .ok_or(AuthError::Malformed("server final message has no signature"))?;
        let signature = STANDARD
            .decode(signature)
            .map_err(|_| AuthError::Malformed("server signature is not base64"))?;
        // compare in constant time, since this is a secret the peer chose the bytes of
        if signature.ct_eq(&expected).unwrap_u8() != 1 {
            return Err(AuthError::ServerNotAuthenticated);
        }
        Ok(())
    }
}

/// Where a server has got to in an exchange
enum ServerState {
    /// Nothing has arrived yet
    Initial,
    /// A challenge has been sent and the proof has not arrived
    Challenged {
        /// The credential this exchange is being checked against, real or decoy
        credential: StoredCredential,
        /// Whether that credential belongs to a user that exists
        known: bool,
        /// The name that was asked for
        username: String,
        /// The nonce both peers are using
        combined_nonce: String,
        /// The two messages so far, which the proof will be computed over
        partial_auth_message: String,
    },
    /// This exchange is over, either way
    Done,
}

/// The server half of a SCRAM-SHA-256 exchange
///
/// # Invariants
///
/// **A failure is returned as an [`AuthError`], never as a `Success` with a name attached.** The
/// only path that constructs a [`Principal`] is the one past the constant time proof check.
pub struct ScramServer<'a> {
    /// The users this server knows
    store: &'a CredentialStore,
    /// Where this exchange has got to
    state: ServerState,
}

impl<'a> ScramServer<'a> {
    /// Start a SCRAM exchange as a server
    ///
    /// # Arguments
    ///
    /// * `store` - The users this server will accept
    pub const fn new(store: &'a CredentialStore) -> Self {
        ScramServer {
            store,
            state: ServerState::Initial,
        }
    }

    /// Take the next message from a client
    ///
    /// # Arguments
    ///
    /// * `input` - The payload of the client's `Auth` frame
    pub fn step(&mut self, input: &[u8]) -> Result<ServerStep, AuthError> {
        // read whichever message this is, by where the exchange has got to
        match std::mem::replace(&mut self.state, ServerState::Done) {
            ServerState::Initial => self.first(input),
            ServerState::Challenged {
                credential,
                known,
                username,
                combined_nonce,
                partial_auth_message,
            } => self.verify(
                input,
                &credential,
                known,
                &username,
                &combined_nonce,
                &partial_auth_message,
            ),
            // a third message on a two message exchange is a peer that is not following it
            ServerState::Done => Err(AuthError::OutOfOrder),
        }
    }

    /// Answer a client's first message with a challenge
    ///
    /// # Arguments
    ///
    /// * `input` - The payload of the client's first `Auth` frame
    fn first(&mut self, input: &[u8]) -> Result<ServerStep, AuthError> {
        let client_first = std::str::from_utf8(input)
            .map_err(|_| AuthError::Malformed("first message is not utf-8"))?;
        // the gs2 header is not part of what gets signed, so it is stripped before anything else
        //
        // only the no channel binding header is accepted: a `y` here is a client that believes the
        // server supports binding, and answering it without binding is the downgrade `y` detects
        let bare = client_first
            .strip_prefix(GS2_HEADER)
            .ok_or(AuthError::Malformed("unsupported channel binding"))?;
        let username = attribute(bare, 'n').ok_or(AuthError::Malformed("no username"))?;
        let username = unescape_username(username)?;
        let client_nonce = attribute(bare, 'r').ok_or(AuthError::Malformed("no client nonce"))?;
        // an empty nonce would let a replayed challenge match, so it is not one we accept
        if client_nonce.is_empty() {
            return Err(AuthError::Malformed("empty client nonce"));
        }
        // look the user up, getting a decoy that fails identically when there is no such user
        let (credential, known) = self.store.lookup(&username);
        // extend the client's nonce with our own, which is what proves this challenge is fresh
        let combined_nonce = format!("{client_nonce}{}", nonce());
        let server_first = format!(
            "r={combined_nonce},s={},i={}",
            STANDARD.encode(&credential.salt),
            credential.iterations
        );
        // keep the two messages so far, since the proof is computed over all three joined
        self.state = ServerState::Challenged {
            credential,
            known,
            username,
            combined_nonce,
            partial_auth_message: format!("{bare},{server_first}"),
        };
        Ok(ServerStep::Challenge(server_first.into_bytes()))
    }

    /// Check a client's proof
    ///
    /// # Arguments
    ///
    /// * `input` - The payload of the client's second `Auth` frame
    /// * `credential` - The credential this exchange is being checked against
    /// * `known` - Whether that credential belongs to a user that exists
    /// * `username` - The name that was asked for
    /// * `combined_nonce` - The nonce both peers are using
    /// * `partial_auth_message` - The first two messages of this exchange, joined
    #[allow(clippy::too_many_arguments)]
    fn verify(
        &mut self,
        input: &[u8],
        credential: &StoredCredential,
        known: bool,
        username: &str,
        combined_nonce: &str,
        partial_auth_message: &str,
    ) -> Result<ServerStep, AuthError> {
        let client_final = std::str::from_utf8(input)
            .map_err(|_| AuthError::Malformed("final message is not utf-8"))?;
        // the proof is not part of what the proof is computed over, so it is cut off first
        let (without_proof, proof) = client_final
            .rsplit_once(",p=")
            .ok_or(AuthError::Malformed("final message has no proof"))?;
        // the client has to repeat the nonce we extended, which is what binds the two messages
        let echoed =
            attribute(without_proof, 'r').ok_or(AuthError::Malformed("final message has no nonce"))?;
        if echoed != combined_nonce {
            return Err(AuthError::NonceMismatch);
        }
        // and it has to repeat the gs2 header it opened with, which is what detects a downgrade
        let binding = attribute(without_proof, 'c')
            .ok_or(AuthError::Malformed("final message has no channel binding"))?;
        if binding != GS2_HEADER_B64 {
            return Err(AuthError::Malformed("channel binding does not match"));
        }
        let proof = STANDARD
            .decode(proof)
            .map_err(|_| AuthError::Malformed("proof is not base64"))?;
        // a proof of the wrong length cannot be xored, and is a wrong proof either way
        let proof: [u8; KEY_LEN] = proof
            .try_into()
            .map_err(|_| AuthError::Malformed("proof is the wrong length"))?;
        // recover the client key by undoing the mask, then check its hash against what we stored
        let auth_message = format!("{partial_auth_message},{without_proof}");
        let stored_key: [u8; KEY_LEN] = credential
            .stored_key
            .as_slice()
            .try_into()
            .map_err(|_| AuthError::Malformed("stored key is the wrong length"))?;
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let client_key = xor(&proof, &client_signature);
        // compare in constant time, since this is the check the whole mechanism rests on
        let matched = sha256(&client_key).ct_eq(&stored_key).unwrap_u8() == 1;
        // a decoy credential can never match, and this second test is what makes that a promise
        // rather than a property of how the decoy happened to be derived
        if !matched || !known {
            return Err(AuthError::BadCredentials);
        }
        // prove this server holds the credential too, which is the half that authenticates us
        let server_signature = hmac(&credential.server_key, auth_message.as_bytes());
        Ok(ServerStep::Success {
            payload: format!("v={}", STANDARD.encode(server_signature)).into_bytes(),
            principal: Principal::new(username, AuthMechanism::ScramSha256),
        })
    }
}
