//! Who a connection belongs to, and how it proved it
//!
//! [`protocol::auth`] carries the two frames an exchange runs on and gives their payloads no
//! meaning. This is where they get one.
//!
//! # Invariants
//!
//! **The server never holds a password.** A [`StoredCredential`] is a salt, an iteration count and
//! two derived keys, and the password cannot be recovered from any of them without the work the
//! iteration count names. That is the whole point of choosing SCRAM over a bearer token, and a
//! config file that carries a password instead is deriving one of these at startup and throwing
//! the password away — not storing it.
//!
//! **An unknown user is indistinguishable from a wrong password.** Both run the full exchange and
//! both fail at the same step with the same message, and an unknown user is answered with a salt
//! derived from its own name so that even the challenge looks like a real one. A store that
//! answered "no such user" early would turn a login form into a directory listing, and one that
//! answered late but differently would do the same thing a stopwatch away.
//!
//! **Every comparison of a secret is constant time.** The proof and the signature are compared
//! with [`subtle`], never with `==`, because a byte-at-a-time comparison of a value the peer
//! chose is a way to guess it one byte at a time.
//!
//! **A mechanism is a pair of state machines, not a trait.** [`ScramClient`] and [`ScramServer`]
//! have the same `first` / `step` / `finish` shape, and the call sites dispatch on
//! [`AuthMechanism`] with one arm each. Mutual TLS becomes a second arm — it is the one mechanism
//! that has nothing to send, because the identity is already established by the time the first
//! frame could be written — so there is no shared behaviour for a trait to hold yet, and inventing
//! one before there are two implementations would be guessing at the shape of the second.
//!
//! [`protocol::auth`]: crate::shared::protocol::auth

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::protocol::auth::{AuthMechanism, AuthMechanisms};

pub mod scram;

#[cfg(test)]
mod tests;

/// The number of PBKDF2 iterations a credential is derived with unless it says otherwise
///
/// This is the floor RFC 7677 names and the number Postgres uses. It is deliberately not larger:
/// the cost is paid by the *server*, once per connection, and a client pool opens ten of them
/// before it has sent a query, so raising it raises the time to first query rather than the cost
/// of an offline guess against a file the attacker has to have stolen first.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// The number of bytes of salt a derived credential carries
pub const SALT_LEN: usize = 16;

/// The things that can go wrong while a peer is proving who it is
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// This build cannot do the mechanism the server selected
    UnsupportedMechanism(AuthMechanism),
    /// The server wants this client to prove who it is and it has nothing to prove it with
    NoCredentials,
    /// A message in the exchange was not shaped the way the mechanism says it is
    Malformed(&'static str),
    /// The peer replayed a nonce that is not the one this exchange is using
    NonceMismatch,
    /// The peer could not prove it knows the password, or there is no such user
    ///
    /// These are one variant on purpose. Splitting them is what turns a login into a way to ask
    /// whether an account exists.
    BadCredentials,
    /// The server could not prove it holds the credential it claims to
    ///
    /// SCRAM authenticates both ways. A server that cannot produce this signature has the client's
    /// salt and iteration count but not the key derived from its password, which is what a peer
    /// replaying a captured handshake looks like.
    ServerNotAuthenticated,
    /// A message arrived at a point in the exchange where it does not belong
    OutOfOrder,
}

impl std::fmt::Display for AuthError {
    /// Write a legible description of this authentication error
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::UnsupportedMechanism(mechanism) => {
                write!(f, "this build cannot authenticate with {mechanism}")
            }
            AuthError::NoCredentials => {
                write!(f, "the server requires authentication and none was given")
            }
            AuthError::Malformed(what) => write!(f, "malformed authentication message: {what}"),
            AuthError::NonceMismatch => write!(f, "the peer replayed the wrong nonce"),
            AuthError::BadCredentials => write!(f, "authentication failed"),
            AuthError::ServerNotAuthenticated => {
                write!(f, "the server could not prove it holds this credential")
            }
            AuthError::OutOfOrder => write!(f, "an authentication message arrived out of order"),
        }
    }
}

impl std::error::Error for AuthError {}

impl AuthError {
    /// Get the prose a server is willing to say about this failure over the wire
    ///
    /// Every failure a *peer* can cause collapses to one sentence here. The variants stay distinct
    /// in the server's own log, where telling "no such user" from "wrong password" is useful and
    /// nobody untrusted is reading.
    pub const fn wire_msg(&self) -> &'static str {
        match self {
            AuthError::UnsupportedMechanism(_) | AuthError::NoCredentials => {
                "authentication is required on this server"
            }
            // everything a client got wrong is one message, whichever of them it was
            _ => "authentication failed",
        }
    }
}

/// Who a connection belongs to
///
/// This is what an exchange yields, and it is the thing per-table authorization has been waiting
/// on. **Nothing consults it yet** — it is logged and carried, and the catalog that would check it
/// does not exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The name this connection authenticated as
    pub name: String,
    /// How it proved that name
    pub mechanism: AuthMechanism,
}

impl Principal {
    /// Create a principal for a connection that authenticated
    ///
    /// # Arguments
    ///
    /// * `name` - The name this connection authenticated as
    /// * `mechanism` - How it proved that name
    pub fn new<N: Into<String>>(name: N, mechanism: AuthMechanism) -> Self {
        Principal {
            name: name.into(),
            mechanism,
        }
    }
}

impl std::fmt::Display for Principal {
    /// Write this principal's name and how it was established
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} via {}", self.name, self.mechanism)
    }
}

/// What a client has to prove itself with
///
/// [`Credentials::None`] is not a degenerate case, it is the default. A client built with it
/// offers no mechanism, and a server that requires none lets it in — which is every deployment
/// this database has had until now, and is what keeps the benchmark harness comparable.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum Credentials {
    /// This client has nothing to prove and will be refused by a server that wants proof
    #[default]
    None,
    /// This client has a username and a password to prove it with
    Scram {
        /// The name to authenticate as
        username: String,
        /// The password to prove it with, which never crosses the wire
        password: String,
    },
}

impl Credentials {
    /// Build credentials that prove nothing
    pub const fn none() -> Self {
        Credentials::None
    }

    /// Build credentials for a username and password
    ///
    /// # Arguments
    ///
    /// * `username` - The name to authenticate as
    /// * `password` - The password to prove it with
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::shared::auth::Credentials;
    ///
    /// let creds = Credentials::scram("reader", "hunter2");
    /// ```
    pub fn scram<U: Into<String>, P: Into<String>>(username: U, password: P) -> Self {
        Credentials::Scram {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Get the mechanisms these credentials can do
    pub const fn mechanisms(&self) -> AuthMechanisms {
        match self {
            Credentials::None => AuthMechanisms::NONE,
            Credentials::Scram { .. } => AuthMechanisms::SCRAM_SHA_256,
        }
    }

    /// Check whether these credentials can prove anything at all
    pub const fn is_none(&self) -> bool {
        matches!(self, Credentials::None)
    }
}

impl std::fmt::Debug for Credentials {
    /// Write these credentials without writing the password
    ///
    /// The derived implementation would print the password into any log line that debugs a client,
    /// a connection manager, or a config, which is the most common way a secret ends up on disk.
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credentials::None => write!(f, "Credentials::None"),
            Credentials::Scram { username, .. } => {
                write!(f, "Credentials::Scram {{ username: {username:?}, .. }}")
            }
        }
    }
}

/// What a server keeps so that it can check a password without holding one
///
/// The four fields are exactly RFC 5802's: the salt and iteration count the client needs to derive
/// the same keys, and the two keys the server needs to check the client's proof and to produce its
/// own. **None of them can be replayed as a password**, but `stored_key` *can* be replayed as a
/// login by anything that reads this file, which is why it is still a secret and why a config file
/// carrying one is a config file with permissions on it.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoredCredential {
    /// The salt this credential's keys were derived with
    #[serde(with = "base64_bytes")]
    pub salt: Vec<u8>,
    /// The number of PBKDF2 iterations they were derived with
    pub iterations: u32,
    /// `H(HMAC(SaltedPassword, "Client Key"))`, which checks a client's proof
    #[serde(with = "base64_bytes")]
    pub stored_key: Vec<u8>,
    /// `HMAC(SaltedPassword, "Server Key")`, which proves this server to a client
    #[serde(with = "base64_bytes")]
    pub server_key: Vec<u8>,
}

impl std::fmt::Debug for StoredCredential {
    /// Write this credential without writing either of its keys
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCredential")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

/// Read and write a byte string as base64 in a config file
///
/// A salt and a key are bytes, and YAML has no way to spell those. Base64 is what RFC 5802 already
/// puts them in on the wire, so the file and the wire agree with each other.
mod base64_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Write a byte string out as base64
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to write
    /// * `serializer` - The serializer to write them with
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    /// Read a byte string in from base64
    ///
    /// # Arguments
    ///
    /// * `deserializer` - The deserializer to read from
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        // read the field as a string first, since that is what it is in the file
        let encoded = String::deserialize(deserializer)?;
        STANDARD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

/// Read and write mechanisms as the SASL names a config file spells them with
///
/// [`AuthMechanism`] lives in [`protocol::auth`], which has to stay reachable from `core` and
/// `uuid` alone so it can move to a client only crate later — so it cannot derive `serde`, and the
/// config file's spelling of it lives here instead. Writing the SASL names rather than Rust
/// variant names is what makes a config file and an RFC agree with each other.
///
/// [`protocol::auth`]: crate::shared::protocol::auth
pub mod mechanism_names {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::AuthMechanism;

    /// Write a list of mechanisms out as their SASL names
    ///
    /// # Arguments
    ///
    /// * `mechanisms` - The mechanisms to write
    /// * `serializer` - The serializer to write them with
    pub fn serialize<S: Serializer>(
        mechanisms: &[AuthMechanism],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // write each one as the name it is known by outside this codebase
        serializer.collect_seq(mechanisms.iter().map(|mechanism| mechanism.name()))
    }

    /// Read a list of mechanisms in from their SASL names
    ///
    /// A name this build does not know is an error rather than a mechanism that is skipped. A
    /// deployment that misspelled the only mechanism it accepts would otherwise get a server that
    /// requires authentication and can never grant it, which looks like a bug in the client.
    ///
    /// # Arguments
    ///
    /// * `deserializer` - The deserializer to read from
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<AuthMechanism>, D::Error> {
        // read the field as the list of strings it is in the file
        let names = Vec::<String>::deserialize(deserializer)?;
        // then map each one onto a mechanism, refusing anything we cannot name
        names
            .into_iter()
            .map(|name| {
                AuthMechanism::from_name(&name).ok_or_else(|| {
                    serde::de::Error::custom(format!("unknown authentication mechanism: {name}"))
                })
            })
            .collect()
    }
}

/// The users a server will accept, and what it needs to check them
///
/// # Invariants
///
/// **`fake_salt_key` is generated per process and never leaves it.** It exists to give an unknown
/// user a salt that looks real and stays the same across probes of the same name, which is what
/// keeps "no such user" from being observable. A constant would let anyone with a copy of this
/// source compute which challenges are fake.
pub struct CredentialStore {
    /// The users this server knows, by name
    users: HashMap<String, StoredCredential>,
    /// The key an unknown user's fake salt is derived from
    fake_salt_key: [u8; 32],
    /// The mechanisms this server accepts, strongest first
    mechanisms: Vec<AuthMechanism>,
    /// Whether a connection has to authenticate at all
    required: bool,
}

impl std::fmt::Debug for CredentialStore {
    /// Write this store without writing anything it holds
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialStore")
            .field("users", &self.users.len())
            .field("mechanisms", &self.mechanisms)
            .field("required", &self.required)
            .finish_non_exhaustive()
    }
}

impl CredentialStore {
    /// Build a store from the users a config named
    ///
    /// # Arguments
    ///
    /// * `users` - The credentials this server will accept, by name
    /// * `mechanisms` - The mechanisms this server accepts, strongest first
    /// * `required` - Whether a connection has to authenticate at all
    pub fn new(
        users: HashMap<String, StoredCredential>,
        mechanisms: Vec<AuthMechanism>,
        required: bool,
    ) -> Self {
        // draw the key an unknown user's fake salt will be derived from
        let mut fake_salt_key = [0u8; 32];
        scram::fill_random(&mut fake_salt_key);
        CredentialStore {
            users,
            fake_salt_key,
            mechanisms,
            required,
        }
    }

    /// Build a store that requires nothing, which is what a server with no `auth` section gets
    pub fn open() -> Self {
        CredentialStore::new(HashMap::new(), Vec::new(), false)
    }

    /// Check whether this server requires a connection to authenticate
    pub const fn is_required(&self) -> bool {
        self.required
    }

    /// Pick the mechanism a client that offered this set has to use
    ///
    /// `None` means this connection does not have to authenticate. `Some(Err(()))` is not a case:
    /// a client that offered nothing this server accepts is refused by the caller, which is the
    /// only place that knows how to write a [`RefusalReason`] into a `HelloAck`.
    ///
    /// # Arguments
    ///
    /// * `offered` - The mechanisms the client said it can do
    ///
    /// [`RefusalReason`]: crate::shared::protocol::handshake::RefusalReason
    pub fn select(&self, offered: AuthMechanisms) -> Option<AuthMechanism> {
        // a server that requires nothing never asks a client for anything, whatever it offered
        if !self.required {
            return None;
        }
        offered.first_supported(&self.mechanisms)
    }

    /// Look a user up, or manufacture a credential that will fail the same way
    ///
    /// This never returns "no such user", by construction. An unknown name gets a salt derived
    /// from its own bytes and this process' key, the default iteration count, and keys that no
    /// password derives to — so the exchange runs to its end and fails at the proof, exactly where
    /// a wrong password fails.
    ///
    /// # Arguments
    ///
    /// * `username` - The name the client is trying to authenticate as
    pub fn lookup(&self, username: &str) -> (StoredCredential, bool) {
        // hand back the real credential when there is one
        if let Some(found) = self.users.get(username) {
            return (found.clone(), true);
        }
        // otherwise manufacture one that looks exactly like a real one and cannot be satisfied
        (
            scram::decoy_credential(&self.fake_salt_key, username),
            false,
        )
    }
}
