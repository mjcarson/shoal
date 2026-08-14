//! Unit tests for the authentication mechanism
//!
//! The exchange these cover runs entirely inside one process here. What it does over a socket is
//! `shoal/tests/auth.rs`, and the split is deliberate: everything that can be established without
//! a server is established without one, so a failure here is a failure of the mechanism rather
//! than of the plumbing.

use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

use super::scram::{ClientStep, ScramClient, ScramServer, ServerStep};
use super::{AuthError, CredentialStore, Credentials, StoredCredential, DEFAULT_ITERATIONS};
use crate::shared::protocol::auth::{AuthMechanism, AuthMechanisms};

/// Flip one bit inside the base64 value of a message's last attribute
///
/// The tampering has to happen in the *decoded* bytes and be re-encoded, rather than in the base64
/// text: flipping a character there usually produces something that is not base64 at all, which
/// would make these tests pass for the wrong reason — a parse failure rather than a failed check.
///
/// # Arguments
///
/// * `msg` - The message to tamper with
/// * `separator` - The attribute prefix whose value should be tampered with
fn flip_a_bit_in(msg: &[u8], separator: &str) -> Vec<u8> {
    // split the value off the end of the message
    let msg = std::str::from_utf8(msg).expect("the message is not text");
    let (head, encoded) = msg
        .rsplit_once(separator)
        .expect("the message has no such attribute");
    // flip a bit in the bytes it decodes to, then spell it again
    let mut raw = STANDARD.decode(encoded).expect("the value is not base64");
    raw[0] ^= 0b0000_0001;
    format!("{head}{separator}{}", STANDARD.encode(&raw)).into_bytes()
}

/// Build a store holding one user with one password
///
/// # Arguments
///
/// * `username` - The name to store
/// * `password` - The password to derive a credential from
fn store_with(username: &str, password: &str) -> CredentialStore {
    // derive the credential the way a config that named a password would
    let mut users = HashMap::new();
    users.insert(
        username.to_owned(),
        StoredCredential::from_password(password, DEFAULT_ITERATIONS),
    );
    CredentialStore::new(users, vec![AuthMechanism::ScramSha256], true)
}

/// Run a whole exchange between a fresh client and a server, handing back both halves' verdicts
///
/// # Arguments
///
/// * `store` - The users the server will accept
/// * `username` - The name the client authenticates as
/// * `password` - The password the client proves it with
fn exchange(
    store: &CredentialStore,
    username: &str,
    password: &str,
) -> Result<super::Principal, AuthError> {
    // the client speaks first, the same way it does on the wire
    let mut client = ScramClient::new(username, password);
    let mut server = ScramServer::new(store);
    let first = client.first()?;
    // the server answers with a challenge
    let challenge = match server.step(&first)? {
        ServerStep::Challenge(challenge) => challenge,
        ServerStep::Success { .. } => panic!("the server let a client in without a proof"),
    };
    // the client answers the challenge with its proof
    let ClientStep::Send(proof) = client.step(&challenge)?;
    // and the server checks it
    let (payload, principal) = match server.step(&proof)? {
        ServerStep::Success { payload, principal } => (payload, principal),
        ServerStep::Challenge(_) => panic!("the server challenged twice"),
    };
    // the client checks the server back, which is the half that makes this mutual
    client.finish(&payload)?;
    Ok(principal)
}

/// RFC 7677's test vector produces RFC 7677's proof, byte for byte
///
/// This is the test that pins this implementation to the standard rather than to itself. Both
/// nonces are fixed by the vector, so the client is built with the one it names and is fed the
/// server's message verbatim.
#[test]
fn the_rfc_7677_vector_round_trips() {
    // the vector's four messages, exactly as RFC 7677 section 3 prints them
    const CLIENT_FIRST: &str = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    const SERVER_FIRST: &str = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                                s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                                p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    const SERVER_FINAL: &str = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
    // drive the client with the nonce the vector fixes
    let mut client = ScramClient::with_nonce("user", "pencil", "rOprNGfwEbeRWgbNEkqO");
    assert_eq!(client.first().unwrap(), CLIENT_FIRST.as_bytes());
    // the proof it computes over the vector's challenge is the vector's proof
    let ClientStep::Send(final_message) = client.step(SERVER_FIRST.as_bytes()).unwrap();
    assert_eq!(
        std::str::from_utf8(&final_message).unwrap(),
        CLIENT_FINAL,
        "the client final message does not match RFC 7677"
    );
    // and the signature the vector says the server answers with is the one it expects
    client.finish(SERVER_FINAL.as_bytes()).unwrap();
}

/// A client with the right password authenticates, and the server learns its name
#[test]
fn the_right_password_authenticates() {
    let store = store_with("reader", "hunter2");
    let principal = exchange(&store, "reader", "hunter2").unwrap();
    assert_eq!(principal.name, "reader");
    assert_eq!(principal.mechanism, AuthMechanism::ScramSha256);
}

/// A client with the wrong password does not authenticate
#[test]
fn the_wrong_password_is_refused() {
    let store = store_with("reader", "hunter2");
    assert_eq!(
        exchange(&store, "reader", "hunter3").unwrap_err(),
        AuthError::BadCredentials
    );
}

/// A user that does not exist fails exactly the way a wrong password does
///
/// This is the property that keeps a login from being a directory listing, and it is why the
/// store hands back a decoy credential rather than a `None` the caller has to branch on.
#[test]
fn an_unknown_user_is_refused_identically() {
    let store = store_with("reader", "hunter2");
    let unknown = exchange(&store, "nobody", "hunter2").unwrap_err();
    let wrong = exchange(&store, "reader", "wrong").unwrap_err();
    assert_eq!(unknown, wrong);
    assert_eq!(unknown, AuthError::BadCredentials);
}

/// The challenge an unknown user gets is shaped like a real one, and is stable across probes
///
/// A challenge that differed — a missing salt, a different iteration count, a salt that changed
/// every time — would answer the question the test above stops the proof from answering.
#[test]
fn an_unknown_user_gets_a_plausible_challenge() {
    let store = store_with("reader", "hunter2");
    // ask for the same missing user twice, with a fresh client each time
    let challenge_of = |name: &str| {
        let mut client = ScramClient::new(name, "irrelevant");
        let mut server = ScramServer::new(&store);
        let first = client.first().unwrap();
        match server.step(&first).unwrap() {
            ServerStep::Challenge(challenge) => String::from_utf8(challenge).unwrap(),
            ServerStep::Success { .. } => panic!("a missing user was let in"),
        }
    };
    let first = challenge_of("nobody");
    let second = challenge_of("nobody");
    // the salt and the iteration count are the parts a prober can compare, and they do not move
    let salt_of = |msg: &str| {
        msg.split(',')
            .find(|field| field.starts_with("s="))
            .unwrap()
            .to_owned()
    };
    assert_eq!(salt_of(&first), salt_of(&second));
    assert!(first.contains(&format!("i={DEFAULT_ITERATIONS}")));
    // and a different missing user gets a different salt, the way two real users would
    assert_ne!(salt_of(&first), salt_of(&challenge_of("someone-else")));
}

/// A proof with a single bit flipped in it does not authenticate
#[test]
fn a_tampered_proof_is_refused() {
    let store = store_with("reader", "hunter2");
    let mut client = ScramClient::new("reader", "hunter2");
    let mut server = ScramServer::new(&store);
    let first = client.first().unwrap();
    let ServerStep::Challenge(challenge) = server.step(&first).unwrap() else {
        panic!("the server did not challenge");
    };
    let ClientStep::Send(proof) = client.step(&challenge).unwrap();
    // flip a bit inside the proof, leaving every other part of the message correct
    let proof = flip_a_bit_in(&proof, ",p=");
    assert_eq!(server.step(&proof).unwrap_err(), AuthError::BadCredentials);
}

/// A server that replaces the client's nonce rather than extending it is refused
///
/// This is the check that stops a peer replaying a challenge captured from another exchange: it
/// cannot contain a nonce that was drawn after it was captured.
#[test]
fn a_server_that_replaces_the_nonce_is_refused() {
    let mut client = ScramClient::new("reader", "hunter2");
    client.first().unwrap();
    // a challenge that is well formed in every way except whose nonce it extends
    let challenge = "r=someoneElsesNonce123,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    assert_eq!(
        client.step(challenge.as_bytes()).unwrap_err(),
        AuthError::NonceMismatch
    );
}

/// A client that echoes a nonce the server did not send is refused
#[test]
fn a_client_that_echoes_the_wrong_nonce_is_refused() {
    let store = store_with("reader", "hunter2");
    let mut client = ScramClient::new("reader", "hunter2");
    let mut server = ScramServer::new(&store);
    let first = client.first().unwrap();
    let ServerStep::Challenge(challenge) = server.step(&first).unwrap() else {
        panic!("the server did not challenge");
    };
    let ClientStep::Send(proof) = client.step(&challenge).unwrap();
    // rewrite the echoed nonce into one this exchange never used
    let proof = String::from_utf8(proof).unwrap();
    let (_, tail) = proof.split_once(",p=").unwrap();
    let tampered = format!("c=biws,r=a-nonce-nobody-drew,p={tail}");
    assert_eq!(
        server.step(tampered.as_bytes()).unwrap_err(),
        AuthError::NonceMismatch
    );
}

/// A server that cannot produce the final signature is refused by the client
#[test]
fn a_server_that_cannot_sign_is_refused() {
    let store = store_with("reader", "hunter2");
    let mut client = ScramClient::new("reader", "hunter2");
    let mut server = ScramServer::new(&store);
    let first = client.first().unwrap();
    let ServerStep::Challenge(challenge) = server.step(&first).unwrap() else {
        panic!("the server did not challenge");
    };
    let ClientStep::Send(proof) = client.step(&challenge).unwrap();
    let ServerStep::Success { payload, .. } = server.step(&proof).unwrap() else {
        panic!("the server did not accept a correct proof");
    };
    // flip a bit inside the signature the server proved itself with
    let payload = flip_a_bit_in(&payload, "v=");
    assert_eq!(
        client.finish(&payload).unwrap_err(),
        AuthError::ServerNotAuthenticated
    );
}

/// A client that says it wants channel binding is refused, rather than answered without it
#[test]
fn a_channel_binding_request_is_refused() {
    let store = store_with("reader", "hunter2");
    let mut server = ScramServer::new(&store);
    // `y` is a client that believes this server supports binding, which it does not
    let downgrade = "y,,n=reader,r=aNonceOfSomeLength";
    assert!(matches!(
        server.step(downgrade.as_bytes()).unwrap_err(),
        AuthError::Malformed(_)
    ));
}

/// Messages that arrive at the wrong point in the exchange are refused
#[test]
fn messages_out_of_order_are_refused() {
    // a client that is challenged before it has said anything
    let mut client = ScramClient::new("reader", "hunter2");
    assert_eq!(
        client.step(b"r=abc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096").unwrap_err(),
        AuthError::OutOfOrder
    );
    // a client that is let in before it has proved anything
    let mut client = ScramClient::new("reader", "hunter2");
    client.first().unwrap();
    assert_eq!(client.finish(b"v=abc").unwrap_err(), AuthError::OutOfOrder);
    // and a server sent a third message on a two message exchange
    let store = store_with("reader", "hunter2");
    let mut server = ScramServer::new(&store);
    let mut client = ScramClient::new("reader", "hunter2");
    let first = client.first().unwrap();
    let ServerStep::Challenge(challenge) = server.step(&first).unwrap() else {
        panic!("the server did not challenge");
    };
    let ClientStep::Send(proof) = client.step(&challenge).unwrap();
    server.step(&proof).unwrap();
    assert_eq!(server.step(&proof).unwrap_err(), AuthError::OutOfOrder);
}

/// A username with a comma or an equals in it survives the exchange intact
///
/// Both characters are structural in the message grammar, so an unescaped one would either break
/// the parse or inject a field into the message the proof is computed over.
#[test]
fn a_username_with_structural_characters_round_trips() {
    for username in ["a,b", "a=b", "=,=", "cn=svc,ou=prod"] {
        let store = store_with(username, "hunter2");
        let principal = exchange(&store, username, "hunter2").unwrap();
        assert_eq!(principal.name, username);
    }
}

/// Two credentials derived from the same password differ, and neither carries the password
#[test]
fn a_stored_credential_carries_no_password() {
    let first = StoredCredential::from_password("hunter2", DEFAULT_ITERATIONS);
    let second = StoredCredential::from_password("hunter2", DEFAULT_ITERATIONS);
    // a fresh salt each time is what stops two users with one password looking like one user
    assert_ne!(first.salt, second.salt);
    assert_ne!(first.stored_key, second.stored_key);
    // and nothing it holds is the password, in any encoding either half of this uses
    for field in [&first.salt, &first.stored_key, &first.server_key] {
        assert!(!field.windows(7).any(|window| window == b"hunter2"));
    }
    // the debug output is what ends up in a log line, and it names neither key
    let debugged = format!("{first:?}");
    assert!(!debugged.contains("stored_key"), "{debugged}");
    assert!(!debugged.contains("server_key"), "{debugged}");
}

/// A credential round trips through the base64 a config file spells it in
#[test]
fn a_stored_credential_round_trips_through_yaml() {
    let credential = StoredCredential::from_password("hunter2", DEFAULT_ITERATIONS);
    let yaml = serde_yaml::to_string(&credential).unwrap();
    let read: StoredCredential = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(credential, read);
}

/// A client's credentials say which mechanisms it can offer, and never print the password
#[test]
fn credentials_offer_what_they_can_do() {
    assert_eq!(Credentials::none().mechanisms(), AuthMechanisms::NONE);
    let creds = Credentials::scram("reader", "hunter2");
    assert_eq!(creds.mechanisms(), AuthMechanisms::SCRAM_SHA_256);
    // the debug output is what a client, a pool or a config drags into a log line
    let debugged = format!("{creds:?}");
    assert!(debugged.contains("reader"), "{debugged}");
    assert!(!debugged.contains("hunter2"), "{debugged}");
}

/// A store that requires nothing selects nothing, whatever a client offered
#[test]
fn a_store_that_requires_nothing_selects_nothing() {
    let open = CredentialStore::open();
    assert!(open.select(AuthMechanisms::SCRAM_SHA_256).is_none());
    assert!(open.select(AuthMechanisms::NONE).is_none());
    assert!(!open.is_required());
}

/// A store that requires authentication selects only what it accepts
#[test]
fn a_store_selects_from_its_own_preference_order() {
    let store = store_with("reader", "hunter2");
    assert_eq!(
        store.select(AuthMechanisms::SCRAM_SHA_256),
        Some(AuthMechanism::ScramSha256)
    );
    // a client that offers nothing, and one that offers only a mechanism this server will not do
    assert!(store.select(AuthMechanisms::NONE).is_none());
    assert!(store.select(AuthMechanisms::MUTUAL_TLS).is_none());
    // and a bit from a newer client that this build cannot name costs nothing
    assert_eq!(
        store.select(AuthMechanisms::SCRAM_SHA_256.union(AuthMechanisms::from_bits(1 << 9))),
        Some(AuthMechanism::ScramSha256)
    );
}
