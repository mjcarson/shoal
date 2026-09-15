//! Tests over what a TLS session is loaded from and allowed to negotiate
//!
//! The handshake itself is exercised end to end in `shoal/tests/tls.rs`, where there are two
//! sockets and a runtime to drive it over. What is here is everything that can be decided without
//! one: certificate loading, the cipher list, and the shape of the two option structs.

use super::*;
use std::io::Write;

/// Write a self signed certificate and its key into a temporary directory
///
/// Returns the directory, which has to outlive the paths, along with the two paths.
fn certificate() -> (tempfile::TempDir, PathBuf, PathBuf) {
    // one throwaway certificate, generated rather than committed so that nothing expires
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("failed to generate a certificate");
    let dir = tempfile::tempdir().expect("failed to make a temporary directory");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::File::create(&cert_path)
        .expect("failed to create a certificate file")
        .write_all(issued.cert.pem().as_bytes())
        .expect("failed to write a certificate");
    std::fs::File::create(&key_path)
        .expect("failed to create a key file")
        .write_all(issued.key_pair.serialize_pem().as_bytes())
        .expect("failed to write a key");
    (dir, cert_path, key_path)
}

#[test]
/// A certificate and key round trip from PEM on disk into a server config
fn a_certificate_and_key_load_from_pem() {
    let (_dir, cert, key) = certificate();
    assert_eq!(load_certs(&cert).unwrap().len(), 1);
    load_key(&key).unwrap();
    server_config(&TlsServerOptions { cert, key }).unwrap();
}

#[test]
/// A file that is not there is named in the error rather than swallowed
///
/// A server that cannot find its certificate has to say which path it looked at, because the
/// commonest cause is a relative path resolved against the wrong working directory.
fn a_missing_certificate_names_the_path_it_looked_for() {
    let missing = PathBuf::from("/nonexistent/shoal/cert.pem");
    match load_certs(&missing) {
        Err(TlsError::Io { path, .. }) => assert_eq!(path, missing),
        other => panic!("expected an io error naming the path, got {other:?}"),
    }
}

#[test]
/// A PEM file holding no certificate is refused rather than producing an empty chain
///
/// An empty chain is accepted by `with_single_cert` in some rustls versions and produces a server
/// that completes no handshake, which is a long way from the mistake.
fn a_pem_file_with_no_certificate_is_refused() {
    let dir = tempfile::tempdir().expect("failed to make a temporary directory");
    let path = dir.path().join("empty.pem");
    std::fs::write(&path, b"not a certificate\n").expect("failed to write");
    assert!(matches!(
        load_certs(&path),
        Err(TlsError::NoCertificates(_))
    ));
    assert!(matches!(load_key(&path), Err(TlsError::NoPrivateKey(_))));
}

#[test]
/// Only the ciphers the kernel can be given keys for are offered
///
/// A suite outside this list negotiates a session that completes its handshake and then cannot be
/// handed to the kernel, which fails at `setsockopt` rather than anywhere useful.
fn only_kernel_supported_ciphers_are_offered() {
    assert_eq!(TLS_CIPHER_SUITES.len(), 2);
    let names: Vec<_> = TLS_CIPHER_SUITES
        .iter()
        .map(|suite| format!("{:?}", suite.suite()))
        .collect();
    assert!(names.iter().all(|name| name.contains("AES")));
    assert!(
        !names.iter().any(|name| name.contains("CHACHA")),
        "chacha20-poly1305 is deliberately not offered - see TLS_CIPHER_SUITES"
    );
}

#[test]
/// A server config always has extraction on and tickets off
///
/// Both are load bearing and neither is visible at a call site, so this is the test that fails if
/// someone rebuilds the config without them. Tickets in particular break every read on the
/// connection some milliseconds after it appears to have succeeded.
fn a_server_config_extracts_secrets_and_sends_no_tickets() {
    let (_dir, cert, key) = certificate();
    let config = server_config(&TlsServerOptions { cert, key }).unwrap();
    assert!(config.enable_secret_extraction);
    assert_eq!(config.send_tls13_tickets, 0);
}

#[test]
/// A client config has extraction on for the same reason the server's does
fn a_client_config_extracts_secrets() {
    let (_dir, cert, _key) = certificate();
    let config = client_config(&TlsClientOptions::new(cert)).unwrap();
    assert!(config.enable_secret_extraction);
}

#[test]
/// A certificate authority that is not a certificate is refused when the client is built
fn a_client_refuses_an_authority_it_cannot_read() {
    let dir = tempfile::tempdir().expect("failed to make a temporary directory");
    let path = dir.path().join("ca.pem");
    std::fs::write(&path, b"still not a certificate\n").expect("failed to write");
    assert!(matches!(
        client_config(&TlsClientOptions::new(path)),
        Err(TlsError::NoCertificates(_))
    ));
}

#[test]
/// An explicit server name wins over the address, and an address is used when there is none
///
/// The address is what a certificate is checked against by default, so a deployment whose
/// certificate carries a hostname rather than an IP needs the override to connect at all.
fn an_explicit_server_name_overrides_the_address() {
    let addr = "127.0.0.1:12000".parse().unwrap();
    let named = TlsClientOptions::new("/ca.pem").server_name("shoal.internal");
    assert_eq!(
        server_name(&named, &addr).unwrap(),
        ServerName::try_from("shoal.internal").unwrap()
    );
    let unnamed = TlsClientOptions::new("/ca.pem");
    assert_eq!(
        server_name(&unnamed, &addr).unwrap(),
        ServerName::from(std::net::IpAddr::from([127, 0, 0, 1]))
    );
}

#[test]
/// A name a certificate could never carry is refused rather than passed through
fn an_invalid_server_name_is_refused() {
    let addr = "127.0.0.1:12000".parse().unwrap();
    let options = TlsClientOptions::new("/ca.pem").server_name("not a host name");
    assert!(matches!(
        server_name(&options, &addr),
        Err(TlsError::InvalidServerName(_))
    ));
}

/// A cluster authority and one node certificate it signed, written to a directory
///
/// The leaf carries the `shoal-node://<id>` URI SAN the Q11 contract names, which nothing checks
/// at M2 - it is written so that the first build to check it has certificates to check.
fn cluster_pki(dir: &std::path::Path, node: &str) -> PeerTlsOptions {
    // the authority, self signed
    let ca_key = rcgen::KeyPair::generate().expect("a ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("a ca certificate");
    // the node, signed by it, reachable at the loopback name and named as a node
    let key = rcgen::KeyPair::generate().expect("a node key");
    let mut params =
        rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("node params");
    params.subject_alt_names.push(rcgen::SanType::URI(
        format!("shoal-node://{node}")
            .try_into()
            .expect("a uri san"),
    ));
    let cert = params
        .signed_by(&key, &ca, &ca_key)
        .expect("a node certificate");
    let options = PeerTlsOptions {
        cert: dir.join("node.pem"),
        key: dir.join("node.key"),
        ca: dir.join("ca.pem"),
        bind_identity: true,
    };
    std::fs::write(&options.cert, cert.pem()).expect("write the certificate");
    std::fs::write(&options.key, key.serialize_pem()).expect("write the key");
    std::fs::write(&options.ca, ca.pem()).expect("write the authority");
    options
}

/// Drive a client and a server handshake against each other in memory until both are done
///
/// # Arguments
///
/// * `client` - The dialling half
/// * `server` - The accepting half
fn pump(client: &mut TlsClientHandshake, server: &mut TlsServerHandshake) -> Result<(), TlsError> {
    let (mut client_done, mut server_done) = (false, false);
    // a bounded loop, so a handshake that stalls fails rather than hangs
    for _ in 0..32 {
        if !client_done {
            match client.step()? {
                TlsStep::Done => client_done = true,
                TlsStep::Transmit | TlsStep::NeedRead => {}
            }
            // whatever the client encoded goes to the server
            let out = client.take_outgoing();
            if !out.is_empty() {
                server.feed(&out);
            }
        }
        if !server_done {
            match server.step()? {
                TlsStep::Done => server_done = true,
                TlsStep::Transmit | TlsStep::NeedRead => {}
            }
            let out = server.take_outgoing();
            if !out.is_empty() {
                client.feed(&out);
            }
        }
        if client_done && server_done {
            return Ok(());
        }
    }
    panic!("the handshake did not finish in thirty two rounds");
}

#[test]
/// A peer with a certificate from the cluster's authority is accepted, and one without is not
///
/// The first is the mutual handshake every lane runs; the second is a client config - the
/// server-proves-itself shape a database client uses - offered to a peer listener, which is what
/// a node without a certificate looks like on the wire.
fn a_peer_listener_requires_a_certificate_from_the_cluster_authority() {
    let dir = tempfile::tempdir().expect("failed to make a temporary directory");
    let options = cluster_pki(dir.path(), "5f3c9a1e-0000-4000-8000-000000000001");
    let server = peer_server_config(&options).unwrap();
    assert!(server.enable_secret_extraction);
    assert_eq!(server.send_tls13_tickets, 0);
    let name = ServerName::try_from("localhost").unwrap();
    // a node presenting its certificate completes the handshake at both ends
    let peer = peer_client_config(&options).unwrap();
    assert!(peer.enable_secret_extraction);
    let mut client = TlsClientHandshake::client(peer, name.clone()).unwrap();
    let mut accept = TlsServerHandshake::server(server.clone()).unwrap();
    pump(&mut client, &mut accept).unwrap();
    // a client with the right authority and no certificate of its own is refused
    let anonymous = client_config(&TlsClientOptions::new(options.ca.clone())).unwrap();
    let mut client = TlsClientHandshake::client(anonymous, name).unwrap();
    let mut accept = TlsServerHandshake::server(server).unwrap();
    assert!(pump(&mut client, &mut accept).is_err());
}

/// A peer's certificate names its node through the `shoal-node://<id>` URI SAN and nothing else
///
/// The name is read off the DER by this crate, both ends of a finished handshake report it,
/// a leaf with no such name reports none, and a holder swaps both configs on a reload of new
/// material and keeps the old pair when the material is bad (F50).
#[test]
fn a_peer_certificate_names_its_node_and_a_reload_swaps_whole() {
    use crate::shared::identity::NodeId;
    let dir = tempfile::tempdir().expect("failed to make a temporary directory");
    let node = "5f3c9a1e-0000-4000-8000-000000000001";
    let options = cluster_pki(dir.path(), node);
    // the name, off the leaf
    let leaf = load_certs(&options.cert)
        .expect("the chain loads")
        .remove(0);
    assert_eq!(
        node_identity_of(leaf.as_ref()).expect("the leaf parses"),
        Some(NodeId(node.parse().expect("a uuid")))
    );
    // a leaf with a host name and no node name names no node
    let key = rcgen::KeyPair::generate().expect("a key");
    let unnamed = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
        .expect("params")
        .self_signed(&key)
        .expect("a certificate");
    assert_eq!(
        node_identity_of(unnamed.der().as_ref()).expect("the leaf parses"),
        None
    );
    // a uri of another scheme, or of the scheme with no uuid, names no node either
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("params");
    params.subject_alt_names.push(rcgen::SanType::URI(
        "https://example.com/".try_into().expect("a uri"),
    ));
    params.subject_alt_names.push(rcgen::SanType::URI(
        "shoal-node://not-a-uuid".try_into().expect("a uri"),
    ));
    let other = params.self_signed(&key).expect("a certificate");
    assert_eq!(
        node_identity_of(other.der().as_ref()).expect("the leaf parses"),
        None
    );
    // bytes that are not a certificate are an error, not an absence
    assert!(node_identity_of(b"not a certificate").is_err());
    // both ends of a handshake report the peer's name once it is done
    let name = ServerName::try_from("localhost").unwrap();
    let mut client =
        TlsClientHandshake::client(peer_client_config(&options).unwrap(), name).unwrap();
    let mut accept = TlsServerHandshake::server(peer_server_config(&options).unwrap()).unwrap();
    pump(&mut client, &mut accept).unwrap();
    let expected = PeerIdentity::Node(NodeId(node.parse().expect("a uuid")));
    assert_eq!(client.finish().expect("the client finishes").peer, expected);
    assert_eq!(accept.finish().expect("the server finishes").peer, expected);
    // the holder: the pair, the node's own name, and a reload that swaps both
    let holder = PeerTlsHolder::build(Some(&options)).expect("the holder builds");
    assert!(holder.is_encrypted() && holder.binds_identity());
    let before_client = holder.client().expect("a client config");
    let before_server = holder.server().expect("a server config");
    let other_node = "5f3c9a1e-0000-4000-8000-000000000002";
    let reissued = cluster_pki(dir.path(), other_node);
    assert_eq!(reissued.cert, options.cert, "the pki is rewritten in place");
    let report = holder.reload().expect("the reload succeeds");
    assert_eq!(
        report.own_identity,
        Some(NodeId(other_node.parse().expect("a uuid")))
    );
    assert_eq!((report.chain, report.authorities), (1, 1));
    let after_client = holder.client().expect("a client config");
    let after_server = holder.server().expect("a server config");
    assert!(
        !Arc::ptr_eq(&before_client, &after_client) && !Arc::ptr_eq(&before_server, &after_server)
    );
    // bad material reloads nothing: the pair stays what the last good reload made it
    std::fs::write(&options.key, b"not a key").expect("write");
    assert!(holder.reload().is_err());
    assert!(Arc::ptr_eq(
        &after_client,
        &holder.client().expect("a client config")
    ));
    assert!(Arc::ptr_eq(
        &after_server,
        &holder.server().expect("a server config")
    ));
    // plaintext lanes have nothing to reload
    let plaintext = PeerTlsHolder::build(None).expect("an empty holder");
    assert!(
        !plaintext.is_encrypted() && plaintext.client().is_none() && plaintext.reload().is_err()
    );
}
