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
