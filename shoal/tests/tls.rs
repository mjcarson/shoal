//! Integration tests for encryption in transit against a running server
//!
//! `shoal-core`'s own unit tests establish that a certificate loads, that only kernel-supported
//! ciphers are offered, and that the `setsockopt` structs are built the way the kernel reads them.
//! These establish the things only a socket can — above all the one the whole feature exists for:
//! **that a response still lands, once, in memory the client allocated and aligned**, with the
//! kernel doing the record layer underneath it.
//!
//! # These tests need `modprobe tls`
//!
//! `setsockopt(TCP_ULP, "tls")` does not autoload the kernel's TLS module, so a machine that has
//! never used kTLS answers `ENOENT`. Every test here skips with a message naming that rather than
//! failing, the same way the `stage-profile` tests sit outside a default run. A skipped test that
//! reported green would be the exact failure mode this feature is about, which is why the skip is
//! loud and why the server refuses to start rather than falling back to plaintext.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{ClientOptions, Shoal};
use shoal::server::ShoalPool;
use shoal::shared::auth::Credentials;
use shoal::tables::EphemeralSortedTable;
use shoal_derive::{db, ShoalSortedTable};

mod utils;

use utils::{TestCertificate, TestError};

/// The name the authenticating server in these tests accepts
const USER: &str = "reader";

/// The password that name authenticates with
const PASSWORD: &str = "hunter2";

/// How wide the row that spans many TLS records is
///
/// A MiB is about sixty four records at TLS's ~16 KiB maximum, which is what makes this the arm
/// that proves the kernel reassembles across boundaries and the client never sees one. It is also
/// the width `macro/transport/*/large` uses, so the two are measuring the same shape.
const LARGE_ROW: usize = 1024 * 1024;

/// A row in the table these tests query
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TlsDb")]
pub struct TlsRecord {
    /// The partition key
    #[shoal(partition)]
    pub partition_key: String,
    /// The sort key
    #[shoal(sort)]
    pub sort_key: String,
    /// The payload
    #[shoal(update)]
    pub data: String,
}

/// The schema these tests run against
///
/// An ephemeral table, because none of this is about storage — it is about what happens to the
/// bytes between a shard and a client.
#[db]
pub struct TlsDb {
    /// The only table in this schema
    pub tls_records: EphemeralSortedTable<TlsRecord>,
}

/// Start an encrypted server and connect a client that trusts it
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this server should store its data in, which has to outlive it
async fn start_encrypted(
    temp_dir: &tempfile::TempDir,
) -> Result<(Shoal<TlsDbClient>, ShoalPool<TlsDb>, String, TestCertificate), TestError> {
    // generate a certificate for this run and point a server at it
    let cert = TestCertificate::new(temp_dir);
    let conf = utils::build_tls_config(temp_dir, &cert);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let pool = ShoalPool::<TlsDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // and a client that trusts it and nothing else
    let client =
        Shoal::<TlsDbClient>::with_options(&addr, ClientOptions::new().tls(cert.client_options()))
            .await?;
    Ok((client, pool, addr, cert))
}

/// Write one row through a client and read it back
///
/// # Arguments
///
/// * `client` - The client to query through
/// * `key` - The partition and sort key to use
/// * `data` - The payload to store
async fn round_trip(
    client: &Shoal<TlsDbClient>,
    key: &str,
    data: String,
) -> Result<Vec<String>, TestError> {
    // write the row, then read it straight back on the same pool
    client
        .send_one(TlsRecord {
            partition_key: key.to_owned(),
            sort_key: key.to_owned(),
            data,
        })
        .await?;
    let mut stream = client
        .send(client.query().add(TlsRecordGet::new(vec![key.to_owned()])))
        .await?;
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<TlsRecord>()? {
            for row in found.iter() {
                rows.push(row.data.to_string());
            }
        }
    }
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread")]
/// A query round trips over an encrypted connection
///
/// The floor. If this fails nothing else here means anything.
async fn a_query_round_trips_over_tls() -> Result<(), TestError> {
    skip_without_ktls!("a_query_round_trips_over_tls");
    let temp_dir = utils::test_dir();
    let (client, _pool, _addr, _cert) = start_encrypted(&temp_dir).await?;
    let rows = round_trip(&client, "one", "the quick brown fox".to_owned()).await?;
    assert_eq!(rows, vec!["the quick brown fox".to_owned()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// A MiB response over TLS still arrives through the aligned read path
///
/// The twin of `the_response_payload_lands_on_a_sixteen_byte_boundary` in
/// `shoal-core/src/client.rs`, and it pins the same thing that one does: a response goes into a
/// buffer this client allocated, at the start of it, whole, at a width that spans many TLS
/// records. A change that reintroduced a preamble-plus-payload read, or that handed responses back
/// through some other buffer, fails here.
///
/// **What it does not catch, stated because the assertion looks stronger than it is.**
/// `AlignedVec<16>` is aligned by construction, so this cannot distinguish kTLS from a userspace
/// TLS implementation that decrypted into a scratch buffer and copied — that version would be
/// slower, would lose the property F14 exists for, and would pass every test in this file. The
/// assertion that tells those apart is
/// `shared::tls::ktls::tests::a_socket_reports_the_tls_ulp_once_it_is_attached`, which asks the
/// kernel whether it owns the record layer. `ktls::enable` makes the same check per connection.
async fn the_response_buffer_lands_on_a_sixteen_byte_boundary_over_tls() -> Result<(), TestError> {
    skip_without_ktls!("the_response_buffer_lands_on_a_sixteen_byte_boundary_over_tls");
    let temp_dir = utils::test_dir();
    let (client, _pool, _addr, _cert) = start_encrypted(&temp_dir).await?;
    // seed a row wide enough that its response spans many TLS records
    client
        .send_one(TlsRecord {
            partition_key: "aligned".to_owned(),
            sort_key: "aligned".to_owned(),
            data: "z".repeat(LARGE_ROW),
        })
        .await?;
    // read it back and look at where the archive actually is in memory
    let mut stream = client
        .send(
            client
                .query()
                .add(TlsRecordGet::new(vec!["aligned".to_owned()])),
        )
        .await?;
    let mut checked = 0;
    while let Some(response) = stream.next().await? {
        let addr = response.buffer_address();
        assert_eq!(
            addr % 16,
            0,
            "a response buffer landed at {addr:#x}, which is not 16 byte aligned - the kernel is \
             no longer landing plaintext in the client's own buffer and the response path is copying"
        );
        checked += 1;
    }
    assert!(checked > 0, "no response was checked");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// A response spanning many TLS records comes back byte for byte
///
/// A MiB is ~64 records. The kernel reassembles across those boundaries and the client's two
/// `read_exact` calls never learn a boundary happened — this is the committed form of what the
/// kTLS spike established as its third phase before any of this was built.
async fn a_mib_response_over_tls_matches_its_plaintext_bytes() -> Result<(), TestError> {
    skip_without_ktls!("a_mib_response_over_tls_matches_its_plaintext_bytes");
    let temp_dir = utils::test_dir();
    let (client, _pool, _addr, _cert) = start_encrypted(&temp_dir).await?;
    // a payload whose bytes say where in it they came from, so a misordered record is visible
    let payload: String = (0..LARGE_ROW)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();
    let rows = round_trip(&client, "wide", payload.clone()).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), LARGE_ROW);
    assert_eq!(
        rows[0], payload,
        "a response spanning ~64 TLS records did not survive the record layer"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// SCRAM runs over TLS, in that order
///
/// The pair the two features are meant to be deployed as. It also pins the ordering: the TLS
/// handshake happens before the Shoal handshake, which happens before the exchange, so a SCRAM
/// proof is never the thing an observer on the path sees.
async fn scram_over_tls_authenticates() -> Result<(), TestError> {
    skip_without_ktls!("scram_over_tls_authenticates");
    let temp_dir = utils::test_dir();
    let cert = TestCertificate::new(&temp_dir);
    let conf = utils::build_tls_auth_config(&temp_dir, &cert, USER, PASSWORD);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let _pool = ShoalPool::<TlsDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // a client with both halves gets in
    let client = Shoal::<TlsDbClient>::with_options(
        &addr,
        ClientOptions::new()
            .credentials(Credentials::scram(USER, PASSWORD))
            .tls(cert.client_options()),
    )
    .await?;
    let rows = round_trip(&client, "authed", "over tls".to_owned()).await?;
    assert_eq!(rows, vec!["over tls".to_owned()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// A client with the wrong password is refused even though the link is encrypted
///
/// Encryption is not authentication. This is here because the two land together and it would be
/// easy to wire the second in a way the first accidentally satisfies.
async fn tls_does_not_authenticate_on_its_own() -> Result<(), TestError> {
    skip_without_ktls!("tls_does_not_authenticate_on_its_own");
    let temp_dir = utils::test_dir();
    let cert = TestCertificate::new(&temp_dir);
    let conf = utils::build_tls_auth_config(&temp_dir, &cert, USER, PASSWORD);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let _pool = ShoalPool::<TlsDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // the right certificate and the wrong password is still a refusal
    let refused = Shoal::<TlsDbClient>::with_options(
        &addr,
        ClientOptions::new()
            .credentials(Credentials::scram(USER, "not the password"))
            .tls(cert.client_options()),
    )
    .await;
    assert!(
        refused.is_err(),
        "an encrypted connection with a wrong password was accepted"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// A plaintext client is refused by an encrypted server
///
/// The failure is at connect time rather than at the first query, and the connection never becomes
/// usable. Without this a deployment could turn encryption on and not notice that half its clients
/// were still connecting.
async fn a_plaintext_client_is_refused_by_a_tls_server() -> Result<(), TestError> {
    skip_without_ktls!("a_plaintext_client_is_refused_by_a_tls_server");
    let temp_dir = utils::test_dir();
    let cert = TestCertificate::new(&temp_dir);
    let conf = utils::build_tls_config(&temp_dir, &cert);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let _pool = ShoalPool::<TlsDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // a client that offers no tls at all cannot complete this server's first handshake
    let refused = Shoal::<TlsDbClient>::new(&addr).await;
    assert!(
        refused.is_err(),
        "a plaintext client connected to an encrypted server"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// A TLS client is refused by a plaintext server
///
/// The converse, and the one more likely to happen by accident: a client configured for a cluster
/// that has not been switched over yet.
async fn a_tls_client_is_refused_by_a_plaintext_server() -> Result<(), TestError> {
    skip_without_ktls!("a_tls_client_is_refused_by_a_plaintext_server");
    let temp_dir = utils::test_dir();
    let cert = TestCertificate::new(&temp_dir);
    let conf = utils::build_config(&temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    let _pool = ShoalPool::<TlsDb>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // the server answers a ClientHello with a shoal HelloAck, which is not a TLS record
    let refused =
        Shoal::<TlsDbClient>::with_options(&addr, ClientOptions::new().tls(cert.client_options()))
            .await;
    assert!(
        refused.is_err(),
        "an encrypted client connected to a plaintext server"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// A client that does not trust the server's authority is refused
///
/// The certificate is checked rather than merely presented. A version of this feature that
/// encrypted without verifying would pass every other test in this file.
async fn a_client_that_does_not_trust_the_certificate_is_refused() -> Result<(), TestError> {
    skip_without_ktls!("a_client_that_does_not_trust_the_certificate_is_refused");
    let temp_dir = utils::test_dir();
    let (_client, _pool, addr, _cert) = start_encrypted(&temp_dir).await?;
    // a second, unrelated certificate that has nothing to do with the running server
    let other_dir = utils::test_dir();
    let other = TestCertificate::new(&other_dir);
    let refused =
        Shoal::<TlsDbClient>::with_options(&addr, ClientOptions::new().tls(other.client_options()))
            .await;
    assert!(
        refused.is_err(),
        "a client trusting an unrelated authority was accepted"
    );
    Ok(())
}
