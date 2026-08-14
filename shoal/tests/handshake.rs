//! Integration tests for the handshake that opens every connection
//!
//! `shoal/tests/fingerprint.rs` establishes that two different schemas fingerprint differently.
//! This file establishes the thing that actually matters: that a client built from one schema
//! cannot open a connection to a server built from another, and gets told why.
//!
//! Two schemas can live in one test binary because nothing ties a client type to a server type —
//! `Shoal::new` takes an address and a free `S: QuerySupport`, and it is precisely that freedom
//! that made this failure possible in the first place. `utils::start` cannot be used here for the
//! same reason it is convenient everywhere else: it returns a client and a server that are
//! already paired.

use shoal_core::client::{ConnectError, Errors, Shoal};
use shoal_core::server::ShoalPool;
use shoal_core::shared::protocol::ProtocolError;

mod utils;

use utils::TestError;

/// The schema the server in these tests is built from
mod server_schema {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal_core::tables::EphemeralSortedTable;
    use shoal_derive::{db, ShoalSortedTable};

    /// The row this schema's only table holds
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Wire")]
    pub struct Row {
        /// The partition this row belongs to
        #[shoal(partition)]
        pub partition_key: String,
        /// The key this row is sorted by within its partition
        #[shoal(sort)]
        pub sort_key: String,
        /// The payload
        #[shoal(update)]
        pub data: String,
    }

    /// The schema the server is built from
    #[db]
    pub struct Wire {
        /// The only table in this schema
        pub rows: EphemeralSortedTable<Row>,
    }
}

/// A schema that differs from the server's by exactly one field
///
/// One extra field is the smallest difference that is still a different schema, and it is the
/// shape of difference that `bytecheck` is least likely to catch on its own: both peers archive
/// well formed rows, they just disagree about how many fields a row has.
mod client_schema {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal_core::tables::EphemeralSortedTable;
    use shoal_derive::{db, ShoalSortedTable};

    /// The server's row, with one field added
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Wire")]
    pub struct Row {
        /// The partition this row belongs to
        #[shoal(partition)]
        pub partition_key: String,
        /// The key this row is sorted by within its partition
        #[shoal(sort)]
        pub sort_key: String,
        /// The payload
        #[shoal(update)]
        pub data: String,
        /// A field the server's schema does not have
        #[shoal(filter)]
        pub extra: String,
    }

    /// The schema the mismatched client is built from
    #[db]
    pub struct Wire {
        /// The only table in this schema
        pub rows: EphemeralSortedTable<Row>,
    }
}

/// Start a server from the server schema and hand back the address it is listening on
///
/// This is `utils::start_with_conf` with the client half left off, since the whole point here is
/// to connect a client that was built from something else.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this server should store its data in, which has to outlive it
async fn start_server(
    temp_dir: &tempfile::TempDir,
) -> Result<(String, ShoalPool<server_schema::Wire>), TestError> {
    // build a config with a port nobody else in this run is using
    let conf = utils::build_config(temp_dir);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    // start the server and give it a moment to bind
    let pool = ShoalPool::<server_schema::Wire>::start(conf)?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    Ok((addr, pool))
}

/// A client built from a different schema is refused, and both fingerprints survive
///
/// The second half of this test is what keeps it honest. A check that refused every connection
/// would pass the first assertion, so the matching client has to be shown connecting to the very
/// same server.
///
/// **This takes about five seconds to fail on purpose.** `bb8` retries `connect` with backoff
/// until its connection timeout elapses, and a schema mismatch is permanent, so the pool spends
/// the full timeout before it gives up. That is not a hang.
#[tokio::test]
async fn a_client_built_from_a_different_schema_is_refused() -> Result<(), TestError> {
    // stand up a server from one schema
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_server(&temp_dir).await?;
    // a client from the other schema cannot open a connection to it
    let refused = Shoal::<client_schema::WireClient>::new(&addr)
        .await
        .err()
        .expect("a client built from a different schema was let in");
    // and the error names both fingerprints rather than being a closed socket
    match refused {
        Errors::Handshake(ConnectError::Protocol(ProtocolError::SchemaMismatch {
            ours,
            theirs,
        })) => {
            assert_ne!(ours, theirs, "a schema mismatch named the same two numbers");
        }
        other => panic!("a schema mismatch was reported as something else: {other:?}"),
    }
    // and a client from the matching schema still connects to the same server
    Shoal::<server_schema::WireClient>::new(&addr)
        .await
        .expect("a client built from the servers own schema was refused");
    Ok(())
}

/// A matching client can still query the server it shook hands with
///
/// The handshake sits in front of every connection in the pool, so an error in it would show up
/// as a client that connects and then cannot do anything. This is the cheapest way to notice.
#[tokio::test]
async fn a_matching_client_can_still_query() -> Result<(), TestError> {
    // stand up a server and a client that agree
    let temp_dir = utils::test_dir();
    let (addr, _pool) = start_server(&temp_dir).await?;
    let client = Shoal::<server_schema::WireClient>::new(&addr).await?;
    // write a row and read it back
    client
        .send_one(server_schema::Row {
            partition_key: "key".to_owned(),
            sort_key: "key".to_owned(),
            data: "value".to_owned(),
        })
        .await?;
    let mut stream = client
        .send(
            client
                .query()
                .add(server_schema::RowGet::new(vec!["key".to_owned()])),
        )
        .await?;
    // collect the rows that get answered with
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<server_schema::Row>()? {
            for row in found.iter() {
                rows.push(row.sort_key.to_string());
            }
        }
    }
    assert_eq!(rows, vec!["key".to_owned()]);
    Ok(())
}
