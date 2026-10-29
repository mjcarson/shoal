//! Integration tests for per-table authorization against a running server
//! ([F77](../../docs/src/features/authorization.md))
//!
//! `shoal-proto` holds the catalog's own rules and `shoal-core` holds what a grant compiles to.
//! These hold the things only a running server can: that a standalone server reads its grants
//! from `auth.grants`, that a principal is held to them query by query inside one bundle, that a
//! refused query changes nothing and says why by name, and that a server requiring no
//! authentication holds nobody to anything. The cluster's committed grants are the fixture's.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::{Errors, Shoal};
use shoal::server::conf::Auth;
use shoal::server::ShoalPool;
use shoal::shared::auth::Credentials;
use shoal::shared::grants::{Privileges, EVERY_TABLE};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::traits::{QuerySupport, TableNameSupport};
use shoal::tables::EphemeralUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};

mod utils;

use utils::TestError;

/// The password every user in these tests authenticates with
const PASSWORD: &str = "hunter2";

/// A row of the first table, which most grants name
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "GrantDb")]
pub struct Ledger {
    /// The partition key
    #[shoal(partition)]
    pub account: String,
    /// The payload
    #[shoal(update)]
    pub balance: String,
}

/// A row of the second table, which most grants leave out
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "GrantDb")]
pub struct Secret {
    /// The partition key
    #[shoal(partition)]
    pub name: String,
    /// The payload
    #[shoal(update)]
    pub value: String,
}

/// The schema these tests run against: two tables, so a grant on one can be told from both
#[db]
pub struct GrantDb {
    /// The table most grants name
    pub ledger: EphemeralUnsortedTable<Ledger>,
    /// The table most grants leave out
    pub secret: EphemeralUnsortedTable<Secret>,
}

/// Start a server that requires authentication and holds its users to these grants
///
/// Four users: `reader` may read the ledger, `writer` may write it, `owner` may do anything to
/// every table, and `stranger` is granted nothing at all.
///
/// # Arguments
///
/// * `temp_dir` - The temp dir this server should store its data in, which has to outlive it
async fn start_granted(
    temp_dir: &tempfile::TempDir,
) -> Result<(String, ShoalPool<GrantDb>), TestError> {
    // every user, and what each one holds
    let auth = Auth::default()
        .required(true)
        .user("reader", PASSWORD)
        .user("writer", PASSWORD)
        .user("owner", PASSWORD)
        .user("stranger", PASSWORD)
        .grant("reader", "Ledger", Privileges::READ)
        .grant("writer", "Ledger", Privileges::WRITE)
        .grant("owner", EVERY_TABLE, Privileges::ALL);
    let conf = utils::build_config(temp_dir).auth(auth);
    // start the server and wait until its shards are answering
    let mut pool = ShoalPool::<GrantDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    Ok((addr, pool))
}

/// Connect as one of the users these servers accept
///
/// # Arguments
///
/// * `addr` - The server
/// * `user` - The user to authenticate as
async fn connect_as(addr: &str, user: &str) -> Result<Shoal<GrantDbClient>, TestError> {
    Ok(Shoal::<GrantDbClient>::with_credentials(addr, Credentials::scram(user, PASSWORD)).await?)
}

/// The failure a query was answered with, if it was one
///
/// # Arguments
///
/// * `result` - What sending one query returned
fn refusal_of<T>(result: Result<T, Errors>) -> Option<(ErrorCode, String)> {
    match result {
        Err(Errors::Server { code, msg, .. }) => Some((code, msg)),
        Err(other) => panic!("a query failed some other way: {other:?}"),
        Ok(_) => None,
    }
}

/// Read one ledger row back as a user that may, returning its balance if it exists
///
/// # Arguments
///
/// * `client` - A client allowed to read the ledger
/// * `account` - The row to read
async fn balance_of(
    client: &Shoal<GrantDbClient>,
    account: &str,
) -> Result<Option<String>, TestError> {
    let response = client
        .send_one(LedgerGet::new(vec![account.to_string()]))
        .await?;
    Ok(response
        .access::<Ledger>()?
        .and_then(|rows| rows.first().map(|row| row.balance.to_string())))
}

/// A principal reads and writes exactly what it was granted, and each refusal says by name who
/// was refused what on which table, under `Unauthorized`
#[tokio::test]
async fn a_principal_does_only_what_it_was_granted() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, pool) = start_granted(&temp_dir).await?;
    let reader = connect_as(&addr, "reader").await?;
    let writer = connect_as(&addr, "writer").await?;
    // the writer may write the ledger, and the reader may read what it wrote
    writer
        .send_one(Ledger {
            account: "alice".to_string(),
            balance: "10".to_string(),
        })
        .await?;
    assert_eq!(balance_of(&reader, "alice").await?, Some("10".to_string()));
    // the writer may not read it back, and is told so by name
    let (code, msg) = refusal_of(
        writer
            .send_one(LedgerGet::new(vec!["alice".to_string()]))
            .await,
    )
    .expect("the writer read the ledger");
    assert_eq!(code, ErrorCode::Unauthorized);
    assert!(
        msg.contains("writer") && msg.contains("read") && msg.contains("Ledger"),
        "{msg}"
    );
    // nor test a row's existence, which is a read
    let exists = writer
        .exists(LedgerExists::new("alice".to_string()))
        .await;
    assert!(
        matches!(&exists, Err(Errors::Server { code: ErrorCode::Unauthorized, .. })),
        "{exists:?}"
    );
    // the reader may not write, update or delete it
    let (code, msg) = refusal_of(
        reader
            .send_one(Ledger {
                account: "alice".to_string(),
                balance: "0".to_string(),
            })
            .await,
    )
    .expect("the reader wrote the ledger");
    assert_eq!(code, ErrorCode::Unauthorized);
    assert!(msg.contains("reader may not write Ledger"), "{msg}");
    let deleted = reader
        .send_one(LedgerDelete::new("alice".to_string()))
        .await;
    assert_eq!(
        refusal_of(deleted).map(|(code, _)| code),
        Some(ErrorCode::Unauthorized)
    );
    // and none of that changed the row
    assert_eq!(balance_of(&reader, "alice").await?, Some("10".to_string()));
    // neither may touch the table no grant of theirs names
    for client in [&reader, &writer] {
        let (code, msg) = refusal_of(
            client
                .send_one(SecretGet::new(vec!["launch".to_string()]))
                .await,
        )
        .expect("a secret was read without a grant");
        assert_eq!(code, ErrorCode::Unauthorized);
        assert!(msg.contains("Secret") && msg.contains("nothing"), "{msg}");
    }
    pool.exit()?;
    Ok(())
}

/// A principal named nowhere in the grants is refused every query, and `*` grants every table
#[tokio::test]
async fn nothing_is_allowed_that_no_grant_names() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, pool) = start_granted(&temp_dir).await?;
    let stranger = connect_as(&addr, "stranger").await?;
    let owner = connect_as(&addr, "owner").await?;
    // the stranger authenticated, and may do nothing with it
    let written = stranger
        .send_one(Secret {
            name: "launch".to_string(),
            value: "now".to_string(),
        })
        .await;
    assert_eq!(
        refusal_of(written).map(|(code, _)| code),
        Some(ErrorCode::Unauthorized)
    );
    let read = stranger
        .send_one(LedgerGet::new(vec!["alice".to_string()]))
        .await;
    assert_eq!(
        refusal_of(read).map(|(code, _)| code),
        Some(ErrorCode::Unauthorized)
    );
    // a grant on every table reaches a table no grant names by name
    owner
        .send_one(Secret {
            name: "launch".to_string(),
            value: "later".to_string(),
        })
        .await?;
    let response = owner
        .send_one(SecretGet::new(vec!["launch".to_string()]))
        .await?;
    let found = response
        .access::<Secret>()?
        .and_then(|rows| rows.first().map(|row| row.value.to_string()));
    assert_eq!(found, Some("later".to_string()));
    pool.exit()?;
    Ok(())
}

/// One bundle is judged query by query: the queries its principal may run are answered, each one
/// it may not is refused under its own index, and the refused write never happened
#[tokio::test]
async fn a_bundle_is_refused_query_by_query() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (addr, pool) = start_granted(&temp_dir).await?;
    let owner = connect_as(&addr, "owner").await?;
    let reader = connect_as(&addr, "reader").await?;
    owner
        .send_one(Ledger {
            account: "bob".to_string(),
            balance: "5".to_string(),
        })
        .await?;
    // a read it may run, a write it may not, and a read of a table it holds nothing on
    let bundle = reader
        .query()
        .add(LedgerGet::new(vec!["bob".to_string()]))
        .add(Ledger {
            account: "bob".to_string(),
            balance: "500".to_string(),
        })
        .add(SecretGet::new(vec!["launch".to_string()]));
    let mut stream = reader.send(bundle).await?;
    let mut answers = Vec::new();
    while let Some(response) = stream.next().await? {
        answers.push(response.error().map(|error| error.code()));
    }
    assert_eq!(
        answers,
        vec![None, Some(ErrorCode::Unauthorized), Some(ErrorCode::Unauthorized)],
        "the bundle was not judged query by query"
    );
    // the refused write changed nothing
    assert_eq!(balance_of(&owner, "bob").await?, Some("5".to_string()));
    pool.exit()?;
    Ok(())
}

/// A server that requires no authentication has no principal to hold, so its grants hold nobody
#[tokio::test]
async fn an_open_server_holds_nobody_to_its_grants() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // grants, and no authentication
    let auth = Auth::default().grant("reader", "Ledger", Privileges::READ);
    let conf = utils::build_config(&temp_dir).auth(auth);
    let mut pool = ShoalPool::<GrantDb>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<GrantDbClient>::new(&addr).await?;
    // an anonymous client writes and reads a table no grant names
    client
        .send_one(Secret {
            name: "launch".to_string(),
            value: "now".to_string(),
        })
        .await?;
    let response = client
        .send_one(SecretGet::new(vec!["launch".to_string()]))
        .await?;
    assert!(response.access::<Secret>()?.is_some());
    pool.exit()?;
    Ok(())
}

/// A grant on a table the schema does not serve refuses the start by name, rather than being a
/// grant on nothing that leaves its principal refused with no word about why
#[tokio::test]
async fn a_grant_on_a_table_the_schema_does_not_serve_refuses_the_start() {
    let temp_dir = utils::test_dir();
    let auth = Auth::default()
        .required(true)
        .user("reader", PASSWORD)
        .grant("reader", "Ledgr", Privileges::READ);
    let conf = utils::build_config(&temp_dir).auth(auth);
    match ShoalPool::<GrantDb>::start(conf) {
        Ok(_) => panic!("a grant on a table nobody serves was accepted"),
        Err(error) => {
            let said = format!("{error:?}");
            assert!(said.contains("Ledgr") && said.contains("Ledger"), "{said}");
        }
    }
}

/// A table's position is where the schema declares it, the order `table_names` lists, which is
/// what a compiled grant is indexed by
#[test]
fn a_tables_position_is_its_place_among_the_table_names() {
    let names = <GrantDbClient as QuerySupport>::table_names();
    for table in [GrantDbTableNames::Ledger, GrantDbTableNames::Secret] {
        assert_eq!(names[table.position()], table.to_string());
    }
}
