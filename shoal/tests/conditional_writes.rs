//! Integration tests for conditional writes on a standalone server
//!
//! A conditional write is an insert, a delete or an update applied only if the row stored
//! under its key is as its writer expects: absent, or present and passing a filter. A write
//! whose condition does not hold is refused with a reason and changes nothing
//! ([F68](../../docs/src/features/conditional-writes.md)). These tests hold that to every table
//! kind a standalone server serves - persistent and ephemeral, unsorted and sorted - and to rows
//! that are resident, on disk only, and in a sorted partition only partly read. The replicated
//! half, judged at apply in committed order, is in `cluster_fixture.rs`.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Errors;
use shoal::shared::queries::{ConditionRefusal, ConditionalInsert, ConditionalWrite};
use shoal::shared::traits::RkyvSupport;
use shoal::storage::FileSystem;
use shoal::tables::{
    EphemeralSortedTable, EphemeralUnsortedTable, PersistentSortedTable, PersistentUnsortedTable,
};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// An account, one to a partition, whose version a writer expects
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CondDb")]
pub struct Account {
    /// The account's name
    #[shoal(partition)]
    pub id: String,
    /// The version a conditional write expects to find
    #[shoal(filter, update)]
    pub version: u64,
    /// Who holds this account
    #[shoal(update)]
    pub owner: String,
}

/// An entry in a bucket, many to a partition, whose version a writer expects
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CondDb")]
pub struct Entry {
    /// The bucket this entry is in
    #[shoal(partition)]
    pub bucket: String,
    /// The entry's name within its bucket
    #[shoal(sort)]
    pub name: String,
    /// The version a conditional write expects to find
    #[shoal(filter, update)]
    pub version: u64,
    /// Who holds this entry
    #[shoal(update)]
    pub owner: String,
}

/// An account held in memory only
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CondDb")]
pub struct MemAccount {
    /// The account's name
    #[shoal(partition)]
    pub id: String,
    /// The version a conditional write expects to find
    #[shoal(filter, update)]
    pub version: u64,
}

/// An entry held in memory only
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CondDb")]
pub struct MemEntry {
    /// The bucket this entry is in
    #[shoal(partition)]
    pub bucket: String,
    /// The entry's name within its bucket
    #[shoal(sort)]
    pub name: String,
    /// The version a conditional write expects to find
    #[shoal(filter, update)]
    pub version: u64,
}

/// The test database: every kind of table a conditional write can be sent to
#[db]
pub struct CondDb {
    /// Persistent and unsorted
    pub account: PersistentUnsortedTable<Account, FileSystem>,
    /// Persistent and sorted
    pub entry: PersistentSortedTable<Entry, FileSystem>,
    /// Ephemeral and unsorted
    pub mem_account: EphemeralUnsortedTable<MemAccount>,
    /// Ephemeral and sorted
    pub mem_entry: EphemeralSortedTable<MemEntry>,
}

/// The client this database is spoken to with
type Client = shoal::client::Shoal<CondDbClient>;

/// Build an account
///
/// # Arguments
///
/// * `id` - The account's name
/// * `version` - Its version
/// * `owner` - Who holds it
fn account(id: &str, version: u64, owner: &str) -> Account {
    Account {
        id: id.to_owned(),
        version,
        owner: owner.to_owned(),
    }
}

/// Build an entry
///
/// # Arguments
///
/// * `bucket` - The bucket it is in
/// * `name` - Its name within the bucket
/// * `version` - Its version
/// * `owner` - Who holds it
fn entry(bucket: &str, name: &str, version: u64, owner: &str) -> Entry {
    Entry {
        bucket: bucket.to_owned(),
        name: name.to_owned(),
        version,
        owner: owner.to_owned(),
    }
}

/// A filter matching an account at one version
///
/// # Arguments
///
/// * `version` - The version expected
fn account_at(version: u64) -> AccountFilter {
    AccountFilter {
        version: Some(vec![version]),
    }
}

/// A filter matching an entry at one version
///
/// # Arguments
///
/// * `version` - The version expected
fn entry_at(version: u64) -> EntryFilter {
    EntryFilter {
        version: Some(vec![version]),
    }
}

/// Send one write and return why it was refused, or `None` if it was applied
///
/// Any failure but a refusal fails the test, so a refusal can never hide one.
///
/// # Arguments
///
/// * `client` - The client to send with
/// * `query` - The write to send
async fn refusal_of<Q: Into<CondDbQueryKinds>>(
    client: &Client,
    query: Q,
) -> Result<Option<ConditionRefusal>, TestError> {
    // a refused write fails its response by name, with why
    match client.send_one(query).await {
        Ok(_) => Ok(None),
        Err(Errors::Refused { reason, .. }) => Ok(Some(reason)),
        Err(error) => Err(error.into()),
    }
}

/// Read an account back, if one is stored
///
/// # Arguments
///
/// * `client` - The client to read with
/// * `id` - The account's name
async fn read_account(client: &Client, id: &str) -> Result<Option<Account>, TestError> {
    // ask for this one partition
    let response = answer_of(client, AccountGet::new(vec![id.to_owned()])).await?;
    // an empty get is no row
    let Some(rows) = response.access::<Account>()? else {
        return Ok(None);
    };
    Ok(rows
        .first()
        .map(|archived| Account::deserialize(archived).expect("an account deserializes")))
}

/// Read an entry back, if one is stored
///
/// # Arguments
///
/// * `client` - The client to read with
/// * `bucket` - The bucket it is in
/// * `name` - Its name within the bucket
async fn read_entry(client: &Client, bucket: &str, name: &str) -> Result<Option<Entry>, TestError> {
    // ask for this one row of this one partition
    let get = EntryGet::new(vec![bucket.to_owned()]).sort_keys(vec![name.to_owned()]);
    let response = answer_of(client, get).await?;
    // an empty get is no row
    let Some(rows) = response.access::<Entry>()? else {
        return Ok(None);
    };
    Ok(rows
        .first()
        .map(|archived| Entry::deserialize(archived).expect("an entry deserializes")))
}

/// Send one query and return its answer unjudged, so a get that finds nothing is an answer
///
/// # Arguments
///
/// * `client` - The client to send with
/// * `query` - The query to send
async fn answer_of<Q: Into<CondDbQueryKinds>>(
    client: &Client,
    query: Q,
) -> Result<shoal::client::ShoalResponse<CondDbClient>, TestError> {
    // send a bundle of this one query
    let mut bundle = client.query();
    bundle.add_mut(query);
    let mut stream = client.send(bundle).await?;
    // its one answer, which a read failure would carry rather than hide
    let response = stream.next().await?.expect("a query is answered");
    if let Some(error) = response.error() {
        panic!("the read failed: {}", error.msg());
    }
    Ok(response)
}

/// Start a server and do nothing, so its intent log is compacted into archives on the way
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
async fn cycle_server(temp_dir: &TempDir) -> Result<(), TestError> {
    // start a shoal server on our existing data
    let (_client, pool) = utils::start::<CondDb>(temp_dir).await?;
    // shut it right back down
    pool.exit()?;
    // wait for threads to fully clean up
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Write some rows and leave them on disk with nothing resident in memory
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
/// * `accounts` - The accounts to write
/// * `entries` - The entries to write
async fn write_then_evict(
    temp_dir: &TempDir,
    accounts: &[Account],
    entries: &[Entry],
) -> Result<(), TestError> {
    // start a server and write our rows to it
    let (client, pool) = utils::start::<CondDb>(temp_dir).await?;
    for row in accounts {
        client.send_one(row.clone()).await?;
    }
    for row in entries {
        client.send_one(row.clone()).await?;
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so the writes are compacted into archives and nothing is resident
    cycle_server(temp_dir).await
}

/// An insert expecting no row is applied once and refused after
#[tokio::test]
async fn insert_if_absent_applies_once() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    // the first insert finds nothing and is applied
    assert_eq!(
        refusal_of(&client, account("a", 1, "first").if_absent()).await?,
        None
    );
    // the second finds the first's row and is refused by name
    assert_eq!(
        refusal_of(&client, account("a", 1, "second").if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    // and the stored row is the first one, untouched
    assert_eq!(
        read_account(&client, "a").await?,
        Some(account("a", 1, "first"))
    );
    pool.exit()?;
    Ok(())
}

/// An update expecting a version is applied while the row has it, and refused once it moved
#[tokio::test]
async fn update_if_matches_follows_the_row() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    client.send_one(account("a", 1, "first")).await?;
    // a writer that read version 1 moves the row to 2
    let move_to_two = AccountUpdate {
        partition_key: "a".to_owned(),
        version: Some(2),
        owner: Some("second".to_owned()),
    };
    assert_eq!(
        refusal_of(&client, move_to_two.clone().if_matches(account_at(1))).await?,
        None
    );
    // a second writer that also read version 1 is refused: the row moved under it
    assert_eq!(
        refusal_of(&client, move_to_two.if_matches(account_at(1))).await?,
        Some(ConditionRefusal::RowMismatch)
    );
    // an update of a row that does not exist says so
    let missing = AccountUpdate {
        partition_key: "nobody".to_owned(),
        version: Some(9),
        owner: None,
    };
    assert_eq!(
        refusal_of(&client, missing.if_matches(account_at(1))).await?,
        Some(ConditionRefusal::RowMissing)
    );
    // the row holds the one write that was applied
    assert_eq!(
        read_account(&client, "a").await?,
        Some(account("a", 2, "second"))
    );
    // and nothing was written under the key that was missing
    assert_eq!(read_account(&client, "nobody").await?, None);
    pool.exit()?;
    Ok(())
}

/// A whole row replaced only over the version its writer read
#[tokio::test]
async fn insert_if_matches_replaces_only_the_expected_row() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    client.send_one(account("a", 1, "first")).await?;
    // a replacement built on version 2 is refused, since the row is at 1
    assert_eq!(
        refusal_of(&client, account("a", 3, "late").if_matches(account_at(2))).await?,
        Some(ConditionRefusal::RowMismatch)
    );
    // one built on version 1 replaces it
    assert_eq!(
        refusal_of(&client, account("a", 2, "second").if_matches(account_at(1))).await?,
        None
    );
    // and a filter naming no field matches any row, so this one only asks that a row exists
    assert_eq!(
        refusal_of(
            &client,
            account("a", 3, "third").if_matches(AccountFilter::default())
        )
        .await?,
        None
    );
    assert_eq!(
        refusal_of(
            &client,
            account("b", 1, "new").if_matches(AccountFilter::default())
        )
        .await?,
        Some(ConditionRefusal::RowMissing)
    );
    assert_eq!(
        read_account(&client, "a").await?,
        Some(account("a", 3, "third"))
    );
    assert_eq!(read_account(&client, "b").await?, None);
    pool.exit()?;
    Ok(())
}

/// A delete expecting a version is refused at another, and a deleted row is no row
#[tokio::test]
async fn delete_if_matches_and_the_tombstone_is_absent() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    client.send_one(account("a", 1, "first")).await?;
    // a delete built on a version the row is not at changes nothing
    assert_eq!(
        refusal_of(
            &client,
            AccountDelete::new("a".to_owned()).if_matches(account_at(7))
        )
        .await?,
        Some(ConditionRefusal::RowMismatch)
    );
    assert!(read_account(&client, "a").await?.is_some());
    // one built on its version deletes it
    assert_eq!(
        refusal_of(
            &client,
            AccountDelete::new("a".to_owned()).if_matches(account_at(1))
        )
        .await?,
        None
    );
    assert_eq!(read_account(&client, "a").await?, None);
    // and the tombstone it left is no row, so an insert expecting none is applied over it
    assert_eq!(
        refusal_of(&client, account("a", 5, "again").if_absent()).await?,
        None
    );
    assert_eq!(
        read_account(&client, "a").await?,
        Some(account("a", 5, "again"))
    );
    pool.exit()?;
    Ok(())
}

/// A conditional write to a row that is on disk only reads the row before judging it
#[tokio::test]
async fn unsorted_conditions_judge_rows_on_disk() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // leave two accounts on disk with nothing resident
    write_then_evict(
        &temp_dir,
        &[account("a", 1, "first"), account("b", 1, "first")],
        &[],
    )
    .await?;
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    // a row on disk is a row: an insert expecting none is refused
    assert_eq!(
        refusal_of(&client, account("a", 9, "clobber").if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    // an update expecting the version on disk is applied
    let move_to_two = AccountUpdate {
        partition_key: "b".to_owned(),
        version: Some(2),
        owner: Some("second".to_owned()),
    };
    assert_eq!(
        refusal_of(&client, move_to_two.if_matches(account_at(1))).await?,
        None
    );
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // across a restart, which replays the intent log, the refusal left nothing and the
    // applied write is all there is
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    assert_eq!(
        read_account(&client, "a").await?,
        Some(account("a", 1, "first"))
    );
    assert_eq!(
        read_account(&client, "b").await?,
        Some(account("b", 2, "second"))
    );
    pool.exit()?;
    Ok(())
}

/// A refusal read out of a bundle rather than through `send_one`
#[tokio::test]
async fn a_refusal_is_readable_from_a_bundle() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    client.send_one(account("a", 1, "first")).await?;
    // one bundle: a write that is refused, then one that is applied
    let mut bundle = client.query();
    bundle.add_mut(account("a", 2, "lost").if_absent());
    bundle.add_mut(account("b", 1, "won").if_absent());
    let mut stream = client.send(bundle).await?;
    // collect every response by its place in the bundle
    let mut refusals = vec![None, None];
    while let Some(response) = stream.next().await? {
        refusals[response.get_index()] = Some(response.refusal());
    }
    assert_eq!(
        refusals,
        vec![Some(Some(ConditionRefusal::RowExists)), Some(None)]
    );
    pool.exit()?;
    Ok(())
}

/// A sorted conditional write is judged on its own row and no other in the partition
#[tokio::test]
async fn sorted_conditions_name_one_row() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    client.send_one(entry("b", "a", 1, "first")).await?;
    client.send_one(entry("b", "c", 1, "first")).await?;
    // a row between two others is absent, whatever its neighbours hold
    assert_eq!(
        refusal_of(&client, entry("b", "b", 1, "new").if_absent()).await?,
        None
    );
    // and a row that is there is not
    assert_eq!(
        refusal_of(&client, entry("b", "a", 1, "clobber").if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    // an update expecting a row's version is applied to that row
    let move_to_two = EntryUpdate {
        partition_key: "b".to_owned(),
        sort_key: "c".to_owned(),
        version: Some(2),
        owner: Some("second".to_owned()),
    };
    assert_eq!(
        refusal_of(&client, move_to_two.clone().if_matches(entry_at(1))).await?,
        None
    );
    assert_eq!(
        refusal_of(&client, move_to_two.if_matches(entry_at(1))).await?,
        Some(ConditionRefusal::RowMismatch)
    );
    // a delete at the wrong version is refused, at the right one applied
    let delete_a = EntryDelete::new("b".to_owned(), "a".to_owned());
    assert_eq!(
        refusal_of(&client, delete_a.clone().if_matches(entry_at(2))).await?,
        Some(ConditionRefusal::RowMismatch)
    );
    assert_eq!(
        refusal_of(&client, delete_a.clone().if_matches(entry_at(1))).await?,
        None
    );
    // and once it is gone, a delete expecting it says it is missing
    assert_eq!(
        refusal_of(&client, delete_a.if_matches(entry_at(1))).await?,
        Some(ConditionRefusal::RowMissing)
    );
    assert_eq!(read_entry(&client, "b", "a").await?, None);
    assert_eq!(
        read_entry(&client, "b", "b").await?,
        Some(entry("b", "b", 1, "new"))
    );
    assert_eq!(
        read_entry(&client, "b", "c").await?,
        Some(entry("b", "c", 2, "second"))
    );
    pool.exit()?;
    Ok(())
}

/// A sorted partition only partly read judges a row it lacks against disk
///
/// An insert into a partition that is not resident starts it with that one row and marks it as
/// having more on disk. A conditional write to another row of it has to read the partition
/// before it can say the row is absent, or it would insert over a row it never saw.
#[tokio::test]
async fn sorted_conditions_judge_rows_on_disk() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // leave two entries of one bucket on disk with nothing resident
    write_then_evict(
        &temp_dir,
        &[],
        &[entry("b", "a", 1, "first"), entry("b", "c", 1, "first")],
    )
    .await?;
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    // a plain insert of a third row leaves the partition resident but only partly read
    client.send_one(entry("b", "z", 1, "new")).await?;
    // a row on disk is a row: an insert expecting none is refused
    assert_eq!(
        refusal_of(&client, entry("b", "a", 9, "clobber").if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    // an update expecting the version on disk is applied
    let move_to_two = EntryUpdate {
        partition_key: "b".to_owned(),
        sort_key: "c".to_owned(),
        version: Some(2),
        owner: Some("second".to_owned()),
    };
    assert_eq!(
        refusal_of(&client, move_to_two.if_matches(entry_at(1))).await?,
        None
    );
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // across a restart the refusal left nothing and the applied write is all there is
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    assert_eq!(
        read_entry(&client, "b", "a").await?,
        Some(entry("b", "a", 1, "first"))
    );
    assert_eq!(
        read_entry(&client, "b", "c").await?,
        Some(entry("b", "c", 2, "second"))
    );
    assert_eq!(
        read_entry(&client, "b", "z").await?,
        Some(entry("b", "z", 1, "new"))
    );
    pool.exit()?;
    Ok(())
}

/// The ephemeral tables judge conditions the same way, through the same code
#[tokio::test]
async fn ephemeral_tables_judge_conditions() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<CondDb>(&temp_dir).await?;
    // an unsorted row: inserted once, then compared and swapped
    let first = MemAccount {
        id: "a".to_owned(),
        version: 1,
    };
    assert_eq!(refusal_of(&client, first.clone().if_absent()).await?, None);
    assert_eq!(
        refusal_of(&client, first.if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    let bump = MemAccountUpdate {
        partition_key: "a".to_owned(),
        version: Some(2),
    };
    let at_one = MemAccountFilter {
        version: Some(vec![1]),
    };
    assert_eq!(
        refusal_of(&client, bump.clone().if_matches(at_one.clone())).await?,
        None
    );
    assert_eq!(
        refusal_of(&client, bump.if_matches(at_one)).await?,
        Some(ConditionRefusal::RowMismatch)
    );
    // a sorted row: the same, on one row of a partition
    let row = MemEntry {
        bucket: "b".to_owned(),
        name: "a".to_owned(),
        version: 1,
    };
    assert_eq!(refusal_of(&client, row.clone().if_absent()).await?, None);
    assert_eq!(
        refusal_of(&client, row.if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    let delete = MemEntryDelete::new("b".to_owned(), "a".to_owned());
    let at_two = MemEntryFilter {
        version: Some(vec![2]),
    };
    assert_eq!(
        refusal_of(&client, delete.clone().if_matches(at_two)).await?,
        Some(ConditionRefusal::RowMismatch)
    );
    assert_eq!(
        refusal_of(&client, delete.if_matches(MemEntryFilter::default())).await?,
        None
    );
    pool.exit()?;
    Ok(())
}
