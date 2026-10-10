//! Integration tests for tables whose partition key is more than one field
//!
//! A composite partition key is a tuple of the fields marked `#[shoal(partition)]`, in the order
//! they are declared. The row is hashed field by field when it is inserted and the tuple is
//! hashed the same way when a query names it, so the two have to agree for any read, update,
//! delete or exists to find what an insert wrote
//! ([Resolved #92, #198](../../docs/src/appendix/resolved/composite-partition-key.md)). These
//! hold every query kind to that on every table kind a standalone server serves, and the
//! persistent tables to it again once their rows are read back from disk.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Errors;
use shoal::shared::queries::{ConditionRefusal, ConditionalInsert};
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

/// A stripe of an object, keyed the way an object store's stripe row is: by three integers
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CompositeDb")]
pub struct Stripe {
    /// Who owns the object
    #[shoal(partition)]
    pub consumer: u64,
    /// The object this stripe is part of
    #[shoal(partition)]
    pub object: u64,
    /// Which stripe of the object this is
    #[shoal(partition)]
    pub stripe: u64,
    /// The version a writer expects to find
    #[shoal(filter, update)]
    pub version: u64,
    /// A label for the stripe's contents
    #[shoal(update)]
    pub label: String,
}

/// A stripe held in memory only
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CompositeDb")]
pub struct MemStripe {
    /// Who owns the object
    #[shoal(partition)]
    pub consumer: u64,
    /// The object this stripe is part of
    #[shoal(partition)]
    pub object: u64,
    /// Which stripe of the object this is
    #[shoal(partition)]
    pub stripe: u64,
    /// The version a writer expects to find
    #[shoal(filter, update)]
    pub version: u64,
    /// A label for the stripe's contents
    #[shoal(update)]
    pub label: String,
}

/// A shelf's entry, many to a partition, keyed by a warehouse and an aisle
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CompositeDb")]
pub struct Shelf {
    /// The warehouse the shelf is in
    #[shoal(partition)]
    pub warehouse: String,
    /// The aisle within the warehouse
    #[shoal(partition)]
    pub aisle: u64,
    /// The item on the shelf, which rows are ordered by within an aisle
    #[shoal(sort)]
    pub item: String,
    /// The version a writer expects to find
    #[shoal(filter, update)]
    pub version: u64,
}

/// A shelf's entry held in memory only
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CompositeDb")]
pub struct MemShelf {
    /// The warehouse the shelf is in
    #[shoal(partition)]
    pub warehouse: String,
    /// The aisle within the warehouse
    #[shoal(partition)]
    pub aisle: u64,
    /// The item on the shelf, which rows are ordered by within an aisle
    #[shoal(sort)]
    pub item: String,
    /// The version a writer expects to find
    #[shoal(filter, update)]
    pub version: u64,
}

/// The test database: every kind of table, each with a composite partition key
#[db]
pub struct CompositeDb {
    /// Persistent and unsorted
    pub stripe: PersistentUnsortedTable<Stripe, FileSystem>,
    /// Ephemeral and unsorted
    pub mem_stripe: EphemeralUnsortedTable<MemStripe>,
    /// Persistent and sorted
    pub shelf: PersistentSortedTable<Shelf, FileSystem>,
    /// Ephemeral and sorted
    pub mem_shelf: EphemeralSortedTable<MemShelf>,
}

/// The client this database is spoken to with
type Client = shoal::client::Shoal<CompositeDbClient>;

/// Send one write and return why it was refused, or `None` if it was applied
///
/// Any failure but a refusal fails the test, so a refusal can never hide one.
///
/// # Arguments
///
/// * `client` - The client to send with
/// * `query` - The write to send
async fn refusal_of<Q: Into<CompositeDbQueryKinds>>(
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

/// Send one query and return its answer unjudged, so a get that finds nothing is an answer
///
/// # Arguments
///
/// * `client` - The client to send with
/// * `query` - The query to send
async fn answer_of<Q: Into<CompositeDbQueryKinds>>(
    client: &Client,
    query: Q,
) -> Result<shoal::client::ShoalResponse<CompositeDbClient>, TestError> {
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

/// Read the rows a get finds, deserialized, in the order they were answered
///
/// # Arguments
///
/// * `$client` - The client to read with
/// * `$row` - The row type the get returns
/// * `$get` - The get to send
macro_rules! rows_of {
    ($client:expr, $row:ty, $get:expr) => {{
        // send the get and take its answer whole
        let response = answer_of($client, $get).await?;
        // an empty get is no rows, and every row found is deserialized
        match response.access::<$row>()? {
            Some(rows) => rows
                .iter()
                .map(|archived| <$row>::deserialize(archived).expect("a row deserializes"))
                .collect::<Vec<$row>>(),
            None => Vec::new(),
        }
    }};
}

/// Expand a test holding every query kind to an unsorted table keyed by three integers
///
/// The persistent and the ephemeral table are different types with different query types, so
/// the one body is written once and expanded for each.
///
/// # Arguments
///
/// * `$test` - The test's name
/// * `$row` - The row type
/// * `$get` - Its get query
/// * `$exists` - Its exists query
/// * `$update` - Its update query
/// * `$delete` - Its delete query
macro_rules! unsorted_round_trip {
    ($test:ident, $row:ident, $get:ident, $exists:ident, $update:ident, $delete:ident) => {
        /// Every query kind finds what an insert wrote under a three field key, and only that
        #[tokio::test]
        async fn $test() -> Result<(), TestError> {
            let temp_dir = utils::test_dir();
            let (client, pool) = utils::start::<CompositeDb>(&temp_dir).await?;
            // build a row from its key and a label
            let row = |consumer, object, stripe, version, label: &str| $row {
                consumer,
                object,
                stripe,
                version,
                label: label.to_owned(),
            };
            // three rows: the second is the first's key reversed, the third differs in one field
            let first = row(1, 2, 3, 1, "first");
            let reversed = row(3, 2, 1, 1, "reversed");
            let neighbour = row(1, 2, 4, 1, "neighbour");
            for written in [&first, &reversed, &neighbour] {
                client.send_one(written.clone()).await?;
            }
            // one get naming all three keys finds all three rows
            let mut found = rows_of!(
                &client,
                $row,
                $get::new(vec![(1, 2, 3), (3, 2, 1), (1, 2, 4)])
            );
            found.sort_by_key(|row| (row.consumer, row.object, row.stripe));
            assert_eq!(
                found,
                vec![first.clone(), neighbour.clone(), reversed.clone()]
            );
            // each key alone finds its own row and nothing else
            for written in [&first, &reversed, &neighbour] {
                let key = (written.consumer, written.object, written.stripe);
                assert_eq!(
                    rows_of!(&client, $row, $get::new(vec![key])),
                    vec![written.clone()],
                    "the row under {key:?}"
                );
            }
            // a key nothing was written under finds nothing
            assert!(rows_of!(&client, $row, $get::new(vec![(2, 1, 3)])).is_empty());
            // exists agrees with the gets
            assert!(client.exists($exists::new((1, 2, 3))).await?);
            assert!(!client.exists($exists::new((2, 1, 3))).await?);
            // an update reaches the row its key names
            let update = $update {
                partition_key: (1, 2, 3),
                version: Some(2),
                label: Some("updated".to_owned()),
            };
            client.send_one(update).await?;
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![(1, 2, 3)])),
                vec![row(1, 2, 3, 2, "updated")]
            );
            // and leaves the reversed key's row alone
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![(3, 2, 1)])),
                vec![reversed.clone()]
            );
            // an insert expecting no row is refused where one is, and applied where none is
            assert_eq!(
                refusal_of(&client, row(1, 2, 3, 9, "again").if_absent()).await?,
                Some(ConditionRefusal::RowExists)
            );
            assert_eq!(
                refusal_of(&client, row(5, 5, 5, 1, "fresh").if_absent()).await?,
                None
            );
            assert!(client.exists($exists::new((5, 5, 5))).await?);
            // a delete removes the row its key names and no other
            client.send_one($delete::new(3, 2, 1)).await?;
            assert!(!client.exists($exists::new((3, 2, 1))).await?);
            assert!(client.exists($exists::new((1, 2, 3))).await?);
            pool.exit()?;
            Ok(())
        }
    };
}

/// Expand a test holding every query kind to a sorted table keyed by a string and an integer
///
/// # Arguments
///
/// * `$test` - The test's name
/// * `$row` - The row type
/// * `$get` - Its get query
/// * `$exists` - Its exists query
/// * `$update` - Its update query
/// * `$delete` - Its delete query
macro_rules! sorted_round_trip {
    ($test:ident, $row:ident, $get:ident, $exists:ident, $update:ident, $delete:ident) => {
        /// Every query kind finds what an insert wrote under a two field key, and only that
        #[tokio::test]
        async fn $test() -> Result<(), TestError> {
            let temp_dir = utils::test_dir();
            let (client, pool) = utils::start::<CompositeDb>(&temp_dir).await?;
            // build a row from its key, its sort key and a version
            let row = |warehouse: &str, aisle, item: &str, version| $row {
                warehouse: warehouse.to_owned(),
                aisle,
                item: item.to_owned(),
                version,
            };
            // two rows in one partition, one in a partition differing by the aisle, one by the
            // warehouse
            let bolts = row("north", 1, "bolts", 1);
            let nuts = row("north", 1, "nuts", 1);
            let other_aisle = row("north", 2, "bolts", 1);
            let other_house = row("south", 1, "bolts", 1);
            for written in [&bolts, &nuts, &other_aisle, &other_house] {
                client.send_one(written.clone()).await?;
            }
            // a partition is both rows written under its two fields, in sort key order
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![("north".to_owned(), 1)])),
                vec![bolts.clone(), nuts.clone()]
            );
            // a sort key narrows the partition to one row
            assert_eq!(
                rows_of!(
                    &client,
                    $row,
                    $get::new(vec![("north".to_owned(), 1)]).sort_keys(vec!["nuts".to_owned()])
                ),
                vec![nuts.clone()]
            );
            // a key differing in either field is a partition of its own
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![("north".to_owned(), 2)])),
                vec![other_aisle.clone()]
            );
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![("south".to_owned(), 1)])),
                vec![other_house.clone()]
            );
            assert!(rows_of!(&client, $row, $get::new(vec![("south".to_owned(), 2)])).is_empty());
            // exists agrees with the gets
            assert!(
                client
                    .exists($exists::new(vec![("north".to_owned(), 2)]))
                    .await?
            );
            assert!(
                !client
                    .exists($exists::new(vec![("south".to_owned(), 2)]))
                    .await?
            );
            // an update reaches the row its key and sort key name
            let update = $update {
                partition_key: ("north".to_owned(), 1),
                sort_key: "bolts".to_owned(),
                version: Some(2),
            };
            client.send_one(update).await?;
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![("north".to_owned(), 1)])),
                vec![row("north", 1, "bolts", 2), nuts.clone()]
            );
            // and leaves the same sort key in the other partitions alone
            assert_eq!(
                rows_of!(&client, $row, $get::new(vec![("north".to_owned(), 2)])),
                vec![other_aisle.clone()]
            );
            // an insert expecting no row is refused where one is, and applied where none is
            assert_eq!(
                refusal_of(&client, row("north", 1, "nuts", 9).if_absent()).await?,
                Some(ConditionRefusal::RowExists)
            );
            assert_eq!(
                refusal_of(&client, row("south", 2, "nuts", 1).if_absent()).await?,
                None
            );
            assert!(
                client
                    .exists($exists::new(vec![("south".to_owned(), 2)]))
                    .await?
            );
            // a delete removes the row its key names and no other
            client
                .send_one($delete::new("north".to_owned(), 2, "bolts".to_owned()))
                .await?;
            assert!(
                !client
                    .exists($exists::new(vec![("north".to_owned(), 2)]))
                    .await?
            );
            assert!(
                client
                    .exists($exists::new(vec![("north".to_owned(), 1)]))
                    .await?
            );
            pool.exit()?;
            Ok(())
        }
    };
}

unsorted_round_trip!(
    persistent_unsorted_round_trip,
    Stripe,
    StripeGet,
    StripeExists,
    StripeUpdate,
    StripeDelete
);
unsorted_round_trip!(
    ephemeral_unsorted_round_trip,
    MemStripe,
    MemStripeGet,
    MemStripeExists,
    MemStripeUpdate,
    MemStripeDelete
);
sorted_round_trip!(
    persistent_sorted_round_trip,
    Shelf,
    ShelfGet,
    ShelfExists,
    ShelfUpdate,
    ShelfDelete
);
sorted_round_trip!(
    ephemeral_sorted_round_trip,
    MemShelf,
    MemShelfGet,
    MemShelfExists,
    MemShelfUpdate,
    MemShelfDelete
);

/// Start a server and do nothing, so its intent log is compacted into archives on the way
///
/// # Arguments
///
/// * `temp_dir` - The storage dir this servers data lives in
async fn cycle_server(temp_dir: &TempDir) -> Result<(), TestError> {
    // start a shoal server on our existing data
    let (_client, pool) = utils::start::<CompositeDb>(temp_dir).await?;
    // shut it right back down
    pool.exit()?;
    // wait for threads to fully clean up
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Rows written under a composite key are found by it again once they are read from disk
///
/// A row reloaded from an archive is filed under the partition its stored hash names, so a
/// read after a restart is the check that the hash written to disk is the one a query computes.
#[tokio::test]
async fn persistent_rows_are_found_after_a_restart() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // the rows, in both persistent tables
    let stripes: Vec<Stripe> = [(1, 2, 3), (3, 2, 1), (7, 7, 7)]
        .into_iter()
        .map(|(consumer, object, stripe)| Stripe {
            consumer,
            object,
            stripe,
            version: 1,
            label: format!("{consumer}/{object}/{stripe}"),
        })
        .collect();
    let shelves: Vec<Shelf> = [
        ("north", 1, "bolts"),
        ("north", 1, "nuts"),
        ("south", 3, "gears"),
    ]
    .into_iter()
    .map(|(warehouse, aisle, item)| Shelf {
        warehouse: warehouse.to_owned(),
        aisle,
        item: item.to_owned(),
        version: 1,
    })
    .collect();
    // write them and shut the server down
    let (client, pool) = utils::start::<CompositeDb>(&temp_dir).await?;
    for row in &stripes {
        client.send_one(row.clone()).await?;
    }
    for row in &shelves {
        client.send_one(row.clone()).await?;
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so the writes are compacted into archives and nothing is resident
    cycle_server(&temp_dir).await?;
    // a fresh server reads each row back from disk by its key
    let (client, pool) = utils::start::<CompositeDb>(&temp_dir).await?;
    for row in &stripes {
        let key = (row.consumer, row.object, row.stripe);
        assert_eq!(
            rows_of!(&client, Stripe, StripeGet::new(vec![key])),
            vec![row.clone()],
            "the stripe under {key:?}"
        );
    }
    assert_eq!(
        rows_of!(&client, Shelf, ShelfGet::new(vec![("north".to_owned(), 1)])),
        shelves[..2].to_vec()
    );
    assert_eq!(
        rows_of!(&client, Shelf, ShelfGet::new(vec![("south".to_owned(), 3)])),
        shelves[2..].to_vec()
    );
    // and a key with its fields swapped is still nobody's
    assert!(!client.exists(StripeExists::new((2, 1, 3))).await?);
    pool.exit()?;
    Ok(())
}
