//! Integration tests for sort keys of every type a sorted table may order its rows by
//!
//! A sorted table keeps a partition's rows in a tree by their sort key, and a partition read
//! from disk is sought in its archived form, so every query depends on the key's archived order
//! being its own ([Resolved #207, #42](../../docs/src/appendix/resolved/sort-keys-of-any-type.md)).
//! Each key shape below is chosen so that its order and the order of its decimal strings
//! disagree: `2` before `10`, `-5` before `-1`, `(2, "a")` before `(10, "a")`. A table that
//! fell back on a string's order, or an archive searched in an order of its own, answers in the
//! wrong order or misses a row. Every shape is held to every query a get, exists, update and
//! delete can make, on rows in memory, and on the persistent tables on rows read back from an
//! archive and over a restart; SHQL is held to the shapes it can name, a composite key among
//! them; and the walk is held to an integer key across a partition handed out in pieces.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::server::shard::walk::WalkAsk;
use shoal::shared::identity::TableId;
use shoal::shared::placement::tablet_of;
use shoal::shared::protocol::admin::WalkClass;
use shoal::shared::queries::parser::SORT_KEY_PRODUCT_LIMIT;
use shoal::shared::queries::SortRange;
use shoal::shared::traits::{
    check_sort_key_order, PartitionKeySupport, RkyvSupport, ShoalSortedTable, SortKey,
};
use shoal::storage::FileSystem;
use shoal::tables::walk::walked_rows;
use shoal::tables::{EphemeralSortedTable, PersistentSortedTable};
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

mod utils;

use utils::TestError;

/// The partition every shape's rows are written into
const STREAM: u64 = 7;

/// A row ordered by an unsigned integer, whose decimal strings sort another way
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct Counted {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// Where in its stream this row is
    #[shoal(sort)]
    pub at: u64,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// The same row held in memory only
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct MemCounted {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// Where in its stream this row is
    #[shoal(sort)]
    pub at: u64,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// A row ordered by a signed integer, negative ones among them
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct Delta {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// How far this row is from the stream's origin, either side of it
    #[shoal(sort)]
    pub delta: i64,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// A row ordered by a uuid, which orders by its bytes as a v7's time does
///
/// deepsize2 has no `uuid` feature, so this row sizes itself by hand rather than deriving
/// `DeepSizeOf`. A uuid is sixteen inline bytes with no children, so only the note counts.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct Minted {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// The id this row was minted with
    #[shoal(sort)]
    pub id: Uuid,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

impl DeepSizeOf for Minted {
    /// The heap bytes this row holds, which are its note's alone
    ///
    /// # Arguments
    ///
    /// * `context` - What deepsize2 has already counted
    fn deep_size_of_children(&self, context: &mut deepsize2::Context) -> usize {
        // the uuid is inline, so the note is the only heap this row owns
        self.note.deep_size_of_children(context)
    }
}

/// A row ordered by two fields, an integer and then a string, as one composite key
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct Paired {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// The first field of the key, which orders before the second
    #[shoal(sort)]
    pub seq: u64,
    /// The second field of the key, which orders rows of one `seq`
    #[shoal(sort)]
    pub name: String,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// The same composite keyed row held in memory only
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct MemPaired {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// The first field of the key, which orders before the second
    #[shoal(sort)]
    pub seq: u64,
    /// The second field of the key, which orders rows of one `seq`
    #[shoal(sort)]
    pub name: String,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// A row ordered by an optional integer, whose absence orders first
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct Slotted {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// The slot this row holds, if it holds one
    #[shoal(sort)]
    pub slot: Option<u32>,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// A day a schema numbers itself: a key type of the schema's own, opted in by hand
///
/// Its archived form orders by its derive, field by field, which is its own derived `Ord`, and
/// `days_order_as_they_archive` holds the two to each other the way a schema's own test would.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    serde::Serialize,
    serde::Deserialize,
    DeepSizeOf,
)]
#[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord))]
pub struct Day(pub u32);

impl RkyvSupport for Day {}

impl SortKey for Day {
    /// A day is four inline bytes and holds nothing on the heap
    fn heap_bytes(&self) -> usize {
        0
    }
}

/// A row ordered by a key type of the schema's own
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeyedDb")]
pub struct Dated {
    /// The stream this row is part of
    #[shoal(partition)]
    pub stream: u64,
    /// The day this row is filed under
    #[shoal(sort)]
    pub day: Day,
    /// A note a test rewrites
    #[shoal(update)]
    pub note: String,
}

/// The schema holding one table for every key shape
#[db]
pub struct KeyedDb {
    /// Rows ordered by an unsigned integer
    pub counted: PersistentSortedTable<Counted, FileSystem>,
    /// Rows ordered by an unsigned integer, in memory
    pub mem_counted: EphemeralSortedTable<MemCounted>,
    /// Rows ordered by a signed integer
    pub deltas: PersistentSortedTable<Delta, FileSystem>,
    /// Rows ordered by a uuid
    pub minted: PersistentSortedTable<Minted, FileSystem>,
    /// Rows ordered by an integer and a string
    pub paired: PersistentSortedTable<Paired, FileSystem>,
    /// Rows ordered by an integer and a string, in memory
    pub mem_paired: EphemeralSortedTable<MemPaired>,
    /// Rows ordered by an optional integer
    pub slotted: PersistentSortedTable<Slotted, FileSystem>,
    /// Rows ordered by a key type of the schema's own
    pub dated: PersistentSortedTable<Dated, FileSystem>,
}

/// Every query a sorted table answers, held to its key type's order
///
/// Stamped once per key shape, since each table's queries are types of their own. The keys are
/// five, distinct and ascending: the first four are written, in reverse so their order is the
/// table's and never the writer's, and the fifth is held back as a key no row has until the
/// archived test writes it beside the archive. A trailing test name stamps that test too, for a
/// persistent table.
///
/// # Arguments
///
/// * `$module` - The module the shape's tests live in
/// * `$row`, `$get`, `$exists`, `$update`, `$delete` - The table's row and query types
/// * `$keys` - The five keys, ascending
/// * `$make` - A row of a stream at a key with a note
/// * `$archived` - The name of the archived test, for a persistent table alone
macro_rules! shape {
    (
        $module:ident,
        $row:ident,
        $get:ident,
        $exists:ident,
        $update:ident,
        $delete:ident,
        $keys:expr,
        $make:expr
        $(, $archived:ident)?
    ) => {
        mod $module {
            use super::*;

            /// The key type this table orders its rows by
            type Key = <$row as ShoalSortedTable>::Sort;

            /// Five distinct keys in ascending order, whose order a string's would not keep
            fn keys() -> Vec<Key> {
                $keys
            }

            /// One of this table's rows
            ///
            /// # Arguments
            ///
            /// * `key` - The row's sort key
            /// * `note` - The row's note
            fn row(key: Key, note: &str) -> $row {
                ($make)(STREAM, key, note)
            }

            /// The keys and notes of every row a get answered with, in the order it answered
            ///
            /// # Arguments
            ///
            /// * `client` - The client to send the get with
            /// * `get` - The get to send
            async fn answered(
                client: &Shoal<KeyedDbClient>,
                get: $get,
            ) -> Result<Vec<(Key, String)>, TestError> {
                // send the get and drain every response it produced
                let mut stream = client.send(client.query().add(get)).await?;
                let mut rows = Vec::new();
                while let Some(response) = stream.next().await? {
                    // pull the key and the note out of each row this response carried
                    if let Some(found) = response.access::<$row>()? {
                        for archived in found.iter() {
                            let row = <$row as RkyvSupport>::deserialize(archived)
                                .expect("an answered row decodes");
                            rows.push((row.get_sort(), row.note.clone()));
                        }
                    }
                }
                Ok(rows)
            }

            /// The keys of rows a get answered with, in the order it answered
            ///
            /// # Arguments
            ///
            /// * `rows` - The keys and notes a get answered with
            fn keys_of(rows: &[(Key, String)]) -> Vec<Key> {
                rows.iter().map(|(key, _)| key.clone()).collect()
            }

            /// Whether a row exists at one key of the stream
            ///
            /// # Arguments
            ///
            /// * `client` - The client to ask with
            /// * `key` - The key to ask about
            async fn exists_at(client: &Shoal<KeyedDbClient>, key: &Key) -> Result<bool, TestError> {
                // ask about exactly one row of the stream
                let exists = client
                    .exists($exists::new(vec![STREAM]).sort_keys(vec![key.clone()]))
                    .await?;
                Ok(exists)
            }

            /// Write the first four keys, the last first, so their order is the table's
            ///
            /// # Arguments
            ///
            /// * `client` - The client to write with
            async fn write_first_four(client: &Shoal<KeyedDbClient>) -> Result<(), TestError> {
                // write every key but the one held back, in reverse
                for key in keys()[..4].iter().rev() {
                    let answer = client.send_one(row(key.clone(), "first")).await?;
                    assert!(answer.error().is_none(), "an insert at {key:?} failed");
                }
                Ok(())
            }

            /// Read the first four keys back every way a get and an exists can ask for them
            ///
            /// # Arguments
            ///
            /// * `client` - The client to read with
            async fn check_reads(client: &Shoal<KeyedDbClient>) -> Result<(), TestError> {
                let keys = keys();
                // every row comes back in its key type's order
                let all = answered(client, $get::new(vec![STREAM])).await?;
                assert_eq!(keys_of(&all), keys[..4].to_vec(), "a get of every row");
                // named keys come back in key order whatever order they were named in, and a
                // key no row has is a miss
                let named = $get::new(vec![STREAM]).sort_keys(vec![
                    keys[3].clone(),
                    keys[0].clone(),
                    keys[4].clone(),
                ]);
                let named = answered(client, named).await?;
                assert_eq!(keys_of(&named), vec![keys[0].clone(), keys[3].clone()], "named keys");
                // a range takes the rows between its bounds by the key type's order
                let range = SortRange::new(
                    Bound::Excluded(keys[0].clone()),
                    Bound::Included(keys[2].clone()),
                );
                let ranged = answered(client, $get::new(vec![STREAM]).sort_range(range)).await?;
                assert_eq!(keys_of(&ranged), keys[1..3].to_vec(), "a range");
                // the partition pages by feeding the last row of each page back as its cursor
                let mut paged: Vec<Key> = Vec::new();
                let mut cursor: Option<Key> = None;
                loop {
                    // the page after the last row seen, or the first page
                    let range = match &cursor {
                        Some(last) => SortRange::after(last.clone()),
                        None => SortRange::new(Bound::Unbounded, Bound::Unbounded),
                    };
                    let page = $get::new(vec![STREAM]).sort_range(range).limit(3);
                    let page = answered(client, page).await?;
                    // a page with nothing in it is the end of the partition
                    let Some((last, _)) = page.last() else {
                        break;
                    };
                    cursor = Some(last.clone());
                    paged.extend(keys_of(&page));
                    // four rows cannot take more than four pages, so bail rather than spin
                    assert!(paged.len() <= 4, "paging did not terminate: {paged:?}");
                }
                assert_eq!(paged, keys[..4].to_vec(), "paging by cursor");
                // an exists answers for the row it names, and for none it does not
                assert!(exists_at(client, &keys[1]).await?, "a written row does not exist");
                assert!(!exists_at(client, &keys[4]).await?, "a row never written exists");
                Ok(())
            }

            /// Update one row and delete another, each found by its key
            ///
            /// # Arguments
            ///
            /// * `client` - The client to write with
            async fn update_and_delete(client: &Shoal<KeyedDbClient>) -> Result<(), TestError> {
                let keys = keys();
                // rewrite the note of the third row
                let update = $update {
                    partition_key: STREAM,
                    sort_key: keys[2].clone(),
                    note: Some("updated".to_string()),
                };
                let answer = client.send_one(update).await?;
                assert!(answer.error().is_none(), "an update failed");
                // and delete the second
                let delete = $delete {
                    partition_key: STREAM,
                    sort_key: keys[1].clone(),
                };
                let answer = client.send_one(delete).await?;
                assert!(answer.error().is_none(), "a delete failed");
                // the deleted row is gone, and the one beside it is not
                assert!(!exists_at(client, &keys[1]).await?, "a deleted row exists");
                assert!(exists_at(client, &keys[2]).await?, "an updated row is gone");
                Ok(())
            }

            /// Rows held in memory answer every query in their key type's order
            #[tokio::test]
            async fn resident_rows_answer_in_key_order() -> Result<(), TestError> {
                // start a server and write the rows
                let temp_dir = utils::test_dir();
                let (client, pool) = utils::start::<KeyedDb>(&temp_dir).await?;
                write_first_four(&client).await?;
                // every read sees them in key order
                check_reads(&client).await?;
                // and an update and a delete each find the row they name
                update_and_delete(&client).await?;
                let keys = keys();
                let all = answered(&client, $get::new(vec![STREAM])).await?;
                let expected = vec![
                    (keys[0].clone(), "first".to_string()),
                    (keys[2].clone(), "updated".to_string()),
                    (keys[3].clone(), "first".to_string()),
                ];
                assert_eq!(all, expected, "the rows after an update and a delete");
                // Shutdown server
                pool.exit()?;
                Ok(())
            }

            $(
                /// Rows read back from an archive answer every query in their key type's
                /// order, beside rows written after it and across a restart
                ///
                /// An archived partition is sought in its archived form, so this is what holds
                /// the key type's archived order to its own.
                #[tokio::test]
                async fn $archived() -> Result<(), TestError> {
                    let temp_dir = utils::test_dir();
                    // write the rows to one shard and cycle it until they are in an archive
                    let (client, pool) = start_one_shard(&temp_dir).await?;
                    write_first_four(&client).await?;
                    archive(&temp_dir, client, pool).await?;
                    // start again over that storage, with nothing resident
                    let (client, pool) = start_one_shard(&temp_dir).await?;
                    // every read is answered from the archive in key order
                    check_reads(&client).await?;
                    // a row written now is resident over the archive, and a get spans both
                    let keys = keys();
                    let answer = client.send_one(row(keys[4].clone(), "late")).await?;
                    assert!(answer.error().is_none(), "an insert over the archive failed");
                    let all = answered(&client, $get::new(vec![STREAM])).await?;
                    assert_eq!(keys_of(&all), keys.clone(), "a get across memory and an archive");
                    // an update and a delete of archived rows each find the row they name
                    update_and_delete(&client).await?;
                    pool.exit()?;
                    drop(client);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    // and both hold over a restart
                    let (client, pool) = start_one_shard(&temp_dir).await?;
                    let all = answered(&client, $get::new(vec![STREAM])).await?;
                    let expected = vec![
                        (keys[0].clone(), "first".to_string()),
                        (keys[2].clone(), "updated".to_string()),
                        (keys[3].clone(), "first".to_string()),
                        (keys[4].clone(), "late".to_string()),
                    ];
                    assert_eq!(all, expected, "the rows after a restart");
                    // Shutdown server
                    pool.exit()?;
                    Ok(())
                }
            )?
        }
    };
}

/// Start a single shard server over a test's storage
///
/// One shard, so every partition a test writes is compacted by the same writer and read back by
/// the same reader.
///
/// # Arguments
///
/// * `temp_dir` - The storage to start over
async fn start_one_shard(
    temp_dir: &TempDir,
) -> Result<(Shoal<KeyedDbClient>, ShoalPool<KeyedDb>), TestError> {
    utils::start_with_conf::<KeyedDb>(utils::build_single_shard_config(temp_dir)).await
}

/// Stop a single shard server, then cycle its storage so what it wrote is compacted into an
/// archive and nothing of it is resident when the next server starts
///
/// # Arguments
///
/// * `temp_dir` - The storage the server wrote into
/// * `client` - The server's client
/// * `pool` - The server
async fn archive(
    temp_dir: &TempDir,
    client: Shoal<KeyedDbClient>,
    pool: ShoalPool<KeyedDb>,
) -> Result<(), TestError> {
    // stop the server that wrote the rows
    pool.exit()?;
    drop(client);
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so these inserts have been compacted into an archive
    let (client, pool) = start_one_shard(temp_dir).await?;
    pool.exit()?;
    drop(client);
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

shape!(
    unsigned,
    Counted,
    CountedGet,
    CountedExists,
    CountedUpdate,
    CountedDelete,
    vec![2, 10, 100, 1_000, u64::MAX],
    |stream, at, note: &str| Counted {
        stream,
        at,
        note: note.to_string()
    },
    archived_rows_answer_in_key_order
);

shape!(
    unsigned_in_memory,
    MemCounted,
    MemCountedGet,
    MemCountedExists,
    MemCountedUpdate,
    MemCountedDelete,
    vec![2, 10, 100, 1_000, u64::MAX],
    |stream, at, note: &str| MemCounted {
        stream,
        at,
        note: note.to_string()
    }
);

shape!(
    signed,
    Delta,
    DeltaGet,
    DeltaExists,
    DeltaUpdate,
    DeltaDelete,
    vec![i64::MIN, -5, -1, 3, i64::MAX],
    |stream, delta, note: &str| Delta {
        stream,
        delta,
        note: note.to_string()
    },
    archived_rows_answer_in_key_order
);

shape!(
    uuid_key,
    Minted,
    MintedGet,
    MintedExists,
    MintedUpdate,
    MintedDelete,
    // a uuid orders by its bytes, big end first, so as the integer they spell
    [2u128, 10, 100, 1_000, u128::MAX]
        .into_iter()
        .map(Uuid::from_u128)
        .collect(),
    |stream, id, note: &str| Minted {
        stream,
        id,
        note: note.to_string()
    },
    archived_rows_answer_in_key_order
);

shape!(
    composite,
    Paired,
    PairedGet,
    PairedExists,
    PairedUpdate,
    PairedDelete,
    vec![
        (1, "b".to_string()),
        (2, "a".to_string()),
        (2, "b".to_string()),
        (10, "a".to_string()),
        (10, "c".to_string()),
    ],
    |stream, (seq, name): (u64, String), note: &str| Paired {
        stream,
        seq,
        name,
        note: note.to_string()
    },
    archived_rows_answer_in_key_order
);

shape!(
    composite_in_memory,
    MemPaired,
    MemPairedGet,
    MemPairedExists,
    MemPairedUpdate,
    MemPairedDelete,
    vec![
        (1, "b".to_string()),
        (2, "a".to_string()),
        (2, "b".to_string()),
        (10, "a".to_string()),
        (10, "c".to_string()),
    ],
    |stream, (seq, name): (u64, String), note: &str| MemPaired {
        stream,
        seq,
        name,
        note: note.to_string()
    }
);

shape!(
    optional,
    Slotted,
    SlottedGet,
    SlottedExists,
    SlottedUpdate,
    SlottedDelete,
    vec![None, Some(0), Some(2), Some(10), Some(u32::MAX)],
    |stream, slot, note: &str| Slotted {
        stream,
        slot,
        note: note.to_string()
    },
    archived_rows_answer_in_key_order
);

shape!(
    schema_owned,
    Dated,
    DatedGet,
    DatedExists,
    DatedUpdate,
    DatedDelete,
    vec![Day(0), Day(2), Day(10), Day(100), Day(u32::MAX)],
    |stream, day, note: &str| Dated {
        stream,
        day,
        note: note.to_string()
    },
    archived_rows_answer_in_key_order
);

/// A schema's own key type orders the way it archives, which is the check a schema runs on a
/// key type it opts in
#[test]
fn days_order_as_they_archive() {
    // the edges and a few between, out of order, so the check sorts them itself
    let days = [Day(10), Day(0), Day(u32::MAX), Day(2), Day(100), Day(2)];
    check_sort_key_order(&days).expect("a day orders as it archives");
}

/// Write rows of the unsigned table at a set of keys
///
/// # Arguments
///
/// * `client` - The client to write with
/// * `keys` - The keys to write
async fn write_counted(client: &Shoal<KeyedDbClient>, keys: &[u64]) -> Result<(), TestError> {
    // one row a key, each acknowledged
    for at in keys {
        let row = Counted {
            stream: STREAM,
            at: *at,
            note: format!("row {at}"),
        };
        let answer = client.send_one(row).await?;
        assert!(answer.error().is_none(), "an insert at {at} failed");
    }
    Ok(())
}

/// The keys of the unsigned table's rows a SHQL query answered with
///
/// # Arguments
///
/// * `client` - The client to parse and send the query with
/// * `query` - The query
async fn shql_counted(client: &Shoal<KeyedDbClient>, query: &str) -> Result<Vec<u64>, TestError> {
    // `Queries` has no Debug impl, so the parse result is unwrapped by hand
    let queries = match client.query().parse(query) {
        Ok(queries) => queries,
        Err(error) => panic!("{query} did not bind: {}", error.message),
    };
    // send it and collect the key of every row it answered with
    let mut stream = client.send(queries).await?;
    let mut keys = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<Counted>()? {
            keys.extend(found.iter().map(|row| row.at.to_native()));
        }
    }
    Ok(keys)
}

/// An integer sort key is named and bounded in SHQL by integer literals, and answers in its own
/// order rather than its decimal strings'
#[tokio::test]
async fn shql_binds_an_integer_sort_key() -> Result<(), TestError> {
    // start a server and write rows whose decimal strings sort another way
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<KeyedDb>(&temp_dir).await?;
    write_counted(&client, &[1_000, 2, 100, 10]).await?;
    // a point names its row
    let point = shql_counted(
        &client,
        "SELECT * FROM Counted WHERE stream = 7 AND at = 10",
    )
    .await?;
    assert_eq!(point, vec![10]);
    // a set of points comes back in the key's order
    let set = shql_counted(
        &client,
        "SELECT * FROM Counted WHERE stream = 7 AND at IN (1000, 2)",
    )
    .await?;
    assert_eq!(set, vec![2, 1_000]);
    // and a range bounds by number: 10 and 100, never "100" < "2"
    let range = "SELECT * FROM Counted WHERE stream = 7 AND at > 2 AND at <= 100";
    assert_eq!(shql_counted(&client, range).await?, vec![10, 100]);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// A signed sort key is bounded in SHQL by a negative literal
#[tokio::test]
async fn shql_binds_a_negative_sort_key() -> Result<(), TestError> {
    // start a server and write rows either side of zero
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<KeyedDb>(&temp_dir).await?;
    for delta in [3i64, -1, -5, 0] {
        let row = Delta {
            stream: STREAM,
            delta,
            note: String::new(),
        };
        client.send_one(row).await?;
    }
    // a range below zero takes the negative rows, most negative first
    let query = "SELECT * FROM Delta WHERE stream = 7 AND delta >= -5 AND delta < 0";
    let queries = match client.query().parse(query) {
        Ok(queries) => queries,
        Err(error) => panic!("a negative bound did not bind: {}", error.message),
    };
    let mut stream = client.send(queries).await?;
    let mut deltas = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<Delta>()? {
            deltas.extend(found.iter().map(|row| row.delta.to_native()));
        }
    }
    assert_eq!(deltas, vec![-5, -1]);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// A uuid sort key is named in SHQL by its string, and a newtype by its inner value
#[tokio::test]
async fn shql_binds_a_uuid_and_a_newtype_sort_key() -> Result<(), TestError> {
    // start a server and write a minted row and a dated one
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<KeyedDb>(&temp_dir).await?;
    let id = Uuid::now_v7();
    let minted = Minted {
        stream: STREAM,
        id,
        note: "minted".to_string(),
    };
    client.send_one(minted.clone()).await?;
    let dated = Dated {
        stream: STREAM,
        day: Day(42),
        note: "dated".to_string(),
    };
    client.send_one(dated.clone()).await?;
    // the uuid is named by the string it prints as
    let query = format!("SELECT * FROM Minted WHERE stream = 7 AND id = '{id}'");
    let queries = match client.query().parse(&query) {
        Ok(queries) => queries,
        Err(error) => panic!("a uuid key did not bind: {}", error.message),
    };
    let mut stream = client.send(queries).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<Minted>()? {
            found.extend(
                rows.iter()
                    .map(|row| Minted::deserialize(row).expect("a row decodes")),
            );
        }
    }
    assert_eq!(found, vec![minted]);
    // and the day by the integer inside it
    let queries = match client
        .query()
        .parse("SELECT * FROM Dated WHERE stream = 7 AND day = 42")
    {
        Ok(queries) => queries,
        Err(error) => panic!("a newtype key did not bind: {}", error.message),
    };
    let mut stream = client.send(queries).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<Dated>()? {
            found.extend(
                rows.iter()
                    .map(|row| Dated::deserialize(row).expect("a row decodes")),
            );
        }
    }
    assert_eq!(found, vec![dated]);
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// The keys of the composite table's rows a SHQL query answered with, or the refusal it bound to
///
/// # Arguments
///
/// * `client` - The client to parse and send the query with
/// * `query` - The query
async fn shql_paired(
    client: &Shoal<KeyedDbClient>,
    query: &str,
) -> Result<Result<Vec<(u64, String)>, String>, TestError> {
    // a refusal is the answer some of these rows are after, so it is handed back
    let queries = match client.query().parse(query) {
        Ok(queries) => queries,
        Err(error) => return Ok(Err(error.message)),
    };
    // send it and collect the key of every row it answered with
    let mut stream = client.send(queries).await?;
    let mut keys = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<Paired>()? {
            keys.extend(
                found
                    .iter()
                    .map(|row| (row.seq.to_native(), row.name.to_string())),
            );
        }
    }
    Ok(Ok(keys))
}

/// A composite sort key is named in SHQL by a point on every one of its fields, and a set of
/// points on each is crossed into the keys they name together (item 42)
#[tokio::test]
async fn shql_names_a_composite_sort_key() -> Result<(), TestError> {
    // start a server and write rows whose first field's strings sort another way
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<KeyedDb>(&temp_dir).await?;
    for (seq, name) in [(10, "a"), (2, "b"), (1, "b"), (2, "a"), (10, "c")] {
        let row = Paired {
            stream: STREAM,
            seq,
            name: name.to_string(),
            note: String::new(),
        };
        client.send_one(row).await?;
    }
    // a point on each field names one row
    let point = "SELECT * FROM Paired WHERE stream = 7 AND seq = 2 AND name = 'b'";
    assert_eq!(
        shql_paired(&client, point).await?,
        Ok(vec![(2, "b".to_string())])
    );
    // the fields may be named in either order, since the key is built in declaration order
    let reversed = "SELECT * FROM Paired WHERE stream = 7 AND name = 'a' AND seq = 10";
    assert_eq!(
        shql_paired(&client, reversed).await?,
        Ok(vec![(10, "a".to_string())])
    );
    // a set on each field names every pairing of them, in key order, a pairing no row has
    // being a miss
    let crossed = "SELECT * FROM Paired WHERE stream = 7 AND seq IN (10, 2) AND name IN ('b', 'a')";
    let expected = vec![
        (2, "a".to_string()),
        (2, "b".to_string()),
        (10, "a".to_string()),
    ];
    assert_eq!(shql_paired(&client, crossed).await?, Ok(expected));
    // and leaving the key out reads the whole partition in key order
    let whole = "SELECT * FROM Paired WHERE stream = 7";
    let expected = vec![
        (1, "b".to_string()),
        (2, "a".to_string()),
        (2, "b".to_string()),
        (10, "a".to_string()),
        (10, "c".to_string()),
    ];
    assert_eq!(shql_paired(&client, whole).await?, Ok(expected));
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// A composite sort key named in part, or bounded by a range, or crossed into more keys than a
/// query may name, is refused by name before anything is sent (item 42)
#[tokio::test]
async fn shql_refuses_what_a_composite_sort_key_cannot_name() -> Result<(), TestError> {
    // start a server, which none of these queries should reach
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<KeyedDb>(&temp_dir).await?;
    // a prefix of the key is a range over the rest of it, which SHQL cannot write yet
    let partial = "SELECT * FROM Paired WHERE stream = 7 AND seq = 2";
    let refused = shql_paired(&client, partial)
        .await?
        .expect_err("a partial key bound");
    assert!(
        refused.contains("composite sort key"),
        "a partial key refused as: {refused}"
    );
    assert!(
        refused.contains("'name'"),
        "the refusal does not name the missing field: {refused}"
    );
    // a range on any field of it is refused, the field it bounds named
    let ranged = "SELECT * FROM Paired WHERE stream = 7 AND seq > 1 AND name = 'a'";
    let refused = shql_paired(&client, ranged)
        .await?
        .expect_err("a range on a composite key bound");
    assert!(refused.contains("'seq'"), "a range refused as: {refused}");
    assert!(refused.contains("range"), "a range refused as: {refused}");
    // and two sets whose pairings pass the bound are refused before any is built
    let side = SORT_KEY_PRODUCT_LIMIT / 99 + 1;
    let seqs: Vec<String> = (0..side).map(|seq| seq.to_string()).collect();
    let names: Vec<String> = (0..100).map(|name| format!("'{name}'")).collect();
    let wide = format!(
        "SELECT * FROM Paired WHERE stream = 7 AND seq IN ({}) AND name IN ({})",
        seqs.join(", "),
        names.join(", "),
    );
    let refused = shql_paired(&client, &wide)
        .await?
        .expect_err("a product past the bound bound");
    assert!(
        refused.contains(&SORT_KEY_PRODUCT_LIMIT.to_string()),
        "a product past the bound refused as: {refused}"
    );
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// A partition of an integer sort key that a page hands out in pieces walks in its key's order:
/// rows written before a restart and read from their archive, rows written over them since,
/// resident, and a row deleted over its archive, folded together a piece at a time by a scan's
/// page and whole by a driver's (F87, M23)
#[tokio::test(flavor = "multi_thread")]
async fn an_integer_keyed_partition_walks_in_key_order() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::build_single_shard_config(&temp_dir);
    // four hundred rows of a kibibyte, at keys whose decimal strings sort another way, archived
    // by a restart
    let (client, pool) = utils::start_with_conf::<KeyedDb>(conf.clone()).await?;
    let body = "x".repeat(1_000);
    let mut expected: BTreeMap<u64, String> = BTreeMap::new();
    for at in (0..400u64).map(|seq| seq * 7) {
        let row = Counted {
            stream: STREAM,
            at,
            note: format!("{at} {body}"),
        };
        client.send_one(row.clone()).await?;
        expected.insert(at, row.note);
    }
    pool.exit()?;
    drop(client);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (client, pool) = utils::start_with_conf::<KeyedDb>(conf).await?;
    // rows between the archived ones and past them written over the archive, and one deleted
    for at in [1u64, 50, 3_000, 10_000] {
        let row = Counted {
            stream: STREAM,
            at,
            note: format!("late {at}"),
        };
        client.send_one(row.clone()).await?;
        expected.insert(at, row.note);
    }
    let delete = CountedDelete {
        partition_key: STREAM,
        sort_key: 14,
    };
    client.send_one(delete).await?;
    expected.remove(&14);
    // the partition's tablet walked by a scan's page, which folds it in pieces, and a driver's
    let key = Counted {
        stream: STREAM,
        at: 0,
        note: String::new(),
    }
    .get_partition_key();
    for class in [WalkClass::Scan, WalkClass::Driver] {
        let mut ask = WalkAsk::tablet(TableId::of("Counted"), tablet_of(key) as u16);
        ask.class = class;
        ask.limit = 1_000;
        let page = match pool.walk(ask) {
            Ok(Ok(page)) => page,
            other => panic!("a {class:?} page was refused: {other:?}"),
        };
        let partition = page
            .partitions
            .iter()
            .find(|partition| partition.key == key)
            .expect("the page handed out the partition");
        // a scan's page cut it into pieces, whose cursors are integer keys
        if class == WalkClass::Scan {
            assert!(
                partition.pieces().count() > 1,
                "a scan's page handed it out whole"
            );
        }
        // and every row came out once, in the key's order
        let walked: Vec<(u64, String)> = walked_rows::<Counted>(partition)
            .expect("the partition reads back")
            .into_iter()
            .map(|row| (row.at, row.note))
            .collect();
        let wanted: Vec<(u64, String)> = expected.clone().into_iter().collect();
        assert_eq!(
            walked, wanted,
            "a {class:?} page walked the partition out of order"
        );
    }
    // Shutdown server
    pool.exit()?;
    Ok(())
}
