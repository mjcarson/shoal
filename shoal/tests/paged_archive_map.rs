//! Integration tests for the paged archive map
//!
//! Since [F76](../../docs/src/features/paged-archive-map.md) a table's archive map is paged: what
//! a shard holds in memory is the delta since its last flush, each run's directory and filter,
//! and a cache of index pages, and a lookup the delta and the cache cannot answer is read by the
//! loader. These tests write enough partitions under a delta of sixteen and no cache that the
//! map is many runs on disk, restart the server so nothing is resident, and hold every way a
//! query reaches the map to what was written: a get of every partition, and the answers that
//! depend on a partition being nowhere - a get, an exists and a conditional insert of a key that
//! was never written.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Errors;
use shoal::server::conf::Conf;
use shoal::shared::queries::{ConditionRefusal, ConditionalInsert};
use shoal::shared::traits::RkyvSupport;
use shoal::storage::FileSystem;
use shoal::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use std::path::Path;
use std::time::Duration;

mod utils;

use utils::TestError;

/// How many items are written, enough for many runs at a delta of sixteen
const ITEMS: u64 = 6_000;

/// How many buckets of entries are written
const BUCKETS: u64 = 1_500;

/// One item to a partition
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "PagedDb")]
pub struct Item {
    /// The item's name
    #[shoal(partition)]
    pub id: String,
    /// What it holds
    #[shoal(update)]
    pub data: String,
}

/// Entries in a bucket, several to a partition
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "PagedDb")]
pub struct Entry {
    /// The bucket
    #[shoal(partition)]
    pub bucket: String,
    /// The entry's name in its bucket
    #[shoal(sort)]
    pub name: String,
    /// What it holds
    #[shoal(update)]
    pub data: String,
}

/// The test database: a persistent table of each kind
#[db]
pub struct PagedDb {
    /// Unsorted
    pub item: PersistentUnsortedTable<Item, FileSystem>,
    /// Sorted
    pub entry: PersistentSortedTable<Entry, FileSystem>,
}

/// The client this database is spoken to with
type Client = shoal::client::Shoal<PagedDbClient>;

/// An item, its contents derived from its number
///
/// # Arguments
///
/// * `at` - The item's number
fn item(at: u64) -> Item {
    Item {
        id: format!("item-{at}"),
        data: format!("the contents of item {at}"),
    }
}

/// An entry, its contents derived from its bucket and name
///
/// # Arguments
///
/// * `bucket` - The bucket's number
/// * `name` - The entry's number in it
fn entry(bucket: u64, name: u64) -> Entry {
    Entry {
        bucket: format!("bucket-{bucket}"),
        name: format!("entry-{name}"),
        data: format!("entry {name} of bucket {bucket}"),
    }
}

/// How many run files the archive maps under a directory are paged into
///
/// # Arguments
///
/// * `dir` - The storage directory
fn run_files(dir: &Path) -> usize {
    // every table's maps directory, and the run files in it
    let mut runs = 0;
    for table in std::fs::read_dir(dir).expect("the storage dir").flatten() {
        let maps = table.path().join("maps");
        if let Ok(files) = std::fs::read_dir(&maps) {
            runs += files
                .flatten()
                .filter(|file| file.file_name().to_string_lossy().contains(".run-"))
                .count();
        }
    }
    runs
}

/// Start a server on a config, and stop it again once its start compacted everything
///
/// # Arguments
///
/// * `conf` - The config
async fn cycle(conf: &Conf) -> Result<(), TestError> {
    let (_client, pool) = utils::start_with_conf::<PagedDb>(conf.clone()).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Send a bundle of queries, failing on any answer that carries an error
///
/// # Arguments
///
/// * `client` - The client
/// * `queries` - The queries
async fn send_all<Q: Into<PagedDbQueryKinds>>(
    client: &Client,
    queries: impl IntoIterator<Item = Q>,
) -> Result<(), TestError> {
    let mut bundle = client.query();
    for query in queries {
        bundle.add_mut(query);
    }
    let mut stream = client.send(bundle).await?;
    while let Some(response) = stream.next().await? {
        if let Some(error) = response.error() {
            panic!("a write failed: {}", error.msg());
        }
    }
    Ok(())
}

/// Whether a conditional write was refused, and why
///
/// # Arguments
///
/// * `client` - The client
/// * `query` - The write
async fn refusal_of<Q: Into<PagedDbQueryKinds>>(
    client: &Client,
    query: Q,
) -> Result<Option<ConditionRefusal>, TestError> {
    match client.send_one(query).await {
        Ok(_) => Ok(None),
        Err(Errors::Refused { reason, .. }) => Ok(Some(reason)),
        Err(error) => Err(error.into()),
    }
}

/// Every partition of an unsorted table reads back through a map paged into many runs, with
/// nothing resident and no page cached, and a key never written is absent to every query
///
/// The loader reads the index page a lookup needs; a key the delta and the filters rule out is
/// answered on the shard's loop as before. A conditional insert expecting no row is the object
/// store's path creation, and has to be refused for a key that is only on disk.
#[tokio::test]
async fn every_partition_reads_back_through_a_paged_map() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // a delta of sixteen and no page cache, and every partition evicted on every loop
    let conf = utils::build_pressured_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<PagedDb>(conf.clone()).await?;
    let ids: Vec<u64> = (0..ITEMS).collect();
    for chunk in ids.chunks(500) {
        send_all(&client, chunk.iter().map(|at| item(*at))).await?;
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // started twice, so every write is compacted into an archive and nothing is resident
    cycle(&conf).await?;
    cycle(&conf).await?;
    // the maps are paged into runs on disk
    let runs = run_files(temp_dir.path());
    assert!(runs > 1, "the maps were paged into {runs} runs");
    let (client, pool) = utils::start_with_conf::<PagedDb>(conf.clone()).await?;
    // every item reads back as it was written
    for chunk in ids.chunks(250) {
        let keys: Vec<String> = chunk.iter().map(|at| item(*at).id).collect();
        let response = client.send_one(ItemGet::new(keys)).await?;
        let mut rows: Vec<Item> = response
            .access::<Item>()?
            .expect("rows")
            .iter()
            .map(|row| Item::deserialize(row).expect("an item"))
            .collect();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        let mut expected: Vec<Item> = chunk.iter().map(|at| item(*at)).collect();
        expected.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(rows, expected);
    }
    // a key never written is absent to a get and an exists
    for at in ITEMS..ITEMS + 100 {
        assert!(!client.exists(ItemExists::new(item(at).id)).await?);
        // a get that finds nothing is an unsucceeded query to `send_one`, so its answer is read
        // out of a bundle
        let mut bundle = client.query();
        bundle.add_mut(ItemGet::new(vec![item(at).id]));
        let mut stream = client.send(bundle).await?;
        let response = stream.next().await?.expect("a get is answered");
        assert!(response.error().is_none(), "a get of item {at} failed");
        let rows = response.access::<Item>()?.map_or(0, |rows| rows.len());
        assert_eq!(rows, 0, "item {at} was never written");
    }
    // and a conditional insert expecting none is applied for a new key, refused for one on disk
    assert_eq!(
        refusal_of(&client, item(ITEMS + 500).if_absent()).await?,
        None
    );
    assert_eq!(
        refusal_of(&client, item(17).if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    // a removed one is absent once the removal is on disk too
    client.send_one(ItemDelete::new(item(23).id)).await?;
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cycle(&conf).await?;
    let (client, pool) = utils::start_with_conf::<PagedDb>(conf.clone()).await?;
    assert!(!client.exists(ItemExists::new(item(23).id)).await?);
    assert!(client.exists(ItemExists::new(item(24).id)).await?);
    assert!(client.exists(ItemExists::new(item(ITEMS + 500).id)).await?);
    pool.exit()?;
    Ok(())
}

/// Every bucket of a sorted table reads back through a paged map, and an entry never written is
/// absent to a conditional insert while one on disk is refused
#[tokio::test]
async fn every_bucket_reads_back_through_a_paged_map() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let conf = utils::build_pressured_config(&temp_dir);
    let (client, pool) = utils::start_with_conf::<PagedDb>(conf.clone()).await?;
    let buckets: Vec<u64> = (0..BUCKETS).collect();
    for chunk in buckets.chunks(200) {
        send_all(
            &client,
            chunk
                .iter()
                .flat_map(|bucket| (0..3).map(move |name| entry(*bucket, name))),
        )
        .await?;
    }
    pool.exit()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cycle(&conf).await?;
    cycle(&conf).await?;
    let (client, pool) = utils::start_with_conf::<PagedDb>(conf.clone()).await?;
    // every bucket's three entries read back, in sort order
    for bucket in (0..BUCKETS).step_by(7) {
        let response = client
            .send_one(EntryGet::new(vec![entry(bucket, 0).bucket]))
            .await?;
        let rows: Vec<Entry> = response
            .access::<Entry>()?
            .expect("rows")
            .iter()
            .map(|row| Entry::deserialize(row).expect("an entry"))
            .collect();
        let expected: Vec<Entry> = (0..3).map(|name| entry(bucket, name)).collect();
        assert_eq!(rows, expected, "bucket {bucket}");
    }
    // a bucket never written is absent
    for bucket in BUCKETS..BUCKETS + 50 {
        assert!(
            !client
                .exists(EntryExists::new(vec![entry(bucket, 0).bucket]))
                .await?
        );
    }
    // a new entry expecting none is applied, and one on disk is refused
    assert_eq!(
        refusal_of(&client, entry(BUCKETS + 9, 0).if_absent()).await?,
        None
    );
    assert_eq!(
        refusal_of(&client, entry(11, 1).if_absent()).await?,
        Some(ConditionRefusal::RowExists)
    );
    pool.exit()?;
    Ok(())
}
